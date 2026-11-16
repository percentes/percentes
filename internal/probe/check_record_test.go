package probe

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// A check is refused by an ordinary 429, a 5xx status or a dispatched
// request with no status, also when it was then interrupted; the last
// request of an interrupted check that ended on the cancellation, a
// request never dispatched and a blocked check refuse nothing.
func TestOutcomeRefusedBy(t *testing.T) {
	sent := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	rec := func(interrupted string, blocked bool, statuses ...int) *ReleaseRecord {
		r := &ReleaseRecord{Interrupted: interrupted, Start: Start{Blocked: blocked}}
		for i, s := range statuses {
			r.Requests = append(r.Requests, CheckRequest{Index: i, Exchange: Exchange{Status: s, DispatchUTC: sent, ConnAcquiredNs: 1}})
		}
		return r
	}
	lastErr := func(r *ReleaseRecord, e string) *ReleaseRecord {
		r.Requests[len(r.Requests)-1].Exchange.Error = e
		return r
	}
	undispatched := rec("", false, 200, 200)
	undispatched.Requests[1].Exchange.DispatchUTC = time.Time{}
	for _, c := range []struct {
		name    string
		rec     *ReleaseRecord
		outcome string
		by      string
	}{
		{"all 200", rec("", false, 200, 200), CheckRan, ""},
		{"429", rec("", false, 200, 429, 200), CheckRefused, "HTTP 429"},
		{"529", rec("", false, 529, 200), CheckRefused, "HTTP 529"},
		{"500", rec("", false, 200, 500), CheckRefused, "HTTP 500"},
		{"503 before 429", rec("", false, 503, 429), CheckRefused, "HTTP 503"},
		{"no status", rec("", false, 200, 0, 200), CheckRefused, "no response"},
		{"429, then interrupted", rec("context ended", false, 200, 429, 200), CheckRefused, "HTTP 429"},
		{"no status, then interrupted", rec("context ended", false, 0, 200, 0), CheckRefused, "no response"},
		{"interrupted on its last request", lastErr(rec("context ended", false, 200, 0), "cancelled"), CheckStopped, ""},
		{"no response on its last request, then interrupted", lastErr(rec("context ended", false, 200, 0), "connection closed"), CheckRefused, "no response"},
		{"interrupted between requests", rec("context ended", false, 200, 200), CheckStopped, ""},
		{"never dispatched", undispatched, CheckRan, ""},
		{"billing 429 blocks", rec("", true, 200, 429), CheckStopped, "HTTP 429"},
		{"400", rec("", false, 400), CheckRan, ""},
	} {
		if got, by := c.rec.Outcome(), c.rec.RefusedBy(); got != c.outcome || by != c.by {
			t.Errorf("%s: outcome %s, refused by %q; want %s, %q", c.name, got, by, c.outcome, c.by)
		}
	}
}

// spending is a Shape S record of route at ceiling and byteCap admitted
// at ledger line seq whose requests report prompt tokens, exceeded of
// them above the 1,200-token bound.
func spending(route string, ceiling, byteCap, seq, exceeded int) *ReleaseRecord {
	r := checked(route, ceiling, 10)
	r.ByteCap, r.LedgerSeq = byteCap, seq
	for i := 0; i < CeilingRequests; i++ {
		n := 1000
		if i < exceeded {
			n = 1250
		}
		r.Requests = append(r.Requests, CheckRequest{Index: i, Exchange: Exchange{Status: http.StatusOK}, PromptTokens: &n, Pass: true})
	}
	r.InputBound = inputCheck(r.Requests, 1200)
	return r
}

// A byte cap is spent by a Shape S record that ran with a prompt above
// the input bound, the first such record in ledger order; a refused or
// stopped record, missing reports and Shape R spend none.
func TestSpent(t *testing.T) {
	short := spending("azure", 512, 2000, 3, 0)
	n := 900
	short.Requests[0].PromptTokens = nil
	short.Requests[1].PromptTokens = &n
	refusedOver := spending("gemini", 512, 2000, 4, 2)
	refusedOver.Requests[5].Exchange.Status = http.StatusTooManyRequests
	stoppedOver := spending("gemini", 512, 2200, 5, 2)
	stoppedOver.Interrupted = "the process ended"
	shapeR := spending("anthropic", 16, 0, 6, 3)
	shapeR.Shape = config.ShapeR
	first, later := spending("gemini", 512, 2000, 9, 1), spending("baseten", 1024, 2000, 7, 4)
	got := Spent([]*ReleaseRecord{short, refusedOver, stoppedOver, shapeR, first, later, spending("baseten", 512, 1800, 8, 0)})
	if len(got) != 1 || got[2000] != later {
		t.Fatalf("spent %v", got)
	}
}

// A Shape S record at a superseded byte cap decides nothing: a re-check
// at a lower cap decides; a host whose only checks at a ceiling are
// superseded is excluded on the input bound where its latest one there
// exceeded it, and otherwise has no check; a deciding record above the
// input bound excludes the host group.
func TestChooseCeilingSuperseded(t *testing.T) {
	s := &config.Shape{Name: config.ShapeS}
	groq := []string{"groq"}
	failed, passed := spending("groq", 512, 2000, 4, 3), spending("groq", 512, 1800, 6, 0)
	for _, c := range []struct {
		name       string
		recs       []*ReleaseRecord
		superseded []int
		want       CeilingChoice
		err        string
	}{
		{"re-checked at a lower cap", []*ReleaseRecord{failed, passed}, []int{2000}, CeilingChoice{Ceiling: 512, Reason: "every host passed at 512"}, ""},
		{"not superseded", []*ReleaseRecord{failed, passed}, nil, CeilingChoice{},
			"probe: route groq at ceiling 512: the check admitted at ledger line 4 was not refused and decides; the check at line 6 repeats it"},
		{"no re-check", []*ReleaseRecord{failed}, []int{2000}, CeilingChoice{Excluded: true,
			Reason: "groq: the input bound failed at 512 at byte cap 2,000: 10 of 10 requests reported, 3 above 1,200 tokens"}, ""},
		{"deciding record above the bound", []*ReleaseRecord{failed}, nil, CeilingChoice{Excluded: true,
			Reason: "groq: the input bound failed at 512 at byte cap 2,000: 10 of 10 requests reported, 3 above 1,200 tokens"}, ""},
		{"a passed check superseded", []*ReleaseRecord{spending("groq", 512, 2000, 4, 0)}, []int{2000}, CeilingChoice{},
			"probe: route groq has no check at 512 that was not stopped or superseded (0 stopped, 1 superseded)"},
	} {
		got, err := ChooseCeilingAt(s, groq, c.recs, c.superseded)
		if !reflect.DeepEqual(got, c.want) || fmt.Sprint(err) != map[bool]string{true: "<nil>", false: c.err}[c.err == ""] {
			t.Errorf("%s: got %+v, %v; want %+v, %q", c.name, got, err, c.want, c.err)
		}
	}
}

// An inconclusive exclusion names each refusal's status and ends with the
// inconclusive clause, a host left out on its model id named before it.
func TestChooseCeilingInconclusiveReason(t *testing.T) {
	r := &config.Shape{Name: config.ShapeR}
	overloaded := refused("azure-R", 16, 3)
	overloaded.Requests[4].Exchange.Status = 529
	dropped := refused("bedrock", 16, 5)
	dropped.Requests[4].Exchange = Exchange{DispatchUTC: time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC), ConnAcquiredNs: 1}
	left := reporting("groq", 16, 10, "", "openai/gpt-oss-120b-v2")
	left.Shape = config.ShapeR
	got, err := ChooseCeiling(r, []string{"azure-R", "bedrock", "groq"}, []*ReleaseRecord{overloaded, dropped, left})
	want := "groq left out on the reported model id; azure-R: ceiling check inconclusive, HTTP 529 at 16; bedrock: ceiling check inconclusive, no response at 16"
	if err != nil || !got.Excluded || got.Reason != want {
		t.Fatalf("got %+v, %v; want %q", got, err, want)
	}
}

// CheckSnapshots reads every limit's snapshot and refuses a file source
// whose retained copy is missing or does not match its recorded hash; it
// reads nothing with no directory.
func TestCheckSnapshots(t *testing.T) {
	cfg := releaseConfig(t, "https://api.example.test/v1/chat/completions")
	dir := t.TempDir()
	b := []byte("the provider's limits page")
	if err := os.WriteFile(filepath.Join(dir, "limits.png"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 1000, Marking: config.MarkingPublic, Source: "limits.png", SHA256: hex.EncodeToString(sum[:])},
		{Dimension: config.LimitTPM, Value: 250000, Marking: config.MarkingPublic, Source: "https://example.test/limits"}}
	in := ReleaseInput{Config: cfg, SnapshotDir: dir}
	snaps, err := CheckSnapshots(in)
	if err != nil || len(snaps) != 2 || snaps[0].Retained == nil || !*snaps[0].Retained || snaps[1].Retained != nil {
		t.Fatalf("matching: %+v, %v", snaps, err)
	}
	if snaps, err := CheckSnapshots(ReleaseInput{Config: cfg}); snaps != nil || err != nil {
		t.Fatalf("no directory: %+v, %v", snaps, err)
	}
	cfg.Target.Limits[0].SHA256 = strings.Repeat("0", 64)
	if _, err := CheckSnapshots(in); err == nil || !strings.Contains(err.Error(), `retained copy "limits.png" does not match its recorded sha256`) {
		t.Fatalf("mismatch: %v", err)
	}
	cfg.Target.Limits[0].Source = "missing.png"
	if _, err := CheckSnapshots(in); err == nil || !strings.Contains(err.Error(), `retained copy "missing.png" not read: no such file`) {
		t.Fatalf("missing: %v", err)
	}
}

// Write replaces the record through a synced temporary file and leaves
// none behind; WriteRetained refuses an existing file and leaves it as
// it was; a failed write leaves no record.
func TestWriteSynced(t *testing.T) {
	dir := t.TempDir()
	for _, rec := range []*ReleaseRecord{{Route: "groq"}, {Route: "azure"}} {
		if _, err := Write(dir, rec); err != nil {
			t.Fatal(err)
		}
	}
	names := func() []string {
		es, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range es {
			out = append(out, e.Name())
		}
		return out
	}
	if got := names(); !reflect.DeepEqual(got, []string{File}) {
		t.Fatalf("files %v", got)
	}
	if b, err := os.ReadFile(filepath.Join(dir, File)); err != nil || !strings.Contains(string(b), `"route": "azure"`) {
		t.Fatalf("record %s, %v", b, err)
	}
	held := &ReleaseRecord{Route: "groq", Requests: []CheckRequest{{Exchange: Exchange{Retained: &Retained{}}}}}
	path, err := WriteRetained(dir, held)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if _, err := WriteRetained(dir, held); err == nil {
		t.Fatal("an existing retained file replaced")
	}
	if after, _ := os.ReadFile(path); string(after) != string(before) {
		t.Fatal("the existing retained file changed")
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("retained file mode %v, %v", fi.Mode(), err)
	}
	if got := names(); !reflect.DeepEqual(got, []string{RetainedFile, File}) {
		t.Fatalf("files %v", got)
	}
	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	if _, err := Write(locked, &ReleaseRecord{}); err == nil {
		t.Fatal("a write into a read-only directory succeeded")
	}
	if es, _ := os.ReadDir(locked); len(es) != 0 {
		t.Fatalf("a failed write left %v", es)
	}
}
