package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/probe"
)

// checkRecordsOf are the ceiling-check records of route under out by their
// admission's ledger line.
func checkRecordsOf(t *testing.T, out, route string) map[int]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(out, "ceiling-*", "*", probe.File))
	if err != nil {
		t.Fatal(err)
	}
	m := map[int]string{}
	for _, p := range paths {
		rec := readRecord(t, p)
		if rec.Route == route {
			m[rec.LedgerSeq] = p
		}
	}
	return m
}

func readRecord(t *testing.T, path string) *probe.ReleaseRecord {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rec probe.ReleaseRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return &rec
}

// rewriteRecord rewrites the record at path through edit, as ceiling-check
// encodes it.
func rewriteRecord(t *testing.T, path string, edit func(*probe.ReleaseRecord)) {
	t.Helper()
	rec := readRecord(t, path)
	edit(rec)
	b, err := probe.Encode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// refusingMock is a loopback mock route at its chat-completions URL whose
// nth request, 1-based, is answered an ordinary 429 where refuse(n), with
// the count of requests it received.
func refusingMock(t *testing.T, refuse func(n int32) bool) (string, *atomic.Int32) {
	t.Helper()
	endpoint := startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."})
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: u.Scheme, Host: u.Host})
	rp.FlushInterval = -1
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse(hits.Add(1)) {
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":{"type":"rate_limit_exceeded"}}`) //nolint:errcheck
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + u.Path, &hits
}

// fastCeilingFixture is newCeilingFixture with a route cap and a token
// limit that pace the check's requests about 0.25 s apart.
func fastCeilingFixture(t *testing.T, endpoint, key string) *ceilingFixture {
	t.Helper()
	path := routeConfigFile(t, "groq", endpoint, "100")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const tpm = "\n      value: 250000\n"
	if strings.Count(string(b), tpm) != 1 {
		t.Fatalf("configuration: %q not found once", tpm)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(b), tpm, "\n      value: 2500000\n", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	return ceilingFixtureAt(t, path, key)
}

// A check with an ordinary 429 is refused, prints so, and is not
// repeated before the quiet interval after its end; its one repeat
// decides, and a third check is refused before any request. run reads
// both records, and refuses either alone, a repeat sent inside the quiet
// interval, and a record of a line that admits no check.
func TestCeilingCheckRefusedRepeat(t *testing.T) {
	endpoint, hits := refusingMock(t, func(n int32) bool { return n == 3 })
	f := fastCeilingFixture(t, endpoint, testKey)
	var code int
	var err error
	text := stdoutOf(t, func() { code, err = ceilingCheckCmd(context.Background(), f.args()) })
	if err != nil || code != exitAdverse || !strings.Contains(text, "groq: REFUSED: a request was answered HTTP 429") {
		t.Fatalf("ceiling-check: %d, %v:\n%s", code, err, text)
	}
	first := checkRecordsOf(t, f.out, "groq")
	if len(first) != 1 {
		t.Fatalf("records %v", first)
	}
	var refusedPath string
	var refusedSeq int
	for seq, p := range first {
		refusedSeq, refusedPath = seq, p
	}
	if rec := readRecord(t, refusedPath); rec.Outcome() != probe.CheckRefused || rec.LedgerSeq == 0 || rec.LedgerSHA256 != ledgerOpenSHA256(t, f.ledger) {
		t.Fatalf("outcome %s, line %d", rec.Outcome(), rec.LedgerSeq)
	}
	sent := hits.Load()
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err == nil || code != exitError || !strings.Contains(err.Error(), "may be repeated from") || hits.Load() != sent {
		t.Fatalf("a repeat inside the quiet interval: %d, %v, %d requests", code, err, hits.Load()-sent)
	}
	rewriteRecord(t, refusedPath, func(r *probe.ReleaseRecord) { r.EndUTC = r.EndUTC.Add(-time.Hour) })
	text = stdoutOf(t, func() { code, err = ceilingCheckCmd(context.Background(), f.args()) })
	if err != nil || code != exitOK {
		t.Fatalf("the repeat: %d, %v:\n%s", code, err, text)
	}
	both := checkRecordsOf(t, f.out, "groq")
	var repeatPath string
	for seq, p := range both {
		if seq != refusedSeq {
			repeatPath = p
		}
	}
	if len(both) != 2 || repeatPath == "" {
		t.Fatalf("records %v", both)
	}
	sent = hits.Load()
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err == nil || code != exitError || !strings.Contains(err.Error(), "decides (ran); it is not repeated") || hits.Load() != sent {
		t.Fatalf("a third check: %d, %v", code, err)
	}

	cfgs, err := loadReleases([]string{f.cfg})
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, f.ledger)
	hosts := byRoute(hostConfigs(t, cfgs[0]))
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, open))
	entries, err := ledgerEntries(f.ledger, open)
	if err != nil {
		t.Fatal(err)
	}
	choice, copies, _, err := ceilingChoice(cfgs[0], hosts, []string{refusedPath, repeatPath, baseten}, 2000, open, entries)
	want := probe.CeilingChoice{Ceiling: 512, Reason: fmt.Sprintf("every host passed at 512; groq: the check refused at 512 (ledger line %d) was repeated", refusedSeq)}
	if err != nil || !reflect.DeepEqual(choice, want) || len(copies) != 3 {
		t.Fatalf("both records: %+v, %d copies, %v", choice, len(copies), err)
	}
	if _, _, _, err := ceilingChoice(cfgs[0], hosts, []string{repeatPath, baseten}, 2000, open, entries); err == nil ||
		!strings.Contains(err.Error(), "admits a ceiling check of route groq and no ceiling-check record given names it") {
		t.Fatalf("the repeat alone: %v", err)
	}
	if choice, _, _, err := ceilingChoice(cfgs[0], hosts, []string{refusedPath, baseten}, 2000, open, entries[:refusedSeq+1]); err != nil ||
		!choice.Excluded || choice.Reason != "groq: ceiling check inconclusive, HTTP 429 at 512" {
		t.Fatalf("the refused check alone, before the repeat: %+v, %v", choice, err)
	}
	if _, _, _, err := ceilingChoice(cfgs[0], hosts, []string{refusedPath, repeatPath, baseten}, 2000, open, entries[:refusedSeq+1]); err == nil ||
		!strings.Contains(err.Error(), "admits no ceiling check of the route up to the line the schedule froze") {
		t.Fatalf("a record after the frozen line: %v", err)
	}
	rewriteRecord(t, refusedPath, func(r *probe.ReleaseRecord) { r.EndUTC = r.EndUTC.Add(time.Hour) })
	if _, _, _, err := ceilingChoice(cfgs[0], hosts, []string{refusedPath, repeatPath, baseten}, 2000, open, entries); err == nil ||
		!strings.Contains(err.Error(), "the repeat waits") {
		t.Fatalf("a repeat inside the quiet interval: %v", err)
	}
}

// A check whose process ended before it wrote its record leaves its
// admission unsettled: ceiling-check then refuses to send, --recover
// refuses while another process holds the route, and then settles the
// admission at its reservation and writes an interrupted record, after
// which the check may be sent again and run reads both records.
func TestCeilingCheckRecover(t *testing.T) {
	endpoint, hits := refusingMock(t, func(int32) bool { return false })
	f := fastCeilingFixture(t, endpoint, testKey)
	cfgs, err := loadReleases([]string{f.cfg})
	if err != nil {
		t.Fatal(err)
	}
	l, err := admission.Open(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	a, err := l.AdmitCheck("groq", "groq", probe.CeilingRequests, admission.ReleaseProbeTokens(cfgs[0]))
	if err != nil {
		t.Fatal(err)
	}
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err == nil || code != exitError || !strings.Contains(err.Error(), "--recover writes it") || hits.Load() != 0 {
		t.Fatalf("with an unsettled check: %d, %v, %d requests", code, err, hits.Load())
	}
	owner, err := l.LockRoute("groq")
	if err != nil {
		t.Fatal(err)
	}
	if code, err := ceilingCheckCmd(context.Background(), append(f.args(), "--recover")); err == nil || code != exitError || !strings.Contains(err.Error(), "another process holds it") {
		t.Fatalf("--recover while the route is held: %d, %v", code, err)
	}
	owner.Close()
	var code int
	text := stdoutOf(t, func() { code, err = ceilingCheckCmd(context.Background(), append(f.args(), "--recover")) })
	if err != nil || code != exitOK || !strings.Contains(text, "settled at 10 requests sent") {
		t.Fatalf("--recover: %d, %v:\n%s", code, err, text)
	}
	recs := checkRecordsOf(t, f.out, "groq")
	rec := readRecord(t, recs[a.Seq])
	if rec.Interrupted == "" || rec.Outcome() != probe.CheckStopped || rec.Sent() != 0 {
		t.Fatalf("recovered record %+v", rec)
	}
	if sent, reserved := f.probeCharge(t); sent != 10 || reserved != 0 {
		t.Fatalf("ledger: %d probe requests, %d reserved", sent, reserved)
	}
	if code, err := ceilingCheckCmd(context.Background(), append(f.args(), "--recover")); err == nil || code != exitError {
		t.Fatalf("--recover with nothing unsettled: %d, %v", code, err)
	}
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err != nil || code != exitOK {
		t.Fatalf("the check after recovery: %d, %v", code, err)
	}
	recs = checkRecordsOf(t, f.out, "groq")
	var files []string
	for _, p := range recs {
		files = append(files, p)
	}
	open := ledgerOpenSHA256(t, f.ledger)
	entries, err := ledgerEntries(f.ledger, open)
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, open)))
	if choice, _, _, err := ceilingChoice(cfgs[0], byRoute(hostConfigs(t, cfgs[0])), files, 2000, open, entries); err != nil || choice.Ceiling != 512 {
		t.Fatalf("run: %+v, %v", choice, err)
	}
}

// A ceiling check is not sent while another route's check on its account
// group is unsettled (admission.Ledger.AdmitCheck).
func TestCeilingCheckAccountGroup(t *testing.T) {
	endpoint, hits := refusingMock(t, func(int32) bool { return false })
	f := newCeilingFixture(t, endpoint, testKey)
	other := routeConfigFile(t, "baseten", endpoint, "10")
	b, err := os.ReadFile(other)
	if err != nil {
		t.Fatal(err)
	}
	other = writeFile(t, t.TempDir(), "baseten.yaml", []byte(strings.Replace(string(b), "\n  account_group: baseten\n", "\n  account_group: groq\n", 1)))
	f.ledger = filepath.Join(t.TempDir(), "ledger.jsonl")
	if code, err := ledgerCmd([]string{"create", "--ledger", f.ledger, "--config", f.cfg, "--config", other}); err != nil || code != exitOK {
		t.Fatalf("ledger create: %d, %v", code, err)
	}
	for _, p := range []string{f.cfg, other} {
		if code, err := ledgerCmd([]string{"price", "--ledger", f.ledger, "--config", p, "--input", "0.15", "--output", "0.60",
			"--source", "https://example.test/pricing", "--retrieved", "2026-10-06"}); err != nil || code != exitOK {
			t.Fatalf("ledger price: %d, %v", code, err)
		}
	}
	cfgs, err := loadReleases([]string{other})
	if err != nil {
		t.Fatal(err)
	}
	l, err := admission.Open(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	a, err := l.AdmitCheck("baseten", cfgs[0].Release.AccountGroup, probe.CeilingRequests, admission.ReleaseProbeTokens(cfgs[0]))
	if err != nil {
		t.Fatal(err)
	}
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err == nil || code != exitError ||
		!strings.Contains(err.Error(), "holds an unsettled ceiling check of route baseten on account group groq") || hits.Load() != 0 {
		t.Fatalf("beside baseten's check: %d, %v, %d requests", code, err, hits.Load())
	}
	if err := l.Settle(a, admission.Counts{Probe: 10}); err != nil {
		t.Fatal(err)
	}
}

// run refuses a ceiling check admitted on its account group after the
// line the schedule froze, and so does lateChecks for a check that names
// the group; a check on another group, or before the line, passes.
func TestLateChecks(t *testing.T) {
	cfg, err := config.LoadFile(routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10"))
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, cfg, "+0.005032")
	l, err := admission.Open(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	a, err := l.AdmitCheck("groq", "groq", probe.CeilingRequests, admission.ReleaseProbeTokens(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Settle(a, admission.Counts{Probe: 10}); err != nil {
		t.Fatal(err)
	}
	enoughOpenFiles(t)
	if _, err := newRoute(f.in); err == nil || !strings.Contains(err.Error(), "admits a ceiling check of route groq on account group groq after line") {
		t.Fatalf("run: %v", err)
	}
	entries := []admission.Entry{{Seq: 1}, {Seq: 2, Kind: admission.KindAdmit, Purpose: admission.PurposeProbe, Route: "azure", AccountGroup: "azure"},
		{Seq: 3, Kind: admission.KindAdmit, Purpose: admission.PurposeProbe, Route: "gemini", AccountGroup: "gemini"}}
	for _, c := range []struct {
		frozen int
		group  string
		routes []string
		fails  bool
	}{
		{1, "azure", []string{"azure-R"}, true},
		{1, "azure", nil, true},
		{2, "azure", []string{"azure", "azure-R"}, false},
		{1, "baseten", []string{"baseten"}, false},
		{3, "gemini", []string{"gemini"}, false},
	} {
		if err := lateChecks(entries, c.frozen, c.group, c.routes); (err != nil) != c.fails {
			t.Errorf("frozen at %d, group %s: %v", c.frozen, c.group, err)
		}
	}
}

// soloFile is the configuration at cfgPath with no other host, saved as
// name.
func soloFile(t *testing.T, cfgPath, name string) string {
	t.Helper()
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	i := strings.Index(s, "\n  other_hosts:")
	if i < 0 {
		t.Fatal("configuration: no other_hosts")
	}
	rest := s[i+1:]
	for k := 0; k < 3; k++ {
		rest = rest[strings.Index(rest, "\n")+1:]
	}
	return writeFile(t, t.TempDir(), name, []byte(s[:i+1]+rest))
}

// A scheduled route whose ceiling check was refused and not repeated
// is excluded and runs no cell; beside a route with a cell, pages
// writes its page and JSON stating the exclusion and its reason, one
// index line, and the route's pack carries both. Pages refuses without
// the excluded route's configuration or its check record, and schedule
// refuses without a check's record.
func TestPagesExcludedRoute(t *testing.T) {
	publishAt(t, noticeClosed)
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusNotFound, "" })
	groqPath := soloFile(t, routeConfigFile(t, "groq", e.url(), "10"), "groq.yaml")
	endpoint, _ := refusingMock(t, func(n int32) bool { return n == 2 })
	soloPath := shapeRFile(t, soloFile(t, routeConfigFile(t, "solo", endpoint, "10"), "solo.yaml"), false)
	snapshots := snapshotted(t, soloPath)
	ledger := releaseLedger(t, groqPath, soloPath)
	cfgs, err := loadReleases([]string{groqPath, soloPath})
	if err != nil {
		t.Fatal(err)
	}
	groqCfg := cfgs[0]
	open := ledgerOpenSHA256(t, ledger)

	l, err := admission.Open(ledger)
	if err != nil {
		t.Fatal(err)
	}
	a, err := l.AdmitCheck("groq", "groq", probe.CeilingRequests, admission.ReleaseProbeTokens(groqCfg))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Settle(a, admission.Counts{Probe: probe.CeilingRequests}); err != nil {
		t.Fatal(err)
	}
	groqRec := passedCeiling(groqCfg, 2000, open)
	groqRec.LedgerSeq = a.Seq
	groqCheck := writeCeiling(t, *groqRec)

	out := t.TempDir()
	env := writeFile(t, t.TempDir(), "solo.env", []byte("GROQ_API_KEY="+testKey+"\n"))
	var code int
	text := stdoutOf(t, func() {
		code, err = ceilingCheckCmd(context.Background(), []string{"--config", soloPath, "--env", env, "--ledger", ledger, "--out", out, "--snapshots", snapshots})
	})
	if err != nil || code != exitAdverse || !strings.Contains(text, "solo: REFUSED") {
		t.Fatalf("solo's check: %d, %v:\n%s", code, err, text)
	}
	var soloCheck string
	for _, p := range checkRecordsOf(t, out, "solo") {
		soloCheck = p
	}

	schedulePath := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groqPath, "--config", soloPath, "--ledger", ledger, "--release-key", testReleaseKeyPath, "--ceiling-probe", groqCheck}
	if code, err := scheduleCmd(append(args, "--schedule", schedulePath)); err == nil || code != exitError ||
		!strings.Contains(err.Error(), "admits a ceiling check of route solo and no ceiling-check record given names it") {
		t.Fatalf("schedule without solo's record: %d, %v", code, err)
	}
	if code, err := scheduleCmd(append(args, "--ceiling-probe", soloCheck, "--schedule", schedulePath)); err == nil || code != exitError ||
		!strings.Contains(err.Error(), "schedule: route solo: the ceiling check excludes the host group (solo: ceiling check inconclusive, HTTP 429 at 16); --excluded does not name it") {
		t.Fatalf("schedule without --excluded solo: %d, %v", code, err)
	}
	text = stdoutOf(t, func() {
		code, err = scheduleCmd(append(args, "--ceiling-probe", soloCheck, "--excluded", "solo", "--schedule", schedulePath))
	})
	if err != nil || code != exitOK || !strings.Contains(text, "solo: excluded by the ceiling check: solo: ceiling check inconclusive, HTTP 429 at 16; offset ") {
		t.Fatalf("schedule: %d, %v\n%s", code, err, text)
	}

	t.Setenv(groqCfg.Target.APIKeyEnv, testKey)
	f := newFixture(t, groqCfg, "+0.005032")
	f.ledger, f.open, f.in.ledgerPath, f.in.schedulePath, f.in.probeFiles = ledger, open, ledger, schedulePath, []string{groqCheck}
	f.in.configName = "groq.yaml"
	enoughOpenFiles(t)
	groq, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	shortLadder(groq)
	if rec := runCellOK(t, groq, groq.runs[0]); rec.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	solo := f.in
	solo.cfg, solo.probeFiles, solo.byteCap = cfgs[1], []string{soloCheck}, 0
	if _, err := newRoute(solo); err == nil || !strings.Contains(err.Error(), "run: the ceiling check excludes the host group: solo: ceiling check inconclusive, HTTP 429 at 16") {
		t.Fatalf("solo's run: %v", err)
	}

	pages := t.TempDir()
	base := []string{"--out", groq.out, "--ledger", ledger, "--frames", groq.framesRoot, "--schedule", schedulePath}
	full := append(base[:len(base):len(base)], "--config", groqPath, "--config", soloPath, "--check-record", soloCheck)
	text = stdoutOf(t, func() { code, err = pagesCmd(append(full[:len(full):len(full)], "--pages", pages)) })
	if err != nil || code != exitOK || !strings.Contains(text, "solo: excluded by the ceiling check: solo: ceiling check inconclusive, HTTP 429 at 16; page ") {
		t.Fatalf("pages: %d, %v:\n%s", code, err, text)
	}
	files := readPages(t, pages)
	page, index := string(files["routes/solo.txt"]), string(files[indexPage])
	if !strings.Contains(page, "Route solo (Groq, model openai/gpt-oss-120b, Shape R at 16) is excluded from the release by the ceiling check (SPEC-HOSTED HA4, Shape R and refused checks): solo: ceiling check inconclusive, HTTP 429 at 16.\n") ||
		!strings.Contains(page, ", refused, sha256 ") || files["routes/solo.json"] == nil || files["routes/groq.txt"] == nil {
		t.Fatalf("files %v; solo's page:\n%s", keys(files), page)
	}
	if !strings.Contains(index, ", [solo](routes/solo.txt), openai/gpt-oss-120b, Shape R: solo: ceiling check inconclusive, HTTP 429 at 16\n") || strings.Contains(index, "| solo |") {
		t.Fatalf("index:\n%s", index)
	}
	var x routeExclusion
	if err := json.Unmarshal(files["routes/solo.json"], &x); err != nil || !x.Excluded || len(x.Records) != 1 || x.Records[0].Outcome != probe.CheckRefused {
		t.Fatalf("solo.json %+v, %v", x, err)
	}
	packed, err := evidence.ReadPack(packOf(t, full, "solo"))
	if err != nil || packed["pages/routes/solo.txt"] == nil || packed["pages/routes/solo.json"] == nil {
		t.Fatalf("solo's pack: %v: %v", err, keys(packed))
	}
	for _, r := range x.Records {
		b := packed["out/solo/"+r.Name]
		onDisk, err := os.ReadFile(filepath.Join(groq.out, "solo", r.Name))
		if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != r.SHA256 || err != nil || !bytes.Equal(onDisk, b) {
			t.Fatalf("record %s: in the pack %t, under --out %v; keys %v", r.Name, b != nil, err, keys(packed))
		}
	}

	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"no configuration", append(base[:len(base):len(base)], "--config", groqPath, "--check-record", soloCheck), "pages: route solo of the schedule, whose ceiling check ledger line 7 admits, has no --config"},
		{"no check record", append(base[:len(base):len(base)], "--config", groqPath, "--config", soloPath), "pages: route solo has no cell and no --check-record of its host group"},
	} {
		other := filepath.Join(t.TempDir(), "pages")
		if code, err := pagesCmd(append(c.args, "--pages", other)); code != exitError || err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: exit %d: %v, want %q", c.name, code, err, c.want)
		}
		if _, err := os.Stat(other); !os.IsNotExist(err) {
			t.Errorf("%s: pages written on a refusal: %v", c.name, err)
		}
	}
}

// Every ceiling-check admission of a route given has exactly one record
// naming its line, and a record names only a check of its route; pages
// applies the rule to the records the manifests and --check-record hold.
func TestCheckAdmitted(t *testing.T) {
	probeAt := func(seq int, route string) admission.Entry {
		return admission.Entry{Seq: seq, Kind: admission.KindAdmit, Purpose: admission.PurposeProbe, Route: route}
	}
	entries := []admission.Entry{{Seq: 1}, probeAt(2, "azure"), {Seq: 3, Kind: admission.KindAdmit, Purpose: admission.PurposeCell, Route: "azure"}, probeAt(4, "gemini")}
	rec := func(route string, seq int) *probe.ReleaseRecord {
		return &probe.ReleaseRecord{Route: route, LedgerSeq: seq}
	}
	for _, c := range []struct {
		name   string
		routes []string
		recs   []*probe.ReleaseRecord
		want   string
	}{
		{"each check recorded", nil, []*probe.ReleaseRecord{rec("azure", 2), rec("gemini", 4)}, ""},
		{"another route's check unrecorded", []string{"azure"}, []*probe.ReleaseRecord{rec("azure", 2)}, ""},
		{"a check unrecorded", nil, []*probe.ReleaseRecord{rec("azure", 2)}, "ledger line 4 admits a ceiling check of route gemini and no ceiling-check record given names it"},
		{"a record of another route's line", nil, []*probe.ReleaseRecord{rec("azure", 2), rec("azure", 4)}, "ledger line 4 admits no ceiling check of the route"},
		{"a record of a cell's line", nil, []*probe.ReleaseRecord{rec("azure", 3)}, "ledger line 3 admits no ceiling check of the route"},
		{"two records of one line", nil, []*probe.ReleaseRecord{rec("azure", 2), rec("azure", 2), rec("gemini", 4)}, "another record names ledger line 2"},
		{"a record naming no line", nil, []*probe.ReleaseRecord{rec("azure", 0), rec("azure", 2), rec("gemini", 4)}, ""},
	} {
		err := checkAdmitted(entries, c.routes, c.recs)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	if err := allChecks(nil, nil, entries, []string{"gemini"}); err == nil || !strings.Contains(err.Error(), "pages: ledger line 4 admits a ceiling check of route gemini") {
		t.Fatalf("pages with no record: %v", err)
	}
}
