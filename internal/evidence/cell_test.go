package evidence

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

func releaseCfg(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// releaseCell is a cell record that agrees with the reference release
// configuration.
func releaseCell(cfg *config.Config) *Cell {
	start := time.Date(2026, 10, 17, 8, 20, 0, 0, time.UTC)
	tpm := admission.Demand{Dimension: config.LimitTPM, Limit: 1, Value: 250000, Demand: 205440, Fraction: 0.82176, Half: true}
	return &Cell{
		Run:           schedule.Run{Route: cfg.Target.Name, Label: "weekend-08", Attempt: 1, Date: "2026-10-17", Start: start, Seed: 7},
		OffsetMinutes: 20,
		Concurrent:    []schedule.Run{{Route: "baseten", Label: "weekend-08", Attempt: 1, Date: "2026-10-17", Start: start.Add(10 * time.Minute), Seed: 8}},
		Admission: &admission.Verdict{Route: cfg.Target.Name, InputTokens: 1200, CeilingTokens: 512, TopRPS: 1, Steps: []admission.Step{
			{RPS: 0.25, Admitted: true}, {RPS: 0.5, Admitted: true}, {RPS: 1, Admitted: true},
			{RPS: 2, Binding: &tpm, Demands: []admission.Demand{tpm}}, {RPS: 4, Binding: &tpm},
		}},
		CeilingTokens:     512,
		Ceiling:           probe.CeilingChoice{Ceiling: 512, Reason: "every host passed at 512"},
		Probes:            []FileRef{refOf("probe-groq-512.json", []byte("{}"))},
		AccountCreated:    "2026-09-15",
		PreWindowRequests: 25,
	}
}

// A release record carries the request's shape, ceiling, step and
// deadline split, the reasoning token count from its usage object and the
// reuse flag, and never the answer.
func TestReleaseRecords(t *testing.T) {
	cfg := releaseCfg(t)
	const answer = "Billed twice; refund the second charge. secret-answer-text"
	res := &loadgen.Result{Requests: []loadgen.Request{
		{Index: 0, Step: 1, Warmup: true, Outcome: loadgen.OutcomeCompleted, Content: answer, Tokens: 4},
		{Index: 1, Step: 1, Outcome: loadgen.OutcomeCompleted, Late: true, Content: answer, ConnReused: true,
			Usage: json.RawMessage(`{"completion_tokens":40,"completion_tokens_details":{"reasoning_tokens":31}}`), UsageSeen: true, CompletionTokens: 40},
		{Index: 2, Step: 2, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrMalformedStream, Content: answer},
		{Index: 3, Step: 5},
	}}
	var b bytes.Buffer
	if err := Write(&b, Records(cfg, res, Options{})); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "secret-answer-text") || strings.Contains(b.String(), `"answer"`) {
		t.Fatalf("answer text in a release record:\n%s", b.Bytes())
	}
	got := lines(t, b.Bytes())
	want := []map[string]string{
		{"shape": `"S"`, "ceiling_tokens": "512", "step": "1", "warmup": "true", "in_time": "true"},
		{"shape": `"S"`, "ceiling_tokens": "512", "step": "1", "late": "true", "reasoning_tokens": "31", "conn_reused": "true"},
		{"shape": `"S"`, "ceiling_tokens": "512", "step": "2"},
		{"shape": `"S"`, "ceiling_tokens": "512", "step": "5"},
	}
	optional := []string{"warmup", "in_time", "late", "reasoning_tokens", "conn_reused"}
	for i, m := range got {
		for _, k := range append([]string{"shape", "ceiling_tokens", "step"}, optional...) {
			if string(m[k]) != want[i][k] {
				t.Errorf("line %d: %s = %s, want %q", i, k, m[k], want[i][k])
			}
		}
	}

	back, err := ReadRecords(&b)
	if err != nil {
		t.Fatal(err)
	}
	for i := range back {
		r, w := back[i].Request(), res.Requests[i]
		if r.Step != w.Step || r.Warmup != w.Warmup || r.Late != w.Late || r.Content != "" {
			t.Errorf("request %d read back: step %d warm-up %v late %v content %q", i, r.Step, r.Warmup, r.Late, r.Content)
		}
	}
}

// A release run scored for a test still scores the answer it received.
func TestReleaseScoringReadsTheRequest(t *testing.T) {
	res := &loadgen.Result{Requests: []loadgen.Request{
		{Index: 0, Outcome: loadgen.OutcomeCompleted, Content: `{"route":"billing"}`, FinishReason: "stop"},
	}}
	recs := Records(releaseCfg(t), res, Options{Scoring: &Scoring{Scorer: testScorer(t), Expected: []string{"billing"}}})
	if s := recs[0].Score; recs[0].Answer != nil || s == nil || s.Route != "billing" {
		t.Fatalf("answer %v, score %+v", recs[0].Answer, s)
	}
}

func TestReasoningTokens(t *testing.T) {
	n := func(v int) *int { return &v }
	for _, tc := range []struct {
		name, usage string
		want        *int
	}{
		{"none", `{"completion_tokens":5}`, nil},
		{"empty", ``, nil},
		{"not JSON", `{"reasoning_tokens":`, nil},
		{"top level", `{"reasoning_tokens":3}`, n(3)},
		{"zero", `{"completion_tokens_details":{"reasoning_tokens":0}}`, n(0)},
		{"nested", `{"completion_tokens_details":{"reasoning_tokens":31,"accepted_prediction_tokens":0}}`, n(31)},
		{"in an array", `{"parts":[{"x":1},{"reasoning_tokens":9}]}`, n(9)},
		{"two that agree", `{"reasoning_tokens":4,"details":{"reasoning_tokens":4}}`, n(4)},
		{"two that disagree", `{"reasoning_tokens":4,"details":{"reasoning_tokens":5}}`, nil},
		{"not an integer", `{"reasoning_tokens":4.5}`, nil},
		{"a string", `{"reasoning_tokens":"4"}`, nil},
	} {
		got := reasoningTokens(json.RawMessage(tc.usage))
		if (got == nil) != (tc.want == nil) || got != nil && *got != *tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Every manifest records the build's capture limits; a release manifest
// carries its cell record and reads it back field for field.
func TestManifestCellRecord(t *testing.T) {
	in, out := manifestRun(t)
	m, err := NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	if want := (loadgen.Capture{FrameLimitBytes: 8 << 20, ErrorBodyLimitBytes: 4096, ErrorBodyWaitNs: int64(time.Second)}); m.Capture != want || m.Release != nil {
		t.Fatalf("pilot manifest: capture %+v, release %+v", m.Capture, m.Release)
	}

	cfg := releaseCfg(t)
	in.Config, in.Target, in.Release = cfg, &cfg.Target, releaseCell(cfg)
	m, err = NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	path, err := WriteManifest(out, m)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"frame_limit_bytes": 8388608`, `"error_body_limit_bytes": 4096`, `"error_body_wait_ns": 1000000000`,
		`"offset_minutes": 20`, `"concurrent_runs": [`, `"binding": {`, `"ceiling_choice": {`, `"account_created": "2026-09-15"`, `"pre_window_requests": 25`} {
		if !strings.Contains(string(b), field) {
			t.Errorf("manifest lacks %s", field)
		}
	}
	back, err := ReadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Release, in.Release) || back.Capture != m.Capture {
		t.Errorf("read back\n%+v\nwritten\n%+v", back.Release, in.Release)
	}
}

func TestManifestCellRefusals(t *testing.T) {
	in, _ := manifestRun(t)
	in.Release = releaseCell(releaseCfg(t))
	if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "cell record under") {
		t.Errorf("cell record on a pilot run: %v", err)
	}
	cfg := releaseCfg(t)
	in.Config, in.Target, in.Release = cfg, &cfg.Target, nil
	if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "needs its cell record") {
		t.Errorf("release run without a cell record: %v", err)
	}
	for name, edit := range map[string]func(c *Cell){
		"another route":              func(c *Cell) { c.Run.Route = "baseten" },
		"no label":                   func(c *Cell) { c.Run.Label = "" },
		"attempt 0":                  func(c *Cell) { c.Run.Attempt = 0 },
		"no start":                   func(c *Cell) { c.Run.Start = time.Time{} },
		"offset off the grid":        func(c *Cell) { c.OffsetMinutes = 15 },
		"negative offset":            func(c *Cell) { c.OffsetMinutes = -10 },
		"concurrent with itself":     func(c *Cell) { c.Concurrent[0].Route = cfg.Target.Name },
		"concurrent without a route": func(c *Cell) { c.Concurrent[0].Route = "" },
		"other ceiling":              func(c *Cell) { c.CeilingTokens = 1024 },
		"choice at the other":        func(c *Cell) { c.Ceiling.Ceiling = 1024 },
		"model excluded":             func(c *Cell) { c.Ceiling.Excluded = true },
		"no probe record":            func(c *Cell) { c.Probes = nil },
		"no verdict":                 func(c *Cell) { c.Admission = nil },
		"verdict of another route":   func(c *Cell) { c.Admission.Route = "baseten" },
		"verdict at another ceiling": func(c *Cell) { c.Admission.CeilingTokens = 1024 },
		"account date":               func(c *Cell) { c.AccountCreated = "15 Sep 2026" },
		"negative volume":            func(c *Cell) { c.PreWindowRequests = -1 },
	} {
		c := releaseCell(cfg)
		edit(c)
		in.Release = c
		if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "release cell") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A release run passes the check only with its cell record and an
// attempts file without answer text.
func TestCheckReleaseCellAndAnswer(t *testing.T) {
	r := newReleaseRun(t, config.ProfileRelease)
	_, err := r.check()
	if err == nil || !strings.Contains(err.Error(), "cell record: not in the manifest") || !strings.Contains(err.Error(), "attempt 0 carries answer text") {
		t.Fatalf("release run with answers and no cell record: %v", err)
	}

	recs, err := ReadRecords(bytes.NewReader(mustRead(t, filepath.Join(r.dir, AttemptsFile))))
	if err != nil {
		t.Fatal(err)
	}
	for i := range recs {
		recs[i].Answer, recs[i].Score = nil, nil
	}
	var b bytes.Buffer
	if err := Write(&b, recs); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir, AttemptsFile), b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	r.m.Artifacts[0] = refOf(AttemptsFile, b.Bytes())
	r.write(t)
	if _, err := r.check(); err == nil || strings.Contains(err.Error(), "answer text") || !strings.Contains(err.Error(), "cell record") {
		t.Fatalf("release run without answers: %v", err)
	}
	r.m.Release = releaseCell(releaseCfg(t))
	r.write(t)
	if _, err := r.check(); err != nil {
		t.Fatalf("complete release run: %v", err)
	}
}

// The provider notices, the AWS replication pack's send and the emails
// and forms that described the work load as operator records.
func TestOperatorPublicationRecords(t *testing.T) {
	body := strings.Replace(operatorJSON, `{"kind": "source_diff"`,
		`{"kind": "notice", "path": "operator-notes.txt", "retrieved": "2026-10-27"},
    {"kind": "replication_pack", "path": "operator-notes.txt", "retrieved": "2026-10-27"},
    {"kind": "disclosure", "path": "operator-notes.txt", "retrieved": "2026-09-15", "note": "email"},
    {"kind": "source_diff"`, 1)
	if body == operatorJSON {
		t.Fatal("fixture edit did not apply")
	}
	op, err := LoadOperator(writeOperator(t, body))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{RecordNotice, RecordReplicationPack, RecordDisclosure} {
		if op.find(kind) == nil {
			t.Errorf("no %s record", kind)
		}
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
