package evidence

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/schedule"
	"github.com/percentes/percentes/internal/validity"
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
	return withDispatch(&Cell{
		Run:            schedule.Run{Route: cfg.Target.Name, Label: "weekend-08", Attempt: 1, Date: "2026-10-17", Start: start, Seed: 7},
		OffsetMinutes:  20,
		ScheduleSHA256: "5c0e1d2b3a4f5e6d7c8b9a0f1e2d3c4b5a6f7e8d9c0b1a2f3e4d5c6b7a8f9e0d",
		Concurrent:     []schedule.Run{{Route: "baseten", Label: "weekend-08", Attempt: 1, Date: "2026-10-17", Start: start.Add(10 * time.Minute), Seed: 8}},
		Admission: &admission.Verdict{Route: cfg.Target.Name, InputTokens: 1200, CeilingTokens: 512, TopRPS: 1, Steps: []admission.Step{
			{RPS: 0.25, Admitted: true}, {RPS: 0.5, Admitted: true}, {RPS: 1, Admitted: true},
			{RPS: 2, Binding: &tpm, Demands: []admission.Demand{tpm}}, {RPS: 4, Binding: &tpm},
		}},
		CeilingTokens:     512,
		Ceiling:           probe.CeilingChoice{Ceiling: 512, Reason: "every host passed at 512"},
		Probes:            []FileRef{refOf("probe-groq-512.json", []byte("{}"))},
		AccountCreated:    "2026-09-15",
		PreWindowRequests: 25,
		Outcome:           CellCounted,
		LadderStarted:     true,
		Gaps:              []GapHistogram{},
	}, 3)
}

// releaseInput makes in a release run's input under cfg, valid, with no
// gate report and a one-step ladder whose gates passed.
func releaseInput(in *ManifestInput, cfg *config.Config) {
	res := *in.Result
	res.ReportedModels, res.ModelMismatches, res.SplitSeries = nil, 0, false
	res.Gates = loadgen.GateReport{Pass: true, CPUMeasured: true}
	res.Ladder = &loadgen.LadderRecord{Steps: []loadgen.StepRecord{{RateRPS: 0.25, Attempted: true, Gates: &loadgen.GateReport{Pass: true, CPUMeasured: true}}}}
	in.Config, in.Target, in.Result, in.Gates = cfg, &cfg.Target, &res, nil
	in.RunValid, in.InvalidReasons = true, nil
}

// withDispatch gives c an aggregate dispatch series in which its own run
// sends own requests in the first second and its concurrent run two in
// the third.
func withDispatch(c *Cell, own int) *Cell {
	end := c.Run.Start.Add(2500 * time.Millisecond)
	at := c.Run.Start.Add(2 * time.Second)
	d := &Dispatch{StartUTC: c.Run.Start, EndUTC: end, Counts: []int{own, 0, 2}, MaxPerSecond: 2, Sources: []DispatchSource{
		{Run: c.Run, Dispatches: own, Final: true},
		{Run: c.Concurrent[0], Dispatches: 2, ThroughUTC: &end},
	}}
	if own >= 2 {
		d.MaxPerSecond, at = own, c.Run.Start
	}
	d.MaxAtUTC = &at
	c.Dispatch = d
	return c
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
	releaseInput(&in, cfg)
	in.Release = releaseCell(cfg)
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
		`"offset_minutes": 20`, `"concurrent_runs": [`, `"binding": {`, `"ceiling_choice": {`, `"account_created": "2026-09-15"`, `"pre_window_requests": 25`,
		`"schedule_sha256": "5c0e1d2b3a4f5e6d7c8b9a0f1e2d3c4b5a6f7e8d9c0b1a2f3e4d5c6b7a8f9e0d"`,
		`"outcome": "counted"`, `"ladder_started": true`, `"dispatch_aggregate": {`, `"max_per_second": 3`, `"inter_chunk_gaps": []`,
		`"balance_start": {`, `"balance_end": {`} {
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
	if !reflect.DeepEqual(back.BalanceStart, m.BalanceStart) || !reflect.DeepEqual(back.BalanceEnd, m.BalanceEnd) {
		t.Errorf("balance read back %+v %+v, written %+v %+v", back.BalanceStart, back.BalanceEnd, m.BalanceStart, m.BalanceEnd)
	}

	// The end balance is absent where it was not recorded; the start
	// balance is required.
	in.BalanceEndPath = ""
	if m, err = NewManifest(in); err != nil {
		t.Fatal(err)
	}
	if m.BalanceEnd != nil || m.BalanceStart == nil {
		t.Errorf("no end balance: %+v %+v", m.BalanceStart, m.BalanceEnd)
	}
	in.BalanceStartPath = ""
	if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "needs its start balance record") {
		t.Errorf("release run without a start balance: %v", err)
	}
}

// A release target's not-applied parameters publish in its manifest's
// endpoint record.
func TestManifestReleaseNotApplied(t *testing.T) {
	raw, err := os.ReadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	with := strings.Replace(string(raw), `, "reasoning_format": "parsed"}`, "}", 1)
	with = strings.Replace(with, "  naming:\n", "  not_applied: [reasoning_format]\n  naming:\n", 1)
	cfg, err := config.Parse([]byte(with))
	if err != nil {
		t.Fatal(err)
	}
	in, out := manifestRun(t)
	releaseInput(&in, cfg)
	in.Release = releaseCell(cfg)
	m, err := NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	path, err := WriteManifest(out, m)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ReadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"reasoning_format"}; !reflect.DeepEqual(back.Endpoint.NotApplied, want) {
		t.Errorf("endpoint not_applied %q, want %q", back.Endpoint.NotApplied, want)
	}
}

func TestManifestCellRefusals(t *testing.T) {
	in, _ := manifestRun(t)
	in.Release = releaseCell(releaseCfg(t))
	if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "cell record under") {
		t.Errorf("cell record on a pilot run: %v", err)
	}
	cfg := releaseCfg(t)
	releaseInput(&in, cfg)
	in.Release = nil
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
		"no schedule hash":           func(c *Cell) { c.ScheduleSHA256 = "" },
		"schedule hash in uppercase": func(c *Cell) { c.ScheduleSHA256 = strings.ToUpper(c.ScheduleSHA256) },
		"concurrent with itself":     func(c *Cell) { c.Concurrent[0].Route = cfg.Target.Name },
		"concurrent without a route": func(c *Cell) { c.Concurrent[0].Route = "" },
		"other ceiling":              func(c *Cell) { c.CeilingTokens = 1024 },
		"choice at the other":        func(c *Cell) { c.Ceiling.Ceiling = 1024 },
		"model excluded":             func(c *Cell) { c.Ceiling.Excluded = true },
		"no probe record":            func(c *Cell) { c.Probes = nil },
		"no verdict":                 func(c *Cell) { c.Admission = nil },
		"verdict of another route":   func(c *Cell) { c.Admission.Route = "baseten" },
		"verdict at another ceiling": func(c *Cell) { c.Admission.CeilingTokens = 1024 },
		"verdict off the limits":     func(c *Cell) { c.Admission.Steps[3].Demands[0].Value = 1 },
		"account date":               func(c *Cell) { c.AccountCreated = "15 Sep 2026" },
		"negative volume":            func(c *Cell) { c.PreWindowRequests = -1 },
		"no dispatch aggregate":      func(c *Cell) { c.Dispatch = nil },
		"concurrent run unsummed":    func(c *Cell) { c.Dispatch.Sources = c.Dispatch.Sources[:1] },
		"another run summed":         func(c *Cell) { c.Dispatch.Sources[1].Run.Attempt = 2 },
		"own series not final":       func(c *Cell) { c.Dispatch.Sources[0].Final = false },
		"counts off the sources":     func(c *Cell) { c.Dispatch.Counts[1] = 1 },
		"counts off the span":        func(c *Cell) { c.Dispatch.Counts = append(c.Dispatch.Counts, 0) },
		"negative count":             func(c *Cell) { c.Dispatch.Counts[1], c.Dispatch.Counts[2] = -1, 3 },
		"maximum not the counts'":    func(c *Cell) { c.Dispatch.MaxPerSecond = 2 },
		"maximum at another second":  func(c *Cell) { c.Dispatch.MaxAtUTC = &c.Dispatch.EndUTC },
		"no gap histograms":          func(c *Cell) { c.Gaps = nil },
		"gaps off the bins":          func(c *Cell) { c.Gaps = []GapHistogram{{Step: 1, Gaps: 1, Counts: []int64{1}}} },
	} {
		c := releaseCell(cfg)
		edit(c)
		in.Release = c
		if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "release cell") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A release cell's label agrees with the manifest's run validity and
// invalid reasons: a counted cell started its ladder and is valid; any
// other outcome is invalid for the reasons it carries; a cell whose
// ladder never started, and an interrupted one, has no requests; a
// missed cell never started its ladder. Only a counted release cell
// enters its route's frame.
func TestManifestCellLabel(t *testing.T) {
	in, _ := manifestRun(t)
	pilot, err := NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	if pilot.Counted() {
		t.Error("a pilot manifest counted toward a release frame")
	}
	cfg := releaseCfg(t)
	releaseInput(&in, cfg)
	base := *in.Result
	stop := []string{"route stop: clock offset: 75 s from sntp exceeds 60 s"}
	ended := []string{"ladder ended at step 4: status_429 above half the step's scheduled requests"}
	blocked := []string{"probe request returned 401"}
	missed := []string{"not started: 20m0s after its start at 2026-10-17T08:20:00Z, beyond the 15m0s start tolerance"}
	gone := []string{"the process ended during the cell"}
	for _, tc := range []struct {
		name     string
		outcome  string
		reasons  []string
		ladder   bool
		valid    bool
		invalid  []string
		requests bool
		want     string
	}{
		{"counted", CellCounted, nil, true, true, nil, true, ""},
		{"counted with its ladder ended", CellCounted, ended, true, true, nil, true, ""},
		{"stopped inside the ladder", CellStopped, stop, true, false, stop, true, ""},
		{"stopped before the ladder", CellStopped, stop, false, false, stop, false, ""},
		{"blocked", CellBlocked, blocked, false, false, blocked, false, ""},
		{"client-invalid", CellInvalid, ended, true, false, ended, true, ""},
		{"no outcome", "", nil, true, true, nil, true, `outcome "" is not counted`},
		{"missed", CellMissed, missed, false, false, missed, false, ""},
		{"interrupted inside the ladder", CellInterrupted, gone, true, false, gone, false, ""},
		{"interrupted before the ladder", CellInterrupted, gone, false, false, gone, false, ""},
		{"unknown outcome", "skipped", stop, false, false, stop, false, `outcome "skipped" is not`},
		{"missed after its ladder started", CellMissed, missed, true, false, missed, false, "missed after its ladder started"},
		{"missed with no reason", CellMissed, nil, false, false, nil, false, "missed with no reason"},
		{"interrupted and valid", CellInterrupted, gone, true, true, nil, false, "and the manifest records valid=true"},
		{"interrupted with requests", CellInterrupted, gone, true, false, gone, true, "an interrupted release cell has 4 requests"},
		{"counted with no ladder", CellCounted, nil, false, true, nil, false, "counted with no ladder"},
		{"counted and invalid", CellCounted, nil, true, false, stop, true, "counted, and the manifest records the run invalid"},
		{"stopped with no reason", CellStopped, nil, true, false, nil, true, "stopped with no reason"},
		{"stopped and valid", CellStopped, stop, true, true, nil, true, "and the manifest records valid=true"},
		{"stopped for other reasons", CellStopped, stop, true, false, blocked, true, "and the manifest records valid=false"},
		{"blocked inside the ladder", CellBlocked, blocked, true, false, blocked, true, "blocked after its ladder started"},
		{"requests with no ladder", CellStopped, stop, false, false, stop, true, "ladder never started has 4 requests"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := releaseCell(cfg)
			c.Outcome, c.Reasons, c.LadderStarted = tc.outcome, tc.reasons, tc.ladder
			res := base
			if !tc.requests {
				res.Requests = nil
			}
			in.Result, in.Release, in.RunValid, in.InvalidReasons = &res, c, tc.valid, tc.invalid
			m, err := NewManifest(in)
			switch {
			case tc.want == "" && err != nil:
				t.Fatal(err)
			case tc.want == "":
				if m.Counted() != (tc.outcome == CellCounted) {
					t.Errorf("counted %t for a %s cell", m.Counted(), tc.outcome)
				}
			case err == nil || !strings.Contains(err.Error(), tc.want):
				t.Errorf("error %v, want %q", err, tc.want)
			}
		})
	}
}

// A cell with a request whose reported model differs from the expected
// one is labelled model_mismatch and never counts; the label needs such a
// request. The manifest names the expected model.
func TestManifestModelMismatchLabel(t *testing.T) {
	in, _ := manifestRun(t)
	cfg := releaseCfg(t)
	cfg.Target.ReportedModel = &config.ReportedModel{Name: "openai/gpt-oss-120b-2026", Source: "models.html", Retrieved: "2026-10-12", SHA256: strings.Repeat("0", 64)}
	releaseInput(&in, cfg)
	base := *in.Result
	reason := []string{`reported model "openai/gpt-oss-120b" differs from the expected "openai/gpt-oss-120b-2026" on 1 requests (SPEC-HOSTED §2)`}
	for _, tc := range []struct {
		name       string
		outcome    string
		reasons    []string
		valid      bool
		mismatches int
		want       string
	}{
		{"labelled", CellModelMismatch, reason, false, 1, ""},
		{"counted with a mismatch", CellCounted, nil, true, 1, "counted, and 1 requests reported a model other than the expected one"},
		{"labelled with no mismatch", CellModelMismatch, reason, false, 0, "model_mismatch, and no request reported a model other than the expected one"},
		{"labelled with no reason", CellModelMismatch, nil, false, 1, "model_mismatch with no reason"},
		{"labelled and valid", CellModelMismatch, reason, true, 1, "and the manifest records valid=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := releaseCell(cfg)
			c.Outcome, c.Reasons = tc.outcome, tc.reasons
			res := base
			res.ModelMismatches = tc.mismatches
			in.Result, in.Release, in.RunValid, in.InvalidReasons = &res, c, tc.valid, nil
			if !tc.valid {
				in.InvalidReasons = tc.reasons
			}
			m, err := NewManifest(in)
			switch {
			case tc.want == "" && err != nil:
				t.Fatal(err)
			case tc.want == "":
				if m.Counted() || m.Endpoint.ExpectedModel != "openai/gpt-oss-120b-2026" || m.Endpoint.RequestedModel != "openai/gpt-oss-120b" {
					t.Errorf("counted %t, expected %q, requested %q", m.Counted(), m.Endpoint.ExpectedModel, m.Endpoint.RequestedModel)
				}
			case err == nil || !strings.Contains(err.Error(), tc.want):
				t.Errorf("error %v, want %q", err, tc.want)
			}
		})
	}
	m := &Manifest{Profile: config.ProfileRelease, Release: releaseCell(cfg), RunValid: true}
	if !m.Counted() {
		t.Fatal("a counted cell with no mismatch does not count")
	}
	m.Endpoint.ModelMismatches = 1
	if m.Counted() {
		t.Error("a counted cell with a mismatch counts")
	}
}

// A release cell's manifest carries G2 over the whole ladder and at each
// attempted step, and hosted-G6 at each step its measured records reach:
// completion within the deadline over the step's scheduled requests,
// the warm-up and the steps after the last dispatch left out. A run a
// stop interrupted has no whole-ladder G2, and a step it left before the
// drain's end has an unobserved G2. A cell with no ladder has no gates.
func TestManifestReleaseGates(t *testing.T) {
	cfg := releaseCfg(t)
	pass, fail := &loadgen.GateReport{Pass: true, CPUMeasured: true}, &loadgen.GateReport{Pass: false, CPUMeasured: true, Undispatched: 1}
	done := func(step int, late bool) loadgen.Request {
		return loadgen.Request{Step: step, DispatchNs: 1, Outcome: loadgen.OutcomeCompleted, Late: late}
	}
	requests := []loadgen.Request{
		{Step: 1, Warmup: true, DispatchNs: 1, Outcome: loadgen.OutcomeCompleted},
		done(1, false), done(1, false), done(1, false), done(1, true),
		done(2, false), {Step: 2, DispatchNs: 1, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429}, {Step: 2},
		{Step: 3}, {Step: 3},
	}
	for i := range requests {
		requests[i].Index = int64(i)
	}
	ladder := func(steps ...*loadgen.GateReport) *loadgen.LadderRecord {
		l := &loadgen.LadderRecord{Steps: make([]loadgen.StepRecord, 3)}
		for k, g := range steps {
			l.Steps[k].Attempted, l.Steps[k].Gates = true, g
		}
		return l
	}
	g6 := func(step, in, n int) validity.Gate { return validity.ReleaseG6(step, in, n, 30) }
	stopped := []string{"route stop: HTTP 402, the documented billing refusal: balance exhausted"}
	for _, tc := range []struct {
		name        string
		res         loadgen.Result
		outcome     string
		reasons     []string
		want        []validity.Gate
		wantErr     string
		ladderStart bool
	}{
		{"counted", loadgen.Result{Gates: *pass, Ladder: ladder(pass, pass), Requests: requests}, CellCounted, nil,
			[]validity.Gate{validity.G2(0, pass), validity.G2(1, pass), validity.G2(2, pass), g6(1, 3, 4), g6(2, 1, 3)}, "", true},
		{"client-invalid at step 2", loadgen.Result{Gates: *fail, Ladder: ladder(pass, fail), Requests: requests}, CellInvalid, []string{"client-validity gate failed (§2)"},
			[]validity.Gate{validity.G2(0, fail), validity.G2(1, pass), validity.G2(2, fail), g6(1, 3, 4), g6(2, 1, 3)}, "", true},
		{"stopped inside step 2", loadgen.Result{Gates: *fail, Ladder: ladder(pass, nil), Interrupted: &loadgen.Interruption{Cause: "route stop"}, Requests: requests}, CellStopped, stopped,
			[]validity.Gate{validity.G2(1, pass), validity.G2(2, nil), g6(1, 3, 4), g6(2, 1, 3)}, "", true},
		{"stopped before the ladder", loadgen.Result{}, CellStopped, stopped, nil, "", false},
		{"counted with a failed step", loadgen.Result{Gates: *pass, Ladder: ladder(pass, fail), Requests: requests}, CellCounted, nil,
			nil, "counted, and G2 at step 2 failed: skew p99=0us/max=0us; undispatched=1", true},
		{"counted and interrupted", loadgen.Result{Gates: *pass, Ladder: ladder(pass, pass), Interrupted: &loadgen.Interruption{}, Requests: requests}, CellCounted, nil,
			nil, "counted with no whole-ladder G2 result", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, out := manifestRun(t)
			releaseInput(&in, cfg)
			res := tc.res
			res.EpochWall = in.Result.EpochWall
			c := releaseCell(cfg)
			c.Outcome, c.Reasons, c.LadderStarted = tc.outcome, tc.reasons, tc.ladderStart
			in.Result, in.Release, in.RunValid, in.InvalidReasons = &res, c, tc.outcome == CellCounted, tc.reasons
			m, err := NewManifest(in)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(m.Gates, tc.want) {
				t.Fatalf("gates\n%+v\nwant\n%+v", m.Gates, tc.want)
			}
			path, err := WriteManifest(out, m)
			if err != nil {
				t.Fatal(err)
			}
			back, err := ReadManifest(path)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(back.Gates, m.Gates) {
				t.Errorf("gates read back\n%+v\nwritten\n%+v", back.Gates, m.Gates)
			}
		})
	}
	g := g6(2, 1, 3)
	if g.Pass || !g.ReportedOnly || g.Detail != "completion within 30 s 0.3333: 1 of 3 scheduled (pinned line 0.99, SPEC-HOSTED §7; reported, not run-invalidating)" ||
		g.Name != "completion within the 30 s deadline >= 0.99 (hosted G6, SPEC-HOSTED HA3 point 13)" {
		t.Errorf("hosted-G6 %+v", g)
	}
	if g := validity.G2(2, nil); g.Observed || g.Pass || g.Detail != "no verdict: the run stopped before the step's drain ended" {
		t.Errorf("G2 with no verdict %+v", g)
	}

	in, _ := manifestRun(t)
	gates := in.Gates
	releaseInput(&in, cfg)
	in.Release, in.Gates = releaseCell(cfg), gates
	if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "gates come from its ladder") {
		t.Errorf("a release run given a gate report: %v", err)
	}
}

// A labelled release cell passes the check and does not count; one whose
// ladder never started, and an interrupted one, passes with empty
// attempts and frame files. The check refuses a label that disagrees with
// the manifest, and attempts in a cell whose ladder never started or that
// was interrupted.
func TestCheckReleaseLabelledCell(t *testing.T) {
	put := func(t *testing.T, dir, name string, b []byte) FileRef {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), b, 0o600); err != nil {
			t.Fatal(err)
		}
		return refOf(name, b)
	}
	stop := []string{"route stop: HTTP 402, the documented billing refusal: balance exhausted"}
	// labelled is a release run stopped with its label; before the ladder
	// it has empty attempts and frame files.
	labelled := func(t *testing.T, ladder bool) *releaseRun {
		r := newReleaseRun(t, config.ProfileRelease)
		recs, err := ReadRecords(bytes.NewReader(mustRead(t, filepath.Join(r.dir, AttemptsFile))))
		if err != nil {
			t.Fatal(err)
		}
		for i := range recs {
			recs[i].Answer, recs[i].Score, recs[i].RunFailures = nil, nil, stop
		}
		frames := mustRead(t, filepath.Join(r.runDir, FramesFile))
		if !ladder {
			recs, frames = nil, nil
		}
		var b bytes.Buffer
		if err := Write(&b, recs); err != nil {
			t.Fatal(err)
		}
		r.m.Artifacts[0] = put(t, r.dir, AttemptsFile, b.Bytes())
		ref := put(t, r.runDir, FramesFile, frames)
		ref.Name = releaseRunDir + "/" + FramesFile
		r.m.Frames = &ref
		c := releaseCell(releaseCfg(t))
		if !ladder {
			withDispatch(c, 0)
		}
		c.Outcome, c.Reasons, c.LadderStarted = CellStopped, stop, ladder
		r.m.Release, r.m.RunValid, r.m.InvalidReasons = c, false, stop
		r.write(t)
		return r
	}
	for _, ladder := range []bool{true, false} {
		r := labelled(t, ladder)
		m, err := r.check()
		if err != nil {
			t.Fatalf("stopped cell, ladder %t: %v", ladder, err)
		}
		if m.Counted() || m.Release.Outcome != CellStopped {
			t.Errorf("stopped cell, ladder %t: counted %t, outcome %q", ladder, m.Counted(), m.Release.Outcome)
		}
	}

	r := labelled(t, false)
	r.m.RunValid, r.m.InvalidReasons = true, nil
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "cell record: evidence: cell label: stopped") {
		t.Errorf("stopped cell recorded valid: %v", err)
	}

	r = labelled(t, true)
	r.m.Release.LadderStarted = false
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), AttemptsFile+": 3 attempts in a cell whose ladder never started") {
		t.Errorf("attempts with no ladder: %v", err)
	}

	// A missed cell passes with empty attempts and frame files.
	r = labelled(t, false)
	r.m.Release.Outcome = CellMissed
	r.write(t)
	if m, err := r.check(); err != nil || m.Counted() {
		t.Errorf("missed cell: %v", err)
	}

	// An interrupted cell whose ladder started has empty attempts and
	// frame files beside its own running series; a stopped one may not
	// end on a running series, and an interrupted one with attempts is
	// refused.
	r = labelled(t, false)
	c := withDispatch(r.m.Release, 3)
	c.Dispatch.Sources[0].Final = false
	c.Outcome, c.LadderStarted = CellInterrupted, true
	r.write(t)
	if m, err := r.check(); err != nil || m.Counted() || !m.Release.LadderStarted {
		t.Errorf("interrupted cell: %v", err)
	}
	c.Outcome = CellStopped
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "the cell's own series is not final") ||
		!strings.Contains(err.Error(), "own series counts 3 dispatches, "+AttemptsFile+" 0") {
		t.Errorf("stopped cell on a running series: %v", err)
	}
	r = labelled(t, true)
	r.m.Release.Outcome = CellInterrupted
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), AttemptsFile+": 3 attempts in an interrupted cell") {
		t.Errorf("attempts in an interrupted cell: %v", err)
	}
}

// A release run passes the check only with its cell record, whose
// aggregate dispatch series counts the attempts file's dispatches as its
// own and whose gap histograms are the attempts file's, and an attempts
// file without answer text.
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
	r.m.Gates = []validity.Gate{validity.G2(0, &loadgen.GateReport{Pass: true})}
	r.write(t)
	if _, err := r.check(); err != nil {
		t.Fatalf("complete release run: %v", err)
	}

	// A counted cell's manifest has a whole-ladder G2, and no G2 failed.
	gates := r.m.Gates
	r.m.Gates = append(gates, validity.G2(1, &loadgen.GateReport{CPUMeasured: true}))
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "cell record: evidence: cell label: counted, and G2 at step 1 failed") {
		t.Errorf("counted cell with a failed G2: %v", err)
	}
	r.m.Gates = nil
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "cell label: counted with no whole-ladder G2 result") {
		t.Errorf("counted cell with no gates: %v", err)
	}
	r.m.Gates = gates

	// A release run needs its start balance record.
	start := r.m.BalanceStart
	r.m.BalanceStart = nil
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "start balance: not in the manifest") {
		t.Errorf("release run without a start balance: %v", err)
	}
	r.m.BalanceStart = start

	// The gap histograms are the attempts file's: its completions carry no
	// content event, so no step has a gap.
	counts := make([]int64, len(GapEdgesUs))
	counts[4] = 1
	r.m.Release.Gaps = []GapHistogram{{Step: 1, Gaps: 1, Counts: counts}}
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "cell record: inter_chunk_gaps: map[1:1] gaps per step, "+AttemptsFile+" gives map[]") {
		t.Errorf("gaps the attempts do not give: %v", err)
	}
	r.m.Release.Gaps = nil
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "cell record: inter_chunk_gaps: none") {
		t.Errorf("no gap histograms: %v", err)
	}
	r.m.Release.Gaps = []GapHistogram{}

	// The aggregate's own part counts the attempts file's dispatches, and
	// the aggregate agrees with itself.
	withDispatch(r.m.Release, 2)
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "own series counts 2 dispatches, "+AttemptsFile+" 3") {
		t.Errorf("own series short of the attempts: %v", err)
	}
	withDispatch(r.m.Release, 3).Dispatch.Counts[1] = 1
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "cell record: dispatch_aggregate: counts sum to 6, the sources to 5") {
		t.Errorf("counts off the sources: %v", err)
	}
}

// The manifest's model fields agree with the attempts file's records and
// the configuration's expected model: each record whose first reported
// model differs from the expected one carries the mismatch.
func TestCheckReleaseModels(t *testing.T) {
	r := newReleaseRun(t, config.ProfileRelease)
	cfg := releaseCfg(t)
	r.m.Release = releaseCell(cfg)
	r.m.Gates = []validity.Gate{validity.G2(0, &loadgen.GateReport{Pass: true})}
	writeRecs := func(edit func([]Record)) {
		t.Helper()
		recs, err := ReadRecords(bytes.NewReader(mustRead(t, filepath.Join(r.dir, AttemptsFile))))
		if err != nil {
			t.Fatal(err)
		}
		for i := range recs {
			recs[i].Answer, recs[i].Score, recs[i].ReportedModel, recs[i].ModelMismatch = nil, nil, cfg.Target.ModelName, nil
		}
		edit(recs)
		var b bytes.Buffer
		if err := Write(&b, recs); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(r.dir, AttemptsFile), b.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
		r.m.Artifacts[0] = refOf(AttemptsFile, b.Bytes())
		r.write(t)
	}
	r.m.Endpoint.ReportedModels = []string{cfg.Target.ModelName}
	writeRecs(func([]Record) {})
	if _, err := r.check(); err != nil {
		t.Fatalf("matching models: %v", err)
	}

	// A labelled cell whose records carry the mismatch passes and never
	// counts; a counted one is refused.
	other := "openai/gpt-oss-120b-v2"
	reason := []string{"reported model differs"}
	writeRecs(func(recs []Record) { recs[2].ReportedModel, recs[2].ModelMismatch = other, []string{other} })
	r.m.Endpoint.ReportedModels, r.m.Endpoint.ModelMismatches = []string{cfg.Target.ModelName, other}, 1
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "cell label: counted, and 1 requests reported a model other than the expected one") {
		t.Errorf("counted cell with a mismatch: %v", err)
	}
	r.m.Release.Outcome, r.m.Release.Reasons, r.m.RunValid, r.m.InvalidReasons = CellModelMismatch, reason, false, reason
	r.write(t)
	if m, err := r.check(); err != nil || m.Counted() {
		t.Fatalf("model_mismatch cell: %v", err)
	}

	// The manifest's count and list must be the records'.
	r.m.Endpoint.ModelMismatches = 2
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "models: "+AttemptsFile+" gives 1 mismatches") {
		t.Errorf("count off the records: %v", err)
	}
	r.m.Endpoint.ModelMismatches = 1

	// A record whose first reported model is not the expected one carries
	// it as a mismatch, and no mismatch is the expected model.
	writeRecs(func(recs []Record) { recs[2].ReportedModel = other })
	r.m.Endpoint.ModelMismatches = 0
	r.m.Release.Outcome, r.m.Release.Reasons, r.m.RunValid, r.m.InvalidReasons = CellCounted, nil, true, nil
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), `attempt 2 reported model "openai/gpt-oss-120b-v2", the expected "openai/gpt-oss-120b", with no mismatch recorded`) {
		t.Errorf("unrecorded mismatch: %v", err)
	}
	writeRecs(func(recs []Record) { recs[1].ModelMismatch = []string{cfg.Target.ModelName} })
	r.m.Endpoint.ReportedModels, r.m.Endpoint.ModelMismatches = []string{cfg.Target.ModelName}, 1
	r.m.Release.Outcome, r.m.Release.Reasons, r.m.RunValid, r.m.InvalidReasons = CellModelMismatch, reason, false, reason
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), `attempt 1 records the expected model "openai/gpt-oss-120b" as a mismatch`) {
		t.Errorf("expected model as a mismatch: %v", err)
	}

	// The expected model is the configuration's.
	writeRecs(func([]Record) {})
	r.m.Endpoint.ModelMismatches = 0
	r.m.Release.Outcome, r.m.Release.Reasons, r.m.RunValid, r.m.InvalidReasons = CellCounted, nil, true, nil
	r.m.Endpoint.ExpectedModel = "openai/gpt-oss-120b-2026"
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), `expected model "openai/gpt-oss-120b-2026", the configuration's "openai/gpt-oss-120b"`) {
		t.Errorf("expected model off the configuration: %v", err)
	}
}

// The provider notices, the AWS replication pack's send and the emails
// and forms that described the work are the publication record's, never
// a cell's operator records.
func TestOperatorPublicationRecords(t *testing.T) {
	for _, c := range []struct{ kind, record string }{
		{"notice", `{"kind": "notice", "path": "operator-notes.txt", "retrieved": "2026-10-27"}`},
		{"replication_pack", `{"kind": "replication_pack", "path": "operator-notes.txt", "retrieved": "2026-10-27"}`},
		{"disclosure", `{"kind": "disclosure", "path": "operator-notes.txt", "retrieved": "2026-09-15", "note": "email"}`},
	} {
		body := strings.Replace(operatorJSON, `{"kind": "source_diff"`, c.record+`,
    {"kind": "source_diff"`, 1)
		if body == operatorJSON {
			t.Fatal("fixture edit did not apply")
		}
		if _, err := LoadOperator(writeOperator(t, body)); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("kind: unknown kind %q", c.kind)) {
			t.Errorf("%s: %v", c.kind, err)
		}
	}
}

// CheckConfidential does not search the account tier, which the
// provider fixes, and still refuses a note that carries the value.
func TestCheckConfidentialAccountTier(t *testing.T) {
	target := &config.Target{Limits: []config.Limit{{Dimension: config.LimitConcurrency, Value: 4, Marking: config.MarkingConfidential}}}
	tier := strings.Replace(operatorJSON, `"account_tier": "Basic"`, `"account_tier": "Tier 4"`, 1)
	if tier == operatorJSON {
		t.Fatal("fixture edit did not apply")
	}
	op, err := LoadOperator(writeOperator(t, tier))
	if err != nil {
		t.Fatal(err)
	}
	if err := op.CheckConfidential(target, nil); err != nil {
		t.Errorf("account tier: %v", err)
	}
	note := strings.Replace(tier, `"note": "T&C of 15 Sep 2026"`, `"note": "4 requests at once"`, 1)
	if note == tier {
		t.Fatal("fixture edit did not apply")
	}
	if op, err = LoadOperator(writeOperator(t, note)); err != nil {
		t.Fatal(err)
	}
	want := "a confidential limit's value is in records[3].note (concurrent_requests)"
	if err := op.CheckConfidential(target, nil); err == nil || err.Error() != "evidence: operator file: "+want {
		t.Errorf("note: %v; want %q", err, want)
	}
}

// CheckConfidential does not count a value within the text of a model
// identifier the provider fixes, with or without its provider prefix, on
// the route or on another host of the release, and refuses one that runs
// on past that text or stands outside it.
func TestCheckConfidentialFixedIdentifiers(t *testing.T) {
	const url, note = `"url": "https://docs.example/limits"`, `"note": "T&C of 15 Sep 2026"`
	rel := &config.Release{OtherHosts: []config.ModelHost{{Route: "baseten", Model: "deepseek-ai/DeepSeek-V3.1"}}}
	for _, tc := range []struct {
		name, model string
		value       float64
		old, new    string
		// field is the field refused, or "" where the file passes.
		field string
	}{
		{"url with the model", "openai/gpt-oss-120b", 120, url, `"url": "https://console.groq.com/docs/model/openai/gpt-oss-120b"`, ""},
		{"note without the prefix", "openai/gpt-oss-120b", 120, note, `"note": "gpt-oss-120b"`, ""},
		{"note with the value outside", "openai/gpt-oss-120b", 120, note, `"note": "120 requests on gpt-oss-120b"`, "records[3].note"},
		{"another host's model", "openai/gpt-oss-120b", 3.1, note, `"note": "also DeepSeek-V3.1"`, ""},
		{"the model itself", "qwen3-32", 32, note, `"note": "qwen3-32"`, ""},
		{"a decimal point after", "qwen3-32", 32.5, note, `"note": "qwen3-32.5"`, "records[3].note"},
		{"a decimal comma after", "qwen3-32", 32000, note, `"note": "qwen3-32,000"`, "records[3].note"},
		{"a space after", "qwen3-32", 32000, note, `"note": "qwen3-32 000"`, "records[3].note"},
	} {
		body := strings.Replace(operatorJSON, tc.old, tc.new, 1)
		if body == operatorJSON {
			t.Fatalf("%s: fixture edit did not apply", tc.name)
		}
		op, err := LoadOperator(writeOperator(t, body))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		target := &config.Target{ModelName: tc.model, Limits: []config.Limit{{Dimension: config.LimitConcurrency, Value: tc.value, Marking: config.MarkingConfidential}}}
		want := "<nil>"
		if tc.field != "" {
			want = "evidence: operator file: a confidential limit's value is in " + tc.field + " (concurrent_requests)"
		}
		if err := op.CheckConfidential(target, rel); fmt.Sprint(err) != want {
			t.Errorf("%s: %v; want %s", tc.name, err, want)
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

// confidentialCfg is the reference configuration with its
// tokens_per_minute limit at 300,001 marked confidential, and its
// rate-limit headers retained.
func confidentialCfg(t *testing.T) *config.Config {
	t.Helper()
	raw, err := os.ReadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	const public = "      value: 250000\n      marking: public\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      sha256:"
	const headers = "  response_headers: [Retry-After, x-ratelimit-*]\n"
	if !bytes.Contains(raw, []byte(public)) || !bytes.Contains(raw, []byte(headers)) {
		t.Fatal("reference no longer carries the lines this test edits")
	}
	raw = bytes.Replace(raw, []byte(headers), []byte(headers+"  retained_headers: [x-ratelimit-*]\n"), 1)
	cfg, err := config.Parse(bytes.Replace(raw, []byte(public), []byte("      value: 300001\n      marking: confidential\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      hmac_sha256:"), 1))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// confidentialCell is releaseCell's record with its tokens_per_minute
// demands as published against cfg's confidential limit.
func confidentialCell(cfg *config.Config) *Cell {
	c := releaseCell(cfg)
	l := cfg.Target.Limits[1]
	hide := func(d *admission.Demand) {
		d.Value, d.Fraction, d.Confidential, d.Source, d.HMACSHA256 = 0, 0, true, l.Source, l.HMACSHA256
	}
	for k := range c.Admission.Steps {
		s := &c.Admission.Steps[k]
		for i := range s.Demands {
			hide(&s.Demands[i])
		}
		if s.Binding != nil {
			b := *s.Binding
			hide(&b)
			s.Binding = &b
		}
	}
	return c
}

// A configuration with a confidential limit is named in the manifest by
// the file as published; the run's frame directory keeps the file as
// loaded; the manifest names the frame store, the operator file and its
// records by HMAC-SHA256 under the release key alone; the completeness
// check reads the file as loaded and refuses a manifest naming another
// file, a missing or different copy, a verdict that publishes the value,
// and a frame store, operator file or operator record named by its
// SHA-256 or not matching under the key it is given.
func TestConfidentialConfig(t *testing.T) {
	cfg := confidentialCfg(t)
	published, err := cfg.Published()
	if err != nil {
		t.Fatal(err)
	}
	key, err := releasekey.New()
	if err != nil {
		t.Fatal(err)
	}
	in, out := manifestRun(t)
	releaseInput(&in, cfg)
	in.ConfigName = "groq.yaml"
	in.Release = confidentialCell(cfg)
	if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "needs the release key") {
		t.Errorf("no key: %v", err)
	}
	in.Key = key
	m, err := NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	if m.Config != refOf("groq.yaml", published) || m.Config == refOf("groq.yaml", cfg.Raw) {
		t.Fatalf("manifest config %+v, want the file as published", m.Config)
	}
	frames := mustRead(t, in.FramesPath)
	if want := (FileRef{Name: "20261010T200000.000000000Z/" + FramesFile, HMACSHA256: key.Sum(frames)}); *m.Frames != want {
		t.Errorf("frame store %+v, want %+v", *m.Frames, want)
	}
	if len(m.Records) != len(in.Operator.Records) {
		t.Fatalf("records %+v", m.Records)
	}
	for i, r := range m.Records {
		if want := (FileRef{Name: in.Operator.Records[i].Name, HMACSHA256: key.Sum(in.Operator.Records[i].data)}); r.FileRef != want {
			t.Errorf("record %s: %+v, want %+v", r.Kind, r.FileRef, want)
		}
	}
	if want := (FileRef{Name: in.Operator.File.Name, HMACSHA256: key.Sum(in.Operator.raw)}); m.Source.Diff == nil || !m.Source.Diff.Keyed() || m.Operator != want {
		t.Errorf("source diff %+v, operator file %+v, want %+v", m.Source.Diff, m.Operator, want)
	}
	path, err := WriteManifest(out, m)
	if err != nil {
		t.Fatal(err)
	}
	if b := mustRead(t, path); bytes.Contains(b, []byte("300001")) || !bytes.Contains(b, []byte(`"confidential": true`)) {
		t.Errorf("manifest carries the value or no confidential demand:\n%s", b)
	}
	b := mustRead(t, path)
	for _, r := range append([]OperatorRecord{{FileRef: refOf(m.Frames.Name, frames)}, {FileRef: in.Operator.File}}, in.Operator.Records...) {
		if bytes.Contains(b, []byte(r.SHA256)) {
			t.Errorf("manifest carries %s's SHA-256:\n%s", r.Name, b)
		}
	}
	if bytes.Contains(b, []byte(`"bytes": `+fmt.Sprint(len(frames)))) {
		t.Errorf("manifest carries the frame store's size:\n%s", b)
	}
	in.Release.Admission.Steps[3].Demands[0].Value = 300001
	if _, err := NewManifest(in); err == nil || !strings.Contains(err.Error(), "a confidential limit published with its value") {
		t.Errorf("verdict with the value: %v", err)
	}

	dir := t.TempDir()
	if err := WriteConfigCopy(dir, releaseCfg(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ConfigCopyFile)); err == nil {
		t.Error("a configuration with public limits only wrote a copy")
	}
	if err := WriteConfigCopy(dir, cfg); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(dir, ConfigCopyFile)); err != nil || st.Mode().Perm() != 0o600 || !bytes.Equal(mustRead(t, filepath.Join(dir, ConfigCopyFile)), cfg.Raw) {
		t.Errorf("config copy: %v, %v", st, err)
	}
	if err := WriteConfigCopy(dir, cfg); err == nil {
		t.Error("an existing config copy was written again")
	}

	// run is a complete release run of cfg, its copy kept unless noCopy.
	run := func(t *testing.T, noCopy bool) *releaseRun {
		r := newReleaseRun(t, config.ProfileRelease)
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
		r.cfg, r.m.Config = cfg.Raw, refOf("groq.yaml", published)
		r.m.Release = confidentialCell(cfg)
		r.m.Gates = []validity.Gate{validity.G2(0, &loadgen.GateReport{Pass: true})}
		r.key = key
		r.m.Frames = keyedAt(t, r.root, *r.m.Frames, key)
		for i := range r.m.Records {
			r.m.Records[i].FileRef = *keyedAt(t, filepath.Join(r.runDir, RecordsCopyDir), r.m.Records[i].FileRef, key)
		}
		r.m.Operator = keyedRefOf(r.m.Operator.Name, mustRead(t, filepath.Join(r.runDir, OperatorCopyFile)), key)
		diff := r.m.Records[1].FileRef
		r.m.Source.Diff = &diff
		r.write(t)
		if !noCopy {
			if err := WriteConfigCopy(r.runDir, cfg); err != nil {
				t.Fatal(err)
			}
		}
		return r
	}
	if _, err := run(t, false).check(); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(t *testing.T, r *releaseRun)
		want string
	}{
		{"the file as loaded named", func(t *testing.T, r *releaseRun) { r.m.Config = refOf("groq.yaml", cfg.Raw); r.write(t) }, "config: not groq.yaml"},
		{"the file as published given", func(t *testing.T, r *releaseRun) { r.cfg = published }, "config: parse:"},
		{"no copy", func(t *testing.T, r *releaseRun) { os.Remove(filepath.Join(r.runDir, ConfigCopyFile)) }, "config copy: " + ConfigCopyFile + " is missing"},
		{"another copy", func(t *testing.T, r *releaseRun) {
			if err := os.WriteFile(filepath.Join(r.runDir, ConfigCopyFile), published, 0o600); err != nil {
				t.Fatal(err)
			}
		}, "config copy: " + ConfigCopyFile + " is not the configuration given"},
		{"value published", func(t *testing.T, r *releaseRun) {
			r.m.Release.Admission.Steps[3].Demands[0].Value = 300001
			r.write(t)
		},
			"cell record: admission: published verdict: step 4, tokens_per_minute: a confidential limit published with its value or fraction"},
		{"pass off the value", func(t *testing.T, r *releaseRun) { r.m.Release.Admission.Steps[3].Demands[0].Pass = true; r.write(t) },
			"step 4, tokens_per_minute: pass false against the recorded value, published true"},
		{"frame store by SHA-256", func(t *testing.T, r *releaseRun) {
			ref := refOf(r.m.Frames.Name, mustRead(t, filepath.Join(r.root, filepath.FromSlash(r.m.Frames.Name))))
			r.m.Frames = &ref
			r.write(t)
		}, "frame store: " + releaseRunDir + "/" + FramesFile + " is named by its SHA-256 on a target with a confidential limit"},
		{"operator file by SHA-256", func(t *testing.T, r *releaseRun) {
			r.m.Operator = refOf(r.m.Operator.Name, mustRead(t, filepath.Join(r.runDir, OperatorCopyFile)))
			r.write(t)
		}, "operator copy: " + OperatorCopyFile + " is named by its SHA-256 on a target with a confidential limit"},
		{"operator copy changed", func(t *testing.T, r *releaseRun) {
			if err := os.WriteFile(filepath.Join(r.runDir, OperatorCopyFile), []byte(`{"provider":"q"}`), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "operator copy: " + OperatorCopyFile + " has another HMAC-SHA256 under the release key than the manifest records"},
		{"limits record by SHA-256", func(t *testing.T, r *releaseRun) {
			r.m.Records[0].FileRef = refOf("limits/groq.txt", []byte("limits"))
			r.write(t)
		}, "limits record copy: limits/groq.txt is named by its SHA-256 on a target with a confidential limit"},
		{"keyed with a size", func(t *testing.T, r *releaseRun) { r.m.Records[0].Bytes = "6"; r.write(t) },
			"limits record copy: limits/groq.txt: a keyed reference with a SHA-256 or a size"},
		{"no key", func(t *testing.T, r *releaseRun) { r.key = nil },
			"release key: the target has a confidential limit, whose frame store, operator file and records are keyed, and no release key was given"},
		{"another key", func(t *testing.T, r *releaseRun) {
			var err error
			if r.key, err = releasekey.New(); err != nil {
				t.Fatal(err)
			}
		}, "frame store: " + releaseRunDir + "/" + FramesFile + " has another HMAC-SHA256 under the release key than the manifest records"},
		{"limits copy changed", func(t *testing.T, r *releaseRun) {
			if err := os.WriteFile(filepath.Join(r.runDir, RecordsCopyDir, "limits", "groq.txt"), []byte("limitz"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "limits record copy: limits/groq.txt has another HMAC-SHA256 under the release key than the manifest records"},
	} {
		r := run(t, false)
		tc.edit(t, r)
		if _, err := r.check(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got %v", tc.name, tc.want, err)
		}
	}

	// A target with public limits only keeps every plain reference.
	r := newReleaseRun(t, config.ProfilePilot)
	r.key = key
	if _, err := r.check(); err != nil {
		t.Fatalf("public run given the key: %v", err)
	}
	r.m.Frames = keyedAt(t, r.root, *r.m.Frames, key)
	r.write(t)
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "frame store: "+releaseRunDir+"/"+FramesFile+" is keyed on a target with public limits only") {
		t.Errorf("keyed frame store on a public target: %v", err)
	}
	r = newReleaseRun(t, config.ProfilePilot)
	r.m.Operator = keyedRefOf(r.m.Operator.Name, mustRead(t, filepath.Join(r.runDir, OperatorCopyFile)), key)
	r.write(t)
	r.key = key
	if _, err := r.check(); err == nil || !strings.Contains(err.Error(), "operator copy: "+OperatorCopyFile+" is keyed on a target with public limits only") {
		t.Errorf("keyed operator file on a public target: %v", err)
	}
}

// keyedAt is ref keyed under key from its file under base.
func keyedAt(t *testing.T, base string, ref FileRef, key *releasekey.Key) *FileRef {
	t.Helper()
	k := keyedRefOf(ref.Name, mustRead(t, filepath.Join(base, filepath.FromSlash(ref.Name))), key)
	return &k
}
