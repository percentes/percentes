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
	return withDispatch(&Cell{
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
		Outcome:           CellCounted,
		LadderStarted:     true,
		Gaps:              []GapHistogram{},
	}, 3)
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
	in.Config, in.Target, in.Release = cfg, &cfg.Target, releaseCell(cfg)
	in.RunValid, in.InvalidReasons = true, nil
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
	in.Config, in.Target, in.Release = cfg, &cfg.Target, releaseCell(cfg)
	in.RunValid, in.InvalidReasons = true, nil
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
	in.Config, in.Target, in.Release = cfg, &cfg.Target, nil
	in.RunValid, in.InvalidReasons = true, nil
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
	in.Config, in.Target = cfg, &cfg.Target
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
	r.write(t)
	if _, err := r.check(); err != nil {
		t.Fatalf("complete release run: %v", err)
	}

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
