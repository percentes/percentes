package report

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
	"github.com/percentes/percentes/internal/validity"
)

// cellManifest is a release cell's manifest under stepsConfig with the
// label outcome and reasons.
func cellManifest(t *testing.T, outcome string, reasons []string, ladder bool) (*evidence.Manifest, *config.Config) {
	t.Helper()
	cfg, v := stepsConfig(t)
	sum := sha256.Sum256(cfg.Raw)
	m := &evidence.Manifest{
		Profile: config.ProfileRelease,
		Source:  evidence.Source{Revision: "0123456789abcdef0123456789abcdef01234567"},
		Config:  evidence.FileRef{Name: "groq.yaml", SHA256: hex.EncodeToString(sum[:]), Bytes: json.Number(strconv.Itoa(len(cfg.Raw)))},
		Release: &evidence.Cell{
			Run:       schedule.Run{Route: "groq", Label: "weekend-08", Attempt: 2, Date: "2026-10-18", Start: time.Date(2026, 10, 18, 8, 20, 0, 0, time.UTC), Seed: 7},
			Admission: v, CeilingTokens: 512, Outcome: outcome, Reasons: reasons, LadderStarted: ladder,
		},
		RunValid: outcome == evidence.CellCounted,
	}
	if !m.RunValid {
		m.InvalidReasons = reasons
	}
	return m, cfg
}

// A labelled cell's report carries its label, its table with no step
// attempted when its ladder never started, and no frame line; a counted
// cell's report says it enters the frame. Records whose run failures
// are not the manifest's are refused.
func TestCellReport(t *testing.T) {
	stop := []string{"route stop: HTTP 402, the documented billing refusal: balance exhausted (errors.png, retrieved 2026-10-07)"}
	m, cfg := cellManifest(t, evidence.CellStopped, stop, false)
	raw, text, err := cellReport(m, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	var rep CellReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != evidence.CellStopped || rep.CountedInFrame || rep.LadderStarted || rep.Run != m.Release.Run || rep.ConfigSHA256 != m.Config.SHA256 ||
		rep.InstrumentCommit != "0123456789abcdef0123456789abcdef01234567" || len(rep.Reasons) != 1 || rep.Tables == nil || len(rep.Tables.Cells) != 1 {
		t.Fatalf("report %+v", rep)
	}
	ct := rep.Tables.Cells[0]
	if ct.Cell != "weekend-08-a2" || rep.Tables.Pooled != nil {
		t.Fatalf("table %+v, pooled %v", ct, rep.Tables.Pooled)
	}
	for k, want := range []string{StepNotAttempted, StepNotAttempted, StepNotAttempted, StepExcluded, StepExcluded} {
		if s := ct.Steps[k]; s.State != want || s.Scheduled != 0 {
			t.Errorf("step %d: %s with %d scheduled, want %s", k+1, s.State, s.Scheduled, want)
		}
	}
	for _, want := range []string{
		"== Release cell: route groq, cell weekend-08 attempt 2 ==\n",
		"Scheduled start 2026-10-18T08:20:00Z, seed 7; instrument 0123456789abcdef0123456789abcdef01234567\n",
		"Outcome: stopped; a labelled record, outside the route's frame of 12 cells\n  " + stop[0] + "\n",
		"The ladder did not start; the attempts file is empty.\n",
		"step 1, 0.25 rps: not attempted (the cell's ladder ended before this step)\n",
		"step 4, 2 rps: excluded by the ceiling rule\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "frame:") || strings.Contains(text, "Pooled") {
		t.Errorf("a cell report prints the frame's figures:\n%s", text)
	}

	m, cfg = cellManifest(t, evidence.CellCounted, nil, true)
	recs := cellOf("x", outcome{1, LineInTime, 0, 3}, outcome{2, loadgen.ErrStatus429, 0, 1}).Records
	pass := &loadgen.GateReport{Pass: true}
	g2 := []validity.Gate{validity.G2(0, pass), validity.G2(1, pass), validity.G2(2, pass)}
	m.Gates = append(slices.Clone(g2), validity.ReleaseG6(1, 3, 3, 30), validity.ReleaseG6(2, 0, 1, 30))
	raw, text, err = cellReport(m, cfg, recs)
	if err != nil {
		t.Fatal(err)
	}
	rep = CellReport{}
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	if !rep.CountedInFrame || rep.Tables.Cells[0].Steps[0].State != StepAttempted || rep.Tables.Cells[0].Steps[0].Scheduled != 3 {
		t.Fatalf("counted report %+v", rep.Tables.Cells[0])
	}
	if !strings.Contains(text, "Outcome: counted; the cell enters the route's frame of 12 cells\n") || strings.Contains(text, "did not start") {
		t.Errorf("counted text:\n%s", text)
	}

	// The manifest's hosted-G6 results are the records'.
	gates := m.Gates
	g6 := gates[len(g2):]
	for name, edit := range map[string][]validity.Gate{
		"a step left out":      g6[:1],
		"another count":        {g6[0], validity.ReleaseG6(2, 1, 1, 30)},
		"another step":         {g6[0], validity.ReleaseG6(3, 0, 1, 30)},
		"none":                 nil,
		"a verdict reversed":   {g6[0], func() validity.Gate { g := g6[1]; g.Pass = true; return g }()},
		"another deadline":     {g6[0], validity.ReleaseG6(2, 0, 1, 90)},
		"a step not attempted": append(slices.Clone(g6), validity.ReleaseG6(3, 0, 1, 30)),
	} {
		m.Gates = append(slices.Clone(g2), edit...)
		if _, _, err := cellReport(m, cfg, recs); err == nil || !strings.Contains(err.Error(), "hosted-G6 from the records differs from the manifest's") {
			t.Errorf("%s: %v", name, err)
		}
	}
	m.Gates = gates

	recs[1].RunFailures = []string{"tripwire fired"}
	if _, _, err := cellReport(m, cfg, recs); err == nil || !strings.Contains(err.Error(), "record 1 run failures") {
		t.Errorf("records with another run's failures: %v", err)
	}
	m.Release = nil
	if _, _, err := cellReport(m, cfg, nil); err == nil || !strings.Contains(err.Error(), "no cell record") {
		t.Errorf("no cell record: %v", err)
	}
}

// A cell report prints each step's G2 result from the manifest beside
// hosted-G6, and on a step the ladder ran with nothing dispatched; the
// results must cover the attempted steps and no excluded one, once each
// and with no gap.
func TestCellReportG2(t *testing.T) {
	reasons := []string{"G2 failed at step 2"}
	m, cfg := cellManifest(t, evidence.CellInvalid, reasons, true)
	recs := cellOf("x", outcome{1, LineInTime, 0, 3}, outcome{2, LineInTime, 0, 2}, outcome{3, LineUnsent, 0, 4}).Records
	for i := range recs {
		recs[i].RunFailures = reasons
	}
	pass := &loadgen.GateReport{Pass: true, CPUMeasured: true}
	fail := &loadgen.GateReport{SendSkewP99Us: 7000, SendSkewMaxUs: 9000, CPUMeasured: true, CPUWorstWindowPct: 81.5}
	g2 := []validity.Gate{validity.G2(1, pass), validity.G2(2, fail), validity.G2(3, nil)}
	g6 := []validity.Gate{validity.ReleaseG6(1, 3, 3, 30), validity.ReleaseG6(2, 2, 2, 30)}
	m.Gates = append(slices.Clone(g2), g6...)
	raw, text, err := cellReport(m, cfg, recs)
	if err != nil {
		t.Fatal(err)
	}
	var rep CellReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	steps := rep.Tables.Cells[0].Steps
	for k, want := range []*G2{
		{Observed: true, Pass: true, Detail: g2[0].Detail},
		{Observed: true, Pass: false, Detail: g2[1].Detail},
		{Observed: false, Pass: false, Detail: "no verdict: the run stopped before the step's drain ended"},
		nil, nil,
	} {
		if got := steps[k].G2; !reflect.DeepEqual(got, want) {
			t.Errorf("step %d: G2 %+v, want %+v", k+1, got, want)
		}
	}
	if steps[2].State != StepNotAttempted {
		t.Fatalf("step 3 is %s", steps[2].State)
	}
	const line = "  G2 (client validity at the step's drain end): "
	for _, want := range []string{
		line + "pass; " + g2[0].Detail + " (run-failing)\n  hosted-G6 (completion within 30 s over scheduled): 100.00%",
		line + "FAIL; " + g2[1].Detail + " (run-failing)\n  hosted-G6 (completion within 30 s over scheduled): 100.00%",
		"step 3, 1 rps: not attempted (the cell's ladder ended before this step)\n" +
			line + "not observed, so not passed; no verdict: the run stopped before the step's drain ended (run-failing)\n",
		"step 4, 2 rps: excluded by the ceiling rule\nstep 5,",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
	}

	for name, c := range map[string]struct {
		gates []validity.Gate
		want  string
	}{
		"an attempted step left out": {g2[:1], "step 2 is attempted and the manifest records no G2 result for it"},
		"an excluded step":           {append(slices.Clone(g2), validity.G2(4, pass)), "G2 at step 4, which the ceiling rule excluded"},
		"beyond the ladder":          {append(slices.Clone(g2), validity.G2(6, pass)), "G2 at step 6 of a 5-step ladder"},
		"a step twice":               {append(slices.Clone(g2), validity.G2(2, pass)), "G2 at step 2 given twice"},
		"the first step left out":    {[]validity.Gate{g2[1], g2[2]}, "step 1 is attempted and the manifest records no G2 result for it"},
	} {
		m.Gates = append(slices.Clone(c.gates), g6...)
		if _, _, err := cellReport(m, cfg, recs); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	m.Gates = []validity.Gate{g2[0], g2[2], g6[0]}
	if _, _, err := cellReport(m, cfg, recs[:4]); err == nil || !strings.Contains(err.Error(), "G2 at step 3 and none at step 2") {
		t.Errorf("a gap after an attempted step: %v", err)
	}
}

// A cell report prints each step's loopback canary summary from the
// manifest beside its G2 result, including a step the ladder ran with
// nothing dispatched; a manifest with no summaries prints none, and one
// whose summaries miss a step with a G2 result is refused.
func TestCellReportCanary(t *testing.T) {
	reasons := []string{"G2 failed at step 2"}
	m, cfg := cellManifest(t, evidence.CellInvalid, reasons, true)
	recs := cellOf("x", outcome{1, LineInTime, 0, 3}, outcome{2, LineInTime, 0, 2}, outcome{3, LineUnsent, 0, 4}).Records
	for i := range recs {
		recs[i].RunFailures = reasons
	}
	pass := &loadgen.GateReport{Pass: true, CPUMeasured: true}
	m.Gates = []validity.Gate{validity.G2(1, pass), validity.G2(2, pass), validity.G2(3, nil), validity.ReleaseG6(1, 3, 3, 30), validity.ReleaseG6(2, 2, 2, 30)}
	_, plain, err := cellReport(m, cfg, recs)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, "canary") {
		t.Errorf("a manifest with no canary summaries prints one:\n%s", plain)
	}

	at := loadgen.CanarySummary{TTFTMs: 20, ITLMs: 10, Tokens: 32, Streams: 4, Completed: 4,
		TTFTDevP50Us: 300, TTFTDevMaxUs: 900, ITLDevP50Us: 80, ITLDevP99Us: 400, ITLDevMaxUs: 700, EventLagP99Us: 1100, EventLagMaxUs: 1500}
	m.Release.Canary = []loadgen.CanarySummary{at, at, {TTFTMs: 20, ITLMs: 10, Tokens: 32, Streams: 1}}
	m.Release.Canary[1].EventLagMaxUs = 2250
	raw, text, err := cellReport(m, cfg, recs)
	if err != nil {
		t.Fatal(err)
	}
	var rep CellReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	steps := rep.Tables.Cells[0].Steps
	for k := range steps {
		var want *loadgen.CanarySummary
		if k < len(m.Release.Canary) {
			want = &m.Release.Canary[k]
		}
		if got := steps[k].Canary; !reflect.DeepEqual(got, want) {
			t.Errorf("step %d: canary %+v, want %+v", k+1, got, want)
		}
	}
	const line = "  canary (loopback, client read path): "
	for _, want := range []string{
		" (run-failing)\n" + line + "4 of 4 streams completed, 32 tokens at 20 ms TTFT and 10 ms ITL; event lag p99 1.10 ms, max 1.50 ms; " +
			"TTFT deviation p50 0.30 ms, max 0.90 ms; ITL deviation p50 0.08 ms, p99 0.40 ms, max 0.70 ms (reported, not run-invalidating)\n  hosted-G6",
		"; event lag p99 1.10 ms, max 2.25 ms; ",
		"step 3, 1 rps: not attempted (the cell's ladder ended before this step)\n" +
			"  G2 (client validity at the step's drain end): not observed, so not passed; no verdict: the run stopped before the step's drain ended (run-failing)\n" +
			line + "0 of 1 streams completed, so no bound on read-loop lag\nstep 4,",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
	}
	if n := strings.Count(text, line); n != 3 {
		t.Errorf("%d canary lines for 3 steps with a G2 result:\n%s", n, text)
	}

	m.Release.Canary = m.Release.Canary[:2]
	if _, _, err := cellReport(m, cfg, recs); err == nil || !strings.Contains(err.Error(), "2 canary summaries for 3 steps with a G2 result") {
		t.Errorf("a step's summary left out: %v", err)
	}
}

// A release cell manifest's hosted-G6 results are its step table's: the
// same steps with the same counts and verdicts.
func TestReleaseG6MatchesStepTable(t *testing.T) {
	cfg, v := stepsConfig(t)
	for name, c := range map[string]CellRecords{
		"every line": cellOf("x",
			outcome{1, LineInTime, 0, 100}, outcome{1, LineLate, 0, 3}, outcome{1, loadgen.ErrStatus429, 0, 2},
			outcome{1, loadgen.ErrConnect, 0, 1}, outcome{1, LineCensored, 0, 2},
			outcome{2, LineInTime, 0, 238}, outcome{2, LineUnsent, 0, 2},
			outcome{3, LineUnsent, 0, 470}, outcome{4, LineUnsent, 0, 950}),
		"ended at step 1":           cellOf("x", outcome{1, LineInTime, 0, 4}, outcome{2, LineUnsent, 0, 8}, outcome{3, LineUnsent, 0, 16}),
		"step 1 unsent":             cellOf("x", outcome{1, LineUnsent, 0, 4}, outcome{2, LineInTime, 0, 8}),
		"warm-up only":              cellOf("x"),
		"every step below the line": cellOf("x", outcome{1, LineLate, 0, 4}, outcome{2, loadgen.ErrReset, 0, 8}, outcome{3, LineInTime, 0, 16}),
	} {
		st, err := stepTables(cfg, v, []CellRecords{c})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var table []validity.Gate
		for _, s := range st.Cells[0].Steps {
			if g := s.G6; g != nil {
				tg := validity.ReleaseG6(s.Step, g.InTime.Count, g.InTime.Scheduled, st.DeadlineS)
				if tg.Pass != g.Pass {
					t.Errorf("%s: step %d: table verdict %t, gate %t", name, s.Step, g.Pass, tg.Pass)
				}
				table = append(table, tg)
			}
		}
		if got := evidence.ReleaseG6(c.Records, st.DeadlineS); !reflect.DeepEqual(got, table) {
			t.Errorf("%s: hosted-G6\n%+v\nthe table's\n%+v", name, got, table)
		}
	}
}

// An interrupted cell whose ladder started says its requests ended with
// the process; one interrupted before its ladder says the ladder did not
// start.
func TestCellReportInterrupted(t *testing.T) {
	reasons := []string{"the process ended during the cell"}
	const ended = "The ladder started and its requests ended with the process; the attempts file is empty.\n"
	for _, ladder := range []bool{true, false} {
		m, cfg := cellManifest(t, evidence.CellInterrupted, reasons, ladder)
		_, text, err := cellReport(m, cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(text, "Outcome: interrupted; a labelled record, outside the route's frame of 12 cells\n  "+reasons[0]+"\n") ||
			strings.Contains(text, ended) != ladder || strings.Contains(text, "The ladder did not start") == ladder {
			t.Errorf("ladder %t: text:\n%s", ladder, text)
		}
	}
}
