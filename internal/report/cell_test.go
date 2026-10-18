package report

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
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
		Config:  evidence.FileRef{Name: "groq.yaml", SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(cfg.Raw))},
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
	m.Gates = []validity.Gate{validity.G2(0, &loadgen.GateReport{Pass: true}), validity.ReleaseG6(1, 3, 3, 30), validity.ReleaseG6(2, 0, 1, 30)}
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
	for name, edit := range map[string][]validity.Gate{
		"a step left out":      gates[:2],
		"another count":        {gates[0], gates[1], validity.ReleaseG6(2, 1, 1, 30)},
		"another step":         {gates[0], gates[1], validity.ReleaseG6(3, 0, 1, 30)},
		"none":                 gates[:1],
		"a verdict reversed":   {gates[0], gates[1], func() validity.Gate { g := gates[2]; g.Pass = true; return g }()},
		"another deadline":     {gates[0], gates[1], validity.ReleaseG6(2, 0, 1, 90)},
		"a step not attempted": append(append([]validity.Gate{}, gates...), validity.ReleaseG6(3, 0, 1, 30)),
	} {
		m.Gates = edit
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
