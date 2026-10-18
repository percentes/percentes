package report

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
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

	recs[1].RunFailures = []string{"tripwire fired"}
	if _, _, err := cellReport(m, cfg, recs); err == nil || !strings.Contains(err.Error(), "record 1 run failures") {
		t.Errorf("records with another run's failures: %v", err)
	}
	m.Release = nil
	if _, _, err := cellReport(m, cfg, nil); err == nil || !strings.Contains(err.Error(), "no cell record") {
		t.Errorf("no cell record: %v", err)
	}
}
