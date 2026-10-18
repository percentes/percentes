package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/schedule"
)

// clockRoute is a route over testSchedule with a fake clock that each
// wait moves to its target, a fake cell that records the outcome
// outcomes gives a run and appends the run to ran, and a fake label that
// writes an empty manifest and appends the run to r.labelled.
func clockRoute(t *testing.T, start time.Time, outcomes func(schedule.Run) string, ran *[]schedule.Run) *route {
	t.Helper()
	sched := testSchedule(t, "groq", "other")
	runs, err := sched.Runs("groq")
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	cfg := &config.Config{Target: config.Target{Name: "groq"}}
	r := &route{cfg: cfg, sched: sched, runs: runs, out: out, dir: filepath.Join(out, "groq"), verdict: &admission.Verdict{}}
	now := start
	r.now = func() time.Time { return now }
	r.wait = func(_ context.Context, at time.Time) error {
		if at.After(now) {
			now = at
		}
		return nil
	}
	r.cell = func(_ context.Context, run schedule.Run) (*cellRecord, error) {
		if !run.OnTime(now) {
			t.Fatalf("%s attempt %d started at %s, scheduled %s", run.Label, run.Attempt, now, run.Start)
		}
		*ran = append(*ran, run)
		rec := r.newRecord(run)
		rec.Outcome = outcomes(run)
		now = now.Add(schedule.CellLength)
		return rec, r.writeNew(run, rec)
	}
	r.label = func(_ context.Context, rec *cellRecord) error {
		labelled = append(labelled, rec.Run)
		return os.WriteFile(filepath.Join(r.cellDir(rec.Run), evidence.ManifestFile), []byte("{}"), 0o600)
	}
	return r
}

// labelled are the runs clockRoute's fake label was given, in order.
var labelled []schedule.Run

func counted(schedule.Run) string { return outcomeCounted }

// Every attempt 1 runs once, in start order, at its start; the route ends
// with every cell counted.
func TestLoopRunsEveryCell(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	code, err := r.loop(context.Background())
	if err != nil || code != exitOK {
		t.Fatalf("loop: %d, %v", code, err)
	}
	if !slices.Equal(ran, r.runs) || len(ran) != 12 {
		t.Fatalf("ran %d runs, want the 12 attempt 1 runs in order", len(ran))
	}
}

// Cells that do not count take re-runs on their attempt 2 dates while the
// route has two left; a failed attempt 2 is never re-run, and the route
// ends adverse.
func TestLoopReruns(t *testing.T) {
	var ran []schedule.Run
	var failing []string
	for _, c := range testSchedule(t, "groq", "other").Cells {
		if len(c.Attempts) == 2 && len(failing) < 3 {
			failing = append(failing, c.Label)
		}
	}
	if len(failing) != 3 {
		t.Fatalf("the test schedule has %d cells with an attempt 2", len(failing))
	}
	outcomes := func(run schedule.Run) string {
		if slices.Contains(failing, run.Label) && (run.Attempt == 1 || run.Label == failing[0]) {
			return outcomeInvalid
		}
		return outcomeCounted
	}
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), outcomes, &ran)
	code, err := r.loop(context.Background())
	if err != nil || code != exitAdverse {
		t.Fatalf("loop: %d, %v", code, err)
	}
	var reruns []string
	for _, run := range ran {
		if run.Attempt == 2 {
			reruns = append(reruns, run.Label)
		}
	}
	if len(ran) != 14 || !slices.Equal(reruns, failing[:2]) && !slices.Equal(reruns, []string{failing[1], failing[0]}) {
		t.Fatalf("ran %d runs; re-ran %v, want %v", len(ran), reruns, failing[:2])
	}
	if !slices.IsSortedFunc(ran, func(a, b schedule.Run) int { return a.Start.Compare(b.Start) }) {
		t.Fatal("runs out of start order")
	}
}

// A process started after some runs' tolerance records them missed and
// re-runs the first two; a cell left running by an ended process becomes
// interrupted and takes no re-run once the route has none left. Each
// missed or interrupted cell is labelled once, before the next run; one
// whose directory holds an attempts file is not.
func TestLoopMissedAndInterrupted(t *testing.T) {
	var ran []schedule.Run
	labelled = nil
	sched := testSchedule(t, "groq", "other")
	runs, _ := sched.Runs("groq")
	start := runs[2].Start.Add(schedule.StartTolerance + time.Second)
	r := clockRoute(t, start, counted, &ran)
	left := r.newRecord(runs[3])
	left.Outcome = outcomeRunning
	if err := r.writeNew(runs[3], left); err != nil {
		t.Fatal(err)
	}
	code, err := r.loop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{outcomeMissed, outcomeMissed, outcomeMissed, outcomeInterrupted} {
		if got := st.records[runKey(runs[i])]; got == nil || got.Outcome != want {
			t.Fatalf("run %d: %+v, want %s", i, got, want)
		}
	}
	taken := 0
	for _, run := range st.runs {
		if run.Attempt == 2 {
			taken++
		}
	}
	if taken != schedule.MaxReruns || len(ran) != 12-4+taken {
		t.Fatalf("%d re-runs taken, %d runs ran", taken, len(ran))
	}
	if code != exitAdverse {
		t.Fatalf("code %d", code)
	}
	if want := []schedule.Run{runs[3], runs[0], runs[1], runs[2]}; !slices.Equal(labelled, want) {
		t.Fatalf("labelled %v, want the interrupted run, then each missed run", labelled)
	}

	if err := os.RemoveAll(r.cellDir(runs[4])); err != nil {
		t.Fatal(err)
	}
	left = r.newRecord(runs[4])
	left.Outcome = outcomeInterrupted
	if err := r.writeNew(runs[4], left); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.cellDir(runs[4]), evidence.AttemptsFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if st, err := r.state(); err != nil || len(st.unlabelled) != 0 {
		t.Fatalf("unlabelled %v: %v", st.unlabelled, err)
	}
}

// A stopped cell ends the loop with the stop code; a later process takes
// its re-run and goes on. A wait cut short by the operator stops too.
func TestLoopStopAndRestart(t *testing.T) {
	var ran []schedule.Run
	stopAt := ""
	outcomes := func(run schedule.Run) string {
		if run.Label == stopAt && run.Attempt == 1 {
			return outcomeStopped
		}
		return outcomeCounted
	}
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), outcomes, &ran)
	stopAt = r.runs[1].Label
	code, err := r.loop(context.Background())
	if err != nil || code != exitStopped || len(ran) != 2 {
		t.Fatalf("loop: %d, %v, ran %d", code, err, len(ran))
	}
	code, err = r.loop(context.Background())
	if err != nil || code != exitOK || len(ran) != 13 {
		t.Fatalf("restart: %d, %v, ran %d", code, err, len(ran))
	}

	r2 := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, new([]schedule.Run))
	r2.wait = func(context.Context, time.Time) error { return errors.New("operator abort") }
	if code, err := r2.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("aborted wait: %d, %v", code, err)
	}
}

// Concurrent runs are the other routes' cells whose start-to-end time
// overlaps; a missed cell never ran.
func TestConcurrentRuns(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	other := func(label string, outcome string, start time.Time, end *time.Time) {
		rec := &cellRecord{Run: schedule.Run{Route: "other", Label: label, Attempt: 1, Start: start}, Outcome: outcome, StartUTC: start, EndUTC: end}
		if err := replaceJSON(filepath.Join(r.out, "other", label+"-a1", cellFile), rec); err != nil {
			t.Fatal(err)
		}
	}
	end := base.Add(30 * time.Minute)
	other("a", outcomeCounted, base, &end)
	other("b", outcomeRunning, base.Add(40*time.Minute), nil)
	other("c", outcomeMissed, base, nil)
	before := base.Add(-time.Hour)
	other("d", outcomeCounted, before, &before)
	got, err := r.concurrent(base.Add(10*time.Minute), base.Add(58*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Label != "a" || got[1].Label != "b" {
		t.Fatalf("concurrent %+v", got)
	}
	if _, err := os.Stat(r.dir); err == nil {
		t.Fatal("the route's own directory was created")
	}
}
