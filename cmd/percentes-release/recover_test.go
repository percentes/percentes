package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/report"
)

// billingStopRoute is a route whose every request is a documented billing
// refusal, with its clock at the first run's start.
func billingStopRoute(t *testing.T) (*route, *endpoint) {
	t.Helper()
	e := newEndpoint(t, func(int64) (int, string) { return 402, "no credit" })
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, _ := newTestRoute(t, cfg, "+0.005")
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	return r, e
}

// A process that ends after a stopped cell's manifest and before its run
// record leaves the record running; the next process takes the manifest's
// label and stops the route before sending or waiting. A later process
// is held too, and goes on once the stop is acknowledged.
func TestLoopRecoversStopFromManifest(t *testing.T) {
	r, e := billingStopRoute(t)
	run := r.runs[0]
	live := r.newRecord(run)
	live.Outcome = outcomeRunning
	rec, err := r.runCell(context.Background(), run)
	if err != nil || rec.Outcome != outcomeStopped || rec.BillingRefusals != 1 {
		t.Fatalf("fixture: %+v, %v", rec, err)
	}
	if err := replaceJSON(filepath.Join(r.cellDir(run), cellFile), live); err != nil {
		t.Fatal(err)
	}
	hits := e.hits.Load()
	var waited []time.Time
	r.wait = func(_ context.Context, at time.Time) error {
		waited = append(waited, at)
		return loadgen.ErrOperatorAbort
	}
	code, err := r.loop(context.Background())
	if err != nil || code != exitStopped || len(waited) != 0 || e.hits.Load() != hits {
		t.Fatalf("restart: exit %d, %v; waited %v; %d requests after the stop", code, err, waited, e.hits.Load()-hits)
	}
	got := readCell(t, r, run)
	if got.Outcome != outcomeStopped || !slices.Equal(got.Reasons, rec.Reasons) || got.BillingRefusals != 1 || got.EndUTC == nil || got.EpochUTC != nil {
		t.Fatalf("recovered record %+v", got)
	}
	if _, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key); err != nil {
		t.Fatalf("pages: %v", err)
	}
	if code, err := r.loop(context.Background()); code != exitStopped || err != nil || len(waited) != 0 || e.hits.Load() != hits {
		t.Fatalf("restart with no acknowledgement: exit %d, %v; waited %v", code, err, waited)
	}
	acknowledge(t, r, cellLabel(run))
	if code, err := r.loop(context.Background()); code != exitStopped || err != nil || len(waited) != 1 {
		t.Fatalf("second restart: exit %d, %v; waited %v", code, err, waited)
	}
}

// A stopped cell whose evidence fails to write keeps its stop: the loop
// exits with the stop code, the record stays stopped with the error, and
// pages takes it as a cell with no manifest. A later process never labels
// it, also where neither its frame store nor its attempts file was
// written; once acknowledged it goes on and pages reads the stop from the
// run record.
func TestLoopKeepsStopAfterWriteError(t *testing.T) {
	r, e := billingStopRoute(t)
	run := r.runs[0]
	blocker := filepath.Join(r.cellDir(run), report.JSONFile)
	if err := os.MkdirAll(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	r.wait = noWait
	code, err := r.loop(context.Background())
	if err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	got := readCell(t, r, run)
	if got.Outcome != outcomeStopped || got.Error == "" || got.BillingRefusals != 1 {
		t.Fatalf("record %+v", got)
	}
	if fileExists(filepath.Join(r.cellDir(run), evidence.ManifestFile)) {
		t.Fatal("a failed pair left a manifest")
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || rr.unrecorded != 0 || len(rr.stops) != 1 || len(rr.billing) != 1 {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
	st, err := r.state()
	if err != nil || len(st.recovered) != 0 || len(st.unlabelled) != 0 {
		t.Fatalf("state: %+v, %v", st, err)
	}

	frames, err := filepath.Glob(filepath.Join(r.framesRoot, "*"))
	if err != nil || len(frames) != 1 {
		t.Fatalf("frame store %v: %v", frames, err)
	}
	for _, p := range []string{blocker, filepath.Join(r.cellDir(run), evidence.AttemptsFile), frames[0]} {
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}
	if st, err = r.state(); err != nil || len(st.unlabelled) != 0 {
		t.Fatalf("state: %+v, %v", st, err)
	}
	stopPublishedFromRecord(t, r, e.hits.Load, cellLabel(run))
}
