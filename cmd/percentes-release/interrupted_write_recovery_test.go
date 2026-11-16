package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// countWaits replaces r's wait with one that counts its calls and ends
// the loop.
func countWaits(r *route) *int {
	n := new(int)
	r.wait = func(context.Context, time.Time) error {
		*n++
		return loadgen.ErrOperatorAbort
	}
	return n
}

// A process that ends after a pre-ladder cell's frame store and before
// its attempts file leaves the store at the cell's start; the cell is
// held, with its stop taken from the decided label the running record
// carries or from its probe record's billing refusal, and once
// acknowledged it is never labelled over that store: the route goes on
// and pages reads the stop from the run record.
func TestEndBeforeAttemptsFileHoldsStopOverFrames(t *testing.T) {
	for _, decided := range []bool{false, true} {
		r, e := billingStopRoute(t)
		run := r.runs[0]
		pinned := r.now
		r.now = time.Now
		rec, err := r.runCell(context.Background(), run)
		r.now = pinned
		if err != nil || rec.Outcome != outcomeStopped {
			t.Fatalf("fixture: %+v, %v", rec, err)
		}
		dir := r.cellDir(run)
		for _, name := range []string{evidence.AttemptsFile, evidence.ManifestFile, report.JSONFile, report.TextFile} {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
		}
		frames := filepath.Join(evidence.FramesDir(r.framesRoot, rec.StartUTC), evidence.FramesFile)
		if !fileExists(frames) {
			t.Fatal("no frame store at the cell's start")
		}
		live := *rec
		live.Outcome, live.Reasons, live.BillingRefusals, live.EndUTC, live.Error, live.ProbeStatus, live.Sent = outcomeRunning, nil, 0, nil, "", 0, admission.Counts{}
		if decided {
			live.Decided = &decidedLabel{Outcome: outcomeStopped, Reasons: rec.Reasons, BillingRefusals: rec.BillingRefusals}
		}
		if err := replaceJSON(filepath.Join(dir, cellFile), &live); err != nil {
			t.Fatal(err)
		}
		hits := e.hits.Load()
		waits := countWaits(r)
		if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != hits || fileExists(filepath.Join(dir, evidence.ManifestFile)) {
			t.Fatalf("decided %t: first restart: exit %d, %v; %d waits", decided, code, err, *waits)
		}
		wantError := "the cell's record was not rewritten after its probe; its label is its probe record's"
		if decided {
			wantError = "the process ended before the cell's evidence was written"
		}
		held := readCell(t, r, run)
		if held.Outcome != outcomeStopped || held.Error != wantError || held.EndUTC == nil || held.BillingRefusals != 1 ||
			held.ProbeStatus != http.StatusPaymentRequired || held.Sent.Probe != 1 || !slices.Equal(held.Reasons, rec.Reasons) {
			t.Fatalf("decided %t: held record %+v", decided, held)
		}
		stopPublishedFromRecord(t, r, e.hits.Load, cellLabel(run))
		if !fileExists(frames) {
			t.Fatalf("decided %t: the cell's frame store was removed", decided)
		}
	}
}

// A stopped cell whose attempts file fails to write after its frame
// store is held; once acknowledged the route goes on and pages reads the
// stop from the run record.
func TestAttemptsWriteFailureHoldsStoppedCell(t *testing.T) {
	r, e := billingStopRoute(t)
	run := r.runs[0]
	blocker := filepath.Join(r.cellDir(run), evidence.AttemptsFile+".tmp")
	if err := os.MkdirAll(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	r.wait = noWait
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	waits := countWaits(r)
	if got := readCell(t, r, run); got.Outcome != outcomeStopped || got.Error == "" || got.Decided != nil {
		t.Fatalf("record %+v", got)
	}
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 0 || fileExists(filepath.Join(r.cellDir(run), evidence.ManifestFile)) {
		t.Fatalf("restart with no acknowledgement: exit %d, %v; %d waits", code, err, *waits)
	}
	stopPublishedFromRecord(t, r, e.hits.Load, cellLabel(run))
}

// A frame store that fails to write leaves the attempts file and no
// manifest; the cell is not labelled again, so a restart goes on. A label
// of an interrupted cell over a frame store at its start that holds
// frames leaves that store as it is.
func TestExistingFrameStoreKeptWithNoManifest(t *testing.T) {
	r, _ := billingStopRoute(t)
	run := r.runs[0]
	frames := filepath.Join(evidence.FramesDir(r.framesRoot, run.Start), evidence.FramesFile)
	other := []byte("{\"index\":0}\n")
	if err := os.MkdirAll(filepath.Dir(frames), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(frames, other, 0o600); err != nil {
		t.Fatal(err)
	}
	dir := r.cellDir(run)
	r.wait = noWait
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	waits := countWaits(r)
	got := readCell(t, r, run)
	if got.Outcome != outcomeStopped || !strings.Contains(got.Error, "file exists") || !fileExists(filepath.Join(dir, evidence.AttemptsFile)) {
		t.Fatalf("record %+v", got)
	}
	for _, name := range []string{evidence.ManifestFile, report.JSONFile, report.TextFile} {
		if fileExists(filepath.Join(dir, name)) {
			t.Fatalf("%s written after the frame store failed", name)
		}
	}
	if rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key); err != nil || rr.unrecorded != 0 || len(rr.stops) != 1 || len(rr.billing) != 1 {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
	acknowledge(t, r, cellLabel(run))
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 1 || fileExists(filepath.Join(dir, evidence.ManifestFile)) {
		t.Fatalf("acknowledged restart: exit %d, %v; %d waits", code, err, *waits)
	}

	if err := os.Remove(filepath.Join(dir, evidence.AttemptsFile)); err != nil {
		t.Fatal(err)
	}
	got.Outcome = outcomeInterrupted
	if err := replaceJSON(filepath.Join(dir, cellFile), got); err != nil {
		t.Fatal(err)
	}
	st, err := r.state()
	if err != nil || len(st.unlabelled) != 1 {
		t.Fatalf("state: %+v, %v", st, err)
	}
	if err := r.label(context.Background(), st.unlabelled[0]); err == nil || !strings.Contains(err.Error(), "file exists") {
		t.Fatalf("label over a store with frames: %v", err)
	}
	if b, err := os.ReadFile(frames); err != nil || !bytes.Equal(b, other) || fileExists(filepath.Join(dir, evidence.ManifestFile)) {
		t.Fatalf("frame store %q: %v", b, err)
	}
}

// A record left running beside a manifest takes the manifest's label
// whatever it is, with the probe's status and count; only a stopped one
// is recovered, so a counted cell takes no re-run.
func TestRunningRecordTakesManifestLabel(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	run := r.runs[0]
	rec := r.newRecord(run)
	rec.Outcome = outcomeRunning
	if err := r.writeNew(run, rec); err != nil {
		t.Fatal(err)
	}
	dir := r.cellDir(run)
	epoch := run.Start.Add(time.Minute)
	reasons := []string{"ladder ended at step 2: backoff"}
	if err := replaceJSON(filepath.Join(dir, evidence.ManifestFile), &evidence.Manifest{StartUTC: epoch,
		Release: &evidence.Cell{Run: run, Outcome: outcomeCounted, Reasons: reasons, LadderStarted: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := probe.Write(dir, &probe.Record{Request: probe.Exchange{DispatchUTC: run.Start, Status: http.StatusOK}}); err != nil {
		t.Fatal(err)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	got := st.records[runKey(run)]
	if got.Outcome != outcomeCounted || !slices.Equal(got.Reasons, reasons) || got.EpochUTC == nil || !got.EpochUTC.Equal(epoch) || got.EndUTC == nil ||
		got.ProbeStatus != http.StatusOK || got.Sent.Probe != 1 || got.Error != "" {
		t.Fatalf("record %+v", got)
	}
	if len(st.recovered) != 0 || len(st.unlabelled) != 0 || len(st.runs) != len(r.runs) || st.next == nil || *st.next != r.runs[1] {
		t.Fatalf("state: %d recovered, %d unlabelled, %d runs, next %+v", len(st.recovered), len(st.unlabelled), len(st.runs), st.next)
	}
	if disk := readCell(t, r, run); disk.Outcome != outcomeCounted || disk.ProbeStatus != http.StatusOK {
		t.Fatalf("record on disk %+v", disk)
	}
}

// A retained cell whose ledger settlement fails keeps its outcome, with
// the error recorded.
func TestSettleFailureKeepsOutcome(t *testing.T) {
	var ledgerPath string
	e := newEndpoint(t, func(int64) (int, string) {
		if err := os.Chmod(ledgerPath, 0o444); err != nil {
			t.Error(err)
		}
		return http.StatusNotFound, "no such model"
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	ledgerPath = r.ledgerPath
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	rec, err := r.runCell(context.Background(), run)
	if err == nil || !errors.Is(err, syscall.EACCES) {
		t.Fatalf("settle: %v", err)
	}
	got := readCell(t, r, run)
	if rec.Outcome != outcomeBlocked || got.Outcome != outcomeBlocked || got.Error == "" || got.Sent.Probe != 1 {
		t.Fatalf("record %+v", got)
	}
	if m, _ := checkRetained(t, r, run); m.Release.Outcome != outcomeBlocked {
		t.Fatalf("manifest %+v", m.Release)
	}
}

// A stopped cell writes its running record with its decided label before
// its evidence; a process that ends before the manifest leaves that
// record, which the next process takes as stopped with an execution error
// and holds, and once acknowledged the route goes on and pages reads the
// stop from the run record.
func TestDecidedLabelBeforeEvidenceRestartsStopped(t *testing.T) {
	r, e := billingStopRoute(t)
	run := r.runs[0]
	rec := r.newRecord(run)
	rec.Outcome = outcomeRunning
	if err := r.writeNew(run, rec); err != nil {
		t.Fatal(err)
	}
	dir := r.cellDir(run)
	live, err := startLiveSeries(filepath.Join(dir, dispatchFile), run)
	if err != nil {
		t.Fatal(err)
	}
	rec.Outcome, rec.Reasons, rec.BillingRefusals = outcomeStopped, []string{"route stop: HTTP 402, a documented billing refusal"}, 1
	// The ledger read that follows the decided record fails, as a process
	// that ended there would leave it.
	ledgerPath := r.ledgerPath
	r.ledgerPath = filepath.Join(t.TempDir(), "absent.jsonl")
	err = r.retain(context.Background(), &cellFiles{dir: dir, run: run, cfg: cellConfig(r.cfg, run.Seed, r.steps), live: live}, rec, nil)
	r.ledgerPath = ledgerPath
	if err == nil {
		t.Fatal("retain wrote the cell's evidence")
	}
	held := readCell(t, r, run)
	if d := held.Decided; held.Outcome != outcomeRunning || d == nil || d.Outcome != outcomeStopped || !slices.Equal(d.Reasons, rec.Reasons) || d.BillingRefusals != 1 {
		t.Fatalf("running record %+v, decided %+v", held, held.Decided)
	}
	hits := e.hits.Load()
	waits := countWaits(r)
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != hits {
		t.Fatalf("restart: exit %d, %v; %d waits", code, err, *waits)
	}
	got := readCell(t, r, run)
	if got.Outcome != outcomeStopped || got.Decided != nil || got.Error != "the process ended before the cell's evidence was written" ||
		got.BillingRefusals != 1 || !slices.Equal(got.Reasons, rec.Reasons) {
		t.Fatalf("record %+v", got)
	}
	stopPublishedFromRecord(t, r, e.hits.Load, cellLabel(run))
}
