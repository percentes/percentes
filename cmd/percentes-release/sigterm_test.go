package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// terminatedAfter ends a context on SIGTERM's cause after d.
func terminatedAfter(d time.Duration) context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	time.AfterFunc(d, func() { cancel(errTerminated) })
	return ctx
}

// copyTree copies the files under src, other than temporary files and
// files gone before they are read, to dst; it may run off the test's
// goroutine.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if strings.HasSuffix(p, ".tmp") {
			return nil
		}
		b, err := os.ReadFile(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o600)
	})
}

// restoreTree leaves r's route directory as the copy under snap, as a
// process killed when the copy was taken would.
func restoreTree(t *testing.T, r *route, snap string) {
	t.Helper()
	if err := os.RemoveAll(r.dir); err != nil {
		t.Fatal(err)
	}
	if err := copyTree(snap, r.dir); err != nil {
		t.Fatal(err)
	}
}

// run takes SIGTERM as errTerminated; every other command, ceiling-check
// and rehearse included, as an operator abort.
func TestSigtermCause(t *testing.T) {
	if !errors.Is(sigtermCause("run"), errTerminated) {
		t.Fatalf("run: %v", sigtermCause("run"))
	}
	for _, c := range []string{"ceiling-check", "rehearse", "schedule", "pages"} {
		if !errors.Is(sigtermCause(c), loadgen.ErrOperatorAbort) {
			t.Fatalf("%s: %v", c, sigtermCause(c))
		}
	}
}

// A real SIGTERM ends run's context with errTerminated and another
// command's with an operator abort; SIGINT ends run's with an operator
// abort.
func TestSigtermSignals(t *testing.T) {
	for _, tc := range []struct {
		term error
		sig  syscall.Signal
		want error
	}{
		{errTerminated, syscall.SIGTERM, errTerminated},
		{loadgen.ErrOperatorAbort, syscall.SIGTERM, loadgen.ErrOperatorAbort},
		{errTerminated, syscall.SIGINT, loadgen.ErrOperatorAbort},
	} {
		ctx, stop := operatorContext(tc.term)
		if err := syscall.Kill(os.Getpid(), tc.sig); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("%v did not end the context", tc.sig)
		}
		if !errors.Is(context.Cause(ctx), tc.want) {
			t.Fatalf("%v with %v: cause %v", tc.sig, tc.term, context.Cause(ctx))
		}
		stop()
	}
}

// A ladder cut by SIGTERM is interrupted, never counted or stopped; a
// stop found with it still stops the route. SIGTERM after a ladder that
// ran to its end leaves the cell's own label.
func TestSigtermCellOutcome(t *testing.T) {
	res := &loadgen.Result{Gates: loadgen.GateReport{Pass: true, CPUMeasured: true}, Interrupted: &loadgen.Interruption{Cause: "SIGTERM"}}
	clock := &evidence.ClockRecord{}
	if o, r := cellOutcome(res, errors.New("loadgen: run aborted by context"), errTerminated, clock, nil); o != outcomeInterrupted || !slices.Equal(r, []string{terminatedReason}) {
		t.Fatalf("SIGTERM: %s %q", o, r)
	}
	if o, _ := cellOutcome(res, nil, errTerminated, clock, nil, &routeStop{"route stop: billing"}); o != outcomeStopped {
		t.Fatalf("SIGTERM with a billing stop: %s", o)
	}
	if o, r := cellOutcome(res, nil, loadgen.ErrOperatorAbort, clock, nil); o != outcomeStopped || r[0] != "stopped: operator abort" {
		t.Fatalf("operator abort: %s %q", o, r)
	}
	ended := &loadgen.Result{Gates: loadgen.GateReport{Pass: true, CPUMeasured: true}}
	if o, r := cellOutcome(ended, nil, errTerminated, clock, nil); o != outcomeCounted || len(r) != 0 {
		t.Fatalf("SIGTERM after the ladder's end: %s %q", o, r)
	}
	if o, r := cellOutcome(ended, errors.New("loadgen: a response arrived over HTTP/2.0"), errTerminated, clock, nil); o != outcomeInvalid || len(r) != 1 {
		t.Fatalf("SIGTERM after a ladder with a run error: %s %q", o, r)
	}
}

// SIGTERM during a cell's start checks interrupts it before admission:
// nothing admitted or sent, no evidence retained, no hold; the next
// start labels it.
func TestSigtermBeforeAdmission(t *testing.T) {
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	r.ackDir = t.TempDir()
	r.refresh = slowCommand(r.refresh)
	before, err := os.ReadFile(r.ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	run := r.runs[0]
	rec, err := r.runCell(terminatedAfter(500*time.Millisecond), run)
	if err != nil || rec.Outcome != outcomeInterrupted || !slices.Equal(rec.Reasons, []string{terminatedReason}) {
		t.Fatalf("%+v, %v", rec, err)
	}
	after, err := os.ReadFile(r.ledgerPath)
	if err != nil || !bytes.Equal(before, after) || e.hits.Load() != 0 {
		t.Fatalf("ledger changed %t, %d requests: %v", !bytes.Equal(before, after), e.hits.Load(), err)
	}
	for _, f := range []string{evidence.ManifestFile, evidence.AttemptsFile} {
		if fileExists(filepath.Join(r.cellDir(run), f)) {
			t.Fatalf("%s written", f)
		}
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 0 || len(st.unlabelled) != 1 || len(markers(t, r)) != 0 {
		t.Fatalf("holds %q, %d unlabelled, markers %v: %v", holds, len(st.unlabelled), markers(t, r), err)
	}
}

// SIGTERM while the probe is answered interrupts the cell before the
// first dispatch: the probe's admission is settled with the probe alone,
// and its dispatch series is not final.
func TestSigtermBeforeFirstDispatch(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	e := newEndpoint(t, func(n int64) (int, string) {
		cancel(errTerminated)
		return http.StatusOK, ""
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	rec, err := r.runCell(ctx, r.runs[0])
	if err != nil || rec.Outcome != outcomeInterrupted || !slices.Equal(rec.Reasons, []string{terminatedReason}) || e.hits.Load() != 1 {
		t.Fatalf("%+v, %v, %d requests", rec, err, e.hits.Load())
	}
	u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]
	if u.Reserved != 0 || u.Probe != 1 || u.Warmup+u.Measured != 0 {
		t.Fatalf("ledger %+v", u)
	}
	var own dispatchSeries
	if b, err := os.ReadFile(filepath.Join(r.cellDir(r.runs[0]), dispatchFile)); err != nil || json.Unmarshal(b, &own) != nil || own.Final {
		t.Fatalf("dispatch series %+v: %v", own, err)
	}
}

// The whole cycle: SIGTERM mid-ladder ends the loop with exitTerminated,
// no marker and no hold, the cell interrupted with its admission settled
// at what the endpoint received; the next start, with no acknowledgement
// file, labels the cell with its ladder's final dispatch series, takes
// its re-run and sends a fresh cell's requests, and pages reads the
// interrupted attempt once.
func TestSigtermMidLadderCycle(t *testing.T) {
	fastDispatch(t)
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	r.steps = 1
	r.ackDir = t.TempDir()
	run := r.runs[0]
	if !r.hasAttempt2(run.Label) {
		t.Fatalf("%s has no attempt 2", run.Label)
	}
	shiftClock(r, run.Start)
	ctx, cancel := context.WithCancelCause(context.Background())
	r.onEpoch = func(time.Time) { time.AfterFunc(1500*time.Millisecond, func() { cancel(errTerminated) }) }
	if code, err := r.loop(ctx); err != nil || code != exitTerminated {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	rec := readCell(t, r, run)
	if rec.Outcome != outcomeInterrupted || !slices.Equal(rec.Reasons, []string{terminatedReason}) || rec.Interrupted == nil || rec.Decided != nil {
		t.Fatalf("record %s %q, interrupted %+v", rec.Outcome, rec.Reasons, rec.Interrupted)
	}
	sent1 := e.hits.Load()
	u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]
	if u.Reserved != 0 || int64(u.Probe+u.Warmup+u.Measured) != sent1 || int64(rec.Sent.Probe+rec.Sent.Warmup+rec.Sent.Measured) != sent1 {
		t.Fatalf("ledger %+v, record %+v, endpoint %d", u, rec.Sent, sent1)
	}
	if len(markers(t, r)) != 0 || fileExists(filepath.Join(r.cellDir(run), evidence.ManifestFile)) {
		t.Fatalf("markers %v, or a manifest written", markers(t, r))
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 0 {
		t.Fatalf("holds %q: %v", holds, err)
	}
	var rerun *schedule.Run
	for i := range st.runs {
		if st.runs[i].Label == run.Label && st.runs[i].Attempt == 2 {
			rerun = &st.runs[i]
		}
	}
	if rerun == nil {
		t.Fatalf("no re-run of %s in %v", run.Label, st.runs)
	}

	// The next start, at the re-run's start: every attempt 1 between is
	// missed, the interrupted cell is labelled, and the re-run is sent.
	shiftClock(r, rerun.Start)
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	r.onEpoch = nil
	var sent2 int64
	r.cell = func(ctx context.Context, next schedule.Run) (*cellRecord, error) {
		if next != *rerun {
			t.Fatalf("ran %s attempt %d, want the re-run", next.Label, next.Attempt)
		}
		routeRecs, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
		if err != nil || len(routeRecs.stops) != 0 || routeRecs.unrecorded != 0 || routeRecs.labelled != 4 {
			t.Fatalf("pages: %+v, %v", routeRecs, err)
		}
		before := e.hits.Load()
		rec, err := r.runCell(ctx, next)
		sent2 = e.hits.Load() - before
		cancel2(errTerminated)
		return rec, err
	}
	if code, err := r.loop(ctx2); err != nil || code != exitTerminated {
		t.Fatalf("next start: exit %d, %v", code, err)
	}
	m, _ := checkRetained(t, r, run)
	if m.Release.Outcome != evidence.CellInterrupted || !m.Release.LadderStarted || m.Release.RecordsComplete {
		t.Fatalf("label %+v", m.Release)
	}
	if src := m.Release.Dispatch.Sources[0]; !src.Final || src.Dispatches != rec.Sent.Warmup+rec.Sent.Measured {
		t.Fatalf("own dispatch source %+v, record %+v", src, rec.Sent)
	}
	rr := readCell(t, r, *rerun)
	fresh := freshSends(len(loadgen.BuildSchedule(cellConfig(r.cfg, rerun.Seed, r.steps))), rr)
	if sent2 != fresh || int64(rr.Sent.Probe+rr.Sent.Warmup+rr.Sent.Measured) != fresh {
		t.Fatalf("the re-run sent %d (record %+v), a fresh cell %d", sent2, rr.Sent, fresh)
	}
	if u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]; u.Reserved != 0 || int64(u.Probe+u.Warmup+u.Measured) != sent1+fresh {
		t.Fatalf("ledger %+v, sent %d", u, sent1+fresh)
	}
}

// SIGTERM while the loop waits, before a label, and with no run left
// ends the loop with exitTerminated and no marker; the next start goes
// on with no acknowledgement file.
func TestSigtermBetweenCells(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	r.ackDir = t.TempDir()
	r.wait = func(ctx context.Context, _ time.Time) error {
		<-ctx.Done()
		return context.Cause(ctx)
	}
	if code, err := r.loop(terminatedAfter(100 * time.Millisecond)); err != nil || code != exitTerminated || len(ran) != 0 || len(markers(t, r)) != 0 {
		t.Fatalf("wait: exit %d, %v, ran %d, markers %v", code, err, len(ran), markers(t, r))
	}

	r = clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	r.ackDir = t.TempDir()
	ended, cancel := context.WithCancelCause(context.Background())
	cancel(errTerminated)
	if err := os.MkdirAll(r.cellDir(r.runs[0]), 0o755); err != nil {
		t.Fatal(err)
	}
	rec := r.newRecord(r.runs[0])
	rec.Outcome = outcomeInterrupted
	if err := r.writeNew(r.runs[0], rec); err != nil {
		t.Fatal(err)
	}
	labelled = nil
	if code, err := r.loop(ended); err != nil || code != exitTerminated || len(labelled) != 0 || len(markers(t, r)) != 0 {
		t.Fatalf("label: exit %d, %v, labelled %v", code, err, labelled)
	}

	ran = nil
	r = clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	if code, err := r.loop(context.Background()); err != nil || code != exitOK {
		t.Fatalf("loop: %d, %v", code, err)
	}
	closedAfter = nil
	if code, err := r.loop(ended); err != nil || code != exitTerminated || closedAfter != nil || len(markers(t, r)) != 0 {
		t.Fatalf("window close: exit %d, %v, closed after %v", code, err, closedAfter)
	}
}

// SIGTERM during a cell that a stop ended keeps the stop's hold and
// exits 3; after a counted cell it exits with exitTerminated.
func TestSigtermAfterCell(t *testing.T) {
	for _, outcome := range []string{outcomeStopped, outcomeCounted} {
		var ran []schedule.Run
		r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), func(schedule.Run) string { return outcome }, &ran)
		r.ackDir = t.TempDir()
		ctx, cancel := context.WithCancelCause(context.Background())
		inner := r.cell
		r.cell = func(c context.Context, run schedule.Run) (*cellRecord, error) {
			rec, err := inner(c, run)
			cancel(errTerminated)
			return rec, err
		}
		want := exitTerminated
		if outcome == outcomeStopped {
			want = exitStopped
		}
		if code, err := r.loop(ctx); err != nil || code != want || len(ran) != 1 || len(markers(t, r)) != 0 {
			t.Fatalf("%s: exit %d, %v, ran %d", outcome, code, err, len(ran))
		}
		st, err := r.state()
		if err != nil {
			t.Fatal(err)
		}
		if holds, err := r.holds(st); err != nil || (len(holds) == 1) != (outcome == outcomeStopped) {
			t.Fatalf("%s: holds %q: %v", outcome, holds, err)
		}
	}
}

// pages reads a cell SIGTERM interrupted after its probe, labelled at the
// next start with no acknowledgement file, as one labelled attempt, and
// publishes the route.
func TestSigtermPages(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	e := newEndpoint(t, func(int64) (int, string) {
		cancel(errTerminated)
		return http.StatusOK, ""
	})
	r, f := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	r.ackDir = t.TempDir()
	run := r.runs[0]
	shiftClock(r, run.Start)
	if code, err := r.loop(ctx); err != nil || code != exitTerminated || e.hits.Load() != 1 {
		t.Fatalf("loop: exit %d, %v, %d requests", code, err, e.hits.Load())
	}
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	r.wait = func(context.Context, time.Time) error {
		cancel2(errTerminated)
		return errTerminated
	}
	if code, err := r.loop(ctx2); err != nil || code != exitTerminated || e.hits.Load() != 1 || len(markers(t, r)) != 0 {
		t.Fatalf("next start: exit %d, %v, %d requests", code, err, e.hits.Load())
	}
	if m, _ := checkRetained(t, r, run); m.Release.Outcome != evidence.CellInterrupted || m.Release.LadderStarted {
		t.Fatalf("label %+v", m.Release)
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || rr.labelled != 1 || rr.unrecorded != 0 || len(rr.stops) != 0 {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
	cfgs := append(slices.Clone(f.in.hostCfgs), r.cfg)
	if _, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, f.in.probeFiles, cfgs, r.key, ""); err != nil {
		t.Fatal(err)
	}
}

// A process killed at the stop timeout after SIGTERM, its cell still
// running, restarts as a crash does: the cell is interrupted and holds
// nothing, the next start, with no acknowledgement file, labels it and
// goes on to its wait with nothing sent, and pages reads it.
func TestSigtermKilledAtStopTimeout(t *testing.T) {
	srv, hits := hangingEndpoint(t, func(int64) int { return 0 })
	r, f := newTestRoute(t, releaseConfig(t, srv.URL+"/openai/v1/chat/completions"), "+0.005")
	ladder, binding, cpu := r.cfg.Release.Ladder, r.cfg.Release.BindingRPS, r.cfg.ClientValidity.CPUWindowS
	shortLadder(r)
	r.cfg.Client.HTTPTimeoutS = 2
	r.ackDir = t.TempDir()
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	r.wait = noWait
	ctx, cancel := context.WithCancelCause(context.Background())
	snap := t.TempDir()
	snapped := make(chan error, 1)
	r.onEpoch = func(time.Time) {
		go func() {
			time.Sleep(time.Second)
			cancel(errTerminated)
			time.Sleep(500 * time.Millisecond)
			snapped <- copyTree(r.dir, snap)
		}()
	}
	if code, err := r.loop(ctx); err != nil || code != exitTerminated {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	if err := <-snapped; err != nil {
		t.Fatal(err)
	}
	restoreTree(t, r, snap)
	if got := readCell(t, r, run); got.Outcome != outcomeRunning {
		t.Fatalf("record at the kill %s", got.Outcome)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 0 || len(st.unlabelled) != 1 || st.unlabelled[0].Outcome != outcomeInterrupted {
		t.Fatalf("holds %q, unlabelled %+v: %v", holds, st.unlabelled, err)
	}

	r.cfg.Release.Ladder, r.cfg.Release.BindingRPS, r.cfg.ClientValidity.CPUWindowS = ladder, binding, cpu
	r.onEpoch = nil
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	r.wait = func(context.Context, time.Time) error {
		cancel2(errTerminated)
		return errTerminated
	}
	sent := hits.Load()
	var code int
	stdout := stdoutOf(t, func() { code, err = r.loop(ctx2) })
	if err != nil || code != exitTerminated || hits.Load() != sent || len(markers(t, r)) != 0 || strings.Contains(stdout, "held") ||
		!strings.Contains(stdout, run.Label+" attempt 1: interrupted, labelled") {
		t.Fatalf("next start: exit %d, %v; %d requests:\n%s", code, err, hits.Load()-sent, stdout)
	}
	if m, err := evidence.ReadManifest(filepath.Join(r.cellDir(run), evidence.ManifestFile)); err != nil || m.Release.Outcome != evidence.CellInterrupted || !m.Release.LadderStarted || m.Release.RecordsComplete {
		t.Fatalf("label %+v: %v", m, err)
	}
	cfgs := append(slices.Clone(f.in.hostCfgs), r.cfg)
	if _, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, f.in.probeFiles, cfgs, r.key, ""); err != nil {
		t.Fatal(err)
	}
}

// A documented billing refusal answered after SIGTERM still stops the
// cell and holds the route, with no abort marker, and its end status and
// balance commands run.
func TestSigtermBillingAfterSignal(t *testing.T) {
	termed := make(chan struct{})
	srv := newEndpoint(t, func(n int64) (int, string) {
		if n == 1 {
			return http.StatusOK, ""
		}
		<-termed
		return 402, "no credit"
	})
	cfg := releaseConfig(t, srv.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, _ := newTestRoute(t, cfg, "+0.005")
	shortLadder(r)
	r.ackDir = t.TempDir()
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	r.wait = noWait
	ctx, cancel := context.WithCancelCause(context.Background())
	r.onEpoch = func(time.Time) {
		time.AfterFunc(500*time.Millisecond, func() {
			cancel(errTerminated)
			close(termed)
		})
	}
	if code, err := r.loop(ctx); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	rec := readCell(t, r, run)
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || rec.Outcome != outcomeStopped || rec.BillingRefusals == 0 || len(holds) != 1 || len(markers(t, r)) != 0 {
		t.Fatalf("record %s %q, holds %q, markers %v: %v", rec.Outcome, rec.Reasons, holds, markers(t, r), err)
	}
	m, _ := checkRetained(t, r, run)
	endRecordsRan(t, r, run, m)
}

// pages reads a cell SIGTERM interrupted mid-ladder, labelled at the next
// start, with the route's pinned ladder restored for the label.
func TestSigtermLadderPages(t *testing.T) {
	fastDispatch(t)
	e := newEndpoint(t, always200)
	r, f := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	ladder, binding, cpu := r.cfg.Release.Ladder, r.cfg.Release.BindingRPS, r.cfg.ClientValidity.CPUWindowS
	shortLadder(r)
	r.ackDir = t.TempDir()
	run := r.runs[0]
	shiftClock(r, run.Start)
	ctx, cancel := context.WithCancelCause(context.Background())
	r.onEpoch = func(time.Time) { time.AfterFunc(1500*time.Millisecond, func() { cancel(errTerminated) }) }
	if code, err := r.loop(ctx); err != nil || code != exitTerminated {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	r.cfg.Release.Ladder, r.cfg.Release.BindingRPS, r.cfg.ClientValidity.CPUWindowS = ladder, binding, cpu
	r.onEpoch = nil
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	r.wait = func(context.Context, time.Time) error {
		cancel2(errTerminated)
		return errTerminated
	}
	if code, err := r.loop(ctx2); err != nil || code != exitTerminated {
		t.Fatalf("next start: exit %d, %v", code, err)
	}
	if m, _ := checkRetained(t, r, run); m.Release.Outcome != evidence.CellInterrupted || !m.Release.LadderStarted || m.Release.RecordsComplete {
		t.Fatalf("label %+v", m.Release)
	}
	cfgs := append(slices.Clone(f.in.hostCfgs), r.cfg)
	if _, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, f.in.probeFiles, cfgs, r.key, ""); err != nil {
		t.Fatal(err)
	}
}

// ceiling-check takes a real SIGTERM as an operator abort: it stops
// sending, writes its record with its admission settled and exits 2; the
// next check is sent, and every admission of the route has its record.
func TestSigtermCeilingCheckWritesRecord(t *testing.T) {
	endpoint, hits := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n == 3 {
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				t.Error(err)
			}
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	var code int
	text := stdoutOf(t, func() { code = dispatch(append([]string{"ceiling-check"}, c.args(groq, 2000)...)) })
	if code != exitAdverse || !strings.Contains(text, "groq: STOPPED: interrupted: ") || !strings.Contains(text, ": operator abort; the check may be sent again\n") {
		t.Fatalf("ceiling-check: exit %d\n%s", code, text)
	}
	recs := c.records("groq")
	if len(recs) != 1 {
		t.Fatalf("records %v", recs)
	}
	if rec := readRecord(t, recs[0]); !strings.HasSuffix(rec.Interrupted, ": "+loadgen.ErrOperatorAbort.Error()) || rec.Sent() == probe.CeilingRequests {
		t.Fatalf("interrupted %q, %d sent", rec.Interrupted, rec.Sent())
	}
	if u := ledgerState(t, ledger).Routes["groq"]; u.Reserved != 0 || int32(u.Probe) != hits.Load() {
		t.Fatalf("ledger %+v, %d requests", u, hits.Load())
	}
	if code, text, err := c.check(context.Background(), groq, 2000); err != nil || code != exitOK {
		t.Fatalf("the next check: exit %d, %v\n%s", code, err, text)
	}
	entries, err := ledgerEntries(ledger, "")
	if err != nil {
		t.Fatal(err)
	}
	var read []*probe.ReleaseRecord
	for _, p := range c.records("groq") {
		rec, err := readCheckRecord(p)
		if err != nil {
			t.Fatal(err)
		}
		read = append(read, rec)
	}
	if err := checkAdmitted(entries, []string{"groq"}, read); err != nil || len(read) != 2 {
		t.Fatalf("%d records: %v", len(read), err)
	}
}

// A label under way when SIGTERM arrives runs its commands to their end:
// its end status records no cancellation and the loop then ends with
// exitTerminated and no marker; the next start, with no acknowledgement
// file, labels nothing again, sends nothing and goes on to its wait, and
// pages reads the cell as one labelled attempt.
func TestSigtermDuringLabelRunsItsCommands(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	e := newEndpoint(t, func(int64) (int, string) {
		cancel(errTerminated)
		return http.StatusOK, ""
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	r.ackDir = t.TempDir()
	run := r.runs[0]
	shiftClock(r, run.Start)
	if code, err := r.loop(ctx); err != nil || code != exitTerminated || e.hits.Load() != 1 {
		t.Fatalf("loop: exit %d, %v, %d requests", code, err, e.hits.Load())
	}

	status := r.status
	r.status = slowCommand(status)
	var (
		code int
		err  error
	)
	stdout := stdoutOf(t, func() { code, err = r.loop(terminatedAfter(500 * time.Millisecond)) })
	if err != nil || code != exitTerminated || e.hits.Load() != 1 || len(markers(t, r)) != 0 || !strings.Contains(stdout, run.Label+" attempt 1: interrupted, labelled") {
		t.Fatalf("label: exit %d, %v, %d requests:\n%s", code, err, e.hits.Load(), stdout)
	}
	b, err := os.ReadFile(filepath.Join(r.cellDir(run), evidence.StatusEndFile))
	if err != nil {
		t.Fatal(err)
	}
	var end evidence.StatusRecord
	if err := json.Unmarshal(b, &end); err != nil || end.Error != "" {
		t.Fatalf("end status %+v: %v", end, err)
	}

	r.status = status
	ctx3, cancel3 := context.WithCancelCause(context.Background())
	r.wait = func(context.Context, time.Time) error {
		cancel3(errTerminated)
		return errTerminated
	}
	stdout = stdoutOf(t, func() { code, err = r.loop(ctx3) })
	if err != nil || code != exitTerminated || e.hits.Load() != 1 || len(markers(t, r)) != 0 || strings.Contains(stdout, "labelled") {
		t.Fatalf("next start: exit %d, %v, %d requests:\n%s", code, err, e.hits.Load(), stdout)
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || rr.labelled != 1 || rr.unrecorded != 0 || len(rr.stops) != 0 {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
}

// A wait that ends for a cause other than an operator abort or SIGTERM is
// an execution error that writes no marker and holds nothing: the next
// start, with no acknowledgement file, runs every cell.
func TestWaitEndedOtherwiseHoldsNothing(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	r.ackDir = t.TempDir()
	wait := r.wait
	r.wait = func(context.Context, time.Time) error { return errors.New("clock read failed") }
	code, err := r.loop(context.Background())
	if code != exitError || err == nil || !strings.Contains(err.Error(), "clock read failed") || len(ran) != 0 || len(markers(t, r)) != 0 {
		t.Fatalf("wait: exit %d, %v, ran %d, markers %v", code, err, len(ran), markers(t, r))
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 0 {
		t.Fatalf("holds %q: %v", holds, err)
	}
	r.wait = wait
	if code, err := r.loop(context.Background()); err != nil || code != exitOK || len(ran) != len(r.runs) {
		t.Fatalf("next start: exit %d, %v, ran %d of %d", code, err, len(ran), len(r.runs))
	}
}

// hostGatesOpen widens the client-validity gate's GC pause and CPU bounds,
// which read this test process, so the cell's label rests on its ladder.
func hostGatesOpen(r *route) {
	r.cfg.ClientValidity.GoGCPauseP99Ms = 1000
	r.cfg.ClientValidity.MaxCPUPct = 100
}

// hostGatesWide widens hostGatesOpen's bounds and the send-skew bounds,
// so the cell's label rests on its ladder and its undispatched requests.
func hostGatesWide(r *route) {
	hostGatesOpen(r)
	r.cfg.ClientValidity.SendSkewP99Ms, r.cfg.ClientValidity.SendSkewMaxMs = 1000, 1000
}

// gateShut fails the client-validity gate on its send-skew bounds.
func gateShut(r *route) {
	hostGatesWide(r)
	r.cfg.ClientValidity.SendSkewP99Ms, r.cfg.ClientValidity.SendSkewMaxMs = 0, 0
}

// ladderSignalRun runs the first cell of a route cut to its first steps
// ladder steps through run's operatorContext and r.loop, with a real
// signal sig sent sigAt after the epoch unless the loop has returned. The
// probe is answered 200 and each ladder request status; with hold above 0
// the first request to arrive after the midpoint of step k's last two
// intended times is held until hold after that step's drain end, and the
// test is skipped when none arrived and the cell left a request
// undispatched. setup, where not nil, edits the route before the loop. It
// returns the route, the run, and the loop's exit code and output.
func ladderSignalRun(t *testing.T, sig syscall.Signal, steps, k, status int, hold time.Duration, sigAt func(drains []time.Duration) time.Duration, setup func(*route)) (*route, schedule.Run, int, string) {
	t.Helper()
	fastDispatch(t)
	var holdFrom, holdUntil, held atomic.Int64
	e := newEndpoint(t, func(n int64) (int, string) {
		if n == 1 {
			return http.StatusOK, ""
		}
		if from := holdFrom.Load(); hold > 0 && from != 0 && time.Now().UnixNano() >= from && held.Add(1) == 1 {
			time.Sleep(time.Until(time.Unix(0, holdUntil.Load())))
		}
		if status != http.StatusOK {
			return status, `{"error":{"message":"rate limited"}}`
		}
		return http.StatusOK, ""
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	r.steps = steps
	r.ackDir = t.TempDir()
	run := r.runs[0]
	l := loadgen.BuildLadder(cellConfig(r.cfg, run.Seed, r.steps))
	var drains []time.Duration
	for _, s := range l.Steps {
		drains = append(drains, time.Duration(s.DrainEndNs))
	}
	end := l.Steps[k].End
	from := time.Duration(l.Offsets[end-1]+l.Offsets[end-2]) / 2
	shiftClock(r, run.Start)
	if setup != nil {
		setup(r)
	}
	ctx, stop := operatorContext(sigtermCause("run"))
	var (
		mu         sync.Mutex
		done, sent bool
		tm         *time.Timer
	)
	r.onEpoch = func(ep time.Time) {
		holdUntil.Store(ep.Add(drains[k] + hold).UnixNano())
		holdFrom.Store(ep.Add(from).UnixNano())
		mu.Lock()
		defer mu.Unlock()
		tm = time.AfterFunc(time.Until(ep.Add(sigAt(drains))), func() {
			mu.Lock()
			defer mu.Unlock()
			if done {
				return
			}
			sent = true
			if err := syscall.Kill(os.Getpid(), sig); err != nil {
				t.Error(err)
			}
		})
	}
	var (
		code int
		err  error
	)
	out := stdoutOf(t, func() { code, err = r.loop(ctx) })
	mu.Lock()
	done = true
	if tm != nil {
		tm.Stop()
	}
	signalled := sent
	mu.Unlock()
	if signalled {
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Error("signal sent but not received")
		}
	}
	stop()
	if err != nil {
		t.Fatal(err)
	}
	if hold > 0 && held.Load() == 0 {
		if rec := readCell(t, r, run); rec.Ladder != nil {
			for i := range rec.Ladder.Steps {
				if g := rec.Ladder.Steps[i].Gates; g != nil && g.Undispatched > 0 {
					t.Skipf("step %d left %d requests undispatched and none was held", i, g.Undispatched)
				}
			}
		}
		t.Fatalf("no request held\n%s", out)
	}
	return r, run, code, out
}

// rerunOf reports whether the route's state schedules run's attempt 2,
// with no hold and no marker.
func rerunOf(t *testing.T, r *route, run schedule.Run) bool {
	t.Helper()
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 0 || len(markers(t, r)) != 0 {
		t.Fatalf("holds %q, markers %v: %v", holds, markers(t, r), err)
	}
	for i := range st.runs {
		if st.runs[i].Label == run.Label && st.runs[i].Attempt == 2 {
			return true
		}
	}
	return false
}

// endRecordsRan fails t unless run's end status and balance commands ran
// to a zero exit and the manifest lists both.
func endRecordsRan(t *testing.T, r *route, run schedule.Run, m *evidence.Manifest) {
	t.Helper()
	for _, name := range []string{evidence.StatusEndFile, evidence.BalanceEndFile} {
		b, err := os.ReadFile(filepath.Join(r.cellDir(run), name))
		if err != nil {
			t.Fatal(err)
		}
		var rec evidence.StatusRecord
		if err := json.Unmarshal(b, &rec); err != nil || rec.ExitCode == nil || *rec.ExitCode != 0 || rec.Error != "" {
			t.Fatalf("%s: exit %v, error %q: %v", name, rec.ExitCode, rec.Error, err)
		}
	}
	if m.StatusEnd == nil || m.BalanceEnd == nil {
		t.Fatalf("manifest end records: status %v, balance %v", m.StatusEnd, m.BalanceEnd)
	}
}

// SIGTERM after the ladder's end, its last request still answered past
// the drain, leaves the cell its own label: counted with its evidence
// retained and its end commands run, its admission settled, exit 5 and
// no re-run, hold or marker. A ladder its verdict ended at step 1 keeps
// that verdict's label. A cell the client-validity gate failed keeps
// client_invalid and takes its re-run. SIGTERM inside the last drain
// interrupts the cell and takes its re-run.
func TestSigtermAfterLadderEnd(t *testing.T) {
	t.Run("ladder ran to its end", func(t *testing.T) {
		r, run, code, out := ladderSignalRun(t, syscall.SIGTERM, 1, 0, http.StatusOK, 700*time.Millisecond, func(d []time.Duration) time.Duration { return d[0] + 300*time.Millisecond }, hostGatesWide)
		rec := readCell(t, r, run)
		if code != exitTerminated || rec.Interrupted != nil || rec.Outcome != outcomeCounted && !hostGateFailed(rec) {
			t.Fatalf("exit %d, %s %q, interrupted %+v\n%s", code, rec.Outcome, rec.Reasons, rec.Interrupted, out)
		}
		skipOnHostGate(t, rec)
		if !strings.Contains(out, "ended on SIGTERM after "+run.Label) {
			t.Fatalf("no end line\n%s", out)
		}
		m, _ := checkRetained(t, r, run)
		if m.Release.Outcome != evidence.CellCounted {
			t.Fatalf("label %+v", m.Release)
		}
		endRecordsRan(t, r, run, m)
		if u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]; u.Reserved != 0 || u.Probe+u.Warmup+u.Measured != rec.Sent.Probe+rec.Sent.Warmup+rec.Sent.Measured {
			t.Fatalf("ledger %+v, record %+v", u, rec.Sent)
		}
		if rerunOf(t, r, run) {
			t.Fatalf("%s re-run scheduled", run.Label)
		}
	})
	t.Run("ladder its verdict ended", func(t *testing.T) {
		r, run, code, out := ladderSignalRun(t, syscall.SIGTERM, 2, 0, http.StatusTooManyRequests, 700*time.Millisecond, func(d []time.Duration) time.Duration { return d[0] + 300*time.Millisecond }, nil)
		rec := readCell(t, r, run)
		if code != exitTerminated || rec.Outcome == outcomeInterrupted || rec.Interrupted != nil || rec.Ladder == nil || rec.Ladder.EndStep != 1 {
			t.Fatalf("exit %d, %s %q, ladder %+v\n%s", code, rec.Outcome, rec.Reasons, rec.Ladder, out)
		}
		checkRetained(t, r, run)
	})
	t.Run("failed gate", func(t *testing.T) {
		r, run, code, out := ladderSignalRun(t, syscall.SIGTERM, 1, 0, http.StatusOK, 700*time.Millisecond, func(d []time.Duration) time.Duration { return d[0] + 300*time.Millisecond }, gateShut)
		rec := readCell(t, r, run)
		if code != exitTerminated || rec.Interrupted != nil || !clientGateOnly(rec) {
			t.Fatalf("exit %d, %s %q, interrupted %+v\n%s", code, rec.Outcome, rec.Reasons, rec.Interrupted, out)
		}
		m, _ := checkRetained(t, r, run)
		if m.Release.Outcome != evidence.CellInvalid {
			t.Fatalf("label %+v", m.Release)
		}
		endRecordsRan(t, r, run, m)
		if !strings.Contains(out, "ended on SIGTERM after "+run.Label) {
			t.Fatalf("no end line\n%s", out)
		}
		if !rerunOf(t, r, run) {
			t.Fatalf("no re-run of %s", run.Label)
		}
	})
	t.Run("signal inside the last drain", func(t *testing.T) {
		r, run, code, out := ladderSignalRun(t, syscall.SIGTERM, 1, 0, http.StatusOK, 0, func(d []time.Duration) time.Duration { return d[0] - 500*time.Millisecond }, nil)
		rec := readCell(t, r, run)
		if code != exitTerminated || rec.Outcome != outcomeInterrupted || rec.Interrupted == nil || fileExists(filepath.Join(r.cellDir(run), evidence.ManifestFile)) {
			t.Fatalf("exit %d, %s %q, interrupted %+v\n%s", code, rec.Outcome, rec.Reasons, rec.Interrupted, out)
		}
		if !rerunOf(t, r, run) {
			t.Fatalf("no re-run of %s", run.Label)
		}
	})
}

// A loop that returns before ladderSignalRun's signal is due sends no
// signal: a cell stopped soon after its epoch ends the loop, and no
// SIGTERM arrives in the time the signal was due.
func TestLadderSignalRunEarlyReturnSendsNoSignal(t *testing.T) {
	stopSoon := func(r *route) {
		cell := r.cell
		r.cell = func(ctx context.Context, run schedule.Run) (*cellRecord, error) {
			ctx, cancel := context.WithCancelCause(ctx)
			defer cancel(nil)
			tm := time.AfterFunc(1500*time.Millisecond, func() { cancel(&routeStop{"injected stop"}) })
			defer tm.Stop()
			return cell(ctx, run)
		}
	}
	r, run, code, out := ladderSignalRun(t, syscall.SIGTERM, 1, 0, http.StatusOK, 0, func(d []time.Duration) time.Duration { return d[0] - 500*time.Millisecond }, stopSoon)
	if code != exitStopped {
		t.Fatalf("exit %d\n%s", code, out)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)
	due := time.Duration(loadgen.BuildLadder(cellConfig(r.cfg, run.Seed, r.steps)).Steps[0].DrainEndNs)
	select {
	case <-sig:
		t.Fatal("SIGTERM sent after the loop returned")
	case <-time.After(due):
	}
}

// A signal before a cell's end leaves its end status and balance
// commands to run to their end: SIGINT or SIGHUP while the probe is
// answered stops the route with its abort marker, and SIGTERM during the
// end status of a cell the probe blocked exits 5 with no hold.
func TestSignalLeavesEndCommandsToRun(t *testing.T) {
	kill := func(sig syscall.Signal) {
		if err := syscall.Kill(os.Getpid(), sig); err != nil {
			t.Error(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, tc := range []struct {
		name    string
		sig     syscall.Signal
		probe   int
		inEnd   bool
		code    int
		outcome string
		markers int
	}{
		{"SIGINT at the probe", syscall.SIGINT, http.StatusOK, false, exitStopped, outcomeStopped, 1},
		{"SIGHUP at the probe", syscall.SIGHUP, http.StatusOK, false, exitStopped, outcomeStopped, 1},
		{"SIGTERM in the end status", syscall.SIGTERM, http.StatusNotFound, true, exitTerminated, outcomeBlocked, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) {
				if !tc.inEnd {
					kill(tc.sig)
				}
				return tc.probe, ""
			})
			r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
			shortLadder(r)
			r.ackDir = t.TempDir()
			if tc.inEnd {
				calls := filepath.Join(t.TempDir(), "calls")
				script := fmt.Sprintf(`n=$(cat "$0" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$0"; if [ $n -ge 2 ]; then kill -TERM %d; sleep 0.3; fi; exec "$@"`, os.Getpid())
				r.status = append([]string{"/bin/sh", "-c", script, calls}, r.status...)
			}
			run := r.runs[0]
			shiftClock(r, run.Start)
			ctx, stop := operatorContext(sigtermCause("run"))
			var (
				code int
				err  error
			)
			out := stdoutOf(t, func() { code, err = r.loop(ctx) })
			stop()
			rec := readCell(t, r, run)
			if err != nil || code != tc.code || rec.Outcome != tc.outcome || e.hits.Load() != 1 || len(markers(t, r)) != tc.markers {
				t.Fatalf("exit %d, %v, %s %q, %d requests, markers %v\n%s", code, err, rec.Outcome, rec.Reasons, e.hits.Load(), markers(t, r), out)
			}
			m, _ := checkRetained(t, r, run)
			endRecordsRan(t, r, run, m)
			st, err := r.state()
			if err != nil {
				t.Fatal(err)
			}
			if holds, err := r.holds(st); err != nil || (len(holds) != 0) != (tc.code == exitStopped) {
				t.Fatalf("holds %q: %v", holds, err)
			}
		})
	}
}
