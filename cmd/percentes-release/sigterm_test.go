package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
)

// terminatedAfter ends a context on SIGTERM's cause after d.
func terminatedAfter(d time.Duration) context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	time.AfterFunc(d, func() { cancel(errTerminated) })
	return ctx
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
