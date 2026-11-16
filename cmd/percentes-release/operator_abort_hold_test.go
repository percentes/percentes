package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
)

// abortedContext is a context ended with an operator abort.
func abortedContext() context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(loadgen.ErrOperatorAbort)
	return ctx
}

// slowCommand runs args after two seconds.
func slowCommand(args []string) []string {
	return append([]string{"/bin/sh", "-c", `sleep 2; exec "$@"`, "sh"}, args...)
}

// abortAfter ends a context with an operator abort after d.
func abortAfter(d time.Duration) context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	time.AfterFunc(d, func() { cancel(loadgen.ErrOperatorAbort) })
	return ctx
}

// markers reads the route's abort and stop markers.
func markers(t *testing.T, r *route) []abortMarker {
	t.Helper()
	var out []abortMarker
	for _, prefix := range []string{abortPrefix, stopPrefix} {
		paths, err := filepath.Glob(filepath.Join(r.dir, prefix+"*.json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			var m abortMarker
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			out = append(out, m)
		}
	}
	return out
}

// shiftClock runs r's clock and waits at the real clock moved to at.
func shiftClock(r *route, at time.Time) {
	shift := time.Until(at)
	r.now = func() time.Time { return time.Now().Add(shift) }
	r.wait = func(ctx context.Context, t time.Time) error { return waitUntil(ctx, t.Add(-shift)) }
}

// A wait under an ended context returns its cause, even for a time
// already passed.
func TestWaitUntilEndedReturnsCause(t *testing.T) {
	for _, at := range []time.Time{time.Now().Add(-time.Minute), time.Now().Add(time.Hour)} {
		if err := waitUntil(abortedContext(), at); !errors.Is(err, loadgen.ErrOperatorAbort) {
			t.Fatalf("wait until %s: %v", at, err)
		}
	}
}

// An operator abort during the refresh or balance command stops the cell
// before admission, retained with its own label, with nothing sent or
// admitted.
func TestAbortDuringStartChecksRetainsCell(t *testing.T) {
	for _, slow := range []string{"refresh", "balance"} {
		e := newEndpoint(t, always200)
		r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
		shortLadder(r)
		if slow == "refresh" {
			r.refresh = slowCommand(r.refresh)
		} else {
			r.balance = slowCommand(r.balance)
		}
		before, err := os.ReadFile(r.ledgerPath)
		if err != nil {
			t.Fatal(err)
		}
		run := r.runs[0]
		rec, err := r.runCell(abortAfter(500*time.Millisecond), run)
		if err != nil || rec.Outcome != outcomeStopped || !slices.Equal(rec.Reasons, []string{"stopped before admission: operator abort"}) {
			t.Fatalf("%s: %+v, %v", slow, rec, err)
		}
		if m, _ := checkRetained(t, r, run); m.Release.Outcome != outcomeStopped {
			t.Fatalf("%s: manifest %+v", slow, m.Release)
		}
		after, err := os.ReadFile(r.ledgerPath)
		if err != nil || !bytes.Equal(before, after) || e.hits.Load() != 0 {
			t.Fatalf("%s: ledger changed %t, %d requests: %v", slow, !bytes.Equal(before, after), e.hits.Load(), err)
		}
	}
}

// In the loop, an abort during a cell's start checks holds the route
// under that cell's stop and a marker naming it; an abort before the
// loop's first wait, with that run's start passed inside the tolerance,
// runs no cell and holds the route before it.
func TestLoopAbortBeforeAdmissionHoldsRoute(t *testing.T) {
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	r.ackDir = t.TempDir()
	run := r.runs[0]
	shiftClock(r, run.Start)
	r.refresh = slowCommand(r.refresh)
	if code, err := r.loop(abortAfter(500 * time.Millisecond)); err != nil || code != exitStopped || e.hits.Load() != 0 {
		t.Fatalf("loop: exit %d, %v, %d requests", code, err, e.hits.Load())
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	holds, err := r.holds(st)
	if err != nil || len(holds) != 2 || !strings.Contains(holds[0], run.Label+" attempt 1 stopped: [stopped before admission: operator abort]") ||
		!strings.Contains(holds[1], "during "+run.Label+" attempt 1: operator abort") {
		t.Fatalf("holds %q: %v", holds, err)
	}

	e2 := newEndpoint(t, always200)
	r2, _ := newTestRoute(t, releaseConfig(t, e2.url()), "+0.005")
	r2.ackDir = t.TempDir()
	run = r2.runs[0]
	shiftClock(r2, run.Start.Add(time.Minute))
	if code, err := r2.loop(abortedContext()); err != nil || code != exitStopped || e2.hits.Load() != 0 || fileExists(r2.cellDir(run)) {
		t.Fatalf("aborted before the loop: exit %d, %v, %d requests", code, err, e2.hits.Load())
	}
	if m := markers(t, r2); len(m) != 1 || m[0].Run != run || m[0].During {
		t.Fatalf("markers %+v", m)
	}
}

// An abort that finds no run left stops the loop before the window
// close, with no marker.
func TestAbortBeforeWindowCloseWritesNoMarker(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	r.ackDir = t.TempDir()
	if code, err := r.loop(context.Background()); err != nil || code != exitOK {
		t.Fatalf("loop: %d, %v", code, err)
	}
	closedAfter = nil
	if code, err := r.loop(abortedContext()); err != nil || code != exitStopped || closedAfter != nil || len(markers(t, r)) != 0 {
		t.Fatalf("aborted at the window close: %d, %v, window close after %v", code, err, closedAfter)
	}
}

// hangingEndpoint answers its first request 200 and status(n) for each
// later one, a 0 holding the request until its client gives up.
func hangingEndpoint(t *testing.T, status func(n int64) int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	hits := new(atomic.Int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		io.ReadAll(req.Body) //nolint:errcheck
		n := hits.Add(1)
		s := http.StatusOK
		if n > 1 {
			s = status(n)
		}
		switch s {
		case 0:
			<-req.Context().Done()
		case http.StatusOK:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, chatStream) //nolint:errcheck
		default:
			w.WriteHeader(s)
			io.WriteString(w, "no credit") //nolint:errcheck
		}
	}))
	t.Cleanup(srv.Close)
	return srv, hits
}

// snapshot copies the route directory's cell records and markers; it
// may run off the test's goroutine.
func snapshot(t *testing.T, r *route) map[string][]byte {
	out := map[string][]byte{}
	for _, pattern := range []string{filepath.Join(r.dir, "*", cellFile), filepath.Join(r.dir, abortPrefix+"*.json"), filepath.Join(r.dir, stopPrefix+"*.json")} {
		paths, _ := filepath.Glob(pattern)
		for _, p := range paths {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Error(err)
			}
			out[p] = b
		}
	}
	return out
}

// restore leaves the route directory as snap, as a process killed when
// snap was taken would.
func restore(t *testing.T, r *route, snap map[string][]byte) {
	t.Helper()
	if err := os.RemoveAll(r.dir); err != nil {
		t.Fatal(err)
	}
	for p, b := range snap {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// An operator abort mid-ladder, with requests left hanging to the client
// timeout, holds the route from the signal: the route directory a second
// later restarts held.
func TestAbortMidLadderHoldsRoute(t *testing.T) {
	srv, _ := hangingEndpoint(t, func(int64) int { return 0 })
	r, _ := newTestRoute(t, releaseConfig(t, srv.URL+"/openai/v1/chat/completions"), "+0.005")
	shortLadder(r)
	r.cfg.Client.HTTPTimeoutS = 2
	r.ackDir = t.TempDir()
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	r.wait = noWait
	ctx, cancel := context.WithCancelCause(context.Background())
	snap := make(chan map[string][]byte, 1)
	r.onEpoch = func(time.Time) {
		go func() {
			time.Sleep(time.Second)
			cancel(loadgen.ErrOperatorAbort)
			time.Sleep(time.Second)
			snap <- snapshot(t, r)
		}()
	}
	if code, err := r.loop(ctx); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	restore(t, r, <-snap)
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) == 0 {
		t.Fatalf("holds %q: %v", holds, err)
	}
	if m := markers(t, r); len(m) != 1 || m[0].Run != run || !m[0].During {
		t.Fatalf("markers %+v", m)
	}
}

// An abort while a counted cell's evidence is written, or after an
// exhausted client ended its ladder, holds the route from the signal.
func TestAbortAfterLadderHoldsRoute(t *testing.T) {
	for _, outcome := range []string{outcomeCounted, outcomeInvalid} {
		var ran []schedule.Run
		r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
		r.ackDir = t.TempDir()
		start := r.runs[0].Start
		r.now = func() time.Time { return start }
		r.wait = noWait
		ctx, cancel := context.WithCancelCause(context.Background())
		var snap map[string][]byte
		r.cell = func(ctx context.Context, run schedule.Run) (*cellRecord, error) {
			rec := r.newRecord(run)
			rec.Outcome = outcomeRunning
			if err := r.writeNew(run, rec); err != nil {
				return nil, err
			}
			if outcome == outcomeInvalid {
				cellCtx, stop := context.WithCancelCause(ctx)
				stop(errEMFILE)
				<-cellCtx.Done()
			}
			cancel(loadgen.ErrOperatorAbort)
			for deadline := time.Now().Add(2 * time.Second); len(markers(t, r)) == 0 && time.Now().Before(deadline); {
				time.Sleep(10 * time.Millisecond)
			}
			snap = snapshot(t, r)
			rec.Outcome = outcome
			return rec, replaceJSON(filepath.Join(r.cellDir(run), cellFile), rec)
		}
		if code, err := r.loop(ctx); err != nil || code != exitStopped {
			t.Fatalf("%s: loop: exit %d, %v", outcome, code, err)
		}
		restore(t, r, snap)
		st, err := r.state()
		if err != nil {
			t.Fatal(err)
		}
		if holds, err := r.holds(st); err != nil || len(holds) != 1 || !strings.Contains(holds[0], "during "+r.runs[0].Label+" attempt 1") {
			t.Fatalf("%s: holds %q: %v", outcome, holds, err)
		}
	}
}

// A context ended with no cause at the window's end writes no marker.
func TestCancelWithoutCauseWritesNoMarker(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	ctx, cancel := context.WithCancelCause(context.Background())
	r.close = func(context.Context, *routeState) error {
		cancel(nil)
		return nil
	}
	if code, err := r.loop(ctx); err != nil || code != exitOK || len(markers(t, r)) != 0 {
		t.Fatalf("loop: %d, %v, markers %+v", code, err, markers(t, r))
	}
}

// An operator abort during a cell that ends with an execution error
// stops the route; a marker the signal failed to write is written once
// more as the cell ends.
func TestAbortMarkerRewrittenAtCellEnd(t *testing.T) {
	for _, unblock := range []bool{true, false} {
		r, _ := billingStopRoute(t)
		r.ackDir = t.TempDir()
		r.wait = noWait
		run := r.runs[0]
		ctx, cancel := context.WithCancelCause(context.Background())
		r.cell = func(_ context.Context, run schedule.Run) (*cellRecord, error) {
			rec := r.newRecord(run)
			rec.Outcome = outcomeInterrupted
			if err := r.writeNew(run, rec); err != nil {
				return nil, err
			}
			if err := os.Chmod(r.dir, 0o555); err != nil {
				return nil, err
			}
			cancel(loadgen.ErrOperatorAbort)
			time.Sleep(100 * time.Millisecond)
			if unblock {
				if err := os.Chmod(r.dir, 0o755); err != nil {
					return nil, err
				}
			}
			return rec, errors.New("disk write failed")
		}
		code, err := r.loop(ctx)
		os.Chmod(r.dir, 0o755) //nolint:errcheck
		if err != nil || code != exitStopped {
			t.Fatalf("unblocked %t: exit %d, %v", unblock, code, err)
		}
		m := markers(t, r)
		if unblock && (len(m) != 1 || m[0].Run != run || !m[0].During) || !unblock && len(m) != 0 {
			t.Fatalf("unblocked %t: markers %+v", unblock, m)
		}
	}
}

// An abort after a cell returns, before the loop names the next run,
// names no run: the cell that ended writes no marker.
func TestAbortAfterCellNamesNoRun(t *testing.T) {
	r, _ := billingStopRoute(t)
	run := r.runs[0]
	hold := &abortHold{r: r}
	hold.at(&run, true)
	ctx, cancel := context.WithCancelCause(context.Background())
	stop := hold.watch(ctx)
	rec := r.newRecord(run)
	rec.Outcome = outcomeCounted
	if code, done, err := r.cellEnded(ctx, hold, run, rec, nil); done || err != nil {
		t.Fatalf("cell ended: exit %d, done %t, %v", code, done, err)
	}
	cancel(loadgen.ErrOperatorAbort)
	stop()
	if m := markers(t, r); len(m) != 0 {
		t.Fatalf("markers %+v", m)
	}
}
