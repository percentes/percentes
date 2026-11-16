package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
