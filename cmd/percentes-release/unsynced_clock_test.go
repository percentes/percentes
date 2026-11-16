package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
)

// stubChronyc is a chronyc tracking command running body as a shell
// script.
func stubChronyc(t *testing.T, body string) evidence.ClockCommand {
	t.Helper()
	p := filepath.Join(t.TempDir(), "chronyc")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	c, err := evidence.ParseClockCommand(p + " tracking")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const unsyncedTracking = "cat <<'X'\nReference ID    : 00000000 ()\nStratum         : 0\nSystem time     : 0.000000000 seconds fast of NTP time\nLeap status     : Not synchronised\nX\n"

// A cell whose clock gives no reading is blocked before admission:
// nothing is admitted or sent, its evidence is retained with the clock's
// flag, no hold or marker is written, and the loop goes on to the next
// run.
func TestUnsyncedClockBlocksCell(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"not synchronised", unsyncedTracking},
		{"daemon not answering", "echo '506 Cannot talk to daemon' >&2; exit 1\n"},
		{"local reference", "cat <<'X'\nReference ID    : 7F7F0101 ()\nStratum         : 10\nSystem time     : 0.000000000 seconds fast of NTP time\nLeap status     : Normal\nX\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, always200)
			r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
			shortLadder(r)
			r.ackDir = t.TempDir()
			r.clock = stubChronyc(t, tc.body)
			run := r.runs[0]
			shiftClock(r, run.Start)
			ctx, cancel := context.WithCancelCause(context.Background())
			wait := r.wait
			var waits []time.Time
			r.wait = func(ctx context.Context, at time.Time) error {
				if waits = append(waits, at); len(waits) == 1 {
					return wait(ctx, at)
				}
				cancel(errTerminated)
				return errTerminated
			}
			var (
				code int
				err  error
			)
			out := stdoutOf(t, func() { code, err = r.loop(ctx) })
			rec := readCell(t, r, run)
			if err != nil || code != exitTerminated || rec.Outcome != outcomeBlocked || len(rec.Reasons) != 1 ||
				!strings.HasPrefix(rec.Reasons[0], "clock offset: no reading from chronyc: ") || strings.Contains(out, "held") {
				t.Fatalf("exit %d, %v, %s %q\n%s", code, err, rec.Outcome, rec.Reasons, out)
			}
			if n := rec.Sent.Probe + rec.Sent.Warmup + rec.Sent.Measured; n != 0 || e.hits.Load() != 0 {
				t.Fatalf("sent %+v, %d requests", rec.Sent, e.hits.Load())
			}
			if len(waits) != 2 || !waits[1].After(run.Start) {
				t.Fatalf("waits %v after %s", waits, run.Start)
			}
			if m, _ := checkRetained(t, r, run); m.Release.Outcome != evidence.CellBlocked {
				t.Fatalf("label %+v", m.Release)
			}
			if u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]; u != nil && (u.Reserved != 0 || u.Probe+u.Warmup+u.Measured != 0) {
				t.Fatalf("ledger %+v", u)
			}
			if rerunOf(t, r, run) != r.hasAttempt2(run.Label) {
				t.Fatalf("%s: re-run scheduled %t", run.Label, !r.hasAttempt2(run.Label))
			}
		})
	}
}

// A real SIGTERM while the clock is read interrupts the cell before
// admission with nothing admitted or sent, exit 5 and no marker; the next
// start finds no hold and the cell to label.
func TestSigtermDuringClockReadInterruptsCell(t *testing.T) {
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	r.ackDir = t.TempDir()
	r.clock = stubChronyc(t, fmt.Sprintf("kill -TERM %d\nexec sleep 5\n", os.Getpid()))
	run := r.runs[0]
	shiftClock(r, run.Start)
	before, err := os.ReadFile(r.ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := operatorContext(sigtermCause("run"))
	var code int
	out := stdoutOf(t, func() { code, err = r.loop(ctx) })
	stop()
	rec := readCell(t, r, run)
	if err != nil || code != exitTerminated || rec.Outcome != outcomeInterrupted || len(rec.Reasons) != 1 || rec.Reasons[0] != terminatedReason || e.hits.Load() != 0 {
		t.Fatalf("exit %d, %v, %s %q, %d requests\n%s", code, err, rec.Outcome, rec.Reasons, e.hits.Load(), out)
	}
	if after, err := os.ReadFile(r.ledgerPath); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("ledger changed: %v", err)
	}
	if rerunOf(t, r, run) != r.hasAttempt2(run.Label) {
		t.Fatalf("%s: re-run scheduled %t", run.Label, !r.hasAttempt2(run.Label))
	}
	st, err := r.state()
	if err != nil || len(st.unlabelled) != 1 || st.unlabelled[0].Run != run {
		t.Fatalf("unlabelled %+v: %v", st.unlabelled, err)
	}
}
