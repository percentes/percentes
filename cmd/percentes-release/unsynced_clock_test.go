package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
