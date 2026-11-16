package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// cancelAtDispatch ends its base with cause at the probe exchange's first
// trace lookup, after the probe's dispatch time is stamped and before its
// round trip.
type cancelAtDispatch struct {
	context.Context
	cancel func(error)
	cause  error
	fired  bool
}

func (c *cancelAtDispatch) Value(key any) any {
	if !c.fired && fmt.Sprintf("%T", key) == "httptrace.clientEventContextKey" {
		c.fired = true
		c.cancel(c.cause)
	}
	return c.Context.Value(key)
}

// A signal that lands after admission and before the probe reaches a
// connection settles no probe: SIGTERM ends the cell interrupted with
// exit 5 and no hold, SIGINT stops it held with exit 3. In both, the
// endpoint reads nothing, the record, the ledger and the account group's
// history count no probe, and the history shows no gap.
func TestSignalBeforeProbeConnectsSettlesNoProbe(t *testing.T) {
	for _, c := range []struct {
		name    string
		cause   error
		code    int
		outcome string
		held    bool
	}{
		{"SIGTERM", errTerminated, exitTerminated, outcomeInterrupted, false},
		{"SIGINT", loadgen.ErrOperatorAbort, exitStopped, outcomeStopped, true},
	} {
		fastDispatch(t)
		e := newEndpoint(t, always200)
		r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
		shortLadder(r)
		r.ackDir = t.TempDir()
		run := r.runs[0]
		shiftClock(r, run.Start)
		base, cancel := context.WithCancelCause(context.Background())
		ctx := &cancelAtDispatch{Context: base, cancel: cancel, cause: c.cause}
		code, err := r.loop(ctx)
		if err != nil || code != c.code || !ctx.fired {
			t.Fatalf("%s: loop: exit %d, %v, cancelled at dispatch %t", c.name, code, err, ctx.fired)
		}
		rec := readCell(t, r, run)
		u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]
		if rec.Outcome != c.outcome || e.hits.Load() != 0 || rec.Sent.Probe != 0 || u.Probe != 0 || u.Reserved != 0 {
			t.Errorf("%s: outcome %s %q, endpoint %d, record %+v, ledger probe %d reserved %d", c.name, rec.Outcome, rec.Reasons, e.hits.Load(), rec.Sent, u.Probe, u.Reserved)
		}
		st, err := r.state()
		if err != nil {
			t.Fatal(err)
		}
		if holds, err := r.holds(st); err != nil || (len(holds) != 0) != c.held {
			t.Errorf("%s: holds %q, %v", c.name, holds, err)
		}
		entries, _, err := readLedger(r.ledgerPath, "")
		if err != nil {
			t.Fatal(err)
		}
		h, err := readHistory(r.out, r.cfg.Release.AccountGroup, []string{r.cfg.Target.Name}, []*config.Config{r.cfg}, entries, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range h.Sends {
			if s.Source == cellLabel(run) {
				t.Errorf("%s: the history holds the probe as a send: %+v", c.name, s)
			}
		}
		if len(h.Gaps) != 0 {
			t.Errorf("%s: history gaps %+v", c.name, h.Gaps)
		}
	}
}
