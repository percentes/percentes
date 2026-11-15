package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/schedule"
)

// SPEC-HOSTED §4: a ladder whose epoch falls past the start tolerance,
// though the cell's own check before it passed, is missed with nothing
// sent after the probe.
func TestLateEpochMissesCell(t *testing.T) {
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005032")
	shortLadder(r)
	var sample time.Time
	r.due = func(schedule.Run) time.Time {
		sample = time.Now()
		return sample.Add(-schedule.StartTolerance)
	}
	r.now = func() time.Time {
		if !sample.IsZero() {
			at := sample
			sample = time.Time{}
			time.Sleep(20 * time.Millisecond)
			return at
		}
		return time.Now()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onEpoch = func(time.Time) { cancel() }
	rec, err := r.runCell(ctx, r.runs[0])
	if err != nil {
		t.Fatal(err)
	}
	if rec.Outcome != outcomeMissed || rec.EpochUTC != nil || e.hits.Load() != 1 || len(rec.Reasons) != 1 || !strings.Contains(rec.Reasons[0], "start tolerance") {
		t.Fatalf("outcome %s, reasons %q, ladder epoch set %t, provider requests %d", rec.Outcome, rec.Reasons, rec.EpochUTC != nil, e.hits.Load())
	}
}
