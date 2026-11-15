package loadgen

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// cancelAtVerdict passes every evaluation and cancels ctx with cause at
// the nth call, 1-based.
func cancelAtVerdict(n int, cancel context.CancelCauseFunc, cause error) gateFunc {
	var calls atomic.Int64
	return func(*config.Config, []Request, []cpuSample, float64, float64, int64, int64) GateReport {
		if calls.Add(1) == int64(n) {
			cancel(cause)
		}
		return GateReport{Pass: true}
	}
}

// A context ended after the last step's verdict leaves a ladder that ran
// to its end complete: no interruption and no error. One ended at an
// earlier step's verdict cuts the ladder at the next step, recorded as
// interrupted with that step's requests unsent.
func TestLadderCancelAfterVerdict(t *testing.T) {
	t.Parallel()
	cause := errors.New("SIGTERM")
	for _, tc := range []struct {
		name  string
		steps []float64
		cut   bool
	}{
		{"after the last step's verdict", []float64{2}, false},
		{"after the first of two steps' verdicts", []float64{2, 4}, true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var hits atomic.Int64
			srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			res, err := run(ctx, ladderRunCfg(srv.URL, tc.steps...), nil, cancelAtVerdict(1, cancel, cause))
			if ctx.Err() == nil {
				t.Fatal("context not cancelled")
			}
			lr := res.Ladder
			if !tc.cut {
				if err != nil || res.Interrupted != nil || lr.Steps[0].Gates == nil || hits.Load() != 6 {
					t.Fatalf("err %v, interrupted %+v, step %+v, %d requests", err, res.Interrupted, lr.Steps[0], hits.Load())
				}
				return
			}
			in := res.Interrupted
			if !errors.Is(err, context.Canceled) || in == nil || in.Cause != cause.Error() || in.Unsent != 8 || in.Dispatched != 6 ||
				lr.Steps[0].Gates == nil || !lr.Steps[1].Attempted || lr.Steps[1].Gates != nil {
				t.Fatalf("err %v, interrupted %+v, steps %+v", err, in, lr.Steps)
			}
		})
	}
}
