package loadgen

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

// OnDone sees every dispatched request once, in its terminal state.
func TestOnDoneSeesEveryRequest(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(n int64) int {
		if n%3 == 0 {
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	})
	var mu sync.Mutex
	seen := map[int64]Outcome{}
	hooks := &Hooks{OnDone: func(r *Request) {
		mu.Lock()
		defer mu.Unlock()
		if _, dup := seen[r.Index]; dup {
			t.Errorf("request %d seen twice", r.Index)
		}
		seen[r.Index] = r.Outcome
	}}
	res, err := run(context.Background(), ladderRunCfg(srv.URL, 2), hooks, recordGates(gateCall{}, &mu, new([]gateCall)))
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != len(res.Requests) || len(seen) != 6 {
		t.Fatalf("OnDone saw %d requests of %d, want 6", len(seen), len(res.Requests))
	}
	for i := range res.Requests {
		r := &res.Requests[i]
		if got := seen[r.Index]; got == "" || got != r.Outcome {
			t.Fatalf("request %d: OnDone saw outcome %q, the result has %q", r.Index, got, r.Outcome)
		}
	}
}

// A stop cancelled from OnDone ends the ladder with its cause recorded,
// and the later requests stay unsent.
func TestOnDoneStopsRun(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(n int64) int {
		if n == 3 {
			return http.StatusPaymentRequired
		}
		return http.StatusOK
	})
	stop := errors.New("route stop: billing refusal")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	hooks := &Hooks{OnDone: func(r *Request) {
		if r.Status == http.StatusPaymentRequired {
			cancel(stop)
		}
	}}
	var mu sync.Mutex
	res, err := run(ctx, ladderRunCfg(srv.URL, 2, 4), hooks, recordGates(gateCall{}, &mu, new([]gateCall)))
	if err == nil || res == nil || res.Interrupted == nil {
		t.Fatalf("run returned %v, %+v", err, res)
	}
	if res.Interrupted.Cause != stop.Error() || res.Interrupted.Unsent == 0 {
		t.Fatalf("interruption %+v", res.Interrupted)
	}
	if n := hits.Load(); n != 3 {
		t.Fatalf("server saw %d requests, want 3", n)
	}
}
