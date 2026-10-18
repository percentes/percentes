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

// OnDispatch sees every sent request once, with its dispatch time set,
// before OnDone sees it terminal.
func TestOnDispatchSeesEverySend(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
	var mu sync.Mutex
	sent := map[int64]int64{}
	hooks := &Hooks{
		OnDispatch: func(r *Request) {
			mu.Lock()
			defer mu.Unlock()
			if _, dup := sent[r.Index]; dup || r.DispatchNs == 0 || r.DoneNs != 0 {
				t.Errorf("request %d: dispatch seen again %t, dispatch_ns %d, done_ns %d", r.Index, dup, r.DispatchNs, r.DoneNs)
			}
			sent[r.Index] = r.DispatchNs
		},
		OnDone: func(r *Request) {
			mu.Lock()
			defer mu.Unlock()
			if _, ok := sent[r.Index]; !ok {
				t.Errorf("request %d terminal before OnDispatch saw it", r.Index)
			}
		},
	}
	res, err := run(context.Background(), ladderRunCfg(srv.URL, 2), hooks, recordGates(gateCall{}, &mu, new([]gateCall)))
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != len(res.Requests) || len(sent) != 6 {
		t.Fatalf("OnDispatch saw %d requests of %d, want 6", len(sent), len(res.Requests))
	}
	for i := range res.Requests {
		if r := &res.Requests[i]; sent[r.Index] != r.DispatchNs {
			t.Fatalf("request %d: OnDispatch saw dispatch_ns %d, the result has %d", r.Index, sent[r.Index], r.DispatchNs)
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
