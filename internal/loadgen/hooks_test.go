package loadgen

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// A stop while a spawned worker waits for its intended time leaves its
// request unsent: request 1's dispatch arms the stop 5 ms before request
// 2's intended time, inside the lead its worker is spawned with.
func TestStopDuringWorkerWait(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
	cfg := ladderRunCfg(srv.URL, 2)
	schedule := BuildSchedule(cfg)
	stop := errors.New("operator stop")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	armed := make(chan *time.Timer, 1)
	defer func() {
		select {
		case tm := <-armed:
			tm.Stop()
		default:
		}
	}()
	var epoch time.Time
	hooks := &Hooks{
		OnEpoch: func(e time.Time) { epoch = e },
		OnDispatch: func(r *Request) {
			if r.Index == 1 {
				at := epoch.Add(time.Duration(schedule[2]) - 5*time.Millisecond)
				armed <- time.AfterFunc(time.Until(at), func() { cancel(stop) })
			}
		},
	}
	var mu sync.Mutex
	res, err := run(ctx, cfg, hooks, recordGates(gateCall{}, &mu, new([]gateCall)))
	checkStopped(t, res, err, stop, &hits, -1)
}

// A stop while the pacer is behind schedule leaves the overdue requests
// unsent: the stop lands 1.2 s after the epoch, when the requests intended
// at 0, 0.5 and 1 s are due and none has been spawned.
func TestStopWhilePacerBehind(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
	stop := errors.New("operator stop")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	hooks := &Hooks{OnEpoch: func(time.Time) {
		time.Sleep(1200 * time.Millisecond)
		cancel(stop)
	}}
	var mu sync.Mutex
	res, err := run(ctx, ladderRunCfg(srv.URL, 2), hooks, recordGates(gateCall{}, &mu, new([]gateCall)))
	checkStopped(t, res, err, stop, &hits, 0)
}

// Once the run's stop is closed, execute sends nothing and leaves the
// request without a dispatch time.
func TestExecuteAfterStop(t *testing.T) {
	t.Parallel()
	var hits, dispatched atomic.Int64
	srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
	stop := make(chan struct{})
	close(stop)
	g := &gen{cfg: ladderRunCfg(srv.URL, 1), client: srv.Client(), epoch: time.Now(), filler: "x", model: "percentes-mock",
		stop: stop, onDispatch: func(*Request) { dispatched.Add(1) }}
	r := &Request{}
	if sent := g.execute(r); sent || r.DispatchNs != 0 || r.Outcome != "" || hits.Load() != 0 || dispatched.Load() != 0 {
		t.Fatalf("after the stop: sent %t, request %+v, server saw %d, OnDispatch saw %d", sent, r, hits.Load(), dispatched.Load())
	}
}

// checkStopped checks a run stopped by cause: no request carries a
// dispatch time after the stop, and the server saw exactly the requests
// dispatched, sent of them (-1 = any number).
func checkStopped(t *testing.T, res *Result, err, cause error, hits *atomic.Int64, sent int) {
	t.Helper()
	if err == nil || res == nil || res.Interrupted == nil || res.Interrupted.Cause != cause.Error() {
		t.Fatalf("run returned %v, %+v", err, res)
	}
	in := res.Interrupted
	for i := range res.Requests {
		if d := res.Requests[i].DispatchNs; d > in.StopNs {
			t.Errorf("request %d dispatched at %d ns, after the stop at %d ns", i, d, in.StopNs)
		}
	}
	if n := hits.Load(); n != int64(in.Dispatched) || (sent >= 0 && in.Dispatched != sent) || in.Unsent == 0 {
		t.Fatalf("server saw %d requests; interruption %+v, want %d sent", n, in, sent)
	}
}
