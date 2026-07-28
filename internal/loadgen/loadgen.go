// Package loadgen is the open-loop, coordinated-omission-correct,
// streaming-aware load generator (SPEC.md §2; safety-critical).
//
// Design invariants:
//   - The intended dispatch schedule is fixed before the run; the pacer is
//     a pure function of the monotonic clock and never inspects system
//     state, queue depth, or in-flight count.
//   - Latency is re-based to intended dispatch time t_i and recorded via
//     plain HdrHistogram recordValue() only (in the collector); the
//     correction APIs are forbidden (lint-enforced).
//   - Concurrency is provisioned above lambda x worst-case latency (the
//     pinned 30 s timeout bounds request lifetime, so max in-flight is
//     lambda x 30; connections are provisioned at 2x that) and nothing on
//     the dispatch path blocks: a request that cannot reuse an idle
//     connection dials a new one, and a dial failure fails that request.
//   - No retries anywhere. Go's transport never silently retries a POST
//     (non-idempotent, not replayable); reconnect-on-error is the pool
//     discarding broken connections for subsequent requests.
//   - Monotonic clock only: all timestamps are offsets from the run epoch
//     measured with time.Since.
package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// Outcome is the §3 three-state terminal classification.
type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
	OutcomeErrored   Outcome = "errored"
	OutcomeCensored  Outcome = "censored"
)

// Error classes within OutcomeErrored.
const (
	ErrHTTPStatus      = "http_status"
	ErrReset           = "reset"
	ErrMalformedStream = "malformed_stream"
	ErrConnect         = "connect"
)

// Request is one scheduled request's full lifecycle. All times are
// nanosecond offsets from the run epoch on the monotonic clock; zero
// means "never happened".
type Request struct {
	Index      int64   `json:"index"`
	IntendedNs int64   `json:"intended_ns"`
	DispatchNs int64   `json:"dispatch_ns"`
	FirstTokNs int64   `json:"first_token_ns"`
	DoneNs     int64   `json:"done_ns"`
	Outcome    Outcome `json:"outcome"`
	ErrClass   string  `json:"err_class,omitempty"`
	Tokens     int     `json:"tokens"`
	Replica    string  `json:"replica,omitempty"`

	// ITLsUs are inter-token gaps (us) for pooled per-window ITL
	// histograms (§3: per-request p99 is forbidden; pooling happens in
	// the collector). Excluded from JSON records for size; the pooled
	// summaries appear in the report.
	ITLsUs []int64 `json:"-"`
}

// TTFTNs and E2ENs are re-based to intended dispatch time. Valid only for
// completed requests (enforced by the collector).
func (r *Request) TTFTNs() int64 { return r.FirstTokNs - r.IntendedNs }
func (r *Request) E2ENs() int64  { return r.DoneNs - r.IntendedNs }

// Result is the client-side record of one run: the authoritative stream
// for latency, errors, and censoring (§2).
type Result struct {
	EpochWall time.Time  `json:"epoch_wall"`
	Requests  []Request  `json:"requests"`
	Gates     GateReport `json:"gates"`

	// Run-relative phase boundaries (ns), for window alignment.
	WarmupEndNs   int64 `json:"warmup_end_ns"`
	BaselineEndNs int64 `json:"baseline_end_ns"`
	FaultEndNs    int64 `json:"fault_end_ns"`
	RunEndNs      int64 `json:"run_end_ns"`
	// TInjectNs is the planned fault offset in run time (warmup + §
	// fault.t_inject_offset_s).
	TInjectNs int64 `json:"t_inject_ns"`
}

type gen struct {
	cfg    *config.Config
	client *http.Client
	epoch  time.Time
	filler string
}

// now returns nanoseconds since the run epoch (monotonic).
func (g *gen) now() int64 { return time.Since(g.epoch).Nanoseconds() }

// Hooks are optional run-lifecycle callbacks.
type Hooks struct {
	// OnEpoch fires once the run epoch is anchored, before the first
	// dispatch — the orchestrator uses it to pre-arm T_inject.
	OnEpoch func(epoch time.Time)
}

// Run executes the full scheduled load against cfg.Target.BaseURL.
// It returns when every scheduled request has reached a terminal state
// (which can be up to the 30 s timeout after the last dispatch).
func Run(ctx context.Context, cfg *config.Config, hooks *Hooks) (*Result, error) {
	schedule := BuildSchedule(cfg)
	if len(schedule) == 0 {
		return nil, fmt.Errorf("loadgen: empty schedule")
	}

	// Provision connections above lambda x worst-case latency: lifetime
	// is bounded by the pinned 30 s timeout, so 2x lambda x timeout can
	// absorb every request blackholing simultaneously.
	provision := int(cfg.Load.RateRPS*float64(config.PinnedClientTimeoutS)*2) + cfg.Load.Connections
	transport := &http.Transport{
		ForceAttemptHTTP2:   false, // N independent HTTP/1.1 connections (§1)
		MaxIdleConns:        provision,
		MaxIdleConnsPerHost: provision,
		MaxConnsPerHost:     0, // never throttle dispatch on connection count
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	g := &gen{
		cfg:    cfg,
		client: &http.Client{Transport: transport},
		filler: buildFiller(cfg.Load.InputLengthTokens),
	}

	p := cfg.Run.Phases
	res := &Result{
		WarmupEndNs:   int64(p.WarmupS * 1e9),
		BaselineEndNs: int64((p.WarmupS + p.BaselineS) * 1e9),
		FaultEndNs:    int64((p.WarmupS + p.BaselineS + p.FaultWindowTimeoutS) * 1e9),
		RunEndNs:      int64((p.WarmupS + p.BaselineS + p.FaultWindowTimeoutS + p.CooldownS) * 1e9),
		TInjectNs:     int64((p.WarmupS + cfg.Fault.TInjectOffsetS) * 1e9),
	}

	requests := make([]Request, len(schedule))
	for i, off := range schedule {
		requests[i] = Request{Index: int64(i), IntendedNs: off}
	}

	// Epoch first, then gate monitors (their timers are epoch-relative).
	// The measurement span is [warmup end, cooldown start].
	g.epoch = time.Now()
	res.EpochWall = g.epoch
	if hooks != nil && hooks.OnEpoch != nil {
		hooks.OnEpoch(g.epoch)
	}
	cpuMon := startCPUMonitor()
	gcMon := startGCMonitor(res.WarmupEndNs, res.FaultEndNs)

	// Pacer: time-driven only. It spawns each worker a small lead ahead
	// of t_i and the worker performs its own final precise wait, so the
	// workers' waits run in parallel: one late pacer wakeup (scheduler
	// hiccup, GC) cannot serialize into the send skew of every subsequent
	// request. Dispatch remains a pure function of the clock.
	const spawnLeadNs = int64(10 * time.Millisecond)
	var wg sync.WaitGroup
	var aborted bool
	for i := range requests {
		delta := requests[i].IntendedNs - spawnLeadNs - g.now()
		if delta > 0 {
			timer := time.NewTimer(time.Duration(delta))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				aborted = true
			}
		}
		if aborted {
			break // remaining requests stay undispatched -> gate fails
		}
		wg.Add(1)
		go func(r *Request) {
			defer wg.Done()
			// Final precise wait: timer to within ~1.5 ms of t_i, then a
			// short spin — dispatch precision must not depend on runtime
			// timer wakeup latency under read-burst load. Cost: ~1.5 ms of
			// one core per request at 20 rps ≈ 3% of one core.
			const spinNs = int64(1_500_000)
			if wait := r.IntendedNs - spinNs - g.now(); wait > 0 {
				waitTimer := time.NewTimer(time.Duration(wait))
				<-waitTimer.C
			}
			for g.now() < r.IntendedNs {
			}
			g.execute(r)
		}(&requests[i])
	}
	wg.Wait()

	cpuSamples := cpuMon.stopAndCollect(g)
	gcP99Ms := gcMon.stopAndP99Ms()
	res.Requests = requests
	res.Gates = evaluateGates(cfg, requests, cpuSamples, gcP99Ms, res.WarmupEndNs, res.FaultEndNs)
	if aborted {
		return res, fmt.Errorf("loadgen: run aborted by context: %w", ctx.Err())
	}
	return res, nil
}

func buildFiller(tokens int) string {
	return strings.Repeat("tok ", tokens)
}
