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
//     client timeout bounds request lifetime, so max in-flight is lambda x
//     timeout; connections are provisioned at 2x that) and nothing on
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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	ErrStatusOther     = "status_other"
	ErrReset           = "reset"
	ErrMalformedStream = "malformed_stream"
	ErrConnect         = "connect"
	ErrEmptyStream     = "empty_stream"
	ErrStatus429       = "status_429"
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
	// Step is a release request's 1-based ladder step (0 = no ladder);
	// Warmup marks the warm-up requests sent at the first step's rate.
	// Late marks a completion whose whole answer arrived after the
	// release deadline, counted from intended dispatch.
	Step   int  `json:"step,omitempty"`
	Warmup bool `json:"warmup,omitempty"`
	Late   bool `json:"late,omitempty"`
	// CompletionTokens is the completion token count from a usage object
	// in the stream (a Messages message_delta's output_tokens); UsageSeen
	// says a count arrived.
	CompletionTokens int    `json:"completion_tokens,omitempty"`
	UsageSeen        bool   `json:"usage_seen,omitempty"`
	Replica          string `json:"replica,omitempty"`
	// Usage is the last usage object the stream carried, at any path
	// target.response_shape names; on a Messages target, message_start's
	// usage with each later usage object's non-null fields laid over it.
	Usage json.RawMessage `json:"usage,omitempty"`
	// FinishReason is the last finish_reason of the first choice, or the
	// last Messages stop_reason; ReportedModel is the first model id a
	// chunk reported.
	FinishReason  string `json:"finish_reason,omitempty"`
	ReportedModel string `json:"reported_model,omitempty"`
	// SystemFingerprint is the first top-level system_fingerprint a chunk
	// carried.
	SystemFingerprint string `json:"system_fingerprint,omitempty"`
	// ModelMismatch lists, in arrival order, each distinct model id a
	// chunk reported that differs from the expected one (ExpectedModel),
	// up to modelMismatchLimit ids.
	ModelMismatch []string `json:"model_mismatch,omitempty"`
	// StreamError is the first in-band error object, compacted, with
	// credentials masked and cut at errorBodyLimit bytes.
	StreamError string `json:"stream_error,omitempty"`
	// ReasoningChunks counts chunks carrying reasoning text;
	// FirstReasoningNs is the first one's arrival. Reasoning never counts
	// as a token or stamps FirstTokNs.
	ReasoningChunks  int   `json:"reasoning_chunks,omitempty"`
	FirstReasoningNs int64 `json:"first_reasoning_ns,omitempty"`
	// UnknownEvents counts decodable events carrying no choices, usage
	// or error, and Messages events of a type the adapter does not read.
	UnknownEvents int `json:"unknown_events,omitempty"`
	// Content and Reasoning are the visible answer and the reasoning text,
	// kept for hosted targets only; never in JSON.
	Content   string `json:"-"`
	Reasoning string `json:"-"`
	// Refusal is the text at the refusal paths target.response_shape
	// names, joined in arrival order, kept for hosted targets only; never
	// in JSON. RefusalBytes is its length in bytes, counted for every
	// target.
	Refusal      string `json:"-"`
	RefusalBytes int    `json:"refusal_bytes,omitempty"`
	// Proto is the response's protocol version ("" = no response).
	Proto string `json:"proto,omitempty"`
	// Status is the HTTP status code (0 = no response). Headers holds the
	// response headers target.response_headers allows, by canonical name,
	// with config.RetainedHeaderValue in place of each value of a header
	// target.retained_headers names; RetainedHeaders holds those values
	// for the raw-frame store, never in JSON.
	Status          int                 `json:"status,omitempty"`
	Headers         map[string][]string `json:"headers,omitempty"`
	RetainedHeaders map[string][]string `json:"-"`
	// PeerAddr is the remote address of the connection the request used
	// ("" = none obtained); ConnReused says it came from the idle pool.
	PeerAddr   string `json:"peer_addr,omitempty"`
	ConnReused bool   `json:"conn_reused,omitempty"`
	// ErrText is the redacted Go error string of a transport or stream
	// failure.
	ErrText string `json:"err_text,omitempty"`
	// ErrorBody is the first errorBodyLimit bytes of a non-200 body with
	// credentials masked, held for the raw-frame store; never in JSON.
	ErrorBody []byte `json:"-"`
	// Frames are the timed reads of a 200 body, credentials masked, kept
	// for hosted targets only; never in JSON. FrameBytesDropped counts the
	// body bytes read after the first frameLimit and not kept.
	Frames            []Frame `json:"-"`
	FrameBytesDropped int64   `json:"-"`

	// ITLsUs are inter-token latency (ITL) gaps (us) for pooled per-window ITL
	// histograms (§3: per-request p99 is forbidden; pooling happens in
	// the collector). Excluded from JSON records for size; the pooled
	// summaries appear in the report.
	ITLsUs []int64 `json:"-"`
}

// Frame is one read of a response body: the bytes it returned and its
// return time as a nanosecond offset from the run epoch.
type Frame struct {
	Ns   int64
	Data []byte
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
	// TInjectNs is the planned fault offset in run time (warmup +
	// fault.t_inject_offset_s).
	TInjectNs int64 `json:"t_inject_ns"`

	// RequestedModel is the model id every request sent; ExpectedModel
	// the id each reported one is compared with. ReportedModels lists
	// each distinct id recorded on the requests (reported_model and
	// model_mismatch), in request order. ModelMismatches counts requests
	// with a mismatch; SplitSeries marks a hosted run that had one, which
	// splits the series at this run.
	RequestedModel  string   `json:"requested_model,omitempty"`
	ExpectedModel   string   `json:"expected_model,omitempty"`
	ReportedModels  []string `json:"reported_models,omitempty"`
	ModelMismatches int      `json:"model_mismatches,omitempty"`
	SplitSeries     bool     `json:"split_series,omitempty"`

	// Tripwires is set for a pilot or release run only. Interrupted is
	// set when a safety stop cancelled the run's context.
	Tripwires   *Tripwires    `json:"tripwires,omitempty"`
	Interrupted *Interruption `json:"interrupted,omitempty"`

	// Ladder is set for a release run only.
	Ladder *LadderRecord `json:"ladder,omitempty"`
}

// Tripwires are the local dispatch checks of a pilot (SPEC-HOSTED HA2
// §3) and a release (HA3 point 16). They record and never block or
// delay a dispatch; either one firing makes the window client-invalid.
type Tripwires struct {
	// Dispatches counts every request sent, including a further
	// connection taken by one request. ExtraDispatchNs is the first send
	// beyond the schedule (0 = none): beyond Scheduled for a pilot, beyond
	// the request's own step's scheduled count for a release.
	Scheduled       int   `json:"scheduled"`
	Dispatches      int64 `json:"dispatches"`
	ExtraDispatchNs int64 `json:"extra_dispatch_ns,omitempty"`
	// MaxInFlight is the most requests between dispatch and terminal
	// state at once. OverInFlightNs is the first dispatch that took the
	// count above the limit (0 = none): InFlightLimit for a pilot, the
	// dispatching request's step's entry of InFlightLimits for a release.
	InFlightLimit  int   `json:"in_flight_limit,omitempty"`
	InFlightLimits []int `json:"in_flight_limits,omitempty"`
	MaxInFlight    int64 `json:"max_in_flight"`
	OverInFlightNs int64 `json:"over_in_flight_ns,omitempty"`
	Fired          bool  `json:"fired"`
}

// LadderRecord is a release cell's ladder as run. EndStep is the 1-based
// step at whose drain the ladder ended (0 = every step ran to its drain's
// end) and End the reason, one of the LadderEnd* values.
type LadderRecord struct {
	WarmupScheduled int          `json:"warmup_scheduled"`
	Steps           []StepRecord `json:"steps"`
	EndStep         int          `json:"end_step,omitempty"`
	End             string       `json:"end,omitempty"`
}

// Reasons a ladder ends before its top step (SPEC-HOSTED HA3 point 16).
// The backoff rule leaves the cell counting; the other two make it
// client-invalid.
const (
	LadderEndClientValidity = "client_validity"
	LadderEndTripwire       = "tripwire"
	LadderEndBackoff        = "backoff"
)

// StepRecord is one ladder step. A step not attempted carries its
// schedule only. An attempted step's verdict is taken at its drain's end
// once every request of the step is terminal: Status429 over Scheduled
// is the backoff fraction, Gates the client-validity gate over the
// step's measured window, Tripwire whether either tripwire had fired by
// then. A run stopped by its context leaves the step it was in without a
// verdict.
type StepRecord struct {
	RateRPS       float64     `json:"rate_rps"`
	StartNs       int64       `json:"start_ns"`
	EndNs         int64       `json:"end_ns"`
	DrainEndNs    int64       `json:"drain_end_ns"`
	Scheduled     int         `json:"scheduled"`
	InFlightLimit int         `json:"in_flight_limit"`
	Attempted     bool        `json:"attempted"`
	Status429     int         `json:"status_429"`
	Gates         *GateReport `json:"gates,omitempty"`
	Tripwire      bool        `json:"tripwire,omitempty"`
}

// Safety stops: the causes a caller cancels Run's context with
// (context.WithCancelCause). Any other cancellation is recorded by its
// cause's text.
var (
	ErrOperatorAbort = errors.New("operator abort")
	ErrLedgerCap     = errors.New("ledger cap reached")
)

// Interruption records a safety stop. Requests dispatched before it run
// to their terminal state; the unsent ones are never sent.
type Interruption struct {
	Cause      string `json:"cause"`
	StopNs     int64  `json:"stop_ns"`
	Scheduled  int    `json:"scheduled"`
	Dispatched int    `json:"dispatched"`
	Unsent     int    `json:"unsent"`
}

// tripwire holds a run's counts in atomics only, so a dispatch never
// waits on it. A nil tripwire records nothing. A pilot has one slot; a
// release has one per ladder step, the warm-up counted in the first.
type tripwire struct {
	scheduled, limit      []int64
	dispatches            []atomic.Int64
	inFlight, maxInFlight atomic.Int64
	extraNs, overNs       atomic.Int64
	extra, over           atomic.Bool
	ladder                bool
}

func newTripwire(cfg *config.Config, scheduled int) *tripwire {
	if cfg.Profile != config.ProfilePilot {
		return nil
	}
	return &tripwire{
		scheduled:  []int64{int64(scheduled)},
		limit:      []int64{config.PinnedPilotInFlightTripwire},
		dispatches: make([]atomic.Int64, 1),
	}
}

// newLadderTripwire bounds each step's sends by its scheduled count and
// its requests in flight by two times its rate times the hard timeout
// (SPEC-HOSTED HA3 point 16).
func newLadderTripwire(cfg *config.Config, l *Ladder) *tripwire {
	t := &tripwire{dispatches: make([]atomic.Int64, len(l.Steps)), ladder: true}
	for k, st := range l.Steps {
		n := st.End - st.First
		if k == 0 {
			n = st.End
		}
		t.scheduled = append(t.scheduled, int64(n))
		t.limit = append(t.limit, int64(inFlightLimit(cfg, st.RateRPS)))
	}
	return t
}

func inFlightLimit(cfg *config.Config, rate float64) int {
	return int(2 * rate * float64(cfg.Client.HTTPTimeoutS))
}

// slot is the index a request of 1-based ladder step step counts under.
func (t *tripwire) slot(step int) int {
	return max(step-1, 0)
}

// send counts one request of step sent at now.
func (t *tripwire) send(now int64, step int) {
	if t == nil {
		return
	}
	k := t.slot(step)
	if t.dispatches[k].Add(1) > t.scheduled[k] {
		t.extra.Store(true)
		t.extraNs.CompareAndSwap(0, now)
	}
}

// start counts a dispatch of step at now and its request in flight; end
// counts its terminal state.
func (t *tripwire) start(now int64, step int) {
	if t == nil {
		return
	}
	t.send(now, step)
	n := t.inFlight.Add(1)
	for m := t.maxInFlight.Load(); n > m && !t.maxInFlight.CompareAndSwap(m, n); m = t.maxInFlight.Load() {
	}
	if n > t.limit[t.slot(step)] {
		t.over.Store(true)
		t.overNs.CompareAndSwap(0, now)
	}
}

func (t *tripwire) end() {
	if t != nil {
		t.inFlight.Add(-1)
	}
}

// fired reports whether either check has fired so far.
func (t *tripwire) fired() bool {
	return t != nil && (t.extra.Load() || t.over.Load())
}

func (t *tripwire) report() *Tripwires {
	if t == nil {
		return nil
	}
	w := &Tripwires{
		ExtraDispatchNs: t.extraNs.Load(),
		MaxInFlight:     t.maxInFlight.Load(),
		OverInFlightNs:  t.overNs.Load(),
		Fired:           t.fired(),
	}
	for k := range t.scheduled {
		w.Scheduled += int(t.scheduled[k])
		w.Dispatches += t.dispatches[k].Load()
	}
	if t.ladder {
		for _, l := range t.limit {
			w.InFlightLimits = append(w.InFlightLimits, int(l))
		}
	} else {
		w.InFlightLimit = int(t.limit[0])
	}
	return w
}

type gen struct {
	cfg    *config.Config
	client *http.Client
	epoch  time.Time
	filler string
	// model, expected and apiKey are resolved once at Run start: the
	// requested model name (mock default when unset), the identifier
	// reported ones are compared with ("" compares with model) and the key
	// from cfg.Target.APIKeyEnv ("" = send no key header).
	model    string
	expected string
	apiKey   string
	trip     *tripwire
	// cases are a pilot run's user texts by request index.
	cases []string
	// gates evaluates the client-validity gate; a ladder step's verdict
	// uses it too.
	gates gateFunc
	// onDispatch and onDone are Hooks.OnDispatch and Hooks.OnDone.
	onDispatch func(r *Request)
	onDone     func(r *Request)
	// A request takes its dispatch time under stopMu's read lock, and only
	// while stop (Run's ctx.Done(); nil = never) is open; stopNs is taken
	// under the write lock, so no request sent has a dispatch time after it.
	stopMu sync.RWMutex
	stop   <-chan struct{}
	stopNs int64
}

type gateFunc func(cfg *config.Config, requests []Request, samples []cpuSample, gcLoMs, gcHiMs float64, measStartNs, measEndNs int64) GateReport

// now returns nanoseconds since the run epoch (monotonic).
func (g *gen) now() int64 { return time.Since(g.epoch).Nanoseconds() }

// Hooks are optional run-lifecycle callbacks.
type Hooks struct {
	// OnEpoch fires once the run epoch is anchored, before the first
	// dispatch; the orchestrator uses it to pre-arm T_inject.
	OnEpoch func(epoch time.Time)
	// Cases are the user texts of a run whose target carries a literal
	// body or whose config carries a pilot body, one per scheduled
	// request, by index.
	Cases []string
	// OnDispatch is called with each request as it is sent, once its
	// DispatchNs is set, on that request's goroutine, concurrently with
	// other calls. It must not block.
	OnDispatch func(r *Request)
	// OnDone is called with each dispatched request once it is terminal,
	// on that request's goroutine, concurrently with other calls. It must
	// not block.
	OnDone func(r *Request)
}

// Run executes the full scheduled load against cfg.Target.EndpointURL,
// or cfg.Target.BaseURL plus /v1/chat/completions when that is empty.
// It returns when every scheduled request has reached a terminal state
// (which can be up to the client timeout after the last dispatch), and
// for a release run once the last attempted step's drain has ended.
func Run(ctx context.Context, cfg *config.Config, hooks *Hooks) (*Result, error) {
	return run(ctx, cfg, hooks, evaluateGates)
}

func run(ctx context.Context, cfg *config.Config, hooks *Hooks, gates gateFunc) (*Result, error) {
	var ladder *Ladder
	if cfg.Profile == config.ProfileRelease {
		if ladder = BuildLadder(cfg); ladder == nil {
			return nil, fmt.Errorf("loadgen: release run without a ladder")
		}
	}
	schedule := BuildSchedule(cfg)
	if len(schedule) == 0 {
		return nil, fmt.Errorf("loadgen: empty schedule")
	}
	var (
		cases              []string
		onDispatch, onDone func(*Request)
	)
	if hooks != nil {
		cases, onDispatch, onDone = hooks.Cases, hooks.OnDispatch, hooks.OnDone
	}
	if cfg.Target.Body != "" {
		if cfg.Pilot != nil {
			return nil, errors.New("loadgen: target.body and pilot.body both set")
		}
		if err := config.CheckLiteralBody(cfg.Target.Body); err != nil {
			return nil, fmt.Errorf("loadgen: target.body: %w", err)
		}
		if len(cases) != len(schedule) {
			return nil, fmt.Errorf("loadgen: target body: %d case texts for %d scheduled requests", len(cases), len(schedule))
		}
	}
	if cfg.Pilot != nil && len(cases) != len(schedule) {
		return nil, fmt.Errorf("loadgen: pilot body: %d case texts for %d scheduled requests", len(cases), len(schedule))
	}

	// Provision connections above lambda x worst-case latency: lifetime
	// is bounded by the client timeout, so 2x lambda x timeout can absorb
	// every request blackholing simultaneously.
	provision := int(cfg.Load.RateRPS*float64(config.PinnedClientTimeoutS)*2) + cfg.Load.Connections
	if ladder != nil {
		provision = cfg.ReleaseConnectionProvision()
	}
	transport := NewTransport(provision)
	defer transport.CloseIdleConnections()
	model := RequestedModel(cfg)
	var apiKey string
	if cfg.Target.APIKeyEnv != "" {
		apiKey = os.Getenv(cfg.Target.APIKeyEnv)
		if apiKey == "" && cfg.Target.Hosted {
			// Fail before the first dispatch, not 30 s into a run of 401s.
			return nil, fmt.Errorf("loadgen: hosted target: environment variable %s is empty", cfg.Target.APIKeyEnv)
		}
	}
	g := &gen{
		cfg:        cfg,
		client:     &http.Client{Transport: transport},
		filler:     buildFiller(cfg.Load.InputLengthTokens),
		model:      model,
		expected:   ExpectedModel(cfg),
		apiKey:     apiKey,
		trip:       newTripwire(cfg, len(schedule)),
		cases:      cases,
		gates:      gates,
		onDispatch: onDispatch,
		onDone:     onDone,
		stop:       ctx.Done(),
	}
	if ladder != nil {
		g.trip = newLadderTripwire(cfg, ladder)
	}

	res := NewResult(cfg)

	requests := make([]Request, len(schedule))
	for i, off := range schedule {
		requests[i] = Request{Index: int64(i), IntendedNs: off}
	}
	if ladder != nil {
		res.WarmupEndNs, res.RunEndNs = ladder.Steps[0].StartNs, ladder.EndNs
		res.Ladder = newLadderRecord(cfg, ladder)
		lo := 0
		for k, st := range ladder.Steps {
			for i := lo; i < st.End; i++ {
				requests[i].Step, requests[i].Warmup = k+1, i < st.First
			}
			lo = st.End
		}
	}

	// Epoch first, then gate monitors (their timers are epoch-relative).
	// The measurement span is [warmup end, cooldown start], or for a
	// release run each step's window.
	g.epoch = time.Now()
	res.EpochWall = g.epoch
	if hooks != nil && hooks.OnEpoch != nil {
		hooks.OnEpoch(g.epoch)
	}
	stopped := make(chan struct{})
	unwatch := context.AfterFunc(ctx, func() {
		g.stopMu.Lock()
		g.stopNs = g.now()
		g.stopMu.Unlock()
		close(stopped)
	})
	defer unwatch()
	cpuMon := startCPUMonitor()
	var gcMon *gcMonitor
	if ladder == nil {
		gcMon = startGCMonitor(res.WarmupEndNs, res.FaultEndNs)
	}

	// Pacer: time-driven only. It spawns each worker a small lead ahead
	// of t_i and the worker performs its own final precise wait, so the
	// workers' waits run in parallel: one late pacer wakeup (scheduler
	// hiccup, GC) cannot serialize into the send skew of every subsequent
	// request. Dispatch remains a pure function of the clock.
	const spawnLeadNs = int64(10 * time.Millisecond)
	var wg sync.WaitGroup
	var aborted bool
	pace := func(r *Request, step *sync.WaitGroup) bool {
		delta := r.IntendedNs - spawnLeadNs - g.now()
		if (delta > 0 && !g.sleep(ctx, time.Duration(delta))) || ctx.Err() != nil {
			return false // remaining requests stay undispatched -> gate fails
		}
		wg.Add(1)
		step.Add(1)
		go func() {
			defer wg.Done()
			defer step.Done()
			// Final precise wait: timer to within ~1.5 ms of t_i, then a
			// short spin; dispatch precision must not depend on runtime
			// timer wakeup latency under read-burst load. Cost: ~1.5 ms of
			// one core per request at 20 rps ≈ 3% of one core.
			const spinNs = int64(1_500_000)
			if wait := r.IntendedNs - spinNs - g.now(); wait > 0 && !g.sleep(ctx, time.Duration(wait)) {
				return
			}
			for g.now() < r.IntendedNs {
			}
			if g.execute(r) && g.onDone != nil {
				g.onDone(r)
			}
		}()
		return true
	}
	if ladder == nil {
		var all sync.WaitGroup
		for i := range requests {
			if aborted = !pace(&requests[i], &all); aborted {
				break
			}
		}
	} else {
		aborted, gcMon = g.runLadder(ctx, ladder, requests, res.Ladder, pace, cpuMon)
	}
	wg.Wait()

	cpuSamples := cpuMon.stopAndCollect(g)
	gcLoMs, gcHiMs := gcMon.stopAndP99Ms()
	res.Requests = requests
	if ladder == nil {
		res.Gates = g.gates(cfg, requests, cpuSamples, gcLoMs, gcHiMs, res.WarmupEndNs, res.FaultEndNs)
	} else {
		res.Gates = ladderGates(g, res.Ladder, requests, cpuSamples, gcLoMs, gcHiMs)
	}
	compareModels(cfg, g.model, res)
	res.Tripwires = g.trip.report()
	if aborted || ctx.Err() != nil {
		<-stopped
		res.Interrupted = interruption(context.Cause(ctx), g.stopNs, requests)
		return res, fmt.Errorf("loadgen: run aborted by context: %w", ctx.Err())
	}
	if err := checkProtocols(cfg, requests); err != nil {
		return res, err
	}
	return res, nil
}

func newLadderRecord(cfg *config.Config, l *Ladder) *LadderRecord {
	rec := &LadderRecord{WarmupScheduled: l.Steps[0].First}
	for _, st := range l.Steps {
		rec.Steps = append(rec.Steps, StepRecord{
			RateRPS: st.RateRPS, StartNs: st.StartNs, EndNs: st.EndNs, DrainEndNs: st.DrainEndNs,
			Scheduled: st.End - st.First, InFlightLimit: inFlightLimit(cfg, st.RateRPS),
		})
	}
	return rec
}

// runLadder dispatches the ladder step by step and reports whether ctx
// stopped it, with the GC pause snapshots from the first step's start to
// the last attempted step's end. At each drain's end, once every request
// of the step (and of the warm-up, at the first step) is terminal, it
// takes the step's verdict; a verdict that ends the ladder leaves every
// later request unsent.
func (g *gen) runLadder(ctx context.Context, l *Ladder, requests []Request, rec *LadderRecord,
	pace func(*Request, *sync.WaitGroup) bool, cpuMon *cpuMonitor) (bool, *gcMonitor) {
	gcMons := make([]*gcMonitor, len(l.Steps))
	for k, st := range l.Steps {
		gcMons[k] = startGCMonitor(st.StartNs, st.EndNs)
	}
	done := func(aborted bool, k int) (bool, *gcMonitor) {
		for _, m := range gcMons {
			m.stop()
		}
		span := &gcMonitor{}
		gcMons[0].mu.Lock()
		span.start = gcMons[0].start
		gcMons[0].mu.Unlock()
		gcMons[k].mu.Lock()
		span.end = gcMons[k].end
		gcMons[k].mu.Unlock()
		return aborted, span
	}
	lo := 0
	for k, st := range l.Steps {
		sr := &rec.Steps[k]
		sr.Attempted = true
		var step sync.WaitGroup
		for i := lo; i < st.End; i++ {
			if !pace(&requests[i], &step) {
				return done(true, k)
			}
		}
		lo = st.End
		if d := st.DrainEndNs - g.now(); d > 0 && !g.sleep(ctx, time.Duration(d)) {
			return done(true, k)
		}
		step.Wait()
		measured := requests[st.First:st.End]
		for i := range measured {
			if measured[i].ErrClass == ErrStatus429 {
				sr.Status429++
			}
		}
		gcLoMs, gcHiMs := gcMons[k].stopAndP99Ms()
		gates := g.gates(g.cfg, measured, cpuMon.collect(g.epoch), gcLoMs, gcHiMs, st.StartNs, st.EndNs)
		sr.Gates = &gates
		sr.Tripwire = g.trip.fired()
		if end := stepEnd(sr); end != "" {
			rec.EndStep, rec.End = k+1, end
			return done(false, k)
		}
	}
	return done(false, len(l.Steps)-1)
}

// stepEnd is the reason a step's verdict ends the ladder, or "". The
// backoff rule ends it at a status_429 fraction of the step's scheduled
// requests above 0.5 (SPEC-HOSTED HA1, HA3 point 16).
func stepEnd(s *StepRecord) string {
	switch {
	case !s.Gates.Pass:
		return LadderEndClientValidity
	case s.Tripwire:
		return LadderEndTripwire
	case 2*s.Status429 > s.Scheduled:
		return LadderEndBackoff
	}
	return ""
}

// ladderGates evaluates the client-validity gate over every attempted
// step's measured windows, from the first step's start to the last
// attempted step's end; it fails when any step's verdict failed.
func ladderGates(g *gen, rec *LadderRecord, requests []Request, samples []cpuSample, gcLoMs, gcHiMs float64) GateReport {
	last, end := 0, 0
	for k, sr := range rec.Steps {
		if sr.Attempted {
			last = k
		}
	}
	for _, sr := range rec.Steps[:last+1] {
		end += sr.Scheduled
	}
	end += rec.WarmupScheduled
	rep := g.gates(g.cfg, requests[:end], samples, gcLoMs, gcHiMs, rec.Steps[0].StartNs, rec.Steps[last].EndNs)
	for _, sr := range rec.Steps {
		if sr.Gates != nil && !sr.Gates.Pass {
			rep.Pass = false
		}
	}
	return rep
}

// sleep waits d, and reports false if ctx ended first.
func (g *gen) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// NewResult returns an empty result carrying cfg's phase boundaries.
func NewResult(cfg *config.Config) *Result {
	p := cfg.Run.Phases
	return &Result{
		WarmupEndNs:   int64(p.WarmupS * 1e9),
		BaselineEndNs: int64((p.WarmupS + p.BaselineS) * 1e9),
		FaultEndNs:    int64((p.WarmupS + p.BaselineS + p.FaultWindowTimeoutS) * 1e9),
		RunEndNs:      int64((p.WarmupS + p.BaselineS + p.FaultWindowTimeoutS + p.CooldownS) * 1e9),
		TInjectNs:     int64((p.WarmupS + cfg.Fault.TInjectOffsetS) * 1e9),
	}
}

// RequestedModel is the model id every request of a run under cfg sends.
func RequestedModel(cfg *config.Config) string {
	if cfg.Target.ModelName == "" {
		return "percentes-mock"
	}
	return cfg.Target.ModelName
}

// ExpectedModel is the identifier each response's reported model is
// compared with under cfg: target.reported_model's name where recorded,
// otherwise RequestedModel.
func ExpectedModel(cfg *config.Config) string {
	if r := cfg.Target.ReportedModel; r != nil {
		return r.Name
	}
	return RequestedModel(cfg)
}

// CompareModels fills res's model fields from its requests, as Run does.
func CompareModels(cfg *config.Config, res *Result) {
	compareModels(cfg, RequestedModel(cfg), res)
}

// interruption counts a stopped run's requests.
func interruption(cause error, stopNs int64, requests []Request) *Interruption {
	in := &Interruption{Cause: cause.Error(), StopNs: stopNs, Scheduled: len(requests)}
	for i := range requests {
		if requests[i].DispatchNs != 0 {
			in.Dispatched++
		}
	}
	in.Unsent = in.Scheduled - in.Dispatched
	return in
}

// NewTransport pins HTTP/1.1 on plain and TLS connections, so each
// in-flight request holds its own connection. A non-nil empty
// TLSNextProto stops the transport from offering h2 in the TLS handshake.
func NewTransport(provision int) *http.Transport {
	return &http.Transport{
		ForceAttemptHTTP2:   false,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConns:        provision,
		MaxIdleConnsPerHost: provision,
		MaxConnsPerHost:     0, // never throttle dispatch on connection count
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
}

// checkProtocols fails a hosted run in which any response arrived over
// a protocol other than HTTP/1.1.
func checkProtocols(cfg *config.Config, requests []Request) error {
	if !cfg.Target.Hosted {
		return nil
	}
	n, first := 0, ""
	for i := range requests {
		if p := requests[i].Proto; p != "" && p != "HTTP/1.1" {
			if n == 0 {
				first = fmt.Sprintf("request %d over %s", requests[i].Index, p)
			}
			n++
		}
	}
	if n > 0 {
		return fmt.Errorf("loadgen: hosted target: %d responses not over HTTP/1.1 (first: %s)", n, first)
	}
	return nil
}

// compareModels fills res's model fields from its requests.
func compareModels(cfg *config.Config, requested string, res *Result) {
	res.RequestedModel, res.ExpectedModel = requested, requested
	if r := cfg.Target.ReportedModel; r != nil {
		res.ExpectedModel = r.Name
	}
	for i := range res.Requests {
		r := &res.Requests[i]
		for _, m := range append([]string{r.ReportedModel}, r.ModelMismatch...) {
			if m != "" && !slices.Contains(res.ReportedModels, m) {
				res.ReportedModels = append(res.ReportedModels, m)
			}
		}
		if len(r.ModelMismatch) > 0 {
			res.ModelMismatches++
		}
	}
	res.SplitSeries = cfg.Target.Hosted && res.ModelMismatches > 0
}

func buildFiller(tokens int) string {
	return strings.Repeat("tok ", tokens)
}
