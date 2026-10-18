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
	// CompletionTokens is the completion token count from a usage object
	// in the stream; UsageSeen says one arrived.
	CompletionTokens int    `json:"completion_tokens,omitempty"`
	UsageSeen        bool   `json:"usage_seen,omitempty"`
	Replica          string `json:"replica,omitempty"`
	// Usage is the last usage object the stream carried, at any path
	// target.response_shape names.
	Usage json.RawMessage `json:"usage,omitempty"`
	// FinishReason is the last finish_reason of the first choice;
	// ReportedModel is the first model id a chunk reported.
	FinishReason  string `json:"finish_reason,omitempty"`
	ReportedModel string `json:"reported_model,omitempty"`
	// SystemFingerprint is the first top-level system_fingerprint a chunk
	// carried.
	SystemFingerprint string `json:"system_fingerprint,omitempty"`
	// ModelMismatch lists, in arrival order, each distinct model id a
	// chunk reported that differs from the requested one, up to
	// modelMismatchLimit ids.
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
	// or error.
	UnknownEvents int `json:"unknown_events,omitempty"`
	// Content and Reasoning are the visible answer and the reasoning text,
	// kept for hosted targets only; never in JSON.
	Content   string `json:"-"`
	Reasoning string `json:"-"`
	// Proto is the response's protocol version ("" = no response).
	Proto string `json:"proto,omitempty"`
	// Status is the HTTP status code (0 = no response). Headers holds the
	// response headers target.response_headers allows, by canonical name.
	Status  int                 `json:"status,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
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

	// RequestedModel is the model id every request sent. ReportedModels
	// lists each distinct id recorded on the requests (reported_model and
	// model_mismatch), in request order. ModelMismatches counts requests
	// with a mismatch; SplitSeries marks a hosted run that had one, which
	// splits the series at this run.
	RequestedModel  string   `json:"requested_model,omitempty"`
	ReportedModels  []string `json:"reported_models,omitempty"`
	ModelMismatches int      `json:"model_mismatches,omitempty"`
	SplitSeries     bool     `json:"split_series,omitempty"`

	// Tripwires is set for a pilot run only. Interrupted is set when a
	// safety stop cancelled the run's context.
	Tripwires   *Tripwires    `json:"tripwires,omitempty"`
	Interrupted *Interruption `json:"interrupted,omitempty"`
}

// Tripwires are the pilot's two local dispatch checks (SPEC-HOSTED HA2
// §3). They record and never block or delay a dispatch; either one
// firing makes the window client-invalid.
type Tripwires struct {
	// Dispatches counts every request sent, including a further
	// connection taken by one request. ExtraDispatchNs is the first send
	// beyond Scheduled (0 = none).
	Scheduled       int   `json:"scheduled"`
	Dispatches      int64 `json:"dispatches"`
	ExtraDispatchNs int64 `json:"extra_dispatch_ns,omitempty"`
	// MaxInFlight is the most requests between dispatch and terminal
	// state at once. OverInFlightNs is the first dispatch that took the
	// count above InFlightLimit (0 = none).
	InFlightLimit  int   `json:"in_flight_limit"`
	MaxInFlight    int64 `json:"max_in_flight"`
	OverInFlightNs int64 `json:"over_in_flight_ns,omitempty"`
	Fired          bool  `json:"fired"`
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

// tripwire holds a pilot run's counts in atomics only, so a dispatch
// never waits on it. A nil tripwire records nothing.
type tripwire struct {
	scheduled, limit                  int64
	dispatches, inFlight, maxInFlight atomic.Int64
	extraNs, overNs                   atomic.Int64
}

func newTripwire(cfg *config.Config, scheduled int) *tripwire {
	if cfg.Profile != config.ProfilePilot {
		return nil
	}
	return &tripwire{scheduled: int64(scheduled), limit: config.PinnedPilotInFlightTripwire}
}

// send counts one request sent at now.
func (t *tripwire) send(now int64) {
	if t == nil {
		return
	}
	if t.dispatches.Add(1) > t.scheduled {
		t.extraNs.CompareAndSwap(0, now)
	}
}

// start counts a dispatch at now and its request in flight; end counts
// its terminal state.
func (t *tripwire) start(now int64) {
	if t == nil {
		return
	}
	t.send(now)
	n := t.inFlight.Add(1)
	for m := t.maxInFlight.Load(); n > m && !t.maxInFlight.CompareAndSwap(m, n); m = t.maxInFlight.Load() {
	}
	if n > t.limit {
		t.overNs.CompareAndSwap(0, now)
	}
}

func (t *tripwire) end() {
	if t != nil {
		t.inFlight.Add(-1)
	}
}

func (t *tripwire) report() *Tripwires {
	if t == nil {
		return nil
	}
	w := &Tripwires{
		Scheduled:       int(t.scheduled),
		Dispatches:      t.dispatches.Load(),
		ExtraDispatchNs: t.extraNs.Load(),
		InFlightLimit:   int(t.limit),
		MaxInFlight:     t.maxInFlight.Load(),
		OverInFlightNs:  t.overNs.Load(),
	}
	w.Fired = w.Dispatches > t.scheduled || w.MaxInFlight > t.limit
	return w
}

type gen struct {
	cfg    *config.Config
	client *http.Client
	epoch  time.Time
	filler string
	// model and apiKey are resolved once at Run start: the requested
	// model name (mock default when unset) and the key from
	// cfg.Target.APIKeyEnv ("" = send no key header).
	model  string
	apiKey string
	trip   *tripwire
	// cases are a pilot run's user texts by request index.
	cases []string
}

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
}

// Run executes the full scheduled load against cfg.Target.EndpointURL,
// or cfg.Target.BaseURL plus /v1/chat/completions when that is empty.
// It returns when every scheduled request has reached a terminal state
// (which can be up to the 30 s timeout after the last dispatch).
func Run(ctx context.Context, cfg *config.Config, hooks *Hooks) (*Result, error) {
	schedule := BuildSchedule(cfg)
	if len(schedule) == 0 {
		return nil, fmt.Errorf("loadgen: empty schedule")
	}
	var cases []string
	if hooks != nil {
		cases = hooks.Cases
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
	// is bounded by the pinned 30 s timeout, so 2x lambda x timeout can
	// absorb every request blackholing simultaneously.
	provision := int(cfg.Load.RateRPS*float64(config.PinnedClientTimeoutS)*2) + cfg.Load.Connections
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
		cfg:    cfg,
		client: &http.Client{Transport: transport},
		filler: buildFiller(cfg.Load.InputLengthTokens),
		model:  model,
		apiKey: apiKey,
		trip:   newTripwire(cfg, len(schedule)),
		cases:  cases,
	}

	res := NewResult(cfg)

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
	var stopNs int64
	stopped := make(chan struct{})
	unwatch := context.AfterFunc(ctx, func() {
		stopNs = g.now()
		close(stopped)
	})
	defer unwatch()
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
			// short spin; dispatch precision must not depend on runtime
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
	gcLoMs, gcHiMs := gcMon.stopAndP99Ms()
	res.Requests = requests
	res.Gates = evaluateGates(cfg, requests, cpuSamples, gcLoMs, gcHiMs, res.WarmupEndNs, res.FaultEndNs)
	compareModels(cfg, g.model, res)
	res.Tripwires = g.trip.report()
	if aborted || ctx.Err() != nil {
		<-stopped
		res.Interrupted = interruption(context.Cause(ctx), stopNs, requests)
		return res, fmt.Errorf("loadgen: run aborted by context: %w", ctx.Err())
	}
	if err := checkProtocols(cfg, requests); err != nil {
		return res, err
	}
	return res, nil
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
	res.RequestedModel = requested
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
