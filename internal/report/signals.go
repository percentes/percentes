package report

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

// In-band error labels of malformed_stream requests whose error object
// names no type.
const (
	// InBandNone: the stream carried no in-band error object.
	InBandNone = "(none)"
	// InBandUntyped: an object with no string at its "type" key.
	InBandUntyped = "(untyped)"
	// InBandUndecodable: not a JSON object, or cut at the capture bound.
	InBandUndecodable = "(undecodable)"
)

// inBandLabel is the in-band error label of a record's stream_error: the
// string at the error object's "type" key, or one of the InBand labels.
func inBandLabel(streamError string) string {
	if streamError == "" {
		return InBandNone
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(streamError), &obj) != nil || obj == nil {
		return InBandUndecodable
	}
	var typ string
	if json.Unmarshal(obj["type"], &typ) != nil || typ == "" {
		return InBandUntyped
	}
	return typ
}

// Connection labels of a completion.
const (
	ConnDialled = "dialled"
	ConnReused  = "reused"
)

// timing keeps a step's durations from intended dispatch, in
// nanoseconds: per error class to the failure signal, and per
// connection (0 dialled, 1 reused) to the first content event and to the
// whole answer. retry counts the status_429 responses per Retry-After
// value. events holds each completion's content events, and tokens and
// reasoning its provider-reported completion and reasoning tokens where
// its record carries them; erroredEvents and censoredEvents hold the
// content events of each errored and censored request. sent counts the
// requests with an outcome, and prompt holds the provider-reported
// prompt tokens of those whose usage carries them. obs holds every
// request's terminal state for the incidence estimator, and gaps the
// inter-chunk gaps per bin of evidence.GapEdgesUs.
type timing struct {
	failure                       map[string][]int64
	ttft, done                    [2][]int64
	retry                         map[string]int
	events, tokens, reasoning     []int
	erroredEvents, censoredEvents []int
	sent                          int
	prompt                        []int
	obs                           []collect.Obs
	gaps                          []int64
}

func newStepTally() stepTally {
	return stepTally{
		lines: map[string]int{}, statuses: map[int]int{}, inBand: map[string]int{}, empty: map[string]int{},
		timing: timing{failure: map[string][]int64{}, retry: map[string]int{}, gaps: make([]int64, len(evidence.GapEdgesUs))},
	}
}

// add keeps the durations and counts of a record that fell on line. An
// unsent request is censored at the horizon.
func (t *timing) add(line string, r *evidence.Record, horizonUs int64) error {
	switch line {
	case LineUnsent:
		t.obs = append(t.obs, collect.Obs{TimeUs: horizonUs, Kind: collect.ObsCensored})
	case LineCensored:
		t.obs = append(t.obs, collect.Obs{TimeUs: max(0, r.DoneNs-r.IntendedNs) / 1000, Kind: collect.ObsCensored})
		t.censoredEvents = append(t.censoredEvents, r.ContentEvents)
	case LineInTime, LineLate:
		if r.FirstTokNs == 0 || r.FirstTokNs < r.IntendedNs || r.DoneNs < r.FirstTokNs {
			return fmt.Errorf("completed with intended_ns %d, first_token_ns %d and done_ns %d", r.IntendedNs, r.FirstTokNs, r.DoneNs)
		}
		c := 0
		if r.ConnReused {
			c = 1
		}
		t.ttft[c] = append(t.ttft[c], r.FirstTokNs-r.IntendedNs)
		t.done[c] = append(t.done[c], r.DoneNs-r.IntendedNs)
		t.obs = append(t.obs, collect.Obs{TimeUs: (r.DoneNs - r.IntendedNs) / 1000, Kind: collect.ObsCompletion})
		t.events = append(t.events, r.ContentEvents)
		if r.CompletionTokens != nil {
			t.tokens = append(t.tokens, *r.CompletionTokens)
		}
		if r.ReasoningTokens != nil {
			t.reasoning = append(t.reasoning, *r.ReasoningTokens)
		}
	default:
		if r.DoneNs == 0 || r.DoneNs < r.IntendedNs {
			return fmt.Errorf("errored with intended_ns %d and done_ns %d", r.IntendedNs, r.DoneNs)
		}
		t.failure[line] = append(t.failure[line], r.DoneNs-r.IntendedNs)
		t.obs = append(t.obs, collect.Obs{TimeUs: (r.DoneNs - r.IntendedNs) / 1000, Kind: collect.ObsError})
		if line == loadgen.ErrStatus429 && r.RetryAfter != "" {
			t.retry[r.RetryAfter]++
		}
		t.erroredEvents = append(t.erroredEvents, r.ContentEvents)
	}
	if line != LineUnsent {
		t.sent++
		if n, ok := promptTokens(r.Usage); ok {
			t.prompt = append(t.prompt, n)
		}
	}
	return nil
}

// promptTokens is the provider-reported prompt token count in a record's
// usage: a chat-completions prompt_tokens, or a Messages input_tokens
// plus its two cache counts, which input_tokens excludes. A missing or
// null count is absent; an explicit 0 is a count.
func promptTokens(usage json.RawMessage) (int, bool) {
	var u map[string]json.RawMessage
	if json.Unmarshal(usage, &u) != nil {
		return 0, false
	}
	count := func(key string) (int, bool) {
		var n *int
		if v, ok := u[key]; !ok || json.Unmarshal(v, &n) != nil || n == nil {
			return 0, false
		}
		return *n, true
	}
	if n, ok := count("prompt_tokens"); ok {
		return n, true
	}
	n, ok := count("input_tokens")
	if !ok {
		return 0, false
	}
	for _, k := range []string{"cache_creation_input_tokens", "cache_read_input_tokens"} {
		if c, ok := count(k); ok {
			n += c
		}
	}
	return n, true
}

// Quantiles are the median and 90th percentile of N durations by
// nearest rank, in nanoseconds; zero when N is 0.
type Quantiles struct {
	N     int   `json:"n"`
	P50Ns int64 `json:"p50_ns"`
	P90Ns int64 `json:"p90_ns"`
}

func quantiles(xs []int64) Quantiles {
	n := len(xs)
	if n == 0 {
		return Quantiles{}
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	return Quantiles{N: n, P50Ns: s[(n*50+99)/100-1], P90Ns: s[(n*90+99)/100-1]}
}

// FailureTiming is one error class's time from intended dispatch to the
// failure signal: the response status, the transport error, or the end
// of the stream.
type FailureTiming struct {
	Class string `json:"class"`
	Quantiles
}

// ConnLatency is the time from intended dispatch to the first content
// event (TTFT) and to the whole answer, over the completions on one kind
// of connection; client-observed and conditional on completion.
type ConnLatency struct {
	Conn        string    `json:"conn"`
	TTFT        Quantiles `json:"ttft"`
	WholeAnswer Quantiles `json:"whole_answer"`
}

// RetryAfter is the status_429 responses carrying Retry-After, With of
// Of, and the count of each value. Recorded is false when
// target.response_headers does not allow the header, so none could be
// recorded.
type RetryAfter struct {
	Recorded bool              `json:"recorded"`
	With     int               `json:"with"`
	Of       int               `json:"of"`
	Values   []RetryAfterValue `json:"values,omitempty"`
}

// RetryAfterValue is one Retry-After value and the responses carrying it.
type RetryAfterValue struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// Signals is a step's failure timing per error class with events; its
// completion latency over every completion and by connection; its
// completion length as content events, provider-reported completion
// tokens and reasoning tokens; the content events of its errored and of
// its censored requests; its provider-reported prompt tokens over the
// requests sent; its inter-chunk gaps; and its Retry-After record, nil
// where the step had no status_429. ConditionalCaveat marks errors and
// censored requests above 5 percent of scheduled, where the latency is
// read beside the incidence curve.
type Signals struct {
	Failures              []FailureTiming `json:"failure_timing,omitempty"`
	TTFT                  Distribution    `json:"ttft"`
	WholeAnswer           Distribution    `json:"whole_answer"`
	Latency               []ConnLatency   `json:"latency_by_connection"`
	ConditionalCaveat     bool            `json:"conditional_caveat,omitempty"`
	ContentEvents         Counts          `json:"content_events"`
	CompletionTokens      Counts          `json:"completion_tokens"`
	ReasoningTokens       Counts          `json:"reasoning_tokens"`
	ContentEventsErrored  Counts          `json:"content_events_errored"`
	ContentEventsCensored Counts          `json:"content_events_censored"`
	PromptTokens          Counts          `json:"prompt_tokens"`
	Gaps                  Gaps            `json:"inter_chunk_gaps"`
	RetryAfter            *RetryAfter     `json:"retry_after,omitempty"`
}

// conditionalLimit is the error and censored fraction above which
// latency conditional on completion is read beside the incidence curve
// (SPEC.md §3).
const conditionalLimit = 0.05

// signals pools the tallies of one step over scheduled requests.
func signals(ts []*stepTally, scheduled int, retryRecorded bool) *Signals {
	s := &Signals{}
	cat := func(f func(*stepTally) []int64) []int64 {
		var out []int64
		for _, t := range ts {
			out = append(out, f(t)...)
		}
		return out
	}
	failed := 0
	for _, t := range ts {
		failed += t.lines[LineCensored]
	}
	for _, class := range errClasses {
		xs := cat(func(t *stepTally) []int64 { return t.failure[class] })
		failed += len(xs)
		if len(xs) > 0 {
			s.Failures = append(s.Failures, FailureTiming{Class: class, Quantiles: quantiles(xs)})
		}
	}
	s.ConditionalCaveat = float64(failed) > conditionalLimit*float64(scheduled)
	s.TTFT = distribution(cat(func(t *stepTally) []int64 { return append(slices.Clone(t.ttft[0]), t.ttft[1]...) }))
	s.WholeAnswer = distribution(cat(func(t *stepTally) []int64 { return append(slices.Clone(t.done[0]), t.done[1]...) }))
	catInts := func(f func(*stepTally) []int) []int {
		var out []int
		for _, t := range ts {
			out = append(out, f(t)...)
		}
		return out
	}
	of := s.WholeAnswer.N
	s.ContentEvents = counts(catInts(func(t *stepTally) []int { return t.events }), of)
	s.CompletionTokens = counts(catInts(func(t *stepTally) []int { return t.tokens }), of)
	s.ReasoningTokens = counts(catInts(func(t *stepTally) []int { return t.reasoning }), of)
	errored := catInts(func(t *stepTally) []int { return t.erroredEvents })
	s.ContentEventsErrored = counts(errored, len(errored))
	censored := catInts(func(t *stepTally) []int { return t.censoredEvents })
	s.ContentEventsCensored = counts(censored, len(censored))
	sent := 0
	for _, t := range ts {
		sent += t.sent
	}
	s.PromptTokens = counts(catInts(func(t *stepTally) []int { return t.prompt }), sent)
	s.Gaps = gapsOf(ts)
	for c, conn := range []string{ConnDialled, ConnReused} {
		s.Latency = append(s.Latency, ConnLatency{
			Conn:        conn,
			TTFT:        quantiles(cat(func(t *stepTally) []int64 { return t.ttft[c] })),
			WholeAnswer: quantiles(cat(func(t *stepTally) []int64 { return t.done[c] })),
		})
	}
	limited := 0
	values := map[string]int{}
	for _, t := range ts {
		limited += t.lines[loadgen.ErrStatus429]
		for v, n := range t.retry {
			values[v] += n
		}
	}
	if limited > 0 {
		ra := &RetryAfter{Recorded: retryRecorded, Of: limited}
		for v, n := range values {
			ra.With += n
			ra.Values = append(ra.Values, RetryAfterValue{Value: v, Count: n})
		}
		slices.SortFunc(ra.Values, func(a, b RetryAfterValue) int { return compareRetryAfter(a.Value, b.Value) })
		s.RetryAfter = ra
	}
	return s
}

// compareRetryAfter orders whole seconds numerically, before any other
// value in string order.
func compareRetryAfter(a, b string) int {
	x, errA := strconv.ParseUint(a, 10, 64)
	y, errB := strconv.ParseUint(b, 10, 64)
	switch {
	case errA == nil && errB == nil && x != y:
		if x < y {
			return -1
		}
		return 1
	case errA == nil && errB != nil:
		return -1
	case errA != nil && errB == nil:
		return 1
	}
	return strings.Compare(a, b)
}

// maxHalfFraction is the largest fraction of a half-tested limit among a
// step's demands; ok is false when no demand takes the half test.
func maxHalfFraction(s admission.Step) (f float64, ok bool) {
	for _, d := range s.Demands {
		if d.Half && (!ok || d.Fraction > f) {
			f, ok = d.Fraction, true
		}
	}
	return f, ok
}

// signalsText renders a step's failure timing, completion latency,
// completion length, the content events of requests that did not
// complete, prompt length and inter-chunk gaps.
func signalsText(w func(string, ...any), s *Signals) {
	q := func(x Quantiles) string {
		if x.N == 0 {
			return "n 0"
		}
		return fmt.Sprintf("n %d, median %s, 90th percentile %s", x.N, msText(x.P50Ns), msText(x.P90Ns))
	}
	for _, f := range s.Failures {
		w("  time to failure signal, %s: %s", f.Class, q(f.Quantiles))
	}
	w("  completions, client-observed from intended dispatch, conditional on completion:")
	w("    TTFT          %s", distributionText(s.TTFT))
	w("    whole answer  %s", distributionText(s.WholeAnswer))
	w("  completions by connection, client-observed from intended dispatch, conditional on completion:")
	for _, l := range s.Latency {
		w("    %-8s TTFT %s; whole answer %s", l.Conn, q(l.TTFT), q(l.WholeAnswer))
	}
	if s.ConditionalCaveat {
		w("    errors and censored above 5%% of scheduled: read with the incidence curve")
	}
	if s.ContentEvents.Of == 0 {
		w("  completion length: no completions")
	} else {
		w("  completion length over the %d completions:", s.ContentEvents.Of)
		w("    %-37s %s", "content events", countsText(s.ContentEvents))
		w("    %-37s %s", "provider-reported completion tokens", countsText(s.CompletionTokens))
		w("    %-37s %s", "provider-reported reasoning tokens", countsText(s.ReasoningTokens))
	}
	for _, o := range []struct {
		name string
		c    Counts
	}{{"errored", s.ContentEventsErrored}, {"censored", s.ContentEventsCensored}} {
		text := fmt.Sprintf("n %d", o.c.N)
		if o.c.N > 0 {
			text += ", " + countsText(o.c)
		}
		w("  content events, %-18s %s", o.name+" requests:", text)
	}
	if s.PromptTokens.Of == 0 {
		w("  prompt length: no requests sent")
	} else {
		w("  prompt length over the %d requests sent:", s.PromptTokens.Of)
		w("    %-37s %s", "provider-reported prompt tokens", countsText(s.PromptTokens))
	}
	gapsText(w, s.Gaps)
}

// retryAfterText renders a step's status_429 context: the largest
// fraction of a half-tested limit its nominal demand reached, and its
// Retry-After record.
func retryAfterText(w func(string, ...any), s *Signals, lim admission.Step) {
	if f, ok := maxHalfFraction(lim); ok {
		w("    nominal demand at most %.1f%% of each half-tested recorded limit; no cause inferred", 100*f)
	} else {
		w("    no half-tested limit recorded; no cause inferred")
	}
	ra := s.RetryAfter
	switch {
	case ra == nil:
	case !ra.Recorded:
		w("    Retry-After not recorded: target.response_headers does not allow it")
	default:
		var vs []string
		for _, v := range ra.Values {
			vs = append(vs, fmt.Sprintf("%q x%d", v.Value, v.Count))
		}
		line := fmt.Sprintf("    Retry-After on %d of %d status_429 responses, not honoured", ra.With, ra.Of)
		if len(vs) > 0 {
			line += ": " + strings.Join(vs, ", ")
		}
		w("%s", line)
	}
}
