package report

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

func TestInBandLabel(t *testing.T) {
	for in, want := range map[string]string{
		"":                                    InBandNone,
		`{"type":"overloaded_error","m":"x"}`: "overloaded_error",
		`{"message":"x"}`:                     InBandUntyped,
		`{"type":null}`:                       InBandUntyped,
		`{"type":""}`:                         InBandUntyped,
		`{"type":529}`:                        InBandUntyped,
		`{"type":"overl`:                      InBandUndecodable,
		`["overloaded_error"]`:                InBandUndecodable,
		`null`:                                InBandUndecodable,
	} {
		if got := inBandLabel(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

// Nearest rank: the median of n values is the ceil(n/2)-th smallest and
// the 90th percentile the ceil(0.9n)-th.
func TestQuantiles(t *testing.T) {
	for _, c := range []struct {
		xs       []int64
		p50, p90 int64
	}{
		{[]int64{7}, 7, 7},
		{[]int64{3, 1, 2}, 2, 3},
		{[]int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, 5, 9},
		{[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 6, 10},
	} {
		in := slices.Clone(c.xs)
		if q := quantiles(c.xs); q != (Quantiles{N: len(c.xs), P50Ns: c.p50, P90Ns: c.p90}) {
			t.Errorf("%v: %+v", c.xs, q)
		}
		if !slices.Equal(in, c.xs) {
			t.Errorf("quantiles reordered its input: %v", c.xs)
		}
	}
	if q := quantiles(nil); q != (Quantiles{}) {
		t.Errorf("no values: %+v", q)
	}
}

func TestCompareRetryAfter(t *testing.T) {
	vs := []string{"Wed, 21 Oct 2026 07:28:00 GMT", "10", "abc", "2", "0", "-1"}
	slices.SortFunc(vs, compareRetryAfter)
	if want := []string{"0", "2", "10", "-1", "Wed, 21 Oct 2026 07:28:00 GMT", "abc"}; !slices.Equal(vs, want) {
		t.Errorf("order %q, want %q", vs, want)
	}
}

// The status_429 context reads the largest fraction among the half
// tests only.
func TestRetryAfterContext(t *testing.T) {
	var lines []string
	w := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	lim := admission.Step{Demands: []admission.Demand{
		{Dimension: "requests_per_minute", Fraction: 0.1, Half: true},
		{Dimension: admission.RouteCapDimension, Fraction: 0.9},
		{Dimension: "tokens_per_minute", Fraction: 0.3, Half: true},
	}}
	retryAfterText(w, &Signals{}, lim)
	lim.Demands = lim.Demands[1:2]
	retryAfterText(w, &Signals{}, lim)
	want := []string{
		"    nominal demand at most 30.0% of each half-tested recorded limit; no cause inferred",
		"    no half-tested limit recorded; no cause inferred",
	}
	if !slices.Equal(lines, want) {
		t.Errorf("lines %q, want %q", lines, want)
	}
}

const ms = int64(1_000_000)

// signalsCell is a cell whose step 1 holds completions on dialled and
// reused connections, three status_429 responses (two carrying
// Retry-After), a 529 carrying Retry-After, six malformed streams of five in-band labels and
// a censored request; step 2 holds 40 completions and one status_429
// without Retry-After.
func signalsCell(label string) CellRecords {
	c := cellOf(label, outcome{2, LineInTime, 0, 40}, outcome{2, loadgen.ErrStatus429, 0, 1},
		outcome{3, LineUnsent, 0, 480}, outcome{4, LineUnsent, 0, 960}, outcome{5, LineUnsent, 0, 1920})
	const base = 5_000 * ms
	add := func(r evidence.Record) {
		r.Index, r.Shape, r.CeilingTokens, r.Step = int64(len(c.Records)), "S", 512, 1
		r.IntendedNs, r.DispatchNs = base, base+1
		c.Records = append(c.Records, r)
	}
	done := func(reused bool, ttftMs, wholeMs int64) {
		r := evidence.Record{Outcome: loadgen.OutcomeCompleted, ConnReused: reused, FirstTokNs: base + ttftMs*ms, DoneNs: base + wholeMs*ms}
		r.InTime, r.Late = wholeMs <= 30_000, wholeMs > 30_000
		add(r)
	}
	failed := func(class string, status int, afterMs int64, retryAfter, streamError string) {
		add(evidence.Record{Outcome: loadgen.OutcomeErrored, ErrClass: class, Status: status, DoneNs: base + afterMs*ms,
			RetryAfter: retryAfter, StreamError: streamError})
	}
	done(false, 200, 1000)
	done(true, 100, 300)
	done(true, 400, 900)
	done(true, 500, 1200)
	done(true, 600, 1500)
	done(true, 1000, 31000)
	failed(loadgen.ErrStatus429, 429, 50, "10", "")
	failed(loadgen.ErrStatus429, 429, 60, "", "")
	failed(loadgen.ErrStatus429, 429, 70, "2", "")
	failed(loadgen.ErrStatusOther, 529, 25000, "30", "")
	failed(loadgen.ErrMalformedStream, 200, 3000, "", `{"type":"overloaded_error","message":"Overloaded"}`)
	failed(loadgen.ErrMalformedStream, 200, 4000, "", `{"type":"overloaded_error","message":"Overloaded"}`)
	failed(loadgen.ErrMalformedStream, 200, 5000, "", `{"message":"no type"}`)
	failed(loadgen.ErrMalformedStream, 200, 6000, "", `{"type":"overl`)
	failed(loadgen.ErrMalformedStream, 200, 7000, "", "")
	failed(loadgen.ErrMalformedStream, 200, 8000, "", `{"type":"api_error"}`)
	add(evidence.Record{Outcome: loadgen.OutcomeCensored, DoneNs: base + 90_000*ms})
	return c
}

// One cell: time to the failure signal per error class with events,
// completion latency split by connection reuse, in-band error lines per
// label with the documented mapping, the status_429 context and
// Retry-After record, and the conditional caveat where errors and
// censored pass 5 percent.
func TestStepTablesSignals(t *testing.T) {
	cfg, v := stepsConfig(t)
	cfg.Target.ErrorDocs = []config.ErrorDoc{
		{Type: "overloaded_error", Status: 529, Meaning: "overloaded", Source: "errors.png", Retrieved: "2026-10-07"},
	}
	st, err := NewStepTables(cfg, v, []CellRecords{signalsCell("weekday-00")})
	if err != nil {
		t.Fatal(err)
	}
	s := st.Cells[0].Steps[0]
	if s.Scheduled != 17 {
		t.Fatalf("step 1: %d scheduled", s.Scheduled)
	}
	sig := s.Signals
	if sig == nil {
		t.Fatal("step 1: no signals")
	}
	q := func(n int, p50, p90 int64) Quantiles { return Quantiles{N: n, P50Ns: p50 * ms, P90Ns: p90 * ms} }
	wantFail := []FailureTiming{
		{loadgen.ErrStatus429, q(3, 60, 70)},
		{loadgen.ErrStatusOther, q(1, 25000, 25000)},
		{loadgen.ErrMalformedStream, q(6, 5000, 8000)},
	}
	if !slices.Equal(sig.Failures, wantFail) {
		t.Errorf("failure timing %+v, want %+v", sig.Failures, wantFail)
	}
	wantLat := []ConnLatency{
		{ConnDialled, q(1, 200, 200), q(1, 1000, 1000)},
		{ConnReused, q(5, 500, 1000), q(5, 1200, 31000)},
	}
	if !slices.Equal(sig.Latency, wantLat) {
		t.Errorf("latency %+v, want %+v", sig.Latency, wantLat)
	}
	if !sig.ConditionalCaveat {
		t.Error("11 of 17 errored or censored: no conditional caveat")
	}
	ra := sig.RetryAfter
	if ra == nil || !ra.Recorded || ra.With != 2 || ra.Of != 3 || !slices.Equal(ra.Values, []RetryAfterValue{{"2", 1}, {"10", 1}}) {
		t.Errorf("Retry-After %+v", ra)
	}
	inBand := map[string]int{}
	var labels []string
	for _, l := range s.Lines {
		if l.InBand != "" {
			if l.Name != loadgen.ErrMalformedStream {
				t.Errorf("in-band label on line %s", l.Name)
			}
			inBand[l.InBand] = l.Count
			labels = append(labels, l.InBand)
			if l.Rate != newRate(l.Count, 17) {
				t.Errorf("in-band %s: %+v", l.InBand, l.Rate)
			}
			if (l.ErrorDoc != nil) != (l.InBand == "overloaded_error") {
				t.Errorf("in-band %s: documented %+v", l.InBand, l.ErrorDoc)
			}
		}
	}
	if want := []string{InBandNone, InBandUndecodable, InBandUntyped, "api_error", "overloaded_error"}; !slices.Equal(labels, want) {
		t.Errorf("in-band labels %q, want %q", labels, want)
	}
	if inBand["overloaded_error"] != 2 || inBand[InBandNone] != 1 || inBand[InBandUntyped] != 1 || inBand[InBandUndecodable] != 1 || inBand["api_error"] != 1 {
		t.Errorf("in-band counts %v", inBand)
	}

	s2 := st.Cells[0].Steps[1].Signals
	if s2 == nil || s2.ConditionalCaveat || s2.RetryAfter == nil || s2.RetryAfter.With != 0 || s2.RetryAfter.Of != 1 || s2.RetryAfter.Values != nil {
		t.Errorf("step 2 signals %+v", s2)
	}
	if s3 := st.Cells[0].Steps[2]; s3.Signals != nil {
		t.Errorf("step 3, not attempted: signals %+v", s3.Signals)
	}

	text := st.Text()
	for _, frag := range []string{
		"  status_429             3/17 17.65% [",
		"\n    nominal demand at most 10.3% of each half-tested recorded limit; no cause inferred\n" +
			"    Retry-After on 2 of 3 status_429 responses, not honoured: \"2\" x1, \"10\" x1\n" +
			"  status_other ",
		"\n  malformed_stream (none) 1/17 5.88% [",
		"\n  malformed_stream (untyped) 1/17 5.88% [",
		"]; api_error: no documented meaning recorded\n",
		"]; overloaded_error documented as \"overloaded\", corresponding to status 529 (errors.png, retrieved 2026-10-07)\n",
		"\n  time to failure signal, status_429: n 3, median 60.0 ms, 90th percentile 70.0 ms\n" +
			"  time to failure signal, status_other: n 1, median 25000.0 ms, 90th percentile 25000.0 ms\n" +
			"  time to failure signal, malformed_stream: n 6, median 5000.0 ms, 90th percentile 8000.0 ms\n" +
			"  completions, client-observed from intended dispatch, conditional on completion:\n" +
			"    TTFT          n 6, median 400.0 ms, 90th percentile 1000.0 ms; 95th and 99th percentiles not reported: their interval ranks fall outside the sample\n" +
			"    whole answer  n 6, median 1000.0 ms, 90th percentile 31000.0 ms; 95th and 99th percentiles not reported: their interval ranks fall outside the sample\n" +
			"  completions by connection, client-observed from intended dispatch, conditional on completion:\n" +
			"    dialled  TTFT n 1, median 200.0 ms, 90th percentile 200.0 ms; whole answer n 1, median 1000.0 ms, 90th percentile 1000.0 ms\n" +
			"    reused   TTFT n 5, median 500.0 ms, 90th percentile 1000.0 ms; whole answer n 5, median 1200.0 ms, 90th percentile 31000.0 ms\n" +
			"    errors and censored above 5% of scheduled: read with the incidence curve\n",
		"\n    Retry-After on 0 of 1 status_429 responses, not honoured\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	if n := strings.Count(text, "read with the incidence curve"); n != 1 {
		t.Errorf("conditional caveat printed %d times", n)
	}
	for _, label := range []string{InBandNone, InBandUntyped, InBandUndecodable} {
		if strings.Contains(text, label+": no documented") {
			t.Errorf("in-band label %s carries a documentation note:\n%s", label, text)
		}
	}

	cfg.Target.ResponseHeaders = []string{"x-ratelimit-*"}
	st, err = NewStepTables(cfg, v, []CellRecords{signalsCell("weekday-00")})
	if err != nil {
		t.Fatal(err)
	}
	if ra := st.Cells[0].Steps[0].Signals.RetryAfter; ra == nil || ra.Recorded {
		t.Errorf("Retry-After not allowed: %+v", ra)
	}
	if text := st.Text(); !strings.Contains(text, "\n    Retry-After not recorded: target.response_headers does not allow it\n") {
		t.Errorf("text lacks the unrecorded Retry-After line:\n%s", text)
	}
}

// A complete frame pools each step's durations, Retry-After values and
// in-band labels over the cells that attempted it.
func TestStepTablesSignalsPooled(t *testing.T) {
	cfg, v := stepsConfig(t)
	var cells []CellRecords
	for i := 0; i < 12; i++ {
		cells = append(cells, signalsCell(fmt.Sprintf("cell-%02d", i)))
	}
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	p := st.Pooled[0]
	if p.Cells != 12 || p.Scheduled != 204 || p.Signals == nil {
		t.Fatalf("pooled step 1: %+v", p)
	}
	q := func(n int, p50, p90 int64) Quantiles { return Quantiles{N: n, P50Ns: p50 * ms, P90Ns: p90 * ms} }
	sig := p.Signals
	if !slices.Equal(sig.Failures, []FailureTiming{
		{loadgen.ErrStatus429, q(36, 60, 70)},
		{loadgen.ErrStatusOther, q(12, 25000, 25000)},
		{loadgen.ErrMalformedStream, q(72, 5000, 8000)},
	}) {
		t.Errorf("pooled failure timing %+v", sig.Failures)
	}
	if !slices.Equal(sig.Latency, []ConnLatency{
		{ConnDialled, q(12, 200, 200), q(12, 1000, 1000)},
		{ConnReused, q(60, 500, 1000), q(60, 1200, 31000)},
	}) {
		t.Errorf("pooled latency %+v", sig.Latency)
	}
	if ra := sig.RetryAfter; ra == nil || ra.With != 24 || ra.Of != 36 || !slices.Equal(ra.Values, []RetryAfterValue{{"2", 12}, {"10", 12}}) || !sig.ConditionalCaveat {
		t.Errorf("pooled signals %+v", sig)
	}
	var found bool
	for _, l := range p.Lines {
		if l.InBand == "overloaded_error" {
			found = true
			if l.Count != 24 || l.Scheduled != 204 || l.CellsWithEvents != 12 {
				t.Errorf("pooled overloaded_error: %+v", l.Pooled)
			}
		}
	}
	if !found {
		t.Error("no pooled overloaded_error line")
	}
	if p2 := st.Pooled[1].Signals; p2 == nil || p2.ConditionalCaveat || p2.RetryAfter == nil || p2.RetryAfter.Of != 12 || p2.RetryAfter.With != 0 {
		t.Errorf("pooled step 2 signals %+v", p2)
	}
	if st.Pooled[2].Signals != nil {
		t.Errorf("pooled step 3, attempted in no cell: signals %+v", st.Pooled[2].Signals)
	}

	text := st.Text()
	pooledPart := text[strings.Index(text, "-- Pooled per step"):]
	for _, frag := range []string{
		"\n    Retry-After on 24 of 36 status_429 responses, not honoured: \"2\" x12, \"10\" x12\n",
		"\n  malformed_stream overloaded_error 24/204 11.76%; cell rates 11.76% to 11.76%; 12 of 12 cells with events;",
		"\n  time to failure signal, malformed_stream: n 72, median 5000.0 ms, 90th percentile 8000.0 ms\n",
		"\n    reused   TTFT n 60, median 500.0 ms, 90th percentile 1000.0 ms; whole answer n 60, median 1200.0 ms, 90th percentile 31000.0 ms\n",
	} {
		if !strings.Contains(pooledPart, frag) {
			t.Errorf("pooled text lacks %q:\n%s", frag, pooledPart)
		}
	}
}

// A completion or error whose times cannot give a duration from intended
// dispatch is refused.
func TestStepTablesTimeRefusals(t *testing.T) {
	for _, c := range []struct {
		name   string
		index  int
		mutate func(*evidence.Record)
		want   string
	}{
		{"completed without first token", 1, func(r *evidence.Record) { r.FirstTokNs = 0 }, "record 1: completed with intended_ns 0, first_token_ns 0 and done_ns 1"},
		{"first token before intended", 1, func(r *evidence.Record) { r.IntendedNs = 5 }, "record 1: completed with intended_ns 5, first_token_ns 1 and done_ns 1"},
		{"done before first token", 1, func(r *evidence.Record) { r.FirstTokNs = 2 }, "record 1: completed with intended_ns 0, first_token_ns 2 and done_ns 1"},
		{"errored without done", 2, func(r *evidence.Record) { r.DoneNs = 0 }, "record 2: errored with intended_ns 0 and done_ns 0"},
		{"errored before intended", 2, func(r *evidence.Record) { r.IntendedNs = 2 }, "record 2: errored with intended_ns 2 and done_ns 1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, v := stepsConfig(t)
			cell := cellOf("a", outcome{1, LineInTime, 0, 1}, outcome{1, loadgen.ErrStatus429, 0, 1}, outcome{4, LineUnsent, 0, 1})
			c.mutate(&cell.Records[c.index])
			if _, err := NewStepTables(cfg, v, []CellRecords{cell}); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("got %v, want %q", err, c.want)
			}
		})
	}
}
