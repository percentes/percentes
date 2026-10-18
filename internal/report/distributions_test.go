package report

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

// distCell is a cell whose step 1 holds 100 completions, the ith with
// TTFT i ms, whole answer 10i ms, i%5+1 content events and, for even i,
// i provider-reported completion tokens; step 2 holds four completions
// at 1 s and six requests censored at the hard timeout; step 3 six
// completions at 2 s, two status_429, two censored and two unsent. Its
// gap histogram puts step 1's 200 gaps in the 20 ms and 500 ms bins.
func distCell(label string) CellRecords {
	c := CellRecords{Cell: label, Outcome: evidence.CellCounted}
	add := func(r evidence.Record) {
		r.Index, r.Shape, r.CeilingTokens = int64(len(c.Records)), "S", 512
		r.IntendedNs, r.DispatchNs = 10*sec, 10*sec+1
		c.Records = append(c.Records, r)
	}
	done := func(step int, ttftNs, wholeNs int64, events int) evidence.Record {
		return evidence.Record{Step: step, Outcome: loadgen.OutcomeCompleted, InTime: wholeNs <= 30*sec, Late: wholeNs > 30*sec,
			FirstTokNs: 10*sec + ttftNs, DoneNs: 10*sec + wholeNs, ContentEvents: events}
	}
	for i := 1; i <= 100; i++ {
		r := done(1, int64(i)*ms, int64(i)*10*ms, i%5+1)
		if i%2 == 0 {
			n := i
			r.CompletionTokens = &n
		}
		add(r)
	}
	censored := evidence.Record{Outcome: loadgen.OutcomeCensored, DoneNs: 100 * sec}
	for i := 0; i < 4; i++ {
		add(done(2, 100*ms, sec, 1))
	}
	for i := 0; i < 6; i++ {
		r := censored
		r.Step = 2
		add(r)
	}
	for i := 0; i < 6; i++ {
		add(done(3, 100*ms, 2*sec, 1))
	}
	for i := 0; i < 2; i++ {
		add(evidence.Record{Step: 3, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429, Status: 429, DoneNs: 10*sec + 50*ms})
		r := censored
		r.Step = 3
		add(r)
		add(evidence.Record{Step: 3})
		c.Records[len(c.Records)-1].DispatchNs = 0
	}
	counts := make([]int64, len(evidence.GapEdgesUs))
	counts[slices.Index(evidence.GapEdgesUs, 20_000)] = 150
	counts[slices.Index(evidence.GapEdgesUs, 500_000)] = 50
	c.Gaps = []evidence.GapHistogram{{Step: 1, Gaps: 200, Counts: counts}}
	return c
}

// Beside each curve, the completion-incidence quantiles inside the hard
// timeout or their refusals; per step, the TTFT and whole-answer
// distributions with the order-statistic intervals where their ranks
// fall inside the completions, the completion length as content events,
// provider-reported completion tokens and reasoning tokens, the content
// events of errored and censored requests, the provider-reported prompt
// tokens over the requests sent, and the inter-chunk gap histogram.
func TestStepTablesDistributions(t *testing.T) {
	cfg, v := stepsConfig(t)
	st, err := NewStepTables(cfg, v, []CellRecords{distCell("weekday-00")})
	if err != nil {
		t.Fatal(err)
	}
	if st.HardTimeoutS != 90 {
		t.Fatalf("hard timeout %d s", st.HardTimeoutS)
	}
	s1 := st.Cells[0].Steps[0]
	sig := s1.Signals
	want := Distribution{Quantiles: Quantiles{N: 100, P50Ns: 500 * ms, P90Ns: 900 * ms}, P95: &Tail{950 * ms, 900 * ms, 1000 * ms}}
	if d := sig.WholeAnswer; d.Quantiles != want.Quantiles || d.P95 == nil || *d.P95 != *want.P95 || d.P99 != nil {
		t.Errorf("whole answer %+v, p95 %+v, p99 %+v", d.Quantiles, d.P95, d.P99)
	}
	if d := sig.TTFT; d.N != 100 || d.P50Ns != 50*ms || d.P95 == nil || *d.P95 != (Tail{95 * ms, 90 * ms, 100 * ms}) || d.P99 != nil {
		t.Errorf("TTFT %+v, p95 %+v", d.Quantiles, d.P95)
	}
	if c := sig.ContentEvents; c != (Counts{Of: 100, N: 100, Min: 1, P50: 3, P90: 5, Max: 5}) {
		t.Errorf("content events %+v", c)
	}
	if c := sig.CompletionTokens; c != (Counts{Of: 100, N: 50, Min: 2, P50: 50, P90: 90, Max: 100}) {
		t.Errorf("completion tokens %+v", c)
	}
	if c := sig.ReasoningTokens; c != (Counts{Of: 100}) {
		t.Errorf("reasoning tokens %+v", c)
	}
	if g := sig.Gaps; g.N != 200 || len(g.Bins) != len(evidence.GapEdgesUs) || g.Bins[0] != (GapBin{1, 2, 0}) ||
		g.Bins[len(g.Bins)-1] != (GapBin{LoUs: 50_000_000}) || g.Bins[slices.Index(evidence.GapEdgesUs, 20_000)] != (GapBin{20_000, 50_000, 150}) {
		t.Errorf("gaps %+v", g)
	}

	type q struct {
		crossed      bool
		t            int64
		unattainable bool
	}
	check := func(step int, iq *IncidenceQuantiles, final, ceiling float64, want []q) {
		t.Helper()
		if iq == nil || iq.HorizonNs != 90*sec || !near(iq.FinalIncidence, final) || !near(iq.Ceiling, ceiling) || len(iq.Quantiles) != len(want) {
			t.Fatalf("step %d: %+v", step, iq)
		}
		for i, w := range want {
			if g := iq.Quantiles[i]; g.Q != []float64{0.5, 0.9, 0.95, 0.99}[i] || g.Crossed != w.crossed || g.TimeNs != w.t || g.Unattainable != w.unattainable {
				t.Errorf("step %d quantile %g: %+v", step, g.Q, g)
			}
		}
	}
	check(1, s1.Incidence, 1, 1, []q{{true, 500 * ms, false}, {true, 900 * ms, false}, {true, 950 * ms, false}, {true, 990 * ms, false}})
	check(2, st.Cells[0].Steps[1].Incidence, 0.4, 1, []q{{}, {}, {}, {}})
	check(3, st.Cells[0].Steps[2].Incidence, 0.5, 10.0/12, []q{{true, 2 * sec, false}, {false, 0, true}, {false, 0, true}, {false, 0, true}})

	text := st.Text()
	for _, frag := range []string{
		"  completion-incidence quantiles, Aalen-Johansen over scheduled, inside the 90 s hard timeout:\n" +
			"    median           0.500 s\n" +
			"    90th percentile  0.900 s\n" +
			"    95th percentile  0.950 s\n" +
			"    99th percentile  0.990 s\n",
		"    TTFT          n 100, median 50.0 ms, 90th percentile 90.0 ms; 95th percentile 95.0 ms, 95% order-statistic interval [90.0 ms, 100.0 ms]; 99th percentile not reported: its interval ranks fall outside the sample\n" +
			"    whole answer  n 100, median 500.0 ms, 90th percentile 900.0 ms; 95th percentile 950.0 ms, 95% order-statistic interval [900.0 ms, 1000.0 ms]; 99th percentile not reported: its interval ranks fall outside the sample\n",
		"  completion length over the 100 completions:\n" +
			"    content events                        min 1, median 3, 90th percentile 5, max 5\n" +
			"    provider-reported completion tokens   on 50 of 100: min 2, median 50, 90th percentile 90, max 100\n" +
			"    provider-reported reasoning tokens    on 0 of 100: not verifiable\n" +
			"  content events, errored requests:  n 0\n" +
			"  content events, censored requests: n 0\n" +
			"  prompt length over the 100 requests sent:\n" +
			"    provider-reported prompt tokens       on 0 of 100: not verifiable\n" +
			"  inter-chunk gaps between successive content events, over completions: n 200\n" +
			"    20 ms to under 50 ms   150 (75.00%)\n" +
			"    500 ms to under 1 s    50 (25.00%)\n" +
			"step 2, 0.5 rps, 10 scheduled:",
		"    median           > 90 s (ceiling 100.00%)\n    90th percentile  > 90 s (ceiling 100.00%)\n",
		"    median           2.000 s\n    90th percentile  unattainable (final completion incidence 50.00%; ceiling 83.33%)\n",
		"  inter-chunk gaps between successive content events, over completions: n 0\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}

	// A complete frame sums the cells' gap histograms and pools their
	// completions, so the 99th percentile's interval ranks fall inside the
	// 1,200 of step 1, and pools the curve: where six cells' censored
	// requests at step 2 are status_429 instead, its ceiling is 70
	// percent.
	var cells []CellRecords
	for i := 0; i < 12; i++ {
		c := distCell(fmt.Sprintf("cell-%02d", i))
		for k := range c.Records {
			if r := &c.Records[k]; i < 6 && r.Step == 2 && r.Outcome == loadgen.OutcomeCensored {
				r.Outcome, r.ErrClass, r.Status, r.DoneNs = loadgen.OutcomeErrored, loadgen.ErrStatus429, 429, 10*sec+50*ms
			}
		}
		cells = append(cells, c)
	}
	st, err = NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	p := st.Pooled[0]
	if d := p.Signals.WholeAnswer; d.N != 1200 || d.P99 == nil || *d.P99 != (Tail{990 * ms, 990 * ms, 1000 * ms}) {
		t.Errorf("pooled whole answer %+v, p99 %+v", d.Quantiles, d.P99)
	}
	if g := p.Signals.Gaps; g.N != 2400 || g.Bins[slices.Index(evidence.GapEdgesUs, 500_000)].Count != 600 {
		t.Errorf("pooled gaps %+v", g)
	}
	if c := p.Signals.CompletionTokens; c.Of != 1200 || c.N != 600 {
		t.Errorf("pooled completion tokens %+v", c)
	}
	check(2, st.Pooled[1].Incidence, 0.4, 0.7, []q{{}, {false, 0, true}, {false, 0, true}, {false, 0, true}})
	check(3, st.Pooled[2].Incidence, 0.5, 10.0/12, []q{{true, 2 * sec, false}, {false, 0, true}, {false, 0, true}, {false, 0, true}})
	text = st.Text()
	pooledPart := text[strings.Index(text, "-- Pooled per step"):]
	for _, frag := range []string{
		"; 99th percentile 990.0 ms, 95% order-statistic interval [990.0 ms, 1000.0 ms]\n",
		"  inter-chunk gaps between successive content events, over completions: n 2400\n",
		"    median           > 90 s (ceiling 70.00%)\n    90th percentile  unattainable (final completion incidence 40.00%; ceiling 70.00%)\n",
	} {
		if !strings.Contains(pooledPart, frag) {
			t.Errorf("pooled text lacks %q:\n%s", frag, pooledPart)
		}
	}
}

// A cell's gap histograms must agree with themselves and with its
// completions' content events.
func TestStepTablesGapRefusals(t *testing.T) {
	cfg, v := stepsConfig(t)
	for name, c := range map[string]struct {
		edit func(*CellRecords)
		want string
	}{
		"no histogram":       {func(c *CellRecords) { c.Gaps = nil }, "inter_chunk_gaps: map[] gaps per step, attempts.jsonl gives map[1:200]"},
		"another step":       {func(c *CellRecords) { c.Gaps[0].Step = 2 }, "map[2:200] gaps per step, attempts.jsonl gives map[1:200]"},
		"a content event":    {func(c *CellRecords) { c.Records[1].ContentEvents++ }, "map[1:200] gaps per step, attempts.jsonl gives map[1:201]"},
		"counts off the sum": {func(c *CellRecords) { c.Gaps[0].Counts[0] = 1 }, "step 1 counts sum to 201 of 200 gaps"},
	} {
		cell := distCell("weekday-00")
		c.edit(&cell)
		if _, err := NewStepTables(cfg, v, []CellRecords{cell}); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
}
