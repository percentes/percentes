package evidence

import (
	"slices"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/loadgen"
)

// A release run's gap histograms hold each measured completion's gaps
// per ladder step, a gap under 1 us in the first bin and one of 50 s or
// more in the last; warm-up requests and requests that did not complete
// are left out.
func TestStepGaps(t *testing.T) {
	res := &loadgen.Result{Requests: []loadgen.Request{
		{Step: 2, Outcome: loadgen.OutcomeCompleted, Tokens: 4, ITLsUs: []int64{0, 1, 3}},
		{Step: 1, Outcome: loadgen.OutcomeCompleted, Tokens: 3, ITLsUs: []int64{999_999, 60_000_000}},
		{Step: 1, Outcome: loadgen.OutcomeCompleted, Warmup: true, Tokens: 2, ITLsUs: []int64{5}},
		{Step: 1, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrMalformedStream, Tokens: 2, ITLsUs: []int64{5}},
		{Step: 3, Outcome: loadgen.OutcomeCompleted, Tokens: 1},
		{Step: 2, Outcome: loadgen.OutcomeCompleted, Tokens: 2, ITLsUs: []int64{20_000}},
	}}
	got := StepGaps(res)
	bins := func(at map[int64]int64) []int64 {
		c := make([]int64, len(GapEdgesUs))
		for edge, n := range at {
			c[slices.Index(GapEdgesUs, edge)] = n
		}
		return c
	}
	want := []GapHistogram{
		{Step: 1, Gaps: 2, Counts: bins(map[int64]int64{500_000: 1, 50_000_000: 1})},
		{Step: 2, Gaps: 4, Counts: bins(map[int64]int64{1: 2, 2: 1, 20_000: 1})},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i].Step != want[i].Step || got[i].Gaps != want[i].Gaps || !slices.Equal(got[i].Counts, want[i].Counts) {
			t.Errorf("step %d: %+v, want %+v", want[i].Step, got[i], want[i])
		}
	}
	recs := make([]Record, len(res.Requests))
	for i, r := range res.Requests {
		recs[i] = Record{Step: r.Step, Outcome: r.Outcome, Warmup: r.Warmup, ContentEvents: r.Tokens}
	}
	if err := CheckGaps(got, recs); err != nil {
		t.Errorf("the run's own records: %v", err)
	}
	if g := StepGaps(&loadgen.Result{}); g == nil || len(g) != 0 {
		t.Errorf("no requests: %#v", g)
	}
}

// Gap histograms must agree with themselves and with the gaps the
// records' completions give.
func TestCheckGaps(t *testing.T) {
	recs := []Record{
		{Step: 1, Outcome: loadgen.OutcomeCompleted, ContentEvents: 3},
		{Step: 2, Outcome: loadgen.OutcomeCompleted, ContentEvents: 2},
		{Step: 2, Outcome: loadgen.OutcomeCompleted, ContentEvents: 1},
		{Step: 2, Outcome: loadgen.OutcomeErrored, ContentEvents: 5},
		{Step: 1, Outcome: loadgen.OutcomeCompleted, Warmup: true, ContentEvents: 5},
	}
	valid := func() []GapHistogram {
		a, b := make([]int64, len(GapEdgesUs)), make([]int64, len(GapEdgesUs))
		a[0], a[3], b[9] = 1, 1, 1
		return []GapHistogram{{Step: 1, Gaps: 2, Counts: a}, {Step: 2, Gaps: 1, Counts: b}}
	}
	if err := CheckGaps(valid(), recs); err != nil {
		t.Fatal(err)
	}
	if err := CheckGaps(nil, nil); err != nil {
		t.Errorf("no gaps and no records: %v", err)
	}
	for name, c := range map[string]struct {
		edit func([]GapHistogram) []GapHistogram
		want string
	}{
		"step 0":          {func(g []GapHistogram) []GapHistogram { g[0].Step = 0; return g }, "inter_chunk_gaps: step 0"},
		"steps reversed":  {func(g []GapHistogram) []GapHistogram { g[0], g[1] = g[1], g[0]; return g }, "step 1 after step 2"},
		"step twice":      {func(g []GapHistogram) []GapHistogram { g[1].Step = 1; return g }, "step 1 after step 1"},
		"no gap":          {func(g []GapHistogram) []GapHistogram { g[1].Gaps, g[1].Counts[9] = 0, 0; return g }, "step 2 has 0 gaps"},
		"bins short":      {func(g []GapHistogram) []GapHistogram { g[0].Counts = g[0].Counts[1:]; return g }, "step 1 has 23 counts for 24 bins"},
		"negative count":  {func(g []GapHistogram) []GapHistogram { g[0].Counts[1], g[0].Counts[2] = -1, 1; return g }, "step 1 has a negative count"},
		"counts off":      {func(g []GapHistogram) []GapHistogram { g[0].Counts[5] = 1; return g }, "step 1 counts sum to 3 of 2 gaps"},
		"records differ":  {func(g []GapHistogram) []GapHistogram { g[1].Gaps, g[1].Counts[9] = 2, 2; return g }, "map[1:2 2:2] gaps per step, attempts.jsonl gives map[1:2 2:1]"},
		"a step missing":  {func(g []GapHistogram) []GapHistogram { return g[:1] }, "map[1:2] gaps per step, attempts.jsonl gives map[1:2 2:1]"},
		"none for gaps":   {func([]GapHistogram) []GapHistogram { return nil }, "map[] gaps per step, attempts.jsonl gives map[1:2 2:1]"},
		"another step":    {func(g []GapHistogram) []GapHistogram { g[1].Step = 3; return g }, "map[1:2 3:1] gaps per step, attempts.jsonl gives map[1:2 2:1]"},
		"bin count wrong": {func(g []GapHistogram) []GapHistogram { g[1].Counts = append(g[1].Counts, 0); return g }, "step 2 has 25 counts for 24 bins"},
	} {
		if err := CheckGaps(c.edit(valid()), recs); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
}
