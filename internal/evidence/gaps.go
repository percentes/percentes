package evidence

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/percentes/percentes/internal/loadgen"
)

// GapEdgesUs are the lower edges, in microseconds, of the bins of an
// inter-chunk gap histogram; the last bin has no upper edge.
var GapEdgesUs = []int64{
	1, 2, 5, 10, 20, 50, 100, 200, 500,
	1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000,
	1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000,
}

// GapHistogram is one ladder step's gaps between successive content
// events over a release run's completions, the warm-up left out: Gaps in
// all and Counts per bin of GapEdgesUs. A gap under 1 us counts as 1 us
// (SPEC.md §3). The per-request gaps are in no published file
// (SPEC-HOSTED §9).
type GapHistogram struct {
	Step   int     `json:"step"`
	Gaps   int64   `json:"gaps"`
	Counts []int64 `json:"counts"`
}

// gapBin is the bin of GapEdgesUs that holds a gap of us microseconds.
func gapBin(us int64) int {
	return max(0, sort.Search(len(GapEdgesUs), func(i int) bool { return GapEdgesUs[i] > us })-1)
}

// StepGaps are the gap histograms of res's measured completions, one per
// ladder step with a gap, in step order.
func StepGaps(res *loadgen.Result) []GapHistogram {
	by := map[int]*GapHistogram{}
	for i := range res.Requests {
		r := &res.Requests[i]
		if r.Outcome != loadgen.OutcomeCompleted || r.Warmup || r.Step < 1 || len(r.ITLsUs) == 0 {
			continue
		}
		h := by[r.Step]
		if h == nil {
			h = &GapHistogram{Step: r.Step, Counts: make([]int64, len(GapEdgesUs))}
			by[r.Step] = h
		}
		for _, us := range r.ITLsUs {
			h.Counts[gapBin(us)]++
			h.Gaps++
		}
	}
	out := []GapHistogram{}
	for _, step := range sortedSteps(by) {
		out = append(out, *by[step])
	}
	return out
}

func sortedSteps[V any](m map[int]V) []int {
	steps := make([]int, 0, len(m))
	for k := range m {
		steps = append(steps, k)
	}
	sort.Ints(steps)
	return steps
}

// GapCounts are the gaps per ladder step that recs's measured
// completions give: each completion's content events less one.
func GapCounts(recs []Record) map[int]int64 {
	out := map[int]int64{}
	for i := range recs {
		r := &recs[i]
		if r.Outcome == loadgen.OutcomeCompleted && !r.Warmup && r.ContentEvents > 1 {
			out[r.Step] += int64(r.ContentEvents - 1)
		}
	}
	return out
}

// gapProblems reports every way gaps disagrees with itself: steps
// ascending from 1, each with at least one gap, one count per bin, the
// counts summing to its gaps.
func gapProblems(gaps []GapHistogram) []string {
	var problems []string
	fail := func(format string, a ...any) {
		problems = append(problems, "inter_chunk_gaps: "+fmt.Sprintf(format, a...))
	}
	for i, g := range gaps {
		switch {
		case g.Step < 1:
			fail("step %d", g.Step)
		case i > 0 && g.Step <= gaps[i-1].Step:
			fail("step %d after step %d", g.Step, gaps[i-1].Step)
		}
		if g.Gaps < 1 {
			fail("step %d has %d gaps", g.Step, g.Gaps)
		}
		if len(g.Counts) != len(GapEdgesUs) {
			fail("step %d has %d counts for %d bins", g.Step, len(g.Counts), len(GapEdgesUs))
			continue
		}
		var sum int64
		for _, n := range g.Counts {
			if n < 0 {
				fail("step %d has a negative count", g.Step)
			}
			sum += n
		}
		if sum != g.Gaps {
			fail("step %d counts sum to %d of %d gaps", g.Step, sum, g.Gaps)
		}
	}
	return problems
}

// CheckGaps reports an error when gaps disagrees with itself or with the
// gaps recs gives per step (GapCounts).
func CheckGaps(gaps []GapHistogram, recs []Record) error {
	problems := gapProblems(gaps)
	got := map[int]int64{}
	for _, g := range gaps {
		got[g.Step] = g.Gaps
	}
	if want := GapCounts(recs); len(problems) == 0 && !maps.Equal(got, want) {
		problems = append(problems, fmt.Sprintf("inter_chunk_gaps: %v gaps per step, %s gives %v", got, AttemptsFile, want))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
