// Package stats implements the SPEC.md §7 single-stack statistics: the
// run-level scalar summary (all five values verbatim, median, mean, a
// t-interval with the pre-registered t=2.776 at df=4, and the min-max
// range), the run-to-run coefficient of variation that becomes the
// measured noise floor for the deferred cross-stack comparison, and Holm
// correction for a family of secondary comparisons.
//
// Normative rules encoded here:
//   - All five per-run values are published verbatim (§5).
//   - For plausibly heavy-tailed scalars (the TTRs), the median and the
//     min-max range lead; the t-interval carries a normality caveat and
//     is NOT the headline. Callers pass Heavy=true for such scalars.
//   - Bootstrap at N=5 is forbidden — there is no bootstrap anywhere.
//   - No MDE / power claim is made for the single-stack study.
package stats

import (
	"fmt"
	"math"
	"slices"
	"sort"
)

// studentTDF4 is the §7 pre-registered two-sided 95% t multiplier at df=4.
const studentTDF4 = 2.776

// studentT95 holds two-sided 95% t multipliers by degrees of freedom.
// §7 pre-registers df=4 (N=5 contributing runs); when dropped runs leave
// fewer contributors the multiplier for df=n-1 must be used: applying
// 2.776 at n<5 publishes an interval that is too narrow.
var studentT95 = map[int]float64{
	1: 12.706, 2: 4.303, 3: 3.182, 4: studentTDF4, 5: 2.571,
	6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
}

// tMultiplier returns the two-sided 95% multiplier for df. Beyond the
// table it returns the df=10 value, which is conservative (t decreases
// with df, so the interval is wider than exact, never narrower).
func tMultiplier(df int) float64 {
	if t, ok := studentT95[df]; ok {
		return t
	}
	return studentT95[10]
}

// Summary is the §7 run-level scalar summary over exactly the published
// per-run values.
type Summary struct {
	// Values are the per-run scalars verbatim, in run order (§5).
	Values []float64 `json:"values"`
	N      int       `json:"n"`

	Median float64 `json:"median"`
	Mean   float64 `json:"mean"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`

	// SampleSD is the sample standard deviation (n-1 denominator).
	SampleSD float64 `json:"sample_sd"`
	// SEM is SampleSD/sqrt(n).
	SEM float64 `json:"sem"`

	// TIntervalLo/Hi is the t-interval (mean ± t·SEM) at df = n-1. For
	// Heavy scalars this is reported with the normality caveat, never as
	// the headline. DF and TMultiplier record what was actually applied;
	// AtPinnedDF is true only when df==4, the §7 pre-registered case
	// (N=5 contributing runs) — a df<4 interval means runs were dropped
	// and the pinned §7 assumption was not met, which the report states.
	TIntervalLo float64 `json:"t_interval_lo"`
	TIntervalHi float64 `json:"t_interval_hi"`
	TMultiplier float64 `json:"t_multiplier"`
	DF          int     `json:"df"`
	AtPinnedDF  bool    `json:"at_pinned_df"`

	// CoV is the run-to-run coefficient of variation (SampleSD/mean),
	// the measured noise floor the cross-stack comparison's MDE will
	// build on (§7). CoVDefined
	// is false when the mean is zero OR n<2 (a single observation has no
	// dispersion; its SampleSD=0 is a default, not a real 0); readers
	// must check it.
	CoV        float64 `json:"coefficient_of_variation"`
	CoVDefined bool    `json:"cov_defined"`

	// Heavy marks a plausibly heavy-tailed scalar: the report leads with
	// median + range and caveats the t-interval.
	Heavy bool `json:"heavy_tailed"`
	// Headline is the spec-mandated lead statistic for this scalar:
	// "median (range lo-hi)" for Heavy, "mean (t-interval)" otherwise.
	Headline string `json:"headline"`
}

// Summarize computes the §7 summary. It panics on an empty input (a
// campaign always has N>=1 runs by construction; an empty slice is a
// programming error).
func Summarize(values []float64, heavy bool) Summary {
	n := len(values)
	if n == 0 {
		panic("stats: Summarize requires at least one value")
	}
	sorted := slices.Clone(values)
	sort.Float64s(sorted)

	s := Summary{
		Values: slices.Clone(values),
		N:      n,
		Min:    sorted[0],
		Max:    sorted[n-1],
		Median: median(sorted),
		Heavy:  heavy,
	}

	var sum float64
	for _, v := range values {
		sum += v
	}
	s.Mean = sum / float64(n)

	if n >= 2 {
		var ss float64
		for _, v := range values {
			d := v - s.Mean
			ss += d * d
		}
		s.SampleSD = math.Sqrt(ss / float64(n-1))
		s.SEM = s.SampleSD / math.Sqrt(float64(n))
		s.DF = n - 1
		s.TMultiplier = tMultiplier(s.DF)
		s.AtPinnedDF = s.DF == 4
		half := s.TMultiplier * s.SEM
		s.TIntervalLo = s.Mean - half
		s.TIntervalHi = s.Mean + half
	} else {
		// A single run has no dispersion estimate and no interval.
		s.TIntervalLo, s.TIntervalHi = s.Mean, s.Mean
	}

	if s.Mean != 0 && n >= 2 {
		s.CoV = s.SampleSD / s.Mean
		s.CoVDefined = true
	}

	if heavy {
		s.Headline = fmtHeadlineMedian(s)
	} else {
		s.Headline = fmtHeadlineMean(s)
	}
	return s
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func fmtHeadlineMedian(s Summary) string {
	return fmt.Sprintf("median %.3f (range %.3f–%.3f, N=%d); t-interval [%.3f, %.3f] reported with normality caveat",
		s.Median, s.Min, s.Max, s.N, s.TIntervalLo, s.TIntervalHi)
}

func fmtHeadlineMean(s Summary) string {
	return fmt.Sprintf("mean %.3f (95%% t-interval [%.3f, %.3f], t=%.3f df=%d), median %.3f",
		s.Mean, s.TIntervalLo, s.TIntervalHi, s.TMultiplier, s.DF, s.Median)
}
