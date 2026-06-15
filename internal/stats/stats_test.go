package stats

import (
	"math"
	"strings"
	"testing"
)

func approx(a, b, tol float64) bool { return math.Abs(a-b) <= tol }

// Hand-computed oracle for N=5. values = {10,12,14,16,18}.
// mean=14, median=14, min=10, max=18.
// sample variance = (16+4+0+4+16)/4 = 40/4 = 10; SD=sqrt(10)=3.16228.
// SEM = 3.16228/sqrt(5) = 1.41421.
// t-interval = 14 ± 2.776*1.41421 = 14 ± 3.92585 = [10.074, 17.926].
// CoV = 3.16228/14 = 0.225877.
func TestSummarizeHandComputed(t *testing.T) {
	s := Summarize([]float64{10, 12, 14, 16, 18}, false)
	if s.N != 5 || s.Mean != 14 || s.Median != 14 || s.Min != 10 || s.Max != 18 {
		t.Fatalf("basic stats: %+v", s)
	}
	if !approx(s.SampleSD, math.Sqrt(10), 1e-9) {
		t.Errorf("sample SD: got %v, want sqrt(10)", s.SampleSD)
	}
	if !approx(s.SEM, math.Sqrt(10)/math.Sqrt(5), 1e-9) {
		t.Errorf("SEM: got %v", s.SEM)
	}
	if !approx(s.TIntervalLo, 10.07415, 1e-4) || !approx(s.TIntervalHi, 17.92585, 1e-4) {
		t.Errorf("t-interval: got [%v, %v], want [10.074, 17.926]", s.TIntervalLo, s.TIntervalHi)
	}
	if s.TMultiplier != 2.776 {
		t.Errorf("t multiplier must be the pinned 2.776, got %v", s.TMultiplier)
	}
	if !approx(s.CoV, 0.225877, 1e-5) {
		t.Errorf("CoV: got %v, want 0.225877", s.CoV)
	}
}

// Even-N median is the mean of the two central order statistics.
func TestMedianEven(t *testing.T) {
	s := Summarize([]float64{1, 2, 3, 4}, false)
	if s.Median != 2.5 {
		t.Errorf("even-N median: got %v, want 2.5", s.Median)
	}
}

// Heavy-tailed scalars (the TTRs) lead with median + range; the headline
// must not lead with the mean/t-interval.
func TestHeavyTailedHeadline(t *testing.T) {
	s := Summarize([]float64{5, 6, 6, 7, 40}, true) // an outlier run
	if !s.Heavy {
		t.Fatal("Heavy flag must be set")
	}
	if s.Median != 6 {
		t.Errorf("median should be robust to the outlier: got %v", s.Median)
	}
	if !strings.HasPrefix(s.Headline, "median") {
		t.Errorf("heavy-tailed headline must lead with the median, got %q", s.Headline)
	}
	if !strings.Contains(s.Headline, "normality caveat") {
		t.Error("heavy-tailed headline must carry the normality caveat on the t-interval")
	}
}

func TestLightTailedHeadline(t *testing.T) {
	s := Summarize([]float64{10, 12, 14, 16, 18}, false)
	if !strings.HasPrefix(s.Headline, "mean") {
		t.Errorf("light-tailed headline leads with the mean, got %q", s.Headline)
	}
}

func TestSingleRun(t *testing.T) {
	s := Summarize([]float64{7}, true)
	if s.Mean != 7 || s.Median != 7 || s.SampleSD != 0 {
		t.Errorf("single run: %+v", s)
	}
	if s.TIntervalLo != 7 || s.TIntervalHi != 7 {
		t.Error("single run has no interval width")
	}
}

// Holm step-down: family of 4 raw p-values at alpha=0.05.
// sorted: 0.005, 0.01, 0.03, 0.04; thresholds 0.05/4, /3, /2, /1 =
// 0.0125, 0.01667, 0.025, 0.05. Reject 0.005 (<0.0125), reject 0.01
// (<0.01667), reject 0.03? 0.03 > 0.025 -> stop. So 0.03 and 0.04 not
// rejected. Adjusted: 0.005*4=0.02, 0.01*3=0.03, 0.03*2=0.06, 0.04*1=0.04
// -> monotone: 0.02, 0.03, 0.06, 0.06.
func TestHolmHandComputed(t *testing.T) {
	res := holm([]comparison{
		{Name: "d", PRaw: 0.04},
		{Name: "a", PRaw: 0.005},
		{Name: "c", PRaw: 0.03},
		{Name: "b", PRaw: 0.01},
	}, 0.05)

	by := map[string]holmResult{}
	for _, r := range res {
		by[r.Name] = r
	}
	if !by["a"].Rejected || !by["b"].Rejected {
		t.Errorf("a and b must be rejected: %+v", by)
	}
	if by["c"].Rejected || by["d"].Rejected {
		t.Errorf("c and d must NOT be rejected (step-down stops at c): %+v", by)
	}
	if !approx(by["a"].PAdjusted, 0.02, 1e-9) || !approx(by["b"].PAdjusted, 0.03, 1e-9) {
		t.Errorf("adjusted p a=%v b=%v, want 0.02, 0.03", by["a"].PAdjusted, by["b"].PAdjusted)
	}
	if !approx(by["c"].PAdjusted, 0.06, 1e-9) || !approx(by["d"].PAdjusted, 0.06, 1e-9) {
		t.Errorf("adjusted p must be monotone non-decreasing: c=%v d=%v, want 0.06, 0.06", by["c"].PAdjusted, by["d"].PAdjusted)
	}
	if by["a"].Rank != 1 || by["d"].Rank != 4 {
		t.Errorf("ranks: a=%d d=%d", by["a"].Rank, by["d"].Rank)
	}
}

func TestHolmEmpty(t *testing.T) {
	if len(holm(nil, 0.05)) != 0 {
		t.Error("empty family yields no results")
	}
}

// Holm never rejects more than Bonferroni would at the smallest p, and
// caps adjusted p at 1.
func TestHolmCapsAtOne(t *testing.T) {
	res := holm([]comparison{{Name: "x", PRaw: 0.9}, {Name: "y", PRaw: 0.8}}, 0.05)
	for _, r := range res {
		if r.PAdjusted > 1 {
			t.Errorf("adjusted p must cap at 1: %+v", r)
		}
		if r.Rejected {
			t.Errorf("high p-values must not be rejected: %+v", r)
		}
	}
}
