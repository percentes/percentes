package stats

import (
	"math"
	"testing"
)

// The pinned t=2.776 applies only at df=4 (§7 assumes N=5 contributing
// runs). When dropped runs reduce n, the df-correct multiplier must be
// used — 2.776 at n=3 would publish an interval that is too narrow.
func TestTIntervalDFCorrect(t *testing.T) {
	s := Summarize([]float64{10, 12, 14}, true)
	if s.DF != 2 || s.TMultiplier != 4.303 || s.AtPinnedDF {
		t.Fatalf("n=3 must use df=2/t=4.303, not the pinned df=4: %+v", s)
	}
	// Hand-computed: mean 12, sd 2, sem 2/sqrt(3)=1.15470, half=4.96868.
	if math.Abs(s.TIntervalLo-(12-4.96868)) > 0.001 || math.Abs(s.TIntervalHi-(12+4.96868)) > 0.001 {
		t.Errorf("df=2 interval wrong: [%v, %v]", s.TIntervalLo, s.TIntervalHi)
	}

	s5 := Summarize([]float64{1, 2, 3, 4, 5}, false)
	if s5.DF != 4 || s5.TMultiplier != studentTDF4 || !s5.AtPinnedDF {
		t.Fatalf("n=5 must use the pre-registered df=4/t=2.776: %+v", s5)
	}
}

// A single observation has no dispersion: CoV must be undefined, never a
// misleading 0 (at n=1 SampleSD is left at its zero value).
func TestCoVUndefinedForSingleValue(t *testing.T) {
	s := Summarize([]float64{42}, true)
	if s.CoVDefined {
		t.Fatalf("CoV must be undefined at n=1, got defined CoV=%v", s.CoV)
	}
}
