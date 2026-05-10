package collect

import (
	"math"
	"testing"
)

func almost(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// Hand-computed product-limit check: events at 1,2,3s; censored at 2.5s.
// S(1)=3/4, S(2)=1/2, S(3)=0 (censored obs leaves risk set after 2).
func TestKMHandComputed(t *testing.T) {
	obs := []KMObservation{
		{TimeUs: 1_000_000, Completed: true},
		{TimeUs: 2_000_000, Completed: true},
		{TimeUs: 2_500_000, Completed: false},
		{TimeUs: 3_000_000, Completed: true},
	}
	c := EstimateKM(obs, 30_000_000)
	if c.N != 4 || len(c.Points) != 3 {
		t.Fatalf("want 4 obs / 3 event points, got n=%d points=%d", c.N, len(c.Points))
	}
	wantCompletion := []float64{0.25, 0.5, 1.0}
	wantAtRisk := []int{4, 3, 1}
	for i, p := range c.Points {
		if !almost(p.Completion, wantCompletion[i]) || p.AtRisk != wantAtRisk[i] {
			t.Errorf("point %d: completion=%v atRisk=%d, want %v/%d", i, p.Completion, p.AtRisk, wantCompletion[i], wantAtRisk[i])
		}
	}
	if q, ok := c.Quantile(0.5); !ok || q != 2_000_000 {
		t.Errorf("median: got %d ok=%v, want 2s", q, ok)
	}
	if got := c.CompletionAt(2_400_000); !almost(got, 0.5) {
		t.Errorf("CompletionAt(2.4s)=%v, want 0.5", got)
	}
}

// A quantile the curve never crosses inside the horizon must be reported
// as not-crossed ("p_q > 30 s"), never extrapolated.
func TestKMQuantileBeyondHorizon(t *testing.T) {
	obs := []KMObservation{
		{TimeUs: 1_000_000, Completed: true},
		{TimeUs: 30_000_000, Completed: false}, // censored at the timeout
		{TimeUs: 30_000_000, Completed: false},
		{TimeUs: 30_000_000, Completed: false},
	}
	c := EstimateKM(obs, 30_000_000)
	if _, ok := c.Quantile(0.5); ok {
		t.Error("median never crossed (only 25% complete); must report p_50 > horizon")
	}
	if q, ok := c.Quantile(0.25); !ok || q != 1_000_000 {
		t.Errorf("p25 should cross at 1s: got %d ok=%v", q, ok)
	}
}

// Ties at the same time: events first, censored observations at t remain
// at risk for events at t.
func TestKMTieConvention(t *testing.T) {
	obs := []KMObservation{
		{TimeUs: 1_000_000, Completed: true},
		{TimeUs: 1_000_000, Completed: false},
		{TimeUs: 2_000_000, Completed: true},
	}
	c := EstimateKM(obs, 30_000_000)
	// t=1: event with atRisk=3 -> S=2/3; censored leaves after.
	// t=2: S = 2/3 * (1 - 1/1) = 0.
	if !almost(c.Points[0].Completion, 1.0/3.0) || c.Points[0].AtRisk != 3 {
		t.Errorf("tie handling: %+v", c.Points[0])
	}
	if !almost(c.Points[1].Completion, 1.0) {
		t.Errorf("final completion: %+v", c.Points[1])
	}
}

func TestKMEmptyAndAllCensored(t *testing.T) {
	if c := EstimateKM(nil, 30_000_000); c.N != 0 || len(c.Points) != 0 {
		t.Error("empty window must yield an empty curve")
	}
	c := EstimateKM([]KMObservation{{TimeUs: 30_000_000, Completed: false}}, 30_000_000)
	if len(c.Points) != 0 {
		t.Error("all-censored window has no events, hence no curve points")
	}
	if _, ok := c.Quantile(0.01); ok {
		t.Error("no quantile is crossed when nothing completes")
	}
}
