package collect

import (
	"math"
	"testing"
)

func almost(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// Aalen-Johansen reduces to 1-KM when censoring is the only non-completion
// mechanism. Hand-computed: completions at 1,2,3s; timeout-censored at 2.5s.
// CIF(1)=1/4, CIF(2)=1/2, CIF(3)=1 (censored obs leaves the risk set after 2).
func TestAJPureCensoringEqualsKM(t *testing.T) {
	obs := []Obs{
		{TimeUs: 1_000_000, Kind: ObsCompletion},
		{TimeUs: 2_000_000, Kind: ObsCompletion},
		{TimeUs: 2_500_000, Kind: ObsCensored},
		{TimeUs: 3_000_000, Kind: ObsCompletion},
	}
	c := EstimateIncidence(obs, 30_000_000)
	if c.N != 4 || len(c.Points) != 3 {
		t.Fatalf("want 4 obs / 3 completion points, got n=%d points=%d", c.N, len(c.Points))
	}
	wantIncidence := []float64{0.25, 0.5, 1.0}
	wantAtRisk := []int{4, 3, 1}
	for i, p := range c.Points {
		if !almost(p.Incidence, wantIncidence[i]) || p.AtRisk != wantAtRisk[i] {
			t.Errorf("point %d: incidence=%v atRisk=%d, want %v/%d", i, p.Incidence, p.AtRisk, wantIncidence[i], wantAtRisk[i])
		}
	}
	if q, ok := c.Quantile(0.5); !ok || q != 2_000_000 {
		t.Errorf("median: got %d ok=%v, want 2s", q, ok)
	}
	if got := c.IncidenceAt(2_400_000); !almost(got, 0.5) {
		t.Errorf("IncidenceAt(2.4s)=%v, want 0.5", got)
	}
}

// An error is a competing terminal event, not a censored observation: the
// errored request can never complete, so it must not be treated as "still
// waiting". Completions at 1,2,3s with an ERROR at 2.5s:
// CIF(1)=1/4, CIF(2)=1/2; the error at 2.5 consumes overall survival
// (S: 1/2 -> 1/4) without adding completion incidence; CIF(3)=1/2+1/4=3/4.
// Under the old errors-as-censored KM this window claimed 1.0.
func TestAJErrorsCompete(t *testing.T) {
	obs := []Obs{
		{TimeUs: 1_000_000, Kind: ObsCompletion},
		{TimeUs: 2_000_000, Kind: ObsCompletion},
		{TimeUs: 2_500_000, Kind: ObsError},
		{TimeUs: 3_000_000, Kind: ObsCompletion},
	}
	c := EstimateIncidence(obs, 30_000_000)
	want := []float64{0.25, 0.5, 0.75}
	if len(c.Points) != 3 {
		t.Fatalf("want 3 completion points, got %d", len(c.Points))
	}
	for i, p := range c.Points {
		if !almost(p.Incidence, want[i]) {
			t.Errorf("point %d: incidence=%v, want %v", i, p.Incidence, want[i])
		}
	}
	if got := c.IncidenceAt(30_000_000); !almost(got, 0.75) {
		t.Errorf("final incidence=%v, want 0.75 (never 1.0: one request errored)", got)
	}
}

// The executed counterexample from the external reviews: 50 errors at 0.5s
// plus 50 completions at 1s. Errors-as-censored KM reported completion 1.000
// by 1s; the true fraction of scheduled requests that completed is 0.500.
func TestAJCounterexampleFiftyFifty(t *testing.T) {
	var obs []Obs
	for i := 0; i < 50; i++ {
		obs = append(obs, Obs{TimeUs: 500_000, Kind: ObsError})
		obs = append(obs, Obs{TimeUs: 1_000_000, Kind: ObsCompletion})
	}
	c := EstimateIncidence(obs, 30_000_000)
	if got := c.IncidenceAt(1_000_000); !almost(got, 0.5) {
		t.Errorf("incidence at 1s = %v, want 0.500 (old KM claimed 1.000)", got)
	}
	if _, ok := c.Quantile(0.9); ok {
		t.Error("p90 must be refused: only half the scheduled requests ever complete")
	}
}

// Ties at the same time: all terminal events at t (completions AND errors)
// share the same at-risk count. Completion at 1s + error at 1s + completion
// at 2s: CIF(1)=1/3 (n=3), S(1)=1/3, CIF(2)=1/3+1/3=2/3.
func TestAJTieConvention(t *testing.T) {
	obs := []Obs{
		{TimeUs: 1_000_000, Kind: ObsCompletion},
		{TimeUs: 1_000_000, Kind: ObsError},
		{TimeUs: 2_000_000, Kind: ObsCompletion},
	}
	c := EstimateIncidence(obs, 30_000_000)
	if !almost(c.Points[0].Incidence, 1.0/3.0) || c.Points[0].AtRisk != 3 {
		t.Errorf("tie handling: %+v", c.Points[0])
	}
	if !almost(c.Points[1].Incidence, 2.0/3.0) {
		t.Errorf("final incidence: %+v (old errors-as-censored KM claimed 1.0)", c.Points[1])
	}
}

// A quantile the curve never crosses inside the horizon must be reported
// as not-crossed ("p_q > 30 s"), never extrapolated.
func TestAJQuantileBeyondHorizon(t *testing.T) {
	obs := []Obs{
		{TimeUs: 1_000_000, Kind: ObsCompletion},
		{TimeUs: 30_000_000, Kind: ObsCensored}, // censored at the timeout
		{TimeUs: 30_000_000, Kind: ObsCensored},
		{TimeUs: 30_000_000, Kind: ObsCensored},
	}
	c := EstimateIncidence(obs, 30_000_000)
	if _, ok := c.Quantile(0.5); ok {
		t.Error("median never crossed (only 25% complete); must report p_50 > horizon")
	}
	if q, ok := c.Quantile(0.25); !ok || q != 1_000_000 {
		t.Errorf("p25 should cross at 1s: got %d ok=%v", q, ok)
	}
}

func TestAJEmptyAllCensoredAllErrored(t *testing.T) {
	if c := EstimateIncidence(nil, 30_000_000); c.N != 0 || len(c.Points) != 0 {
		t.Error("empty window must yield an empty curve")
	}
	c := EstimateIncidence([]Obs{{TimeUs: 30_000_000, Kind: ObsCensored}}, 30_000_000)
	if len(c.Points) != 0 {
		t.Error("all-censored window has no completions, hence no curve points")
	}
	if _, ok := c.Quantile(0.01); ok {
		t.Error("no quantile is crossed when nothing completes")
	}
	c = EstimateIncidence([]Obs{{TimeUs: 1_000_000, Kind: ObsError}}, 30_000_000)
	if len(c.Points) != 0 || c.IncidenceAt(30_000_000) != 0 {
		t.Error("all-errored window: incidence stays 0; errors never count as completions")
	}
}
