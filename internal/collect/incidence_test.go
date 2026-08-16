package collect

import (
	"math"
	"testing"
)

func almost(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// Aalen-Johansen reduces to 1-KM when censoring is the only non-completion
// mechanism. Hand-computed: completions at 1,2,3s; censored at 2.5s
// (run-end). CIF(1)=1/4, CIF(2)=1/2, CIF(3)=1 (censored obs leaves the
// risk set after 2).
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
// An errors-as-censored estimator gives 1.0 here.
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

// 50 errors at 0.5s plus 50 completions at 1s: half the scheduled requests
// complete, so incidence at 1s is 0.500. An errors-as-censored estimator
// gives 1.000.
func TestAJCounterexampleFiftyFifty(t *testing.T) {
	var obs []Obs
	for i := 0; i < 50; i++ {
		obs = append(obs, Obs{TimeUs: 500_000, Kind: ObsError})
		obs = append(obs, Obs{TimeUs: 1_000_000, Kind: ObsCompletion})
	}
	c := EstimateIncidence(obs, 30_000_000)
	if got := c.IncidenceAt(1_000_000); !almost(got, 0.5) {
		t.Errorf("incidence at 1s = %v, want 0.500", got)
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
		t.Errorf("final incidence: %+v, want 2/3", c.Points[1])
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

// An undefined ObsKind is a programming error: EstimateIncidence must panic.
func TestAJInvalidKindPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("EstimateIncidence must panic on an undefined ObsKind")
		}
	}()
	EstimateIncidence([]Obs{{TimeUs: 1_000_000, Kind: ObsKind(99)}}, 30_000_000)
}

// Quantile panics on q outside (0, 1]; ok=false is reserved for the §3
// not-crossed refusal.
func TestAJQuantileOutOfRangePanics(t *testing.T) {
	c := EstimateIncidence([]Obs{{TimeUs: 1_000_000, Kind: ObsCompletion}}, 30_000_000)
	for _, q := range []float64{1.0000000005, 0, -0.5, 1.5} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Quantile(%v) must panic: outside (0, 1]", q)
				}
			}()
			c.Quantile(q)
		}()
	}
	if tUs, ok := c.Quantile(1.0); !ok || tUs != 1_000_000 {
		t.Errorf("Quantile(1.0) crosses at 1s when incidence reaches 1: got %d ok=%v", tUs, ok)
	}
}

// Re-basing to intended dispatch time can push a completion's TimeUs past
// the horizon by up to the max send-skew. The point stays on the curve;
// quantiles are never claimed past the horizon.
func TestAJRebasedOvershootBeyondHorizon(t *testing.T) {
	c := EstimateIncidence([]Obs{{TimeUs: 30_040_000, Kind: ObsCompletion}}, 30_000_000)
	if len(c.Points) != 1 {
		t.Fatalf("the overshoot completion is real and must stay on the curve: %+v", c.Points)
	}
	if _, ok := c.Quantile(0.5); ok {
		t.Error("no quantile may be claimed beyond the horizon")
	}
	if got := c.IncidenceAt(31_000_000); !almost(got, 1.0) {
		t.Errorf("IncidenceAt reports observed data: got %v, want 1.0", got)
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
