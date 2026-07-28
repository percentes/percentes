package ac

import (
	"math"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/loadgen"
)

// tol returns the AC1 tolerance: ±2% or ±1ms, whichever is greater (us).
func tol(valueUs int64) int64 {
	t := valueUs * 2 / 100
	if t < 1000 {
		t = 1000
	}
	return t
}

func within(t *testing.T, name string, gotUs, wantUs int64) {
	t.Helper()
	if d := gotUs - wantUs; d < -tol(wantUs) || d > tol(wantUs) {
		t.Errorf("AC1 %s: got %dus, want %dus +-%dus (2%% or 1ms)", name, gotUs, wantUs, tol(wantUs))
	}
}

// TestAC1MeasurementCorrectness: reported p50/p95/p99 against a known
// injected distribution — uniform TTFT on [400,600] ms, fixed ITL 10 ms,
// so e2e = TTFT + 2550 ms and every percentile has a closed form.
func TestAC1MeasurementCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	cfg := buildConfig(t, scenario{
		warmupS: 5, baselineS: 30, windowS: 8, cooldownS: 2, tInjectS: 30,
		ttft: config.LatencyDist{Distribution: "uniform", MinMs: 400, MaxMs: 600},
		itl:  fixed(10),
	})
	res, _ := runScenario(t, cfg)

	// Baseline window, completed only, recorded via recordValue() into
	// the pinned histogram configuration.
	ttftH, e2eH := histo.New(cfg.Histogram), histo.New(cfg.Histogram)
	for _, r := range completedIn(res, res.WarmupEndNs, res.BaselineEndNs) {
		if err := ttftH.Record(r.TTFTNs() / 1000); err != nil {
			t.Fatal(err)
		}
		if err := e2eH.Record(r.E2ENs() / 1000); err != nil {
			t.Fatal(err)
		}
	}
	if ttftH.Count() < 500 {
		t.Fatalf("baseline window too thin: %d samples", ttftH.Count())
	}

	// Uniform[400,600]: p50=500, p95=590, p99=598 (ms).
	within(t, "TTFT p50", ttftH.Percentile(50), 500_000)
	within(t, "TTFT p95", ttftH.Percentile(95), 590_000)
	within(t, "TTFT p99", ttftH.Percentile(99), 598_000)
	within(t, "e2e p50", e2eH.Percentile(50), 3_050_000)
	within(t, "e2e p95", e2eH.Percentile(95), 3_140_000)
	within(t, "e2e p99", e2eH.Percentile(99), 3_148_000)

	if !res.Gates.Pass {
		t.Errorf("client-validity gate must pass on an unloaded run: %+v", res.Gates)
	}
}

// TestAC2CoordinatedOmissionPlumbing: a known mid-run stall of D is
// reflected in p99.9 and max within 5%% of D. With latencies re-based to
// intended dispatch time, the request that entered just before the freeze
// carries the full D; a generator that throttled in sympathy would show
// neither the tail nor the during-stall samples.
func TestAC2CoordinatedOmissionPlumbing(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	sr := getStallRun(t)
	res := sr.res

	// Nominal e2e measured from the pre-fault baseline (robust to
	// constant client/network overhead).
	baseH := histo.New(sr.cfg.Histogram)
	for _, r := range completedIn(res, res.WarmupEndNs, res.BaselineEndNs) {
		if err := baseH.Record(r.E2ENs() / 1000); err != nil {
			t.Fatal(err)
		}
	}
	nominalUs := baseH.Percentile(50)

	allH := histo.New(sr.cfg.Histogram)
	for _, r := range completedIn(res, res.WarmupEndNs, res.RunEndNs) {
		if err := allH.Record(r.E2ENs() / 1000); err != nil {
			t.Fatal(err)
		}
	}

	dUs := stallDS * 1e6
	for name, got := range map[string]int64{"p99.9": allH.Percentile(99.9), "max": allH.Max()} {
		excess := float64(got - nominalUs)
		if math.Abs(excess-dUs) > 0.05*dUs {
			t.Errorf("AC2 %s: excess over nominal = %.1fms, want %v +-5%% (%.0fms)", name, excess/1000, stallDS*1000, 0.05*dUs/1000)
		}
	}
}

// TestAC2bTailSampleCount: recorded samples attributable to the stall
// within +-10%% of D x lambda. Attributable = completed with excess-over-
// nominal >= 250 ms; with intended-time re-basing the expected count is
// lambda x (D - 0.25) ~= 195 for D=10, lambda=20 — an open-loop schedule
// keeps arrivals flowing through the freeze, so the stalled period
// contributes its full complement of samples.
func TestAC2bTailSampleCount(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	sr := getStallRun(t)
	res := sr.res

	baseH := histo.New(sr.cfg.Histogram)
	for _, r := range completedIn(res, res.WarmupEndNs, res.BaselineEndNs) {
		if err := baseH.Record(r.E2ENs() / 1000); err != nil {
			t.Fatal(err)
		}
	}
	nominalNs := baseH.Percentile(50) * 1000

	const excessThresholdNs = int64(250_000_000) // 0.25 s
	attributable := 0
	for _, r := range completedIn(res, res.WarmupEndNs, res.RunEndNs) {
		if r.E2ENs()-nominalNs >= excessThresholdNs {
			attributable++
		}
	}

	want := sr.cfg.Load.RateRPS * stallDS // D x lambda = 200
	if lo, hi := 0.9*want, 1.1*want; float64(attributable) < lo || float64(attributable) > hi {
		t.Errorf("AC2b: %d samples attributable to the stall, want within +-10%% of Dxlambda=%.0f [%.0f, %.0f]",
			attributable, want, lo, hi)
	}
}

// TestAC2cZeroUndispatched: every scheduled request must be dispatched;
// the send-skew gate numbers are enforced and reported.
func TestAC2cZeroUndispatched(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	sr := getStallRun(t)
	g := sr.res.Gates

	if g.Undispatched != 0 || !g.UndispatchedPass {
		t.Errorf("AC2c: %d scheduled-but-never-dispatched requests; must be zero", g.Undispatched)
	}
	if !g.SendSkewPass {
		t.Errorf("AC2c: send-skew gate failed: p99=%dus (limit %dms), max=%dus (limit %dms)",
			g.SendSkewP99Us, sr.cfg.ClientValidity.SendSkewP99Ms, g.SendSkewMaxUs, sr.cfg.ClientValidity.SendSkewMaxMs)
	}
	if g.SendSkewMaxUs == 0 && g.SendSkewP99Us == 0 && len(sr.res.Requests) > 0 {
		// The gate must be *reported*, not just pass vacuously; a real run
		// always has nonzero max skew at ns resolution.
		t.Error("AC2c: send-skew numbers not reported")
	}
	t.Logf("AC2c: skew p99=%dus max=%dus over %d requests", g.SendSkewP99Us, g.SendSkewMaxUs, len(sr.res.Requests))
}

// TestAC2cGateFiresOnSyntheticUndispatched: the gate itself is exercised
// with a synthetic result (a run abort mid-schedule) — an undispatched
// request must fail the run, not vanish.
func TestAC2cGateFiresOnSyntheticUndispatched(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	res := loadgen.SyntheticGateCheck()
	if res.UndispatchedPass || res.Pass {
		t.Error("AC2c: a scheduled-but-never-dispatched request must fail the run gate")
	}
}
