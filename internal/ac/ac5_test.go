package ac

import (
	"math"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/loadgen"
)

// runDetector executes a scenario and the full §5 analysis, returning the
// result plus the actual fire offset of the first scheduled fault.
func runDetector(t *testing.T, sc scenario) (*detect.Result, *loadgen.Result, int64) {
	t.Helper()
	cfg := buildConfig(t, sc)
	res, base := runScenario(t, cfg)
	recs := adminFaults(t, base)
	if len(recs) == 0 || recs[0].FiredAt == nil {
		t.Fatalf("no fired fault record: %+v", recs)
	}
	fireNs := recs[0].FiredAt.Sub(res.EpochWall).Nanoseconds()
	buckets := detect.BuildSeries(cfg, res.Requests, res.WarmupEndNs, res.RunEndNs)
	return detect.Run(cfg, buckets, res.WarmupEndNs, fireNs, res.FaultEndNs), res, fireNs
}

// TestAC5ScriptedRecovery: a scripted 20 s outage yields TTR within +-R
// (10 s) of the script; both baselines are reported distinctly.
func TestAC5ScriptedRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	const outageS = 20.0
	res, _, _ := func() (*detect.Result, *loadgen.Result, int64) {
		return runDetector(t, scenario{
			warmupS: 3, baselineS: 40, windowS: 60, cooldownS: 2, tInjectS: 40,
			ttft: fixed(100), itl: fixed(5),
			schedule: []config.MockFault{{Mode: config.MockFaultError, StartOffsetS: 43, DurationS: outageS}},
		})
	}()

	d := res.ToPreFault
	if d.NotRecovered || d.TTRSeconds == nil {
		t.Fatalf("AC5: scripted recovery not detected: %+v", d)
	}
	if err := math.Abs(*d.TTRSeconds - outageS); err > float64(config.PinnedDetectorWindowS) {
		t.Errorf("AC5: TTR %.1fs vs scripted %.0fs; must be within +-R=%ds", *d.TTRSeconds, outageS, config.PinnedDetectorWindowS)
	}
	// Both baselines reported DISTINCTLY: a total outage has no degraded
	// plateau, so the single-replica equilibrium is explicitly
	// not-estimable with its reason — never silently collapsed into the
	// post-recovery (= pre-fault) level, which is the §5 conflation. The
	// estimable path is covered by the detector's plateau unit test and
	// the two-replica kind e2e.
	if res.EquilibriumEstimable {
		t.Errorf("AC5: total-outage equilibrium must be not-estimable, got %.3f", res.EquilibriumBaseline)
	}
	if res.EquilibriumNote == "" || !res.ToEquilibrium.NotRecovered {
		t.Error("AC5: the equilibrium baseline must be reported distinctly (explicit not-estimable + reason)")
	}
	if res.PreFaultBaseline < 0.95 {
		t.Errorf("AC5: pre-fault baseline should be ~1.0 on a healthy mock, got %.3f", res.PreFaultBaseline)
	}
	if len(res.Sensitivity) != 27 {
		t.Errorf("AC5: sensitivity table must carry the full 3x3x3 sweep, got %d rows", len(res.Sensitivity))
	}
	t.Logf("AC5: TTR_prefault=%.1fs equilibrium=%s deficit=%.1f goodput-seconds",
		*d.TTRSeconds, res.EquilibriumNote, res.DeficitToPreFault)
}

// TestAC5HysteresisPreventsFlappingRecovery: a scripted flapping scenario
// (outage, brief service, two more flaps) must not yield the naive
// first-crossing TTR; recovery is declared only after the last flap.
func TestAC5HysteresisPreventsFlappingRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	// Outage [43,53), then 15 clean seconds — long enough for an entry
	// candidate to form — then a flap at [68,70) INSIDE that candidate's
	// 30 s hold. The hold must cancel the candidate (the oscillation-
	// induced early recovery), and only the post-flap entry survives.
	res, _, _ := runDetector(t, scenario{
		warmupS: 3, baselineS: 40, windowS: 60, cooldownS: 2, tInjectS: 40,
		ttft: fixed(100), itl: fixed(5),
		schedule: []config.MockFault{
			{Mode: config.MockFaultError, StartOffsetS: 43, DurationS: 10}, // main outage
			{Mode: config.MockFaultError, StartOffsetS: 68, DurationS: 2},  // flap during the hold
		},
	})

	d := res.ToPreFault
	if d.NotRecovered || d.TTRSeconds == nil {
		t.Fatalf("AC5: flapping scenario must eventually recover: %+v", d)
	}
	if d.CanceledEntries < 1 {
		t.Errorf("AC5: the flap inside the hold must CANCEL the first entry candidate (hysteresis); canceled=%d", d.CanceledEntries)
	}
	// Naive first-crossing claims ~10s; the flap clears at +27s.
	if *d.TTRSeconds < 20 {
		t.Errorf("AC5: hysteresis failed — TTR %.1fs is the oscillation-induced early value (naive ~10s; scripted stable point ~27s)", *d.TTRSeconds)
	}
	if *d.TTRSeconds > 32 {
		t.Errorf("AC5: TTR %.1fs is far beyond the scripted stable point ~27s", *d.TTRSeconds)
	}
	t.Logf("AC5: flapping TTR=%.1fs (naive ~10s), canceled entries=%d", *d.TTRSeconds, d.CanceledEntries)
}

// TestAC5NonRecovery: a fault outlasting the fault-window timeout is
// reported as non-recovery, never extrapolated.
func TestAC5NonRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	res, _, _ := runDetector(t, scenario{
		warmupS: 2, baselineS: 20, windowS: 30, cooldownS: 2, tInjectS: 20,
		ttft: fixed(100), itl: fixed(5),
		schedule: []config.MockFault{{Mode: config.MockFaultError, StartOffsetS: 22, DurationS: 40}}, // outlasts the window
	})

	if !res.ToPreFault.NotRecovered || res.ToPreFault.TTRSeconds != nil {
		t.Errorf("AC5: non-recovery past the timeout must be reported as such: %+v", res.ToPreFault)
	}
	// The two baselines answer different questions and must be distinct
	// here: the equilibrium tail is dead while pre-fault was healthy.
	if res.EquilibriumBaseline > 0.2 || res.PreFaultBaseline < 0.95 {
		t.Errorf("AC5: baselines must be computed from their own windows: pre=%.3f eq=%.3f", res.PreFaultBaseline, res.EquilibriumBaseline)
	}
}
