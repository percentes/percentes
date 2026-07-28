package detect

import (
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// mkBuckets builds a synthetic 1 Hz series: 20 scheduled per second, with
// goodFn(sec) good outcomes.
func mkBuckets(totalS int, goodFn func(sec int) int) []Bucket {
	buckets := make([]Bucket, totalS)
	for i := range buckets {
		g := goodFn(i)
		buckets[i] = Bucket{StartNs: int64(i) * 1e9, Scheduled: 20, Completed: g, Good: g, GoodTTFT: g, GoodE2E: g}
	}
	return buckets
}

func pinnedParams(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

const sec = int64(1e9)

// Clean scripted recovery: outage [40,60), full service after. Leading
// windows put entry at the fault-clear boundary: TTR = 20 s.
func TestDetectCleanRecovery(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := mkBuckets(160, func(s int) int {
		if s >= 40 && s < 60 {
			return 0
		}
		return 20
	})
	res := Run(cfg, buckets, 0, 40*sec, 160*sec)

	if res.PreFaultBaseline != 1.0 {
		t.Fatalf("pre-fault baseline: %v", res.PreFaultBaseline)
	}
	if res.ToPreFault.TTRSeconds == nil || res.ToPreFault.NotRecovered {
		t.Fatal("must recover")
	}
	if ttr := *res.ToPreFault.TTRSeconds; ttr < 19 || ttr > 21 {
		t.Errorf("TTR to pre-fault: %vs, scripted 20s", ttr)
	}
	// A total outage has NO single-replica equilibrium (nothing served
	// during the degraded plateau): it must be reported not-estimable,
	// never silently estimated from the post-recovery tail (the §5
	// conflation).
	if res.EquilibriumEstimable {
		t.Errorf("total outage must not yield an estimable equilibrium (got %.3f)", res.EquilibriumBaseline)
	}
	if res.EquilibriumNote == "" || !res.ToEquilibrium.NotRecovered {
		t.Error("non-estimable equilibrium must carry its note and a non-recovery detection")
	}
	if res.DeficitToPreFault < 19 || res.DeficitToPreFault > 21 {
		t.Errorf("integrated deficit ~20 goodput-seconds for a 20s total outage, got %v", res.DeficitToPreFault)
	}
	if len(res.Sensitivity) != 27 {
		t.Errorf("sensitivity sweep must have 3x3x3=27 rows, got %d", len(res.Sensitivity))
	}
	if res.BacklogDrainMeasured {
		t.Error("backlog drain must be N/A in Phase 0")
	}
}

// Hysteresis: a dip during the hold cancels the candidate; recovery is
// only declared once service stays above the entry bar for the full hold.
func TestDetectHysteresisCancelsFlappingEntry(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := mkBuckets(200, func(s int) int {
		switch {
		case s >= 40 && s < 50: // outage
			return 0
		case s >= 70 && s < 72: // flap during the would-be hold
			return 0
		default:
			return 20
		}
	})
	res := Run(cfg, buckets, 0, 40*sec, 200*sec)

	d := res.ToPreFault
	if d.TTRSeconds == nil {
		t.Fatal("must eventually recover")
	}
	if d.CanceledEntries < 1 {
		t.Errorf("the flap at t=70 must cancel the first entry candidate, canceled=%d", d.CanceledEntries)
	}
	// Naive first-crossing would claim TTR=10s; hysteresis must hold out
	// past the flap (clean from 72, so TTR = 32s).
	if ttr := *d.TTRSeconds; ttr < 25 || ttr > 35 {
		t.Errorf("oscillation-resistant TTR: got %vs, want ~32s (naive would be 10s)", ttr)
	}
}

// Partial degradation: the single-replica equilibrium is the DEGRADED
// plateau level, detected quickly (the system settles into overloaded-
// but-stable service), while recovery to the pre-fault baseline comes
// only when the fault clears — the two baselines answer different
// questions and must not collapse.
func TestDetectEquilibriumPlateau(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := mkBuckets(220, func(s int) int {
		if s >= 40 && s < 120 {
			return 10 // 50% degraded plateau for 80s
		}
		return 20
	})
	res := Run(cfg, buckets, 0, 40*sec, 220*sec)

	if !res.EquilibriumEstimable {
		t.Fatalf("80s half-goodput plateau must be estimable: %+v", res.EquilibriumNote)
	}
	if res.EquilibriumBaseline < 0.45 || res.EquilibriumBaseline > 0.55 {
		t.Errorf("equilibrium must be the degraded plateau level (~0.5), got %.3f — estimating from the post-recovery tail is the §5 conflation", res.EquilibriumBaseline)
	}
	if res.ToEquilibrium.TTRSeconds == nil || *res.ToEquilibrium.TTRSeconds > 3 {
		t.Errorf("TTR to equilibrium should be ~0 (instant settle into the plateau): %+v", res.ToEquilibrium)
	}
	if res.ToPreFault.TTRSeconds == nil || *res.ToPreFault.TTRSeconds < 78 || *res.ToPreFault.TTRSeconds > 82 {
		t.Errorf("TTR to pre-fault should be ~80s (fault clear): %+v", res.ToPreFault)
	}
}

// Non-recovery past the timeout is reported as such.
func TestDetectNonRecovery(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := mkBuckets(120, func(s int) int {
		if s >= 40 {
			return 0
		}
		return 20
	})
	res := Run(cfg, buckets, 0, 40*sec, 120*sec)
	if !res.ToPreFault.NotRecovered || res.ToPreFault.TTRSeconds != nil {
		t.Errorf("non-recovery must be reported as such: %+v", res.ToPreFault)
	}
	// A dead degraded plateau means no equilibrium exists — reported so,
	// with the pre-fault baseline intact and distinct.
	if res.EquilibriumEstimable || res.EquilibriumBaseline != 0 || res.PreFaultBaseline != 1 {
		t.Errorf("baselines must be distinct and honest: estimable=%v eq=%v pre=%v",
			res.EquilibriumEstimable, res.EquilibriumBaseline, res.PreFaultBaseline)
	}
}

// Exit hysteresis: after confirmed recovery, a dip below the exit bar is
// a re-degradation; a dip that stays inside the [exit, entry) band is not.
func TestDetectExitBandAndReDegradation(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := mkBuckets(240, func(s int) int {
		switch {
		case s >= 40 && s < 50: // outage
			return 0
		case s >= 120 && s < 132: // hard re-degradation (0% < exit bar)
			return 0
		case s >= 180 && s < 192: // mild dip: 17/20 = 85% >= exit bar
			return 17
		default:
			return 20
		}
	})
	res := Run(cfg, buckets, 0, 40*sec, 240*sec)
	d := res.ToPreFault
	if d.TTRSeconds == nil {
		t.Fatal("must recover at t=50")
	}
	if d.ReDegradations != 1 {
		t.Errorf("exactly one re-degradation (the sub-exit dip at 120s; the 85%% dip at 180s stays in the hysteresis band): got %d", d.ReDegradations)
	}
}

// A fractional-second span must not panic: the final partial second gets
// its own bucket.
func TestBuildSeriesFractionalSpan(t *testing.T) {
	cfg := pinnedParams(t)
	reqs := []loadgen.Request{
		{IntendedNs: int64(32.4 * 1e9), DispatchNs: 1, DoneNs: int64(32.6 * 1e9), Outcome: loadgen.OutcomeCompleted},
	}
	buckets := BuildSeries(cfg, reqs, 0, int64(32.5*1e9))
	if len(buckets) != 33 {
		t.Fatalf("fractional span must round the bucket count up: got %d, want 33", len(buckets))
	}
	if buckets[32].Scheduled != 1 {
		t.Error("the request in the fractional tail must land in the final bucket")
	}
}

// Per-component recovery: an error-rate-only fault recovers on the error
// component; TTFT component recovers with it.
func TestDetectComponents(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := make([]Bucket, 160)
	for i := range buckets {
		b := Bucket{StartNs: int64(i) * 1e9, Scheduled: 20}
		if i >= 40 && i < 60 {
			b.Errored = 20 // hard error window
		} else {
			b.Completed, b.Good, b.GoodTTFT, b.GoodE2E = 20, 20, 20, 20
		}
		buckets[i] = b
	}
	res := Run(cfg, buckets, 0, 40*sec, 160*sec)
	for _, name := range []string{"ttft_slo", "e2e_slo", "error_rate"} {
		c, ok := res.Components[name]
		if !ok || c.TTRSeconds == nil {
			t.Errorf("component %s must be reported and recovered: %+v", name, c)
			continue
		}
		if ttr := *c.TTRSeconds; ttr < 19 || ttr > 21 {
			t.Errorf("component %s TTR: %v, want ~20s", name, ttr)
		}
	}
}
