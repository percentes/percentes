package detect

import (
	"reflect"
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

// Clean scripted recovery: outage [40,60), full service after. Leading windows
// put entry at the fault-clear boundary: time to recovery (TTR) = 20 s.
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
	if res.EquilibriumNote == "" {
		t.Error("non-estimable equilibrium must carry its note")
	}
	// §5: a non-estimable equilibrium records no verdict.
	if res.ToEquilibrium.NotRecovered || res.ToEquilibrium.TTRSeconds != nil {
		t.Errorf("non-estimable equilibrium must yield no detection verdict: %+v", res.ToEquilibrium)
	}
	for _, row := range res.Sensitivity {
		if row.NotRecoveredEq || row.TTRToEquilibrium != nil {
			t.Fatalf("sensitivity rows must carry no equilibrium verdict either: %+v", row)
		}
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

// The pre-fault baseline stops at the guard start, one pinned 30 s client
// timeout before the fire anchor (§3, §5). Oracle: fire at 40 s puts the
// guard at [10s, 40s); the 10 s of baseline before it is clean and the
// guard is wholly bad, so the baseline is 1.0. Booking the guard to the
// baseline would give 200/800 = 0.25 and drop the entry bar with it.
func TestPreFaultBaselineExcludesGuardWindow(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := mkBuckets(160, func(s int) int {
		if s >= 10 && s < 60 { // guard window, then the outage after fire
			return 0
		}
		return 20
	})
	res := Run(cfg, buckets, 0, 40*sec, 160*sec)

	if res.PreFaultBaseline != 1.0 {
		t.Errorf("guard-window seconds must stay out of the pre-fault baseline: got %v, want 1.0", res.PreFaultBaseline)
	}
	for _, name := range []string{"ttft_slo", "e2e_slo"} {
		if c := res.Components[name]; c.Baseline != 1.0 {
			t.Errorf("component %s baseline must be guard-bounded too: got %v, want 1.0", name, c.Baseline)
		}
	}
	if res.ToPreFault.TTRSeconds == nil {
		t.Fatal("service returns at 60 s: recovery to the undiluted baseline must be detected")
	}
	if ttr := *res.ToPreFault.TTRSeconds; ttr < 19 || ttr > 21 {
		t.Errorf("TTR to pre-fault: %vs, want ~20s (fire 40 s, service back at 60 s)", ttr)
	}
}

// The §3 straddling-bucket rule applies at the new boundary: a guard start
// inside a bucket leaves that bucket in no window. Oracle: fire at 40.5 s
// puts the guard start at 10.5 s, so the bad bucket [10s, 11s) is dropped
// and the baseline is the clean 1.0; admitting it would give 200/220 = 0.909.
func TestPreFaultBaselineDropsBucketStraddlingGuardStart(t *testing.T) {
	cfg := pinnedParams(t)
	buckets := mkBuckets(160, func(s int) int {
		if s >= 10 && s < 60 {
			return 0
		}
		return 20
	})
	res := Run(cfg, buckets, 0, 40*sec+5e8, 160*sec)

	if res.PreFaultBaseline != 1.0 {
		t.Errorf("the bucket straddling the guard start belongs to no window: got %v, want 1.0", res.PreFaultBaseline)
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
// only when the fault clears; the two baselines answer different
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
		t.Errorf("equilibrium must be the degraded plateau level (~0.5), got %.3f; estimating from the post-recovery tail is the §5 conflation", res.EquilibriumBaseline)
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
	// A tail of one sweep window past the timeout, so every entry window
	// before it is observed in full.
	buckets := mkBuckets(140, func(s int) int {
		if s >= 40 {
			return 0
		}
		return 20
	})
	res := Run(cfg, buckets, 0, 40*sec, 120*sec)
	if !res.ToPreFault.NotRecovered || res.ToPreFault.TTRSeconds != nil || res.ToPreFault.HoldUnobserved {
		t.Errorf("non-recovery must be reported as such: %+v", res.ToPreFault)
	}
	for _, row := range res.Sensitivity {
		if row.HoldUnobservedPre {
			t.Errorf("a total outage has no candidate, so no sweep cell is unobserved: %+v", row)
		}
	}
	// A dead degraded plateau means no equilibrium exists, reported so,
	// with the pre-fault baseline intact and distinct.
	if res.EquilibriumEstimable || res.EquilibriumBaseline != 0 || res.PreFaultBaseline != 1 {
		t.Errorf("baselines must be distinct: estimable=%v eq=%v pre=%v",
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

// A hold cut off by the end of the series is flagged unobserved; with the
// data present it recovers.
func TestHoldUnobservedWhenTheSeriesEnds(t *testing.T) {
	cfg := pinnedParams(t)
	cfg.RecoveryDetector.HoldS = 60
	outage := func(s int) int {
		if s >= 40 && s < 60 {
			return 0
		}
		return 20
	}
	short := Run(cfg, mkBuckets(70, outage), 0, 40*sec, 160*sec)
	if !short.ToPreFault.HoldUnobserved || short.ToPreFault.NotRecovered || short.ToPreFault.TTRSeconds != nil {
		t.Fatalf("a hold cut off by the end of the series must be unobserved and nothing else: %+v", short.ToPreFault)
	}
	never := Run(cfg, mkBuckets(220, func(s int) int {
		if s >= 40 {
			return 0
		}
		return 20
	}), 0, 40*sec, 160*sec)
	if never.ToPreFault.HoldUnobserved || !never.ToPreFault.NotRecovered {
		t.Fatalf("a total outage with the series observed to its end is not recovered: %+v", never.ToPreFault)
	}
	full := Run(cfg, mkBuckets(160, outage), 0, 40*sec, 160*sec)
	pinned := Run(pinnedParams(t), mkBuckets(160, outage), 0, 40*sec, 160*sec)
	if full.ToPreFault.HoldUnobserved || full.ToPreFault.TTRSeconds == nil || *full.ToPreFault.TTRSeconds != *pinned.ToPreFault.TTRSeconds {
		t.Fatalf("with the hold observed the run recovers where the pinned hold does: %+v vs %+v", full.ToPreFault, pinned.ToPreFault)
	}
}

// Under the black-hole variant the raw crossing may precede the heal; the
// partition-heal recovery is the first held entry at or after it, and a
// series with no held entry after the heal leaves it not recovered.
func TestPartitionHealRecovery(t *testing.T) {
	cfg := pinnedParams(t)
	survivorThenOutage := func(s int) int {
		switch {
		case s < 40:
			return 20
		case s < 160:
			return 19 // the survivor holds above the entry bar
		case s < 175:
			return 0 // outage at the heal
		}
		return 20
	}
	heal := 160 * sec
	res := RunWith(cfg, Input{Buckets: mkBuckets(300, survivorThenOutage), WarmupEndNs: 0, FireAnchorNs: 40 * sec, TimeoutNs: 300 * sec, HealNs: &heal})
	if res.ToPreFault.TTRSeconds == nil || *res.ToPreFault.TTRSeconds != 0 {
		t.Fatalf("the raw crossing lands at the fire: %+v", res.ToPreFault)
	}
	// The leading window [174, 184) is the first at the entry bar after the
	// heal outage, so the held entry lands at 174 s: TTR 134 from the fire.
	if res.PartitionHealRecovery == nil || res.PartitionHealRecovery.TTRSeconds == nil || *res.PartitionHealRecovery.TTRSeconds != 134 || *res.PartitionHealRecovery.RecoveredAtNs < heal {
		t.Fatalf("partition-heal recovery must be the first held entry after the heal outage: %+v", res.PartitionHealRecovery)
	}
	if res.HealAnchorNs == nil || *res.HealAnchorNs != heal {
		t.Fatalf("heal anchor not recorded: %v", res.HealAnchorNs)
	}
	// 120 s of the survivor at 0.95 plus 14 s of outage before the held entry.
	if res.DeficitToPartitionHeal == nil || *res.DeficitToPartitionHeal < 19.99 || *res.DeficitToPartitionHeal > 20.01 {
		t.Fatalf("deficit to the partition-heal recovery must run to 174 s: %v", res.DeficitToPartitionHeal)
	}
	for _, row := range res.Sensitivity {
		if row.TTRPartitionHeal == nil && !row.NotRecoveredHeal && !row.HoldUnobservedHeal {
			t.Fatalf("a black-hole sweep row carries a partition-heal cell: %+v", row)
		}
		if row.TTRPartitionHeal != nil && *row.TTRPartitionHeal < 120 {
			t.Fatalf("a partition-heal sweep cell is never below the partition duration: %+v", row)
		}
		if row.Params == res.PartitionHealRecovery.Params && (row.TTRPartitionHeal == nil || *row.TTRPartitionHeal != 134) {
			t.Fatalf("the pinned sweep row must match the labelled recovery: %+v", row)
		}
	}
	early := func(s int) int {
		if s >= 40 && s < 158 {
			return 0
		}
		return 20
	}
	res = RunWith(cfg, Input{Buckets: mkBuckets(300, early), WarmupEndNs: 0, FireAnchorNs: 40 * sec, TimeoutNs: 300 * sec, HealNs: &heal})
	if res.ToPreFault.TTRSeconds == nil || *res.ToPreFault.TTRSeconds >= 120 {
		t.Fatalf("the raw crossing precedes the heal: %+v", res.ToPreFault)
	}
	if res.PartitionHealRecovery.TTRSeconds == nil || *res.PartitionHealRecovery.TTRSeconds != 120 {
		t.Fatalf("partition-heal recovery is the first held entry at the heal, 120 s: %+v", res.PartitionHealRecovery)
	}
	never := func(s int) int {
		if s >= 40 {
			return 0
		}
		return 20
	}
	res = RunWith(cfg, Input{Buckets: mkBuckets(320, never), WarmupEndNs: 0, FireAnchorNs: 40 * sec, TimeoutNs: 300 * sec, HealNs: &heal})
	if !res.PartitionHealRecovery.NotRecovered {
		t.Fatalf("no held entry after the heal must read not recovered: %+v", res.PartitionHealRecovery)
	}
	plain := Run(cfg, mkBuckets(300, never), 0, 40*sec, 300*sec)
	if plain.PartitionHealRecovery != nil || plain.HealAnchorNs != nil {
		t.Fatal("a run without a heal anchor carries no partition-heal field")
	}
}

// The collector's exact-window baseline, when supplied, is the baseline
// every detection and the deficit use.
func TestExactBaselineInputIsUsed(t *testing.T) {
	cfg := pinnedParams(t)
	exact := 0.987
	res := RunWith(cfg, Input{Buckets: mkBuckets(160, func(int) int { return 20 }), WarmupEndNs: 0, FireAnchorNs: 40 * sec, TimeoutNs: 160 * sec, PreFaultBaseline: &exact})
	if res.PreFaultBaseline != exact || res.ToPreFault.Baseline != exact {
		t.Fatalf("exact baseline ignored: pre=%v det=%v", res.PreFaultBaseline, res.ToPreFault.Baseline)
	}
	for _, row := range res.Sensitivity {
		if row.HoldUnobservedPre {
			t.Fatalf("a fully observed series must not mark the sweep unobserved: %+v", row)
		}
	}
}

// A window cut by the end of the series is a candidate only when fully
// observed; below the bar it is ruled out only when the unobserved seconds,
// each carrying the observed mean load and all of it good, would not reach
// the bar.
func TestEntryVerdictOnPartialWindows(t *testing.T) {
	zeros := mkBuckets(5, func(int) int { return 0 })
	if c, u := entryVerdict(zeros, 4, 10, compGoodput, 0.9); c || !u {
		t.Fatalf("one observed zero of ten with nine unobserved can still reach 0.9: candidate=%v unobserved=%v", c, u)
	}
	if c, u := entryVerdict(zeros, 4, 10, compGoodput, 0.95); c || u {
		t.Fatalf("nine of ten cannot reach 0.95: candidate=%v unobserved=%v", c, u)
	}
	if c, u := entryVerdict(zeros, 2, 10, compGoodput, 0.9); c || u {
		t.Fatalf("three observed zeros cannot reach 0.9: candidate=%v unobserved=%v", c, u)
	}
	full := mkBuckets(5, func(int) int { return 20 })
	if c, u := entryVerdict(full, 0, 10, compGoodput, 0.9); c || !u {
		t.Fatalf("a partial window above the bar is unobserved, never a candidate: candidate=%v unobserved=%v", c, u)
	}
	if c, u := entryVerdict(mkBuckets(10, func(int) int { return 20 }), 0, 10, compGoodput, 0.9); !c || u {
		t.Fatalf("a full window above the bar is a candidate: candidate=%v unobserved=%v", c, u)
	}
}

func TestNewDecompositionProcessKillRows(t *testing.T) {
	d := NewDecomposition(config.VariantProcessKill)
	noService := "one replica addressed directly: no Service"
	want := []struct{ name, source, note string }{
		{"reschedule", "api", "no scheduler: the container runtime restarts in place"},
		{"container_start", "api", "docker inspect unavailable"},
		{"log_bringup", "log", ""},
		{"engine_init", "log", ""},
		{"weight_download", "log", ""},
		{"weight_load", "log", ""},
		{"torch_compile", "log", ""},
		{"profile_kv_capture", "log", ""},
		{"engine_ready", "log", ""},
		{"server_ready", "log", ""},
		{"replica_ready", "probe", ""},
		{"traffic_restored", "probe", noService},
		{"routing_propagation", "probe", noService},
		{"goodput_restored", "client", ""},
	}
	if len(d.Segments) != len(want) {
		t.Fatalf("%d segments, want %d", len(d.Segments), len(want))
	}
	for i, w := range want {
		s := d.Segments[i]
		if s.Name != w.name || s.Source != w.source || s.Measured || s.Note == "" || (w.note != "" && s.Note != w.note) {
			t.Errorf("row %d = %+v, want %s from %s, unmeasured, note %q", i, s, w.name, w.source, w.note)
		}
	}
	if d.LogFigures != nil {
		t.Errorf("LogFigures = %v, want nil", d.LogFigures)
	}
}

func TestNewDecompositionKeepsPhase0Rows(t *testing.T) {
	names := []string{"reschedule", "container_start", "weight_load", "cuda_graph_capture", "replica_ready", "traffic_restored", "routing_propagation", "goodput_restored"}
	sources := []string{"api", "api", "log", "log", "probe", "probe", "probe", "client"}
	phase0 := NewPhase0Decomposition()
	for _, v := range []string{config.VariantMock, config.VariantCleanDelete, config.VariantBlackHole, config.VariantNone} {
		d := NewDecomposition(v)
		if !reflect.DeepEqual(d, phase0) {
			t.Errorf("%s: %+v differs from the Phase 0 table", v, d.Segments)
		}
		if len(d.Segments) != len(names) {
			t.Fatalf("%s: %d segments, want %d", v, len(d.Segments), len(names))
		}
		for i, s := range d.Segments {
			if s.Name != names[i] || s.Source != sources[i] || s.Measured || s.Note == "" {
				t.Errorf("%s row %d = %+v, want %s from %s, unmeasured with a note", v, i, s, names[i], sources[i])
			}
		}
	}
}
