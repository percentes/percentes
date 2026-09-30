package campaign

import (
	"context"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/run"
)

func f64(v float64) *float64 { return &v }

// fakeRunner returns pre-scripted artifacts per run index, so campaign
// aggregation is tested without a cluster; the seed offset per run is
// asserted.
func fakeRunner(scripts []*run.Artifacts, seenSeeds *[]int64) Runner {
	i := 0
	return func(ctx context.Context, cfg *config.Config, opts run.Options) (*run.Artifacts, error) {
		*seenSeeds = append(*seenSeeds, cfg.Run.Seed)
		art := scripts[i]
		i++
		return art, nil
	}
}

func artWith(ttrEq, ttrPf *float64, estimable bool, lossFrac float64, p95Ms float64, deficit float64, valid bool) *run.Artifacts {
	det := &detect.Result{
		EquilibriumEstimable: estimable,
		ToEquilibrium:        detect.Detection{TTRSeconds: ttrEq},
		ToPreFault:           detect.Detection{TTRSeconds: ttrPf},
		DeficitToPreFault:    deficit,
	}
	inf := collect.InFlightAccounting{}
	// Encode lossFrac as 100 in-flight with lossFrac*100 lost.
	inf.Total = 100
	inf.Errored = int(lossFrac * 100)
	fault := &collect.Stats{}
	fault.E2EConditional.Count = 10
	fault.E2EConditional.P95Us = int64(p95Ms * 1000)
	return &run.Artifacts{
		Detector: det,
		InFlight: inf,
		Windows:  map[string]*collect.Stats{"fault": fault},
		RunValid: valid,
	}
}

func baseCfg(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/experiment.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// A clean-delete campaign of N=5 runs (N, the repetition count): the
// equilibrium time to recovery (TTR) is the PRIMARY endpoint, heavy-tailed
// (median-led), and its coefficient of variation (CoV) is surfaced as the
// noise floor.
func TestCampaignPrimaryEndpoint(t *testing.T) {
	cfg := baseCfg(t)
	cfg.Fault.Variant = config.VariantCleanDelete
	cfg.Run.Repetitions = 5
	cfg.Run.Seed = 100

	scripts := []*run.Artifacts{
		artWith(f64(10), f64(20), true, 0.5, 300, 40, true),
		artWith(f64(12), f64(22), true, 0.4, 310, 42, true),
		artWith(f64(14), f64(24), true, 0.6, 320, 44, true),
		artWith(f64(16), f64(26), true, 0.5, 330, 46, true),
		artWith(f64(18), f64(28), true, 0.5, 340, 48, true),
	}
	var seeds []int64
	rep, err := Run(context.Background(), cfg, run.Options{}, config.VariantCleanDelete, fakeRunner(scripts, &seeds))
	if err != nil {
		t.Fatal(err)
	}

	if rep.ValidRuns != 5 || len(rep.PerRun) != 5 {
		t.Fatalf("want 5 valid runs: %+v", rep.ValidRuns)
	}
	// Independent-but-reproducible seeds: base+0..base+4.
	for i, s := range seeds {
		if s != 100+int64(i) {
			t.Errorf("run %d seed: got %d, want %d", i, s, 100+i)
		}
	}

	var eq *ScalarSummary
	for i := range rep.Endpoints {
		if rep.Endpoints[i].Name == "ttr_equilibrium_s" {
			eq = &rep.Endpoints[i]
		}
	}
	if eq == nil || eq.Endpoint != "primary" {
		t.Fatalf("equilibrium TTR must be the PRIMARY endpoint under clean_delete: %+v", eq)
	}
	if eq.Summary.Median != 14 || !eq.Summary.Heavy {
		t.Errorf("primary endpoint: median %v, heavy %v (want 14, true)", eq.Summary.Median, eq.Summary.Heavy)
	}
	if len(eq.Summary.Values) != 5 {
		t.Error("all five per-run values must be published verbatim (§5)")
	}
	if rep.NoiseFloorCoV == nil {
		t.Error("run-to-run CoV noise floor must be surfaced (§7)")
	}
}

// Under a non-clean-delete variant the equilibrium TTR is SECONDARY.
func TestCampaignSecondaryUnderBlackHole(t *testing.T) {
	cfg := baseCfg(t)
	cfg.Fault.Variant = config.VariantBlackHole
	cfg.Run.Repetitions = 3
	scripts := []*run.Artifacts{
		artWith(f64(10), f64(20), true, 0.9, 300, 40, true),
		artWith(f64(11), f64(21), true, 0.9, 300, 41, true),
		artWith(f64(12), f64(22), true, 0.9, 300, 42, true),
	}
	var seeds []int64
	rep, err := Run(context.Background(), cfg, run.Options{}, config.VariantBlackHole, fakeRunner(scripts, &seeds))
	if err != nil {
		t.Fatal(err)
	}
	var eq *ScalarSummary
	for i := range rep.Endpoints {
		if rep.Endpoints[i].Name == "ttr_equilibrium_s" {
			eq = &rep.Endpoints[i]
		}
	}
	if eq == nil {
		t.Fatal("equilibrium TTR summary must be present under black_hole")
	}
	if eq.Endpoint != "secondary" {
		t.Errorf("equilibrium TTR must be secondary under black_hole, got %q", eq.Endpoint)
	}
}

// A run with no estimable equilibrium contributes no equilibrium value;
// it is dropped and reported, never imputed (§7).
func TestCampaignDropsNonEstimable(t *testing.T) {
	cfg := baseCfg(t)
	cfg.Fault.Variant = config.VariantCleanDelete
	cfg.Run.Repetitions = 3
	scripts := []*run.Artifacts{
		artWith(f64(10), f64(20), true, 0.5, 300, 40, true),
		artWith(nil, f64(22), false, 0.5, 300, 42, true), // total outage: no equilibrium
		artWith(f64(14), f64(24), true, 0.5, 300, 44, true),
	}
	var seeds []int64
	rep, err := Run(context.Background(), cfg, run.Options{}, config.VariantCleanDelete, fakeRunner(scripts, &seeds))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rep.Endpoints {
		if e.Name == "ttr_equilibrium_s" {
			if e.ContributingN != 2 || e.DroppedRuns != 1 {
				t.Errorf("non-estimable run must be dropped, not imputed: contributing=%d dropped=%d", e.ContributingN, e.DroppedRuns)
			}
			if e.DroppedReason == "" {
				t.Error("dropped runs must carry a reason")
			}
			if len(e.Summary.Values) != 2 {
				t.Error("only contributing runs enter the summary values")
			}
		}
	}
}

// A black-hole run publishes the partition-heal recovery and its deficit
// as one pair. Without that field the crossing is published.
func TestBlackHoleScalarIsThePartitionHealRecovery(t *testing.T) {
	crossing, heal, healDeficit := 0.0, 135.0, 20.0
	art := &run.Artifacts{Detector: &detect.Result{ToPreFault: detect.Detection{TTRSeconds: &crossing}, PartitionHealRecovery: &detect.Detection{TTRSeconds: &heal}, DeficitToPartitionHeal: &healDeficit}}
	if s := extractScalars(1, art); s.TTRPreFaultS == nil || *s.TTRPreFaultS != heal || s.IntegratedDeficit != healDeficit {
		t.Fatalf("got %v deficit %v, want the partition-heal pair", s.TTRPreFaultS, s.IntegratedDeficit)
	}
	art.Detector.PartitionHealRecovery = nil
	if s := extractScalars(1, art); s.TTRPreFaultS == nil || *s.TTRPreFaultS != crossing || s.IntegratedDeficit != 0 {
		t.Fatalf("got %v, want the crossing when no heal field exists", s.TTRPreFaultS)
	}
}

// An unobserved hold is carried into the scalars and named apart from a
// run that never recovered.
func TestUnobservedHoldIsNamedInTheSummary(t *testing.T) {
	unobserved := &run.Artifacts{RunValid: true, Detector: &detect.Result{ToPreFault: detect.Detection{HoldUnobserved: true}}}
	never := &run.Artifacts{RunValid: true, Detector: &detect.Result{ToPreFault: detect.Detection{NotRecovered: true}}}
	ttr := 12.0
	recovered := &run.Artifacts{RunValid: true, Detector: &detect.Result{ToPreFault: detect.Detection{TTRSeconds: &ttr}}}
	rows := []Scalars{extractScalars(1, unobserved), extractScalars(2, never), extractScalars(3, recovered)}
	if !rows[0].TTRPreFaultUnobserved || rows[1].TTRPreFaultUnobserved {
		t.Fatalf("unobserved flag misplaced: %+v", rows[:2])
	}
	summaries, _ := summarize(rows, config.VariantCleanDelete)
	for _, s := range summaries {
		if s.Name == "ttr_pre_fault_s" {
			if s.DroppedRuns != 2 || !strings.Contains(s.DroppedReason, "1 runs never recovered") || !strings.Contains(s.DroppedReason, "1 ended before the hold could be observed") {
				t.Fatalf("drop reason must name both groups: %+v", s)
			}
			return
		}
	}
	t.Fatal("no ttr_pre_fault_s summary")
}

// The survivor percentile comes from the survivor cohort's window; the
// pooled fault-window percentile carries its own name.
func TestSurvivorP95IsTheCohortFigure(t *testing.T) {
	art := &run.Artifacts{Windows: map[string]*collect.Stats{
		"fault":          {E2EConditional: histo.Summary{Count: 20, P95Us: 2000895}},
		"fault_survivor": {E2EConditional: histo.Summary{Count: 10, P95Us: 100031}},
	}}
	s := extractScalars(1, art)
	if s.SurvivorP95Ms == nil || *s.SurvivorP95Ms != 100.031 || s.FaultWindowE2EP95Ms == nil || *s.FaultWindowE2EP95Ms != 2000.895 || s.SurvivorCohortAbsent {
		t.Fatalf("survivor %v pooled %v absent %v", s.SurvivorP95Ms, s.FaultWindowE2EP95Ms, s.SurvivorCohortAbsent)
	}
	delete(art.Windows, "fault_survivor")
	if s := extractScalars(1, art); s.SurvivorP95Ms != nil || !s.SurvivorCohortAbsent {
		t.Fatalf("without a cohort no survivor figure is published and the absence is recorded: %v %v", s.SurvivorP95Ms, s.SurvivorCohortAbsent)
	}
}

// A run without a survivor cohort is named as such in the survivor
// summary, apart from a cohort with no completed samples.
func TestSurvivorSummaryNamesTheMissingCohort(t *testing.T) {
	pooled := 12.5
	rows := []Scalars{{Run: 1, Valid: true, FaultWindowE2EP95Ms: &pooled, SurvivorCohortAbsent: true}}
	summaries, _ := summarize(rows, config.VariantCleanDelete)
	for _, s := range summaries {
		if s.Name == "survivor_p95_ms" {
			if s.DroppedRuns != 1 || !strings.Contains(s.DroppedReason, "1 runs had no survivor cohort") || !strings.Contains(s.DroppedReason, "fault_window_e2e_p95_ms") {
				t.Fatalf("reason must name the missing cohort and the pooled figure: %+v", s)
			}
			return
		}
	}
	t.Fatal("no survivor_p95_ms summary")
}
