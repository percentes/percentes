package campaign

import (
	"context"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/run"
)

func f64(v float64) *float64 { return &v }

// fakeRunner returns pre-scripted artifacts per run index, so campaign
// aggregation is tested without a cluster. The seed offset per run is
// asserted to prove repetitions are independent-but-reproducible.
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

// N=5 clean-delete campaign: the equilibrium TTR is the PRIMARY endpoint,
// heavy-tailed (median-led), and its CoV is surfaced as the noise floor.
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

// A run with no estimable equilibrium contributes no equilibrium value —
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
