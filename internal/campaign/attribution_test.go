package campaign

import (
	"context"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/run"
)

// The §10 pre-registered in_flight_loss_fraction is defined over the
// killed replica (§3). A run without victim attribution contributes an
// explicitly-unscoped all-replica ratio under its own name and is
// DROPPED from the pre-registered endpoint — never silently merged.
func TestLossFractionRequiresVictimAttribution(t *testing.T) {
	cfg := baseCfg(t)
	cfg.Run.Repetitions = 2

	attributed := artWith(f64(10), f64(20), true, 0, 300, 40, true)
	attributed.VictimReplica = "pod-a"
	attributed.InFlight.OnVictim = 50
	attributed.InFlight.OnVictimErrored = 25 // victim-scoped fraction 0.5

	unattributed := artWith(f64(12), f64(22), true, 0.10, 310, 42, true) // all-replica 0.10, no victim

	var seeds []int64
	rep, err := Run(context.Background(), cfg, run.Options{}, config.VariantCleanDelete,
		fakeRunner([]*run.Artifacts{attributed, unattributed}, &seeds))
	if err != nil {
		t.Fatal(err)
	}

	if rep.PerRun[0].InFlightLossFraction == nil || *rep.PerRun[0].InFlightLossFraction != 0.5 {
		t.Errorf("victim-attributed run must publish the on-victim fraction 0.5: %+v", rep.PerRun[0])
	}
	if rep.PerRun[1].InFlightLossFraction != nil {
		t.Error("unattributed run must NOT publish under the pre-registered name")
	}
	if rep.PerRun[1].InFlightLossAllReplicasUnscoped == nil || *rep.PerRun[1].InFlightLossAllReplicasUnscoped != 0.10 {
		t.Errorf("unattributed run's all-replica ratio must appear under its own unscoped name: %+v", rep.PerRun[1])
	}

	for _, e := range rep.Endpoints {
		if e.Name != "in_flight_loss_fraction" {
			continue
		}
		if e.ContributingN != 1 || e.DroppedRuns != 1 || !strings.Contains(e.DroppedReason, "victim") {
			t.Errorf("endpoint must summarize only victim-attributed runs with the drop named: %+v", e)
		}
	}
}

// The cross-stack-MDE noise floor is the PRIMARY endpoint's CoV, clean_delete
// only (§7). A black-hole campaign's equilibrium CoV gets no such label.
func TestNoiseFloorOnlyForCleanDelete(t *testing.T) {
	cfg := baseCfg(t)
	cfg.Run.Repetitions = 2
	scripts := []*run.Artifacts{
		artWith(f64(10), f64(20), true, 0.5, 300, 40, true),
		artWith(f64(14), f64(24), true, 0.5, 320, 44, true),
	}
	var seeds []int64
	rep, err := Run(context.Background(), cfg, run.Options{}, config.VariantBlackHole,
		fakeRunner(scripts, &seeds))
	if err != nil {
		t.Fatal(err)
	}
	if rep.NoiseFloorCoV != nil {
		t.Fatalf("black_hole equilibrium CoV must not be labeled the cross-stack noise floor (secondary endpoint): %v", *rep.NoiseFloorCoV)
	}
	for _, e := range rep.Endpoints {
		if e.NoiseFloorNote != "" {
			t.Errorf("no endpoint may carry the noise-floor note under black_hole: %+v", e)
		}
	}
}
