package validity

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
)

// Zero victim-attributed in-flight requests cannot attest §1(i): G3 is
// unobserved (label cost), never a vacuous pass.
func TestG3ZeroAttributedIsUnobserved(t *testing.T) {
	rep := Evaluate(blackHoleArt(collect.InFlightAccounting{}), Observations{GPUFingerprints: bothReplicaFingerprints()})
	g := gateByID(rep, "G3")
	if g.Observed || g.Pass {
		t.Fatalf("zero attributed in-flight must be unobserved/failed, got observed=%v pass=%v", g.Observed, g.Pass)
	}
	if rep.NodeLossRepresentative {
		t.Fatal("unobserved G3 must strip the node-loss-representative label")
	}
	if !rep.AllPass {
		t.Fatal("unobserved G3 costs the label, never the run")
	}
}

// Until the nvidia-smi collector exists, G5 with no fingerprints is not
// applicable (as §10 treats G7), so a Phase 0 run can be valid.
func TestG5NotApplicableWithoutCollector(t *testing.T) {
	rep := Evaluate(cleanArt(config.VariantCleanDelete), Observations{})
	g := gateByID(rep, "G5")
	if g.Applicable {
		t.Fatal("G5 with no observations must be not applicable, not run-failing")
	}
	if !rep.AllPass {
		t.Fatal("a clean Phase 0 run with no GPU collector must be valid")
	}
}

// Injected fingerprints keep G5 applicable and gating.
func TestG5StillGatesWhenObserved(t *testing.T) {
	fps := bothReplicaFingerprints()
	fps[1].Fingerprint = "different"
	rep := Evaluate(cleanArt(config.VariantCleanDelete), Observations{GPUFingerprints: fps})
	g := gateByID(rep, "G5")
	if !g.Applicable || g.Pass {
		t.Fatalf("mismatched fingerprints must fail an applicable G5, got applicable=%v pass=%v", g.Applicable, g.Pass)
	}
	if rep.AllPass {
		t.Fatal("a failed G5 invalidates the run")
	}
}

// Hosted targets evaluate only G2 and G6; G6 is completion-within-timeout
// and reported, never run-invalidating (§6).
func TestHostedGateScope(t *testing.T) {
	art := cleanArt(config.VariantNone)
	art.Config.Target.Hosted = true
	art.Windows["baseline"] = &collect.Stats{Completed: 40, Errored: 50, Censored: 10}
	rep := Evaluate(art, Observations{})
	for _, id := range []string{"G1", "G3", "G4", "G5"} {
		if g := gateByID(rep, id); g.Applicable || g.Pass {
			t.Fatalf("%s must be not applicable and never passed against a hosted target", id)
		}
	}
	g6 := gateByID(rep, "G6")
	if !g6.Applicable || !g6.ReportedOnly {
		t.Fatalf("hosted G6 must be applicable and reported-only, got applicable=%v reportedOnly=%v", g6.Applicable, g6.ReportedOnly)
	}
	if g6.Pass {
		t.Fatal("0.40 completion-within-timeout must fail the 0.99 line")
	}
	if !strings.Contains(g6.Detail, "completion-within-timeout") {
		t.Fatalf("hosted G6 must be labeled with its definition, got %q", g6.Detail)
	}
	if !rep.AllPass {
		t.Fatal("a failed hosted G6 publishes with the gate marked failed; it does not invalidate the run")
	}
}
