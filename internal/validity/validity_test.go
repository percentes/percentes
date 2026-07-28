package validity

import (
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/run"
)

func cleanArt(variant string) *run.Artifacts {
	cfg := &config.Config{
		Fault:     config.Fault{Variant: variant, EndpointStalenessMinS: 20},
		ShareGate: config.ShareGate{MinPct: 45, MaxPct: 55},
		Target:    config.Target{Replicas: 2}, // §1 pinned topology
	}
	return &run.Artifacts{
		Config:    cfg,
		Loadgen:   &loadgen.Result{Gates: loadgen.GateReport{Pass: true, CPUMeasured: true}},
		ShareGate: run.ShareGateResult{Applicable: true, Pass: true, Shares: map[string]float64{"a": 0.5, "b": 0.5}},
		Detector:  &detect.Result{PreFaultBaseline: 0.999},
		Windows:   map[string]*collect.Stats{},
	}
}

func gateByID(rep Report, id string) Gate {
	for _, g := range rep.Gates {
		if g.ID == id {
			return g
		}
	}
	return Gate{}
}

// A clean clean-delete run passes: G1/G2/G6 derive clean, G3/G4 are not
// applicable to the variant, G5 needs a fingerprint (absent in Phase 0),
// so G5 is the only failing gate — and it is applicable, so the run is
// invalid. This is correct: a real Phase 1 run MUST supply fingerprints.
func TestCleanDeleteGatesWithoutHardware(t *testing.T) {
	rep := Evaluate(cleanArt(config.VariantCleanDelete), Observations{})
	if gateByID(rep, "G1").Pass != true || gateByID(rep, "G2").Pass != true || gateByID(rep, "G6").Pass != true {
		t.Error("artifact-derived gates G1/G2/G6 must pass on a clean run")
	}
	if gateByID(rep, "G3").Applicable || gateByID(rep, "G4").Applicable {
		t.Error("G3/G4 must be inapplicable to clean_delete")
	}
	g5 := gateByID(rep, "G5")
	if g5.Pass || g5.Observed {
		t.Error("G5 must fail-unobserved without fingerprints (never a silent pass)")
	}
	if rep.AllPass {
		t.Error("a run missing a required GPU fingerprint must not be all-pass")
	}
}

// Full clean-delete pass with fingerprints supplied.
func TestCleanDeleteAllPass(t *testing.T) {
	obs := Observations{GPUFingerprints: []GPUFingerprint{
		{Replica: "a", Run: 1, Fingerprint: "1410MHz/300W"},
		{Replica: "b", Run: 1, Fingerprint: "1410MHz/300W"},
	}}
	rep := Evaluate(cleanArt(config.VariantCleanDelete), obs)
	if !rep.AllPass {
		t.Errorf("clean run with equal fingerprints must be all-pass: %+v", rep.Gates)
	}
}

// Black-hole applicability: G3 and G4 become run-failing and require
// their observations; without them the run is invalid (a black-hole run
// missing the assertions is clean-variant-equivalent, not node-loss, §1).
func TestBlackHoleRequiresAssertions(t *testing.T) {
	rep := Evaluate(cleanArt(config.VariantBlackHole), Observations{
		GPUFingerprints: []GPUFingerprint{{Replica: "a", Run: 1, Fingerprint: "x"}},
	})
	g3, g4 := gateByID(rep, "G3"), gateByID(rep, "G4")
	if !g3.Applicable || !g4.Applicable {
		t.Fatal("G3/G4 must be applicable to black_hole")
	}
	if g3.Pass || g4.Pass {
		t.Error("black-hole G3/G4 must fail without their observations")
	}
	if rep.AllPass {
		t.Error("black-hole run without capture/staleness observations must be invalid")
	}
}

// Black-hole with satisfying observations passes G3/G4.
func TestBlackHoleAssertionsSatisfied(t *testing.T) {
	obs := Observations{
		RSTCapture:        &RSTCaptureResult{Captured: true, RSTsFromDeadPod: 0},
		EndpointStaleness: &StalenessResult{Observed: true, StalenessWindowS: 24},
		// Coverage across BOTH replicas is part of G5 (§10).
		GPUFingerprints: []GPUFingerprint{
			{Replica: "a", Run: 1, Fingerprint: "x"},
			{Replica: "b", Run: 1, Fingerprint: "x"},
		},
	}
	rep := Evaluate(cleanArt(config.VariantBlackHole), obs)
	if !gateByID(rep, "G3").Pass || !gateByID(rep, "G4").Pass {
		t.Errorf("satisfied assertions must pass: %+v", rep.Gates)
	}
	if !rep.AllPass {
		t.Errorf("all gates satisfied must be all-pass: %+v", rep.Gates)
	}
}

// G3 fails on any RST; G4 fails below the pinned 20s.
func TestBlackHoleAssertionFailures(t *testing.T) {
	obs := Observations{
		RSTCapture:        &RSTCaptureResult{Captured: true, RSTsFromDeadPod: 3},
		EndpointStaleness: &StalenessResult{Observed: true, StalenessWindowS: 12},
		// Coverage across BOTH replicas is part of G5 (§10).
		GPUFingerprints: []GPUFingerprint{
			{Replica: "a", Run: 1, Fingerprint: "x"},
			{Replica: "b", Run: 1, Fingerprint: "x"},
		},
	}
	rep := Evaluate(cleanArt(config.VariantBlackHole), obs)
	if gateByID(rep, "G3").Pass {
		t.Error("G3 must fail when RSTs were sourced from the dead pod")
	}
	if gateByID(rep, "G4").Pass {
		t.Error("G4 must fail when the staleness window is under the pinned 20s")
	}
}

// G5 fails on any fingerprint mismatch across replicas/runs.
func TestG5FingerprintMismatch(t *testing.T) {
	obs := Observations{GPUFingerprints: []GPUFingerprint{
		{Replica: "a", Run: 1, Fingerprint: "1410MHz/300W"},
		{Replica: "b", Run: 1, Fingerprint: "1400MHz/300W"}, // throttled
	}}
	if gateByID(Evaluate(cleanArt(config.VariantCleanDelete), obs), "G5").Pass {
		t.Error("G5 must fail on a fingerprint mismatch")
	}
}

// G6 fails when the baseline goodput is not near 100%.
func TestG6BaselineCalibration(t *testing.T) {
	art := cleanArt(config.VariantCleanDelete)
	art.Detector.PreFaultBaseline = 0.94
	if gateByID(Evaluate(art, Observations{}), "G6").Pass {
		t.Error("G6 must fail when baseline goodput is not near 100% (load miscalibration, §4)")
	}
}

// G2 propagates a failing client-validity gate (e.g. unmeasured CPU).
func TestG2PropagatesClientGate(t *testing.T) {
	art := cleanArt(config.VariantCleanDelete)
	art.Loadgen.Gates.Pass = false
	art.Loadgen.Gates.CPUMeasured = false
	if gateByID(Evaluate(art, Observations{}), "G2").Pass {
		t.Error("G2 must fail when the client-validity gate failed")
	}
}
