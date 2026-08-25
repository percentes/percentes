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

// blackHoleArt is a black-hole run with per-request victim attribution and
// the victim-scoped in-flight outcome split G3 reads (§1(i)).
func blackHoleArt(inf collect.InFlightAccounting) *run.Artifacts {
	art := cleanArt(config.VariantBlackHole)
	art.VictimReplica = "pod-a"
	art.InFlight = inf
	return art
}

// bothReplicaFingerprints satisfies G5's coverage requirement (§10).
func bothReplicaFingerprints() []GPUFingerprint {
	return []GPUFingerprint{
		{Replica: "a", Run: 1, Fingerprint: "x"},
		{Replica: "b", Run: 1, Fingerprint: "x"},
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
// applicable to the variant; G5 with no fingerprint collector reports
// not applicable (as §10 treats G7), so a clean Phase 0 run is valid.
// Once fingerprints are supplied G5 gates again.
func TestCleanDeleteGatesWithoutHardware(t *testing.T) {
	rep := Evaluate(cleanArt(config.VariantCleanDelete), Observations{})
	if gateByID(rep, "G1").Pass != true || gateByID(rep, "G2").Pass != true || gateByID(rep, "G6").Pass != true {
		t.Error("artifact-derived gates G1/G2/G6 must pass on a clean run")
	}
	if gateByID(rep, "G3").Applicable || gateByID(rep, "G4").Applicable {
		t.Error("G3/G4 must be inapplicable to clean_delete")
	}
	g5 := gateByID(rep, "G5")
	if g5.Pass || g5.Observed || g5.Applicable {
		t.Error("G5 without a collector must report not applicable, never a silent pass")
	}
	if !rep.AllPass {
		t.Error("a clean Phase 0 run with no fingerprint collector must be valid")
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

// Black-hole applicability: G3 and G4 become applicable and, unobserved,
// strip the node-loss-representative label while the run stays valid (§10).
func TestBlackHoleRequiresAssertions(t *testing.T) {
	rep := Evaluate(cleanArt(config.VariantBlackHole), Observations{GPUFingerprints: bothReplicaFingerprints()})
	g3, g4 := gateByID(rep, "G3"), gateByID(rep, "G4")
	if !g3.Applicable || !g4.Applicable {
		t.Fatal("G3/G4 must be applicable to black_hole")
	}
	if g3.Observed || g3.Pass {
		t.Error("G3 must fail unobserved without per-request victim attribution (§1(i))")
	}
	if g4.Observed || g4.Pass {
		t.Error("G4 must fail unobserved without the staleness observation")
	}
	if rep.NodeLossRepresentative {
		t.Error("unobserved G3/G4 must strip the node-loss-representative label (§10)")
	}
	if !rep.AllPass {
		t.Errorf("G3/G4 are label-determining: the run stays valid (§10): %+v", rep.Gates)
	}
}

// Black-hole with satisfying artifacts and observations passes G3/G4: the
// victim's in-flight requests all ended completed or censored, and the
// stale endpoint was observed carrying traffic.
func TestBlackHoleAssertionsSatisfied(t *testing.T) {
	art := blackHoleArt(collect.InFlightAccounting{OnVictim: 12, OnVictimCompleted: 4, OnVictimCensored: 8})
	obs := Observations{
		EndpointStaleness: &StalenessResult{Observed: true, StalenessWindowS: 24, VictimTrafficObserved: true},
		GPUFingerprints:   bothReplicaFingerprints(),
	}
	rep := Evaluate(art, obs)
	if !gateByID(rep, "G3").Pass || !gateByID(rep, "G4").Pass {
		t.Errorf("satisfied assertions must pass: %+v", rep.Gates)
	}
	if !rep.AllPass {
		t.Errorf("all gates satisfied must be all-pass: %+v", rep.Gates)
	}
	if !rep.NodeLossRepresentative {
		t.Error("satisfied assertions must keep the node-loss-representative label (§10)")
	}
}

// G3 fails on any errored victim-attributed in-flight request; G4 fails
// below the pinned 20s.
func TestBlackHoleAssertionFailures(t *testing.T) {
	art := blackHoleArt(collect.InFlightAccounting{OnVictim: 12, OnVictimErrored: 3, OnVictimCensored: 9})
	obs := Observations{
		EndpointStaleness: &StalenessResult{Observed: true, StalenessWindowS: 12, VictimTrafficObserved: true},
		GPUFingerprints:   bothReplicaFingerprints(),
	}
	rep := Evaluate(art, obs)
	if gateByID(rep, "G3").Pass {
		t.Error("G3 must fail when a victim-attributed in-flight request errored")
	}
	if gateByID(rep, "G4").Pass {
		t.Error("G4 must fail when the staleness window is under the pinned 20s")
	}
	if rep.NodeLossRepresentative {
		t.Error("failed assertions must strip the node-loss-representative label (§10)")
	}
	if !rep.AllPass {
		t.Errorf("failed G3/G4 must leave the run valid (§10): %+v", rep.Gates)
	}
}

// A long-enough staleness window that never carried traffic fails G4: a
// stale EndpointSlice entry alone does not prove the routed path (§1(ii)).
func TestG4RequiresVictimBoundTraffic(t *testing.T) {
	art := blackHoleArt(collect.InFlightAccounting{OnVictim: 5, OnVictimCensored: 5})
	obs := Observations{
		EndpointStaleness: &StalenessResult{Observed: true, StalenessWindowS: 30},
		GPUFingerprints:   bothReplicaFingerprints(),
	}
	rep := Evaluate(art, obs)
	if gateByID(rep, "G4").Pass {
		t.Error("G4 must fail when no victim-bound traffic was observed inside the window")
	}
	if rep.NodeLossRepresentative {
		t.Error("a failed G4 must strip the node-loss-representative label (§10)")
	}
}

// The packet capture is recorded evidence: RSTs sourced from the dead pod
// are carried into the report and do not decide G3 (§1(i)).
func TestRSTCaptureIsRecordedNotGated(t *testing.T) {
	art := blackHoleArt(collect.InFlightAccounting{OnVictim: 6, OnVictimCensored: 6})
	obs := Observations{
		RSTCapture:        &RSTCaptureResult{Captured: true, RSTsFromDeadPod: 3},
		EndpointStaleness: &StalenessResult{Observed: true, StalenessWindowS: 24, VictimTrafficObserved: true},
		GPUFingerprints:   bothReplicaFingerprints(),
	}
	rep := Evaluate(art, obs)
	if !gateByID(rep, "G3").Pass {
		t.Errorf("G3 must derive from the victim-scoped outcome split, not the capture: %+v", gateByID(rep, "G3"))
	}
	if rep.RSTCapture == nil || rep.RSTCapture.RSTsFromDeadPod != 3 {
		t.Errorf("the capture must be recorded in the report: %+v", rep.RSTCapture)
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

// G6 fails below the pinned minimum baseline goodput.
func TestG6BaselineCalibration(t *testing.T) {
	art := cleanArt(config.VariantCleanDelete)
	art.Detector.PreFaultBaseline = 0.94
	if gateByID(Evaluate(art, Observations{}), "G6").Pass {
		t.Error("G6 must fail below the pinned 0.99 (load miscalibration, §10 G6)")
	}
}

// A failing gate outside the label-determining pair invalidates the run,
// even on a black-hole run whose assertions both hold (§10).
func TestNonLabelGateFailureInvalidatesRun(t *testing.T) {
	art := blackHoleArt(collect.InFlightAccounting{OnVictim: 6, OnVictimCensored: 6})
	art.Detector.PreFaultBaseline = 0.94
	obs := Observations{
		EndpointStaleness: &StalenessResult{Observed: true, StalenessWindowS: 24, VictimTrafficObserved: true},
		GPUFingerprints:   bothReplicaFingerprints(),
	}
	rep := Evaluate(art, obs)
	if rep.AllPass {
		t.Errorf("a failing G6 must invalidate the run: %+v", rep.Gates)
	}
	if !rep.NodeLossRepresentative {
		t.Error("satisfied G3/G4 must keep the label regardless of run validity (§10)")
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
