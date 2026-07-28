// Package validity implements the SPEC.md §10 run-validity gates
// G1-G6, all run-failing. A run that fails any applicable gate is
// invalid and its numbers are not published as a characterization result.
//
// Three gates are derived from the harness artifacts and evaluate in
// Phase 0 and Phase 1 alike: G1 per-replica share (45-55% pre-fault),
// G2 client-validity, G6 baseline goodput near 100%. Three depend on
// real-infrastructure observations only available in Phase 1: G3
// zero-RSTs-from-the-dead-replica (packet capture), G4 endpoint-staleness
// window >= 20s (EndpointSlice polling), G5 GPU clock/power fingerprints
// equal (nvidia-smi). Those three take injected Observation values (concrete
// structs) so the gate LOGIC is tested with struct fixtures here; the
// collectors are wired at Phase 1 against the real cluster. A gate whose observation
// is absent is reported "not observed" and, when it is applicable to the
// variant, fails the run — an unobserved run-failing gate is never a
// silent pass.
package validity

import (
	"fmt"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/run"
)

// Gate is one run-validity gate result.
type Gate struct {
	ID         string `json:"id"` // "G1".."G6"
	Name       string `json:"name"`
	Applicable bool   `json:"applicable"`
	Observed   bool   `json:"observed"`
	Pass       bool   `json:"pass"`
	Detail     string `json:"detail"`
}

// Observations carries the Phase-1 infrastructure signals for the gates
// that cannot be derived from client-side artifacts. Zero-valued fields
// mean "not observed" (the gate then reports so and fails if applicable).
type Observations struct {
	// G3: client-side packet capture result for the black-hole variant.
	RSTCapture *RSTCaptureResult
	// G4: observed ready-EndpointSlice staleness window for the dead pod.
	EndpointStaleness *StalenessResult
	// G5: per-replica-per-run GPU clock/power fingerprints.
	GPUFingerprints []GPUFingerprint
}

// RSTCaptureResult is the G3 signal: how many TCP RSTs were sourced from
// the dead replica during the fault window (must be zero).
type RSTCaptureResult struct {
	Captured        bool `json:"captured"`
	RSTsFromDeadPod int  `json:"rsts_from_dead_pod"`
}

// StalenessResult is the G4 signal: the observed window during which the
// dead pod remained in ready EndpointSlices.
type StalenessResult struct {
	Observed         bool    `json:"observed"`
	StalenessWindowS float64 `json:"staleness_window_s"`
}

// GPUFingerprint is one replica's nvidia-smi clock/power fingerprint for
// one run (G5). Equality across replicas and runs is the gate.
type GPUFingerprint struct {
	Replica     string `json:"replica"`
	Run         int    `json:"run"`
	Fingerprint string `json:"fingerprint"`
}

// Report is the full G1-G6 evaluation for one run.
type Report struct {
	Gates   []Gate `json:"gates"`
	AllPass bool   `json:"all_pass"`
	Variant string `json:"variant"`
}

// Evaluate runs all six gates for one run's artifacts plus the Phase-1
// observations. The variant selects which gates are applicable (G3/G4 are
// black-hole only, §10).
func Evaluate(art *run.Artifacts, obs Observations) Report {
	rep := Report{Variant: art.Config.Fault.Variant}
	isBlackHole := art.Config.Fault.Variant == config.VariantBlackHole

	rep.Gates = []Gate{
		gateG1(art),
		gateG2(art),
		gateG3(art, obs, isBlackHole),
		gateG4(art, obs, isBlackHole),
		gateG5(art, obs),
		gateG6(art),
	}
	rep.AllPass = true
	for _, g := range rep.Gates {
		if g.Applicable && !g.Pass {
			rep.AllPass = false
		}
	}
	return rep
}

// G1: per-replica share 45-55% pre-fault (§1, §10). Applicable to
// multi-replica targets; derived from the harness share gate.
func gateG1(art *run.Artifacts) Gate {
	g := Gate{ID: "G1", Name: "per-replica share 45-55% pre-fault"}
	sg := art.ShareGate
	g.Applicable = sg.Applicable
	if !g.Applicable {
		g.Observed, g.Pass = true, true
		g.Detail = "single-replica target: share gate not applicable"
		return g
	}
	g.Observed = true
	g.Pass = sg.Pass
	g.Detail = fmt.Sprintf("shares=%v (band %d-%d%%)", sg.Shares, art.Config.ShareGate.MinPct, art.Config.ShareGate.MaxPct)
	return g
}

// G2: client-validity gate clean (§2, §10).
func gateG2(art *run.Artifacts) Gate {
	g := Gate{ID: "G2", Name: "client-validity gate clean", Applicable: true, Observed: true}
	cg := art.Loadgen.Gates
	g.Pass = cg.Pass
	g.Detail = fmt.Sprintf("skew p99=%dus/max=%dus, undispatched=%d, cpu_measured=%v worst=%.1f%%, gc p99=%.3fms",
		cg.SendSkewP99Us, cg.SendSkewMaxUs, cg.Undispatched, cg.CPUMeasured, cg.CPUWorstWindowPct, cg.GCPauseP99Ms)
	return g
}

// G3 (black-hole only): zero RSTs from the dead replica in the capture
// (§1 runtime assertion i, §10). Requires the packet-capture observation.
func gateG3(art *run.Artifacts, obs Observations, isBlackHole bool) Gate {
	g := Gate{ID: "G3", Name: "zero RSTs from the dead replica (packet capture)", Applicable: isBlackHole}
	if !isBlackHole {
		g.Observed, g.Pass = true, true
		g.Detail = "not the black-hole variant: RST-absence assertion not applicable"
		return g
	}
	if obs.RSTCapture == nil || !obs.RSTCapture.Captured {
		g.Observed, g.Pass = false, false
		g.Detail = "REQUIRES a client-side packet capture (Phase 1); a black-hole run without it is reported clean-variant-equivalent, not node-loss-representative (§1)"
		return g
	}
	g.Observed = true
	g.Pass = obs.RSTCapture.RSTsFromDeadPod == 0
	g.Detail = fmt.Sprintf("%d RSTs from the dead pod during the fault window (must be 0)", obs.RSTCapture.RSTsFromDeadPod)
	return g
}

// G4 (black-hole only): observed endpoint-staleness window >= 20s (§1
// runtime assertion ii, §10). Requires the EndpointSlice observation.
func gateG4(art *run.Artifacts, obs Observations, isBlackHole bool) Gate {
	g := Gate{ID: "G4", Name: "endpoint-staleness window >= 20s", Applicable: isBlackHole}
	if !isBlackHole {
		g.Observed, g.Pass = true, true
		g.Detail = "not the black-hole variant: staleness assertion not applicable"
		return g
	}
	minS := float64(art.Config.Fault.EndpointStalenessMinS)
	if obs.EndpointStaleness == nil || !obs.EndpointStaleness.Observed {
		g.Observed, g.Pass = false, false
		g.Detail = fmt.Sprintf("REQUIRES EndpointSlice polling (Phase 1); pinned minimum %.0fs (§1(ii)/§10 G4)", minS)
		return g
	}
	g.Observed = true
	g.Pass = obs.EndpointStaleness.StalenessWindowS >= minS
	g.Detail = fmt.Sprintf("observed %.1fs staleness window (minimum %.0fs)", obs.EndpointStaleness.StalenessWindowS, minS)
	return g
}

// G5: GPU clock and power fingerprints equal across replicas and runs
// (§6, §10). Requires the nvidia-smi fingerprints.
func gateG5(art *run.Artifacts, obs Observations) Gate {
	g := Gate{ID: "G5", Name: "GPU clock/power fingerprints equal across replicas and runs", Applicable: true}
	if len(obs.GPUFingerprints) == 0 {
		g.Observed, g.Pass = false, false
		g.Detail = "REQUIRES per-replica-per-run nvidia-smi fingerprints (Phase 1); no GPU in Phase 0"
		return g
	}
	// Coverage before equality: §10 requires equality ACROSS replicas and
	// runs, so a set that silently misses a replica must not pass
	// vacuously. (Cross-run coverage is the caller's obligation: the
	// campaign accumulates every run's fingerprints into one Evaluate
	// input; a per-run call attests that run only.)
	distinct := map[string]bool{}
	for _, fp := range obs.GPUFingerprints {
		distinct[fp.Replica] = true
	}
	if len(distinct) < art.Config.Target.Replicas {
		g.Observed, g.Pass = false, false
		g.Detail = fmt.Sprintf("fingerprints cover %d of %d replicas; incomplete observation cannot pass (§10 G5 is across replicas AND runs)", len(distinct), art.Config.Target.Replicas)
		return g
	}
	g.Observed = true
	first := obs.GPUFingerprints[0].Fingerprint
	g.Pass = true
	for _, fp := range obs.GPUFingerprints {
		if fp.Fingerprint != first {
			g.Pass = false
			g.Detail = fmt.Sprintf("fingerprint mismatch: %s/run%d = %q vs baseline %q", fp.Replica, fp.Run, fp.Fingerprint, first)
			return g
		}
	}
	g.Detail = fmt.Sprintf("%d fingerprints all equal (%q) across %d replicas", len(obs.GPUFingerprints), first, len(distinct))
	return g
}

// G6: baseline goodput near 100% (§4, §10). Derived from the detector's
// pre-fault baseline. "Near 100%" is read as the SLO's own near-100
// calibration requirement (§4): >= 0.99.
func gateG6(art *run.Artifacts) Gate {
	g := Gate{ID: "G6", Name: "baseline goodput near 100%", Applicable: true, Observed: true}
	base := 0.0
	if art.Detector != nil {
		base = art.Detector.PreFaultBaseline
	}
	g.Pass = base >= 0.99
	g.Detail = fmt.Sprintf("baseline goodput %.4f (must be >= 0.99; §4: near 100%% or the load calibration is wrong and is redone)", base)
	return g
}
