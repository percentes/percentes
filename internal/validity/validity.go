// Package validity implements the SPEC.md §10 run-validity gates G1-G6. A
// run that fails any applicable gate other than G3 and G4 is invalid and
// its numbers are not published as a characterization result; failure or
// non-observation of G3 or G4 strips the node-loss-representative label
// and reports the run as clean-variant-equivalent, leaving it valid.
//
// Four gates are derived from the harness artifacts and evaluate in
// Phase 0 and Phase 1 alike: G1 per-replica share (45-55% pre-fault),
// G2 client-validity, G3 zero errored outcomes among the victim-attributed
// in-flight requests (§1(i)), G6 baseline goodput >= 0.99. Two depend
// on real-infrastructure observations only available in Phase 1: G4
// endpoint-staleness window >= 20s with victim-bound traffic observed in it
// (EndpointSlice polling), G5 GPU clock/power fingerprints equal
// (nvidia-smi). Those two take injected Observation values (concrete
// structs) so the gate LOGIC is tested with struct fixtures here; the
// collectors are wired at Phase 1 against the real cluster. G4 with no
// observation costs the label (§10); G5 with no fingerprints reports not
// applicable until the collector exists, as §10 already treats G7. Hosted
// targets evaluate only G2 and G6, with G6 redefined as
// completion-within-timeout and reported rather than run-invalidating
// (§6).
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
	// LabelDetermining marks the black-hole assertion gates G3 and G4:
	// failure or non-observation strips the node-loss-representative label
	// and leaves the run valid (§10).
	LabelDetermining bool `json:"label_determining"`
	// ReportedOnly marks the hosted G6: its result publishes with the run
	// rather than invalidating it, since withholding it would leave a
	// published set holding only endpoints that performed well (§6).
	ReportedOnly bool   `json:"reported_only,omitempty"`
	Detail       string `json:"detail"`
}

// Observations carries the Phase-1 infrastructure signals for the gates
// that cannot be derived from client-side artifacts. Zero-valued fields
// mean "not observed" (the gate then reports so and does not pass).
type Observations struct {
	// RSTCapture is mechanism evidence for the black-hole variant, recorded
	// in the Report and gating nothing (§1(i)).
	RSTCapture *RSTCaptureResult
	// G4: observed ready-EndpointSlice staleness window for the dead pod.
	EndpointStaleness *StalenessResult
	// G5: per-replica-per-run GPU clock/power fingerprints.
	GPUFingerprints []GPUFingerprint
}

// RSTCaptureResult records how many TCP RSTs were sourced from the dead
// replica during the fault window. Zero is expected by construction under a
// DROP-all partition, so it is a sanity check, never a gate (§1(i)).
type RSTCaptureResult struct {
	Captured        bool `json:"captured"`
	RSTsFromDeadPod int  `json:"rsts_from_dead_pod"`
}

// StalenessResult is the G4 signal: the observed window during which the
// dead pod remained in ready EndpointSlices.
type StalenessResult struct {
	Observed         bool    `json:"observed"`
	StalenessWindowS float64 `json:"staleness_window_s"`
	// VictimTrafficObserved records at least one post-T_inject connection or
	// request routed to the stale endpoint inside the window; a stale entry
	// alone does not establish that the dataplane still routed to the dead
	// pod (§1(ii)).
	VictimTrafficObserved bool `json:"victim_traffic_observed"`
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
	Gates []Gate `json:"gates"`
	// AllPass is the run's validity: every applicable gate except the
	// label-determining pair passed (§10).
	AllPass bool   `json:"all_pass"`
	Variant string `json:"variant"`
	// NodeLossRepresentative carries the §1 label: false when an applicable
	// label-determining gate failed or went unobserved, and the run is then
	// reported as clean-variant-equivalent (§10).
	NodeLossRepresentative bool `json:"node_loss_representative"`
	// RSTCapture carries the black-hole mechanism evidence through to the
	// report; it is recorded where taken and gates nothing (§1(i)).
	RSTCapture *RSTCaptureResult `json:"rst_capture,omitempty"`
}

// Evaluate runs all six gates for one run's artifacts plus the Phase-1
// observations. The variant selects which gates are applicable (G3/G4 are
// black-hole only, §10), and G3/G4 decide the node-loss-representative
// label rather than the run's validity.
func Evaluate(art *run.Artifacts, obs Observations) Report {
	rep := Report{Variant: art.Config.Fault.Variant, RSTCapture: obs.RSTCapture}
	isBlackHole := art.Config.Fault.Variant == config.VariantBlackHole

	if art.Config.Target.Hosted {
		// §6: against a hosted target only G2 and G6 are evaluable; the
		// rest report not applicable, never passed.
		na := func(id, name string) Gate {
			return Gate{ID: id, Name: name, Detail: "hosted target: reported as not applicable, never as passed (§6)"}
		}
		rep.Gates = []Gate{
			na("G1", "per-replica share 45-55% pre-fault"),
			gateG2(art),
			na("G3", "zero errored outcomes among victim-attributed in-flight requests"),
			na("G4", "endpoint-staleness window >= 20s with victim-bound traffic observed"),
			na("G5", "GPU clock/power fingerprints equal across replicas and runs"),
			gateG6Hosted(art),
			na("G7", "baseline queue stability: per-replica waiting-queue mean <= 1.0"),
		}
	} else {
		rep.Gates = []Gate{
			gateG1(art),
			gateG2(art),
			gateG3(art, isBlackHole),
			gateG4(art, obs, isBlackHole),
			gateG5(art, obs),
			gateG6(art),
			{ID: "G7", Name: "baseline queue stability: per-replica waiting-queue mean <= 1.0",
				Detail: "requires the server-gauge scrape (Phase 1); reported not applicable until it exists (§10)"},
		}
	}
	rep.AllPass = true
	// §6: a hosted run is not a run of the §1 experiment, so the label
	// never applies to it.
	rep.NodeLossRepresentative = isBlackHole && !art.Config.Target.Hosted
	for _, g := range rep.Gates {
		if !g.Applicable || g.Pass || g.ReportedOnly {
			continue
		}
		if g.LabelDetermining {
			rep.NodeLossRepresentative = false
			continue
		}
		rep.AllPass = false
	}
	return rep
}

// FailReasons lists the run-invalidating gate failures: applicable,
// failed, neither label-determining nor reported-only (§10).
func (r Report) FailReasons(skip ...string) []string {
	skipped := map[string]bool{}
	for _, id := range skip {
		skipped[id] = true
	}
	var out []string
	for _, g := range r.Gates {
		if skipped[g.ID] || !g.Applicable || g.Pass || g.LabelDetermining || g.ReportedOnly {
			continue
		}
		out = append(out, fmt.Sprintf("§10 %s failed: %s", g.ID, g.Detail))
	}
	return out
}

// G6 against a hosted target: completion-within-timeout, the fraction of
// scheduled requests completing before the pinned 30 s client timeout,
// because no hosted SLO exists (§6). Reported, never run-invalidating.
func gateG6Hosted(art *run.Artifacts) Gate {
	g := Gate{
		ID:           "G6",
		Name:         fmt.Sprintf("completion-within-timeout >= %.2f (hosted G6 definition, §6)", config.PinnedBaselineGoodputMin),
		Applicable:   true,
		Observed:     true,
		ReportedOnly: true,
	}
	// The derived fault_degraded/fault_recovered windows overlap the fault
	// window; only the phase windows enter the fraction.
	var completed, scheduled int
	for _, name := range []string{"baseline", "guard", "fault"} {
		w, ok := art.Windows[name]
		if !ok {
			continue
		}
		completed += w.Completed
		scheduled += w.Completed + w.Errored + w.Censored
	}
	if scheduled == 0 {
		g.Observed = false
		g.Detail = "no scheduled requests in any window; completion-within-timeout not observable"
		return g
	}
	frac := float64(completed) / float64(scheduled)
	g.Pass = frac >= config.PinnedBaselineGoodputMin
	g.Detail = fmt.Sprintf("completion-within-timeout %.4f over %d scheduled (pinned line %.2f, §6; reported, not run-invalidating)", frac, scheduled, config.PinnedBaselineGoodputMin)
	return g
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
	band := fmt.Sprintf("band %d-%d%%", art.Config.ShareGate.MinPct, art.Config.ShareGate.MaxPct)
	if !sg.BandEnforced {
		g.Name = "per-replica share pre-fault (descriptive: per-connection dataplane)"
		band += ", descriptive"
	}
	g.Detail = fmt.Sprintf("shares=%v (%s)", sg.Shares, band)
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

// G3 (black-hole only): zero errored outcomes among the victim-attributed
// in-flight requests (§1 runtime assertion i, §10). Derived from the
// run's own artifacts; the outcome model leaves every non-completing one
// censored at the pinned timeout.
func gateG3(art *run.Artifacts, isBlackHole bool) Gate {
	g := Gate{ID: "G3", Name: "zero errored outcomes among victim-attributed in-flight requests", Applicable: isBlackHole, LabelDetermining: true}
	if !isBlackHole {
		g.Observed, g.Pass = true, true
		g.Detail = "not the black-hole variant: client-silence assertion not applicable"
		return g
	}
	if art.VictimReplica == "" {
		g.Observed, g.Pass = false, false
		g.Detail = "REQUIRES per-request victim attribution; a black-hole run without it is reported clean-variant-equivalent, not node-loss-representative (§1(i))"
		return g
	}
	inf := art.InFlight
	if inf.OnVictim == 0 {
		g.Observed, g.Pass = false, false
		g.Detail = "zero victim-attributed in-flight requests at fire: header-based attribution misses requests the victim never answered, so §1(i) cannot be attested; label stripped, run stays valid"
		return g
	}
	g.Observed = true
	g.Pass = inf.OnVictimErrored == 0
	g.Detail = fmt.Sprintf("in flight on victim %s at fire: %d completed, %d errored (must be 0), %d censored at the pinned timeout",
		art.VictimReplica, inf.OnVictimCompleted, inf.OnVictimErrored, inf.OnVictimCensored)
	return g
}

// G4 (black-hole only): observed endpoint-staleness window >= 20s with
// victim-bound traffic observed inside it (§1 runtime assertion ii,
// §10). Requires the EndpointSlice observation.
func gateG4(art *run.Artifacts, obs Observations, isBlackHole bool) Gate {
	g := Gate{ID: "G4", Name: "endpoint-staleness window >= 20s with victim-bound traffic observed", Applicable: isBlackHole, LabelDetermining: true}
	if !isBlackHole {
		g.Observed, g.Pass = true, true
		g.Detail = "not the black-hole variant: staleness assertion not applicable"
		return g
	}
	minS := float64(art.Config.Fault.EndpointStalenessMinS)
	if obs.EndpointStaleness == nil || !obs.EndpointStaleness.Observed {
		g.Observed, g.Pass = false, false
		g.Detail = fmt.Sprintf("REQUIRES EndpointSlice polling and routed-path observation (Phase 1); pinned minimum %.0fs (§1(ii)/§10 G4)", minS)
		return g
	}
	st := obs.EndpointStaleness
	g.Observed = true
	g.Pass = st.StalenessWindowS >= minS && st.VictimTrafficObserved
	g.Detail = fmt.Sprintf("observed %.1fs staleness window (minimum %.0fs), victim-bound traffic routed to the stale endpoint: %v",
		st.StalenessWindowS, minS, st.VictimTrafficObserved)
	return g
}

// G5: GPU clock and power fingerprints equal across replicas and runs
// (§6, §10). Requires the nvidia-smi fingerprints.
func gateG5(art *run.Artifacts, obs Observations) Gate {
	g := Gate{ID: "G5", Name: "GPU clock/power fingerprints equal across replicas and runs", Applicable: true}
	if len(obs.GPUFingerprints) == 0 {
		// The nvidia-smi collector is wired at Phase 1; until observations
		// exist the gate reports not applicable, as §10 treats G7.
		g.Applicable, g.Observed, g.Pass = false, false, false
		g.Detail = "nvidia-smi fingerprint collector not wired (Phase 1); reported not applicable until it exists"
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

// G6: baseline goodput at least the pinned minimum (§10 G6). Derived
// from the detector's pre-fault baseline, which ends at the guard start (§3):
// the guard window never enters this gate.
func gateG6(art *run.Artifacts) Gate {
	g := Gate{ID: "G6", Name: fmt.Sprintf("baseline goodput >= %.2f", config.PinnedBaselineGoodputMin), Applicable: true, Observed: true}
	base := 0.0
	if art.Detector != nil {
		base = art.Detector.PreFaultBaseline
	}
	g.Pass = base >= config.PinnedBaselineGoodputMin
	g.Detail = fmt.Sprintf("baseline goodput %.4f (pinned minimum %.2f, §10 G6; below it the load calibration is wrong and is redone)", base, config.PinnedBaselineGoodputMin)
	return g
}
