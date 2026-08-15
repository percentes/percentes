// Package report generates the full metric set as JSON plus a
// human-readable report from one config (SPEC.md §2): completion-incidence
// curves, failure rates, conditional-on-completion distributions, the recovery analysis
// with its sensitivity table, the decomposition, gates, and the
// conditional headline (appendix template). Distributional numbers come
// from the merged histograms queried once — never averaged percentiles.
package report

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/run"
)

// Caveat is printed in every report and in the AC output itself (§8).
const Caveat = "CAVEAT: passing AC1-AC7 certifies the instrument against the mock, not any claim about real GPU behavior. Small N, injected-fault-versus-reality gaps, and mock fidelity limits are handled by scoping claims, not by pretending."

// Report is the JSON artifact: the config embedded verbatim plus every
// run product.
type Report struct {
	SchemaVersion int    `json:"schema_version"`
	ConfigSHA256  string `json:"config_sha256"`
	Caveat        string `json:"caveat"`
	Headline      string `json:"conditional_headline"`
	*run.Artifacts
}

// Generate renders both artifacts from one run.
func Generate(art *run.Artifacts) ([]byte, string, error) {
	cfgRaw, err := json.Marshal(art.Config)
	if err != nil {
		return nil, "", fmt.Errorf("report: marshal config: %w", err)
	}
	rep := &Report{
		SchemaVersion: 2,
		ConfigSHA256:  fmt.Sprintf("%x", sha256.Sum256(cfgRaw)),
		Caveat:        Caveat,
		Headline:      headline(art),
		Artifacts:     art,
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, "", fmt.Errorf("report: marshal: %w", err)
	}
	return raw, human(rep), nil
}

// headline fills the appendix conditional-headline template with this
// run's measured values, honestly labeled for the mock variant.
func headline(art *run.Artifacts) string {
	fault, haveFault := art.Windows["fault"]
	base, haveBase := art.Windows["baseline"]
	if !haveFault || !haveBase {
		return "no fault/baseline windows collected; headline not applicable"
	}
	// §3's headline class is the KILLED replica's in-flight requests;
	// fall back to all-replica accounting (labeled) when no victim is
	// attributed.
	inFl := art.InFlight
	pop, errored, censored := inFl.Total, inFl.Errored, inFl.Censored
	popLabel := "in flight at fire, all replicas — no victim attributed"
	if art.VictimReplica != "" {
		pop, errored, censored = inFl.OnVictim, inFl.OnVictimErrored, inFl.OnVictimCensored
		popLabel = fmt.Sprintf("in flight on killed replica %s at fire", art.VictimReplica)
	}
	pctErr, pctCens := 0.0, 0.0
	if pop > 0 {
		pctErr = 100 * float64(errored) / float64(pop)
		pctCens = 100 * float64(censored) / float64(pop)
	}
	cif1s := fault.Incidence.IncidenceAt(1_000_000)
	ttr := "not recovered within the fault-window timeout"
	if art.Detector.EquilibriumEstimable {
		if t := art.Detector.ToEquilibrium.TTRSeconds; t != nil {
			ttr = fmt.Sprintf("%.1f s", *t)
		}
	} else {
		ttr = "n/a (" + art.Detector.EquilibriumNote + ")"
	}
	return fmt.Sprintf(
		"Under %s fault injection (mock variant, Phase 0 instrument certification): %.1f%% of in-flight requests failed and %.1f%% timed out at 30 s (%d %s); "+
			"survivor TTFT (conditional on completion) moved from p50 %.0f ms (baseline) to p50 %.0f ms (fault window); "+
			"cumulative incidence of completion within 1 s in the fault window was %.3f (Aalen-Johansen); "+
			"recovery to single-replica equilibrium: %s; goodput deficit %.1f goodput-seconds vs pre-fault; "+
			"decomposed segments: %s. Single run against the mock; no real-GPU claim.",
		art.Config.Fault.Variant, pctErr, pctCens, pop, popLabel,
		float64(base.TTFTConditional.P50Us)/1000, float64(fault.TTFTConditional.P50Us)/1000,
		cif1s, ttr, art.Detector.DeficitToPreFault, measuredSegments(art))
}

func measuredSegments(art *run.Artifacts) string {
	var parts []string
	for _, seg := range art.Decomposition.Segments {
		if seg.Measured {
			parts = append(parts, fmt.Sprintf("%s %.2fs", seg.Name, *seg.DurationS()))
		}
	}
	if len(parts) == 0 {
		return "none measured in this scenario"
	}
	return strings.Join(parts, ", ")
}

func human(r *Report) string {
	var b strings.Builder
	art := r.Artifacts
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	w("Percentes run report — %s", art.Config.Run.Name)
	w("config sha256: %s", r.ConfigSHA256)
	w("")
	w("%s", Caveat)
	w("")
	w("== Conditional headline (appendix template) ==")
	w("%s", r.Headline)
	w("")

	w("== Run validity ==")
	w("valid: %v", art.RunValid)
	for _, reason := range art.InvalidReasons {
		w("  - %s", reason)
	}
	g := art.Loadgen.Gates
	cpuCell := fmt.Sprintf("worst 5s window=%.1f%%", g.CPUWorstWindowPct)
	if !g.CPUMeasured {
		cpuCell = "UNMEASURED on this platform build (gate cannot pass uncertified)"
	}
	w("client-validity gate: pass=%v (skew p99=%dus max=%dus; undispatched=%d; cpu %s; gc pause p99=%.3fms)",
		g.Pass, g.SendSkewP99Us, g.SendSkewMaxUs, g.Undispatched, cpuCell, g.GCPauseP99Ms)
	w("share gate: applicable=%v pass=%v shares=%v", art.ShareGate.Applicable, art.ShareGate.Pass, art.ShareGate.Shares)
	if art.VictimReplica != "" {
		w("killed pod: %s (share at T_inject: %.3f) — §1", art.VictimReplica, art.ShareGate.VictimShareAtInject)
	}
	if art.Orchestration != nil {
		if errMs, err := art.Orchestration.FireErrorMs(); err == nil {
			w("injection timing: fire error %+.1fms (tolerance +-%dms); armed %s",
				errMs, art.Config.Fault.InjectionToleranceMs, art.Orchestration.ArmedAt.Format("15:04:05.000"))
		}
	}
	w("")

	w("== Environment pins (§6) ==")
	if pinsJSON, err := json.Marshal(art.Config.Pins); err != nil {
		w("pins: <unavailable: %v>", err)
	} else {
		w("%s", pinsJSON)
	}
	w("")

	names := make([]string, 0, len(art.Windows))
	for name := range art.Windows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		st := art.Windows[name]
		w("== Window %q [%.1fs, %.1fs) ==", name, float64(st.Window.StartNs)/1e9, float64(st.Window.EndNs)/1e9)
		w("scheduled=%d completed=%d errored=%d censored=%d", st.Scheduled, st.Completed, st.Errored, st.Censored)
		w("error rate=%.4f censored rate=%.4f (first-class)", st.ErrorRate, st.CensoredRate)
		if len(st.ErrClasses) > 0 {
			w("error classes: %v", st.ErrClasses)
		}
		w("TTFT conditional on completion: %s%s", summary(st.TTFTConditional), ciText(st.TTFTTailCI))
		w("e2e  conditional on completion: %s%s", summary(st.E2EConditional), ciText(st.E2ETailCI))
		w("ITL pooled (per-window): %s", summary(st.ITLPooled))
		w("throughput=%.2f rps goodput=%.2f rps goodput-frac=%.4f", st.ThroughputRPS, st.GoodputRPS, st.GoodputFrac)
		w("goodput-versus-threshold sweep (§4):")
		for _, sp := range st.GoodputSweep {
			w("  TTFT<=%4dms & e2e<=%5dms: goodput %.4f", sp.TTFTMs, sp.E2EMs, sp.GoodputFrac)
		}
		if st.ConditionalCaveat {
			w("CONDITIONAL CAVEAT: error+censored fraction exceeds 5%%; read the completed-only percentiles against the completion-incidence curve below (§3).")
		}
		w("%s", incidenceText(&st.Incidence))
		w("")
	}

	w("== In-flight loss accounting at fire ==")
	w("total=%d completed=%d errored=%d censored=%d by-replica=%v",
		art.InFlight.Total, art.InFlight.Completed, art.InFlight.Errored, art.InFlight.Censored, art.InFlight.ByReplica)
	if art.VictimReplica != "" {
		w("on killed replica %s: total=%d completed=%d errored=%d censored=%d (§3 headline class)",
			art.VictimReplica, art.InFlight.OnVictim, art.InFlight.OnVictimCompleted, art.InFlight.OnVictimErrored, art.InFlight.OnVictimCensored)
	}
	w("")

	ta := art.ThresholdAnalysis
	w("== Modal during-fault latency vs thresholds (§4) ==")
	if ta.Valid {
		w("baseline SD: TTFT %.2fms, e2e %.2fms; modal during-fault: TTFT %.1fms, e2e %.1fms", ta.BaselineTTFTSDMs, ta.BaselineE2ESDMs, ta.ModalFaultTTFTMs, ta.ModalFaultE2EMs)
		w("TTFT threshold distances (baseline SDs, signed modal-threshold): %v", ta.TTFTThresholdSDs)
		w("e2e  threshold distances (baseline SDs, signed modal-threshold): %v", ta.E2EThresholdSDs)
	} else {
		w("not computable: %s", ta.Note)
	}
	w("")

	d := art.Detector
	w("== Recovery (two baselines, hysteresis; §5) ==")
	w("pre-fault baseline goodput: %.4f", d.PreFaultBaseline)
	if d.EquilibriumEstimable {
		w("single-replica equilibrium baseline: %.4f (degraded plateau [%.0fs, %.0fs) of run time)", d.EquilibriumBaseline, d.EquilibriumWindowStartS, d.EquilibriumWindowEndS)
	} else {
		w("single-replica equilibrium baseline: NOT ESTIMABLE — %s", d.EquilibriumNote)
	}
	w("TTR to pre-fault baseline:    %s", ttrText(d.ToPreFault))
	w("TTR to equilibrium baseline:  %s", ttrText(d.ToEquilibrium))
	w("integrated goodput deficit: %.2f (vs pre-fault), %.2f (vs equilibrium) goodput-seconds", d.DeficitToPreFault, d.DeficitToEquilibrium)
	compNames := make([]string, 0, len(d.Components))
	for n := range d.Components {
		compNames = append(compNames, n)
	}
	sort.Strings(compNames)
	for _, n := range compNames {
		w("component %-11s %s", n+":", ttrText(d.Components[n]))
	}
	w("backlog drain: measured=%v — %s", d.BacklogDrainMeasured, d.BacklogDrainNote)
	w("")
	w("== Sensitivity table (X x R x H; §5) ==")
	w("%-6s %-4s %-4s | %-22s %-22s", "entry", "R", "H", "TTR->prefault", "TTR->equilibrium")
	for _, row := range d.Sensitivity {
		w("%-6d %-4d %-4d | %-22s %-22s", row.Params.EntryPct, row.Params.WindowS, row.Params.HoldS,
			ttrCell(row.TTRToPreFault, row.NotRecoveredPre), ttrCell(row.TTRToEquilibrium, row.NotRecoveredEq))
	}
	w("")

	w("== Recovery decomposition (§5: only measured boundaries are claimed) ==")
	for _, seg := range art.Decomposition.Segments {
		if seg.Measured {
			w("%-20s [%s] measured: %.2fs", seg.Name, seg.Source, *seg.DurationS())
		} else {
			w("%-20s [%s] N/A: %s", seg.Name, seg.Source, seg.Note)
		}
	}
	w("")
	w("%s", Caveat)
	return b.String()
}

func ciText(ci collect.TailCIs) string {
	one := func(name string, c *collect.OrderStatCI) string {
		if c == nil {
			return ""
		}
		if !c.Permitted {
			return fmt.Sprintf(" %s-CI omitted (sample budget insufficient, §7)", name)
		}
		return fmt.Sprintf(" %s-CI [%.1f, %.1f]ms", name, float64(c.LoUs)/1000, float64(c.HiUs)/1000)
	}
	return one("p95", ci.P95) + one("p99", ci.P99)
}

func summary(s histo.Summary) string {
	if s.Count == 0 {
		return "no completed samples"
	}
	return fmt.Sprintf("n=%d p50=%.1fms p95=%.1fms p99=%.1fms | p99.9=%.1fms max=%.1fms (descriptive-only at fault-window sample sizes, §7)",
		s.Count, float64(s.P50Us)/1000, float64(s.P95Us)/1000, float64(s.P99Us)/1000, float64(s.P999Us)/1000, float64(s.MaxUs)/1000)
}

func incidenceText(cif *collect.IncidenceCurve) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Completion incidence, Aalen-Johansen (n=%d over ALL scheduled; errors compete, timeouts censored; horizon %.0fs):\n", cif.N, float64(cif.HorizonUs)/1e6)
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		if t, ok := cif.Quantile(q); ok {
			fmt.Fprintf(&b, "  p%-4g completion at %.3fs\n", q*100, float64(t)/1e6)
		} else {
			fmt.Fprintf(&b, "  p%-4g > %.0fs (curve does not cross within the timeout horizon)\n", q*100, float64(cif.HorizonUs)/1e6)
		}
	}
	// A compact curve rendering: up to 12 evenly spaced points, plus the
	// last point always. The final incidence is where a window with errors
	// settles below 1.0, so sampling must never drop it.
	point := func(p collect.IncidencePoint) {
		fmt.Fprintf(&b, "  t=%8.3fs incidence=%.4f (at risk %d)\n", float64(p.TimeUs)/1e6, p.Incidence, p.AtRisk)
	}
	step := len(cif.Points)/12 + 1
	for i := 0; i < len(cif.Points); i += step {
		point(cif.Points[i])
	}
	if n := len(cif.Points); n > 0 && (n-1)%step != 0 {
		point(cif.Points[n-1])
	}
	return strings.TrimRight(b.String(), "\n")
}

func ttrText(d detect.Detection) string {
	if d.NotRecovered || d.TTRSeconds == nil {
		return fmt.Sprintf("NOT RECOVERED within the fault-window timeout (baseline %.4f)", d.Baseline)
	}
	return fmt.Sprintf("TTR %.1fs (baseline %.4f, canceled entries %d, re-degradations %d)", *d.TTRSeconds, d.Baseline, d.CanceledEntries, d.ReDegradations)
}

func ttrCell(t *float64, notRecovered bool) string {
	if notRecovered || t == nil {
		return "not recovered"
	}
	return fmt.Sprintf("%.1fs", *t)
}
