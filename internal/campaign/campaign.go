// Package campaign runs N repetitions of one (variant, config) and
// aggregates the per-run scalars under the SPEC.md §5/§7 statistics.
//
// Normative rules encoded here:
//   - N=5 runs per (variant, config); all five per-run values are
//     published verbatim (§5).
//   - The pre-registered PRIMARY endpoint is TTR to single-replica
//     equilibrium under the clean-delete variant (§7); everything else is
//     labeled secondary or exploratory. A run that could not estimate the
//     equilibrium contributes no equilibrium-TTR value, and that is
//     reported (not imputed).
//   - The TTR scalars are heavy-tailed: median + range lead, the
//     t-interval carries a normality caveat (§7).
//   - No bootstrap, no MDE/power claim (§7).
//   - The run-to-run coefficient of variation is surfaced as the measured
//     noise floor for v0.2.
package campaign

import (
	"context"
	"fmt"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/stats"
)

// PrimaryEndpoint is the §7 pre-registered primary endpoint label.
const PrimaryEndpoint = "ttr_single_replica_equilibrium_s (clean_delete)"

// Scalars is one run's published run-level scalars.
type Scalars struct {
	Run             int      `json:"run"`
	Valid           bool     `json:"valid"`
	InvalidReasons  []string `json:"invalid_reasons,omitempty"`
	TTREquilibriumS *float64 `json:"ttr_equilibrium_s,omitempty"`
	TTRPreFaultS    *float64 `json:"ttr_pre_fault_s,omitempty"`
	// InFlightLossFraction is the §10 pre-registered equivalence quantity
	// and is defined over the KILLED replica's in-flight requests (§3).
	// It is nil when no victim was attributed: an all-replica fraction is
	// a different (understated) quantity and must never publish under the
	// pre-registered name. The unattributed value, when computed, appears
	// separately and labeled.
	InFlightLossFraction            *float64 `json:"in_flight_loss_fraction,omitempty"`
	InFlightLossAllReplicasUnscoped *float64 `json:"in_flight_loss_all_replicas_unscoped,omitempty"`
	SurvivorP95Ms                   float64  `json:"survivor_p95_ms"`
	IntegratedDeficit               float64  `json:"integrated_goodput_deficit"`
}

// ScalarSummary is a §7 summary of one scalar across the runs that
// produced it, plus how many runs were dropped and why.
type ScalarSummary struct {
	Name           string        `json:"name"`
	Endpoint       string        `json:"endpoint"` // "primary" | "secondary" | "exploratory"
	Summary        stats.Summary `json:"summary"`
	ContributingN  int           `json:"contributing_n"`
	DroppedRuns    int           `json:"dropped_runs"`
	DroppedReason  string        `json:"dropped_reason,omitempty"`
	NoiseFloorNote string        `json:"noise_floor_note,omitempty"`
}

// Report is the campaign result.
type Report struct {
	Variant     string          `json:"variant"`
	ConfigName  string          `json:"config_name"`
	Repetitions int             `json:"repetitions"`
	PerRun      []Scalars       `json:"per_run"`
	Endpoints   []ScalarSummary `json:"endpoints"`
	ValidRuns   int             `json:"valid_runs"`
	// NoiseFloorCoV is the run-to-run CoV of the primary endpoint, the
	// measured noise floor for v0.2's MDE (§7).
	NoiseFloorCoV *float64 `json:"noise_floor_cov,omitempty"`
	Caveat        string   `json:"caveat"`
}

// Runner executes one run and returns its artifacts. run.Execute
// satisfies this; the seam exists so campaigns are unit-testable with a
// fake runner (no cluster).
type Runner func(ctx context.Context, cfg *config.Config, opts run.Options) (*run.Artifacts, error)

// Run executes cfg.Run.Repetitions runs (each with a per-run seed offset
// so repetitions are independent yet reproducible) and aggregates them.
// opts is applied to every run; variantLabel names the fault regime.
func Run(ctx context.Context, cfg *config.Config, opts run.Options, variantLabel string, runner Runner) (*Report, error) {
	n := cfg.Run.Repetitions
	if n < 1 {
		return nil, fmt.Errorf("campaign: repetitions must be >= 1, got %d", n)
	}
	rep := &Report{
		Variant:     variantLabel,
		ConfigName:  cfg.Run.Name,
		Repetitions: n,
		Caveat:      "Single-stack study: no MDE/power claim, no bootstrap (§7). Per-run values published verbatim; TTR scalars lead with median and range.",
	}

	for i := 0; i < n; i++ {
		runCfg := *cfg
		runCfg.Run.Seed = cfg.Run.Seed + int64(i)
		art, err := runner(ctx, &runCfg, opts)
		if err != nil {
			return nil, fmt.Errorf("campaign: run %d: %w", i+1, err)
		}
		rep.PerRun = append(rep.PerRun, extractScalars(i+1, art))
		if art.RunValid {
			rep.ValidRuns++
		}
	}

	rep.Endpoints = summarize(rep.PerRun, variantLabel)
	// The noise floor for v0.2's MDE is the run-to-run CoV of the
	// PRIMARY endpoint (§7) — equilibrium TTR under clean_delete only.
	// A black-hole campaign's equilibrium CoV is a secondary-endpoint
	// dispersion in a different fault regime and gets no such label.
	for i := range rep.Endpoints {
		if rep.Endpoints[i].Name == "ttr_equilibrium_s" && variantLabel == config.VariantCleanDelete &&
			rep.Endpoints[i].ContributingN >= 2 && rep.Endpoints[i].Summary.CoVDefined {
			cov := rep.Endpoints[i].Summary.CoV
			rep.NoiseFloorCoV = &cov
			rep.Endpoints[i].NoiseFloorNote = "run-to-run CoV of the primary endpoint: the measured noise floor for v0.2's pre-registered two-sample MDE (§7)"
		}
	}
	return rep, nil
}

func extractScalars(runIdx int, art *run.Artifacts) Scalars {
	s := Scalars{Run: runIdx, Valid: art.RunValid, InvalidReasons: art.InvalidReasons}
	if art.Detector != nil {
		if art.Detector.EquilibriumEstimable {
			s.TTREquilibriumS = art.Detector.ToEquilibrium.TTRSeconds
		}
		s.TTRPreFaultS = art.Detector.ToPreFault.TTRSeconds
		s.IntegratedDeficit = art.Detector.DeficitToPreFault
	}
	// In-flight loss fraction: §3 defines it over the killed replica's
	// in-flight requests, and §10 pre-registers it by name. Without a
	// victim attribution the quantity does not exist for this run — the
	// all-replica ratio is recorded under its own explicitly-unscoped
	// name and never merged into the pre-registered endpoint.
	inf := art.InFlight
	if art.VictimReplica != "" && inf.OnVictim > 0 {
		frac := float64(inf.OnVictimErrored+inf.OnVictimCensored) / float64(inf.OnVictim)
		s.InFlightLossFraction = &frac
	} else if inf.Total > 0 {
		frac := float64(inf.Errored+inf.Censored) / float64(inf.Total)
		s.InFlightLossAllReplicasUnscoped = &frac
	}
	if fault, ok := art.Windows["fault"]; ok {
		s.SurvivorP95Ms = float64(fault.E2EConditional.P95Us) / 1000
	}
	return s
}

// summarize builds the §7 scalar summaries. The equilibrium TTR is the
// primary endpoint only for the clean-delete variant; otherwise it is
// secondary. TTRs are heavy-tailed.
func summarize(runs []Scalars, variant string) []ScalarSummary {
	equilibriumEndpoint := "secondary"
	if variant == config.VariantCleanDelete {
		equilibriumEndpoint = "primary"
	}

	collectPtr := func(get func(Scalars) *float64) ([]float64, int) {
		var vals []float64
		dropped := 0
		for _, r := range runs {
			if v := get(r); v != nil {
				vals = append(vals, *v)
			} else {
				dropped++
			}
		}
		return vals, dropped
	}
	collectVal := func(get func(Scalars) float64) []float64 {
		vals := make([]float64, len(runs))
		for i, r := range runs {
			vals[i] = get(r)
		}
		return vals
	}

	var out []ScalarSummary

	if eq, dropped := collectPtr(func(r Scalars) *float64 { return r.TTREquilibriumS }); len(eq) > 0 {
		out = append(out, ScalarSummary{
			Name: "ttr_equilibrium_s", Endpoint: equilibriumEndpoint,
			Summary: stats.Summarize(eq, true), ContributingN: len(eq), DroppedRuns: dropped,
			DroppedReason: reasonIfDropped(dropped, "runs with no estimable single-replica equilibrium (total outage or instant recovery); not imputed"),
		})
	} else {
		out = append(out, ScalarSummary{
			Name: "ttr_equilibrium_s", Endpoint: equilibriumEndpoint,
			ContributingN: 0, DroppedRuns: dropped,
			DroppedReason: "no run produced an estimable single-replica equilibrium",
		})
	}

	if pf, dropped := collectPtr(func(r Scalars) *float64 { return r.TTRPreFaultS }); len(pf) > 0 {
		out = append(out, ScalarSummary{
			Name: "ttr_pre_fault_s", Endpoint: "secondary",
			Summary: stats.Summarize(pf, true), ContributingN: len(pf), DroppedRuns: dropped,
			DroppedReason: reasonIfDropped(dropped, "runs that never recovered to the pre-fault baseline"),
		})
	}

	if lf, dropped := collectPtr(func(r Scalars) *float64 { return r.InFlightLossFraction }); len(lf) > 0 {
		out = append(out, ScalarSummary{
			Name: "in_flight_loss_fraction", Endpoint: "secondary",
			Summary: stats.Summarize(lf, false), ContributingN: len(lf), DroppedRuns: dropped,
			DroppedReason: reasonIfDropped(dropped, "runs without victim attribution: §3 defines this quantity over the killed replica only; the unscoped all-replica ratio is recorded separately, never merged"),
		})
	} else {
		out = append(out, ScalarSummary{
			Name: "in_flight_loss_fraction", Endpoint: "secondary",
			ContributingN: 0, DroppedRuns: dropped,
			DroppedReason: "no run had victim attribution; the §10 pre-registered quantity is unavailable (see in_flight_loss_all_replicas_unscoped per run)",
		})
	}
	out = append(out, ScalarSummary{
		Name: "survivor_p95_ms", Endpoint: "secondary",
		Summary:       stats.Summarize(collectVal(func(r Scalars) float64 { return r.SurvivorP95Ms }), true),
		ContributingN: len(runs),
	})
	out = append(out, ScalarSummary{
		Name: "integrated_goodput_deficit", Endpoint: "exploratory",
		Summary:       stats.Summarize(collectVal(func(r Scalars) float64 { return r.IntegratedDeficit }), false),
		ContributingN: len(runs),
	})
	return out
}

// reasonIfDropped returns reason only when dropped > 0, so a summary with
// no dropped runs leaves DroppedReason empty (omitted from JSON).
func reasonIfDropped(dropped int, reason string) string {
	if dropped == 0 {
		return ""
	}
	return reason
}
