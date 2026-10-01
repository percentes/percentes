// Package campaign runs N repetitions of one (variant, config) and
// aggregates the per-run scalars under the SPEC.md §5/§7 statistics.
//
// Normative rules encoded here:
//   - The repetition count N is 5 runs per (variant, config); all five
//     per-run values are published verbatim (§5).
//   - The pre-registered PRIMARY endpoint is time to recovery (TTR) to
//     single-replica equilibrium under the clean-delete variant, and the
//     outage (fire to replica-ready) under the process-kill variant (§7);
//     everything else is labeled secondary, exploratory or not
//     applicable. A run that
//     could not estimate the equilibrium contributes no equilibrium-TTR
//     value, and that is reported (not imputed).
//   - The TTR scalars are heavy-tailed: median + range lead, the
//     t-interval carries a normality caveat (§7).
//   - No bootstrap, no minimum detectable effect (MDE) or power
//     claim (§7).
//   - The run-to-run coefficient of variation (CoV) is surfaced as the
//     measured noise floor for the deferred cross-stack comparison.
package campaign

import (
	"context"
	"fmt"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/serverstats"
	"github.com/percentes/percentes/internal/stats"
)

// PrimaryEndpoint is the §7 pre-registered primary endpoint label.
const PrimaryEndpoint = "ttr_single_replica_equilibrium_s (clean_delete)"

// PrimaryEndpointProcessKill is the §7 primary endpoint label of the
// process-kill variant.
const PrimaryEndpointProcessKill = "outage_s: fire to replica_ready (process_kill)"

// PrimaryEndpointFor returns the primary endpoint label for the variant.
func PrimaryEndpointFor(variant string) string {
	if variant == config.VariantProcessKill {
		return PrimaryEndpointProcessKill
	}
	return PrimaryEndpoint
}

// Policy is how the campaign treats an invalid run.
type Policy struct{ HaltAfterInvalidRun bool }

// Scalars is one run's published run-level scalars.
type Scalars struct {
	Run             int      `json:"run"`
	Valid           bool     `json:"valid"`
	InvalidReasons  []string `json:"invalid_reasons,omitempty"`
	TTREquilibriumS *float64 `json:"ttr_equilibrium_s,omitempty"`
	TTRPreFaultS    *float64 `json:"ttr_pre_fault_s,omitempty"`
	// The unobserved flags mark a nil TTR whose hold the series ended
	// before, apart from a run that never recovered (§5).
	TTREquilibriumUnobserved bool `json:"ttr_equilibrium_unobserved,omitempty"`
	TTRPreFaultUnobserved    bool `json:"ttr_pre_fault_unobserved,omitempty"`
	// InFlightLossFraction is the §10 pre-registered equivalence quantity
	// and is defined over the KILLED replica's in-flight requests (§3).
	// It is nil when no victim was attributed: an all-replica fraction is
	// a different (understated) quantity and must never publish under the
	// pre-registered name. The unattributed value, when computed, appears
	// separately and labeled. Under process kill with one replica every
	// in-flight request is on the killed replica, and the fraction is
	// taken over all of them.
	InFlightLossFraction            *float64 `json:"in_flight_loss_fraction,omitempty"`
	InFlightLossAllReplicasUnscoped *float64 `json:"in_flight_loss_all_replicas_unscoped,omitempty"`
	// SurvivorP95Ms is the e2e p95 over the survivor cohort (§3: the
	// fault-window completions served by the replica that is not the
	// victim); FaultWindowE2EP95Ms is the same percentile pooled over every
	// replica.
	SurvivorP95Ms       *float64 `json:"survivor_p95_ms,omitempty"`
	FaultWindowE2EP95Ms *float64 `json:"fault_window_e2e_p95_ms,omitempty"`
	// SurvivorCohortAbsent marks a run whose attribution named no survivor
	// cohort (§3), apart from a cohort with no completed samples.
	SurvivorCohortAbsent bool    `json:"survivor_cohort_absent,omitempty"`
	IntegratedDeficit    float64 `json:"integrated_goodput_deficit"`
	// ReceivePath and ServerSide are the run's §2 per-window receive-path
	// reports and kept-family reductions, carried verbatim; FamilyErrors
	// counts the run's kept-family reads that failed.
	ReceivePath  map[string]*collect.ReceivePath                        `json:"receive_path,omitempty"`
	ServerSide   map[string]map[string]map[string]serverstats.Reduction `json:"server_side,omitempty"`
	FamilyErrors int                                                    `json:"family_errors,omitempty"`
	// OutageS and ContainerStartS are the replica_ready and container_start
	// decomposition rows (process kill, §5); FireUncertaintyS is the run's
	// fire uncertainty and EquilibriumNote the detector's not-estimable
	// reason, verbatim.
	OutageS          *float64 `json:"outage_s,omitempty"`
	ContainerStartS  *float64 `json:"container_start_s,omitempty"`
	FireUncertaintyS *float64 `json:"fire_uncertainty_s,omitempty"`
	EquilibriumNote  string   `json:"equilibrium_note,omitempty"`
	// InFlightErroredByClass and InFlightIndeterminate carry the run's
	// in-flight class split and the in-flight requests ending inside the
	// indeterminate zone after the fire; InFlightDeterminate classifies
	// the other in-flight requests (§1, §3). Outage classifies the requests
	// scheduled in [fire, replica_ready).
	InFlightErroredByClass map[string]int        `json:"in_flight_errored_by_class,omitempty"`
	InFlightIndeterminate  int                   `json:"in_flight_indeterminate,omitempty"`
	InFlightDeterminate    *collect.Outcomes     `json:"in_flight_determinate,omitempty"`
	Outage                 *collect.Outcomes     `json:"outage_outcomes,omitempty"`
	Decomposition          *detect.Decomposition `json:"decomposition,omitempty"`
	Container              *run.ContainerRestart `json:"container,omitempty"`
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
	// InvalidRuns counts runs excluded from the §7 endpoint summaries as
	// invalid for any recorded reason; their rows stay in PerRun (§5).
	InvalidRuns int `json:"invalid_runs"`
	// NoiseFloorCoV is the run-to-run CoV of the primary endpoint, the
	// measured noise floor for the cross-stack comparison's MDE (§7).
	NoiseFloorCoV *float64 `json:"noise_floor_cov,omitempty"`
	Caveat        string   `json:"caveat"`
	// Halted marks a campaign ended by Policy.HaltAfterInvalidRun after
	// run HaltedAfterRun; Failed marks one ended by the runner's error.
	Halted         bool   `json:"halted,omitempty"`
	HaltedAfterRun int    `json:"halted_after_run,omitempty"`
	Failed         bool   `json:"failed,omitempty"`
	FailedRun      int    `json:"failed_run,omitempty"`
	FailedReason   string `json:"failed_reason,omitempty"`
}

// Runner executes one run and returns its artifacts. run.Execute
// satisfies this; the seam exists so campaigns are unit-testable with a
// fake runner (no cluster).
type Runner func(ctx context.Context, cfg *config.Config, opts run.Options) (*run.Artifacts, error)

// Run executes cfg.Run.Repetitions runs (each with a per-run seed offset
// so repetitions are independent yet reproducible) and aggregates them.
// opts is applied to every run; variantLabel names the fault regime.
func Run(ctx context.Context, cfg *config.Config, opts run.Options, variantLabel string, runner Runner) (*Report, error) {
	return RunWith(ctx, cfg, opts, variantLabel, runner, Policy{})
}

// RunWith is Run with a policy. On a runner error it returns the report
// of the runs so far, marked Failed, with the error; an invalid run under
// HaltAfterInvalidRun ends the campaign, marked Halted.
func RunWith(ctx context.Context, cfg *config.Config, opts run.Options, variantLabel string, runner Runner, pol Policy) (*Report, error) {
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

	var runErr error
	for i := 0; i < n; i++ {
		runCfg := *cfg
		runCfg.Run.Seed = cfg.Run.Seed + int64(i)
		art, err := runner(ctx, &runCfg, opts)
		if err != nil {
			runErr = fmt.Errorf("campaign: run %d: %w", i+1, err)
			rep.Failed, rep.FailedRun, rep.FailedReason = true, i+1, err.Error()
			break
		}
		rep.PerRun = append(rep.PerRun, extractScalars(i+1, art))
		if art.RunValid {
			rep.ValidRuns++
		} else if pol.HaltAfterInvalidRun {
			rep.Halted, rep.HaltedAfterRun = true, i+1
			break
		}
	}

	rep.Endpoints, rep.InvalidRuns = summarize(rep.PerRun, variantLabel)
	// The noise floor for the cross-stack comparison's MDE is the run-to-run
	// CoV of the PRIMARY endpoint (§7): equilibrium TTR under clean_delete
	// only.
	// A black-hole campaign's equilibrium CoV is a secondary-endpoint
	// dispersion in a different fault regime and gets no such label.
	for i := range rep.Endpoints {
		if rep.Endpoints[i].Name == "ttr_equilibrium_s" && variantLabel == config.VariantCleanDelete &&
			rep.Endpoints[i].ContributingN >= 2 && rep.Endpoints[i].Summary.CoVDefined {
			cov := rep.Endpoints[i].Summary.CoV
			rep.NoiseFloorCoV = &cov
			rep.Endpoints[i].NoiseFloorNote = "run-to-run CoV of the primary endpoint: the measured noise floor for the deferred cross-stack comparison's pre-registered two-sample MDE (§7)"
		}
	}
	return rep, runErr
}

func extractScalars(runIdx int, art *run.Artifacts) Scalars {
	s := Scalars{Run: runIdx, Valid: art.RunValid, InvalidReasons: art.InvalidReasons, ReceivePath: art.ReceivePath, ServerSide: art.ServerSide, FamilyErrors: art.FamilyErrors,
		InFlightErroredByClass: art.InFlight.ErroredByClass, InFlightIndeterminate: art.InFlight.IndeterminateAtFire, InFlightDeterminate: art.InFlight.Determinate,
		Decomposition: art.Decomposition, Container: art.Container}
	processKill := art.Config != nil && art.Config.Fault.Variant == config.VariantProcessKill
	if art.Detector != nil {
		if art.Detector.EquilibriumEstimable {
			s.TTREquilibriumS = art.Detector.ToEquilibrium.TTRSeconds
			s.TTREquilibriumUnobserved = art.Detector.ToEquilibrium.HoldUnobserved
		}
		labelled := art.Detector.ToPreFault
		s.IntegratedDeficit = art.Detector.DeficitToPreFault
		if art.Detector.PartitionHealRecovery != nil {
			labelled = *art.Detector.PartitionHealRecovery
			if art.Detector.DeficitToPartitionHeal != nil {
				s.IntegratedDeficit = *art.Detector.DeficitToPartitionHeal
			}
		}
		s.TTRPreFaultS, s.TTRPreFaultUnobserved = labelled.TTRSeconds, labelled.HoldUnobserved
		if processKill && !art.Detector.EquilibriumEstimable {
			s.EquilibriumNote = art.Detector.EquilibriumNote
		}
	}
	if processKill {
		s.OutageS = segmentS(art.Decomposition, "replica_ready")
		s.ContainerStartS = segmentS(art.Decomposition, "container_start")
		if art.Container != nil && art.Container.Kill != nil {
			u := float64(art.Container.FireUncertaintyNs) / 1e9
			s.FireUncertaintyS = &u
		}
		if o := art.Windows["outage"]; o != nil {
			s.Outage = &collect.Outcomes{Total: o.Scheduled, Completed: o.Completed, Errored: o.Errored, Censored: o.Censored, ErroredByClass: o.ErrClasses}
		}
	}
	// In-flight loss fraction: §3 defines it over the killed replica's
	// in-flight requests, and §10 pre-registers it by name. Without a
	// victim attribution the quantity does not exist for this run, unless
	// one replica was killed in place (every in-flight request was on it);
	// otherwise the all-replica ratio is recorded under its own
	// explicitly-unscoped name and never merged into the pre-registered
	// endpoint.
	inf := art.InFlight
	if art.VictimReplica != "" && inf.OnVictim > 0 {
		frac := float64(inf.OnVictimErrored+inf.OnVictimCensored) / float64(inf.OnVictim)
		s.InFlightLossFraction = &frac
	} else if processKill && art.Config.Target.Replicas == 1 && inf.Total > 0 {
		frac := float64(inf.Errored+inf.Censored) / float64(inf.Total)
		s.InFlightLossFraction = &frac
	} else if inf.Total > 0 {
		frac := float64(inf.Errored+inf.Censored) / float64(inf.Total)
		s.InFlightLossAllReplicasUnscoped = &frac
	}
	// A missing cohort is recorded; a window with no completed samples has
	// no percentile and the scalar stays nil.
	s.SurvivorCohortAbsent = art.Windows["fault_survivor"] == nil
	if sv, ok := art.Windows["fault_survivor"]; ok && sv.E2EConditional.Count > 0 {
		v := float64(sv.E2EConditional.P95Us) / 1000
		s.SurvivorP95Ms = &v
	}
	if fault, ok := art.Windows["fault"]; ok && fault.E2EConditional.Count > 0 {
		v := float64(fault.E2EConditional.P95Us) / 1000
		s.FaultWindowE2EP95Ms = &v
	}
	return s
}

// segmentS is the named decomposition row's duration, nil when unmeasured.
func segmentS(d *detect.Decomposition, name string) *float64 {
	if d == nil {
		return nil
	}
	for _, seg := range d.Segments {
		if seg.Name == name {
			return seg.DurationS()
		}
	}
	return nil
}

// summarize builds the §7 scalar summaries. The equilibrium TTR is the
// primary endpoint only for the clean-delete variant and secondary
// otherwise; under process kill the outage is primary and the
// equilibrium TTR and survivor percentile are not applicable. TTRs are
// heavy-tailed.
func summarize(runs []Scalars, variant string) ([]ScalarSummary, int) {
	// §5 publishes every per-run value verbatim in the table; the §7
	// endpoint summaries hold valid runs only.
	var valid []Scalars
	for _, r := range runs {
		if r.Valid {
			valid = append(valid, r)
		}
	}
	excluded := len(runs) - len(valid)
	runs = valid

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
	processKill := variant == config.VariantProcessKill
	if processKill {
		outage, outageDropped := collectPtr(func(r Scalars) *float64 { return r.OutageS })
		start, startDropped := collectPtr(func(r Scalars) *float64 { return r.ContainerStartS })
		out = append(out,
			durationSummary("outage_s", "primary", "runs with no replica_ready measurement", outage, outageDropped),
			durationSummary("container_start_s", "secondary", "runs with no container_start measurement", start, startDropped))
	}

	unobservedEq, unobservedPre, absentCohort := 0, 0, 0
	for _, r := range valid {
		if r.TTREquilibriumS == nil && r.TTREquilibriumUnobserved {
			unobservedEq++
		}
		if r.TTRPreFaultS == nil && r.TTRPreFaultUnobserved {
			unobservedPre++
		}
		if r.SurvivorP95Ms == nil && r.SurvivorCohortAbsent {
			absentCohort++
		}
	}
	withUnobserved := func(reason string, unobserved int) string {
		if unobserved == 0 {
			return reason
		}
		return fmt.Sprintf("%s; %d ended before the hold could be observed (unobserved)", reason, unobserved)
	}
	preFaultName := "ttr_pre_fault_s"
	if variant == config.VariantBlackHole {
		preFaultName = "partition_heal_recovery_s"
	}

	if processKill {
		out = append(out, ScalarSummary{Name: "ttr_equilibrium_s", Endpoint: "not_applicable",
			DroppedReason: "one replica: the single-replica equilibrium is a survivor quantity (§5)"})
	} else if eq, dropped := collectPtr(func(r Scalars) *float64 { return r.TTREquilibriumS }); len(eq) > 0 {
		out = append(out, ScalarSummary{
			Name: "ttr_equilibrium_s", Endpoint: equilibriumEndpoint,
			Summary: stats.Summarize(eq, true), ContributingN: len(eq), DroppedRuns: dropped,
			DroppedReason: reasonIfDropped(dropped, withUnobserved("runs with no estimable single-replica equilibrium (total outage or instant recovery); not imputed", unobservedEq)),
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
			Name: preFaultName, Endpoint: "secondary",
			Summary: stats.Summarize(pf, true), ContributingN: len(pf), DroppedRuns: dropped,
			DroppedReason: reasonIfDropped(dropped, withUnobserved(fmt.Sprintf("%d runs never recovered to the pre-fault baseline", dropped-unobservedPre), unobservedPre)),
		})
	}

	lossDropReason := "runs without victim attribution: §3 defines this quantity over the killed replica only; the unscoped all-replica ratio is recorded separately, never merged"
	if processKill {
		lossDropReason = "runs with no request in flight at fire, or more than one replica"
	}
	if lf, dropped := collectPtr(func(r Scalars) *float64 { return r.InFlightLossFraction }); len(lf) > 0 {
		out = append(out, ScalarSummary{
			Name: "in_flight_loss_fraction", Endpoint: "secondary",
			Summary: stats.Summarize(lf, false), ContributingN: len(lf), DroppedRuns: dropped,
			DroppedReason: reasonIfDropped(dropped, lossDropReason),
		})
	} else {
		none := "no run had victim attribution; the §10 pre-registered quantity is unavailable (see in_flight_loss_all_replicas_unscoped per run)"
		if processKill {
			none = "no valid run had a request in flight at fire on one replica"
		}
		out = append(out, ScalarSummary{
			Name: "in_flight_loss_fraction", Endpoint: "secondary",
			ContributingN: 0, DroppedRuns: dropped,
			DroppedReason: none,
		})
	}
	sp, spDropped := collectPtr(func(r Scalars) *float64 { return r.SurvivorP95Ms })
	survivor := ScalarSummary{
		Name: "survivor_p95_ms", Endpoint: "secondary",
		ContributingN: len(sp),
		DroppedRuns:   spDropped,
		DroppedReason: droppedReason(spDropped, fmt.Sprintf("%d runs had no survivor cohort (§3: no victim attribution naming exactly one other baseline replica); %d had no completed cohort samples in the fault window; the pooled figure is fault_window_e2e_p95_ms per run", absentCohort, spDropped-absentCohort)),
	}
	if len(sp) > 0 {
		survivor.Summary = stats.Summarize(sp, true)
	}
	if processKill {
		survivor = ScalarSummary{Name: "survivor_p95_ms", Endpoint: "not_applicable", DroppedReason: "one replica: no survivor cohort (§3)"}
	}
	out = append(out, survivor)
	deficit := ScalarSummary{Name: "integrated_goodput_deficit", Endpoint: "exploratory", ContributingN: len(runs)}
	if len(runs) > 0 {
		deficit.Summary = stats.Summarize(collectVal(func(r Scalars) float64 { return r.IntegratedDeficit }), false)
	} else {
		deficit.DroppedReason = "no valid run"
	}
	out = append(out, deficit)
	return out, excluded
}

// durationSummary is a heavy-tailed summary of one per-run duration.
func durationSummary(name, endpoint, why string, vals []float64, dropped int) ScalarSummary {
	s := ScalarSummary{Name: name, Endpoint: endpoint, ContributingN: len(vals), DroppedRuns: dropped, DroppedReason: reasonIfDropped(dropped, why)}
	if len(vals) > 0 {
		s.Summary = stats.Summarize(vals, true)
	} else {
		s.DroppedReason = "no valid run produced a measurement: " + why
	}
	return s
}

// droppedReason labels a nonzero drop count.
func droppedReason(n int, why string) string {
	if n == 0 {
		return ""
	}
	return why
}

// reasonIfDropped returns reason only when dropped > 0, so a summary with
// no dropped runs leaves DroppedReason empty (omitted from JSON).
func reasonIfDropped(dropped int, reason string) string {
	if dropped == 0 {
		return ""
	}
	return reason
}
