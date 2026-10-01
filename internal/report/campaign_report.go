package report

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/validity"
)

// CampaignReport is the JSON artifact for an N-run campaign: the §5/§7
// aggregate plus the per-run §10 validity-gate evaluations.
type CampaignReport struct {
	SchemaVersion    int               `json:"schema_version"`
	InstrumentCommit string            `json:"instrument_commit"`
	ConfigSHA256     string            `json:"config_sha256"`
	Overrides        []string          `json:"overrides,omitempty"`
	Caveat           string            `json:"caveat"`
	Campaign         *campaign.Report  `json:"campaign"`
	ValidityGates    []validity.Report `json:"validity_gates"`
}

// CampaignMeta is what the campaign report records beside the aggregate:
// the build, the config file's hash, the profile that selects the caveat,
// and the config values the command line replaced.
type CampaignMeta struct {
	InstrumentCommit string
	ConfigSHA256     string
	Profile          config.Profile
	Overrides        []string // "target.base_url=..." and "target.metrics_urls=..." when flags replaced the file's values
}

// GenerateCampaign renders the campaign JSON + human-readable pair.
func GenerateCampaign(rep *campaign.Report, gates []validity.Report) ([]byte, string, error) {
	return GenerateCampaignWith(rep, gates, CampaignMeta{})
}

// GenerateCampaignWith renders the campaign pair with its meta; an empty
// InstrumentCommit is read from the build.
func GenerateCampaignWith(rep *campaign.Report, gates []validity.Report, meta CampaignMeta) ([]byte, string, error) {
	commit := meta.InstrumentCommit
	if commit == "" {
		commit = instrumentCommit()
	}
	cr := &CampaignReport{SchemaVersion: 1, InstrumentCommit: commit, ConfigSHA256: meta.ConfigSHA256, Overrides: meta.Overrides,
		Caveat: CaveatFor(rep.Variant, meta.Profile), Campaign: rep, ValidityGates: gates}
	raw, err := json.MarshalIndent(cr, "", "  ")
	if err != nil {
		return nil, "", fmt.Errorf("report: marshal campaign: %w", err)
	}
	return raw, humanCampaign(cr), nil
}

func humanCampaign(cr *CampaignReport) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	rep := cr.Campaign

	w("Percentes campaign report: %s (variant %s)", rep.ConfigName, rep.Variant)
	w("instrument commit: %s", cr.InstrumentCommit)
	if cr.ConfigSHA256 != "" {
		w("config sha256: %s", cr.ConfigSHA256)
	}
	for _, o := range cr.Overrides {
		w("override: %s", o)
	}
	w("")
	w("%s", cr.Caveat)
	w("")
	w("repetitions: %d, valid runs: %d/%d", rep.Repetitions, rep.ValidRuns, rep.Repetitions)
	if rep.Halted {
		w("HALTED after run %d: the run was invalid and the halt-after-invalid-run policy ended the campaign; %d of %d runs executed", rep.HaltedAfterRun, len(rep.PerRun), rep.Repetitions)
	}
	if rep.Failed {
		w("FAILED at run %d: %s; %d of %d runs completed", rep.FailedRun, rep.FailedReason, len(rep.PerRun), rep.Repetitions)
	}
	if cr.Campaign.InvalidRuns > 0 {
		w("invalid runs excluded from endpoint summaries: %d (rows kept in the per-run table, §5)", cr.Campaign.InvalidRuns)
	}
	w("primary endpoint (§7): %s", campaign.PrimaryEndpointFor(rep.Variant))
	w("%s", rep.Caveat)
	w("")

	w("== Per-run scalars (all values verbatim, §5) ==")
	ttrCol := "ttr_prefault"
	if rep.Variant == config.VariantBlackHole {
		ttrCol = "heal_recov"
	}
	w("%-4s %-6s %-10s %-12s %-12s %-10s %-12s %-10s", "run", "valid", "outage", "ttr_equil", ttrCol, "loss_frac", "survivor_p95", "deficit")
	for _, r := range rep.PerRun {
		lossCell := ptrS(r.InFlightLossFraction)
		if r.InFlightLossFraction == nil && r.InFlightLossAllReplicasUnscoped != nil {
			lossCell = fmt.Sprintf("[unscoped %.4f]", *r.InFlightLossAllReplicasUnscoped)
		}
		w("%-4d %-6v %-10s %-12s %-12s %-10s %-12s %-10.2f",
			r.Run, r.Valid, ptrS(r.OutageS), ttrOrUnobserved(r.TTREquilibriumS, r.TTREquilibriumUnobserved), ttrOrUnobserved(r.TTRPreFaultS, r.TTRPreFaultUnobserved), lossCell, ptrS(r.SurvivorP95Ms), r.IntegratedDeficit)
	}
	w("")

	for _, r := range rep.PerRun {
		if r.Decomposition == nil {
			continue
		}
		w("== Run %d: recovery decomposition (§5) ==", r.Run)
		for _, seg := range r.Decomposition.Segments {
			if d := seg.DurationS(); d != nil {
				w("%-20s [%s] measured: %.2fs", seg.Name, seg.Source, *d)
			} else {
				w("%-20s [%s] N/A: %s", seg.Name, seg.Source, seg.Note)
			}
		}
		if len(r.Decomposition.LogFigures) > 0 {
			w("figures printed in the server log: %s", figuresText(r.Decomposition.LogFigures))
		}
		if len(r.InFlightErroredByClass) > 0 {
			w("in-flight errored by class: %s", classText(r.InFlightErroredByClass))
		}
		if r.Container != nil {
			w("in flight at fire, indeterminate (terminal time within the fire uncertainty plus the delivery allowance after the fire): %d", r.InFlightIndeterminate)
		}
		if d := r.InFlightDeterminate; d != nil {
			w("in flight at fire, determinate: total=%d completed=%d errored=%d censored=%d", d.Total, d.Completed, d.Errored, d.Censored)
		}
		if o := r.Outage; o != nil {
			w("scheduled in the outage: total=%d completed=%d errored=%d censored=%d", o.Total, o.Completed, o.Errored, o.Censored)
			if len(o.ErroredByClass) > 0 {
				w("outage errored by class: %s", classText(o.ErroredByClass))
			}
		}
		if r.FireUncertaintyS != nil {
			w("fire uncertainty: %.1fms", *r.FireUncertaintyS*1000)
		}
		if r.EquilibriumNote != "" {
			w("single-replica equilibrium: %s", r.EquilibriumNote)
		}
		w("")
	}

	for _, r := range rep.PerRun {
		if len(r.ReceivePath) == 0 && len(r.ServerSide) == 0 && r.FamilyErrors == 0 {
			continue
		}
		w("== Run %d: receive path and server side per window (§2) ==", r.Run)
		if r.FamilyErrors > 0 {
			w("kept-family reads that failed: %d", r.FamilyErrors)
		}
		for _, name := range sortedKeys(r.ReceivePath) {
			w("%s: %s", name, receivePathText(r.ReceivePath[name]))
		}
		for _, name := range sortedKeys(r.ServerSide) {
			for _, replica := range sortedKeys(r.ServerSide[name]) {
				w("%s server-side %s: %s", name, replica, reductionText(r.ServerSide[name][replica]))
			}
		}
		w("")
	}

	w("== Endpoint summaries (§7) ==")
	for _, e := range rep.Endpoints {
		w("%s [%s]", e.Name, e.Endpoint)
		if e.ContributingN == 0 {
			w("  no contributing runs: %s", e.DroppedReason)
			continue
		}
		w("  %s", e.Summary.Headline)
		w("  values: %v", e.Summary.Values)
		if e.Summary.N >= 2 {
			df := fmt.Sprintf("t-interval [%.2f, %.2f] at t=%.3f df=%d", e.Summary.TIntervalLo, e.Summary.TIntervalHi, e.Summary.TMultiplier, e.Summary.DF)
			if !e.Summary.AtPinnedDF {
				df += ", below the pre-registered df=4 (§7 assumes N=5 contributing runs; dropped runs reduced df)"
			}
			w("  %s", df)
		}
		if e.DroppedRuns > 0 {
			w("  dropped %d run(s): %s", e.DroppedRuns, e.DroppedReason)
		}
		if e.Summary.CoVDefined {
			w("  CoV=%.4f%s", e.Summary.CoV, noteSuffix(e.NoiseFloorNote))
		} else {
			w("  CoV undefined (single contributing run or zero mean; not a real 0)")
		}
	}
	w("")

	if len(cr.ValidityGates) > 0 {
		w("== Run-validity gates (§10 G1-G7) ==")
		for i, gr := range cr.ValidityGates {
			w("run %d (variant %s): all_pass=%v", i+1, gr.Variant, gr.AllPass)
			for _, g := range gr.Gates {
				status := "n/a"
				if g.Applicable {
					if !g.Observed {
						status = "UNOBSERVED->FAIL"
					} else if g.Pass {
						status = "pass"
					} else {
						status = "FAIL"
					}
				}
				w("  %s %-45s %-16s %s", g.ID, g.Name, status, g.Detail)
			}
		}
		w("")
	}
	w("%s", cr.Caveat)
	return b.String()
}

func noteSuffix(note string) string {
	if note == "" {
		return ""
	}
	return " (" + note + ")"
}

func ptrS(p *float64) string {
	if p == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.2f", *p)
}

// ttrOrUnobserved prints a TTR cell, naming an unobserved hold.
func ttrOrUnobserved(t *float64, unobserved bool) string {
	if t == nil && unobserved {
		return "unobserved"
	}
	return ptrS(t)
}
