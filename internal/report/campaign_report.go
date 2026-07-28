package report

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/validity"
)

// CampaignReport is the JSON artifact for an N-run campaign: the §5/§7
// aggregate plus the per-run §10 validity-gate evaluations.
type CampaignReport struct {
	SchemaVersion int               `json:"schema_version"`
	Caveat        string            `json:"caveat"`
	Campaign      *campaign.Report  `json:"campaign"`
	ValidityGates []validity.Report `json:"validity_gates"`
}

// GenerateCampaign renders the campaign JSON + human-readable pair.
func GenerateCampaign(rep *campaign.Report, gates []validity.Report) ([]byte, string, error) {
	cr := &CampaignReport{SchemaVersion: 1, Caveat: Caveat, Campaign: rep, ValidityGates: gates}
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

	w("Percentes campaign report — %s (variant %s)", rep.ConfigName, rep.Variant)
	w("")
	w("%s", Caveat)
	w("")
	w("repetitions: %d, valid runs: %d/%d", rep.Repetitions, rep.ValidRuns, rep.Repetitions)
	w("primary endpoint (§7): %s", campaign.PrimaryEndpoint)
	w("%s", rep.Caveat)
	w("")

	w("== Per-run scalars (all values verbatim, §5) ==")
	w("%-4s %-6s %-12s %-12s %-10s %-12s %-10s", "run", "valid", "ttr_equil", "ttr_prefault", "loss_frac", "survivor_p95", "deficit")
	for _, r := range rep.PerRun {
		lossCell := ptrS(r.InFlightLossFraction)
		if r.InFlightLossFraction == nil && r.InFlightLossAllReplicasUnscoped != nil {
			lossCell = fmt.Sprintf("[unscoped %.4f]", *r.InFlightLossAllReplicasUnscoped)
		}
		w("%-4d %-6v %-12s %-12s %-10s %-12.1f %-10.2f",
			r.Run, r.Valid, ptrS(r.TTREquilibriumS), ptrS(r.TTRPreFaultS), lossCell, r.SurvivorP95Ms, r.IntegratedDeficit)
	}
	w("")

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
				df += " — NOT the pre-registered df=4 (§7 assumes N=5 contributing runs; dropped runs reduced df)"
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
		w("== Run-validity gates (§10 G1-G6) ==")
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
	w("%s", Caveat)
	return b.String()
}

func noteSuffix(note string) string {
	if note == "" {
		return ""
	}
	return " — " + note
}

func ptrS(p *float64) string {
	if p == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.2f", *p)
}
