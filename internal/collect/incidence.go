package collect

import (
	"fmt"
	"sort"
)

// Aalen-Johansen cumulative-incidence estimator of the completion function
// over ALL scheduled requests in a window (SPEC.md §3): completions are the
// event of interest at their re-based latency; errors are COMPETING terminal
// events, since an errored request can never complete and scoring it as
// censored would count it as still waiting; only requests with no terminal
// event by the pinned timeout (or run end) are censored observations. With
// no competing events the estimator reduces exactly to 1-KM. A quantile is
// reported only where the curve actually crosses it within the timeout
// horizon; otherwise "p_q > 30 s".

// ObsKind classifies one scheduled request's terminal state for the curve.
type ObsKind int

const (
	ObsCompletion ObsKind = iota // event of interest
	ObsError                     // competing terminal event
	ObsCensored                  // no terminal event by the horizon: lower bound
)

// Obs is one scheduled request's contribution.
type Obs struct {
	TimeUs int64
	Kind   ObsKind
}

// IncidencePoint is one completion step of the curve: CIF(t) = P(completed
// by t), estimated over all scheduled requests. AtRisk counts before any
// removal at this time; Errors counts competing events tied at this time.
type IncidencePoint struct {
	TimeUs    int64   `json:"t_us"`
	Incidence float64 `json:"incidence"`
	AtRisk    int     `json:"at_risk"`
	Events    int     `json:"events"`
	Errors    int     `json:"errors,omitempty"`
}

// IncidenceCurve is a window's completion-incidence curve (SPEC.md §3): the
// ordered completion steps of the Aalen-Johansen estimator plus the pinned
// horizon inside which quantiles may be claimed.
type IncidenceCurve struct {
	N      int              `json:"n"`
	Points []IncidencePoint `json:"points"`
	// Censored counts the observations still unresolved at their cutoff;
	// the report states this outstanding mass alongside the curve.
	Censored int `json:"censored"`
	// HorizonUs is the pinned client timeout; quantiles are only claimed
	// inside it. Re-based completion times may exceed it by up to the max
	// send-skew; such points stay on the curve and never satisfy a quantile.
	HorizonUs int64 `json:"horizon_us"`
}

// EstimateIncidence computes the completion-incidence curve:
//
//	CIF(t) = sum over event times t_i <= t of S(t_i-) * d_completion(t_i) / n(t_i)
//
// where S is overall event-free survival (completions AND errors both leave
// it) and n is the at-risk count. Ties at the same time follow the standard
// convention: terminal events first — censored observations at t are still
// at risk for events at t, and tied completions and errors share one n.
// An observation with an undefined Kind is a programming error, not a data
// condition: EstimateIncidence panics.
func EstimateIncidence(obs []Obs, horizonUs int64) IncidenceCurve {
	curve := IncidenceCurve{N: len(obs), HorizonUs: horizonUs}
	if len(obs) == 0 {
		return curve
	}
	sorted := make([]Obs, len(obs))
	copy(sorted, obs)
	sort.Slice(sorted, func(a, b int) bool {
		if sorted[a].TimeUs != sorted[b].TimeUs {
			return sorted[a].TimeUs < sorted[b].TimeUs
		}
		return sorted[a].Kind != ObsCensored && sorted[b].Kind == ObsCensored // events first
	})

	atRisk := len(sorted)
	s := 1.0   // overall event-free survival
	cif := 0.0 // cumulative incidence of completion
	i := 0
	for i < len(sorted) {
		t := sorted[i].TimeUs
		completions, errors, removed := 0, 0, 0
		for i < len(sorted) && sorted[i].TimeUs == t {
			switch sorted[i].Kind {
			case ObsCompletion:
				completions++
			case ObsError:
				errors++
			case ObsCensored:
				// no event; leaves the risk set below
				curve.Censored++
			default:
				panic(fmt.Sprintf("collect: undefined ObsKind %d", sorted[i].Kind))
			}
			removed++
			i++
		}
		if completions > 0 {
			cif += s * float64(completions) / float64(atRisk)
			curve.Points = append(curve.Points, IncidencePoint{
				TimeUs: t, Incidence: cif, AtRisk: atRisk,
				Events: completions, Errors: errors,
			})
		}
		if completions+errors > 0 {
			s *= 1 - float64(completions+errors)/float64(atRisk)
		}
		atRisk -= removed
	}
	return curve
}

// Quantile returns the smallest t with completion incidence >= q, and
// whether the curve crosses q within the horizon. When ok is false the
// report must state "p_q > horizon" (§3), never an extrapolated number.
// q outside (0, 1] is a programming error: Quantile panics. eps forgives
// floating-point undershoot at exact step boundaries; a q within eps above
// a step below 1 also passes.
func (c *IncidenceCurve) Quantile(q float64) (tUs int64, ok bool) {
	if q <= 0 || q > 1 {
		panic(fmt.Sprintf("collect: quantile %v outside (0, 1]", q))
	}
	const eps = 1e-9
	for _, p := range c.Points {
		if p.Incidence >= q-eps && p.TimeUs <= c.HorizonUs {
			return p.TimeUs, true
		}
	}
	return 0, false
}

// IncidenceAt returns CIF(tUs) = P(completed by tUs) — the headline uses
// "cumulative incidence of completion within 1 s".
func (c *IncidenceCurve) IncidenceAt(tUs int64) float64 {
	cif := 0.0
	for _, p := range c.Points {
		if p.TimeUs > tUs {
			break
		}
		cif = p.Incidence
	}
	return cif
}
