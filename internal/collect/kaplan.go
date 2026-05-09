package collect

import "sort"

// Kaplan-Meier product-limit estimator of the completion function over
// ALL scheduled requests in a window (SPEC.md §3): completions are events
// at their re-based latency; errored and censored requests are censored
// observations at their observed times. A quantile is reported only where
// the curve actually crosses it within the timeout horizon; otherwise
// "p_q > 30 s".

// KMObservation is one scheduled request's contribution.
type KMObservation struct {
	TimeUs    int64
	Completed bool // event; false = censored observation (errored or timed out)
}

// KMPoint is one step of the completion curve: P(completion <= t).
type KMPoint struct {
	TimeUs     int64   `json:"t_us"`
	Completion float64 `json:"completion"` // 1 - S(t)
	AtRisk     int     `json:"at_risk"`
	Events     int     `json:"events"`
}

// KMCurve is a window's completion curve (SPEC.md §3): the ordered event
// steps of the product-limit estimator plus the pinned horizon inside
// which quantiles may be claimed.
type KMCurve struct {
	N      int       `json:"n"`
	Points []KMPoint `json:"points"`
	// HorizonUs is the pinned client timeout; quantiles are only claimed
	// inside it.
	HorizonUs int64 `json:"horizon_us"`
}

// EstimateKM computes the completion curve. Ties between events and
// censorings at the same time follow the standard convention: events
// first (censored observations at t are still at risk for events at t).
func EstimateKM(obs []KMObservation, horizonUs int64) KMCurve {
	curve := KMCurve{N: len(obs), HorizonUs: horizonUs}
	if len(obs) == 0 {
		return curve
	}
	sorted := make([]KMObservation, len(obs))
	copy(sorted, obs)
	sort.Slice(sorted, func(a, b int) bool {
		if sorted[a].TimeUs != sorted[b].TimeUs {
			return sorted[a].TimeUs < sorted[b].TimeUs
		}
		return sorted[a].Completed && !sorted[b].Completed // events first
	})

	atRisk := len(sorted)
	s := 1.0
	i := 0
	for i < len(sorted) {
		t := sorted[i].TimeUs
		events, removed := 0, 0
		for i < len(sorted) && sorted[i].TimeUs == t {
			if sorted[i].Completed {
				events++
			}
			removed++
			i++
		}
		if events > 0 {
			s *= 1 - float64(events)/float64(atRisk)
			curve.Points = append(curve.Points, KMPoint{TimeUs: t, Completion: 1 - s, AtRisk: atRisk, Events: events})
		}
		atRisk -= removed
	}
	return curve
}

// Quantile returns the smallest t with completion probability >= q, and
// whether the curve crosses q within the horizon. When ok is false the
// report must state "p_q > horizon" (§3), never an extrapolated number.
func (c *KMCurve) Quantile(q float64) (tUs int64, ok bool) {
	const eps = 1e-9 // product-limit arithmetic sits exactly on quantile boundaries
	for _, p := range c.Points {
		if p.Completion >= q-eps && p.TimeUs <= c.HorizonUs {
			return p.TimeUs, true
		}
	}
	return 0, false
}

// CompletionAt returns P(completion <= tUs) — the headline uses
// "completion probability within 1 s".
func (c *KMCurve) CompletionAt(tUs int64) float64 {
	comp := 0.0
	for _, p := range c.Points {
		if p.TimeUs > tUs {
			break
		}
		comp = p.Completion
	}
	return comp
}
