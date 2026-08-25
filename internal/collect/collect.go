// Package collect is the metrics collector (SPEC.md §2, §3): it turns the
// client-side request records (authoritative for latency, errors, and
// censoring) into per-window statistics under the normative three-state
// outcome model.
//
// Normative rules implemented here:
//   - Every scheduled request is a sample ending in exactly one of
//     completed / errored / censored.
//   - Completed-only latency distributions, always labeled conditional on
//     completion. Errored and censored requests NEVER enter latency
//     histograms.
//   - Failure rates (error rate, censored rate) are first-class.
//   - Aalen-Johansen completion-incidence curves over ALL scheduled
//     requests: errors are competing terminal events, only timeouts (and
//     run-end) are censored (§3).
//   - Windows are aligned to phases and never straddle T_inject; requests
//     belong to the window of their INTENDED dispatch time (the schedule
//     is the pre-registered object). The baseline window ends one pinned
//     client timeout before the fire anchor, and the guard window runs
//     from there to T_inject: a request intended inside the guard can
//     still be unresolved when the fault fires, so its outcome would be
//     fault-caused but baseline-attributed (§3). The guard
//     carries the full metric set and feeds no baseline-derived quantity.
//   - Any window with error+censored fraction above 5% must present the
//     incidence curve alongside completed-only percentiles, caveat explicit.
package collect

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/loadgen"
)

// Window is a run-relative interval [StartNs, EndNs).
type Window struct {
	Name    string `json:"name"`
	StartNs int64  `json:"start_ns"`
	EndNs   int64  `json:"end_ns"`
}

// FireAnchorNs is the §3 fire anchor: the earlier of the planned T_inject
// and the recorded actual fire time (§3; the injection-timing
// gate permits firing within 500 ms of T_inject).
func FireAnchorNs(tInjectNs, actualFireNs int64) int64 {
	// Zero means no recorded fire time.
	if actualFireNs != 0 && actualFireNs < tInjectNs {
		return actualFireNs
	}
	return tInjectNs
}

// GuardStartNs is the baseline end and guard-window start (§3): one pinned
// client timeout before the fire anchor, floored at the measurement start.
// A pre-fault phase shorter than the timeout leaves the baseline window
// empty and the whole phase in the guard.
func GuardStartNs(cfg *config.Config, fireAnchorNs, measureStartNs int64) int64 {
	start := fireAnchorNs - int64(cfg.Client.HTTPTimeoutS)*1_000_000_000
	if start < measureStartNs {
		return measureStartNs
	}
	return start
}

// Stats is the full §3 reporting set for one window.
type Stats struct {
	Window Window `json:"window"`

	Scheduled int `json:"scheduled"`
	Completed int `json:"completed"`
	Errored   int `json:"errored"`
	Censored  int `json:"censored"`

	ErrorRate    float64        `json:"error_rate"`
	CensoredRate float64        `json:"censored_rate"`
	ErrClasses   map[string]int `json:"err_classes,omitempty"`

	// Conditional-on-completion distributions (labels are normative).
	TTFTConditional histo.Summary `json:"ttft_conditional_on_completion"`
	E2EConditional  histo.Summary `json:"e2e_conditional_on_completion"`
	ITLPooled       histo.Summary `json:"itl_pooled"`

	Incidence IncidenceCurve `json:"completion_incidence"`
	// ConditionalCaveat: error+censored fraction exceeds 5%, so
	// completed-only percentiles MUST be read against the incidence curve (§3).
	ConditionalCaveat bool `json:"conditional_caveat"`

	ThroughputRPS float64 `json:"throughput_rps"` // completions per second
	GoodputRPS    float64 `json:"goodput_rps"`    // SLO-meeting completions per second
	GoodputFrac   float64 `json:"goodput_frac"`   // SLO-meeting completions / scheduled

	// GoodputSweep is the §4 goodput-versus-threshold sweep: goodput
	// recomputed over the pinned 3x3 threshold grid.
	GoodputSweep []SweepPoint `json:"goodput_sweep"`

	// Order-statistic confidence intervals for p95/p99 (§7 tail policy),
	// whose endpoints are order statistics of the raw completed samples
	// (interval ranks via the normal approximation to the binomial),
	// where the sample budget permits.
	TTFTTailCI TailCIs `json:"ttft_tail_ci"`
	E2ETailCI  TailCIs `json:"e2e_tail_ci"`

	// Raw completed latencies (us), retained for cross-window analysis
	// (§4 modal/SD statement); not serialized (the histograms and CIs
	// above are the reporting surface).
	RawTTFTUs []int64 `json:"-"`
	RawE2EUs  []int64 `json:"-"`
}

// SweepPoint is one cell of the §4 goodput-versus-threshold sweep.
type SweepPoint struct {
	TTFTMs      int     `json:"ttft_ms"`
	E2EMs       int     `json:"e2e_ms"`
	GoodputFrac float64 `json:"goodput_frac"`
}

// TailCIs carries the §7 order-statistic CIs for p95 and p99. A field is
// nil when the window has no completed samples.
type TailCIs struct {
	P95 *OrderStatCI `json:"p95,omitempty"`
	P99 *OrderStatCI `json:"p99,omitempty"`
}

// OrderStatCI is a §7 order-statistic confidence interval [LoUs, HiUs] for
// a tail percentile; Permitted is false when the completed-sample budget
// does not place both interval ranks inside the sample, in which case the
// bounds are zero and the percentile publishes with an explicit no-CI
// caveat rather than a fabricated interval.
type OrderStatCI struct {
	LoUs      int64 `json:"lo_us"`
	HiUs      int64 `json:"hi_us"`
	Permitted bool  `json:"permitted"`
}

// meetsSLO implements §4: TTFT <= 1000 ms, e2e <= 14 s, completed without
// error. Latencies are re-based (conditional on the request's own t_i).
func meetsSLO(cfg *config.Config, r *loadgen.Request) bool {
	return r.Outcome == loadgen.OutcomeCompleted &&
		r.TTFTNs() <= int64(cfg.SLO.TTFTMs)*1_000_000 &&
		r.E2ENs() <= int64(cfg.SLO.E2EMs)*1_000_000
}

// Collect computes Stats for one window. Histograms use the pinned
// configuration; recording is recordValue-only via internal/histo.
func Collect(cfg *config.Config, requests []loadgen.Request, w Window) (*Stats, error) {
	st := &Stats{Window: w, ErrClasses: map[string]int{}}
	ttft, e2e, itl := histo.New(cfg.Histogram), histo.New(cfg.Histogram), histo.New(cfg.Histogram)
	horizonUs := int64(cfg.Client.HTTPTimeoutS) * 1_000_000
	var curveObs []Obs
	goodput := 0
	sweepGood := make([]int, len(cfg.SLO.Sweep.TTFTMs)*len(cfg.SLO.Sweep.E2EMs))

	for i := range requests {
		r := &requests[i]
		if r.IntendedNs < w.StartNs || r.IntendedNs >= w.EndNs {
			continue
		}
		st.Scheduled++
		obsUs := (r.DoneNs - r.IntendedNs) / 1000
		switch r.Outcome {
		case loadgen.OutcomeCompleted:
			st.Completed++
			if err := ttft.Record(r.TTFTNs() / 1000); err != nil {
				return nil, fmt.Errorf("collect: ttft: %w", err)
			}
			if err := e2e.Record(r.E2ENs() / 1000); err != nil {
				return nil, fmt.Errorf("collect: e2e: %w", err)
			}
			for _, gap := range r.ITLsUs {
				if gap < 1 {
					gap = 1 // pinned lowest discernible value
				}
				if err := itl.Record(gap); err != nil {
					return nil, fmt.Errorf("collect: itl: %w", err)
				}
			}
			curveObs = append(curveObs, Obs{TimeUs: r.E2ENs() / 1000, Kind: ObsCompletion})
			if meetsSLO(cfg, r) {
				goodput++
			}
			st.RawTTFTUs = append(st.RawTTFTUs, r.TTFTNs()/1000)
			st.RawE2EUs = append(st.RawE2EUs, r.E2ENs()/1000)
			for ti, tms := range cfg.SLO.Sweep.TTFTMs {
				for ei, ems := range cfg.SLO.Sweep.E2EMs {
					if r.TTFTNs() <= int64(tms)*1_000_000 && r.E2ENs() <= int64(ems)*1_000_000 {
						sweepGood[ti*len(cfg.SLO.Sweep.E2EMs)+ei]++
					}
				}
			}
		case loadgen.OutcomeErrored:
			st.Errored++
			st.ErrClasses[r.ErrClass]++
			curveObs = append(curveObs, Obs{TimeUs: obsUs, Kind: ObsError})
		case loadgen.OutcomeCensored:
			st.Censored++
			curveObs = append(curveObs, Obs{TimeUs: obsUs, Kind: ObsCensored})
		default:
			return nil, fmt.Errorf("collect: request %d has no terminal outcome %q", r.Index, r.Outcome)
		}
	}

	if st.Scheduled > 0 {
		st.ErrorRate = float64(st.Errored) / float64(st.Scheduled)
		st.CensoredRate = float64(st.Censored) / float64(st.Scheduled)
		st.GoodputFrac = float64(goodput) / float64(st.Scheduled)
	}
	durS := float64(w.EndNs-w.StartNs) / 1e9
	if durS > 0 {
		st.ThroughputRPS = float64(st.Completed) / durS
		st.GoodputRPS = float64(goodput) / durS
	}
	st.TTFTConditional = ttft.Summarize()
	st.E2EConditional = e2e.Summarize()
	st.ITLPooled = itl.Summarize()
	st.Incidence = EstimateIncidence(curveObs, horizonUs)
	st.ConditionalCaveat = st.ErrorRate+st.CensoredRate > 0.05

	for ti, tms := range cfg.SLO.Sweep.TTFTMs {
		for ei, ems := range cfg.SLO.Sweep.E2EMs {
			frac := 0.0
			if st.Scheduled > 0 {
				frac = float64(sweepGood[ti*len(cfg.SLO.Sweep.E2EMs)+ei]) / float64(st.Scheduled)
			}
			st.GoodputSweep = append(st.GoodputSweep, SweepPoint{TTFTMs: tms, E2EMs: ems, GoodputFrac: frac})
		}
	}
	st.TTFTTailCI = tailCIs(st.RawTTFTUs)
	st.E2ETailCI = tailCIs(st.RawE2EUs)
	return st, nil
}

// tailCIs computes distribution-free order-statistic confidence intervals
// (95% level, normal approximation to the binomial ranks) for p95 and
// p99. Permitted only when the interval's ranks fall inside the sample:
// the §7 "where the completed-sample budget permits" condition.
func tailCIs(rawUs []int64) TailCIs {
	sorted := make([]int64, len(rawUs))
	copy(sorted, rawUs)
	slices.Sort(sorted)
	return TailCIs{P95: orderStatCI(sorted, 0.95), P99: orderStatCI(sorted, 0.99)}
}

func orderStatCI(sorted []int64, q float64) *OrderStatCI {
	n := len(sorted)
	if n == 0 {
		return nil
	}
	const z = 1.959964 // 95%
	mean := float64(n) * q
	half := z * math.Sqrt(float64(n)*q*(1-q))
	lo := int(math.Floor(mean - half))
	hi := int(math.Ceil(mean + half))
	if lo < 1 || hi > n {
		return &OrderStatCI{Permitted: false}
	}
	return &OrderStatCI{LoUs: sorted[lo-1], HiUs: sorted[hi-1], Permitted: true}
}

// ThresholdAnalysis is the §4 statement: how many baseline standard
// deviations the modal during-fault latency sits from each threshold.
type ThresholdAnalysis struct {
	BaselineTTFTSDMs float64 `json:"baseline_ttft_sd_ms"`
	BaselineE2ESDMs  float64 `json:"baseline_e2e_sd_ms"`
	ModalFaultTTFTMs float64 `json:"modal_fault_ttft_ms"`
	ModalFaultE2EMs  float64 `json:"modal_fault_e2e_ms"`
	// Distances are (modal - threshold) / baselineSD, signed.
	TTFTThresholdSDs map[string]SDDistance `json:"ttft_threshold_sds"`
	E2EThresholdSDs  map[string]SDDistance `json:"e2e_threshold_sds"`
	Valid            bool                  `json:"valid"`
	Note             string                `json:"note,omitempty"`
}

// AnalyzeThresholds computes the §4 modal/SD statement from the baseline
// and fault windows' raw completed samples.
func AnalyzeThresholds(cfg *config.Config, baseline, fault *Stats) ThresholdAnalysis {
	out := ThresholdAnalysis{TTFTThresholdSDs: map[string]SDDistance{}, E2EThresholdSDs: map[string]SDDistance{}}
	if len(baseline.RawTTFTUs) < 2 || len(fault.RawTTFTUs) == 0 {
		out.Note = "insufficient completed samples in baseline or fault window"
		return out
	}
	out.Valid = true
	out.BaselineTTFTSDMs = stddevMs(baseline.RawTTFTUs)
	out.BaselineE2ESDMs = stddevMs(baseline.RawE2EUs)
	out.ModalFaultTTFTMs = modeMs(fault.RawTTFTUs)
	out.ModalFaultE2EMs = modeMs(fault.RawE2EUs)
	for _, tms := range cfg.SLO.Sweep.TTFTMs {
		out.TTFTThresholdSDs[fmt.Sprintf("%dms", tms)] = sdDistance(out.ModalFaultTTFTMs, float64(tms), out.BaselineTTFTSDMs)
	}
	for _, ems := range cfg.SLO.Sweep.E2EMs {
		out.E2EThresholdSDs[fmt.Sprintf("%dms", ems)] = sdDistance(out.ModalFaultE2EMs, float64(ems), out.BaselineE2ESDMs)
	}
	return out
}

func sdDistance(modalMs, thresholdMs, sdMs float64) SDDistance {
	if sdMs == 0 {
		return SDDistance(math.Inf(int(math.Copysign(1, modalMs-thresholdMs))))
	}
	return SDDistance((modalMs - thresholdMs) / sdMs)
}

// SDDistance is a signed modal-threshold distance in baseline SDs. §4
// mandates signed infinity at zero SD; JSON has no Inf, so it marshals
// as the strings "+inf" and "-inf".
type SDDistance float64

func (d SDDistance) MarshalJSON() ([]byte, error) {
	switch {
	case math.IsInf(float64(d), 1):
		return []byte(`"+inf"`), nil
	case math.IsInf(float64(d), -1):
		return []byte(`"-inf"`), nil
	}
	return json.Marshal(float64(d))
}

func (d *SDDistance) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case `"+inf"`:
		*d = SDDistance(math.Inf(1))
		return nil
	case `"-inf"`:
		*d = SDDistance(math.Inf(-1))
		return nil
	}
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	*d = SDDistance(f)
	return nil
}

// stddevMs is the two-pass sample standard deviation (n-1) of the completed
// latencies in ms, matching internal/stats.SampleSD so the §4 "SDs from
// threshold" statement uses one variance definition across the tree.
func stddevMs(rawUs []int64) float64 {
	n := len(rawUs)
	if n < 2 {
		return 0
	}
	var sum float64
	for _, v := range rawUs {
		sum += float64(v) / 1000
	}
	mean := sum / float64(n)
	var ss float64
	for _, v := range rawUs {
		d := float64(v)/1000 - mean
		ss += d * d
	}
	return math.Sqrt(ss / float64(n-1))
}

// modeMs estimates the modal latency by histogram binning at 1% of the
// median (floor 1 ms), returning the densest bin's center.
func modeMs(rawUs []int64) float64 {
	sorted := make([]int64, len(rawUs))
	copy(sorted, rawUs)
	slices.Sort(sorted)
	median := float64(sorted[len(sorted)/2]) / 1000
	binMs := median / 100
	if binMs < 1 {
		binMs = 1
	}
	counts := map[int]int{}
	best, bestCount := 0, -1
	for _, v := range rawUs {
		bin := int(float64(v) / 1000 / binMs)
		counts[bin]++
		if counts[bin] > bestCount {
			best, bestCount = bin, counts[bin]
		}
	}
	return (float64(best) + 0.5) * binMs
}

// InFlightAccounting classifies the requests that were in flight at
// the fire instant (dispatched, no terminal event yet) by their eventual
// outcome (§3 in-flight loss accounting), split by serving replica when
// known.
type InFlightAccounting struct {
	Total     int            `json:"total"`
	Completed int            `json:"completed"`
	Errored   int            `json:"errored"`
	Censored  int            `json:"censored"`
	ByReplica map[string]int `json:"by_replica,omitempty"`

	// The §3 headline class is the KILLED replica's in-flight requests,
	// classified by outcome. Zero-valued when the victim is unknown.
	OnVictim          int `json:"on_victim"`
	OnVictimCompleted int `json:"on_victim_completed"`
	OnVictimErrored   int `json:"on_victim_errored"`
	OnVictimCensored  int `json:"on_victim_censored"`
}

// AccountInFlight performs the §3 in-flight loss accounting: it selects the
// requests dispatched before the fire instant that had no terminal event
// by then, classifies each by eventual outcome, splits the counts by
// serving replica, and isolates the victim replica's in-flight requests
// (the headline killed-replica class) when victimReplica is known.
func AccountInFlight(requests []loadgen.Request, fireNs int64, victimReplica string) InFlightAccounting {
	acc := InFlightAccounting{ByReplica: map[string]int{}}
	for i := range requests {
		r := &requests[i]
		if r.DispatchNs == 0 || r.DispatchNs >= fireNs || r.DoneNs < fireNs {
			continue
		}
		acc.Total++
		acc.ByReplica[r.Replica]++
		onVictim := victimReplica != "" && r.Replica == victimReplica
		if onVictim {
			acc.OnVictim++
		}
		switch r.Outcome {
		case loadgen.OutcomeCompleted:
			acc.Completed++
			if onVictim {
				acc.OnVictimCompleted++
			}
		case loadgen.OutcomeErrored:
			acc.Errored++
			if onVictim {
				acc.OnVictimErrored++
			}
		case loadgen.OutcomeCensored:
			acc.Censored++
			if onVictim {
				acc.OnVictimCensored++
			}
		}
	}
	return acc
}
