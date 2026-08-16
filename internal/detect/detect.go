// Package detect is the recovery detector (SPEC.md §5): hysteresis-based,
// two baselines, pre-registered numbers (entry X=90% of the applicable
// baseline, exit below 85%, sliding window R=10 s, hold H=30 s), with the
// pre-registered sensitivity sweep and the integrated-goodput-deficit
// companion metric.
//
// Window semantics: the goodput value at time t is the ratio-of-sums over
// requests SCHEDULED in the leading window [t, t+R). Leading windows make
// the entry timestamp the start of sustained-good service (TTR is not
// biased by +R as it would be with trailing windows). Recovery entry at
// the first t whose window meets X% of the applicable baseline and whose
// following H one-second window starts all stay >= the entry bar (a dip
// below entry cancels the candidate; a dip below the exit bar after
// confirmed recovery is a re-degradation). TTR = first surviving entry
// minus T_inject. Non-recovery past the fault-window timeout is reported
// as such, never extrapolated.
package detect

import (
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

const nsPerSec int64 = 1e9

// Bucket is one second of scheduled traffic, assigned by intended time.
type Bucket struct {
	StartNs   int64
	Scheduled int
	Completed int
	Errored   int
	Good      int // meets the full §4 SLO
	GoodTTFT  int // completed and TTFT within SLO
	GoodE2E   int // completed and e2e within SLO
}

// BuildSeries buckets requests into 1 s intervals over [startNs, endNs).
// The bucket count rounds UP so a fractional final second still has a
// bucket (every admitted request must have a home).
func BuildSeries(cfg *config.Config, requests []loadgen.Request, startNs, endNs int64) []Bucket {
	n := int((endNs - startNs + nsPerSec - 1) / nsPerSec)
	if n <= 0 {
		return nil
	}
	buckets := make([]Bucket, n)
	for i := range buckets {
		buckets[i].StartNs = startNs + int64(i)*nsPerSec
	}
	ttftSLO, e2eSLO := int64(cfg.SLO.TTFTMs)*1e6, int64(cfg.SLO.E2EMs)*1e6
	for i := range requests {
		r := &requests[i]
		if r.IntendedNs < startNs || r.IntendedNs >= endNs {
			continue
		}
		b := &buckets[(r.IntendedNs-startNs)/nsPerSec]
		b.Scheduled++
		switch r.Outcome {
		case loadgen.OutcomeCompleted:
			b.Completed++
			ttftOK := r.TTFTNs() <= ttftSLO
			e2eOK := r.E2ENs() <= e2eSLO
			if ttftOK {
				b.GoodTTFT++
			}
			if e2eOK {
				b.GoodE2E++
			}
			if ttftOK && e2eOK {
				b.Good++
			}
		case loadgen.OutcomeErrored:
			b.Errored++
		}
	}
	return buckets
}

// component selects which per-bucket numerator a detection runs on.
type component func(*Bucket) (num, den int)

func compGoodput(b *Bucket) (int, int)  { return b.Good, b.Scheduled }
func compTTFTSLO(b *Bucket) (int, int)  { return b.GoodTTFT, b.Scheduled }
func compE2ESLO(b *Bucket) (int, int)   { return b.GoodE2E, b.Scheduled }
func compNonError(b *Bucket) (int, int) { return b.Scheduled - b.Errored, b.Scheduled }

// windowRatio computes the ratio-of-sums over the leading window
// [fromIdx, fromIdx+windowS).
func windowRatio(buckets []Bucket, fromIdx, windowS int, comp component) (float64, bool) {
	num, den := 0, 0
	for i := fromIdx; i < fromIdx+windowS && i < len(buckets); i++ {
		n, d := comp(&buckets[i])
		num += n
		den += d
	}
	if den == 0 {
		return 0, false
	}
	return float64(num) / float64(den), true
}

// Params are detector parameters (pre-registered values live in config;
// the sensitivity sweep varies them).
type Params struct {
	WindowS  int `json:"window_s"`
	EntryPct int `json:"entry_pct"`
	ExitPct  int `json:"exit_pct"`
	HoldS    int `json:"hold_s"`
}

// Detection is one detector outcome against one baseline.
type Detection struct {
	Baseline        float64  `json:"baseline"`
	Params          Params   `json:"params"`
	TTRSeconds      *float64 `json:"ttr_seconds,omitempty"`
	RecoveredAtNs   *int64   `json:"recovered_at_ns,omitempty"`
	NotRecovered    bool     `json:"not_recovered"`
	CanceledEntries int      `json:"canceled_entries"` // hysteresis: candidates killed during hold
	ReDegradations  int      `json:"re_degradations"`  // post-recovery dips below exit
}

// detect runs the hysteresis state machine over buckets, scanning entry
// candidates from tInjectNs; windows must start before timeoutNs.
func detect(buckets []Bucket, comp component, baseline float64, tInjectNs, timeoutNs int64, p Params) Detection {
	det := Detection{Baseline: baseline, Params: p}
	if len(buckets) == 0 || baseline <= 0 {
		det.NotRecovered = true
		return det
	}
	entryBar := baseline * float64(p.EntryPct) / 100
	exitBar := baseline * float64(p.ExitPct) / 100
	origin := buckets[0].StartNs

	// Scan starts at the first bucket fully AFTER the fire time: the
	// bucket straddling T_inject belongs to no window (§3).
	firstIdx := int((tInjectNs - origin + nsPerSec - 1) / nsPerSec)
	if firstIdx < 0 {
		firstIdx = 0
	}
	lastIdx := int((timeoutNs - origin) / nsPerSec)
	if lastIdx > len(buckets) {
		lastIdx = len(buckets)
	}

	for i := firstIdx; i < lastIdx; i++ {
		w, ok := windowRatio(buckets, i, p.WindowS, comp)
		if !ok || w < entryBar {
			continue
		}
		// Candidate entry at bucket i: hold requires every window
		// starting in [i, i+HoldS] to stay at or above the entry bar.
		held := true
		for j := i + 1; j <= i+p.HoldS && held; j++ {
			hw, hok := windowRatio(buckets, j, p.WindowS, comp)
			if !hok || hw < entryBar {
				held = false
				det.CanceledEntries++
				i = j // resume scanning after the dip
			}
		}
		if !held {
			continue
		}
		at := buckets[i].StartNs
		ttr := float64(at-tInjectNs) / float64(nsPerSec)
		det.RecoveredAtNs, det.TTRSeconds = &at, &ttr

		// Post-recovery re-degradation with the full hysteresis band: an
		// episode starts below the EXIT bar and ends only when the
		// window climbs back to the ENTRY bar — oscillation inside
		// [exit, entry) neither starts nor ends an episode.
		below := false
		for j := i + p.HoldS + 1; j < lastIdx; j++ {
			hw, hok := windowRatio(buckets, j, p.WindowS, comp)
			if !hok {
				continue
			}
			switch {
			case hw < exitBar && !below:
				det.ReDegradations++
				below = true
			case hw >= entryBar:
				below = false
			}
		}
		return det
	}
	det.NotRecovered = true
	return det
}

// baselineOver computes the ratio-of-sums over [fromNs, toNs).
func baselineOver(buckets []Bucket, comp component, fromNs, toNs int64) float64 {
	num, den := 0, 0
	for i := range buckets {
		b := &buckets[i]
		if b.StartNs < fromNs || b.StartNs >= toNs {
			continue
		}
		n, d := comp(b)
		num += n
		den += d
	}
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// deficit integrates (baseline - goodput)+ x 1s from T_inject to recovery
// (or the timeout when unrecovered) — the crossing-time-fragility
// companion metric (§5).
func deficit(buckets []Bucket, comp component, baseline float64, tInjectNs, untilNs int64) float64 {
	total := 0.0
	for i := range buckets {
		b := &buckets[i]
		if b.StartNs < tInjectNs || b.StartNs >= untilNs {
			continue
		}
		n, d := comp(b)
		if d == 0 {
			continue
		}
		if gap := baseline - float64(n)/float64(d); gap > 0 {
			total += gap
		}
	}
	return total
}

// SensitivityRow is one entry of the §5 pre-registered sweep.
type SensitivityRow struct {
	Params           Params   `json:"params"`
	TTRToPreFault    *float64 `json:"ttr_to_prefault_s,omitempty"`
	TTRToEquilibrium *float64 `json:"ttr_to_equilibrium_s,omitempty"`
	NotRecoveredPre  bool     `json:"not_recovered_prefault"`
	NotRecoveredEq   bool     `json:"not_recovered_equilibrium"`
}

// Result is the full §5 detector output for one run.
type Result struct {
	PreFaultBaseline    float64 `json:"pre_fault_baseline"`
	EquilibriumBaseline float64 `json:"equilibrium_baseline"`
	// EquilibriumEstimable is false when the run offers no stable
	// degraded plateau to estimate the single-replica equilibrium from
	// (e.g. a total outage, or recovery arriving before a plateau of at
	// least R seconds forms). The two baselines answer different
	// questions and must never collapse into each other: the equilibrium
	// is estimated from the DEGRADED period (post-fire settle to
	// recovery), never from the post-recovery tail.
	EquilibriumEstimable    bool    `json:"equilibrium_estimable"`
	EquilibriumWindowStartS float64 `json:"equilibrium_window_start_s"`
	EquilibriumWindowEndS   float64 `json:"equilibrium_window_end_s"`
	EquilibriumNote         string  `json:"equilibrium_note,omitempty"`

	// Two baselines, both reported (§5): they answer different questions.
	ToPreFault    Detection `json:"to_pre_fault"`
	ToEquilibrium Detection `json:"to_equilibrium"`

	DeficitToPreFault    float64 `json:"integrated_goodput_deficit_to_prefault"`
	DeficitToEquilibrium float64 `json:"integrated_goodput_deficit_to_equilibrium"`

	Sensitivity []SensitivityRow `json:"sensitivity"`

	// Per-component recovery (§5): TTFT-SLO, e2e-SLO, error rate, each
	// against its own pre-fault level with the pinned parameters.
	Components map[string]Detection `json:"components"`

	// BacklogDrain is reported only where a failed-request backlog
	// exists; the mock serves from no queue, so Phase 0 reports N/A.
	BacklogDrainMeasured bool   `json:"backlog_drain_measured"`
	BacklogDrainNote     string `json:"backlog_drain_note"`
}

// Run executes the full §5 analysis. buckets must cover
// [warmupEnd, runEnd); tInjectNs/timeoutNs delimit the fault window.
func Run(cfg *config.Config, buckets []Bucket, warmupEndNs, tInjectNs, timeoutNs int64) *Result {
	p := Params{
		WindowS:  cfg.RecoveryDetector.WindowS,
		EntryPct: cfg.RecoveryDetector.EntryPct,
		ExitPct:  cfg.RecoveryDetector.ExitPct,
		HoldS:    cfg.RecoveryDetector.HoldS,
	}
	res := &Result{
		BacklogDrainMeasured: false,
		BacklogDrainNote:     "no failed-request backlog exists in the Phase 0 mock (no queue); reported N/A per §5",
		Components:           map[string]Detection{},
	}

	// Pre-fault baseline ends at the last bucket boundary fully before
	// the fire: the straddling bucket belongs to no window (§3).
	origin := int64(0)
	if len(buckets) > 0 {
		origin = buckets[0].StartNs
	}
	alignedInject := origin + ((tInjectNs - origin) / nsPerSec * nsPerSec)
	res.PreFaultBaseline = baselineOver(buckets, compGoodput, warmupEndNs, alignedInject)

	res.ToPreFault = detect(buckets, compGoodput, res.PreFaultBaseline, tInjectNs, timeoutNs, p)

	// Single-replica equilibrium: estimated over the DEGRADED plateau —
	// from R seconds after fire (settle) until recovery-to-pre-fault, or
	// the timeout when unrecovered. The fault-window tail would collapse
	// into the post-recovery state in any recovered run (§5).
	plateauStart := tInjectNs + int64(p.WindowS)*nsPerSec
	plateauEnd := timeoutNs
	if at := res.ToPreFault.RecoveredAtNs; at != nil && *at < plateauEnd {
		plateauEnd = *at
	}
	res.EquilibriumWindowStartS = float64(plateauStart) / float64(nsPerSec)
	res.EquilibriumWindowEndS = float64(plateauEnd) / float64(nsPerSec)
	if plateauEnd-plateauStart >= int64(p.WindowS)*nsPerSec {
		res.EquilibriumBaseline = baselineOver(buckets, compGoodput, plateauStart, plateauEnd)
		res.EquilibriumEstimable = res.EquilibriumBaseline > 0
		if !res.EquilibriumEstimable {
			res.EquilibriumNote = "no service during the degraded plateau; single-replica equilibrium undefined for this run"
		}
	} else {
		res.EquilibriumNote = "degraded plateau shorter than R; single-replica equilibrium not estimable for this run"
	}
	if res.EquilibriumEstimable {
		res.ToEquilibrium = detect(buckets, compGoodput, res.EquilibriumBaseline, tInjectNs, timeoutNs, p)
	} else {
		res.ToEquilibrium = Detection{Baseline: res.EquilibriumBaseline, Params: p, NotRecovered: true}
	}

	untilPre := timeoutNs
	if res.ToPreFault.RecoveredAtNs != nil {
		untilPre = *res.ToPreFault.RecoveredAtNs
	}
	res.DeficitToPreFault = deficit(buckets, compGoodput, res.PreFaultBaseline, tInjectNs, untilPre)
	if res.EquilibriumEstimable {
		untilEq := timeoutNs
		if res.ToEquilibrium.RecoveredAtNs != nil {
			untilEq = *res.ToEquilibrium.RecoveredAtNs
		}
		res.DeficitToEquilibrium = deficit(buckets, compGoodput, res.EquilibriumBaseline, tInjectNs, untilEq)
	}

	// Pre-registered sensitivity sweep (§5); exit stays pinned.
	for _, x := range cfg.RecoveryDetector.Sensitivity.EntryPct {
		for _, r := range cfg.RecoveryDetector.Sensitivity.WindowS {
			for _, h := range cfg.RecoveryDetector.Sensitivity.HoldS {
				sp := Params{WindowS: r, EntryPct: x, ExitPct: p.ExitPct, HoldS: h}
				dp := detect(buckets, compGoodput, res.PreFaultBaseline, tInjectNs, timeoutNs, sp)
				row := SensitivityRow{
					Params:        sp,
					TTRToPreFault: dp.TTRSeconds, NotRecoveredPre: dp.NotRecovered,
					NotRecoveredEq: true,
				}
				if res.EquilibriumEstimable {
					de := detect(buckets, compGoodput, res.EquilibriumBaseline, tInjectNs, timeoutNs, sp)
					row.TTRToEquilibrium, row.NotRecoveredEq = de.TTRSeconds, de.NotRecovered
				}
				res.Sensitivity = append(res.Sensitivity, row)
			}
		}
	}

	// Per-component recovery against each component's pre-fault level.
	for name, comp := range map[string]component{
		"ttft_slo":   compTTFTSLO,
		"e2e_slo":    compE2ESLO,
		"error_rate": compNonError,
	} {
		base := baselineOver(buckets, comp, warmupEndNs, alignedInject)
		res.Components[name] = detect(buckets, comp, base, tInjectNs, timeoutNs, p)
	}
	return res
}

// ---------------------------------------------------------------------------
// Recovery decomposition scaffolding (§5): only measured boundaries are
// claimed; everything else is explicitly N/A with its reason.
// ---------------------------------------------------------------------------

// Segment is one boundary-to-boundary piece of the recovery timeline.
type Segment struct {
	Name     string     `json:"name"`
	Source   string     `json:"source"` // "api" | "log" | "probe" | "client"
	Measured bool       `json:"measured"`
	StartAt  *time.Time `json:"start_at,omitempty"`
	EndAt    *time.Time `json:"end_at,omitempty"`
	Note     string     `json:"note,omitempty"`
}

// DurationS returns the segment length in seconds, or nil when the segment
// is unmeasured or missing a boundary.
func (s Segment) DurationS() *float64 {
	if !s.Measured || s.StartAt == nil || s.EndAt == nil {
		return nil
	}
	d := s.EndAt.Sub(*s.StartAt).Seconds()
	return &d
}

// Decomposition carries the §5 segment table. Phase 0 measures
// replica-ready and traffic-restored via harness probes and
// goodput-restored from the client stream; the Kubernetes- and log-
// derived segments are N/A against the mock and are reported as such,
// never inferred.
type Decomposition struct {
	Segments []Segment `json:"segments"`
}

// NewPhase0Decomposition returns the §5 segment table with every boundary
// marked N/A until measured.
func NewPhase0Decomposition() *Decomposition {
	na := func(name, source, note string) Segment {
		return Segment{Name: name, Source: source, Measured: false, Note: note}
	}
	return &Decomposition{Segments: []Segment{
		na("reschedule", "api", "N/A in Phase 0: mock faults do not delete the pod, so there is no kill-to-scheduled event pair"),
		na("container_start", "api", "N/A in Phase 0: no container restart in mock fault scenarios"),
		na("weight_load", "log", "N/A in Phase 0: mock has no weight-load log line; boundary is log-derived and reported N/A, never inferred"),
		na("cuda_graph_capture", "log", "N/A in Phase 0: zero GPU code paths"),
		{Name: "replica_ready", Source: "probe", Measured: false, Note: "first successful inference against the pod IP directly, after the fault was visible on that path"},
		{Name: "traffic_restored", Source: "probe", Measured: false, Note: "first successful inference via the Service served by the recovered replica, after the fault was visible"},
		{Name: "routing_propagation", Source: "probe", Measured: false, Note: "traffic_restored minus replica_ready (§5: reported as its own segment)"},
		{Name: "goodput_restored", Source: "client", Measured: false, Note: "from the client stream per the detector"},
	}}
}

// SetMeasured fills in a segment's boundaries.
func (d *Decomposition) SetMeasured(name string, start, end time.Time) {
	for i := range d.Segments {
		if d.Segments[i].Name == name {
			d.Segments[i].Measured = true
			d.Segments[i].StartAt = &start
			d.Segments[i].EndAt = &end
			return
		}
	}
}
