package loadgen

import (
	"fmt"
	"math"
	"runtime/metrics"
	"sort"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"

	"github.com/percentes/percentes/internal/config"
)

// GateReport is the §2 client-validity gate outcome — run-failing, with
// every threshold pinned in config (enforced equal to SPEC.md values).
type GateReport struct {
	SendSkewP99Us    int64 `json:"send_skew_p99_us"`
	SendSkewMaxUs    int64 `json:"send_skew_max_us"`
	SendSkewPass     bool  `json:"send_skew_pass"`
	Undispatched     int   `json:"undispatched"`
	UndispatchedPass bool  `json:"undispatched_pass"`

	// CPUMeasured is false when the platform provided no host central
	// processing unit (CPU) samples (e.g. darwin built without cgo) or
	// fewer than cpu_window_s samples landed in the measurement span: the
	// sustained-window criterion cannot be certified without at least one
	// full window. An unmeasured gate does not pass. Linux (/proc)
	// measures without cgo; the acceptance-criteria (AC) suite on darwin
	// runs under go test with cgo and measures.
	CPUMeasured       bool    `json:"cpu_measured"`
	CPUPeakPct        float64 `json:"cpu_peak_pct"`
	CPUWorstWindowPct float64 `json:"cpu_worst_window_pct"`
	CPUPass           bool    `json:"cpu_pass"`

	// GCPauseP99LoMs and GCPauseP99Ms are the lower and upper edges of
	// the runtime histogram bucket holding the p99 pause; the gate reads
	// the upper edge.
	GCPauseP99LoMs float64 `json:"gc_pause_p99_lo_ms"`
	GCPauseP99Ms   float64 `json:"gc_pause_p99_ms"`
	GCPass         bool    `json:"gc_pass"`

	Pass bool `json:"pass"`
}

// GCPauseText renders the garbage collection (GC) pause p99 as the
// runtime bucket that holds it, or says that no pause fell in the span.
func (g GateReport) GCPauseText() string {
	if g.GCPauseP99LoMs == 0 && g.GCPauseP99Ms == 0 {
		return "gc pause p99: no pause in the span"
	}
	return fmt.Sprintf("gc pause p99 in [%.3f, %.3f) ms, runtime bucket edges, gate on the upper edge", g.GCPauseP99LoMs, g.GCPauseP99Ms)
}

func evaluateGates(cfg *config.Config, requests []Request, samples []cpuSample, gcLoMs, gcHiMs float64, measStartNs, measEndNs int64) GateReport {
	rep := GateReport{GCPauseP99LoMs: gcLoMs, GCPauseP99Ms: gcHiMs}
	v := cfg.ClientValidity

	// Send skew (actual minus intended dispatch) over the measurement
	// span, exact order statistics.
	var skews []int64
	for i := range requests {
		r := &requests[i]
		if r.DispatchNs == 0 {
			rep.Undispatched++
			continue
		}
		if r.IntendedNs >= measStartNs && r.IntendedNs < measEndNs {
			skews = append(skews, r.DispatchNs-r.IntendedNs)
		}
	}
	sort.Slice(skews, func(a, b int) bool { return skews[a] < skews[b] })
	var p99Ns, maxNs int64
	if n := len(skews); n > 0 {
		p99Ns, maxNs = skews[(n*99+99)/100-1], skews[n-1] // ceil index for p99
		rep.SendSkewP99Us, rep.SendSkewMaxUs = p99Ns/1000, maxNs/1000
	}
	// Compared in nanoseconds; the report fields round down to microseconds.
	rep.SendSkewPass = p99Ns <= int64(v.SendSkewP99Ms)*1_000_000 &&
		maxNs <= int64(v.SendSkewMaxMs)*1_000_000
	rep.UndispatchedPass = rep.Undispatched == 0

	// Client CPU: sustained mean over any window of cpu_window_s
	// consecutive 1 Hz samples inside the measurement span.
	win := v.CPUWindowS
	var inSpan []float64
	for _, s := range samples {
		if s.atNs >= measStartNs && s.atNs < measEndNs {
			inSpan = append(inSpan, s.pct)
			if s.pct > rep.CPUPeakPct {
				rep.CPUPeakPct = s.pct
			}
		}
	}
	rep.CPUMeasured = len(inSpan) >= win
	rep.CPUPass = rep.CPUMeasured
	for i := 0; i+win <= len(inSpan); i++ {
		var sum float64
		for _, p := range inSpan[i : i+win] {
			sum += p
		}
		mean := sum / float64(win)
		if mean > rep.CPUWorstWindowPct {
			rep.CPUWorstWindowPct = mean
		}
		if mean > float64(v.MaxCPUPct) {
			rep.CPUPass = false
		}
	}

	rep.GCPass = gcHiMs < float64(v.GoGCPauseP99Ms)
	rep.Pass = rep.SendSkewPass && rep.UndispatchedPass && rep.CPUPass && rep.GCPass
	return rep
}

// SyntheticGateCheck exercises the undispatched gate with a fabricated
// result containing one scheduled-but-never-dispatched request (AC2c: it
// must fail the run, not vanish).
func SyntheticGateCheck() GateReport {
	cfg := &config.Config{ClientValidity: config.ClientValidity{
		SendSkewP99Ms:  config.PinnedSendSkewP99Ms,
		SendSkewMaxMs:  config.PinnedSendSkewMaxMs,
		MaxCPUPct:      config.PinnedClientCPUPct,
		CPUWindowS:     config.PinnedCPUWindowS,
		GoGCPauseP99Ms: config.PinnedGoGCPauseP99Ms,
	}}
	reqs := []Request{
		{Index: 0, IntendedNs: 1_000_000_000, DispatchNs: 1_000_100_000},
		{Index: 1, IntendedNs: 2_000_000_000, DispatchNs: 0}, // never dispatched
	}
	return evaluateGates(cfg, reqs, nil, 0, 0, 0, 10_000_000_000)
}

// ---------------------------------------------------------------------------
// CPU monitor: 1 Hz host CPU samples (the gate is about the client
// machine, so injected pressure from other processes must be visible).
// ---------------------------------------------------------------------------

type cpuSample struct {
	atNs int64
	pct  float64
}

// rawCPUSample stores a sample with a wall timestamp; converted to a run
// offset relative to the epoch in stopAndCollect.
type rawCPUSample struct {
	at  time.Time
	pct float64
}

type cpuMonitor struct {
	mu      sync.Mutex
	stop    chan struct{}
	done    chan struct{}
	samples []rawCPUSample
}

func startCPUMonitor() *cpuMonitor {
	m := &cpuMonitor{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(m.done)
		for {
			select {
			case <-m.stop:
				return
			default:
			}
			// Blocking 1 s sample of total host CPU.
			pcts, err := cpu.Percent(time.Second, false)
			if err != nil || len(pcts) == 0 {
				// Unsupported platform (returns immediately): keep the
				// 1 Hz cadence rather than busy-spinning; the empty
				// sample set surfaces as CPUMeasured=false.
				select {
				case <-m.stop:
					return
				case <-time.After(time.Second):
				}
				continue
			}
			m.mu.Lock()
			m.samples = append(m.samples, rawCPUSample{at: time.Now(), pct: pcts[0]})
			m.mu.Unlock()
		}
	}()
	return m
}

func (m *cpuMonitor) stopAndCollect(g *gen) []cpuSample {
	close(m.stop)
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]cpuSample, 0, len(m.samples))
	for _, s := range m.samples {
		out = append(out, cpuSample{atNs: s.at.Sub(g.epoch).Nanoseconds(), pct: s.pct})
	}
	return out
}

// ---------------------------------------------------------------------------
// GC pause monitor: /gc/pauses:seconds histogram snapshots at the
// measurement-span boundaries; the p99 of the diff is asserted < 1 ms (§2).
// ---------------------------------------------------------------------------

type gcHist struct {
	counts  []uint64
	buckets []float64
}

func readGCHist() *gcHist {
	s := []metrics.Sample{{Name: "/gc/pauses:seconds"}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindFloat64Histogram {
		return nil
	}
	h := s[0].Value.Float64Histogram()
	out := &gcHist{counts: make([]uint64, len(h.Counts)), buckets: make([]float64, len(h.Buckets))}
	copy(out.counts, h.Counts)
	copy(out.buckets, h.Buckets)
	return out
}

type gcMonitor struct {
	mu     sync.Mutex
	start  *gcHist
	end    *gcHist
	timers []*time.Timer
}

func startGCMonitor(startNs, endNs int64) *gcMonitor {
	m := &gcMonitor{}
	m.timers = append(m.timers,
		time.AfterFunc(time.Duration(startNs), func() {
			m.mu.Lock()
			m.start = readGCHist()
			m.mu.Unlock()
		}),
		time.AfterFunc(time.Duration(endNs), func() {
			m.mu.Lock()
			m.end = readGCHist()
			m.mu.Unlock()
		}))
	return m
}

// stopAndP99Ms returns the edges, in milliseconds, of the runtime
// histogram bucket that holds the GC pause p99 over the measurement span
// (0, 0 if no GC pause occurred). The runtime records pauses in fixed
// buckets, so the p99 is known only to its bucket; the gate reads the
// upper edge, which is +Inf for the last bucket.
func (m *gcMonitor) stopAndP99Ms() (loMs, hiMs float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.timers {
		t.Stop()
	}
	if m.start == nil {
		return 0, 0
	}
	if m.end == nil {
		m.end = readGCHist()
	}
	if len(m.start.counts) != len(m.end.counts) {
		return 0, 0
	}
	var total uint64
	diff := make([]uint64, len(m.end.counts))
	for i := range diff {
		diff[i] = m.end.counts[i] - m.start.counts[i]
		total += diff[i]
	}
	if total == 0 {
		return 0, 0
	}
	target := uint64(math.Ceil(float64(total) * 0.99))
	var cum uint64
	for i, c := range diff {
		cum += c
		if cum >= target {
			// buckets has len(counts)+1 edges; the first is -Inf.
			return math.Max(m.end.buckets[i], 0) * 1000, m.end.buckets[i+1] * 1000
		}
	}
	return 0, 0
}
