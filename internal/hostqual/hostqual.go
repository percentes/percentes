// Package hostqual qualifies the host for the timing-coupled acceptance
// tests. It measures timer wake lateness on an absolute schedule and the
// runtime's garbage-collection (GC) pauses over an allocation burst, and
// records the load average beside them.
package hostqual

import (
	"fmt"
	"math"
	"runtime"
	"runtime/metrics"
	"sort"
	"time"

	"github.com/shirou/gopsutil/v3/load"
)

// Observation is one qualification measurement.
type Observation struct {
	Wakes         int     `json:"wakes"`
	WakeSpacingMs int     `json:"wake_spacing_ms"`
	WakeP99Us     int64   `json:"wake_p99_us"`
	WakeMaxUs     int64   `json:"wake_max_us"`
	GCPauses      uint64  `json:"gc_pauses"`
	GCPauseP99Ms  float64 `json:"gc_pause_p99_ms"`         // upper edge of the runtime bucket holding the p99, or its lower edge when GCPauseOpen; 0 with no pause
	GCPauseOpen   bool    `json:"gc_pause_open,omitempty"` // the p99 bucket has no upper edge
	LoadAvg1      float64 `json:"load_avg_1"`
	NumCPU        int     `json:"num_cpu"`
}

// Limits qualify an observation.
type Limits struct {
	WakeP99Us    int64
	WakeMaxUs    int64
	GCPauseP99Ms float64
}

// Allocation is the pre-registered qualification threshold, dated
// 29 September 2026: one fifth of the §2 send-skew budget (5 ms p99,
// 50 ms max) for wake lateness, and the §2 GC pause pin itself, since the
// same runtime serves the client. It is an allocation of the budget.
var Allocation = Limits{WakeP99Us: 1000, WakeMaxUs: 10000, GCPauseP99Ms: 1.0}

const (
	wakes       = 400
	wakeSpacing = 5 * time.Millisecond
	burst       = 300 * time.Millisecond
)

// Measure runs the probes; it takes about two and a half seconds.
func Measure() Observation {
	o := Observation{Wakes: wakes, WakeSpacingMs: int(wakeSpacing / time.Millisecond), NumCPU: runtime.NumCPU()}
	if avg, err := load.Avg(); err == nil {
		o.LoadAvg1 = avg.Load1
	}

	late := make([]int64, wakes)
	start := time.Now().Add(wakeSpacing)
	for i := range late {
		target := start.Add(time.Duration(i) * wakeSpacing)
		if d := time.Until(target); d > 0 {
			time.Sleep(d)
		}
		late[i] = time.Since(target).Microseconds()
	}
	sort.Slice(late, func(a, b int) bool { return late[a] < late[b] })
	o.WakeP99Us = late[int(math.Ceil(0.99*float64(len(late))))-1]
	o.WakeMaxUs = late[len(late)-1]

	before := gcHist()
	var sink [][]byte
	for t0 := time.Now(); time.Since(t0) < burst; {
		sink = append(sink, make([]byte, 8<<10))
		if len(sink) > 4096 {
			sink = sink[:0]
		}
	}
	runtime.KeepAlive(sink)
	after := gcHist()
	o.GCPauses, o.GCPauseP99Ms, o.GCPauseOpen = pauseP99(before, after)
	return o
}

// Qualify reports whether the observation is inside the limits, with the
// first reason it is not.
func (o Observation) Qualify(l Limits) (bool, string) {
	switch {
	case o.WakeP99Us > l.WakeP99Us:
		return false, fmt.Sprintf("timer wake lateness p99 %d us over the %d us allocation", o.WakeP99Us, l.WakeP99Us)
	case o.WakeMaxUs > l.WakeMaxUs:
		return false, fmt.Sprintf("timer wake lateness max %d us over the %d us allocation", o.WakeMaxUs, l.WakeMaxUs)
	case o.GCPauseOpen:
		return false, "GC pause p99 in the runtime's open-ended bucket"
	case o.GCPauseP99Ms > l.GCPauseP99Ms:
		return false, fmt.Sprintf("GC pause p99 bucket edge %.3f ms over the %.3f ms pin", o.GCPauseP99Ms, l.GCPauseP99Ms)
	}
	return true, ""
}

// Qualified measures the host and qualifies it against Allocation.
func Qualified() (bool, string, Observation) {
	o := Measure()
	ok, reason := o.Qualify(Allocation)
	return ok, reason, o
}

type hist struct {
	counts  []uint64
	buckets []float64
}

func gcHist() hist {
	s := []metrics.Sample{{Name: "/gc/pauses:seconds"}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindFloat64Histogram {
		return hist{}
	}
	h := s[0].Value.Float64Histogram()
	out := hist{counts: make([]uint64, len(h.Counts)), buckets: make([]float64, len(h.Buckets))}
	copy(out.counts, h.Counts)
	copy(out.buckets, h.Buckets)
	return out
}

// pauseP99 returns the pause count between the snapshots and an edge, in
// milliseconds, of the bucket holding their p99: the upper edge, or the
// lower edge when open reports a bucket with no upper edge.
func pauseP99(before, after hist) (n uint64, p99Ms float64, open bool) {
	if len(after.counts) == 0 || len(before.counts) != len(after.counts) {
		return 0, 0, false
	}
	diff := make([]uint64, len(after.counts))
	for i := range diff {
		diff[i] = after.counts[i] - before.counts[i]
		n += diff[i]
	}
	if n == 0 {
		return 0, 0, false
	}
	target := uint64(math.Ceil(0.99 * float64(n)))
	var cum uint64
	for i, c := range diff {
		cum += c
		if cum >= target {
			hi := after.buckets[i+1]
			if math.IsInf(hi, 1) {
				return n, after.buckets[i] * 1000, true
			}
			return n, hi * 1000, false
		}
	}
	return n, 0, false
}
