package loadgen

import (
	"math"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

func pinnedValidity() *config.Config {
	return &config.Config{ClientValidity: config.ClientValidity{
		SendSkewP99Ms: config.PinnedSendSkewP99Ms, SendSkewMaxMs: config.PinnedSendSkewMaxMs,
		MaxCPUPct: config.PinnedClientCPUPct, CPUWindowS: config.PinnedCPUWindowS, GoGCPauseP99Ms: config.PinnedGoGCPauseP99Ms,
	}}
}

// Send skew is judged in nanoseconds: a skew 999 ns over a pin fails,
// and the microsecond report fields round down.
func TestSendSkewComparedInNanoseconds(t *testing.T) {
	cfg := pinnedValidity()
	mk := func(skewsNs ...int64) []Request {
		reqs := make([]Request, len(skewsNs))
		for i, s := range skewsNs {
			reqs[i] = Request{Index: int64(i), IntendedNs: int64(i+1) * 1e9, DispatchNs: int64(i+1)*1e9 + s}
		}
		return reqs
	}
	hundred := func(v int64) []int64 {
		out := make([]int64, 100)
		for i := range out {
			out[i] = v
		}
		return out
	}
	rep := evaluateGates(cfg, mk(hundred(5_000_999)...), nil, 0, 0, 0, 200e9)
	if rep.SendSkewPass || rep.SendSkewP99Us != 5000 {
		t.Fatalf("p99 999 ns over the pin: pass=%v p99=%dus", rep.SendSkewPass, rep.SendSkewP99Us)
	}
	rep = evaluateGates(cfg, mk(hundred(5_000_000)...), nil, 0, 0, 0, 200e9)
	if !rep.SendSkewPass {
		t.Fatalf("p99 at the pin must pass: %+v", rep)
	}
	one := hundred(1)
	one[7] = 50_000_999
	rep = evaluateGates(cfg, mk(one...), nil, 0, 0, 0, 200e9)
	if rep.SendSkewPass || rep.SendSkewMaxUs != 50000 {
		t.Fatalf("max 999 ns over the pin: pass=%v max=%dus", rep.SendSkewPass, rep.SendSkewMaxUs)
	}
}

// The garbage collection (GC) pause p99 is known only to its runtime
// bucket: both edges are reported and the gate reads the upper one.
func TestGCPauseP99BucketEdges(t *testing.T) {
	edges := []float64{math.Inf(-1), 0.0001, 0.0005, 0.001, 0.002, math.Inf(1)}
	hist := func(counts ...uint64) *gcHist { return &gcHist{counts: counts, buckets: edges} }
	m := &gcMonitor{start: hist(0, 0, 0, 0, 0), end: hist(0, 0, 100, 0, 0)}
	lo, hi := m.stopAndP99Ms()
	if lo != 0.5 || hi != 1 {
		t.Fatalf("bucket [0.5, 1) ms: got [%v, %v)", lo, hi)
	}
	rep := evaluateGates(pinnedValidity(), nil, nil, lo, hi, 0, 10e9)
	if rep.GCPass || rep.GCPauseP99LoMs != 0.5 || rep.GCPauseP99Ms != 1 {
		t.Fatalf("an upper edge at the pin must fail: %+v", rep)
	}
	m = &gcMonitor{start: hist(0, 0, 0, 0, 0), end: hist(0, 100, 0, 0, 0)}
	if lo, hi = m.stopAndP99Ms(); lo != 0.1 || hi != 0.5 {
		t.Fatalf("bucket [0.1, 0.5) ms: got [%v, %v)", lo, hi)
	}
	if rep = evaluateGates(pinnedValidity(), nil, nil, lo, hi, 0, 10e9); !rep.GCPass {
		t.Fatalf("an upper edge under the pin must pass: %+v", rep)
	}
	m = &gcMonitor{start: hist(0, 0, 0, 0, 0), end: hist(0, 0, 0, 0, 3)}
	if lo, hi = m.stopAndP99Ms(); lo != 2 || !math.IsInf(hi, 1) {
		t.Fatalf("the last bucket: got [%v, %v)", lo, hi)
	}
	m = &gcMonitor{start: hist(5, 0, 0, 0, 0), end: hist(5, 0, 0, 0, 0)}
	if lo, hi = m.stopAndP99Ms(); lo != 0 || hi != 0 {
		t.Fatalf("no pause: got [%v, %v)", lo, hi)
	}
}
