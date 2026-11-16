package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// A release run's writes stay within the window allowance its hooks
// carry, and a refused write fails the step's send-skew gate.
func TestReleaseWriteAllowance(t *testing.T) {
	var mu sync.Mutex
	var arrivals []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
	}))
	defer srv.Close()
	cfg := ladderRunCfg(srv.URL, 1)
	cfg.Release.BindingRPS = 20
	cfg.Release.Ladder.WarmupS = 0
	cfg.Release.Ladder.StepS = 2
	cfg.Release.Ladder.DrainS = 0.5
	res, err := Run(context.Background(), cfg, &Hooks{WriteAllowances: map[float64]int64{1: 10}})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	peak := peakIn(arrivals, time.Second)
	mu.Unlock()
	g := res.Ladder.Steps[0].Gates
	if peak > 10 || g.WritesDenied == 0 || g.SendSkewPass || g.Pass {
		t.Fatalf("peak arrivals in one second %d, want at most 10; gate %+v", peak, *g)
	}
}

// A pacer that wakes late leaves the overdue release requests unsent:
// the server never sees more in one second than the window admits, and
// the step fails its client-validity gate.
func TestLateReleaseWrites(t *testing.T) {
	var mu sync.Mutex
	var arrivals []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
	}))
	defer srv.Close()
	cfg := ladderRunCfg(srv.URL, 1)
	cfg.Release.BindingRPS = 150.909
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 10000, EvaluationWindowsS: []float64{1, 60}}}
	cfg.Release.Ladder.WarmupS = 0
	cfg.Release.Ladder.StepS = 2
	cfg.Release.Ladder.DrainS = 0.1
	res, err := Run(context.Background(), cfg, &Hooks{OnEpoch: func(time.Time) { time.Sleep(500 * time.Millisecond) }})
	if err != nil {
		t.Fatal(err)
	}
	var writes []int64
	for _, r := range res.Requests {
		if r.WroteRequestNs > 0 {
			writes = append(writes, r.WroteRequestNs)
		}
	}
	slices.Sort(writes)
	mu.Lock()
	peak := peakIn(arrivals, time.Second)
	mu.Unlock()
	if p := peakNs(writes, int64(time.Second)); p > 166 || peak > 166 {
		t.Fatalf("peak in one second: %d writes, %d arrivals; the window admits 166", p, peak)
	}
	if g := res.Ladder.Steps[0].Gates; g.Undispatched == 0 || g.Pass {
		t.Fatalf("late requests dispatched: gate %+v", *g)
	}
}

// peakIn is the most times in any half-open span of length d.
func peakIn(times []time.Time, d time.Duration) int {
	ns := make([]int64, len(times))
	for i, at := range times {
		ns[i] = at.UnixNano()
	}
	slices.Sort(ns)
	return peakNs(ns, d.Nanoseconds())
}

func peakNs(sorted []int64, d int64) int {
	peak, left := 0, 0
	for right, at := range sorted {
		for sorted[left] <= at-d {
			left++
		}
		peak = max(peak, right-left+1)
	}
	return peak
}
