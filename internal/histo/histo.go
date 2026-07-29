// Package histo wraps HdrHistogram with the one pinned configuration
// (SPEC.md §3) used for every window of every run. Values are recorded
// via plain RecordValue() ONLY: latencies are re-based to intended
// dispatch time upstream, so the library's
// coordinated-omission correction calls (the corrected-value recorder and
// its expected-interval variants) are forbidden — they would
// double-correct, and are invalid under Poisson arrivals. A lint test
// fails the suite if a correction API appears anywhere in the tree.
package histo

import (
	"fmt"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/percentes/percentes/internal/config"
)

// H is a histogram in the pinned configuration. The unit is microseconds.
type H struct {
	hist *hdrhistogram.Histogram
}

// New creates a histogram from the pinned config (validated upstream:
// unit "us", lowest 1, 3 significant figures, highest >= 600 s).
func New(c config.Histogram) *H {
	return &H{hist: hdrhistogram.New(c.LowestDiscernibleValue, c.HighestTrackableValue, c.SignificantFigures)}
}

// Record records one re-based latency in microseconds. This is the only
// recording call in the entire harness.
func (h *H) Record(us int64) error {
	if err := h.hist.RecordValue(us); err != nil {
		return fmt.Errorf("histo: record %dus: %w", us, err)
	}
	return nil
}

// Percentile returns the value at percentile p (0-100), in microseconds.
func (h *H) Percentile(p float64) int64 { return h.hist.ValueAtQuantile(p) }

// Max returns the largest recorded latency in microseconds (SPEC.md §7
// tail policy: descriptive-only at fault-window sample sizes).
func (h *H) Max() int64 { return h.hist.Max() }

// Count returns the total number of recorded latencies (SPEC.md §3).
func (h *H) Count() int64 { return h.hist.TotalCount() }

// Summary is the report-facing snapshot of one histogram.
type Summary struct {
	Count int64 `json:"count"`
	P50Us int64 `json:"p50_us"`
	P95Us int64 `json:"p95_us"`
	P99Us int64 `json:"p99_us"`
	// P999Us and MaxUs are descriptive-only at fault-window sample sizes
	// (§7 tail policy); the report labels them as such.
	P999Us int64 `json:"p999_us"`
	MaxUs  int64 `json:"max_us"`
}

// Summarize returns the report-facing snapshot consumed by collect to
// build the run's JSON report (SPEC.md §7): count, p50/p95/p99, and the
// descriptive-only p99.9 and max tail values.
func (h *H) Summarize() Summary {
	return Summary{
		Count:  h.hist.TotalCount(),
		P50Us:  h.hist.ValueAtQuantile(50),
		P95Us:  h.hist.ValueAtQuantile(95),
		P99Us:  h.hist.ValueAtQuantile(99),
		P999Us: h.hist.ValueAtQuantile(99.9),
		MaxUs:  h.hist.Max(),
	}
}
