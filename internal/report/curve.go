package report

import (
	"fmt"
	"slices"
	"sort"
)

// CurvePoint is a cell's completion-incidence curve at one time: Count
// of the step's scheduled requests completed within TimeNs of intended
// dispatch, as a rate with its pointwise 95 percent Wilson interval.
// The fraction is the Aalen-Johansen estimate wherever no request was
// censored before TimeNs (SPEC.md §3).
type CurvePoint struct {
	TimeNs int64   `json:"t_ns"`
	Count  int     `json:"count"`
	Rate   float64 `json:"incidence"`
	Lo     float64 `json:"wilson_lo"`
	Hi     float64 `json:"wilson_hi"`
}

// PooledCurvePoint is the curve pooled over a step's cells at one time,
// with the Wilson interval on the effective sample size of the cells'
// counts at that time (newPooled).
type PooledCurvePoint struct {
	TimeNs              int64   `json:"t_ns"`
	Count               int     `json:"count"`
	Rate                float64 `json:"incidence"`
	CellsWithEvents     int     `json:"cells_with_events"`
	DEFF                float64 `json:"deff"`
	EffectiveN          float64 `json:"effective_n"`
	Lo                  float64 `json:"wilson_lo"`
	Hi                  float64 `json:"wilson_hi"`
	AssumesIndependence bool    `json:"assumes_independent_requests,omitempty"`
}

// completionTimes are a step's completion times from intended dispatch,
// in time and late, ascending.
func completionTimes(t *stepTally) []int64 {
	xs := append(slices.Clone(t.done[0]), t.done[1]...)
	slices.Sort(xs)
	return xs
}

// curve is a cell step's curve over its scheduled requests: a point at 0
// and one at each distinct completion time.
func curve(times []int64, scheduled int) []CurvePoint {
	point := func(t int64, k int) CurvePoint {
		r := newRate(k, scheduled)
		return CurvePoint{TimeNs: t, Count: k, Rate: r.Value, Lo: r.Lo, Hi: r.Hi}
	}
	out := []CurvePoint{point(0, 0)}
	for i, t := range times {
		if i+1 < len(times) && times[i+1] == t {
			continue
		}
		out = append(out, point(t, i+1))
	}
	return out
}

// pooledCurve is the curve of cells with times[i] completion times
// (ascending) out of n[i] scheduled requests: a point at 0 and one at
// each distinct completion time of any cell.
func pooledCurve(times [][]int64, n []int) []PooledCurvePoint {
	k := make([]int, len(times))
	point := func(t int64) PooledCurvePoint {
		p := newPooled(k, n)
		return PooledCurvePoint{
			TimeNs: t, Count: p.Count, Rate: p.Rate, CellsWithEvents: p.CellsWithEvents,
			DEFF: p.DEFF, EffectiveN: p.EffectiveN, Lo: p.Lo, Hi: p.Hi, AssumesIndependence: p.AssumesIndependence,
		}
	}
	var all []int64
	for _, ts := range times {
		all = append(all, ts...)
	}
	slices.Sort(all)
	out := []PooledCurvePoint{point(0)}
	for _, t := range slices.Compact(all) {
		for i, ts := range times {
			for k[i] < len(ts) && ts[k[i]] <= t {
				k[i]++
			}
		}
		out = append(out, point(t))
	}
	return out
}

// curveTextS are the times, in seconds, at which the text prints a
// curve before the deadline; the JSON carries every point.
var curveTextS = []int{1, 2, 5, 10, 20}

// curveTimes are the text's print times, in seconds, up to and
// including the deadline.
func curveTimes(deadlineS int) []int {
	var out []int
	for _, s := range curveTextS {
		if s < deadlineS {
			out = append(out, s)
		}
	}
	return append(out, deadlineS)
}

// lastAt is the index of the last of n ascending times at or before s
// seconds; time(0) is 0.
func lastAt(n int, time func(int) int64, s int) int {
	limit := int64(s) * 1e9
	return sort.Search(n, func(i int) bool { return time(i) > limit }) - 1
}

// curveText renders a cell step's curve at the print times and at its
// last completion.
func curveText(w func(string, ...any), pts []CurvePoint, scheduled, deadlineS int) {
	pct := func(f float64) string { return fmt.Sprintf("%.2f%%", 100*f) }
	line := func(when string, p CurvePoint) {
		w("    %-28s %d/%d %s [%s, %s]", when, p.Count, scheduled, pct(p.Rate), pct(p.Lo), pct(p.Hi))
	}
	w("  completion incidence over scheduled, from intended dispatch, pointwise 95%% Wilson band:")
	for _, s := range curveTimes(deadlineS) {
		line(fmt.Sprintf("by %d s", s), pts[lastAt(len(pts), func(i int) int64 { return pts[i].TimeNs }, s)])
	}
	if len(pts) == 1 {
		w("    no completions")
		return
	}
	last := pts[len(pts)-1]
	line(fmt.Sprintf("by last completion %.3f s", float64(last.TimeNs)/1e9), last)
}

// pooledCurveText renders a pooled step's curve as curveText does.
func pooledCurveText(w func(string, ...any), pts []PooledCurvePoint, scheduled, cells, deadlineS int) {
	pct := func(f float64) string { return fmt.Sprintf("%.2f%%", 100*f) }
	line := func(when string, p PooledCurvePoint) {
		dep := fmt.Sprintf("DEFF %.2f", p.DEFF)
		if p.AssumesIndependence {
			dep += ", assumes independent requests"
		}
		w("    %-28s %d/%d %s; %d of %d cells with events; %s; effective n %.1f; 95%% Wilson [%s, %s]",
			when, p.Count, scheduled, pct(p.Rate), p.CellsWithEvents, cells, dep, p.EffectiveN, pct(p.Lo), pct(p.Hi))
	}
	w("  completion incidence pooled, from intended dispatch, pointwise 95%% Wilson band on the effective n:")
	for _, s := range curveTimes(deadlineS) {
		line(fmt.Sprintf("by %d s", s), pts[lastAt(len(pts), func(i int) int64 { return pts[i].TimeNs }, s)])
	}
	if len(pts) == 1 {
		w("    no completions")
		return
	}
	last := pts[len(pts)-1]
	line(fmt.Sprintf("by last completion %.3f s", float64(last.TimeNs)/1e9), last)
}
