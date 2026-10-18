package report

import (
	"fmt"
	"slices"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/evidence"
)

// incidencePcts are the quantiles, in percent, of the
// completion-incidence curve the tables report or refuse.
var incidencePcts = []int{50, 90, 95, 99}

// IncidenceQuantile is the time at which a step's completion-incidence
// curve first reaches Q, where Crossed. Otherwise the quantile is
// refused: Unattainable where the curve's ceiling is below Q, and
// greater than the horizon where it is not (SPEC.md §3).
type IncidenceQuantile struct {
	Q            float64 `json:"q"`
	Crossed      bool    `json:"crossed"`
	TimeNs       int64   `json:"t_ns,omitempty"`
	Unattainable bool    `json:"unattainable,omitempty"`
}

// IncidenceQuantiles are a step's completion-incidence quantiles from the
// Aalen-Johansen estimate over its scheduled requests, inside the
// horizon, the hard timeout. Ceiling is the final completion incidence
// plus the event-free survival remaining where the curve ends.
type IncidenceQuantiles struct {
	HorizonNs      int64               `json:"horizon_ns"`
	FinalIncidence float64             `json:"final_incidence"`
	Ceiling        float64             `json:"ceiling"`
	Quantiles      []IncidenceQuantile `json:"quantiles"`
}

// incidence estimates the curve over the tallies' requests.
func incidence(ts []*stepTally, horizonS int) *IncidenceQuantiles {
	var obs []collect.Obs
	for _, t := range ts {
		obs = append(obs, t.obs...)
	}
	horizonUs := int64(horizonS) * 1_000_000
	c := collect.EstimateIncidence(obs, horizonUs)
	out := &IncidenceQuantiles{HorizonNs: horizonUs * 1000, FinalIncidence: c.FinalIncidence(), Ceiling: c.Ceiling()}
	for _, pct := range incidencePcts {
		q := float64(pct) / 100
		iq := IncidenceQuantile{Q: q}
		if us, ok := c.Quantile(q); ok {
			iq.Crossed, iq.TimeNs = true, us*1000
		} else {
			iq.Unattainable = out.Ceiling < q
		}
		out.Quantiles = append(out.Quantiles, iq)
	}
	return out
}

// Tail is a percentile by nearest rank with its two-sided 95 percent
// distribution-free order-statistic interval (SPEC.md §7).
type Tail struct {
	Ns   int64 `json:"ns"`
	LoNs int64 `json:"lo_ns"`
	HiNs int64 `json:"hi_ns"`
}

// Distribution is a duration from intended dispatch over a step's
// completions: the median and 90th percentile, and the 95th and 99th
// percentiles with their intervals, each nil where an interval rank
// falls outside the completions (SPEC.md §7).
type Distribution struct {
	Quantiles
	P95 *Tail `json:"p95,omitempty"`
	P99 *Tail `json:"p99,omitempty"`
}

func distribution(xs []int64) Distribution {
	s := slices.Clone(xs)
	slices.Sort(s)
	tail := func(pct int) *Tail {
		n := len(s)
		lo, hi, ok := collect.OrderStatRanks(n, float64(pct)/100)
		if !ok {
			return nil
		}
		return &Tail{Ns: s[(n*pct+99)/100-1], LoNs: s[lo-1], HiNs: s[hi-1]}
	}
	return Distribution{Quantiles: quantiles(s), P95: tail(95), P99: tail(99)}
}

// Counts is a per-request count over Of requests of a step, of which N
// carry it: the minimum, the median and 90th percentile by nearest rank,
// and the maximum, zero where N is 0.
type Counts struct {
	Of  int `json:"of"`
	N   int `json:"n"`
	Min int `json:"min"`
	P50 int `json:"p50"`
	P90 int `json:"p90"`
	Max int `json:"max"`
}

func counts(xs []int, of int) Counts {
	c := Counts{Of: of, N: len(xs)}
	if c.N == 0 {
		return c
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	c.Min, c.P50, c.P90, c.Max = s[0], s[(c.N*50+99)/100-1], s[(c.N*90+99)/100-1], s[c.N-1]
	return c
}

// GapBin is one bin of an inter-chunk gap histogram: Count gaps from
// LoUs microseconds up to HiUs, which is 0 on the last bin.
type GapBin struct {
	LoUs  int64 `json:"lo_us"`
	HiUs  int64 `json:"hi_us,omitempty"`
	Count int64 `json:"count"`
}

// Gaps is a step's histogram of the N gaps between successive content
// events over its completions, every bin of evidence.GapEdgesUs listed.
type Gaps struct {
	N    int64    `json:"n"`
	Bins []GapBin `json:"bins"`
}

func gapsOf(ts []*stepTally) Gaps {
	var g Gaps
	for i, lo := range evidence.GapEdgesUs {
		b := GapBin{LoUs: lo}
		if i+1 < len(evidence.GapEdgesUs) {
			b.HiUs = evidence.GapEdgesUs[i+1]
		}
		for _, t := range ts {
			b.Count += t.gaps[i]
		}
		g.N += b.Count
		g.Bins = append(g.Bins, b)
	}
	return g
}

func msText(ns int64) string { return fmt.Sprintf("%.1f ms", float64(ns)/1e6) }

// incidenceQuantilesText renders a step's completion-incidence
// quantiles.
func incidenceQuantilesText(w func(string, ...any), iq *IncidenceQuantiles) {
	pct := func(f float64) string { return fmt.Sprintf("%.2f%%", 100*f) }
	horizon := fmt.Sprintf("%d s", iq.HorizonNs/1e9)
	w("  completion-incidence quantiles, Aalen-Johansen over scheduled, inside the %s hard timeout:", horizon)
	for i, q := range iq.Quantiles {
		var v string
		switch {
		case q.Crossed:
			v = fmt.Sprintf("%.3f s", float64(q.TimeNs)/1e9)
		case q.Unattainable:
			v = fmt.Sprintf("unattainable (final completion incidence %s; ceiling %s)", pct(iq.FinalIncidence), pct(iq.Ceiling))
		default:
			v = fmt.Sprintf("> %s (ceiling %s)", horizon, pct(iq.Ceiling))
		}
		w("    %-16s %s", percentileName(incidencePcts[i]), v)
	}
}

func percentileName(pct int) string {
	if pct == 50 {
		return "median"
	}
	return fmt.Sprintf("%dth percentile", pct)
}

// distributionText renders a duration distribution; a tail whose
// interval ranks fall outside the completions is not reported.
func distributionText(d Distribution) string {
	if d.N == 0 {
		return "n 0"
	}
	text := fmt.Sprintf("n %d, median %s, 90th percentile %s", d.N, msText(d.P50Ns), msText(d.P90Ns))
	var unreported []string
	for _, t := range []struct {
		name string
		tail *Tail
	}{{"95th", d.P95}, {"99th", d.P99}} {
		if t.tail == nil {
			unreported = append(unreported, t.name)
			continue
		}
		text += fmt.Sprintf("; %s percentile %s, 95%% order-statistic interval [%s, %s]", t.name, msText(t.tail.Ns), msText(t.tail.LoNs), msText(t.tail.HiNs))
	}
	switch len(unreported) {
	case 1:
		text += fmt.Sprintf("; %s percentile not reported: its interval ranks fall outside the sample", unreported[0])
	case 2:
		text += "; 95th and 99th percentiles not reported: their interval ranks fall outside the sample"
	}
	return text
}

// countsText renders a count over requests; where every request carries
// it the share is left out, and where none does it publishes as not
// verifiable.
func countsText(c Counts) string {
	switch {
	case c.N == 0:
		return fmt.Sprintf("on 0 of %d: not verifiable", c.Of)
	case c.N == c.Of:
		return fmt.Sprintf("min %d, median %d, 90th percentile %d, max %d", c.Min, c.P50, c.P90, c.Max)
	}
	return fmt.Sprintf("on %d of %d: min %d, median %d, 90th percentile %d, max %d", c.N, c.Of, c.Min, c.P50, c.P90, c.Max)
}

// usText renders a bin edge.
func usText(us int64) string {
	switch {
	case us >= 1_000_000:
		return fmt.Sprintf("%d s", us/1_000_000)
	case us >= 1_000:
		return fmt.Sprintf("%d ms", us/1_000)
	}
	return fmt.Sprintf("%d us", us)
}

// gapsText renders a gap histogram's bins that hold a gap.
func gapsText(w func(string, ...any), g Gaps) {
	w("  inter-chunk gaps between successive content events, over completions: n %d", g.N)
	for _, b := range g.Bins {
		if b.Count == 0 {
			continue
		}
		bin := fmt.Sprintf("%s and over", usText(b.LoUs))
		if b.HiUs != 0 {
			bin = fmt.Sprintf("%s to under %s", usText(b.LoUs), usText(b.HiUs))
		}
		w("    %-22s %d (%.2f%%)", bin, b.Count, 100*float64(b.Count)/float64(g.N))
	}
}
