package report

import (
	"fmt"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/loadgen"
)

const sec = int64(1e9)

// timed sets the end of each step-1 record of c after the warm-up, in
// order, to done[i] after an intended dispatch at 10 s; a completion's
// first token comes 1 ns after dispatch and odd records reuse their
// connection.
func timed(c CellRecords, done ...int64) CellRecords {
	for i, d := range done {
		r := &c.Records[i+1]
		r.IntendedNs, r.DispatchNs, r.DoneNs = 10*sec, 10*sec, 10*sec+d
		if r.Outcome == loadgen.OutcomeCompleted {
			r.FirstTokNs, r.ConnReused = 10*sec+1, i%2 == 1
		}
	}
	return c
}

// row is a curve line's indent and padded time label.
func row(label string) string { return fmt.Sprintf("    %-28s ", label) }

// A cell's curve at a step: a point at 0 and one per distinct completion
// time, in time and late on either kind of connection, each the
// completed count over scheduled with its Wilson interval. With every
// censored request at the hard timeout it equals the Aalen-Johansen
// estimate, and at the deadline it counts hosted-G6's completions. The
// text prints it at fixed times and at the last completion.
func TestCurveCell(t *testing.T) {
	cfg, v := stepsConfig(t)
	cell := timed(cellOf("weekday-00",
		outcome{1, LineInTime, 0, 6}, outcome{1, LineLate, 0, 1}, outcome{1, loadgen.ErrStatus429, 0, 2},
		outcome{1, LineCensored, 0, 1}, outcome{2, LineUnsent, 0, 4}, outcome{3, loadgen.ErrConnect, 0, 2}),
		sec/2, 3*sec/2, 3*sec/2, 12*sec, 30*sec, 25*sec, 45*sec, sec/20, 3*sec, 90*sec+5)
	cell.Records[11].DispatchNs, cell.Records[11].Outcome, cell.Records[11].InTime = 1, loadgen.OutcomeCompleted, true
	cell.Records[11].IntendedNs, cell.Records[11].FirstTokNs, cell.Records[11].DoneNs = 0, 1, 7*sec
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	s := st.Cells[0].Steps[0]
	type pt struct {
		t int64
		k int
	}
	want := []pt{{0, 0}, {sec / 2, 1}, {3 * sec / 2, 3}, {12 * sec, 4}, {25 * sec, 5}, {30 * sec, 6}, {45 * sec, 7}}
	if len(s.Curve) != len(want) {
		t.Fatalf("step 1 curve %+v", s.Curve)
	}
	for i, w := range want {
		p := s.Curve[i]
		r := newRate(w.k, 10)
		if p.TimeNs != w.t || p.Count != w.k || p.Rate != r.Value || p.Lo != r.Lo || p.Hi != r.Hi {
			t.Errorf("point %d: %+v, want %d of 10 at %d ns", i, p, w.k, w.t)
		}
	}

	var obs []collect.Obs
	for _, r := range cell.Records[1:11] {
		kind := collect.ObsError
		switch r.Outcome {
		case loadgen.OutcomeCompleted:
			kind = collect.ObsCompletion
		case loadgen.OutcomeCensored:
			kind = collect.ObsCensored
		}
		obs = append(obs, collect.Obs{TimeUs: (r.DoneNs - r.IntendedNs) / 1000, Kind: kind})
	}
	aj := collect.EstimateIncidence(obs, 90_000_000)
	for _, p := range s.Curve {
		if got := aj.IncidenceAt(p.TimeNs / 1000); !near(p.Rate, got) {
			t.Errorf("at %d ns: %g, Aalen-Johansen %g", p.TimeNs, p.Rate, got)
		}
	}
	if k := s.Curve[lastAt(len(s.Curve), func(i int) int64 { return s.Curve[i].TimeNs }, st.DeadlineS)].Count; k != s.G6.InTime.Count {
		t.Errorf("curve at the deadline %d, hosted-G6 %d", k, s.G6.InTime.Count)
	}

	s2 := st.Cells[0].Steps[1]
	if len(s2.Curve) != 2 || s2.Curve[1].Count != 1 || s2.Curve[1].Rate != 0.25 || s2.Curve[1].TimeNs != 7*sec {
		t.Errorf("step 2 curve %+v", s2.Curve)
	}
	s3 := st.Cells[0].Steps[2]
	if len(s3.Curve) != 1 || s3.Curve[0].Count != 0 || s3.Curve[0].Hi != newRate(0, 2).Hi {
		t.Errorf("step 3 curve %+v", s3.Curve)
	}

	pct := func(k, n int) string {
		r := newRate(k, n)
		return fmt.Sprintf("%d/%d %.2f%% [%.2f%%, %.2f%%]", k, n, 100*r.Value, 100*r.Lo, 100*r.Hi)
	}
	text := st.Text()
	for _, frag := range []string{
		"  completion incidence over scheduled, from intended dispatch, pointwise 95% Wilson band:\n" +
			row("by 1 s") + pct(1, 10) + "\n" +
			row("by 2 s") + pct(3, 10) + "\n" +
			row("by 5 s") + pct(3, 10) + "\n" +
			row("by 10 s") + pct(3, 10) + "\n" +
			row("by 20 s") + pct(4, 10) + "\n" +
			row("by 30 s") + pct(6, 10) + "\n" +
			row("by last completion 45.000 s") + pct(7, 10) + "\n" +
			"  time to failure signal, status_429:",
		row("by 30 s") + pct(1, 4) + "\n" + row("by last completion 7.000 s") + pct(1, 4) + "\n",
		row("by 30 s") + pct(0, 2) + "\n    no completions\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
}

// The pooled curve: a point at 0 and one per distinct completion time in
// any cell, each the cells' completed count over the pooled scheduled
// requests with the Wilson interval on the effective sample size of the
// cells' counts at that time.
func TestCurvePooled(t *testing.T) {
	cfg, v := stepsConfig(t)
	var cells []CellRecords
	for i := 0; i < 12; i++ {
		c := cellOf(fmt.Sprintf("cell-%02d", i), outcome{1, LineInTime, 0, 4}, outcome{1, loadgen.ErrConnect, 0, 1})
		d := []int64{sec, 2 * sec, 2 * sec, 20 * sec, sec / 10}
		if i == 0 {
			d = []int64{sec / 2, sec, 2 * sec, 2 * sec, sec / 10}
		}
		cells = append(cells, timed(c, d...))
	}
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	ps := st.Pooled[0]
	n := repeat(5, 12)
	type pt struct {
		t int64
		k []int
	}
	want := []pt{
		{0, make([]int, 12)},
		{sec / 2, append([]int{1}, make([]int, 11)...)},
		{sec, append([]int{2}, repeat(1, 11)...)},
		{2 * sec, append([]int{4}, repeat(3, 11)...)},
		{20 * sec, repeat(4, 12)},
	}
	if len(ps.Curve) != len(want) {
		t.Fatalf("pooled curve %+v", ps.Curve)
	}
	for i, w := range want {
		p, q := ps.Curve[i], newPooled(w.k, n)
		if p.TimeNs != w.t || p.Count != q.Count || p.Rate != q.Rate || p.CellsWithEvents != q.CellsWithEvents || p.DEFF != q.DEFF ||
			p.EffectiveN != q.EffectiveN || p.Lo != q.Lo || p.Hi != q.Hi || p.AssumesIndependence != q.AssumesIndependence {
			t.Errorf("point %d: %+v, want %+v at %d ns", i, p, q, w.t)
		}
	}
	if p := ps.Curve[1]; p.DEFF <= 1.01 || p.EffectiveN >= 59.5 || p.AssumesIndependence {
		t.Errorf("one event in one cell: %+v", p)
	}
	if p := ps.Curve[0]; !p.AssumesIndependence || !near(p.Hi, z95*z95/(60+z95*z95)) {
		t.Errorf("origin: %+v", p)
	}
	for _, c := range st.Cells {
		if last := c.Steps[0].Curve[len(c.Steps[0].Curve)-1]; last.Count != 4 {
			t.Errorf("cell %s ends at %+v", c.Cell, last)
		}
	}

	text := st.Text()
	for _, frag := range []string{
		"  completion incidence pooled, from intended dispatch, pointwise 95% Wilson band on the effective n:\n" +
			row("by 1 s") + "13/60 21.67%; 12 of 12 cells with events; DEFF 1.00",
		row("by 30 s") + "48/60 80.00%; 12 of 12 cells with events; DEFF 1.00; effective n 60.0; 95% Wilson [",
		row("by last completion 20.000 s") + "48/60 80.00%;",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
}
