package report

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// nullFrame is twelve counted cells of the release reference, every
// step admitted, each with 524 requests at step 4 and 582 at step 5
// completed in time, the top two steps of azure-S at 512; edit changes
// cell i's outcomes at steps 4 and 5.
func nullFrame(t *testing.T, edit func(i int) []outcome) (*config.Config, *admission.Verdict, []CellRecords) {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, v := ruled(t, cfg, nil)
	if len(v.Admitted()) != 5 {
		t.Fatalf("admitted steps %v", v.Admitted())
	}
	var cells []CellRecords
	for i := 0; i < 12; i++ {
		outs := []outcome{{1, LineInTime, 0, 10}, {2, LineInTime, 0, 10}, {3, LineInTime, 0, 10}}
		if top := edit(i); top != nil {
			outs = append(outs, top...)
		} else {
			outs = append(outs, outcome{4, LineInTime, 0, 524}, outcome{5, LineInTime, 0, 582})
		}
		c := cellOf(fmt.Sprintf("cell-%02d", i), outs...)
		c.Start = time.Date(2026, 10, 31, 4*(i%6), 40, 0, 0, time.UTC).AddDate(0, 0, i/6)
		cells = append(cells, c)
	}
	return cfg, v, cells
}

// The null holds when the 0.9 and 1.0 steps record no status_429 and no
// 5xx: the page names the dated cells, the binding rate and limit, n and
// g, every outcome of the same steps, each cell's 95 percent Wilson upper
// bound assuming independent requests within the cell, and the bound
// over cells assuming independent cells: 0 of 1,106 gives 0.3461 percent
// and 0 of 12 cells 24.249 percent. A pooled rate with no event carries
// the same two kinds of bound.
func TestNullHolds(t *testing.T) {
	cfg, v, cells := nullFrame(t, func(int) []outcome { return nil })
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	nr := st.Null
	switch {
	case nr == nil:
		t.Fatal("no null reading")
	case !nr.Holds || nr.Cells != 12 || nr.Scheduled != 12*1106 || nr.Status429 != 0 || nr.Status5xx != 0 || !slices.Equal(nr.Steps, []int{4, 5}):
		t.Errorf("null %+v", nr)
	case len(nr.CellBounds) != 12 || nr.CellBounds[0].Scheduled != 1106 || fmt.Sprintf("%.4f", 100*nr.CellBounds[0].Hi) != "0.3461" ||
		fmt.Sprintf("%.3f", 100*nr.CellsHi) != "24.249":
		t.Errorf("bounds: cell %+v, over cells %g", nr.CellBounds, nr.CellsHi)
	}
	text := st.Text()
	for _, frag := range []string{
		"-- Null reading at the 0.9 and 1.0 steps of the binding rate (steps 4 and 5) --\n",
		"holds: at cells cell-00 (2026-10-31), cell-01 (2026-10-31), ",
		fmt.Sprintf(", at the 0.9 and 1.0 steps of the route's binding rate of 1 rps, binding limit %s, with paced arrivals, none of the 13272 scheduled requests in 12 counted cells returned a 429 or a 5xx.\n",
			bindingText(v.Solution.Binding)),
		"the same steps' outcomes: completed_in_time 13272; completed_late 0; status_429 0; status_other 0; connect 0; reset 0; empty_stream 0; malformed_stream 0; censored 0; unsent 0\n",
		"  cell cell-11: 0 of 1106, 95% Wilson upper bound 0.3461% (assumes independent requests within the cell)\n",
		"  over cells: 0 of 12 cells, 95% Wilson upper bound 24.249% (assumes independent cells)\n",
		"The frame establishes neither assumption. The null describes this account at these cells.\n",
		"; no event: cell upper bounds 0.6557%, 0.6557%, ",
		"each assuming independent requests within its cell; over 12 cells upper bound 24.25%, assuming independent cells",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	for _, l := range st.Pooled[4].Lines {
		if p := l.Pooled; l.Name == loadgen.ErrStatus429 && (p.Count != 0 || len(p.CellHi) != 12 || p.CellsHi != nr.CellsHi) {
			t.Errorf("pooled step 5 status_429: %+v", p)
		}
	}
}

// A status_429 of any label or a 5xx at the top two steps breaks the
// null, which then prints the counts and no bound; every other status
// prints beside it. A counted cell whose ladder ended before both steps
// leaves g and n and is named; with no such cell the null is not stated,
// and an incomplete frame has none.
func TestNullDoesNotHold(t *testing.T) {
	cfg, v, cells := nullFrame(t, func(i int) []outcome {
		switch i {
		case 3:
			return []outcome{{4, loadgen.ErrStatus429, 0, 1}, {4, LineInTime, 0, 523}, {5, LineInTime, 0, 582}}
		case 5:
			return []outcome{{4, LineInTime, 0, 524}, {5, loadgen.ErrStatusOther, 503, 1}, {5, loadgen.ErrStatusOther, 400, 2}, {5, LineInTime, 0, 579}}
		case 11:
			return []outcome{{4, LineUnsent, 0, 524}, {5, LineUnsent, 0, 582}}
		}
		return nil
	})
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	nr := st.Null
	if nr.Holds || nr.Cells != 11 || nr.Scheduled != 11*1106 || nr.Status429 != 1 || nr.Status5xx != 1 || nr.CellsHi != 0 ||
		!slices.Equal(nr.NotAttempted, []string{"cell-11"}) || nr.CellBounds[0].Hi != 0 {
		t.Errorf("null %+v", nr)
	}
	text := st.Text()
	for _, frag := range []string{
		"does not hold: 1 status_429 and 1 5xx responses among the 12166 scheduled requests of the 0.9 and 1.0 steps in 11 counted cells.\n",
		"the same steps' outcomes: completed_in_time 12162; completed_late 0; status_429 1; status_other 3; status_other 400 2; status_other 503 1; connect 0;",
		"counted cells whose ladders ended before these steps: cell-11\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	if strings.Contains(text, "95% Wilson upper bound") {
		t.Errorf("a broken null prints bounds:\n%s", text)
	}

	cfg, v, cells = nullFrame(t, func(i int) []outcome { return []outcome{{4, LineUnsent, 0, 524}, {5, LineUnsent, 0, 582}} })
	if st, err = NewStepTables(cfg, v, cells); err != nil {
		t.Fatal(err)
	}
	if nr := st.Null; nr.Holds || nr.Cells != 0 || !strings.Contains(st.Text(), "not stated: no counted cell attempted the 0.9 and 1.0 steps\n") {
		t.Errorf("null %+v", nr)
	}
	if st, err := NewStepTables(cfg, v, cells[:11]); err != nil || st.Null != nil || strings.Contains(st.Text(), "Null reading") {
		t.Errorf("an incomplete frame: %v, null %+v", err, st.Null)
	}
}
