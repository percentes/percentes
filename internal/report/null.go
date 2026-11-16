package report

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/loadgen"
)

// NullReading is a route's null reading at its two top steps, the 0.9
// and 1.0 steps of its binding rate (SPEC-HOSTED HA4, §8): over the Cells
// counted cells that attempted either step, their Scheduled requests,
// the status_429 and 5xx responses among them, and every outcome line
// with each status_other status. It holds when there is a cell and no
// status_429 or 5xx. NotAttempted names the counted cells whose ladders
// ended before both steps. Each cell's upper bound is the 95 percent
// Wilson score endpoint for no event in its requests, which assumes
// independent requests within the cell, and CellsHi the endpoint for no
// event in Cells cells, which assumes independent cells; both are set
// only where the null holds. ConditionalCaveat marks errors and
// censored requests above 5 percent of scheduled.
type NullReading struct {
	Steps             []int      `json:"steps"`
	Fractions         []float64  `json:"fractions"`
	Holds             bool       `json:"holds"`
	Cells             int        `json:"cells"`
	NotAttempted      []string   `json:"not_attempted,omitempty"`
	Scheduled         int        `json:"scheduled"`
	Status429         int        `json:"status_429"`
	Status5xx         int        `json:"status_5xx"`
	Lines             []NullLine `json:"lines"`
	ConditionalCaveat bool       `json:"conditional_caveat,omitempty"`
	CellBounds        []NullCell `json:"cell_bounds"`
	CellsHi           float64    `json:"cells_wilson_hi,omitempty"`
}

// NullLine is one outcome line's count over a null reading's requests;
// Status is a status_other line's HTTP status.
type NullLine struct {
	Name   string `json:"name"`
	Status int    `json:"status,omitempty"`
	Count  int    `json:"count"`
}

// NullCell is one cell of a null reading: its label, scheduled start,
// scheduled requests at the two steps and, where the null holds, its
// upper bound.
type NullCell struct {
	Cell      string    `json:"cell"`
	StartUTC  time.Time `json:"start_utc"`
	Scheduled int       `json:"scheduled"`
	Hi        float64   `json:"wilson_hi,omitempty"`
}

// null is the null reading over the cell tables at idx, cells being the
// tables' records in the same order; nil for a ladder of fewer than two
// steps.
func (t *tables) null(idx []int, cells []CellRecords) *NullReading {
	n := len(t.Limits)
	if n < 2 {
		return nil
	}
	nr := &NullReading{Steps: []int{n - 1, n}, Fractions: slices.Clone(t.fractions[n-2:]), CellBounds: []NullCell{}}
	lines := map[string]int{}
	statuses := map[int]int{}
	for _, i := range idx {
		nc := NullCell{Cell: t.Cells[i].Cell, StartUTC: cells[i].Start.UTC()}
		for k := n - 2; k < n; k++ {
			s := &t.Cells[i].Steps[k]
			if s.State != StepAttempted || s.Scheduled == 0 {
				continue
			}
			nc.Scheduled += s.Scheduled
			tl := &t.tallies[i][k]
			for name, c := range tl.lines {
				lines[name] += c
			}
			for code, c := range tl.statuses {
				statuses[code] += c
			}
		}
		if nc.Scheduled == 0 {
			nr.NotAttempted = append(nr.NotAttempted, nc.Cell)
			continue
		}
		nr.Cells++
		nr.Scheduled += nc.Scheduled
		nr.CellBounds = append(nr.CellBounds, nc)
	}
	nr.Status429 = lines[loadgen.ErrStatus429]
	for code, c := range statuses {
		if code >= 500 && code <= 599 {
			nr.Status5xx += c
		}
	}
	failed := lines[LineCensored]
	for _, name := range stepLines {
		nr.Lines = append(nr.Lines, NullLine{Name: name, Count: lines[name]})
		if slices.Contains(errClasses, name) {
			failed += lines[name]
		}
		if name == loadgen.ErrStatusOther {
			var codes []int
			for code := range statuses {
				codes = append(codes, code)
			}
			slices.Sort(codes)
			for _, code := range codes {
				nr.Lines = append(nr.Lines, NullLine{Name: name, Status: code, Count: statuses[code]})
			}
		}
	}
	nr.ConditionalCaveat = float64(failed) > conditionalLimit*float64(nr.Scheduled)
	nr.Holds = nr.Cells > 0 && nr.Status429 == 0 && nr.Status5xx == 0
	if nr.Holds {
		for i := range nr.CellBounds {
			_, nr.CellBounds[i].Hi = wilson(0, float64(nr.CellBounds[i].Scheduled))
		}
		_, nr.CellsHi = wilson(0, float64(nr.Cells))
	}
	return nr
}

// bindingText names a binding limit with its window.
func bindingText(b admission.Rate) string {
	switch {
	case b.WindowS > 0:
		return fmt.Sprintf("%s over %g s", b.Dimension, b.WindowS)
	case b.ResetDay != "":
		return fmt.Sprintf("%s over the reset day %s", b.Dimension, b.ResetDay)
	}
	return b.Dimension
}

// nullText writes a null reading of a route whose binding limit reads as
// binding.
func nullText(w func(string, ...any), nr *NullReading, bindingRPS float64, binding string) {
	if nr == nil {
		return
	}
	fr := make([]string, len(nr.Fractions))
	for i, f := range nr.Fractions {
		fr[i] = strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.Contains(fr[i], ".") {
			fr[i] += ".0"
		}
	}
	steps := strings.Join(fr, " and ")
	w("-- Null reading at the %s steps of the binding rate (steps %d and %d) --", steps, nr.Steps[0], nr.Steps[1])
	switch {
	case nr.Cells == 0:
		w("not stated: no counted cell attempted the %s steps", steps)
	case nr.Holds:
		var dated []string
		for _, c := range nr.CellBounds {
			dated = append(dated, fmt.Sprintf("%s (%s)", c.Cell, c.StartUTC.Format(time.DateOnly)))
		}
		w("holds: at cells %s, at the %s steps of the route's binding rate of %g rps, binding limit %s, with paced arrivals, none of the %d scheduled requests in %d counted cells returned a 429 or a 5xx.",
			strings.Join(dated, ", "), steps, bindingRPS, binding, nr.Scheduled, nr.Cells)
	default:
		w("does not hold: %d status_429 and %d 5xx responses among the %d scheduled requests of the %s steps in %d counted cells.",
			nr.Status429, nr.Status5xx, nr.Scheduled, steps, nr.Cells)
	}
	var parts []string
	for _, l := range nr.Lines {
		name := l.Name
		if l.Status != 0 {
			name = fmt.Sprintf("%s %d", l.Name, l.Status)
		}
		parts = append(parts, fmt.Sprintf("%s %d", name, l.Count))
	}
	w("the same steps' outcomes: %s", strings.Join(parts, "; "))
	if nr.ConditionalCaveat {
		w("errors and censored above 5%% of scheduled: read with the incidence curve")
	}
	if len(nr.NotAttempted) > 0 {
		w("counted cells whose ladders ended before these steps: %s", strings.Join(nr.NotAttempted, ", "))
	}
	if nr.Holds {
		for _, c := range nr.CellBounds {
			w("  cell %s: 0 of %d, 95%% Wilson upper bound %.4f%% (assumes independent requests within the cell)", c.Cell, c.Scheduled, 100*c.Hi)
		}
		w("  over cells: 0 of %d cells, 95%% Wilson upper bound %.3f%% (assumes independent cells)", nr.Cells, 100*nr.CellsHi)
		w("The frame establishes neither assumption. The null describes this account at these cells.")
	}
	w("")
}
