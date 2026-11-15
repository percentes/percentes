package report

import (
	"fmt"
	"time"

	"github.com/percentes/percentes/internal/admission"
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
