package report

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// IndexForbidden are the strings the index page may not carry, matched
// without regard to case (SPEC-HOSTED HA3 point 12).
var IndexForbidden = []string{"rank", "winner", "best", "worst", "interval", "sort key", "p99"}

// IndexRoute is one route as the index reads it: its release
// configuration, the ceiling rule's verdict at its ceiling, its cells'
// records and the link to its page.
type IndexRoute struct {
	Config  *config.Config
	Verdict *admission.Verdict
	Cells   []CellRecords
	Page    string
}

// IndexCount is one count column of an index row.
type IndexCount struct {
	Line  string `json:"line"`
	Count int    `json:"count"`
}

// IndexRow is one route at one offered-load step. Binding names the
// limit that excluded a step the ceiling rule did not admit.
// HighestAttempted is the route's highest step any cell attempted, 0
// where none did. Cells counts the cells that attempted the step with
// requests scheduled. Counts has one entry per stepLines line, summed
// over those cells; the entries sum to Scheduled. Cells, Scheduled and
// Counts are the route's pooled figures: zero and nil for an incomplete
// frame (HA3 point 9). Counts is also nil for an excluded step and a
// step no cell attempted.
type IndexRow struct {
	Provider         string       `json:"provider"`
	Route            string       `json:"route"`
	Model            string       `json:"model"`
	Step             int          `json:"step"`
	RPS              float64      `json:"rps"`
	Admitted         bool         `json:"admitted"`
	Binding          string       `json:"binding_limit,omitempty"`
	HighestAttempted int          `json:"highest_attempted_step"`
	CeilingTokens    int          `json:"ceiling_tokens"`
	FrameCells       int          `json:"frame_cells"`
	RecordedCells    int          `json:"recorded_cells"`
	Cells            int          `json:"cells_attempted"`
	Scheduled        int          `json:"scheduled"`
	Counts           []IndexCount `json:"counts,omitempty"`
	Page             string       `json:"page"`
}

// Index is the release's index table: one row per route and step, in
// alphabetical order of provider, then route, then step ascending. No
// row order depends on a measured value.
type Index struct {
	Rows []IndexRow `json:"rows"`
}

// NewIndex builds the index from each route's per-step tables
// (NewStepTables); its counts are the route page's pooled counts. A
// route whose printed fields carry an IndexForbidden string, a table
// delimiter or a line break is refused.
func NewIndex(routes []IndexRoute) (*Index, error) {
	type built struct {
		r  *IndexRoute
		st *StepTables
	}
	if len(routes) == 0 {
		return nil, errors.New("report: index: no routes")
	}
	var bs []built
	seen := map[string]bool{}
	for i := range routes {
		r := &routes[i]
		if r.Config == nil || r.Config.Target.Naming == nil {
			return nil, fmt.Errorf("report: index: route %d has no release configuration with a naming record", i)
		}
		if seen[r.Config.Target.Name] {
			return nil, fmt.Errorf("report: index: route %q given twice", r.Config.Target.Name)
		}
		seen[r.Config.Target.Name] = true
		st, err := NewStepTables(r.Config, r.Verdict, r.Cells)
		if err != nil {
			return nil, fmt.Errorf("report: index: route %q: %w", r.Config.Target.Name, err)
		}
		bs = append(bs, built{r, st})
	}
	slices.SortFunc(bs, func(a, b built) int {
		pa, pb := a.r.Config.Target.Naming.Provider, b.r.Config.Target.Naming.Provider
		if c := strings.Compare(strings.ToLower(pa), strings.ToLower(pb)); c != 0 {
			return c
		}
		if c := strings.Compare(pa, pb); c != 0 {
			return c
		}
		return strings.Compare(a.st.Route, b.st.Route)
	})
	ix := &Index{}
	for _, b := range bs {
		t := b.r.Config.Target
		for _, f := range []struct{ name, v string }{
			{"provider", t.Naming.Provider}, {"route", t.Name}, {"model", t.ModelName}, {"page", b.r.Page},
		} {
			if err := indexField(f.v); err != nil {
				return nil, fmt.Errorf("report: index: route %q: %s: %w", b.st.Route, f.name, err)
			}
		}
		ix.Rows = append(ix.Rows, indexRows(t.Naming.Provider, t.ModelName, b.r.Page, b.st)...)
	}
	if err := checkIndexText(ix.Markdown()); err != nil {
		return nil, fmt.Errorf("report: index: %w", err)
	}
	return ix, nil
}

func indexField(v string) error {
	switch {
	case strings.TrimSpace(v) == "":
		return errors.New("empty")
	case strings.ContainsAny(v, "|\r\n"):
		return errors.New("carries a table delimiter or a line break")
	}
	return checkIndexText(v)
}

// checkIndexText refuses s if it carries an IndexForbidden string.
func checkIndexText(s string) error {
	low := strings.ToLower(s)
	for _, f := range IndexForbidden {
		if strings.Contains(low, f) {
			return fmt.Errorf("carries %q", f)
		}
	}
	return nil
}

func indexRows(provider, model, page string, st *StepTables) []IndexRow {
	highest := 0
	for _, c := range st.Cells {
		for _, s := range c.Steps {
			if s.State == StepAttempted && s.Step > highest {
				highest = s.Step
			}
		}
	}
	var rows []IndexRow
	for k, lim := range st.Limits {
		row := IndexRow{
			Provider: provider, Route: st.Route, Model: model, Step: k + 1, RPS: lim.RPS, Admitted: lim.Admitted,
			HighestAttempted: highest, CeilingTokens: st.CeilingTokens, FrameCells: st.FrameCells,
			RecordedCells: len(st.Cells), Page: page,
		}
		if !lim.Admitted {
			row.Binding = lim.Binding.Dimension
		}
		if st.Pooled != nil {
			ps := st.Pooled[k]
			row.Cells, row.Scheduled = ps.Cells, ps.Scheduled
			for _, l := range ps.Lines {
				if l.InBand == "" && (l.Status == 0 || l.Name == loadgen.ErrStatus429) {
					row.Counts = append(row.Counts, IndexCount{Line: l.Name, Count: l.Count})
				}
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// Markdown renders the index as one table. Counts print as count over
// the step's scheduled requests.
func (ix *Index) Markdown() string {
	var b strings.Builder
	w := func(cells []string) { fmt.Fprintf(&b, "| %s |\n", strings.Join(cells, " | ")) }
	b.WriteString("One row per route and offered-load step, alphabetical by provider and route, then by step. ")
	b.WriteString("Each count is over the step's scheduled requests in the cells that attempted the step. ")
	b.WriteString("Rates and their uncertainty are on each route's page.\n\n")
	head := []string{"Provider", "Route", "Model", "Step", "Offered load (rps)", "Admission", "Highest attempted step",
		"Ceiling (tokens)", "Cells attempted", "Scheduled"}
	head = append(head, stepLines...)
	head = append(head, "Route page")
	w(head)
	sep := make([]string, len(head))
	for i := range sep {
		sep[i] = "---"
	}
	w(sep)
	for _, r := range ix.Rows {
		adm := "admitted"
		if !r.Admitted {
			adm = "excluded, binding limit " + r.Binding
		}
		highest := "none"
		if r.HighestAttempted > 0 {
			highest = fmt.Sprint(r.HighestAttempted)
		}
		cells := fmt.Sprintf("%d of %d", r.Cells, r.FrameCells)
		scheduled := fmt.Sprint(r.Scheduled)
		if r.RecordedCells < r.FrameCells {
			cells = fmt.Sprintf("incomplete frame, %d of %d cells recorded", r.RecordedCells, r.FrameCells)
			scheduled = ""
		}
		row := []string{r.Provider, r.Route, r.Model, fmt.Sprint(r.Step), fmt.Sprintf("%g", r.RPS), adm, highest,
			fmt.Sprint(r.CeilingTokens), cells, scheduled}
		for i := range stepLines {
			c := ""
			if r.Counts != nil {
				c = fmt.Sprintf("%d/%d", r.Counts[i].Count, r.Scheduled)
			}
			row = append(row, c)
		}
		row = append(row, fmt.Sprintf("[%s](%s)", r.Route, r.Page))
		w(row)
	}
	return b.String()
}
