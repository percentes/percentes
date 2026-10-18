package report

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

// Outcome lines of a per-step table beside the error classes. Every
// scheduled request of a step falls on exactly one line.
const (
	LineInTime   = "completed_in_time"
	LineLate     = "completed_late"
	LineCensored = "censored"
	LineUnsent   = "unsent"
)

// errClasses are the error classes in print order.
var errClasses = []string{
	loadgen.ErrStatus429, loadgen.ErrStatusOther, loadgen.ErrConnect,
	loadgen.ErrReset, loadgen.ErrEmptyStream, loadgen.ErrMalformedStream,
}

// stepLines is the print order. One line per status follows the
// status_other line.
var stepLines = append(append([]string{LineInTime, LineLate}, errClasses...), LineCensored, LineUnsent)

// States of a step in a cell's table.
const (
	// StepExcluded is a step the ceiling rule did not admit.
	StepExcluded = "excluded"
	// StepNotAttempted is an admitted step after the last step at which
	// the cell dispatched a request: its ladder ended before the step.
	StepNotAttempted = "not_attempted"
	StepAttempted    = "attempted"
)

// z95 is the standard normal quantile of a two-sided 95 percent
// interval.
const z95 = 1.959964

// wilson is the 95 percent Wilson score interval of a proportion p over
// n trials. n is positive and need not be whole.
func wilson(p, n float64) (lo, hi float64) {
	z2 := z95 * z95
	den := 1 + z2/n
	c := (p + z2/(2*n)) / den
	h := z95 * math.Sqrt(p*(1-p)/n+z2/(4*n*n)) / den
	return math.Max(0, c-h), math.Min(1, c+h)
}

// designEffect is the larger of 1 and the cluster-robust variance of the
// pooled rate over its variance under independent requests, for cells
// with k[i] events in n[i] requests (SPEC-HOSTED HA3 point 13). It is 1,
// with assumed true, where the spread cannot be estimated: under two
// cells, no event, or every request an event.
func designEffect(k, n []int) (deff float64, assumed bool) {
	var sumK, sumN int
	for i := range k {
		sumK += k[i]
		sumN += n[i]
	}
	g := len(k)
	if g < 2 || sumK == 0 || sumK == sumN {
		return 1, true
	}
	N := float64(sumN)
	p := float64(sumK) / N
	var ss float64
	for i := range k {
		d := float64(k[i]) - p*float64(n[i])
		ss += d * d
	}
	v := float64(g) / float64(g-1) * ss / (N * N)
	return math.Max(1, v/(p*(1-p)/N)), false
}

// Rate is Count requests over Scheduled with its 95 percent Wilson
// interval, which treats the step's requests as independent.
type Rate struct {
	Count     int     `json:"count"`
	Scheduled int     `json:"scheduled"`
	Value     float64 `json:"rate"`
	Lo        float64 `json:"wilson_lo"`
	Hi        float64 `json:"wilson_hi"`
}

func newRate(k, n int) Rate {
	p := float64(k) / float64(n)
	lo, hi := wilson(p, float64(n))
	return Rate{Count: k, Scheduled: n, Value: p, Lo: lo, Hi: hi}
}

// Line is one outcome line of a cell's step. Status is a status line's
// HTTP status and Doc the provider's documented meaning of it, nil where
// target.status_docs records none. InBand is a malformed_stream
// sub-line's in-band error label (inBandLabel) and ErrorDoc the
// provider's documented meaning of it, nil where target.error_docs
// records none.
type Line struct {
	Name     string            `json:"name"`
	Status   int               `json:"status,omitempty"`
	Doc      *config.StatusDoc `json:"documented,omitempty"`
	InBand   string            `json:"in_band_error,omitempty"`
	ErrorDoc *config.ErrorDoc  `json:"documented_error,omitempty"`
	Rate
}

// G6 is hosted-G6 at one step of one cell: completion within the
// deadline over scheduled requests, its point estimate against the
// pinned line (reported, never run-invalidating), and the same fraction
// over the requests that were not connect or reset errors, nil where
// every request was (SPEC-HOSTED §7).
type G6 struct {
	InTime                Rate  `json:"in_time"`
	Pass                  bool  `json:"pass"`
	ExcludingConnectReset *Rate `json:"excluding_connect_reset,omitempty"`
}

// CellStep is one step of a cell. An attempted step with scheduled
// requests carries its lines, hosted-G6 and signals.
type CellStep struct {
	Step      int      `json:"step"`
	RPS       float64  `json:"rps"`
	State     string   `json:"state"`
	Scheduled int      `json:"scheduled"`
	Lines     []Line   `json:"lines,omitempty"`
	G6        *G6      `json:"hosted_g6,omitempty"`
	Signals   *Signals `json:"signals,omitempty"`
}

// CellTable is one cell's per-step table.
type CellTable struct {
	Cell  string     `json:"cell"`
	Steps []CellStep `json:"steps"`
}

// Pooled is a step's count over the requests its Cells pooled cells
// scheduled there, the range of the cell rates, the cells with at least
// one event, and a 95 percent Wilson interval on the effective sample
// size Scheduled / DEFF, DEFF being the design effect (designEffect).
// AssumesIndependence marks a DEFF of 1 that could not be estimated.
type Pooled struct {
	Count               int     `json:"count"`
	Scheduled           int     `json:"scheduled"`
	Rate                float64 `json:"rate"`
	Cells               int     `json:"cells"`
	CellMin             float64 `json:"cell_rate_min"`
	CellMax             float64 `json:"cell_rate_max"`
	CellsWithEvents     int     `json:"cells_with_events"`
	DEFF                float64 `json:"deff"`
	EffectiveN          float64 `json:"effective_n"`
	Lo                  float64 `json:"wilson_lo"`
	Hi                  float64 `json:"wilson_hi"`
	AssumesIndependence bool    `json:"assumes_independent_requests,omitempty"`
}

func newPooled(k, n []int) Pooled {
	p := Pooled{Cells: len(k)}
	for i := range k {
		p.Count += k[i]
		p.Scheduled += n[i]
		r := float64(k[i]) / float64(n[i])
		if i == 0 || r < p.CellMin {
			p.CellMin = r
		}
		if i == 0 || r > p.CellMax {
			p.CellMax = r
		}
		if k[i] > 0 {
			p.CellsWithEvents++
		}
	}
	p.Rate = float64(p.Count) / float64(p.Scheduled)
	p.DEFF, p.AssumesIndependence = designEffect(k, n)
	p.EffectiveN = float64(p.Scheduled) / p.DEFF
	p.Lo, p.Hi = wilson(p.Rate, p.EffectiveN)
	return p
}

// PooledLine is one outcome line pooled over a step's cells.
type PooledLine struct {
	Name     string            `json:"name"`
	Status   int               `json:"status,omitempty"`
	Doc      *config.StatusDoc `json:"documented,omitempty"`
	InBand   string            `json:"in_band_error,omitempty"`
	ErrorDoc *config.ErrorDoc  `json:"documented_error,omitempty"`
	Pooled
}

// PooledG6 is hosted-G6 pooled over a step's cells, its verdict taken on
// the pooled point estimate.
type PooledG6 struct {
	InTime                Pooled  `json:"in_time"`
	Pass                  bool    `json:"pass"`
	ExcludingConnectReset *Pooled `json:"excluding_connect_reset,omitempty"`
}

// PooledStep is one step pooled over the cells that attempted it with
// requests scheduled; Cells counts them. No figure pools across steps.
type PooledStep struct {
	Step      int          `json:"step"`
	RPS       float64      `json:"rps"`
	Admitted  bool         `json:"admitted"`
	Cells     int          `json:"cells"`
	Scheduled int          `json:"scheduled"`
	Lines     []PooledLine `json:"lines,omitempty"`
	G6        *PooledG6    `json:"hosted_g6,omitempty"`
	Signals   *Signals     `json:"signals,omitempty"`
}

// StepTables is one route's per-step tables (SPEC-HOSTED HA3 point 13):
// the ceiling rule's verdict per step with the step's nominal demand as
// a fraction of each recorded limit, one table per cell, and, for a
// complete frame of admission.ReleaseCells cells, the cells pooled per
// step. Pooled is nil for an incomplete frame.
type StepTables struct {
	Route         string           `json:"route"`
	CeilingTokens int              `json:"ceiling_tokens"`
	DeadlineS     int              `json:"deadline_s"`
	Limits        []admission.Step `json:"limits"`
	Cells         []CellTable      `json:"cells"`
	FrameCells    int              `json:"frame_cells"`
	Pooled        []PooledStep     `json:"pooled,omitempty"`
}

// CellRecords is one cell's label, the outcome its manifest records
// (evidence.Cell) and the records of its attempts file.
type CellRecords struct {
	Cell    string
	Outcome string
	Records []evidence.Record
}

// stepTally counts one step's requests per line, the status_other
// requests per status and the malformed_stream requests per in-band
// error label. Codes and labels list those in ascending order;
// reachable counts the requests that were not connect or reset errors.
// It also keeps the durations and Retry-After values signals reads.
type stepTally struct {
	lines     map[string]int
	statuses  map[int]int
	codes     []int
	inBand    map[string]int
	labels    []string
	reachable int
	timing
}

// tables carries the status and error documentation, whether
// Retry-After could be recorded, and each cell's tallies, in the order
// of the cell tables, while the tables are built.
type tables struct {
	*StepTables
	docs          map[int]*config.StatusDoc
	errorDocs     map[string]*config.ErrorDoc
	retryRecorded bool
	tallies       [][]stepTally
}

// lineOf is the line a release record falls on.
func lineOf(r *evidence.Record) (string, error) {
	switch r.Outcome {
	case "":
		if r.DispatchNs != 0 {
			return "", errors.New("dispatched with no outcome")
		}
		return LineUnsent, nil
	case loadgen.OutcomeCompleted:
		switch {
		case r.InTime && !r.Late:
			return LineInTime, nil
		case r.Late && !r.InTime:
			return LineLate, nil
		}
		return "", fmt.Errorf("completed with in_time %v and late %v", r.InTime, r.Late)
	case loadgen.OutcomeCensored:
		return LineCensored, nil
	case loadgen.OutcomeErrored:
		if slices.Contains(errClasses, r.ErrClass) {
			return r.ErrClass, nil
		}
		return "", fmt.Errorf("error class %q", r.ErrClass)
	}
	return "", fmt.Errorf("outcome %q", r.Outcome)
}

// NewStepTables builds a release route's per-step tables from the
// ceiling rule's verdict v and each cell's records. Warm-up records are
// left out. A step is attempted when it or a later step dispatched a
// request; a step v excludes must have dispatched none. Every cell must
// be counted: a labelled cell never enters the route's frame.
func NewStepTables(cfg *config.Config, v *admission.Verdict, cells []CellRecords) (*StepTables, error) {
	for _, c := range cells {
		if c.Outcome != evidence.CellCounted {
			return nil, fmt.Errorf("report: step tables: cell %q is %q: a labelled cell never enters a route's frame", c.Cell, c.Outcome)
		}
	}
	return stepTables(cfg, v, cells)
}

// stepTables builds the tables of cells whatever their outcomes.
func stepTables(cfg *config.Config, v *admission.Verdict, cells []CellRecords) (*StepTables, error) {
	if cfg.Profile != config.ProfileRelease || cfg.Release == nil {
		return nil, errors.New("report: step tables: not a release configuration")
	}
	rel := cfg.Release
	steps := rel.Ladder.StepsRPS
	switch {
	case v == nil:
		return nil, errors.New("report: step tables: no ceiling-rule verdict")
	case v.Route != cfg.Target.Name || v.CeilingTokens != rel.Shape.CeilingTokens:
		return nil, fmt.Errorf("report: step tables: verdict for route %q at %d, the configuration's %q at %d",
			v.Route, v.CeilingTokens, cfg.Target.Name, rel.Shape.CeilingTokens)
	case len(v.Steps) != len(steps):
		return nil, fmt.Errorf("report: step tables: verdict has %d steps, the ladder %d", len(v.Steps), len(steps))
	}
	for k := range steps {
		switch s := v.Steps[k]; {
		case s.RPS != steps[k]:
			return nil, fmt.Errorf("report: step tables: verdict step %d is %g rps, the ladder's %g", k+1, s.RPS, steps[k])
		case !s.Admitted && s.Binding == nil:
			return nil, fmt.Errorf("report: step tables: verdict step %d is excluded with no binding limit", k+1)
		}
	}
	if len(cells) > admission.ReleaseCells {
		return nil, fmt.Errorf("report: step tables: %d cells, a frame holds %d", len(cells), admission.ReleaseCells)
	}
	t := &tables{
		StepTables: &StepTables{
			Route: cfg.Target.Name, CeilingTokens: rel.Shape.CeilingTokens, DeadlineS: rel.Shape.DeadlineS,
			Limits: v.Steps, FrameCells: admission.ReleaseCells,
		},
		docs:          map[int]*config.StatusDoc{},
		errorDocs:     map[string]*config.ErrorDoc{},
		retryRecorded: config.HeaderAllowed(cfg.Target.ResponseHeaders, "Retry-After"),
	}
	for i := range cfg.Target.StatusDocs {
		d := cfg.Target.StatusDocs[i]
		t.docs[d.Status] = &d
	}
	for i := range cfg.Target.ErrorDocs {
		d := cfg.Target.ErrorDocs[i]
		t.errorDocs[d.Type] = &d
	}
	sorted := slices.Clone(cells)
	slices.SortFunc(sorted, func(a, b CellRecords) int { return strings.Compare(a.Cell, b.Cell) })
	for i, c := range sorted {
		switch {
		case c.Cell == "":
			return nil, errors.New("report: step tables: a cell has no label")
		case i > 0 && sorted[i-1].Cell == c.Cell:
			return nil, fmt.Errorf("report: step tables: cell %q given twice", c.Cell)
		}
		ct, tally, err := t.cellTable(cfg, c)
		if err != nil {
			return nil, fmt.Errorf("report: step tables: cell %q: %w", c.Cell, err)
		}
		t.Cells = append(t.Cells, ct)
		t.tallies = append(t.tallies, tally)
	}
	if len(t.Cells) == admission.ReleaseCells {
		t.pool()
	}
	return t.StepTables, nil
}

func (t *tables) cellTable(cfg *config.Config, c CellRecords) (CellTable, []stepTally, error) {
	rel := cfg.Release
	steps := rel.Ladder.StepsRPS
	ct := CellTable{Cell: c.Cell, Steps: make([]CellStep, len(steps))}
	tally := make([]stepTally, len(steps))
	dispatched := make([]bool, len(steps))
	for k := range steps {
		ct.Steps[k] = CellStep{Step: k + 1, RPS: steps[k]}
		tally[k] = newStepTally()
	}
	for i := range c.Records {
		r := &c.Records[i]
		if r.Shape != rel.Shape.Name || r.CeilingTokens != rel.Shape.CeilingTokens {
			return ct, nil, fmt.Errorf("record %d is shape %q at %d, the configuration's %q at %d",
				r.Index, r.Shape, r.CeilingTokens, rel.Shape.Name, rel.Shape.CeilingTokens)
		}
		if r.Warmup {
			continue
		}
		if r.Step < 1 || r.Step > len(steps) {
			return ct, nil, fmt.Errorf("record %d is at step %d of a %d-step ladder", r.Index, r.Step, len(steps))
		}
		line, err := lineOf(r)
		if err != nil {
			return ct, nil, fmt.Errorf("record %d: %w", r.Index, err)
		}
		ct.Steps[r.Step-1].Scheduled++
		tl := &tally[r.Step-1]
		tl.lines[line]++
		if line == loadgen.ErrStatusOther {
			tl.statuses[r.Status]++
		}
		if line == loadgen.ErrMalformedStream {
			tl.inBand[inBandLabel(r.StreamError)]++
		}
		if err := tl.add(line, r); err != nil {
			return ct, nil, fmt.Errorf("record %d: %w", r.Index, err)
		}
		if r.DispatchNs != 0 {
			dispatched[r.Step-1] = true
		}
	}
	later := false
	for k := len(steps) - 1; k >= 0; k-- {
		later = later || dispatched[k]
		s, tl := &ct.Steps[k], &tally[k]
		switch {
		case !t.Limits[k].Admitted && dispatched[k]:
			return ct, nil, fmt.Errorf("step %d (%g rps) is excluded by the ceiling rule and dispatched requests", k+1, steps[k])
		case !t.Limits[k].Admitted:
			s.State = StepExcluded
		case later:
			s.State = StepAttempted
		default:
			s.State = StepNotAttempted
		}
		if s.State != StepAttempted || s.Scheduled == 0 {
			continue
		}
		for code := range tl.statuses {
			tl.codes = append(tl.codes, code)
		}
		slices.Sort(tl.codes)
		for label := range tl.inBand {
			tl.labels = append(tl.labels, label)
		}
		slices.Sort(tl.labels)
		tl.reachable = s.Scheduled - tl.lines[loadgen.ErrConnect] - tl.lines[loadgen.ErrReset]
		s.Lines = t.lines(tl, func(k int) Rate { return newRate(k, s.Scheduled) })
		s.Signals = signals([]*stepTally{tl}, s.Scheduled, t.retryRecorded)
		in := newRate(tl.lines[LineInTime], s.Scheduled)
		s.G6 = &G6{InTime: in, Pass: in.Value >= config.PinnedBaselineGoodputMin}
		if tl.reachable > 0 {
			r := newRate(tl.lines[LineInTime], tl.reachable)
			s.G6.ExcludingConnectReset = &r
		}
	}
	return ct, tally, nil
}

// lines builds a step's outcome lines in print order, a status line per
// code after status_other and an in-band error line per label after
// malformed_stream.
func (t *tables) lines(c *stepTally, rate func(int) Rate) []Line {
	var out []Line
	for _, name := range stepLines {
		l := Line{Name: name, Rate: rate(c.lines[name])}
		if name == loadgen.ErrStatus429 {
			l.Status, l.Doc = 429, t.docs[429]
		}
		out = append(out, l)
		switch name {
		case loadgen.ErrStatusOther:
			for _, code := range c.codes {
				out = append(out, Line{Name: name, Status: code, Doc: t.docs[code], Rate: rate(c.statuses[code])})
			}
		case loadgen.ErrMalformedStream:
			for _, label := range c.labels {
				out = append(out, Line{Name: name, InBand: label, ErrorDoc: t.errorDocs[label], Rate: rate(c.inBand[label])})
			}
		}
	}
	return out
}

// pool fills Pooled from the cell tables: at each step, the cells that
// attempted it with requests scheduled.
func (t *tables) pool() {
	for k, lim := range t.Limits {
		ps := PooledStep{Step: k + 1, RPS: lim.RPS, Admitted: lim.Admitted}
		var cs []*stepTally
		var n []int
		codes, labels := map[int]bool{}, map[string]bool{}
		for i := range t.Cells {
			s := &t.Cells[i].Steps[k]
			if s.State != StepAttempted || s.Scheduled == 0 {
				continue
			}
			cs, n = append(cs, &t.tallies[i][k]), append(n, s.Scheduled)
			ps.Scheduled += s.Scheduled
			for _, code := range t.tallies[i][k].codes {
				codes[code] = true
			}
			for _, label := range t.tallies[i][k].labels {
				labels[label] = true
			}
		}
		ps.Cells = len(cs)
		if len(cs) > 0 {
			var sorted []int
			for code := range codes {
				sorted = append(sorted, code)
			}
			slices.Sort(sorted)
			var sortedLabels []string
			for label := range labels {
				sortedLabels = append(sortedLabels, label)
			}
			slices.Sort(sortedLabels)
			counts := func(f func(*stepTally) int) []int {
				k := make([]int, len(cs))
				for i, s := range cs {
					k[i] = f(s)
				}
				return k
			}
			for _, name := range stepLines {
				l := PooledLine{Name: name, Pooled: newPooled(counts(func(s *stepTally) int { return s.lines[name] }), n)}
				if name == loadgen.ErrStatus429 {
					l.Status, l.Doc = 429, t.docs[429]
				}
				ps.Lines = append(ps.Lines, l)
				switch name {
				case loadgen.ErrStatusOther:
					for _, code := range sorted {
						ps.Lines = append(ps.Lines, PooledLine{Name: name, Status: code, Doc: t.docs[code],
							Pooled: newPooled(counts(func(s *stepTally) int { return s.statuses[code] }), n)})
					}
				case loadgen.ErrMalformedStream:
					for _, label := range sortedLabels {
						ps.Lines = append(ps.Lines, PooledLine{Name: name, InBand: label, ErrorDoc: t.errorDocs[label],
							Pooled: newPooled(counts(func(s *stepTally) int { return s.inBand[label] }), n)})
					}
				}
			}
			in := newPooled(counts(func(s *stepTally) int { return s.lines[LineInTime] }), n)
			ps.G6 = &PooledG6{InTime: in, Pass: in.Rate >= config.PinnedBaselineGoodputMin}
			var rk, rn []int
			for _, s := range cs {
				if s.reachable > 0 {
					rk, rn = append(rk, s.lines[LineInTime]), append(rn, s.reachable)
				}
			}
			if len(rn) > 0 {
				p := newPooled(rk, rn)
				ps.G6.ExcludingConnectReset = &p
			}
			ps.Signals = signals(cs, ps.Scheduled, t.retryRecorded)
		}
		t.Pooled = append(t.Pooled, ps)
	}
}

// Text renders the tables. Rates print as percentages.
func (t *StepTables) Text() string { return t.text(true) }

// text renders the tables, with the frame's pooled figures or the
// incomplete-frame line when frame is set.
func (t *StepTables) text(frame bool) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	pct := func(f float64) string { return fmt.Sprintf("%.2f%%", 100*f) }
	label := func(name string, status int, inBand string) string {
		switch {
		case inBand != "":
			return name + " " + inBand
		case status != 0 && name != loadgen.ErrStatus429:
			return fmt.Sprintf("%s %d", name, status)
		}
		return name
	}
	note := func(status int, doc *config.StatusDoc, inBand string, edoc *config.ErrorDoc) string {
		switch {
		case inBand == InBandNone || inBand == InBandUntyped || inBand == InBandUndecodable:
			return ""
		case inBand != "" && edoc == nil:
			return fmt.Sprintf("; %s: no documented meaning recorded", inBand)
		case inBand != "":
			corr := ""
			if edoc.Status != 0 {
				corr = fmt.Sprintf(", corresponding to status %d", edoc.Status)
			}
			return fmt.Sprintf("; %s documented as %q%s (%s, retrieved %s)", inBand, edoc.Meaning, corr, edoc.Source, edoc.Retrieved)
		case status == 0:
			return ""
		case doc == nil:
			return fmt.Sprintf("; %d: no documented meaning recorded", status)
		}
		return fmt.Sprintf("; %d documented as %q (%s, retrieved %s)", status, doc.Meaning, doc.Source, doc.Retrieved)
	}

	w("== Per-step tables: route %s, ceiling %d tokens, deadline %d s ==", t.Route, t.CeilingTokens, t.DeadlineS)
	w("Each rate is a count over the step's scheduled requests; no figure pools across steps or routes.")
	w("")
	w("-- Ceiling rule: nominal demand as a fraction of each recorded limit --")
	for k, s := range t.Limits {
		var parts []string
		for _, d := range s.Demands {
			test := "half test"
			if !d.Half {
				test = "cover test"
			}
			parts = append(parts, fmt.Sprintf("%s %.1f%% of %g (%s: %s)", d.Dimension, 100*d.Fraction, d.Value, test, passText(d.Pass)))
		}
		state := "admitted"
		if !s.Admitted {
			state = "excluded, binding limit " + s.Binding.Dimension
		}
		w("step %d, %g rps: %s; %s", k+1, s.RPS, state, strings.Join(parts, "; "))
	}
	w("")
	for _, c := range t.Cells {
		w("-- Cell %s: 95 percent Wilson intervals, treating the step's requests as independent --", c.Cell)
		for _, s := range c.Steps {
			switch {
			case s.State == StepExcluded:
				w("step %d, %g rps: excluded by the ceiling rule", s.Step, s.RPS)
				continue
			case s.State == StepNotAttempted:
				w("step %d, %g rps: not attempted (the cell's ladder ended before this step)", s.Step, s.RPS)
				continue
			case s.Scheduled == 0:
				w("step %d, %g rps: no requests scheduled", s.Step, s.RPS)
				continue
			}
			w("step %d, %g rps, %d scheduled:", s.Step, s.RPS, s.Scheduled)
			for _, l := range s.Lines {
				w("  %-22s %d/%d %s [%s, %s]%s", label(l.Name, l.Status, l.InBand), l.Count, l.Scheduled, pct(l.Value), pct(l.Lo), pct(l.Hi), note(l.Status, l.Doc, l.InBand, l.ErrorDoc))
				if l.Name == loadgen.ErrStatus429 {
					retryAfterText(w, s.Signals, t.Limits[s.Step-1])
				}
			}
			w("  hosted-G6 (completion within %d s over scheduled): %s [%s, %s] against %.2f: %s (reported, not run-invalidating)",
				t.DeadlineS, pct(s.G6.InTime.Value), pct(s.G6.InTime.Lo), pct(s.G6.InTime.Hi), config.PinnedBaselineGoodputMin, passText(s.G6.Pass))
			if r := s.G6.ExcludingConnectReset; r != nil {
				w("  hosted-G6 excluding connect and reset: %d/%d %s [%s, %s]", r.Count, r.Scheduled, pct(r.Value), pct(r.Lo), pct(r.Hi))
			}
			signalsText(w, s.Signals)
		}
		w("")
	}
	if !frame {
		return b.String()
	}
	if t.Pooled == nil {
		w("-- Incomplete frame: %d of %d cells; no pooled figures --", len(t.Cells), t.FrameCells)
		return b.String()
	}
	pooled := func(p Pooled) string {
		dep := fmt.Sprintf("DEFF %.2f", p.DEFF)
		if p.AssumesIndependence {
			dep += ", assumes independent requests"
		}
		return fmt.Sprintf("%d/%d %s; cell rates %s to %s; %d of %d cells with events; %s; effective n %.1f; 95%% Wilson [%s, %s]",
			p.Count, p.Scheduled, pct(p.Rate), pct(p.CellMin), pct(p.CellMax), p.CellsWithEvents, p.Cells, dep, p.EffectiveN, pct(p.Lo), pct(p.Hi))
	}
	w("-- Pooled per step over the frame's %d cells: each step pools the cells that attempted it --", t.FrameCells)
	for _, s := range t.Pooled {
		switch {
		case !s.Admitted:
			w("step %d, %g rps: excluded by the ceiling rule", s.Step, s.RPS)
			continue
		case s.Cells == 0:
			w("step %d, %g rps: attempted in no cell", s.Step, s.RPS)
			continue
		}
		w("step %d, %g rps, %d of %d cells, %d scheduled:", s.Step, s.RPS, s.Cells, t.FrameCells, s.Scheduled)
		for _, l := range s.Lines {
			w("  %-22s %s%s", label(l.Name, l.Status, l.InBand), pooled(l.Pooled), note(l.Status, l.Doc, l.InBand, l.ErrorDoc))
			if l.Name == loadgen.ErrStatus429 {
				retryAfterText(w, s.Signals, t.Limits[s.Step-1])
			}
		}
		w("  hosted-G6 (completion within %d s over scheduled): %s against %.2f: %s (reported, not run-invalidating)",
			t.DeadlineS, pooled(s.G6.InTime), config.PinnedBaselineGoodputMin, passText(s.G6.Pass))
		if p := s.G6.ExcludingConnectReset; p != nil {
			w("  hosted-G6 excluding connect and reset: %s", pooled(*p))
		}
		signalsText(w, s.Signals)
	}
	return b.String()
}

func passText(pass bool) string {
	if pass {
		return "pass"
	}
	return "FAIL"
}
