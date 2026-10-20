package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
	"github.com/percentes/percentes/internal/validity"
)

// CellReport is one release cell's report: its run, the label its cell
// record carries and its per-step table (SPEC-HOSTED HA3 point 16).
// CountedInFrame is false for a labelled cell, which no route table
// pools.
type CellReport struct {
	SchemaVersion    int          `json:"schema_version"`
	ConfigSHA256     string       `json:"config_sha256"`
	InstrumentCommit string       `json:"instrument_commit"`
	Route            string       `json:"route"`
	Run              schedule.Run `json:"run"`
	Outcome          string       `json:"outcome"`
	Reasons          []string     `json:"reasons,omitempty"`
	LadderStarted    bool         `json:"ladder_started"`
	CountedInFrame   bool         `json:"counted_in_frame"`
	Tables           *StepTables  `json:"tables"`
}

// cellReport renders a release cell's report pair from its manifest m,
// cfg and the records of its attempts file, with each step's G2 result
// from m's gates.
func cellReport(m *evidence.Manifest, cfg *config.Config, recs []evidence.Record) ([]byte, string, error) {
	c := m.Release
	if c == nil {
		return nil, "", errors.New("report: the release manifest has no cell record")
	}
	for i := range recs {
		if !slices.Equal(recs[i].RunFailures, m.InvalidReasons) {
			return nil, "", fmt.Errorf("report: record %d run failures %q, the manifest's %q", i, recs[i].RunFailures, m.InvalidReasons)
		}
	}
	label := fmt.Sprintf("%s-a%d", c.Run.Label, c.Run.Attempt)
	st, err := stepTables(cfg, c.Admission, []CellRecords{{Cell: label, Outcome: c.Outcome, Gaps: c.Gaps, Records: recs}})
	if err != nil {
		return nil, "", err
	}
	if err := checkG6(m.Gates, recs, cfg.Release.Shape.DeadlineS); err != nil {
		return nil, "", err
	}
	if err := stepG2(st.Cells[0].Steps, m.Gates); err != nil {
		return nil, "", err
	}
	if err := stepCanary(st.Cells[0].Steps, c.Canary); err != nil {
		return nil, "", err
	}
	rep := &CellReport{
		SchemaVersion:    1,
		ConfigSHA256:     m.Config.SHA256,
		InstrumentCommit: commitText(m.Source.Revision, m.Source.Modified),
		Route:            st.Route,
		Run:              c.Run,
		Outcome:          c.Outcome,
		Reasons:          c.Reasons,
		LadderStarted:    c.LadderStarted,
		CountedInFrame:   m.Counted(),
		Tables:           st,
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, "", fmt.Errorf("report: marshal: %w", err)
	}
	return raw, rep.text(), nil
}

// checkG6 reports an error unless the manifest's hosted-G6 results are
// the ones the records give.
func checkG6(gates []validity.Gate, recs []evidence.Record, deadlineS int) error {
	var got []validity.Gate
	for _, g := range gates {
		if g.ID == "G6" {
			got = append(got, g)
		}
	}
	if !reflect.DeepEqual(got, evidence.ReleaseG6(recs, deadlineS)) {
		return errors.New("report: hosted-G6 from the records differs from the manifest's")
	}
	return nil
}

// stepG2 sets each step's G2 from the per-step G2 results in gates. The
// results must cover steps 1 to n for some n, include every attempted
// step and no step the ceiling rule excluded, and name each step once.
func stepG2(steps []CellStep, gates []validity.Gate) error {
	for _, g := range gates {
		if g.ID != "G2" || g.Step == 0 {
			continue
		}
		switch {
		case g.Step < 0 || g.Step > len(steps):
			return fmt.Errorf("report: G2 at step %d of a %d-step ladder", g.Step, len(steps))
		case steps[g.Step-1].State == StepExcluded:
			return fmt.Errorf("report: G2 at step %d, which the ceiling rule excluded", g.Step)
		case steps[g.Step-1].G2 != nil:
			return fmt.Errorf("report: G2 at step %d given twice", g.Step)
		}
		steps[g.Step-1].G2 = &G2{Observed: g.Observed, Pass: g.Pass, Detail: g.Detail}
	}
	for k, s := range steps {
		switch {
		case s.State == StepAttempted && s.G2 == nil:
			return fmt.Errorf("report: step %d is attempted and the manifest records no G2 result for it", s.Step)
		case k > 0 && s.G2 != nil && steps[k-1].G2 == nil:
			return fmt.Errorf("report: G2 at step %d and none at step %d", s.Step, k)
		}
	}
	return nil
}

// stepCanary sets the loopback canary's summary on each step with a G2
// result, from canary in step order. A manifest with no summaries sets
// none.
func stepCanary(steps []CellStep, canary []loadgen.CanarySummary) error {
	if len(canary) == 0 {
		return nil
	}
	n := 0
	for _, s := range steps {
		if s.G2 != nil {
			n++
		}
	}
	if len(canary) != n {
		return fmt.Errorf("report: %d canary summaries for %d steps with a G2 result", len(canary), n)
	}
	for k := range canary {
		steps[k].Canary = &canary[k]
	}
	return nil
}

func (r *CellReport) text() string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	w("== Release cell: route %s, cell %s attempt %d ==", r.Route, r.Run.Label, r.Run.Attempt)
	w("Scheduled start %s, seed %d; instrument %s", r.Run.Start.UTC().Format("2006-01-02T15:04:05Z"), r.Run.Seed, r.InstrumentCommit)
	if r.CountedInFrame {
		w("Outcome: %s; the cell enters the route's frame of %d cells", r.Outcome, r.Tables.FrameCells)
	} else {
		w("Outcome: %s; a labelled record, outside the route's frame of %d cells", r.Outcome, r.Tables.FrameCells)
	}
	for _, reason := range r.Reasons {
		w("  %s", reason)
	}
	switch {
	case !r.LadderStarted:
		w("The ladder did not start; the attempts file is empty.")
	case r.Outcome == evidence.CellInterrupted:
		w("The ladder started and its requests ended with the process; the attempts file is empty.")
	}
	w("")
	b.WriteString(r.Tables.text(false))
	return b.String()
}
