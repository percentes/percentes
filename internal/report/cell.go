package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/schedule"
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
// cfg and the records of its attempts file.
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
	st, err := stepTables(cfg, c.Admission, []CellRecords{{Cell: label, Outcome: c.Outcome, Records: recs}})
	if err != nil {
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
	if !r.LadderStarted {
		w("The ladder did not start; the attempts file is empty.")
	}
	w("")
	b.WriteString(r.Tables.text(false))
	return b.String()
}
