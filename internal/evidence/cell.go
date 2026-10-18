package evidence

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
	"github.com/percentes/percentes/internal/validity"
)

// Cell outcomes (SPEC-HOSTED HA3 point 16). A counted cell enters its
// route's frame; a cell with any other outcome is a labelled record that
// never does. A missed cell never started. An interrupted cell's process
// ended or failed before its evidence was written: its requests went with
// the process, so its attempts file and frame store are empty, and its
// own dispatch series is the running one as last written.
const (
	CellCounted     = "counted"
	CellInvalid     = "client_invalid"
	CellBlocked     = "blocked"
	CellStopped     = "stopped"
	CellMissed      = "missed"
	CellInterrupted = "interrupted"
)

// Cell is a release run's cell record in its manifest (SPEC-HOSTED HA3
// point 15).
type Cell struct {
	// Run is the scheduled cell attempt; OffsetMinutes is the route's
	// pinned start offset inside the slot.
	Run           schedule.Run `json:"run"`
	OffsetMinutes int          `json:"offset_minutes"`
	// Outcome and Reasons are the cell's label: why it does not count, or
	// where the backoff rule ended a counted cell's ladder. LadderStarted
	// is false for a cell stopped, blocked or missed before its ladder,
	// whose attempts file and frame store are empty.
	Outcome       string   `json:"outcome"`
	Reasons       []string `json:"reasons,omitempty"`
	LadderStarted bool     `json:"ladder_started"`
	// Concurrent lists the other routes' runs from the same client host
	// whose cells overlap this one.
	Concurrent []schedule.Run `json:"concurrent_runs"`
	// Dispatch is the host's aggregate dispatch series over the cell.
	Dispatch *Dispatch `json:"dispatch_aggregate"`
	// Gaps are the run's inter-chunk gap histograms per ladder step.
	Gaps []GapHistogram `json:"inter_chunk_gaps"`
	// Admission is the ceiling rule's verdict: the admitted steps, and
	// each excluded step with its binding limit.
	Admission *admission.Verdict `json:"admission"`
	// CeilingTokens is the ceiling in force, Ceiling the choice the probe
	// records made, and Probes those records.
	CeilingTokens int                 `json:"ceiling_tokens"`
	Ceiling       probe.CeilingChoice `json:"ceiling_choice"`
	Probes        []FileRef           `json:"probes"`
	// AccountCreated is the account's creation date; PreWindowRequests
	// counts the route's requests the ledger recorded before the window
	// opened.
	AccountCreated    string `json:"account_created"`
	PreWindowRequests int    `json:"pre_window_requests"`
}

// Dispatch is the client host's aggregate dispatch series over a cell
// (SPEC-HOSTED HA3 points 9 and 15): the requests every route sent per
// UTC second from StartUTC, the second of the cell's start, through the
// second of EndUTC, summed over Sources: the cell's own run first, then
// each of its concurrent runs in order.
type Dispatch struct {
	StartUTC     time.Time        `json:"start_utc"`
	EndUTC       time.Time        `json:"end_utc"`
	Counts       []int            `json:"counts"`
	MaxPerSecond int              `json:"max_per_second"`
	MaxAtUTC     *time.Time       `json:"max_at_utc,omitempty"`
	Sources      []DispatchSource `json:"sources"`
}

// DispatchSource is one run's part of the aggregate: its dispatches
// inside the series' seconds. Final is set when the run had ended and its
// series was whole; otherwise ThroughUTC is the time the series read was
// complete up to, absent when no series was found.
type DispatchSource struct {
	Run        schedule.Run `json:"run"`
	Dispatches int          `json:"dispatches"`
	Final      bool         `json:"final"`
	ThroughUTC *time.Time   `json:"through_utc,omitempty"`
}

// dispatchProblems reports every way c's aggregate dispatch series
// disagrees with itself and with the cell's runs.
func (c *Cell) dispatchProblems() []string {
	d := c.Dispatch
	if d == nil {
		return []string{"dispatch_aggregate: none"}
	}
	var problems []string
	fail := func(format string, a ...any) {
		problems = append(problems, "dispatch_aggregate: "+fmt.Sprintf(format, a...))
	}
	runs := append([]schedule.Run{c.Run}, c.Concurrent...)
	if len(d.Sources) != len(runs) {
		fail("%d sources for the cell's run and %d concurrent runs", len(d.Sources), len(c.Concurrent))
	} else {
		for i, src := range d.Sources {
			if src.Run != runs[i] {
				fail("source %d is %s %s attempt %d, want %s %s attempt %d", i, src.Run.Route, src.Run.Label, src.Run.Attempt, runs[i].Route, runs[i].Label, runs[i].Attempt)
			}
		}
		if !d.Sources[0].Final && c.Outcome != CellInterrupted {
			fail("the cell's own series is not final")
		}
	}
	if d.StartUTC.IsZero() || d.EndUTC.Before(d.StartUTC) {
		fail("span %s to %s", d.StartUTC.Format(time.RFC3339), d.EndUTC.Format(time.RFC3339))
	} else if n := int(d.EndUTC.Sub(d.StartUTC)/time.Second) + 1; len(d.Counts) != n {
		fail("%d counts for %d seconds", len(d.Counts), n)
	}
	sent, sum, top, at := 0, 0, 0, -1
	for _, src := range d.Sources {
		sent += src.Dispatches
	}
	for k, n := range d.Counts {
		if n < 0 {
			fail("negative count at second %d", k)
		}
		sum += n
		if n > top {
			top, at = n, k
		}
	}
	if sum != sent {
		fail("counts sum to %d, the sources to %d", sum, sent)
	}
	switch {
	case d.MaxPerSecond != top:
		fail("max_per_second %d, the counts' %d", d.MaxPerSecond, top)
	case at < 0 && d.MaxAtUTC != nil, at >= 0 && (d.MaxAtUTC == nil || !d.MaxAtUTC.Equal(d.StartUTC.Add(time.Duration(at)*time.Second))):
		fail("max_at_utc is not the first second of the maximum")
	}
	return problems
}

// checkCell reports every way c disagrees with the route's configuration
// and target.
func checkCell(cfg *config.Config, t *config.Target, c *Cell) error {
	var problems []string
	fail := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	if c.Run.Route != t.Name {
		fail("run is route %q, the target %q", c.Run.Route, t.Name)
	}
	if c.Run.Label == "" || c.Run.Attempt < 1 || c.Run.Start.IsZero() {
		fail("run: label, attempt and start are each required")
	}
	if c.OffsetMinutes < 0 || c.OffsetMinutes%int(schedule.OffsetStep/time.Minute) != 0 {
		fail("offset_minutes %d is not a multiple of %s", c.OffsetMinutes, schedule.OffsetStep)
	}
	for _, r := range c.Concurrent {
		if r.Route == "" || r.Route == t.Name {
			fail("concurrent run %q: another route's run is required", r.Route)
		}
	}
	ceiling := cfg.Release.Shape.CeilingTokens
	switch {
	case c.CeilingTokens != ceiling:
		fail("ceiling_tokens %d, the configuration's %d", c.CeilingTokens, ceiling)
	case c.Ceiling.Excluded || c.Ceiling.Ceiling != ceiling:
		fail("ceiling choice %+v does not run the model at %d", c.Ceiling, ceiling)
	}
	if len(c.Probes) == 0 {
		fail("probes: no probe record")
	}
	switch v := c.Admission; {
	case v == nil:
		fail("admission: no verdict")
	case v.Route != t.Name || v.CeilingTokens != ceiling:
		fail("admission verdict is route %q at %d, want %q at %d", v.Route, v.CeilingTokens, t.Name, ceiling)
	}
	if _, err := time.Parse(time.DateOnly, c.AccountCreated); err != nil {
		fail("account_created %q is not a date (2006-01-02)", c.AccountCreated)
	}
	if c.PreWindowRequests < 0 {
		fail("pre_window_requests %d is negative", c.PreWindowRequests)
	}
	problems = append(problems, c.dispatchProblems()...)
	if c.Gaps == nil {
		fail("inter_chunk_gaps: none")
	}
	problems = append(problems, gapProblems(c.Gaps)...)
	if len(problems) > 0 {
		return errors.New("evidence: release cell: " + strings.Join(problems, "; "))
	}
	return nil
}

// checkLabel reports every way c's label disagrees with itself and with
// the manifest's run validity, invalid reasons and gates.
func (c *Cell) checkLabel(runValid bool, invalid []string, gates []validity.Gate) error {
	var problems []string
	fail := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	switch c.Outcome {
	case CellCounted:
		if !c.LadderStarted {
			fail("counted with no ladder")
		}
		if !runValid || len(invalid) > 0 {
			fail("counted, and the manifest records the run invalid %q", invalid)
		}
		whole := false
		for _, g := range gates {
			whole = whole || g.ID == "G2" && g.Step == 0
			if !g.Pass && !g.ReportedOnly {
				fail("counted, and %s at step %d failed: %s", g.ID, g.Step, g.Detail)
			}
		}
		if !whole {
			fail("counted with no whole-ladder G2 result")
		}
	case CellInvalid, CellBlocked, CellStopped, CellMissed, CellInterrupted:
		if len(c.Reasons) == 0 {
			fail("%s with no reason", c.Outcome)
		}
		if runValid || !slices.Equal(c.Reasons, invalid) {
			fail("%s %q, and the manifest records valid=%t %q", c.Outcome, c.Reasons, runValid, invalid)
		}
	default:
		fail("outcome %q is not %s, %s, %s, %s, %s or %s", c.Outcome, CellCounted, CellInvalid, CellBlocked, CellStopped, CellMissed, CellInterrupted)
	}
	if (c.Outcome == CellBlocked || c.Outcome == CellMissed) && c.LadderStarted {
		fail("%s after its ladder started", c.Outcome)
	}
	if len(problems) > 0 {
		return errors.New("evidence: cell label: " + strings.Join(problems, "; "))
	}
	return nil
}

// Counted reports whether m is a release cell that enters its route's
// frame (SPEC-HOSTED HA3 point 9).
func (m *Manifest) Counted() bool {
	return m.Profile == config.ProfileRelease && m.Release != nil && m.Release.Outcome == CellCounted && m.RunValid
}
