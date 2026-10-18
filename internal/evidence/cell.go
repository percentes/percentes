package evidence

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// Cell is a release run's cell record in its manifest (SPEC-HOSTED HA3
// point 15).
type Cell struct {
	// Run is the scheduled cell attempt; OffsetMinutes is the route's
	// pinned start offset inside the slot.
	Run           schedule.Run `json:"run"`
	OffsetMinutes int          `json:"offset_minutes"`
	// Concurrent lists the other routes' runs from the same client host
	// whose cells overlap this one.
	Concurrent []schedule.Run `json:"concurrent_runs"`
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
	if len(problems) > 0 {
		return errors.New("evidence: release cell: " + strings.Join(problems, "; "))
	}
	return nil
}
