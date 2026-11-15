package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"

	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// checkRecords refuses a ceiling-check record at paths that does not
// decode, or that was not sent on the ledger sched froze for one of its
// routes.
func checkRecords(paths []string, sched *schedule.Schedule) error {
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("pages: %w", err)
		}
		var rec probe.ReleaseRecord
		switch err := json.Unmarshal(b, &rec); {
		case err != nil:
			return fmt.Errorf("pages: --check-record %s: %w", p, err)
		case rec.LedgerSHA256 != sched.Input.LedgerSHA256:
			return fmt.Errorf("pages: --check-record %s: admitted in the ledger of open-line sha256 %q; the schedule froze %s", p, rec.LedgerSHA256, sched.Input.LedgerSHA256)
		case !slices.Contains(sched.Input.Routes, rec.Route):
			return fmt.Errorf("pages: --check-record %s: route %q is not a route of the schedule", p, rec.Route)
		}
	}
	return nil
}
