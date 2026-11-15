package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"slices"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// checkAdmitted refuses recs, ceiling-check records of the ledger whose
// lines through the schedule's position are entries, unless each
// ceiling-check admission there of a route of routes has exactly one
// record naming its line (probe.ReleaseRecord.LedgerSeq), and every
// record that names a line names a ceiling-check admission of its route
// there.
func checkAdmitted(entries []admission.Entry, routes []string, recs []*probe.ReleaseRecord) error {
	admits := map[int]admission.Entry{}
	for _, e := range probeAdmissions(entries, nil) {
		admits[e.Seq] = e
	}
	named := map[int]bool{}
	for _, r := range recs {
		if r.LedgerSeq == 0 {
			continue
		}
		e, ok := admits[r.LedgerSeq]
		switch {
		case !ok || e.Route != r.Route:
			return fmt.Errorf("ceiling probe of route %s at %d: ledger line %d admits no ceiling check of the route up to the line the schedule froze", r.Route, r.Ceiling.Ceiling, r.LedgerSeq)
		case named[r.LedgerSeq]:
			return fmt.Errorf("ceiling probe of route %s at %d: another record names ledger line %d", r.Route, r.Ceiling.Ceiling, r.LedgerSeq)
		}
		named[r.LedgerSeq] = true
	}
	for _, e := range probeAdmissions(entries, routes) {
		if !named[e.Seq] {
			return fmt.Errorf("ledger line %d admits a ceiling check of route %s and no ceiling-check record given names it; every check's record is required", e.Seq, e.Route)
		}
	}
	return nil
}

// groupRoutes are the routes sched records on account group, with route
// itself.
func groupRoutes(sched *schedule.Schedule, route, group string) []string {
	out := []string{route}
	for _, c := range sched.Input.Configs {
		if c.AccountGroup == group && !slices.Contains(out, c.Route) {
			out = append(out, c.Route)
		}
	}
	return out
}

// lateChecks refuses a ceiling-check admission among entries after the
// first frozen lines, the lines the schedule froze, on account group
// group: one that names the group, or one of a route of routes, the
// group's routes.
func lateChecks(entries []admission.Entry, frozen int, group string, routes []string) error {
	if frozen > len(entries) {
		return nil
	}
	for _, e := range probeAdmissions(entries[frozen:], nil) {
		if e.AccountGroup == group || slices.Contains(routes, e.Route) {
			return fmt.Errorf("ledger line %d admits a ceiling check of route %s on account group %s after line %d, the line the schedule froze", e.Seq, e.Route, group, frozen)
		}
	}
	return nil
}

// readCheckRecord reads the ceiling-check record at path, which must be
// byte for byte the probe.Encode encoding of the record it decodes to,
// with no field probe.ReleaseRecord does not define.
func readCheckRecord(path string) (*probe.ReleaseRecord, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rec probe.ReleaseRecord
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("ceiling probe %s: %w", path, err)
	}
	if canon, err := probe.Encode(&rec); err != nil || !bytes.Equal(b, canon) {
		return nil, fmt.Errorf("ceiling probe %s: not the record as ceiling-check writes it", path)
	}
	return &rec, nil
}
