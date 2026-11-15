package main

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/probe"
)

// Every ceiling-check admission of a route given has exactly one record
// naming its line, and a record names only a check of its route; pages
// applies the rule to the records the manifests and --check-record hold.
func TestCheckAdmitted(t *testing.T) {
	probeAt := func(seq int, route string) admission.Entry {
		return admission.Entry{Seq: seq, Kind: admission.KindAdmit, Purpose: admission.PurposeProbe, Route: route}
	}
	entries := []admission.Entry{{Seq: 1}, probeAt(2, "azure"), {Seq: 3, Kind: admission.KindAdmit, Purpose: admission.PurposeCell, Route: "azure"}, probeAt(4, "gemini")}
	rec := func(route string, seq int) *probe.ReleaseRecord {
		return &probe.ReleaseRecord{Route: route, LedgerSeq: seq}
	}
	for _, c := range []struct {
		name   string
		routes []string
		recs   []*probe.ReleaseRecord
		want   string
	}{
		{"each check recorded", nil, []*probe.ReleaseRecord{rec("azure", 2), rec("gemini", 4)}, ""},
		{"another route's check unrecorded", []string{"azure"}, []*probe.ReleaseRecord{rec("azure", 2)}, ""},
		{"a check unrecorded", nil, []*probe.ReleaseRecord{rec("azure", 2)}, "ledger line 4 admits a ceiling check of route gemini and no ceiling-check record given names it"},
		{"a record of another route's line", nil, []*probe.ReleaseRecord{rec("azure", 2), rec("azure", 4)}, "ledger line 4 admits no ceiling check of the route"},
		{"a record of a cell's line", nil, []*probe.ReleaseRecord{rec("azure", 3)}, "ledger line 3 admits no ceiling check of the route"},
		{"two records of one line", nil, []*probe.ReleaseRecord{rec("azure", 2), rec("azure", 2), rec("gemini", 4)}, "another record names ledger line 2"},
		{"a record naming no line", nil, []*probe.ReleaseRecord{rec("azure", 0), rec("azure", 2), rec("gemini", 4)}, ""},
	} {
		err := checkAdmitted(entries, c.routes, c.recs)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	if err := allChecks(nil, nil, entries, []string{"gemini"}); err == nil || !strings.Contains(err.Error(), "pages: ledger line 4 admits a ceiling check of route gemini") {
		t.Fatalf("pages with no record: %v", err)
	}
}
