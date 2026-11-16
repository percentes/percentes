package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/probe"
)

// checkRecordsOf are the ceiling-check records of route under out by their
// admission's ledger line.
func checkRecordsOf(t *testing.T, out, route string) map[int]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(out, "ceiling-*", "*", probe.File))
	if err != nil {
		t.Fatal(err)
	}
	m := map[int]string{}
	for _, p := range paths {
		rec := readRecord(t, p)
		if rec.Route == route {
			m[rec.LedgerSeq] = p
		}
	}
	return m
}

func readRecord(t *testing.T, path string) *probe.ReleaseRecord {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rec probe.ReleaseRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return &rec
}

// rewriteRecord rewrites the record at path through edit, as ceiling-check
// encodes it.
func rewriteRecord(t *testing.T, path string, edit func(*probe.ReleaseRecord)) {
	t.Helper()
	rec := readRecord(t, path)
	edit(rec)
	b, err := probe.Encode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// fastCeilingFixture is newCeilingFixture with a route cap and a token
// limit that pace the check's requests about 0.25 s apart.
func fastCeilingFixture(t *testing.T, endpoint, key string) *ceilingFixture {
	t.Helper()
	path := routeConfigFile(t, "groq", endpoint, "100")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const tpm = "\n      value: 250000\n"
	if strings.Count(string(b), tpm) != 1 {
		t.Fatalf("configuration: %q not found once", tpm)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(b), tpm, "\n      value: 2500000\n", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	return ceilingFixtureAt(t, path, key)
}

// soloFile is the configuration at cfgPath with no other host, saved as
// name.
func soloFile(t *testing.T, cfgPath, name string) string {
	t.Helper()
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	i := strings.Index(s, "\n  other_hosts:")
	if i < 0 {
		t.Fatal("configuration: no other_hosts")
	}
	rest := s[i+1:]
	for k := 0; k < 3; k++ {
		rest = rest[strings.Index(rest, "\n")+1:]
	}
	return writeFile(t, t.TempDir(), name, []byte(s[:i+1]+rest))
}

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
