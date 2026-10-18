package report

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

// The documentation state is the account tier and the limits and
// eligibility records by URL and hash, sorted; the other kinds, the
// retrieval date and the copy's name are left out, and a record listed
// twice counts once.
func TestDocsOf(t *testing.T) {
	rec := func(kind, name, url, sum, retrieved string) evidence.OperatorRecord {
		return evidence.OperatorRecord{Kind: kind, FileRef: evidence.FileRef{Name: name, SHA256: sum, Bytes: 1}, URL: url, Retrieved: retrieved}
	}
	m := &evidence.Manifest{Endpoint: evidence.Endpoint{AccountTier: "Developer"}, Records: []evidence.OperatorRecord{
		rec(evidence.RecordLimits, "limits-b.html", "https://example.test/limits/b", "bb", "2026-10-07"),
		rec(evidence.RecordPrices, "prices.html", "https://example.test/pricing", "cc", "2026-10-07"),
		rec(evidence.RecordEligibility, "models.html", "", "dd", "2026-10-07"),
		rec(evidence.RecordLimits, "limits-a.html", "https://example.test/limits/a", "aa", "2026-10-07"),
		rec(evidence.RecordTerms, "terms.html", "https://example.test/terms", "ee", "2026-10-07"),
		rec(evidence.RecordLimits, "limits-a-again.html", "https://example.test/limits/a", "aa", "2026-10-09"),
	}}
	want := []DocRecord{
		{Kind: DocTier, Tier: "Developer"},
		{Kind: evidence.RecordEligibility, SHA256: "dd"},
		{Kind: evidence.RecordLimits, URL: "https://example.test/limits/a", SHA256: "aa"},
		{Kind: evidence.RecordLimits, URL: "https://example.test/limits/b", SHA256: "bb"},
	}
	if got := DocsOf(m); !reflect.DeepEqual(got, want) {
		t.Errorf("DocsOf:\n got %+v\nwant %+v", got, want)
	}
}

// driftDocs are three documentation states: a, then b with the limits
// page changed, then c with the tier changed as well.
var (
	driftA = []DocRecord{{Kind: DocTier, Tier: "Developer"}, {Kind: evidence.RecordEligibility, SHA256: "e1"},
		{Kind: evidence.RecordLimits, URL: "https://example.test/limits", SHA256: "a1"}}
	driftB = []DocRecord{{Kind: DocTier, Tier: "Developer"}, {Kind: evidence.RecordEligibility, SHA256: "e1"},
		{Kind: evidence.RecordLimits, URL: "https://example.test/limits", SHA256: "b2"}}
	driftC = []DocRecord{{Kind: DocTier, Tier: "Scale"}, {Kind: evidence.RecordEligibility, SHA256: "e1"},
		{Kind: evidence.RecordLimits, URL: "https://example.test/limits", SHA256: "b2"}}
)

// driftCells are twelve cells started in the reverse of their label
// order, 8 h apart from 17 October 2026: the four first started record
// driftA, the next four driftB (its entries out of order in one cell)
// and the last four driftC. Only cell-00, started last, has status_429
// requests.
func driftCells() []CellRecords {
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	var cells []CellRecords
	for i := 0; i < 12; i++ {
		outs := []outcome{{1, LineInTime, 0, 120}}
		if i == 0 {
			outs = []outcome{{1, loadgen.ErrStatus429, 0, 12}, {1, LineInTime, 0, 108}}
		}
		outs = append(outs, outcome{2, LineInTime, 0, 240}, outcome{3, LineInTime, 0, 480}, outcome{4, LineUnsent, 0, 960},
			outcome{5, LineUnsent, 0, 1920})
		c := cellOf(fmt.Sprintf("cell-%02d", i), outs...)
		c.Start = base.Add(time.Duration(11-i) * 8 * time.Hour)
		switch {
		case i >= 8:
			c.Docs = driftA
		case i >= 4:
			c.Docs = driftB
			if i == 5 {
				c.Docs = slices.Clone(driftB)
				slices.Reverse(c.Docs)
			}
		default:
			c.Docs = driftC
		}
		cells = append(cells, c)
	}
	return cells
}

// SPEC-HOSTED §4 drift rule: the cell at which a change was first
// observed and every later cell pool apart from the cells before it, in
// start order; each change is named with what it removed and added, and
// each part is labelled with its cells and date range. An incomplete
// frame names the changes and pools nothing.
func TestStepTablesDrift(t *testing.T) {
	cfg, v := stepsConfig(t)
	cells := driftCells()
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	if st.Pooled != nil {
		t.Errorf("pooled over the whole frame: %d steps", len(st.Pooled))
	}
	wantChanges := []DriftChange{
		{Cell: "cell-07", StartUTC: time.Date(2026, 10, 18, 8, 0, 0, 0, time.UTC),
			Removed: []DocRecord{driftA[2]}, Added: []DocRecord{driftB[2]}},
		{Cell: "cell-03", StartUTC: time.Date(2026, 10, 19, 16, 0, 0, 0, time.UTC),
			Removed: []DocRecord{driftB[0]}, Added: []DocRecord{driftC[0]}},
	}
	if !reflect.DeepEqual(st.DriftChanges, wantChanges) {
		t.Errorf("changes:\n got %+v\nwant %+v", st.DriftChanges, wantChanges)
	}
	wantParts := []struct {
		cells       []string
		first, last time.Time
		docs        []DocRecord
		count429    int
	}{
		{[]string{"cell-11", "cell-10", "cell-09", "cell-08"}, time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC), driftA, 0},
		{[]string{"cell-07", "cell-06", "cell-05", "cell-04"}, time.Date(2026, 10, 18, 8, 0, 0, 0, time.UTC), time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC), driftB, 0},
		{[]string{"cell-03", "cell-02", "cell-01", "cell-00"}, time.Date(2026, 10, 19, 16, 0, 0, 0, time.UTC), time.Date(2026, 10, 20, 16, 0, 0, 0, time.UTC), driftC, 12},
	}
	if len(st.DriftParts) != len(wantParts) {
		t.Fatalf("%d parts", len(st.DriftParts))
	}
	for i, w := range wantParts {
		p := st.DriftParts[i]
		if !slices.Equal(p.Cells, w.cells) || !p.FirstStartUTC.Equal(w.first) || !p.LastStartUTC.Equal(w.last) || !reflect.DeepEqual(p.Docs, w.docs) {
			t.Errorf("part %d: %v %s to %s %+v", i+1, p.Cells, p.FirstStartUTC, p.LastStartUTC, p.Docs)
		}
		if len(p.Pooled) != 5 {
			t.Fatalf("part %d: %d pooled steps", i+1, len(p.Pooled))
		}
		s1 := p.Pooled[0]
		var l429 *PooledLine
		for j := range s1.Lines {
			if s1.Lines[j].Name == loadgen.ErrStatus429 {
				l429 = &s1.Lines[j]
			}
		}
		if s1.Cells != 4 || s1.Scheduled != 480 || l429 == nil || l429.Count != w.count429 || l429.Cells != 4 {
			t.Errorf("part %d step 1: %d cells, %d scheduled, status_429 %+v", i+1, s1.Cells, s1.Scheduled, l429)
		}
		if p.Pooled[3].Admitted || p.Pooled[3].Cells != 0 {
			t.Errorf("part %d step 4: %+v", i+1, p.Pooled[3])
		}
	}

	text := st.Text()
	for _, frag := range []string{
		"-- Change of limits or documentation, first observed at cell cell-07, started 2026-10-18 --\n" +
			"  removed: limits https://example.test/limits sha256 a1\n  added: limits https://example.test/limits sha256 b2\n",
		"-- Change of limits or documentation, first observed at cell cell-03, started 2026-10-19 --\n" +
			"  removed: account tier Developer\n  added: account tier Scale\n",
		"-- Part 1 of 3 under the drift rule: 4 cells started 2026-10-17 to 2026-10-18; each step pools the part's cells that attempted it --\n" +
			"cells cell-11, cell-10, cell-09, cell-08\nstep 1, 0.25 rps, 4 of 4 cells, 480 scheduled:\n",
		"-- Part 3 of 3 under the drift rule: 4 cells started 2026-10-19 to 2026-10-20; each step pools the part's cells that attempted it --\n",
		"  status_429             12/480 2.50%; cell rates 0.00% to 10.00%; 1 of 4 cells with events;",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	if strings.Contains(text, "-- Pooled per step over the frame's") || strings.Contains(text, "12 of 12 cells") {
		t.Errorf("text pools the whole frame:\n%s", text)
	}

	reversed := slices.Clone(cells)
	slices.Reverse(reversed)
	again, err := NewStepTables(cfg, v, reversed)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(st)
	b, _ := json.Marshal(again)
	if string(a) != string(b) {
		t.Error("cell order changed the tables")
	}

	inc, err := NewStepTables(cfg, v, cells[:11])
	if err != nil {
		t.Fatal(err)
	}
	if inc.Pooled != nil || inc.DriftParts != nil || !reflect.DeepEqual(inc.DriftChanges, wantChanges) {
		t.Errorf("incomplete frame: pooled %v, parts %v, changes %+v", inc.Pooled != nil, inc.DriftParts != nil, inc.DriftChanges)
	}
	if text := inc.Text(); !strings.Contains(text, "first observed at cell cell-03") || !strings.Contains(text, "-- Incomplete frame: 11 of 12 cells; no pooled figures --") {
		t.Errorf("incomplete frame text:\n%s", text)
	}
}

// One documentation state over the frame, in any entry order and with
// no starts, pools the whole frame; a state recorded for some cells
// only, a change with a cell's start missing, and a change with two
// cells starting together are refused.
func TestStepTablesDriftNone(t *testing.T) {
	cfg, v := stepsConfig(t)
	cells := driftCells()
	for i := range cells {
		cells[i].Start, cells[i].Docs = time.Time{}, slices.Clone(driftA)
		if i%2 == 1 {
			slices.Reverse(cells[i].Docs)
		}
	}
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Pooled) != 5 || st.Pooled[0].Cells != 12 || st.DriftChanges != nil || st.DriftParts != nil {
		t.Errorf("one state: %d pooled steps, changes %+v, parts %d", len(st.Pooled), st.DriftChanges, len(st.DriftParts))
	}

	for name, c := range map[string]struct {
		edit func([]CellRecords)
		want string
	}{
		"state for some cells only": {func(cs []CellRecords) { cs[3].Docs = nil }, "a documentation state is recorded for some cells only"},
		"no start":                  {func(cs []CellRecords) { cs[2].Start = time.Time{} }, `cell "cell-02" has no start, and the documentation state changes`},
		"two cells start together":  {func(cs []CellRecords) { cs[6].Start = cs[1].Start }, "starts with another cell, and the documentation state changes"},
	} {
		cs := driftCells()
		c.edit(cs)
		if _, err := NewStepTables(cfg, v, cs); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A route split by the drift rule carries no counts in the index; its
// row names the parts and points to the route page.
func TestIndexDrift(t *testing.T) {
	r := indexRoute(t, "groq", "Groq", 0, nil)
	r.Cells = driftCells()
	ix, err := NewIndex([]IndexRoute{r})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range ix.Rows {
		if row.DriftParts != 3 || row.Counts != nil || row.Cells != 0 || row.Scheduled != 0 || row.RecordedCells != 12 {
			t.Errorf("step %d: %+v", row.Step, row)
		}
	}
	frag := "| Groq | groq | openai/gpt-oss-120b | 1 | 0.25 | admitted | 3 | 512 | split in 3 parts by a change of limits or documentation; counts on the route page |  |  |  |  |  |  |  |  |  |  |  | [groq](routes/groq.html) |\n"
	if md := ix.Markdown(); !strings.Contains(md, frag) {
		t.Errorf("index lacks %q:\n%s", frag, md)
	}
	raw, err := json.Marshal(ix)
	if err != nil || !strings.Contains(string(raw), `"drift_parts":3`) {
		t.Errorf("index JSON: %v\n%s", err, raw)
	}
}
