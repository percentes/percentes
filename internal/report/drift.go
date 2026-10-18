package report

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/evidence"
)

// DocTier is the DocRecord kind of the account tier.
const DocTier = "account_tier"

// driftKinds are the operator record kinds the drift rule compares: the
// tier's published limits and the model documentation.
var driftKinds = []string{evidence.RecordLimits, evidence.RecordEligibility}

// DocRecord is one entry of the documentation state a cell recorded at
// its start under SPEC-HOSTED §4's drift rule: the account tier, or a
// limits or model-eligibility record by its URL and content hash.
type DocRecord struct {
	Kind   string `json:"kind"`
	Tier   string `json:"account_tier,omitempty"`
	URL    string `json:"url,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
}

func (d DocRecord) String() string {
	if d.Kind == DocTier {
		return "account tier " + d.Tier
	}
	s := d.Kind
	if d.URL != "" {
		s += " " + d.URL
	}
	return s + " sha256 " + d.SHA256
}

func compareDocs(a, b DocRecord) int {
	for _, c := range [][2]string{{a.Kind, b.Kind}, {a.Tier, b.Tier}, {a.URL, b.URL}, {a.SHA256, b.SHA256}} {
		if n := strings.Compare(c[0], c[1]); n != 0 {
			return n
		}
	}
	return 0
}

// DocsOf is the documentation state m records: its account tier and its
// limits and eligibility records, sorted. The retrieval date and the
// copy's file name are left out, so a re-recording of the same text is
// no change.
func DocsOf(m *evidence.Manifest) []DocRecord {
	docs := []DocRecord{{Kind: DocTier, Tier: m.Endpoint.AccountTier}}
	for _, r := range m.Records {
		if slices.Contains(driftKinds, r.Kind) {
			docs = append(docs, DocRecord{Kind: r.Kind, URL: r.URL, SHA256: r.SHA256})
		}
	}
	return sortDocs(docs)
}

func sortDocs(docs []DocRecord) []DocRecord {
	docs = slices.Clone(docs)
	slices.SortFunc(docs, compareDocs)
	return slices.Compact(docs)
}

// DriftChange is a change of limits or documentation inside the window:
// the cell at which it was first observed, with its scheduled start, and
// the entries that cell's state removed and added against the cell
// before it.
type DriftChange struct {
	Cell     string      `json:"cell"`
	StartUTC time.Time   `json:"start_utc"`
	Removed  []DocRecord `json:"removed,omitempty"`
	Added    []DocRecord `json:"added,omitempty"`
}

// DriftPart is one side of a drift split: its cells in start order, the
// scheduled starts of the first and last, the documentation state they
// share, and their pooled figures per step.
type DriftPart struct {
	Cells         []string     `json:"cells"`
	FirstStartUTC time.Time    `json:"first_start_utc"`
	LastStartUTC  time.Time    `json:"last_start_utc"`
	Docs          []DocRecord  `json:"documentation"`
	Pooled        []PooledStep `json:"pooled"`
}

// part is one side of a split while the tables are built: indexes into
// the cell tables, in start order, and the state they share.
type part struct {
	cells []int
	docs  []DocRecord
}

// drift splits cells, in the order of the cell tables, at every change
// of documentation state between consecutive cells in start order, and
// records each change. Without a change it returns one part holding
// every cell in table order. Either every cell records a state or none
// does; a split needs every start set and distinct.
func (t *tables) drift(cells []CellRecords) ([]part, error) {
	docs := make([][]DocRecord, len(cells))
	all := make([]int, len(cells))
	same := true
	for i, c := range cells {
		if (len(c.Docs) == 0) != (len(cells[0].Docs) == 0) {
			return nil, fmt.Errorf("report: step tables: cell %q: a documentation state is recorded for some cells only", c.Cell)
		}
		docs[i], all[i] = sortDocs(c.Docs), i
		same = same && slices.Equal(docs[i], docs[0])
	}
	if same {
		p := part{cells: all}
		if len(docs) > 0 {
			p.docs = docs[0]
		}
		return []part{p}, nil
	}
	for i, c := range cells {
		if c.Start.IsZero() {
			return nil, fmt.Errorf("report: step tables: cell %q has no start, and the documentation state changes", c.Cell)
		}
		if i > 0 && slices.ContainsFunc(cells[:i], func(o CellRecords) bool { return o.Start.Equal(c.Start) }) {
			return nil, fmt.Errorf("report: step tables: cell %q starts with another cell, and the documentation state changes", c.Cell)
		}
	}
	slices.SortFunc(all, func(a, b int) int { return cells[a].Start.Compare(cells[b].Start) })
	parts := []part{{cells: []int{all[0]}, docs: docs[all[0]]}}
	for _, i := range all[1:] {
		p := &parts[len(parts)-1]
		if slices.Equal(docs[i], p.docs) {
			p.cells = append(p.cells, i)
			continue
		}
		t.DriftChanges = append(t.DriftChanges, DriftChange{Cell: cells[i].Cell, StartUTC: cells[i].Start.UTC(),
			Removed: docsLess(p.docs, docs[i]), Added: docsLess(docs[i], p.docs)})
		parts = append(parts, part{cells: []int{i}, docs: docs[i]})
	}
	return parts, nil
}

// docsLess is the entries of a that b lacks.
func docsLess(a, b []DocRecord) []DocRecord {
	var out []DocRecord
	for _, d := range a {
		if !slices.Contains(b, d) {
			out = append(out, d)
		}
	}
	return out
}
