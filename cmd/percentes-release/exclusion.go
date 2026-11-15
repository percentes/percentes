package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
)

// routeExclusion is the page of a route that ran no cell: one its host
// group's ceiling check excluded, or with Excluded false one it did not,
// at Ceiling; the route, its provider, model, request shape and permitted
// ceilings, the reason, and its host group's ceiling-check records.
type routeExclusion struct {
	Route    string            `json:"route"`
	Provider string            `json:"provider"`
	Model    string            `json:"model"`
	Shape    string            `json:"shape"`
	Ceilings []int             `json:"ceilings"`
	Excluded bool              `json:"excluded"`
	Ceiling  int               `json:"ceiling,omitempty"`
	Reason   string            `json:"reason"`
	Records  []exclusionRecord `json:"ceiling_check_records"`
	// copies are the records' bytes by their names.
	copies map[string][]byte
}

// exclusionRecord is one ceiling-check record an exclusion was read
// from: its file name as a cell carries it, SHA-256, route, ceiling,
// Shape S byte cap, admission line and outcome
// (probe.ReleaseRecord.Outcome); Superseded marks a record at a spent
// byte cap above the release's, which decides nothing.
type exclusionRecord struct {
	Name       string `json:"name"`
	SHA256     string `json:"sha256"`
	Route      string `json:"route"`
	Ceiling    int    `json:"ceiling"`
	ByteCap    int    `json:"byte_cap,omitempty"`
	LedgerSeq  int    `json:"ledger_seq"`
	Outcome    string `json:"outcome"`
	Superseded bool   `json:"superseded,omitempty"`
}

// notCharacterized starts the reason of an exclusion on a model fault.
const notCharacterized = "not characterized (SPEC-HOSTED §2): "

func (x *routeExclusion) text() string {
	var b strings.Builder
	head := fmt.Sprintf("Route %s (%s, model %s, Shape %s at %s)", x.Route, x.Provider, x.Model, x.Shape, ceilingWords(x.Ceilings))
	switch {
	case !x.Excluded:
		fmt.Fprintf(&b, "%s ran no cell. Its host group's ceiling check chose %s (%s).\n", head, number(x.Ceiling), x.Reason)
	case strings.HasPrefix(x.Reason, notCharacterized):
		fmt.Fprintf(&b, "%s is excluded from the release: %s.\nIt ran no cell.\n", head, x.Reason)
	default:
		fmt.Fprintf(&b, "%s is excluded from the release by the ceiling check (%s): %s.\nIt ran no cell.\n", head, exclusionRule(x.Shape, x.Reason), x.Reason)
	}
	b.WriteString("The ceiling-check records of its host group:\n")
	for _, r := range x.Records {
		at := "ceiling " + number(r.Ceiling)
		if r.ByteCap != 0 {
			at += ", byte cap " + number(r.ByteCap)
		}
		outcome := r.Outcome
		if r.Superseded {
			outcome += ", superseded"
		}
		fmt.Fprintf(&b, "- %s: route %s at %s, ledger line %d, %s, sha256 %s\n", r.Name, r.Route, at, r.LedgerSeq, outcome, r.SHA256)
	}
	return b.String()
}

// exclusionRule cites the SPEC-HOSTED rule that excludes a host group of
// shape for reason: HA4's Shape R part for Shape R, HA3 point 7
// otherwise, with HA4's byte caps part for an input-bound reason. Either
// shape adds HA4's refused checks part for an inconclusive reason.
func exclusionRule(shape, reason string) string {
	var ha4 []string
	if shape == config.ShapeR {
		ha4 = append(ha4, "Shape R")
	} else if strings.Contains(reason, "the input bound failed") {
		ha4 = append(ha4, "byte caps")
	}
	if strings.Contains(reason, "ceiling check inconclusive") {
		ha4 = append(ha4, "refused checks")
	}
	switch {
	case shape == config.ShapeR:
		return "SPEC-HOSTED HA4, " + strings.Join(ha4, " and ")
	case len(ha4) > 0:
		return "SPEC-HOSTED HA3 point 7 and HA4, " + strings.Join(ha4, " and ")
	}
	return "SPEC-HOSTED HA3 point 7"
}

// ceilingWords prints ceilings as a page does: "16", "512 and 1,024".
func ceilingWords(cs []int) string {
	words := make([]string, len(cs))
	for i, c := range cs {
		words[i] = number(c)
	}
	if n := len(words); n > 1 {
		return strings.Join(words[:n-1], ", ") + " and " + words[n-1]
	}
	return strings.Join(words, "")
}

// number prints n with a comma between each group of three digits.
func number(n int) string {
	return probe.Number(n)
}

// probeCopyName is the name a cell carries rec's copy under.
func probeCopyName(rec *probe.ReleaseRecord) string {
	return fmt.Sprintf("ceiling-probe-%s-%d-%d.json", rec.Route, rec.Ceiling.Ceiling, rec.LedgerSeq)
}

// excludedRoute is the page of cfg's route, which ran no cell, read from
// the ceiling-check records among checkPaths of its host group, and for
// Shape S of other routes that spend a byte cap (probe.Spent), as run
// reads them at byteCap (ceilingChoice), against the targets of cfgs,
// the ledger of open-line SHA-256 ledger and its lines frozen up to the
// schedule's position; a model fault in the route's own records
// excludes it (ceilingChoiceFor). A choice that does not exclude the
// host group gives a page stating the route ran no cell where unrun is
// set, and is refused otherwise.
func excludedRoute(cfg *config.Config, cfgs []*config.Config, checkPaths []string, byteCap int, ledger string, frozen []admission.Entry, unrun bool) (*routeExclusion, error) {
	name := cfg.Target.Name
	if cfg.Target.Naming == nil {
		return nil, fmt.Errorf("pages: route %s has no naming record", name)
	}
	hosts := hostGroup(cfg)
	files, own, err := choiceFiles(cfg, checkPaths)
	if err != nil {
		return nil, fmt.Errorf("pages: %w", err)
	}
	if own == 0 {
		return nil, fmt.Errorf("pages: route %s has no cell and no --check-record of its host group", name)
	}
	targets := map[string]*config.Target{}
	for _, c := range cfgs {
		if c.Target.Name != name && slices.Contains(hosts, c.Target.Name) {
			targets[c.Target.Name] = &c.Target
		}
	}
	if cfg.Release.Shape.Name != config.ShapeS {
		byteCap = 0
	}
	choice, copies, recs, err := ceilingChoiceFor(cfg, targets, files, byteCap, ledger, frozen, true)
	if err != nil {
		return nil, fmt.Errorf("pages: route %s: %w", name, err)
	}
	if !choice.Excluded && !unrun {
		return nil, fmt.Errorf("pages: route %s has no cell, and its ceiling-check records choose %d (%s), which excludes nothing", name, choice.Ceiling, choice.Reason)
	}
	x := &routeExclusion{Route: name, Provider: cfg.Target.Naming.Provider, Model: cfg.Target.ModelName, Shape: cfg.Release.Shape.Name,
		Ceilings: cfg.Release.Shape.Ceilings(), Excluded: choice.Excluded, Ceiling: choice.Ceiling, Reason: choice.Reason, Records: []exclusionRecord{},
		copies: map[string][]byte{}}
	for _, rec := range recs {
		n := probeCopyName(rec)
		x.copies[n] = copies[n]
		sum := sha256.Sum256(copies[n])
		x.Records = append(x.Records, exclusionRecord{Name: n, SHA256: hex.EncodeToString(sum[:]), Route: rec.Route, Ceiling: rec.Ceiling.Ceiling,
			ByteCap: rec.ByteCap, LedgerSeq: rec.LedgerSeq, Outcome: rec.Outcome(),
			Superseded: rec.Shape == config.ShapeS && rec.ByteCap != byteCap && rec.Outcome() != probe.CheckStopped})
	}
	slices.SortFunc(x.Records, func(a, b exclusionRecord) int { return a.LedgerSeq - b.LedgerSeq })
	return x, nil
}

// choiceFiles are the ceiling-check records among paths that cfg's
// host group's choice reads: its hosts', and for Shape S other routes'
// that spend a byte cap (probe.Spent); own counts its hosts'.
func choiceFiles(cfg *config.Config, paths []string) ([]string, int, error) {
	hosts := hostGroup(cfg)
	var files []string
	own := 0
	for _, p := range paths {
		rec, err := readCheckRecord(p)
		switch {
		case err != nil:
			return nil, 0, err
		case slices.Contains(hosts, rec.Route):
			files = append(files, p)
			own++
		case cfg.Release.Shape.Name == config.ShapeS && len(probe.Spent([]*probe.ReleaseRecord{rec})) > 0:
			files = append(files, p)
		}
	}
	return files, own, nil
}

// releaseCheckPaths are the paths of the ceiling-check records the
// manifests of recs carry and of those at checkPaths, a record given
// twice once.
func releaseCheckPaths(recs []*routeRecords, checkPaths []string) ([]string, error) {
	var paths []string
	seen := map[string]bool{}
	add := func(p string) error {
		b, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("pages: %w", err)
		}
		sum := sha256.Sum256(b)
		if k := hex.EncodeToString(sum[:]); !seen[k] {
			seen[k] = true
			paths = append(paths, p)
		}
		return nil
	}
	for _, rr := range recs {
		for _, ref := range rr.probes {
			if err := add(filepath.Join(rr.probeDir, ref.Name)); err != nil {
				return nil, err
			}
		}
	}
	for _, p := range checkPaths {
		if err := add(p); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

// cellAdmitted reports whether entries admit run's traffic of route
// (runAdmission).
func cellAdmitted(entries []admission.Entry, route string) bool {
	return slices.ContainsFunc(entries, func(e admission.Entry) bool { return e.Route == route && runAdmission(e) })
}

// runAdmission reports whether e admits run's traffic: a cell, window or
// replacement admission.
func runAdmission(e admission.Entry) bool {
	return e.Kind == admission.KindAdmit && (e.Purpose == admission.PurposeCell || e.Purpose == admission.PurposeWindow || e.Purpose == admission.PurposeReplacement)
}

// releaseByteCap is the Shape S ticket byte cap the records at paths
// decide at: the least byte cap of a Shape S record that was not
// stopped, or where every one was, of any Shape S record; 0 where there
// is none.
func releaseByteCap(paths []string) (int, error) {
	decided, all := 0, 0
	least := func(c, byteCap int) int {
		if c == 0 || byteCap < c {
			return byteCap
		}
		return c
	}
	for _, p := range paths {
		rec, err := readCheckRecord(p)
		if err != nil {
			return 0, err
		}
		if rec.Shape != config.ShapeS {
			continue
		}
		if all = least(all, rec.ByteCap); rec.Outcome() != probe.CheckStopped {
			decided = least(decided, rec.ByteCap)
		}
	}
	if decided == 0 {
		return all, nil
	}
	return decided, nil
}

// allChecks refuses unless the probe records the routes' manifests carry
// (recs) and the ceiling-check records at checkPaths hold one record of
// every ceiling-check admission among frozen of a route of routes
// (checkAdmitted). A record given twice is read once.
func allChecks(recs []*routeRecords, checkPaths []string, frozen []admission.Entry, routes []string) error {
	seen := map[string]bool{}
	var all []*probe.ReleaseRecord
	read := func(p string) error {
		rec, err := readCheckRecord(p)
		if err != nil {
			return fmt.Errorf("pages: %w", err)
		}
		b, err := probe.Encode(rec)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		if k := hex.EncodeToString(sum[:]); !seen[k] {
			seen[k] = true
			all = append(all, rec)
		}
		return nil
	}
	for _, rr := range recs {
		for _, ref := range rr.probes {
			if err := read(filepath.Join(rr.probeDir, ref.Name)); err != nil {
				return err
			}
		}
	}
	for _, p := range checkPaths {
		if err := read(p); err != nil {
			return err
		}
	}
	if err := checkAdmitted(frozen, routes, all); err != nil {
		return fmt.Errorf("pages: %w", err)
	}
	return nil
}
