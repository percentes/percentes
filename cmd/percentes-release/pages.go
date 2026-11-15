package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// The pages' files under --pages: the index page and its JSON, and each
// route's page and its JSON under routesDir.
const (
	indexPage = "index.md"
	indexJSON = "index.json"
	routesDir = "routes"
)

// routeRecords are one route's cells as the pages read them: the
// ceiling rule's verdict, the ceiling choice and the probe records every
// manifest carries, the counted cells' records, the attempts outside the
// frame, the model_mismatch attempts' identifiers, the cells a billing
// refusal stopped, with a manifest or without one, and the route's
// window-close record, nil when it has none. The verdict is nil when no
// attempt has a manifest. outside are the records of the attempts outside
// the frame whose ladder started and whose records are complete, and
// outsideIncomplete counts those whose ladder started with records
// incomplete.
type routeRecords struct {
	// schedule is the schedule SHA-256 the route's cells carry.
	schedule   string
	verdict    *admission.Verdict
	ceiling    probe.CeilingChoice
	probes     []evidence.FileRef
	cells      []report.CellRecords
	outside    []report.CellRecords
	mismatches []report.ModelMismatch
	billing    []report.BillingStop
	close      *evidence.WindowClose
	// probeDir is a cell directory holding the probe records; excluded
	// is the page of a scheduled route with no cell (excludedRoute).
	probeDir string
	excluded *routeExclusion
	// labelled counts the attempts with a manifest that do not count;
	// unrecorded the missed and interrupted attempts with no manifest,
	// which keep their run record only; stops are the stopped attempts
	// with an execution error and no manifest, in start order.
	labelled, unrecorded int
	outsideIncomplete    int
	stops                []*cellRecord
}

// pagesCmd writes the release's index page and each route's page from
// the cells' records under --out (SPEC-HOSTED HA3 point 12), each
// status_429 labelled from its account group's history (readHistory)
// read with the ledger the schedule froze. Every route with a cell under
// --out must be given, and every cell must have run under the schedule
// file given; nothing is written unless every route's cells read. A
// route whose attempts are all missed or interrupted gets no page; one
// with no manifest and a stopped attempt gets a page and an index line
// stating each stop and its reasons, and its host group's ceiling-check
// records are copied under --out. A route page states each stopped
// attempt with no manifest. A scheduled route with no cell gets a page
// and an index line stating its exclusion by the ceiling check, or that
// it ran no cell, and its host group's ceiling-check records are copied
// under --out (buildPages).
func pagesCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("pages", flag.ContinueOnError)
	out := fs.String("out", "", "the release output directory the route processes write under (required)")
	framesRoot := fs.String("frames", "", "the raw-frame store directory the route processes write under (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration, the file its cells ran (repeatable, required)")
	pages := fs.String("pages", "", "directory to write the index page and the route pages in (required)")
	schedulePath := fs.String("schedule", "", "the committed schedule file the cells ran under (required)")
	keyPath := fs.String("release-key", "", "the release key file (required where a route has a confidential limit)")
	ledgerPath := fs.String("ledger", "", "the release admission ledger the schedule froze, as the routes left it (required)")
	var checkPaths listFlag
	fs.Var(&checkPaths, "check-record", checkRecordUsage)
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for name, v := range map[string]string{"--out": *out, "--frames": *framesRoot, "--pages": *pages, "--schedule": *schedulePath, "--ledger": *ledgerPath} {
		if v == "" {
			return exitError, fmt.Errorf("pages: %s is required", name)
		}
	}
	cfgs, err := loadReleases(cfgPaths)
	if err != nil {
		return exitError, err
	}
	key, err := checkingKey(*keyPath, cfgs)
	if err != nil {
		return exitError, fmt.Errorf("pages: %w", err)
	}
	sched, sum, err := loadSchedule(*schedulePath)
	if err != nil {
		return exitError, fmt.Errorf("pages: %w", err)
	}
	rp, err := buildPages(*out, *framesRoot, sched, sum, *ledgerPath, checkPaths, cfgs, key, "")
	if err != nil {
		return exitError, err
	}
	for name, b := range rp.files {
		if err := replaceFile(filepath.Join(*pages, filepath.FromSlash(name)), b); err != nil {
			return exitError, err
		}
	}
	for name, b := range rp.out {
		if err := replaceFile(filepath.Join(*out, filepath.FromSlash(name)), b); err != nil {
			return exitError, err
		}
	}
	fmt.Printf("index written: %s; %d rows\n", filepath.Join(*pages, indexPage), len(rp.index.Rows))
	for i, r := range rp.routes {
		if x := rp.recs[i].excluded; x != nil {
			page := filepath.Join(*pages, filepath.FromSlash(r.Page))
			if x.Excluded {
				fmt.Printf("%s: excluded by the ceiling check: %s; page %s\n", r.Config.Target.Name, x.Reason, page)
			} else {
				fmt.Printf("%s: ran no cell; the ceiling check chose %s (%s); page %s\n", r.Config.Target.Name, number(x.Ceiling), x.Reason, page)
			}
			continue
		}
		rr := rp.recs[i]
		if rr.verdict == nil && len(rr.stops) > 0 {
			fmt.Printf("%s: no cell with a %s; %d stopped and %d missed or interrupted with a run record only; page %s\n", r.Config.Target.Name, evidence.ManifestFile,
				len(rr.stops), rr.unrecorded, filepath.Join(*pages, filepath.FromSlash(r.Page)))
			continue
		}
		if rr.verdict == nil {
			fmt.Printf("%s: no cell with a %s, %d with a run record only; no page\n", r.Config.Target.Name, evidence.ManifestFile, rr.unrecorded)
			continue
		}
		fmt.Printf("%s: %d of %d cells counted, %d labelled, %d stopped and %d missed or interrupted with a run record only; page %s\n", r.Config.Target.Name,
			len(r.Cells), admission.ReleaseCells, rr.labelled, len(rr.stops), rr.unrecorded, filepath.Join(*pages, filepath.FromSlash(r.Page)))
	}
	return exitOK, nil
}

// releasePages are the pages of a release, by slash path under --pages,
// with the index and each given route's cells, in the order given, and
// the ceiling-check records of the routes with no cell, by slash path
// under --out.
type releasePages struct {
	files  map[string][]byte
	out    map[string][]byte
	index  *report.Index
	routes []report.IndexRoute
	recs   []*routeRecords
}

// checkRecordUsage is the --check-record flag's usage line.
const checkRecordUsage = "a ceiling-check record sent on the ledger the schedule froze that no cell carries, such as an excluded route's (repeatable)"

// buildPages builds the index page and each route's page from the cells
// of cfgs' routes under out, run under sched, the schedule file of
// SHA-256 sum, each route page's status_429 responses labelled from its
// account group's history, the ledger at ledgerPath, which must be the
// one sched froze, and the ceiling-check records at checkPaths that no
// cell carries. Every route of sched with a ceiling-check admission on
// the ledger up to the line sched froze must be among cfgs, as an
// excluded route is, every route with a cell under out must be too, every
// cell must carry sum, and the routes of one host group among them must
// send one request shape and carry one ceiling choice and one set of
// probe records. A route with no manifest and a stopped attempt gets a
// page and an index line stating each stop and its reasons
// (stoppedRoute), and its host group's ceiling-check records, which
// checkPaths must hold (unnamedChecks), copied under out at <route>/
// (stoppedChecks); a route page also states each stopped attempt with no
// manifest. Any other route with no manifest has no page and no index
// row, unless it is scheduled and has no cell: its ceiling-check records
// among checkPaths, read as run reads them at the byte cap the records
// decide at (excludedRoute, releaseByteCap), give it a page and an index
// line stating its exclusion and the reason, or, where the ledger admits
// no cell of the route after the line sched froze, that it ran no cell;
// each record's copy goes under out at <route>/ by the name its page
// prints. The records the manifests carry and those at checkPaths must
// hold one record of every ceiling-check admission on the ledger up to
// the line sched froze (checkAdmitted), and no ceiling check may be
// admitted on a route's account group after it (lateChecks).
// cellFiles is unused until route pages name cell archives.
func buildPages(out, framesRoot string, sched *schedule.Schedule, sum, ledgerPath string, checkPaths []string, cfgs []*config.Config, key *releasekey.Key, cellFiles string) (*releasePages, error) {
	given := map[string]bool{}
	for _, cfg := range cfgs {
		given[cfg.Target.Name] = true
	}
	rp := &releasePages{routes: make([]report.IndexRoute, len(cfgs)), recs: make([]*routeRecords, len(cfgs))}
	var paged []report.IndexRoute
	for i, cfg := range cfgs {
		cells, err := filepath.Glob(filepath.Join(out, cfg.Target.Name, "*", cellFile))
		if err != nil {
			return nil, err
		}
		if len(cells) == 0 && slices.Contains(sched.Input.Routes, cfg.Target.Name) {
			rp.recs[i] = &routeRecords{}
			rp.routes[i] = report.IndexRoute{Config: cfg, Page: path.Join(routesDir, cfg.Target.Name+".txt")}
			continue
		}
		if rp.recs[i], err = readRouteCells(out, framesRoot, cfg, key); err != nil {
			return nil, err
		}
		if s := rp.recs[i].schedule; s != sum {
			return nil, fmt.Errorf("pages: route %s: the cells ran under schedule sha256 %q; --schedule has sha256 %s", cfg.Target.Name, s, sum)
		}
		rp.routes[i] = report.IndexRoute{Config: cfg, Verdict: rp.recs[i].verdict, Cells: rp.recs[i].cells}
		if rp.recs[i].verdict != nil {
			rp.routes[i].Page = path.Join(routesDir, cfg.Target.Name+".txt")
			paged = append(paged, rp.routes[i])
		}
	}
	cells, err := filepath.Glob(filepath.Join(out, "*", "*", cellFile))
	if err != nil {
		return nil, err
	}
	for _, p := range cells {
		if name := filepath.Base(filepath.Dir(filepath.Dir(p))); !given[name] {
			return nil, fmt.Errorf("pages: route %s has cells under %s and no --config", name, out)
		}
	}
	if err := sameCeiling(cfgs, rp.recs); err != nil {
		return nil, err
	}
	entries, read, err := readLedger(ledgerPath, sched.Input.LedgerSHA256)
	if err != nil {
		return nil, fmt.Errorf("pages: %w", err)
	}
	frozen, err := frozenEntries(entries, read.HeadSHA256, sched.Input)
	if err != nil {
		return nil, fmt.Errorf("pages: %w", err)
	}
	for _, name := range sched.Input.Routes {
		if checks := probeAdmissions(frozen, []string{name}); !given[name] && len(checks) > 0 {
			return nil, fmt.Errorf("pages: route %s of the schedule, whose ceiling check ledger line %d admits, has no --config", name, checks[0].Seq)
		}
	}
	if err := checkRecords(checkPaths, sched); err != nil {
		return nil, err
	}
	for _, c := range sched.Input.Configs {
		if err := lateChecks(entries, len(frozen), c.AccountGroup, groupRoutes(sched, c.Route, c.AccountGroup)); err != nil {
			return nil, fmt.Errorf("pages: %w", err)
		}
	}
	paths, err := releaseCheckPaths(rp.recs, checkPaths)
	if err != nil {
		return nil, err
	}
	byteCap, err := releaseByteCap(paths)
	if err != nil {
		return nil, fmt.Errorf("pages: %w", err)
	}
	if err := unnamedChecks(rp, paths, frozen); err != nil {
		return nil, err
	}
	for i, r := range rp.routes {
		if r.Verdict == nil && r.Page != "" {
			unrun := !cellAdmitted(entries[len(frozen):], r.Config.Target.Name)
			if rp.recs[i].excluded, err = excludedRoute(r.Config, cfgs, paths, byteCap, sched.Input.LedgerSHA256, frozen, unrun); err != nil {
				return nil, err
			}
		}
	}
	if err := allChecks(rp.recs, checkPaths, frozen, sched.Input.Routes); err != nil {
		return nil, err
	}
	if len(paged) == 0 && slices.ContainsFunc(rp.recs, func(r *routeRecords) bool { return r.excluded != nil || r.verdict == nil && len(r.stops) > 0 }) {
		rp.index = &report.Index{Rows: []report.IndexRow{}}
	} else if rp.index, err = report.NewIndex(paged); err != nil {
		return nil, err
	}
	rp.files, rp.out = map[string][]byte{}, map[string][]byte{}
	for i, r := range rp.routes {
		x := rp.recs[i].excluded
		if x == nil {
			continue
		}
		t := r.Config.Target
		e := report.IndexExclusion{Provider: t.Naming.Provider, Route: t.Name, Model: t.ModelName, Shape: x.Shape, Reason: x.Reason, Page: r.Page}
		add := rp.index.Exclude
		if !x.Excluded {
			e.Reason, add = fmt.Sprintf("the ceiling check chose %s (%s)", number(x.Ceiling), x.Reason), rp.index.AddUnrun
		}
		if err := add(e); err != nil {
			return nil, fmt.Errorf("pages: %w", err)
		}
		for name, b := range x.copies {
			rp.out[path.Join(t.Name, name)] = b
		}
		rp.files[r.Page] = []byte(x.text())
		if rp.files[path.Join(routesDir, t.Name+".json")], err = marshalPage(x); err != nil {
			return nil, err
		}
	}
	for i, r := range rp.routes {
		rr := rp.recs[i]
		if rr.verdict != nil || len(rr.stops) == 0 {
			continue
		}
		rp.routes[i].Page = path.Join(routesDir, r.Config.Target.Name+".txt")
		if err := stoppedRoute(rp, rp.routes[i], rr.stops); err != nil {
			return nil, err
		}
		if err := stoppedChecks(rp, r.Config, cfgs, paths, byteCap, sched.Input.LedgerSHA256, frozen); err != nil {
			return nil, err
		}
	}
	rp.files[indexPage] = []byte(rp.index.Markdown())
	if rp.files[indexJSON], err = marshalPage(rp.index); err != nil {
		return nil, err
	}
	members := groupMembers(cfgs, sched)
	histories := map[string]*report.History{}
	for i, r := range rp.routes {
		if rp.recs[i].verdict == nil {
			continue
		}
		st, err := report.NewStepTables(r.Config, r.Verdict, r.Cells)
		if err != nil {
			return nil, err
		}
		g := r.Config.Release.AccountGroup
		if histories[g] == nil {
			if histories[g], err = readHistory(out, g, members[g], cfgs, entries, checkPaths); err != nil {
				return nil, err
			}
		}
		for _, c := range rp.recs[i].outside {
			if err := st.AddOutside(r.Config, r.Verdict, c); err != nil {
				return nil, fmt.Errorf("pages: route %s: %w", r.Config.Target.Name, err)
			}
		}
		st.OutsideIncomplete = rp.recs[i].outsideIncomplete
		if err := st.Attribute(r.Config, r.Verdict, histories[g]); err != nil {
			return nil, fmt.Errorf("pages: route %s: %w", r.Config.Target.Name, err)
		}
		st.BillingStops = rp.recs[i].billing
		if c := rp.recs[i].close; c != nil {
			if st.WindowClose, err = report.CompareWindowClose(r.Config.Target.Name, r.Cells, c); err != nil {
				return nil, fmt.Errorf("pages: %w", err)
			}
		}
		st.ModelMismatches = rp.recs[i].mismatches
		page := &pagedRoute{StepTables: st, Stops: stoppedCells(rp.recs[i].stops)}
		rp.files[r.Page] = []byte(page.text())
		if rp.files[path.Join(routesDir, r.Config.Target.Name+".json")], err = marshalPage(page); err != nil {
			return nil, err
		}
	}
	return rp, nil
}

// sameCeiling refuses routes of one host group, each the other's host,
// that send different request shapes or permitted ceilings (sameShape),
// or whose manifests carry different ceiling choices or probe records.
// Where either route has no manifest, their ceilings (ranAt) must agree.
func sameCeiling(cfgs []*config.Config, recs []*routeRecords) error {
	for i, cfg := range cfgs {
		for _, h := range cfg.Release.OtherHosts {
			j := slices.IndexFunc(cfgs, func(c *config.Config) bool { return c.Target.Name == h.Route })
			switch {
			case j < 0:
			case !sameShape(cfg, cfgs[j]):
				return fmt.Errorf("pages: routes %s and %s, hosts of one host group, send Shape %s at ceilings %v and Shape %s at %v",
					cfg.Target.Name, h.Route, cfg.Release.Shape.Name, cfg.Release.Shape.Ceilings(), cfgs[j].Release.Shape.Name, cfgs[j].Release.Shape.Ceilings())
			case recs[i].verdict == nil || recs[j].verdict == nil:
				if a, b := ranAt(cfg, recs[i]), ranAt(cfgs[j], recs[j]); a != b {
					return fmt.Errorf("pages: routes %s and %s, hosts of one host group, ran at ceilings %d and %d", cfg.Target.Name, h.Route, a, b)
				}
			case !reflect.DeepEqual(recs[i].ceiling, recs[j].ceiling):
				return fmt.Errorf("pages: routes %s and %s, hosts of one host group, ran at different ceiling choices: %+v and %+v",
					cfg.Target.Name, h.Route, recs[i].ceiling, recs[j].ceiling)
			case !slices.Equal(recs[i].probes, recs[j].probes):
				return fmt.Errorf("pages: routes %s and %s, hosts of one host group, carry different ceiling probe records", cfg.Target.Name, h.Route)
			}
		}
	}
	return nil
}

// ranAt is the ceiling route cfg ran at: its manifests' choice, or its
// configuration's release.shape.ceiling_tokens where it has no manifest.
func ranAt(cfg *config.Config, rr *routeRecords) int {
	if rr.verdict == nil {
		return cfg.Release.Shape.CeilingTokens
	}
	return rr.ceiling.Ceiling
}

func marshalPage(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// readRouteCells reads cfg's route's cells under out. Every attempt with
// a manifest must pass the completeness check against cfg's file, the
// frame store and the release key, and every manifest must carry the
// same verdict, ceiling choice and probe records. Every run record must
// carry the same schedule SHA-256, and every manifest its run record's.
// An attempt enters the route's frame when its run record and its
// manifest both count it, at most one attempt per scheduled cell; only a
// missed or interrupted attempt, or a stopped one with an execution error,
// may have its run record alone, and a stopped one's billing refusals
// come from that record (billingStop). Each counted
// cell carries its scheduled start and the documentation state its
// manifest records, which the drift rule compares, and the limits read at
// its start, from the refresh record its manifest names. An attempt
// outside the frame whose ladder started and whose records are complete
// is read the same way for its status_429 labels. Each model_mismatch
// attempt's expected and reported model ids are kept for the route's
// page. The route's window-close record is read where it has one and
// must verify against the latest end of any of the route's attempts
// (evidence.WindowClose.Verify). A route with no cell record is refused.
func readRouteCells(out, framesRoot string, cfg *config.Config, key *releasekey.Key) (*routeRecords, error) {
	name := cfg.Target.Name
	paths, err := filepath.Glob(filepath.Join(out, name, "*", cellFile))
	if err != nil {
		return nil, err
	}
	rr := &routeRecords{}
	counted := map[string]string{}
	var lastEnd time.Time
	for i, p := range paths {
		dir := filepath.Dir(p)
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var rec cellRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("pages: %s: %w", p, err)
		}
		switch {
		case rec.Outcome == outcomeRunning:
			return nil, fmt.Errorf("pages: %s: the cell is running", dir)
		case rec.Run.Route != name || filepath.Base(dir) != cellLabel(rec.Run):
			return nil, fmt.Errorf("pages: %s: the run record is route %s, %s", dir, rec.Run.Route, cellLabel(rec.Run))
		case i == 0:
			rr.schedule = rec.ScheduleSHA256
		case rec.ScheduleSHA256 != rr.schedule:
			return nil, fmt.Errorf("pages: %s: the cell ran under schedule sha256 %q, the route's other cells under %q", dir, rec.ScheduleSHA256, rr.schedule)
		}
		end, err := cellEnd(dir, &rec)
		if err != nil {
			return nil, err
		}
		if end.After(lastEnd) {
			lastEnd = end
		}
		if _, err := os.Stat(filepath.Join(dir, evidence.ManifestFile)); errors.Is(err, os.ErrNotExist) {
			switch {
			case !rec.unlabelled():
				return nil, fmt.Errorf("pages: %s: %s with no %s", dir, rec.Outcome, evidence.ManifestFile)
			case rec.Outcome == outcomeStopped:
				rr.stops = append(rr.stops, &rec)
				if rec.BillingRefusals > 0 {
					rr.billing = append(rr.billing, billingStop(&rec, rec.BillingRefusals))
				}
			default:
				rr.unrecorded++
			}
			continue
		}
		m, err := evidence.CheckRelease(evidence.Release{Dir: dir, Config: cfg.Raw, FramesRoot: framesRoot, Pair: [2]string{report.JSONFile, report.TextFile}, Key: key})
		if err != nil {
			return nil, fmt.Errorf("pages: %s: %w", dir, err)
		}
		switch {
		case !sameRun(m.Release.Run, rec.Run):
			return nil, fmt.Errorf("pages: %s: the manifest is %s, the run record %s", dir, cellLabel(m.Release.Run), cellLabel(rec.Run))
		case m.Release.ScheduleSHA256 != rec.ScheduleSHA256:
			return nil, fmt.Errorf("pages: %s: the manifest's schedule sha256 %q, the run record's %q", dir, m.Release.ScheduleSHA256, rec.ScheduleSHA256)
		}
		switch {
		case rr.verdict == nil:
			rr.verdict, rr.ceiling, rr.probes, rr.probeDir = m.Release.Admission, m.Release.Ceiling, m.Release.Probes, dir
		case !reflect.DeepEqual(rr.verdict, m.Release.Admission):
			return nil, fmt.Errorf("pages: %s: the ceiling-rule verdict differs from the route's other cells'", dir)
		case !reflect.DeepEqual(rr.ceiling, m.Release.Ceiling) || !slices.Equal(rr.probes, m.Release.Probes):
			return nil, fmt.Errorf("pages: %s: the ceiling choice or its probe records differ from the route's other cells'", dir)
		}
		if n := m.Release.BillingRefusals; n > 0 {
			rr.billing = append(rr.billing, billingStop(&rec, n))
		}
		switch {
		case rec.Outcome == outcomeCounted && !m.Counted():
			return nil, fmt.Errorf("pages: %s: counted in the run record, %s in the manifest", dir, m.Release.Outcome)
		case rec.Outcome != outcomeCounted || !m.Counted():
			rr.labelled++
			if e := m.Endpoint; m.Release.Outcome == evidence.CellModelMismatch {
				rr.mismatches = append(rr.mismatches, report.ModelMismatch{Cell: cellLabel(rec.Run), Expected: e.ExpectedModel,
					Reported: e.ReportedModels, Requests: e.ModelMismatches})
			}
			switch {
			case !m.Release.LadderStarted:
				continue
			case !m.Release.RecordsComplete:
				rr.outsideIncomplete++
				continue
			}
		default:
			if prev, ok := counted[rec.Run.Label]; ok {
				return nil, fmt.Errorf("pages: cell %s of route %s counted twice: %s and %s", rec.Run.Label, name, prev, dir)
			}
			counted[rec.Run.Label] = dir
		}
		recs, err := readAttempts(dir, m)
		if err != nil {
			return nil, err
		}
		refreshed, err := readRefreshed(dir, m)
		if err != nil {
			return nil, err
		}
		epoch := m.StartUTC
		if rec.EpochUTC != nil {
			epoch = *rec.EpochUTC
		}
		c := report.CellRecords{Cell: cellLabel(rec.Run), Outcome: m.Release.Outcome, Gaps: m.Release.Gaps, Records: recs,
			Start: m.Release.Run.Start, Docs: report.DocsOf(m), EpochUTC: epoch, ClockAbsOffsetS: m.Clock.AbsOffsetS, Refreshed: refreshed}
		if rec.Outcome == outcomeCounted && m.Counted() {
			rr.cells = append(rr.cells, c)
		} else {
			c.Outcome = rec.Outcome
			rr.outside = append(rr.outside, c)
		}
	}
	if rr.verdict == nil && rr.unrecorded == 0 && len(rr.stops) == 0 {
		return nil, fmt.Errorf("pages: route %s has no cell with a %s under %s", name, evidence.ManifestFile, out)
	}
	slices.SortFunc(rr.stops, func(a, b *cellRecord) int { return a.Run.Start.Compare(b.Run.Start) })
	if p := filepath.Join(out, name, evidence.WindowCloseFile); fileExists(p) {
		if rr.close, err = evidence.ReadWindowClose(p); err != nil {
			return nil, fmt.Errorf("pages: %w", err)
		}
		if err := rr.close.Verify(&cfg.Target, framesRoot, key, lastEnd); err != nil {
			return nil, fmt.Errorf("pages: route %s: %w", name, err)
		}
	}
	return rr, nil
}

// billingStop is rec's billing stop of n responses, with its reasons
// that name the operator's billing setting.
func billingStop(rec *cellRecord, n int) report.BillingStop {
	bs := report.BillingStop{Cell: cellLabel(rec.Run), Responses: n, Reasons: []string{}}
	for _, reason := range rec.Reasons {
		if strings.Contains(reason, billingSetting) {
			bs.Reasons = append(bs.Reasons, reason)
		}
	}
	return bs
}

// stoppedPage is the page of a route with no manifest and a stopped
// attempt: each stopped attempt's reasons, billing refusals and
// execution error, from its run record.
type stoppedPage struct {
	Route string        `json:"route"`
	Stops []stoppedCell `json:"stopped_cells"`
}

// stoppedCell is a stopped attempt with no manifest as a page states it.
// Error is stoppedError; the run record keeps the execution error.
type stoppedCell struct {
	Cell            string   `json:"cell"`
	Reasons         []string `json:"reasons"`
	BillingRefusals int      `json:"billing_refusals,omitempty"`
	Error           string   `json:"error"`
}

// stoppedError is a page's execution error of a stopped attempt with no
// manifest.
const stoppedError = "its evidence was not written"

// stoppedCells are stops as a page states them.
func stoppedCells(stops []*cellRecord) []stoppedCell {
	var out []stoppedCell
	for _, rec := range stops {
		out = append(out, stoppedCell{Cell: cellLabel(rec.Run), Reasons: rec.Reasons, BillingRefusals: rec.BillingRefusals, Error: stoppedError})
	}
	return out
}

func (c stoppedCell) line() string {
	return fmt.Sprintf("- %s stopped: %s; billing refusals: %d; execution error: %s\n", c.Cell, strings.Join(c.Reasons, "; "), c.BillingRefusals, c.Error)
}

func (p *stoppedPage) text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: no cell with a %s; stopped cells, from their run records:\n", p.Route, evidence.ManifestFile)
	for _, c := range p.Stops {
		b.WriteString(c.line())
	}
	return b.String()
}

// pagedRoute is a route page's JSON: its step tables and the stopped
// attempts with no manifest.
type pagedRoute struct {
	*report.StepTables
	Stops []stoppedCell `json:"stopped_no_manifest,omitempty"`
}

func (p *pagedRoute) text() string {
	s := p.StepTables.Text()
	if len(p.Stops) == 0 {
		return s
	}
	var b strings.Builder
	b.WriteString(s)
	if !strings.HasSuffix(s, "\n\n") {
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "-- Stopped cells with no %s, from their run records --\n", evidence.ManifestFile)
	for _, c := range p.Stops {
		b.WriteString(c.line())
	}
	return b.String()
}

// stoppedRoute adds r, a route with no manifest and the stopped attempts
// stops, to rp: its page, and an index line stating each stop and its
// reasons, or that the reasons are on the page where the index refuses
// them.
func stoppedRoute(rp *releasePages, r report.IndexRoute, stops []*cellRecord) error {
	t := r.Config.Target
	p := &stoppedPage{Route: t.Name, Stops: stoppedCells(stops)}
	var lines []string
	for _, rec := range stops {
		lines = append(lines, fmt.Sprintf("%s stopped: %s", cellLabel(rec.Run), strings.Join(rec.Reasons, "; ")))
	}
	e := report.IndexExclusion{Provider: t.Naming.Provider, Route: t.Name, Model: t.ModelName, Shape: r.Config.Release.Shape.Name, Page: r.Page,
		Reason: "no cell manifest; " + strings.Join(lines, "; ")}
	if err := rp.index.AddStopped(e); err != nil {
		e.Reason = "no cell manifest; the stopped cells and their reasons are on the route page"
		if err := rp.index.AddStopped(e); err != nil {
			return fmt.Errorf("pages: %w", err)
		}
	}
	rp.files[r.Page] = []byte(p.text())
	b, err := marshalPage(p)
	if err != nil {
		return err
	}
	rp.files[path.Join(routesDir, t.Name+".json")] = b
	return nil
}

// cellEnd is when the cell in dir, with run record rec, last ran: its
// end_utc, or for an interrupted cell with none the latest time its
// records carry (recordsEnd). Any other cell with no end_utc ends at its
// start_utc.
func cellEnd(dir string, rec *cellRecord) (time.Time, error) {
	if rec.EndUTC != nil {
		return *rec.EndUTC, nil
	}
	if rec.Outcome != outcomeInterrupted {
		return rec.StartUTC, nil
	}
	end, err := recordsEnd(dir, rec)
	if err != nil {
		return time.Time{}, fmt.Errorf("pages: %w", err)
	}
	return end, nil
}

// recordsEnd is the latest time the records of the cell in dir, with run
// record rec, carry: start_utc, the dispatch series (to the end of each
// second it counts a request in), the clock, status, balance and refresh
// records (a record that does not decode gives its file's modification
// time), and each attempt's times from epoch_utc.
func recordsEnd(dir string, rec *cellRecord) (time.Time, error) {
	end := rec.StartUTC
	later := func(t time.Time) {
		if t.After(end) {
			end = t
		}
	}
	read := func(name string) ([]byte, error) {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return b, err
	}
	fail := func(name string, err error) (time.Time, error) {
		return time.Time{}, fmt.Errorf("%s: %s: %w", dir, name, err)
	}
	if b, err := read(dispatchFile); err != nil {
		return fail(dispatchFile, err)
	} else if b != nil {
		var s dispatchSeries
		if err := json.Unmarshal(b, &s); err != nil {
			return fail(dispatchFile, err)
		}
		later(s.StartUTC)
		if s.ThroughUTC != nil {
			later(*s.ThroughUTC)
		}
		for k, n := range s.Counts {
			if n > 0 {
				later(s.StartUTC.Add(time.Duration(k+1) * time.Second))
			}
		}
	}
	for _, name := range []string{evidence.ClockFile, evidence.StatusStartFile, evidence.StatusEndFile, evidence.BalanceStartFile, evidence.BalanceEndFile, evidence.RefreshFile} {
		b, err := read(name)
		if err != nil {
			return fail(name, err)
		}
		if b == nil {
			continue
		}
		var r struct {
			StartUTC time.Time `json:"start_utc"`
			EndUTC   time.Time `json:"end_utc"`
		}
		if json.Unmarshal(b, &r) != nil {
			fi, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				return fail(name, err)
			}
			later(fi.ModTime().UTC())
			continue
		}
		later(r.StartUTC)
		later(r.EndUTC)
	}
	b, err := read(evidence.AttemptsFile)
	if err != nil {
		return fail(evidence.AttemptsFile, err)
	}
	if b != nil && rec.EpochUTC != nil {
		recs, err := evidence.ReadRecords(bytes.NewReader(b))
		if err != nil {
			return fail(evidence.AttemptsFile, err)
		}
		for _, a := range recs {
			later(rec.EpochUTC.Add(time.Duration(max(a.DispatchNs, a.FirstTokNs, a.DoneNs))))
		}
	}
	return end, nil
}

// loadSchedule loads the committed schedule file at path, with its
// SHA-256.
func loadSchedule(path string) (*schedule.Schedule, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("schedule: %w", err)
	}
	s, err := schedule.Load(raw)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return s, hex.EncodeToString(sum[:]), nil
}

// readAttempts reads the attempts file in dir when it is the file m
// names.
func readAttempts(dir string, m *evidence.Manifest) ([]evidence.Record, error) {
	ref := m.Artifact(evidence.AttemptsFile)
	b, err := os.ReadFile(filepath.Join(dir, evidence.AttemptsFile))
	if err != nil {
		return nil, fmt.Errorf("pages: %w", err)
	}
	if sum := sha256.Sum256(b); ref == nil || hex.EncodeToString(sum[:]) != ref.SHA256 {
		return nil, fmt.Errorf("pages: %s: %s is not the file the manifest names", dir, evidence.AttemptsFile)
	}
	return evidence.ReadRecords(bytes.NewReader(b))
}

// readRefreshed reads the limits of the refresh record in dir when it is
// the file m names; nil where the refresh command read none.
func readRefreshed(dir string, m *evidence.Manifest) ([]evidence.RefreshedLimit, error) {
	ref := m.Artifact(evidence.RefreshFile)
	b, err := os.ReadFile(filepath.Join(dir, evidence.RefreshFile))
	if err != nil {
		return nil, fmt.Errorf("pages: %w", err)
	}
	if sum := sha256.Sum256(b); ref == nil || hex.EncodeToString(sum[:]) != ref.SHA256 {
		return nil, fmt.Errorf("pages: %s: %s is not the file the manifest names", dir, evidence.RefreshFile)
	}
	var rec evidence.StatusRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("pages: %s: %s: %w", dir, evidence.RefreshFile, err)
	}
	return rec.Limits, nil
}

// cellLabel is a run's directory name under its route's.
func cellLabel(run schedule.Run) string {
	return fmt.Sprintf("%s-a%d", run.Label, run.Attempt)
}

func sameRun(a, b schedule.Run) bool {
	return a.Route == b.Route && a.Label == b.Label && a.Attempt == b.Attempt && a.Date == b.Date && a.Start.Equal(b.Start) && a.Seed == b.Seed
}

// unnamedChecks refuses a route of rp with no manifest and a stopped
// attempt while a ceiling-check admission of its host group among frozen
// has no record among paths, naming the admissions' ledger lines.
func unnamedChecks(rp *releasePages, paths []string, frozen []admission.Entry) error {
	named := map[int]bool{}
	for _, p := range paths {
		rec, err := readCheckRecord(p)
		if err != nil {
			return fmt.Errorf("pages: %w", err)
		}
		named[rec.LedgerSeq] = true
	}
	for i, r := range rp.routes {
		if rr := rp.recs[i]; rr.verdict != nil || len(rr.stops) == 0 {
			continue
		}
		var lines []string
		for _, e := range probeAdmissions(frozen, hostGroup(r.Config)) {
			if !named[e.Seq] {
				lines = append(lines, fmt.Sprint(e.Seq))
			}
		}
		which := "the record of ledger line "
		if len(lines) > 1 {
			which = "the records of ledger lines "
		}
		if len(lines) > 0 {
			return fmt.Errorf("pages: route %s has no cell manifest to carry its host group's ceiling-check records; give with --check-record %s%s",
				r.Config.Target.Name, which, strings.Join(lines, ", "))
		}
	}
	return nil
}

// stoppedChecks copies the ceiling-check records among paths that cfg's
// host group's choice reads, for a route with no manifest and a stopped
// attempt, under rp.out at <route>/ by the names a cell gives them, read
// as run reads them at byteCap against the targets of cfgs, the ledger
// of open-line SHA-256 ledger and its lines frozen up to the schedule's
// position (ceilingChoiceFor).
func stoppedChecks(rp *releasePages, cfg *config.Config, cfgs []*config.Config, paths []string, byteCap int, ledger string, frozen []admission.Entry) error {
	name := cfg.Target.Name
	files, own, err := choiceFiles(cfg, paths)
	if err != nil {
		return fmt.Errorf("pages: %w", err)
	}
	if own == 0 {
		return fmt.Errorf("pages: route %s has a stopped cell and no --check-record of its host group", name)
	}
	hosts := hostGroup(cfg)
	targets := map[string]*config.Target{}
	for _, c := range cfgs {
		if c.Target.Name != name && slices.Contains(hosts, c.Target.Name) {
			targets[c.Target.Name] = &c.Target
		}
	}
	if cfg.Release.Shape.Name != config.ShapeS {
		byteCap = 0
	}
	_, copies, recs, err := ceilingChoiceFor(cfg, targets, files, byteCap, ledger, frozen, false)
	if err != nil {
		return fmt.Errorf("pages: route %s: %w", name, err)
	}
	for _, rec := range recs {
		n := probeCopyName(rec)
		rp.out[path.Join(name, n)] = copies[n]
	}
	return nil
}
