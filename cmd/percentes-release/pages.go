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
// frame, the model_mismatch attempts' identifiers and the route's
// window-close record, nil when it has none. The verdict is nil when
// every attempt is missed or interrupted with no manifest.
type routeRecords struct {
	// schedule is the schedule SHA-256 the route's cells carry.
	schedule   string
	verdict    *admission.Verdict
	ceiling    probe.CeilingChoice
	probes     []evidence.FileRef
	cells      []report.CellRecords
	mismatches []report.ModelMismatch
	close      *evidence.WindowClose
	// labelled counts the attempts with a manifest that do not count;
	// unrecorded the missed and interrupted attempts with no manifest,
	// which keep their run record only.
	labelled, unrecorded int
}

// pagesCmd writes the release's index page and each route's page from
// the cells' records under --out (SPEC-HOSTED HA3 point 12). Every route
// with a cell under --out must be given, and every cell must have run
// under the schedule file given; nothing is written unless every route's
// cells read. A route whose attempts are all missed or interrupted gets
// no page.
func pagesCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("pages", flag.ContinueOnError)
	out := fs.String("out", "", "the release output directory the route processes write under (required)")
	framesRoot := fs.String("frames", "", "the raw-frame store directory the route processes write under (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration, the file its cells ran (repeatable, required)")
	pages := fs.String("pages", "", "directory to write the index page and the route pages in (required)")
	schedulePath := fs.String("schedule", "", "the committed schedule file the cells ran under (required)")
	keyPath := fs.String("release-key", "", "the release key file (required where a route has a confidential limit)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for name, v := range map[string]string{"--out": *out, "--frames": *framesRoot, "--pages": *pages, "--schedule": *schedulePath} {
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
	sched, err := scheduleSHA256(*schedulePath)
	if err != nil {
		return exitError, fmt.Errorf("pages: %w", err)
	}
	rp, err := buildPages(*out, *framesRoot, sched, cfgs, key)
	if err != nil {
		return exitError, err
	}
	for name, b := range rp.files {
		if err := replaceFile(filepath.Join(*pages, filepath.FromSlash(name)), b); err != nil {
			return exitError, err
		}
	}
	fmt.Printf("index written: %s; %d rows\n", filepath.Join(*pages, indexPage), len(rp.index.Rows))
	for i, r := range rp.routes {
		if rp.recs[i].verdict == nil {
			fmt.Printf("%s: no cell with a %s, %d with a run record only; no page\n", r.Config.Target.Name, evidence.ManifestFile, rp.recs[i].unrecorded)
			continue
		}
		fmt.Printf("%s: %d of %d cells counted, %d labelled, %d with a run record only; page %s\n", r.Config.Target.Name,
			len(r.Cells), admission.ReleaseCells, rp.recs[i].labelled, rp.recs[i].unrecorded, filepath.Join(*pages, filepath.FromSlash(r.Page)))
	}
	return exitOK, nil
}

// releasePages are the pages of a release, by slash path under --pages,
// with the index and each given route's cells, in the order given.
type releasePages struct {
	files  map[string][]byte
	index  *report.Index
	routes []report.IndexRoute
	recs   []*routeRecords
}

// buildPages builds the index page and each route's page from the cells
// of cfgs' routes under out. Every route with a cell under out must be
// among cfgs, every cell must carry the schedule SHA-256 sched, and the
// routes of one model among them must carry one ceiling choice and one
// set of probe records. A route with no manifest has no page and no
// index row.
func buildPages(out, framesRoot, sched string, cfgs []*config.Config, key *releasekey.Key) (*releasePages, error) {
	given := map[string]bool{}
	rp := &releasePages{routes: make([]report.IndexRoute, len(cfgs)), recs: make([]*routeRecords, len(cfgs))}
	var paged []report.IndexRoute
	for i, cfg := range cfgs {
		given[cfg.Target.Name] = true
		var err error
		if rp.recs[i], err = readRouteCells(out, framesRoot, cfg, key); err != nil {
			return nil, err
		}
		if s := rp.recs[i].schedule; s != sched {
			return nil, fmt.Errorf("pages: route %s: the cells ran under schedule sha256 %q; --schedule has sha256 %s", cfg.Target.Name, s, sched)
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
	if rp.index, err = report.NewIndex(paged); err != nil {
		return nil, err
	}
	rp.files = map[string][]byte{indexPage: []byte(rp.index.Markdown())}
	if rp.files[indexJSON], err = marshalPage(rp.index); err != nil {
		return nil, err
	}
	for i, r := range rp.routes {
		if rp.recs[i].verdict == nil {
			continue
		}
		st, err := report.NewStepTables(r.Config, r.Verdict, r.Cells)
		if err != nil {
			return nil, err
		}
		if c := rp.recs[i].close; c != nil {
			if st.WindowClose, err = report.CompareWindowClose(r.Config.Target.Name, r.Cells, c); err != nil {
				return nil, fmt.Errorf("pages: %w", err)
			}
		}
		st.ModelMismatches = rp.recs[i].mismatches
		rp.files[r.Page] = []byte(st.Text())
		if rp.files[path.Join(routesDir, r.Config.Target.Name+".json")], err = marshalPage(st); err != nil {
			return nil, err
		}
	}
	return rp, nil
}

// sameCeiling refuses routes of one model, each the other's host, whose
// manifests carry different ceiling choices or probe records. Where
// either route has no manifest, their ceilings (ranAt) must agree.
func sameCeiling(cfgs []*config.Config, recs []*routeRecords) error {
	for i, cfg := range cfgs {
		for _, h := range cfg.Release.OtherHosts {
			j := slices.IndexFunc(cfgs, func(c *config.Config) bool { return c.Target.Name == h.Route })
			switch {
			case j < 0:
			case recs[i].verdict == nil || recs[j].verdict == nil:
				if a, b := ranAt(cfg, recs[i]), ranAt(cfgs[j], recs[j]); a != b {
					return fmt.Errorf("pages: routes %s and %s, hosts of one model, ran at ceilings %d and %d", cfg.Target.Name, h.Route, a, b)
				}
			case !reflect.DeepEqual(recs[i].ceiling, recs[j].ceiling):
				return fmt.Errorf("pages: routes %s and %s, hosts of one model, ran at different ceiling choices: %+v and %+v",
					cfg.Target.Name, h.Route, recs[i].ceiling, recs[j].ceiling)
			case !slices.Equal(recs[i].probes, recs[j].probes):
				return fmt.Errorf("pages: routes %s and %s, hosts of one model, carry different ceiling probe records", cfg.Target.Name, h.Route)
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
// missed or interrupted attempt may have its run record alone. Each counted
// cell carries its scheduled start and the documentation state its
// manifest records, which the drift rule compares. Each model_mismatch
// attempt's expected and reported model ids are kept for the route's
// page, and the route's window-close record is read where it has one.
// A route with no cell record is refused.
func readRouteCells(out, framesRoot string, cfg *config.Config, key *releasekey.Key) (*routeRecords, error) {
	name := cfg.Target.Name
	paths, err := filepath.Glob(filepath.Join(out, name, "*", cellFile))
	if err != nil {
		return nil, err
	}
	rr := &routeRecords{}
	counted := map[string]string{}
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
		if _, err := os.Stat(filepath.Join(dir, evidence.ManifestFile)); errors.Is(err, os.ErrNotExist) {
			if rec.Outcome != outcomeMissed && rec.Outcome != outcomeInterrupted {
				return nil, fmt.Errorf("pages: %s: %s with no %s", dir, rec.Outcome, evidence.ManifestFile)
			}
			rr.unrecorded++
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
			rr.verdict, rr.ceiling, rr.probes = m.Release.Admission, m.Release.Ceiling, m.Release.Probes
		case !reflect.DeepEqual(rr.verdict, m.Release.Admission):
			return nil, fmt.Errorf("pages: %s: the ceiling-rule verdict differs from the route's other cells'", dir)
		case !reflect.DeepEqual(rr.ceiling, m.Release.Ceiling) || !slices.Equal(rr.probes, m.Release.Probes):
			return nil, fmt.Errorf("pages: %s: the ceiling choice or its probe records differ from the route's other cells'", dir)
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
			continue
		}
		if prev, ok := counted[rec.Run.Label]; ok {
			return nil, fmt.Errorf("pages: cell %s of route %s counted twice: %s and %s", rec.Run.Label, name, prev, dir)
		}
		counted[rec.Run.Label] = dir
		recs, err := readAttempts(dir, m)
		if err != nil {
			return nil, err
		}
		rr.cells = append(rr.cells, report.CellRecords{Cell: cellLabel(rec.Run), Outcome: m.Release.Outcome, Gaps: m.Release.Gaps, Records: recs,
			Start: m.Release.Run.Start, Docs: report.DocsOf(m)})
	}
	if rr.verdict == nil && rr.unrecorded == 0 {
		return nil, fmt.Errorf("pages: route %s has no cell with a %s under %s", name, evidence.ManifestFile, out)
	}
	if p := filepath.Join(out, name, evidence.WindowCloseFile); fileExists(p) {
		if rr.close, err = evidence.ReadWindowClose(p); err != nil {
			return nil, fmt.Errorf("pages: %w", err)
		}
	}
	return rr, nil
}

// scheduleSHA256 is the SHA-256 of the schedule file at path, which must
// load as a committed schedule.
func scheduleSHA256(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("schedule: %w", err)
	}
	if _, err := schedule.Load(raw); err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
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

// cellLabel is a run's directory name under its route's.
func cellLabel(run schedule.Run) string {
	return fmt.Sprintf("%s-a%d", run.Label, run.Attempt)
}

func sameRun(a, b schedule.Run) bool {
	return a.Route == b.Route && a.Label == b.Label && a.Attempt == b.Attempt && a.Date == b.Date && a.Start.Equal(b.Start) && a.Seed == b.Seed
}
