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

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
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
// ceiling rule's verdict every manifest carries, the counted cells'
// records, and the attempts outside the frame.
type routeRecords struct {
	verdict *admission.Verdict
	cells   []report.CellRecords
	// labelled counts the attempts with a manifest that do not count;
	// unrecorded the missed and interrupted attempts with no manifest,
	// which keep their run record only.
	labelled, unrecorded int
}

// pagesCmd writes the release's index page and each route's page from
// the cells' records under --out (SPEC-HOSTED HA3 point 12). Every route
// with a cell under --out must be given; nothing is written unless every
// route's cells read.
func pagesCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("pages", flag.ContinueOnError)
	out := fs.String("out", "", "the release output directory the route processes write under (required)")
	framesRoot := fs.String("frames", "", "the raw-frame store directory the route processes write under (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration, the file its cells ran (repeatable, required)")
	pages := fs.String("pages", "", "directory to write the index page and the route pages in (required)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for name, v := range map[string]string{"--out": *out, "--frames": *framesRoot, "--pages": *pages} {
		if v == "" {
			return exitError, fmt.Errorf("pages: %s is required", name)
		}
	}
	cfgs, err := loadReleases(cfgPaths)
	if err != nil {
		return exitError, err
	}
	given := map[string]bool{}
	routes := make([]report.IndexRoute, len(cfgs))
	recs := make([]*routeRecords, len(cfgs))
	for i, cfg := range cfgs {
		given[cfg.Target.Name] = true
		if recs[i], err = readRouteCells(*out, *framesRoot, cfg); err != nil {
			return exitError, err
		}
		routes[i] = report.IndexRoute{Config: cfg, Verdict: recs[i].verdict, Cells: recs[i].cells, Page: path.Join(routesDir, cfg.Target.Name+".txt")}
	}
	cells, err := filepath.Glob(filepath.Join(*out, "*", "*", cellFile))
	if err != nil {
		return exitError, err
	}
	for _, p := range cells {
		if name := filepath.Base(filepath.Dir(filepath.Dir(p))); !given[name] {
			return exitError, fmt.Errorf("pages: route %s has cells under %s and no --config", name, *out)
		}
	}
	ix, err := report.NewIndex(routes)
	if err != nil {
		return exitError, err
	}
	files := map[string][]byte{indexPage: []byte(ix.Markdown())}
	if files[indexJSON], err = marshalPage(ix); err != nil {
		return exitError, err
	}
	for _, r := range routes {
		st, err := report.NewStepTables(r.Config, r.Verdict, r.Cells)
		if err != nil {
			return exitError, err
		}
		files[r.Page] = []byte(st.Text())
		if files[path.Join(routesDir, r.Config.Target.Name+".json")], err = marshalPage(st); err != nil {
			return exitError, err
		}
	}
	for name, b := range files {
		if err := replaceFile(filepath.Join(*pages, filepath.FromSlash(name)), b); err != nil {
			return exitError, err
		}
	}
	fmt.Printf("index written: %s; %d rows\n", filepath.Join(*pages, indexPage), len(ix.Rows))
	for i, r := range routes {
		fmt.Printf("%s: %d of %d cells counted, %d labelled, %d with a run record only; page %s\n", r.Config.Target.Name,
			len(r.Cells), admission.ReleaseCells, recs[i].labelled, recs[i].unrecorded, filepath.Join(*pages, filepath.FromSlash(r.Page)))
	}
	return exitOK, nil
}

func marshalPage(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// readRouteCells reads cfg's route's cells under out. Every attempt with
// a manifest must pass the completeness check against cfg's file and the
// frame store, and every manifest must carry the same verdict. An
// attempt enters the route's frame when its run record and its manifest
// both count it, at most one attempt per scheduled cell; only a missed
// or interrupted attempt may have its run record alone. Each counted
// cell carries its scheduled start and the documentation state its
// manifest records, which the drift rule compares.
func readRouteCells(out, framesRoot string, cfg *config.Config) (*routeRecords, error) {
	name := cfg.Target.Name
	paths, err := filepath.Glob(filepath.Join(out, name, "*", cellFile))
	if err != nil {
		return nil, err
	}
	rr := &routeRecords{}
	counted := map[string]string{}
	for _, p := range paths {
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
		}
		if _, err := os.Stat(filepath.Join(dir, evidence.ManifestFile)); errors.Is(err, os.ErrNotExist) {
			if rec.Outcome != outcomeMissed && rec.Outcome != outcomeInterrupted {
				return nil, fmt.Errorf("pages: %s: %s with no %s", dir, rec.Outcome, evidence.ManifestFile)
			}
			rr.unrecorded++
			continue
		}
		m, err := evidence.CheckRelease(evidence.Release{Dir: dir, Config: cfg.Raw, FramesRoot: framesRoot, Pair: [2]string{report.JSONFile, report.TextFile}})
		if err != nil {
			return nil, fmt.Errorf("pages: %s: %w", dir, err)
		}
		if !sameRun(m.Release.Run, rec.Run) {
			return nil, fmt.Errorf("pages: %s: the manifest is %s, the run record %s", dir, cellLabel(m.Release.Run), cellLabel(rec.Run))
		}
		switch {
		case rr.verdict == nil:
			rr.verdict = m.Release.Admission
		case !reflect.DeepEqual(rr.verdict, m.Release.Admission):
			return nil, fmt.Errorf("pages: %s: the ceiling-rule verdict differs from the route's other cells'", dir)
		}
		switch {
		case rec.Outcome == outcomeCounted && !m.Counted():
			return nil, fmt.Errorf("pages: %s: counted in the run record, %s in the manifest", dir, m.Release.Outcome)
		case rec.Outcome != outcomeCounted || !m.Counted():
			rr.labelled++
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
	if rr.verdict == nil {
		return nil, fmt.Errorf("pages: route %s has no cell with a %s under %s", name, evidence.ManifestFile, out)
	}
	return rr, nil
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
