package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"path"
	"strings"

	"github.com/percentes/percentes/internal/evidence"
)

// The directories of an evidence pack: the route's page files by their
// slash paths under --pages, and its cell manifests by theirs under
// --out.
const (
	packPagesDir = "pages"
	packOutDir   = "out"
)

// packCmd writes a route's evidence pack, the file its notice carries
// (SPEC-HOSTED HA3 point 14): the route's page files and cell manifests,
// built from the cells under --out as pages builds them, with
// inventory.json naming each by SHA-256 (evidence.BuildPack). The same
// cells give the same bytes. A route with a page needs its window-close
// record, which its page prints.
func packCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("pack", flag.ContinueOnError)
	out := fs.String("out", "", "the release output directory the route processes write under (required)")
	framesRoot := fs.String("frames", "", "the raw-frame store directory the route processes write under (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration, the file its cells ran (repeatable, required)")
	schedulePath := fs.String("schedule", "", "the committed schedule file the cells ran under (required)")
	keyPath := fs.String("release-key", "", "the release key file (required where a route has a confidential limit)")
	route := fs.String("route", "", "the route whose pack to write (required)")
	packPath := fs.String("pack", "", "the file to write the pack to (required)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for _, f := range [][2]string{{"--out", *out}, {"--frames", *framesRoot}, {"--schedule", *schedulePath}, {"--route", *route}, {"--pack", *packPath}} {
		if f[1] == "" {
			return exitError, fmt.Errorf("pack: %s is required", f[0])
		}
	}
	cfgs, err := loadReleases(cfgPaths)
	if err != nil {
		return exitError, err
	}
	key, err := checkingKey(*keyPath, cfgs)
	if err != nil {
		return exitError, fmt.Errorf("pack: %w", err)
	}
	sched, err := scheduleSHA256(*schedulePath)
	if err != nil {
		return exitError, fmt.Errorf("pack: %w", err)
	}
	rp, err := buildPages(*out, *framesRoot, sched, cfgs, key)
	if err != nil {
		return exitError, err
	}
	manifests, err := releaseFiles(*out, "*", evidence.ManifestFile)
	if err != nil {
		return exitError, err
	}
	packs, err := routePacks(rp, manifests)
	if err != nil {
		return exitError, err
	}
	b, ok := packs[*route]
	if !ok {
		return exitError, fmt.Errorf("pack: --route %s is not the route of any --config", *route)
	}
	for i, r := range rp.routes {
		if r.Config.Target.Name == *route && rp.recs[i].verdict != nil && rp.recs[i].close == nil {
			return exitError, fmt.Errorf("pack: route %s has a page and no %s; run writes it once no cell is left", *route, evidence.WindowCloseFile)
		}
	}
	if err := replaceFile(*packPath, b); err != nil {
		return exitError, err
	}
	sum := sha256.Sum256(b)
	fmt.Printf("pack written: %s; route %s, sha256 %s\n", *packPath, *route, hex.EncodeToString(sum[:]))
	return exitOK, nil
}

// routePacks builds each route's evidence pack from the pages rp and the
// release's cell manifests, by slash path under the release output
// directory. A route with no page has an inventory with no file.
func routePacks(rp *releasePages, manifests map[string][]byte) (map[string][]byte, error) {
	packs := map[string][]byte{}
	for i, r := range rp.routes {
		name := r.Config.Target.Name
		files := map[string][]byte{}
		if rp.recs[i].verdict != nil {
			for _, p := range []string{r.Page, path.Join(routesDir, name+".json")} {
				files[path.Join(packPagesDir, p)] = rp.files[p]
			}
		}
		for p, b := range manifests {
			if strings.HasPrefix(p, name+"/") {
				files[path.Join(packOutDir, p)] = b
			}
		}
		var err error
		if packs[name], err = evidence.BuildPack(name, files); err != nil {
			return nil, fmt.Errorf("pack: route %s: %w", name, err)
		}
	}
	return packs, nil
}
