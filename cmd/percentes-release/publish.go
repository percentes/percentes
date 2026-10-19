package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
)

// publishCheckCmd checks the release publication record (SPEC-HOSTED HA3
// point 14) against the cell manifests and the window-close records under
// --out, the pages the cells generate under the schedule file given,
// which must be the pages under --pages, and the files the record names.
// A route with a page needs its window-close record. Each notice's pack
// must be the route's pack as pack builds it from the same cells, and a
// replication pack must carry that pack's files. When the record is
// complete it prints the providers told of the measurement before the
// notices. pages runs without a record.
func publishCheckCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("publish-check", flag.ContinueOnError)
	out := fs.String("out", "", "the release output directory the route processes write under (required)")
	framesRoot := fs.String("frames", "", "the raw-frame store directory the route processes write under (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration, the file its cells ran (repeatable, required)")
	pages := fs.String("pages", "", "the directory pages wrote the index page and the route pages in (required)")
	record := fs.String("record", "", "the release publication record; the files it names are relative to its directory (required)")
	schedulePath := fs.String("schedule", "", "the committed schedule file the cells ran under (required)")
	keyPath := fs.String("release-key", "", "the release key file (required where a route has a confidential limit)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for _, f := range [][2]string{{"--out", *out}, {"--frames", *framesRoot}, {"--pages", *pages}, {"--record", *record}, {"--schedule", *schedulePath}} {
		if f[1] == "" {
			return exitError, fmt.Errorf("publish-check: %s is required", f[0])
		}
	}
	cfgs, err := loadReleases(cfgPaths)
	if err != nil {
		return exitError, err
	}
	key, err := checkingKey(*keyPath, cfgs)
	if err != nil {
		return exitError, fmt.Errorf("publish-check: %w", err)
	}
	sched, err := scheduleSHA256(*schedulePath)
	if err != nil {
		return exitError, fmt.Errorf("publish-check: %w", err)
	}
	pub, err := evidence.LoadPublication(*record)
	if err != nil {
		return exitError, err
	}
	rp, err := buildPages(*out, *framesRoot, sched, cfgs, key)
	if err != nil {
		return exitError, err
	}
	manifests, err := releaseManifests(*out)
	if err != nil {
		return exitError, err
	}
	closes, err := releaseFiles(*out, evidence.WindowCloseFile)
	if err != nil {
		return exitError, err
	}
	packs, err := routePacks(rp, manifests)
	if err != nil {
		return exitError, err
	}
	routes := make([]evidence.RouteDuties, len(cfgs))
	for i, cfg := range cfgs {
		n := cfg.Target.Naming
		routes[i] = evidence.RouteDuties{Name: cfg.Target.Name, Permission: n != nil && n.PermissionRequired, ReplicationPack: replicationRoute(cfg),
			WindowClose: rp.recs[i].verdict != nil}
	}
	state := evidence.PublicationState{Manifests: manifests, WindowCloses: closes, Pages: rp.files, PagesDir: *pages, Packs: packs, Routes: routes, Now: time.Now()}
	if err := pub.Check(state); err != nil {
		fmt.Printf("NOT READY to publish: %v\n", err)
		return exitAdverse, nil
	}
	fmt.Printf("publication record complete: %d manifests, %d pages, %d routes\n", len(manifests), len(rp.files), len(routes))
	fmt.Println("told of the measurement before the notices:")
	for _, l := range pub.ToldBefore() {
		fmt.Printf("  %s\n", l)
	}
	return exitOK, nil
}

// releaseManifests reads every cell manifest under out, by its slash
// path under out.
func releaseManifests(out string) (map[string][]byte, error) {
	m, err := releaseFiles(out, "*", evidence.ManifestFile)
	if err == nil && len(m) == 0 {
		return nil, errors.New("publish-check: no cell manifest under " + out)
	}
	return m, err
}

// releaseFiles reads every file at out/<route>/elem..., whose elements
// may be patterns, by its slash path under out.
func releaseFiles(out string, elem ...string) (map[string][]byte, error) {
	paths, err := filepath.Glob(filepath.Join(append([]string{out, "*"}, elem...)...))
	if err != nil {
		return nil, err
	}
	m := map[string][]byte{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(out, p)
		if err != nil {
			return nil, err
		}
		m[filepath.ToSlash(rel)] = b
	}
	return m, nil
}

// replicationRoute reports whether cfg's endpoint is an AWS (Amazon Web
// Services) host, whose route needs the replication pack (SPEC-HOSTED
// HA3 point 14).
func replicationRoute(cfg *config.Config) bool {
	u, err := url.Parse(cfg.Target.EndpointURL)
	return err == nil && strings.HasSuffix(strings.ToLower(u.Hostname()), ".amazonaws.com")
}
