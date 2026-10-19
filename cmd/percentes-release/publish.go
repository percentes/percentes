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
// point 14) against the cell manifests under --out, the pages the cells
// generate, which must be the pages under --pages, and the files the
// record names. When the record is complete it prints the providers told
// of the measurement before the notices. pages runs without a record.
func publishCheckCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("publish-check", flag.ContinueOnError)
	out := fs.String("out", "", "the release output directory the route processes write under (required)")
	framesRoot := fs.String("frames", "", "the raw-frame store directory the route processes write under (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration, the file its cells ran (repeatable, required)")
	pages := fs.String("pages", "", "the directory pages wrote the index page and the route pages in (required)")
	record := fs.String("record", "", "the release publication record; the files it names are relative to its directory (required)")
	keyPath := fs.String("release-key", "", "the release key file (required where a route has a confidential limit)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for _, f := range [][2]string{{"--out", *out}, {"--frames", *framesRoot}, {"--pages", *pages}, {"--record", *record}} {
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
	pub, err := evidence.LoadPublication(*record)
	if err != nil {
		return exitError, err
	}
	rp, err := buildPages(*out, *framesRoot, cfgs, key)
	if err != nil {
		return exitError, err
	}
	manifests, err := releaseManifests(*out)
	if err != nil {
		return exitError, err
	}
	routes := make([]evidence.RouteDuties, len(cfgs))
	for i, cfg := range cfgs {
		n := cfg.Target.Naming
		routes[i] = evidence.RouteDuties{Name: cfg.Target.Name, Permission: n != nil && n.PermissionRequired, ReplicationPack: replicationRoute(cfg)}
	}
	if err := pub.Check(evidence.PublicationState{Manifests: manifests, Pages: rp.files, PagesDir: *pages, Routes: routes, Now: time.Now()}); err != nil {
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
	paths, err := filepath.Glob(filepath.Join(out, "*", "*", evidence.ManifestFile))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, errors.New("publish-check: no cell manifest under " + out)
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
