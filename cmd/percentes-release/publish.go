package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
)

// The publication page's files under --pages.
const (
	publicationPage = "publication.md"
	publicationJSON = "publication.json"
)

// publishNow is the clock publish-check judges notice windows by.
var publishNow = time.Now

// publishCheckCmd checks the release publication record (SPEC-HOSTED HA3
// point 14) against the cell manifests and the window-close records under
// --out, the pages the cells generate under the schedule file given,
// which must be the pages under --pages, and the files the record names.
// A route with a page needs its window-close record. Each notice's pack
// must be the route's pack as pack builds it from the same cells, and a
// replication pack must carry that pack's files. A notice may not be
// dated before the UTC day on which its route's last cell started or its
// window-close record was taken. No text the publication page prints
// from the record may hold a notice's account or recipient, or the value
// of a route's confidential limit as its configuration records it or as
// a refresh command read it. When the record is complete it builds
// the publication page from it, which must be under --pages byte for
// byte, or which --write-page writes there, and prints the providers told
// of the measurement before the notices. pages runs without a record.
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
	writePage := fs.Bool("write-page", false, "write the publication page under --pages once the record is complete")
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
	withheld, err := withheldValues(*out, *framesRoot, cfgs)
	if err != nil {
		return exitError, err
	}
	state := evidence.PublicationState{Manifests: manifests, WindowCloses: closes, Pages: rp.files, PagesDir: *pages, Packs: packs, Routes: routes,
		Withheld: withheld, Now: publishNow()}
	if err := pub.Check(state); err != nil {
		fmt.Printf("NOT READY to publish: %v\n", err)
		return exitAdverse, nil
	}
	pg, err := pub.Page()
	if err != nil {
		return exitError, err
	}
	page := map[string][]byte{publicationPage: []byte(pg.Markdown())}
	if page[publicationJSON], err = marshalPage(pg); err != nil {
		return exitError, err
	}
	var problems []string
	for _, name := range []string{publicationPage, publicationJSON} {
		p := filepath.Join(*pages, name)
		if *writePage {
			if err := replaceFile(p, page[name]); err != nil {
				return exitError, err
			}
			continue
		}
		switch b, err := os.ReadFile(p); {
		case errors.Is(err, os.ErrNotExist):
			problems = append(problems, fmt.Sprintf("%s is missing under %s (publish-check --write-page writes it)", name, *pages))
		case err != nil:
			return exitError, err
		case !bytes.Equal(b, page[name]):
			problems = append(problems, fmt.Sprintf("%s under %s is not the page the record gives", name, *pages))
		}
	}
	if len(problems) > 0 {
		fmt.Printf("NOT READY to publish: publication page: %s\n", strings.Join(problems, "; "))
		return exitAdverse, nil
	}
	fmt.Printf("publication record complete: %d manifests, %d pages, %d routes\n", len(manifests), len(rp.files), len(routes))
	if *writePage {
		fmt.Printf("publication page written: %s, %s\n", filepath.Join(*pages, publicationPage), filepath.Join(*pages, publicationJSON))
	}
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

// withheldValues are the values of each route's confidential limits: as
// its configuration records them, and as the refresh records as run that
// the retained store under framesRoot keeps for its cells and its window
// close read them.
func withheldValues(out, framesRoot string, cfgs []*config.Config) ([]evidence.WithheldValue, error) {
	var vs []evidence.WithheldValue
	add := func(route, dimension string, v float64) {
		if w := (evidence.WithheldValue{Route: route, Dimension: dimension, Value: v}); v > 0 && !slices.Contains(vs, w) {
			vs = append(vs, w)
		}
	}
	for _, cfg := range cfgs {
		t := &cfg.Target
		if !t.Confidential() {
			continue
		}
		for _, l := range t.Limits {
			if l.Marking == config.MarkingConfidential {
				add(t.Name, l.Dimension, l.Value)
			}
		}
		dir := filepath.Join(out, t.Name)
		read, err := retainedReads(dir, framesRoot, t)
		if err != nil {
			return nil, err
		}
		closed, err := windowCloseReads(dir, framesRoot, t)
		if err != nil {
			return nil, err
		}
		for _, l := range append(read, closed...) {
			if l.Confidential {
				add(t.Name, l.Dimension, l.Value)
			}
		}
	}
	return vs, nil
}

// windowCloseReads are the limits of t read by the refresh record as run
// that the retained store under framesRoot keeps beside the copies of
// the window-close record in dir, the route's directory, when there is
// one.
func windowCloseReads(dir, framesRoot string, t *config.Target) ([]evidence.RefreshedLimit, error) {
	w, err := evidence.ReadWindowClose(filepath.Join(dir, evidence.WindowCloseFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec, err := evidence.ReadRetainedRefresh(filepath.Join(framesRoot, w.Copies))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if read, err := evidence.ReadRefreshedLimits(rec, t); err == nil {
		return read, nil
	}
	return nil, nil
}

// replicationRoute reports whether cfg's endpoint is an AWS (Amazon Web
// Services) host, whose route needs the replication pack (SPEC-HOSTED
// HA3 point 14).
func replicationRoute(cfg *config.Config) bool {
	u, err := url.Parse(cfg.Target.EndpointURL)
	return err == nil && strings.HasSuffix(strings.ToLower(u.Hostname()), ".amazonaws.com")
}
