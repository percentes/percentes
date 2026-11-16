package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
)

// A probe whose record fails to write leaves its cell no probe status, so
// a route stop on a later 401 cites only a probe a manifest publishes: the
// next probed cell is blocked and the one after it stops the route; once
// acknowledged the route goes on to its wait with nothing sent, and pages
// reads the stop.
func TestAuthStopCitesOnlyPublishedProbes(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) {
		return http.StatusUnauthorized, `{"error":{"message":"Invalid API Key","code":"invalid_api_key"}}`
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005032")
	shortLadder(r)
	first, blocked, stopped := r.runs[0], r.runs[1], r.runs[2]
	now := first.Start
	r.now = func() time.Time { return now }
	r.wait = func(_ context.Context, at time.Time) error {
		now = at
		return nil
	}
	block := filepath.Join(r.cellDir(first), probe.File+".tmp")
	r.refresh = append([]string{"/bin/sh", "-c", `mkdir -p "$0" "$1" && shift && exec "$@"`, block, block}, r.refresh...)
	var (
		code int
		err  error
	)
	stderrOf(t, func() { code, err = r.loop(context.Background()) })
	if err == nil || code != exitError || e.hits.Load() != 1 {
		t.Fatalf("first start: exit %d, %v; %d requests", code, err, e.hits.Load())
	}
	if got := readCell(t, r, first); got.Outcome != outcomeInterrupted || got.ProbeStatus != 0 || got.Sent.Probe != 1 || fileExists(filepath.Join(r.cellDir(first), probe.File)) {
		t.Fatalf("record %+v", got)
	}
	r.refresh = r.refresh[5:]
	if err := os.RemoveAll(block); err != nil {
		t.Fatal(err)
	}

	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped || e.hits.Load() != 3 || !strings.Contains(stdout, first.Label+" attempt 1: interrupted, labelled") {
		t.Fatalf("restart: exit %d, %v; %d requests:\n%s", code, err, e.hits.Load(), stdout)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 1 || !strings.Contains(holds[0], r.ackFile(cellLabel(stopped))) {
		t.Fatalf("holds %q: %v", holds, err)
	}
	if got := readCell(t, r, blocked); got.Outcome != outcomeBlocked {
		t.Fatalf("%s: %s %q", blocked.Label, got.Outcome, got.Reasons)
	}
	want := "as did its probe at " + blocked.Label + " attempt 1:"
	if got := readCell(t, r, stopped); got.Outcome != outcomeStopped || len(got.Reasons) != 1 || !strings.Contains(got.Reasons[0], want) {
		t.Fatalf("%s: %s %q", stopped.Label, got.Outcome, got.Reasons)
	}
	for _, c := range []struct {
		dir       string
		published bool
	}{{r.cellDir(first), false}, {r.cellDir(blocked), true}} {
		m, err := evidence.ReadManifest(filepath.Join(c.dir, evidence.ManifestFile))
		if err != nil || (m.Artifact(probe.File) != nil) != c.published {
			t.Fatalf("%s: probe record published %t: %v", c.dir, !c.published, err)
		}
	}

	acknowledge(t, r, cellLabel(stopped))
	waits := countWaits(r)
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 1 || e.hits.Load() != 3 {
		t.Fatalf("acknowledged start: exit %d, %v; %d waits, %d requests", code, err, *waits, e.hits.Load())
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || len(rr.stops) != 0 || rr.unrecorded != 0 {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
}
