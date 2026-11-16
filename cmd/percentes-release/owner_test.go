package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// A second process of a route refuses to start while the first runs a
// cell, and leaves the first one's running record alone.
func TestLoopOneOwnerPerRoute(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	second := *r
	ctx := context.Background()
	second.wait = func(context.Context, time.Time) error { return errors.New("test end") }
	r.cell = func(_ context.Context, run schedule.Run) (*cellRecord, error) {
		rec := r.newRecord(run)
		rec.Outcome = outcomeRunning
		if err := r.writeNew(run, rec); err != nil {
			return nil, err
		}
		code, err := second.loop(ctx)
		if got := readCell(t, r, run); got.Outcome != outcomeRunning || code != exitError || err == nil || !strings.Contains(err.Error(), "another process holds it") {
			t.Errorf("second process: exit %d, %v; first process's record %s", code, err, got.Outcome)
		}
		rec.Outcome = outcomeStopped
		return rec, replaceJSON(filepath.Join(r.cellDir(run), cellFile), rec)
	}
	if code, err := r.loop(ctx); err != nil || code != exitStopped {
		t.Fatalf("first process: exit %d, %v", code, err)
	}
}

// Two processes of a route on one ledger with different --out directories
// do not both run its cells.
func TestLoopOneOwnerAcrossOutDirs(t *testing.T) {
	e := newEndpoint(t, always200)
	r1, f := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	in := f.in
	in.out = t.TempDir()
	r2, err := newRoute(in)
	if err != nil {
		t.Fatal(err)
	}
	run := r1.runs[0]
	for _, r := range []*route{r1, r2} {
		r.now = func() time.Time { return run.Start }
		r.wait = noWait
	}
	var code2 int
	var err2 error
	r1.cell = func(_ context.Context, run schedule.Run) (*cellRecord, error) {
		code2, err2 = r2.loop(context.Background())
		rec := r1.newRecord(run)
		rec.Outcome = outcomeStopped
		return rec, r1.writeNew(run, rec)
	}
	if code, err := r1.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("first process: exit %d, %v", code, err)
	}
	if code2 != exitError || err2 == nil || !strings.Contains(err2.Error(), "another process holds it") || e.hits.Load() != 0 {
		t.Fatalf("second process: exit %d, %v; %d requests", code2, err2, e.hits.Load())
	}
	if fileExists(r2.cellDir(run)) {
		t.Fatal("the second process wrote a cell")
	}
}

// A ceiling check of a route refuses to send while another holds the
// route, and goes ahead once the first ends.
func TestCeilingCheckOneOwner(t *testing.T) {
	first := make(chan struct{})
	release := make(chan struct{})
	e := newEndpoint(t, func(n int64) (int, string) {
		if n == 1 {
			close(first)
			<-release
		}
		return always200(n)
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	check := func() (*probe.ReleaseRecord, error) {
		return sendCeiling(context.Background(), r.ledger, r.cfg, r.shape, "", 2000, r.key, 2000, nil, nil)
	}
	done := make(chan error, 1)
	go func() { _, err := check(); done <- err }()
	<-first
	second, err := check()
	hits := e.hits.Load()
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if err == nil || second != nil || hits != 1 || !strings.Contains(err.Error(), "another process holds it") {
		t.Fatalf("overlapping check: record %t, %v; %d requests before the first was released", second != nil, err, hits)
	}
	if _, err := check(); err != nil {
		t.Fatalf("check after the first: %v", err)
	}
}
