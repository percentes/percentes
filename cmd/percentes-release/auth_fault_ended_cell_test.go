package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
)

// invalidKeyBody is an error body refusing the key.
const invalidKeyBody = `{"error":{"message":"Invalid API Key","code":"invalid_api_key"}}`

// A cell whose probe got a 401 and whose process was killed while its end
// status was read, after its probe record and before its attempts file
// and manifest, is relabelled interrupted at the restart with its probe's
// status and count; the next cell's probe 401 then stops the route,
// citing it, and the route is held.
func TestAuthFaultReadsProbeOfCellKilledBeforeItsManifest(t *testing.T) {
	fastDispatch(t)
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, invalidKeyBody })
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	first, second := r.runs[0], r.runs[1]
	snap := filepath.Join(t.TempDir(), "route")
	r.status = []string{"/bin/sh", "-c", `[ -e "$2" ] && [ ! -e "$1" ] && cp -R "$0" "$1"; exit 0`, r.dir, snap, filepath.Join(r.cellDir(first), probe.File)}
	now := first.Start
	r.now = func() time.Time { return now }
	waits := 0
	r.wait = func(_ context.Context, at time.Time) error {
		now = at
		if waits++; waits > 1 {
			return errEndLoop
		}
		return nil
	}
	var (
		code int
		err  error
	)
	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err == nil || !strings.Contains(err.Error(), errEndLoop.Error()) || code != exitError {
		t.Fatalf("exit %d, %v:\n%s", code, err, stdout)
	}
	if c0 := readCell(t, r, first); c0.Outcome != outcomeBlocked || c0.ProbeStatus != http.StatusUnauthorized || c0.Sent.Probe != 1 {
		t.Fatalf("%s: %s %q, probe %d, sent %+v", first.Label, c0.Outcome, c0.Reasons, c0.ProbeStatus, c0.Sent)
	}

	restoreTree(t, r, snap)
	killed := readCell(t, r, first)
	if killed.Outcome != outcomeRunning || killed.ProbeStatus != 0 || !probeDispatched(r.cellDir(first)) || fileExists(filepath.Join(r.cellDir(first), evidence.ManifestFile)) {
		t.Fatalf("%s at the kill: %s, probe %d, dispatched %t", first.Label, killed.Outcome, killed.ProbeStatus, probeDispatched(r.cellDir(first)))
	}
	r.wait = func(_ context.Context, at time.Time) error {
		now = at
		return nil
	}
	stdout = stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped {
		t.Fatalf("restart: exit %d, %v:\n%s", code, err, stdout)
	}
	c0 := readCell(t, r, first)
	if c0.Outcome != outcomeInterrupted || len(c0.Reasons) == 0 || c0.Reasons[0] != "the process ended during the cell" || c0.ProbeStatus != http.StatusUnauthorized || c0.Sent.Probe != 1 {
		t.Fatalf("%s after the restart: %s %q, probe %d, sent %+v", first.Label, c0.Outcome, c0.Reasons, c0.ProbeStatus, c0.Sent)
	}
	want := fmt.Sprintf("route stop: probe request returned 401, as did its probe at %s attempt 1: independent evidence", first.Label)
	if c1 := readCell(t, r, second); c1.Outcome != outcomeStopped || len(c1.Reasons) != 1 || !strings.HasPrefix(c1.Reasons[0], want) {
		t.Fatalf("%s: %s %q\n%s", second.Label, c1.Outcome, c1.Reasons, stdout)
	}
	if n := e.hits.Load(); n != 2 {
		t.Fatalf("%d requests, want the two probes", n)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 1 || !strings.Contains(holds[0], r.ackFile(cellLabel(second))) {
		t.Fatalf("holds %q: %v", holds, err)
	}
}

// A cell SIGTERM interrupted during its start checks dispatched no probe
// and is passed over: after the restart labels it, the next cell's probe
// 401 stops the route, citing the probe 401 of the cell before it, and
// the route is held.
func TestAuthFaultPassesOverCellSigtermEndedBeforeAdmission(t *testing.T) {
	fastDispatch(t)
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, invalidKeyBody })
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	shortLadder(r)
	first, second, third := r.runs[0], r.runs[1], r.runs[2]
	refresh := r.refresh
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	now := first.Start
	r.now = func() time.Time { return now }
	waits := 0
	r.wait = func(_ context.Context, at time.Time) error {
		now = at
		if waits++; waits == 2 {
			r.refresh = slowCommand(refresh)
			time.AfterFunc(500*time.Millisecond, func() { cancel(errTerminated) })
		}
		return nil
	}
	var (
		code int
		err  error
	)
	stdout := stdoutOf(t, func() { code, err = r.loop(ctx) })
	if err != nil || code != exitTerminated {
		t.Fatalf("exit %d, %v:\n%s", code, err, stdout)
	}
	if c0 := readCell(t, r, first); c0.Outcome != outcomeBlocked || c0.ProbeStatus != http.StatusUnauthorized {
		t.Fatalf("%s: %s %q, probe %d", first.Label, c0.Outcome, c0.Reasons, c0.ProbeStatus)
	}
	c1 := readCell(t, r, second)
	if c1.Outcome != outcomeInterrupted || len(c1.Reasons) != 1 || c1.Reasons[0] != terminatedReason || c1.Error != "" || c1.Sent.Probe != 0 || probeDispatched(r.cellDir(second)) || e.hits.Load() != 1 {
		t.Fatalf("%s: %s %q, error %q, sent %+v, %d requests", second.Label, c1.Outcome, c1.Reasons, c1.Error, c1.Sent, e.hits.Load())
	}

	r.refresh = refresh
	r.wait = func(_ context.Context, at time.Time) error {
		now = at
		return nil
	}
	stdout = stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped {
		t.Fatalf("restart: exit %d, %v:\n%s", code, err, stdout)
	}
	if c1 := readCell(t, r, second); c1.Outcome != outcomeInterrupted || c1.Reasons[0] != terminatedReason || !fileExists(filepath.Join(r.cellDir(second), evidence.ManifestFile)) {
		t.Fatalf("%s after the restart: %s %q", second.Label, c1.Outcome, c1.Reasons)
	}
	want := fmt.Sprintf("route stop: probe request returned 401, as did its probe at %s attempt 1: independent evidence", first.Label)
	if c2 := readCell(t, r, third); c2.Outcome != outcomeStopped || len(c2.Reasons) != 1 || !strings.HasPrefix(c2.Reasons[0], want) {
		t.Fatalf("%s: %s %q\n%s", third.Label, c2.Outcome, c2.Reasons, stdout)
	}
	if n := e.hits.Load(); n != 2 {
		t.Fatalf("%d requests, want the two probes", n)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 1 || !strings.Contains(holds[0], r.ackFile(cellLabel(third))) {
		t.Fatalf("holds %q: %v", holds, err)
	}
}
