package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/loadgen"
)

// A cell stopped by a billing refusal on its probe whose record and stop
// marker both fail to write prints the unwritten marker; the next start
// labels the record stopped from its probe record and holds the route
// before it sends, and once acknowledged the route goes on and pages
// reads the stop from the run record.
func TestStopMarkerUnwrittenHeldFromProbeRecord(t *testing.T) {
	r, e := billingStopRoute(t)
	r.wait = noWait
	run := r.runs[0]
	// From the cell's refresh command on, the record's and the marker's
	// temporary paths are directories.
	blocks := []string{filepath.Join(r.cellDir(run), cellFile+".tmp"), r.stopMarkerPath(run) + ".tmp"}
	r.refresh = append([]string{"/bin/sh", "-c", `mkdir -p "$0" "$1" && shift && exec "$@"`, blocks[0], blocks[1]}, r.refresh...)
	var code int
	stderr := stderrOf(t, func() { code, _ = r.loop(context.Background()) })
	if code != exitStopped || !strings.Contains(stderr, r.cfg.Target.Name+": stop marker not written: ") {
		t.Fatalf("first process: exit %d, stderr:\n%s", code, stderr)
	}
	if got := readCell(t, r, run); got.Outcome != outcomeRunning || got.Decided != nil || fileExists(r.stopMarkerPath(run)) {
		t.Fatalf("fixture: record %+v, marker %t", got, fileExists(r.stopMarkerPath(run)))
	}
	for _, b := range blocks {
		if err := os.RemoveAll(b); err != nil {
			t.Fatal(err)
		}
	}
	hits := e.hits.Load()
	var err error
	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped || e.hits.Load() != hits || !strings.Contains(stdout, "held until "+r.ackFile(cellLabel(run))+" exists") {
		t.Fatalf("restart: exit %d, %v; %d requests:\n%s", code, err, e.hits.Load()-hits, stdout)
	}
	got := readCell(t, r, run)
	if got.Outcome != outcomeStopped || got.BillingRefusals != 1 || len(got.Reasons) != 1 || !strings.Contains(got.Reasons[0], "billing refusal") ||
		got.Error != "the cell's record was not rewritten after its probe; its label is its probe record's" || got.EndUTC == nil {
		t.Fatalf("record %+v", got)
	}
	stopPublishedFromRecord(t, r, e.hits.Load, cellLabel(run))
}

// Records an ended process left running take an end when the next start
// labels them, so another route's later cell lists none of them as
// concurrent.
func TestRecoveredRecordsEndForConcurrentRuns(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return 404, "" })
	release, _ := oneRelease(t, e.url(), "groq", "baseten")
	groq, _ := pagesRoute(t, "groq", e.url(), "", "", release)
	baseten, _ := pagesRoute(t, "baseten", e.url(), groq.out, groq.framesRoot, release)
	old := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	kinds := []string{"interrupted", "decided", "marker"}
	want := map[string]time.Time{}
	for i, kind := range kinds {
		run := groq.runs[i]
		rec := groq.newRecord(run)
		rec.StartUTC = old.Add(time.Duration(i) * time.Hour)
		rec.Outcome = outcomeRunning
		if kind == "decided" {
			rec.Decided = &decidedLabel{Outcome: outcomeStopped, Reasons: []string{"route stop: test"}}
		}
		if err := groq.writeNew(run, rec); err != nil {
			t.Fatal(err)
		}
		through := rec.StartUTC.Add(20 * time.Minute)
		ds := &dispatchSeries{Run: run, StartUTC: rec.StartUTC, Counts: []int{}, ThroughUTC: &through}
		want[kind] = through
		if kind == "decided" {
			ds.ThroughUTC, ds.Final = nil, true
			want[kind] = rec.StartUTC
		}
		if err := replaceJSON(filepath.Join(groq.cellDir(run), dispatchFile), ds); err != nil {
			t.Fatal(err)
		}
		if kind == "marker" {
			stopped := *rec
			stopped.Outcome, stopped.Reasons = outcomeStopped, []string{"route stop: test"}
			groq.now = func() time.Time { return through }
			if _, err := groq.writeStopHold(&stopped); err != nil {
				t.Fatal(err)
			}
			groq.now = time.Now
		}
	}
	if _, err := groq.state(); err != nil {
		t.Fatal(err)
	}
	for i, kind := range kinds {
		if got := readCell(t, groq, groq.runs[i]); got.EndUTC == nil || !got.EndUTC.Equal(want[kind]) {
			t.Errorf("%s: end_utc %v, want %s", kind, got.EndUTC, want[kind])
		}
	}
	now := time.Now().UTC()
	if runs, err := baseten.concurrent(now, now.Add(time.Minute)); err != nil || len(runs) != 0 {
		t.Fatalf("concurrent now: %v, %v", runs, err)
	}
	if runs, err := baseten.concurrent(old, old.Add(3*time.Hour)); err != nil || len(runs) != len(kinds) {
		t.Fatalf("concurrent at the cells' start: %v, %v", runs, err)
	}
}

// The loop's abort marker write waits for the signal's, so where the
// signal's try fails and the loop's succeeds the marker the loop reports
// is written.
func TestAbortMarkerLineFollowsSignalWrite(t *testing.T) {
	r, _ := billingStopRoute(t)
	run := r.runs[0]
	hold := &abortHold{r: r}
	hold.at(&run, false)
	ctx, cancel := context.WithCancelCause(context.Background())
	stop := hold.watch(ctx)
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(r.dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(r.dir, 0o755) }) //nolint:errcheck
	// The signal's write waits on hold.mu.
	hold.mu.Lock()
	cancel(loadgen.ErrOperatorAbort)
	done := make(chan error, 1)
	go func() {
		hold.settle(ctx)
		_, err := hold.write(run, false, context.Cause(ctx))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the loop wrote before the signal: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	var n atomic.Int32
	base := r.now
	r.now = func() time.Time {
		if n.Add(1) == 2 {
			os.Chmod(r.dir, 0o755) //nolint:errcheck
		}
		return base()
	}
	hold.mu.Unlock()
	err := <-done
	stop()
	if m := markers(t, r); err != nil || len(m) != 1 || m[0].Run != run {
		t.Fatalf("loop write: %v; markers %+v", err, m)
	}
}

// publish-check's refusal of a missing or changed ceiling-check copy
// names the copy and its route.
func TestCopiedChecksRefusalNamesRoute(t *testing.T) {
	out := t.TempDir()
	copies := map[string][]byte{"baseten/ceiling-probe-baseten-512-0.json": []byte("{}\n"), "groq/ceiling-probe-groq-512-1.json": []byte("{}\n")}
	for name, b := range copies {
		p := filepath.Join(out, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := copiedChecks(out, copies); got != "" {
		t.Fatalf("matching copies: %q", got)
	}
	if err := os.Remove(filepath.Join(out, "baseten", "ceiling-probe-baseten-512-0.json")); err != nil {
		t.Fatal(err)
	}
	want := "baseten/ceiling-probe-baseten-512-0.json under " + out + " is not the copy pages writes under route baseten of a ceiling-check record"
	if got := copiedChecks(out, copies); got != want {
		t.Fatalf("missing copy: %q", got)
	}
}
