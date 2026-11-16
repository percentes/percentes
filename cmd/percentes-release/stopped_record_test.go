package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
)

// stopPublishedFromRecord writes the acknowledgement that releases r's
// hold id on its first run's stopped cell, requires the next start to go
// on to its wait with nothing sent and no label written, and requires
// pages to read the stop and its billing refusal from the run record.
func stopPublishedFromRecord(t *testing.T, r *route, hits func() int64, id string) {
	t.Helper()
	run := r.runs[0]
	dir := r.cellDir(run)
	acknowledge(t, r, id)
	waits := countWaits(r)
	sent := hits()
	var (
		code int
		err  error
	)
	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped || *waits != 1 || hits() != sent || strings.Contains(stdout, "labelled") {
		t.Fatalf("acknowledged start: exit %d, %v; %d waits, %d requests:\n%s", code, err, *waits, hits()-sent, stdout)
	}
	for _, name := range []string{evidence.AttemptsFile, evidence.ManifestFile} {
		if fileExists(filepath.Join(dir, name)) {
			t.Fatalf("%s written for the stopped cell", name)
		}
	}
	if got := readCell(t, r, run); got.Outcome != outcomeStopped || got.Error == "" || got.BillingRefusals < 1 || got.EndUTC == nil {
		t.Fatalf("record %+v", got)
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || len(rr.stops) != 1 || len(rr.billing) != 1 || rr.labelled != 0 || rr.unrecorded != 0 {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
}

// pinRecordTimes sets start_utc and end_utc to at in each clock, status,
// balance and refresh record in dir.
func pinRecordTimes(t *testing.T, dir string, at time.Time) {
	t.Helper()
	for _, name := range []string{evidence.ClockFile, evidence.StatusStartFile, evidence.StatusEndFile, evidence.BalanceStartFile, evidence.BalanceEndFile, evidence.RefreshFile} {
		p := filepath.Join(dir, name)
		b, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		if err := d.Decode(&m); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		for _, k := range []string{"start_utc", "end_utc"} {
			if _, ok := m[k]; ok {
				m[k] = at.UTC()
			}
		}
		if err := replaceJSON(p, m); err != nil {
			t.Fatal(err)
		}
	}
}

// A ladder's billing stop whose attempts file and record fail to write
// is held by its stop marker, which keeps the cell's probe status and
// requests sent: the next start recovers them, and once acknowledged
// they match the ledger and pages reads the stop.
func TestStopMarkerKeepsLadderRequestsSent(t *testing.T) {
	e := newEndpoint(t, func(n int64) (int, string) {
		if n >= 4 {
			return http.StatusPaymentRequired, "no credit"
		}
		return http.StatusOK, ""
	})
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, _ := newTestRoute(t, cfg, "+0.005")
	shortLadder(r)
	run := r.runs[0]
	r.now, r.wait = func() time.Time { return run.Start }, noWait
	dir := r.cellDir(run)
	blocks := []string{filepath.Join(dir, evidence.AttemptsFile+".tmp"), filepath.Join(dir, cellFile+".tmp")}
	orig := blockFromEndStatus(t, r, blocks...)
	var (
		code int
		err  error
	)
	stderrOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped || !fileExists(r.stopMarkerPath(run)) {
		t.Fatalf("first start: exit %d, %v; marker %t", code, err, fileExists(r.stopMarkerPath(run)))
	}
	r.status = orig
	for _, b := range blocks {
		if err := os.RemoveAll(b); err != nil {
			t.Fatal(err)
		}
	}
	sent := e.hits.Load()
	waits := countWaits(r)
	id := stopPrefix + cellLabel(run)
	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != sent || !strings.Contains(stdout, "held until "+r.ackFile(id)+" exists") {
		t.Fatalf("restart: exit %d, %v; %d waits, %d requests:\n%s", code, err, *waits, e.hits.Load()-sent, stdout)
	}
	stopPublishedFromRecord(t, r, e.hits.Load, id)
	got := readCell(t, r, run)
	u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]
	if n := got.Sent.Probe + got.Sent.Warmup + got.Sent.Measured; int64(n) != sent || u.Probe+u.Warmup+u.Measured != n || got.Sent != (admission.Counts{Probe: u.Probe, Warmup: u.Warmup, Measured: u.Measured}) || got.ProbeStatus != http.StatusOK {
		t.Fatalf("record sent %+v, probe status %d; ledger %+v; %d requests", got.Sent, got.ProbeStatus, u, sent)
	}
}

// blockFromEndStatus makes r's status command, from its second call, the
// cell's end status, turn each of blocks into a directory, and returns
// the command as it was.
func blockFromEndStatus(t *testing.T, r *route, blocks ...string) []string {
	t.Helper()
	counter := filepath.Join(t.TempDir(), "calls")
	script := `n=$(cat "$0" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$0"; if [ $n -ge 2 ]; then for b in $1; do mkdir -p "$b"; done; fi; shift; exec "$@"`
	orig := r.status
	r.status = append([]string{"/bin/sh", "-c", script, counter, strings.Join(blocks, " ")}, orig...)
	return orig
}

// A billing refusal inside the ladder whose evidence and record then fail
// to write leaves the running record with its decided label, counting
// every refusal the endpoint answered, its series final or not; it is
// never labelled after the fact: the next start holds the route before it
// sends, and once acknowledged the route goes on and pages reads the stop
// and that count from the run record.
func TestLadderBillingStopPublishedFromRecord(t *testing.T) {
	for _, tc := range []struct {
		name     string
		notFinal bool
	}{
		{"series final", false},
		{"series not final", true},
	} {
		e := newEndpoint(t, func(n int64) (int, string) {
			if n >= 2 {
				return http.StatusPaymentRequired, "no credit"
			}
			return http.StatusOK, ""
		})
		cfg := releaseConfig(t, e.url())
		cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
		r, _ := newTestRoute(t, cfg, "+0.005")
		shortLadder(r)
		run := r.runs[0]
		r.now, r.wait = func() time.Time { return run.Start }, noWait
		dir := r.cellDir(run)
		blocks := []string{filepath.Join(dir, evidence.AttemptsFile+".tmp"), filepath.Join(dir, cellFile+".tmp"), r.stopMarkerPath(run) + ".tmp"}
		orig := blockFromEndStatus(t, r, blocks...)
		var (
			code int
			err  error
		)
		stderr := stderrOf(t, func() { code, err = r.loop(context.Background()) })
		if err != nil || code != exitStopped {
			t.Fatalf("%s: first start: exit %d, %v:\n%s", tc.name, code, err, stderr)
		}
		r.status = orig
		for _, b := range blocks {
			if err := os.RemoveAll(b); err != nil {
				t.Fatal(err)
			}
		}
		if got := readCell(t, r, run); got.Outcome != outcomeRunning || got.Decided == nil || int64(got.Decided.BillingRefusals) != e.hits.Load()-1 || e.hits.Load() < 2 {
			t.Fatalf("%s: record %+v, %d requests", tc.name, got, e.hits.Load())
		}
		if tc.notFinal {
			p := filepath.Join(dir, dispatchFile)
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			var s dispatchSeries
			if err := json.Unmarshal(b, &s); err != nil || !s.Final {
				t.Fatalf("series %+v: %v", s, err)
			}
			through := s.StartUTC.Add(time.Duration(len(s.Counts)) * time.Second)
			s.Final, s.ThroughUTC = false, &through
			if err := replaceJSON(p, &s); err != nil {
				t.Fatal(err)
			}
		}
		sent := e.hits.Load()
		waits := countWaits(r)
		stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
		if err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != sent || !strings.Contains(stdout, "held until "+r.ackFile(cellLabel(run))+" exists") {
			t.Fatalf("%s: restart: exit %d, %v; %d waits, %d requests:\n%s", tc.name, code, err, *waits, e.hits.Load()-sent, stdout)
		}
		stopPublishedFromRecord(t, r, e.hits.Load, cellLabel(run))
		rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
		if err != nil || int64(rr.billing[0].Responses) != e.hits.Load()-1 {
			t.Fatalf("%s: pages billing %+v, %d refusals answered: %v", tc.name, rr.billing, e.hits.Load()-1, err)
		}
	}
}

// A probe record that cannot be read stops no start of a route whose
// running record a manifest labels; with no other label it stops the
// start before anything is sent, and once readable labels the record
// stopped and holds the route.
func TestProbeRecordReadOnlyWithoutOtherLabel(t *testing.T) {
	r, e := billingStopRoute(t)
	run := r.runs[0]
	dir := r.cellDir(run)
	live := r.newRecord(run)
	live.Outcome = outcomeRunning
	if rec, err := r.runCell(context.Background(), run); err != nil || rec.Outcome != outcomeStopped {
		t.Fatalf("fixture: %+v, %v", rec, err)
	}
	if err := replaceJSON(filepath.Join(dir, cellFile), live); err != nil {
		t.Fatal(err)
	}
	pp := filepath.Join(dir, probe.File)
	if err := os.Chmod(pp, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(pp, 0o600) }) //nolint:errcheck
	sent := e.hits.Load()
	waits := countWaits(r)
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != sent {
		t.Fatalf("manifest: exit %d, %v; %d waits, %d requests", code, err, *waits, e.hits.Load()-sent)
	}
	acknowledge(t, r, cellLabel(run))
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 1 || e.hits.Load() != sent {
		t.Fatalf("manifest, acknowledged: exit %d, %v; %d waits, %d requests", code, err, *waits, e.hits.Load()-sent)
	}

	r, e = billingStopRoute(t)
	run = r.runs[0]
	dir = r.cellDir(run)
	blocks := []string{filepath.Join(dir, cellFile+".tmp"), r.stopMarkerPath(run) + ".tmp"}
	r.refresh = append([]string{"/bin/sh", "-c", `mkdir -p "$0" "$1" && shift && exec "$@"`, blocks[0], blocks[1]}, r.refresh...)
	r.wait = noWait
	stderrOf(t, func() { r.loop(context.Background()) }) //nolint:errcheck
	r.refresh = r.refresh[5:]
	for _, b := range blocks {
		if err := os.RemoveAll(b); err != nil {
			t.Fatal(err)
		}
	}
	pp = filepath.Join(dir, probe.File)
	if err := os.Chmod(pp, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(pp, 0o600) }) //nolint:errcheck
	sent = e.hits.Load()
	waits = countWaits(r)
	if code, err := r.loop(context.Background()); err == nil || code != exitError || *waits != 0 || e.hits.Load() != sent || readCell(t, r, run).Outcome != outcomeRunning {
		t.Fatalf("probe record only: exit %d, %v; %d waits, %d requests", code, err, *waits, e.hits.Load()-sent)
	}
	if err := os.Chmod(pp, 0o600); err != nil {
		t.Fatal(err)
	}
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != sent || readCell(t, r, run).Outcome != outcomeStopped {
		t.Fatalf("probe record readable: exit %d, %v; %d waits, %d requests", code, err, *waits, e.hits.Load()-sent)
	}
}

// decidedRefusals is the billing refusal count of the decided label in
// the running record in dir, 0 where it has none or does not decode.
func decidedRefusals(dir string) int {
	b, err := os.ReadFile(filepath.Join(dir, cellFile))
	if err != nil {
		return 0
	}
	var rec cellRecord
	if json.Unmarshal(b, &rec) != nil || rec.Decided == nil {
		return 0
	}
	return rec.Decided.BillingRefusals
}

// A process killed while its ladder drains, after two billing refusals
// answered at different times, leaves the running record with both
// counted: the restart holds the route before it sends, and once
// acknowledged the route goes on and pages publishes the count of two.
func TestKilledDuringDrainKeepsBillingRefusals(t *testing.T) {
	snap := t.TempDir()
	var routeDir, cellDir atomic.Pointer[string]
	snapped := make(chan error, 1)
	e := newEndpoint(t, func(n int64) (int, string) {
		switch n {
		case 1:
			return http.StatusOK, ""
		case 2:
			for deadline := time.Now().Add(5 * time.Second); decidedRefusals(*cellDir.Load()) < 2 && time.Now().Before(deadline); {
				time.Sleep(20 * time.Millisecond)
			}
			snapped <- copyTree(*routeDir.Load(), snap)
			return http.StatusOK, ""
		case 3:
			time.Sleep(time.Second)
			return http.StatusPaymentRequired, "no credit"
		default:
			return http.StatusPaymentRequired, "no credit"
		}
	})
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, _ := newTestRoute(t, cfg, "+0.005")
	shortLadder(r)
	run := r.runs[0]
	r.now, r.wait = func() time.Time { return run.Start }, noWait
	dir := r.cellDir(run)
	routeDir.Store(&r.dir)
	cellDir.Store(&dir)
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || readCell(t, r, run).BillingRefusals != 2 {
		t.Fatalf("first start: exit %d, %v; record %+v", code, err, readCell(t, r, run))
	}
	if err := <-snapped; err != nil {
		t.Fatal(err)
	}
	restoreTree(t, r, snap)
	if got := readCell(t, r, run); got.Outcome != outcomeRunning || got.Decided == nil || got.Decided.BillingRefusals != 2 {
		t.Fatalf("record at the kill %+v", got)
	}
	sent := e.hits.Load()
	waits := countWaits(r)
	var (
		code int
		err  error
	)
	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != sent || !strings.Contains(stdout, "held until "+r.ackFile(cellLabel(run))+" exists") {
		t.Fatalf("restart: exit %d, %v; %d waits, %d requests:\n%s", code, err, *waits, e.hits.Load()-sent, stdout)
	}
	stopPublishedFromRecord(t, r, e.hits.Load, cellLabel(run))
	if got := readCell(t, r, run); got.BillingRefusals != 2 || got.Error != "the process ended before the cell's evidence was written" {
		t.Fatalf("record %+v", got)
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || rr.billing[0].Responses != 2 {
		t.Fatalf("pages billing %+v: %v", rr.billing, err)
	}
	if lines := stoppedCells(rr.stops); len(lines) != 1 || !strings.Contains(lines[0].line(), "billing refusals: 2;") {
		t.Fatalf("stopped cells %+v", lines)
	}
}

// A billing refusal inside the ladder writes the cell's running record
// with the stop as its decided label at once, while other requests hang.
func TestBillingRefusalWritesDecidedRecordAtOnce(t *testing.T) {
	refused := make(chan time.Time, 1)
	srv, _ := hangingEndpoint(t, func(n int64) int {
		if n == 2 {
			refused <- time.Now()
			return http.StatusPaymentRequired
		}
		return 0
	})
	cfg := releaseConfig(t, srv.URL+"/openai/v1/chat/completions")
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, _ := newTestRoute(t, cfg, "+0.005")
	shortLadder(r)
	r.cfg.Client.HTTPTimeoutS = 3
	run := r.runs[0]
	path := filepath.Join(r.cellDir(run), cellFile)
	seen := make(chan time.Duration, 1)
	go func() {
		at := <-refused
		for deadline := at.Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if b, err := os.ReadFile(path); err == nil && bytes.Contains(b, []byte(`"decided"`)) {
				seen <- time.Since(at)
				return
			}
		}
		seen <- -1
	}()
	rec, err := r.runCell(context.Background(), run)
	if err != nil || rec.Outcome != outcomeStopped || rec.BillingRefusals != 1 {
		t.Fatalf("cell %+v: %v", rec, err)
	}
	if d := <-seen; d < 0 || d > time.Second {
		t.Fatalf("decided label on disk %s after the refusal", d)
	}
}

// A stopped record with no manifest gives its billing stop to the pages,
// counted apart from missed and interrupted cells; a route whose only
// record is such a stop gets a page and an index line stating it.
func TestStoppedRecordWithoutManifestPages(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return 402, "no credit" })
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, f := newTestRoute(t, cfg, "+0.005")
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	frames := filepath.Join(evidence.FramesDir(r.framesRoot, run.Start), evidence.FramesFile)
	if err := os.MkdirAll(filepath.Dir(frames), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(frames, []byte("{\"index\":0}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.wait = noWait
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || rr.verdict != nil || rr.unrecorded != 0 || len(rr.stops) != 1 || len(rr.billing) != 1 {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
	if bs := rr.billing[0]; bs.Cell != cellLabel(run) || bs.Responses != 1 || len(bs.Reasons) != 1 || !strings.Contains(bs.Reasons[0], "402") {
		t.Fatalf("billing stop %+v", bs)
	}
	cfgs := append([]*config.Config{r.cfg}, f.in.hostCfgs...)
	rp, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, f.in.probeFiles, cfgs, r.key, "")
	if err != nil {
		t.Fatal(err)
	}
	index := string(rp.files[indexPage])
	if len(rp.index.Stopped) != 1 || !strings.Contains(index, "Stopped with no cell manifest") || !strings.Contains(index, cellLabel(run)+" stopped: ") || !strings.Contains(index, "402") {
		t.Fatalf("index %+v:\n%s", rp.index.Stopped, index)
	}
	if page := string(rp.files[rp.index.Stopped[0].Page]); !strings.Contains(page, cellLabel(run)+" stopped: ") {
		t.Fatalf("route page %q", page)
	}
}

// A stopped cell whose record cannot be written leaves a stop marker that
// holds the route at the next start until it is acknowledged; the record
// takes the marker's label, which the pages publish as a billing stop.
func TestStopRecordUnwrittenLeavesMarker(t *testing.T) {
	r, e, f := billingStopFixture(t)
	run := r.runs[0]
	blocker := filepath.Join(r.cellDir(run), cellFile+".tmp")
	r.refresh = append([]string{"/bin/sh", "-c", `mkdir "$0" && exec "$@"`, blocker}, r.refresh...)
	r.wait = noWait
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	if got := readCell(t, r, run); got.Outcome != outcomeRunning || got.Decided != nil {
		t.Fatalf("record %+v", got)
	}
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	hits := e.hits.Load()
	waits := countWaits(r)
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	holds, err := r.holds(st)
	if err != nil || len(holds) != 1 || !strings.Contains(holds[0], run.Label+" attempt 1 stopped at ") || !strings.Contains(holds[0], "402") ||
		!strings.Contains(holds[0], "resume-"+r.cfg.Target.Name+"-"+stopPrefix+cellLabel(run)) {
		t.Fatalf("holds %q: %v", holds, err)
	}
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 0 || e.hits.Load() != hits {
		t.Fatalf("restart: exit %d, %v; %d waits", code, err, *waits)
	}
	if got := readCell(t, r, run); got.Outcome != outcomeStopped || got.BillingRefusals != 1 || !strings.Contains(fmt.Sprint(got.Reasons), "402") {
		t.Fatalf("restarted record %+v", got)
	}
	acknowledge(t, r, stopPrefix+cellLabel(run))
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped || *waits != 1 || e.hits.Load() != hits {
		t.Fatalf("acknowledged restart: exit %d, %v; %d waits", code, err, *waits)
	}
	if fileExists(filepath.Join(r.cellDir(run), evidence.ManifestFile)) {
		t.Fatal("the stopped cell was labelled")
	}
	rr, err := readRouteCells(r.out, r.framesRoot, r.cfg, r.key)
	if err != nil || rr.labelled != 0 || len(rr.stops) != 1 || len(rr.billing) != 1 || rr.billing[0].Cell != cellLabel(run) {
		t.Fatalf("pages: %+v, %v", rr, err)
	}
	if index := string(stopsPages(t, r, f).files[indexPage]); !strings.Contains(index, cellLabel(run)+" stopped: ") || !strings.Contains(index, "402") {
		t.Fatalf("index:\n%s", index)
	}
}
