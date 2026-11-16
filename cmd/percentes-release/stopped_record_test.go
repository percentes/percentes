package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
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
