package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
)

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
