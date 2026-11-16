package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
)

// A stop marker written without its cell's requests sent gives the
// restarted cell its probe's status and request from the probe record.
func TestStopMarkerWithoutSentReadsProbeRecord(t *testing.T) {
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
	m, err := r.stopMarker(run)
	if err != nil || m == nil || m.Sent == nil {
		t.Fatalf("marker %+v, %v", m, err)
	}
	m.Sent, m.ProbeStatus = nil, 0
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.stopMarkerPath(run), b, 0o600); err != nil {
		t.Fatal(err)
	}
	stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped {
		t.Fatalf("restart: exit %d, %v", code, err)
	}
	if got := readCell(t, r, run); got.ProbeStatus != http.StatusOK || got.Sent.Probe != 1 || got.Sent.Warmup+got.Sent.Measured != 0 || got.Outcome != outcomeStopped {
		t.Fatalf("record %s, sent %+v, probe status %d", got.Outcome, got.Sent, got.ProbeStatus)
	}
}
