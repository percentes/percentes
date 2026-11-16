package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// A cell whose process ended after its attempts file and before its
// manifest, relabelled interrupted with no end and no epoch, is a gap
// bounded by its dispatch series: from its first counted second to the
// hard timeout after its last. Its attempts file is not read, and its
// end is the series' last flush.
func TestOrphanCellGapBounded(t *testing.T) {
	cfg := releaseConfig(t, "http://127.0.0.1:1/v1/chat/completions")
	at := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	rec := cellRecord{Outcome: outcomeInterrupted, StartUTC: at, Reasons: []string{"the process ended during the cell"},
		Run: schedule.Run{Route: cfg.Target.Name, Label: "x", Attempt: 1, Start: at}}
	dir := t.TempDir()
	through := at.Add(130 * time.Second)
	series, err := json.Marshal(dispatchSeries{Run: rec.Run, StartUTC: at, Counts: []int{0, 0, 3, 3, 3}, ThroughUTC: &through})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, dispatchFile, series)
	var buf bytes.Buffer
	if err := evidence.Write(&buf, []evidence.Record{{Index: 0, DispatchNs: 2e9, WroteRequestNs: 2e9 + 1}, {Index: 1, DispatchNs: 3e9, WroteRequestNs: 3e9 + 1}}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, evidence.AttemptsFile, buf.Bytes())
	h := &report.History{Group: cfg.Release.AccountGroup, Routes: []string{cfg.Target.Name}}
	if err := cellSends(h, dir, cfg, h.Routes, map[string]bool{}, &rec); err != nil {
		t.Fatal(err)
	}
	timeout := time.Duration(cfg.Client.HTTPTimeoutS) * time.Second
	if len(h.Sends) != 0 || len(h.Gaps) != 1 {
		t.Fatalf("sends %+v, gaps %+v", h.Sends, h.Gaps)
	}
	g := h.Gaps[0]
	if !g.FromUTC.Equal(at.Add(2*time.Second)) || g.ToUTC == nil || !g.ToUTC.Equal(at.Add(5*time.Second+timeout)) || !strings.HasSuffix(g.Reason, "its dispatch series counts 9 dispatches") {
		t.Fatalf("gap %+v", g)
	}
	if end, err := cellEnd(dir, &rec); err != nil || !end.Equal(through) {
		t.Fatalf("cell end %s: %v", end, err)
	}
}
