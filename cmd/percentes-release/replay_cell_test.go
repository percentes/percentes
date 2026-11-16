package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/report"
)

// replayCell accepts a cell's report pair rendered again from its
// configuration and refuses a pair whose text differs, naming the file
// and the configuration.
func TestReplayCellRefusesEditedReport(t *testing.T) {
	r, _ := newTestRoute(t, releaseConfig(t, newEndpoint(t, always200).url()), "+0.005")
	shortLadder(r)
	r.steps = 1
	run := r.runs[0]
	if _, err := r.runCell(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	checkRetained(t, r, run)
	dir := r.cellDir(run)
	cfgPath := writeFile(t, t.TempDir(), "groq.yaml", r.cfg.Raw)
	if err := replayCell(dir, cfgPath); err != nil {
		t.Fatalf("replay: %v", err)
	}
	text := filepath.Join(dir, report.TextFile)
	b, err := os.ReadFile(text)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(text, append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := replayCell(dir, cfgPath); err == nil || !strings.Contains(err.Error(), "report.txt differs from the one rendered from groq.yaml") {
		t.Fatalf("an edited report: %v", err)
	}
}
