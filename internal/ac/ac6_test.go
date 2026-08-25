package ac

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/validity"
)

// TestAC6Reporting: JSON plus human-readable report from one config,
// including completion-incidence curves, failure rates, the sensitivity table, and the
// conditional headline, with the instrument caveat in the output.
func TestAC6Reporting(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	// The fault window must hold the outage (5 s), the recovery, and the
	// detector's full 30 s hold after the entry candidate; the pre-fault
	// phase must exceed the pinned 30 s timeout so the baseline window
	// carries traffic alongside the guard.
	cfg := buildConfig(t, scenario{
		warmupS: 1, baselineS: 38, windowS: 45, cooldownS: 1, tInjectS: 38,
		ttft: fixed(100), itl: fixed(5),
		schedule: []config.MockFault{{Mode: config.MockFaultError, StartOffsetS: 39, DurationS: 5}},
	})
	base := startMockProcess(t, cfg)
	cfg.Target.BaseURL = base

	// The schedule-driven fault is attested by fire read-back (§2), so the
	// run needs the mock's admin endpoint.
	art, err := run.Execute(context.Background(), cfg, run.Options{AdminURL: base})
	if err != nil {
		t.Fatalf("AC6: run: %v", err)
	}
	gates := validity.Evaluate(art, validity.Observations{})
	rawJSON, humanText, err := report.Generate(art, &gates)
	if err != nil {
		t.Fatalf("AC6: generate: %v", err)
	}

	// JSON artifact: parses, and carries every required product.
	var parsed map[string]any
	if err := json.Unmarshal(rawJSON, &parsed); err != nil {
		t.Fatalf("AC6: report.json must parse: %v", err)
	}
	for _, key := range []string{"config", "config_sha256", "caveat", "conditional_headline", "windows", "detector", "in_flight_at_fire", "decomposition", "share_gate"} {
		if _, ok := parsed[key]; !ok {
			t.Errorf("AC6: report.json missing %q", key)
		}
	}
	windows := parsed["windows"].(map[string]any)
	for _, name := range []string{"baseline", "guard", "fault"} {
		wobj, ok := windows[name].(map[string]any)
		if !ok {
			t.Fatalf("AC6: window %q missing", name)
		}
		cif := wobj["completion_incidence"].(map[string]any)
		if cif["n"].(float64) == 0 {
			t.Errorf("AC6: window %q completion-incidence curve is empty", name)
		}
		if _, ok := wobj["error_rate"]; !ok {
			t.Errorf("AC6: window %q missing failure rates", name)
		}
	}
	det := parsed["detector"].(map[string]any)
	if rows := det["sensitivity"].([]any); len(rows) != 27 {
		t.Errorf("AC6: sensitivity table must have 27 rows, got %d", len(rows))
	}
	// §4: the goodput-versus-threshold sweep and the modal/baseline-SD
	// statement must be present in the report.
	for _, name := range []string{"baseline", "guard", "fault"} {
		if sweep := windows[name].(map[string]any)["goodput_sweep"].([]any); len(sweep) != 9 {
			t.Errorf("AC6: window %q goodput sweep must have 9 cells, got %d", name, len(sweep))
		}
	}
	ta, ok := parsed["threshold_analysis"].(map[string]any)
	if !ok || ta["valid"] != true {
		t.Errorf("AC6: §4 threshold analysis missing or invalid: %v", ta)
	}
	baseWin := windows["baseline"].(map[string]any)
	if ci, ok := baseWin["ttft_tail_ci"].(map[string]any); !ok || ci["p95"] == nil {
		t.Error("AC6: §7 order-statistic tail CIs missing from baseline window")
	}
	for _, want := range []string{"goodput-versus-threshold sweep", "Modal during-fault latency", "baseline SD"} {
		if !strings.Contains(humanText, want) {
			t.Errorf("AC6: human report missing %q", want)
		}
	}
	// The guard window is reported in full and labeled, so its numbers
	// cannot be read as pre-fault degradation (§3).
	if !strings.Contains(humanText, "PRE-FAULT GUARD WINDOW (§3)") {
		t.Error("AC6: the guard window must be labeled where it is rendered")
	}

	// Human-readable artifact.
	for _, want := range []string{
		"Completion incidence, Aalen-Johansen",
		"Sensitivity table",
		"conditional on completion",
		"Conditional headline",
		"certifies the instrument",
		"Recovery (two baselines",
		"In-flight loss accounting",
		"decomposition",
	} {
		if !strings.Contains(humanText, want) {
			t.Errorf("AC6: human report missing %q", want)
		}
	}

	// The detector saw the scripted 5 s outage and recovered.
	if art.Detector.ToPreFault.TTRSeconds == nil {
		t.Error("AC6: scenario should recover within the window")
	}
	if !art.RunValid {
		t.Errorf("AC6: clean scenario must be a valid run: %v", art.InvalidReasons)
	}
	t.Logf("AC6: report.json %d bytes, report.txt %d bytes", len(rawJSON), len(humanText))
}
