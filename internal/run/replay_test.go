package run_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/run"
)

// Replay reads hosted runs only: a self-hosted run's analysis reads the
// replica attribution the per-attempt records do not carry.
func TestReplayRefusesASelfHostedConfig(t *testing.T) {
	cfg := g7Cfg(t)
	if _, err := run.Replay(cfg, time.Now(), nil, &run.Record{}); err == nil || !strings.Contains(err.Error(), "not hosted") {
		t.Fatalf("got %v", err)
	}
}

// The record holds the client gate, the tripwires, each window's pooled
// gaps and canary summary in name order, and the canary's start error.
func TestRecordOfARun(t *testing.T) {
	canary := &loadgen.CanarySummary{TTFTMs: 20, Streams: 3, Completed: 3}
	art := &run.Artifacts{
		Loadgen: &loadgen.Result{Gates: loadgen.GateReport{Undispatched: 2, CPUMeasured: true}, Tripwires: &loadgen.Tripwires{Scheduled: 126, Fired: true}},
		Windows: map[string]*collect.Stats{
			"guard":    {Window: collect.Window{Name: "guard", StartNs: 2, EndNs: 3}, ITLPooled: histo.Summary{Count: 4, P50Us: 10}},
			"baseline": {Window: collect.Window{Name: "baseline", StartNs: 1, EndNs: 2}, ITLPooled: histo.Summary{Count: 7, MaxUs: 99}},
		},
		ReceivePath: map[string]*collect.ReceivePath{
			"baseline": {Canary: canary, CanaryError: "canary: refused"},
			"guard":    {CanaryError: "canary: refused"},
		},
	}
	want := &run.Record{
		ClientGates: art.Loadgen.Gates,
		Tripwires:   art.Loadgen.Tripwires,
		Windows: []run.WindowRecord{
			{Window: collect.Window{Name: "baseline", StartNs: 1, EndNs: 2}, ITLPooled: histo.Summary{Count: 7, MaxUs: 99}, Canary: canary},
			{Window: collect.Window{Name: "guard", StartNs: 2, EndNs: 3}, ITLPooled: histo.Summary{Count: 4, P50Us: 10}},
		},
		CanaryError: "canary: refused",
	}
	if got := art.Record(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}
