package run

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/mock"
)

// fakeInjector records that Arm ran and reports a fired/expired window,
// standing in for the Phase 1 clean-delete / node-partition injectors.
type fakeInjector struct {
	armed  atomic.Bool
	fireIn time.Duration
	durS   float64
	epoch  time.Time
}

func (f *fakeInjector) Arm(ctx context.Context, fireIn time.Duration, durationS float64) error {
	f.fireIn, f.durS, f.epoch = fireIn, durationS, time.Now()
	f.armed.Store(true)
	return nil
}

func (f *fakeInjector) Observed(ctx context.Context) (fired, expired *time.Time, err error) {
	if !f.armed.Load() {
		return nil, nil, nil
	}
	fa := f.epoch.Add(f.fireIn)
	ex := fa.Add(time.Duration(f.durS * float64(time.Second)))
	return &fa, &ex, nil
}

// A caller-supplied Options.Injector must actually be armed and drive the
// orchestration, the seam the Phase 1 injectors reach the run through.
// Without this the clean-delete/node-partition injectors would be
// unreachable from any binary.
func TestExecuteUsesSuppliedInjector(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Run.Phases = config.Phases{WarmupS: 1, BaselineS: 4, FaultWindowTimeoutS: 5, CooldownS: 1}
	cfg.Fault.TInjectOffsetS = 4
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Target.Replicas = 1
	cfg.Mock.ListenAddr = "127.0.0.1:0"
	cfg.Mock.TTFT = config.LatencyDist{Distribution: "fixed", FixedMs: 20}
	cfg.Mock.ITL = config.LatencyDist{Distribution: "fixed", FixedMs: 2}
	cfg.Mock.FaultSchedule = nil
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	cfg.Target.BaseURL = "http://" + srv.Addr()

	inj := &fakeInjector{}
	art, err := Execute(context.Background(), cfg, Options{Injector: inj, InjectDurationS: 2})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !inj.armed.Load() {
		t.Fatal("the supplied injector must be armed (Phase 1 injectors reach the run through Options.Injector)")
	}
	if art.Orchestration == nil || art.Orchestration.ObservedFire == nil {
		t.Fatal("the supplied injector's fire must be recorded in the orchestration audit")
	}
}

// TestExecuteEndToEndUnderRace runs the complete pipeline (load
// generator (pacer, workers, monitors), orchestrator (pre-armed fault),
// collector, detector, probes) in-process at a small scale, so the
// concurrency runs under -race in the unit suite. The AC suite runs
// without -race, for timing fidelity.
func TestExecuteEndToEndUnderRace(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// The pre-fault phase exceeds the pinned 30 s client timeout so the
	// baseline window carries traffic alongside the guard.
	cfg.Run.Phases = config.Phases{WarmupS: 1, BaselineS: 36, FaultWindowTimeoutS: 10, CooldownS: 1}
	cfg.Fault.TInjectOffsetS = 36
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Target.Replicas = 1
	cfg.Mock.ListenAddr = "127.0.0.1:0"
	cfg.Mock.TTFT = config.LatencyDist{Distribution: "fixed", FixedMs: 20}
	cfg.Mock.ITL = config.LatencyDist{Distribution: "fixed", FixedMs: 2}
	cfg.Mock.FaultSchedule = nil
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	base := "http://" + srv.Addr()
	cfg.Target.BaseURL = base
	victim, _ := os.Hostname()

	art, err := Execute(context.Background(), cfg, Options{
		AdminURL:        base,
		InjectMode:      config.MockFaultError,
		InjectDurationS: 2,
		VictimReplica:   victim,
		ProbeDirectURL:  base,
		ProbeServiceURL: base,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if art.Orchestration == nil || art.Orchestration.ObservedFire == nil {
		t.Fatal("orchestrated fault must record its observed fire")
	}
	// Window set: the baseline ends one pinned client timeout before the
	// fire anchor, and the guard runs from there to T_inject.
	wantGuardStart := art.Loadgen.TInjectNs - int64(cfg.Client.HTTPTimeoutS)*1e9
	if art.ActualFireNs < art.Loadgen.TInjectNs {
		wantGuardStart = art.ActualFireNs - int64(cfg.Client.HTTPTimeoutS)*1e9
	}
	if got := art.Windows["baseline"].Window.EndNs; got != wantGuardStart {
		t.Errorf("baseline must end at the guard start: got %.3fs, want %.3fs", float64(got)/1e9, float64(wantGuardStart)/1e9)
	}
	if g := art.Windows["guard"].Window; g.StartNs != wantGuardStart || g.EndNs != art.Loadgen.TInjectNs {
		t.Errorf("guard window must span [guard start, T_inject): got [%.3fs, %.3fs)", float64(g.StartNs)/1e9, float64(g.EndNs)/1e9)
	}
	for _, wname := range []string{"baseline", "guard", "fault"} {
		st, ok := art.Windows[wname]
		if !ok || st.Scheduled == 0 {
			t.Fatalf("window %q missing or empty", wname)
		}
		if len(st.GoodputSweep) != 9 {
			t.Errorf("window %q: §4 goodput sweep must have 9 cells, got %d", wname, len(st.GoodputSweep))
		}
		if st.Scheduled != st.Completed+st.Errored+st.Censored {
			t.Errorf("window %q: three-state totals must partition", wname)
		}
	}
	if !art.ThresholdAnalysis.Valid {
		t.Errorf("§4 threshold analysis must compute: %+v", art.ThresholdAnalysis)
	}
	if art.VictimReplica != victim {
		t.Error("victim identity must be recorded in the artifacts")
	}
	if art.Windows["fault"].ErrorRate == 0 {
		t.Error("the armed error window must surface in the fault window's error rate")
	}
	// The artifacts must serialize in full as JavaScript Object Notation
	// (JSON), which the report embeds.
	if _, err := json.Marshal(art); err != nil {
		t.Fatalf("artifacts must marshal: %v", err)
	}
}

// The decomposition table follows the fault variant: the Phase 0 rows for
// the mock, the fourteen process-kill rows for process_kill.
func TestExecuteKeepsDecompositionPerVariant(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Run.Phases = config.Phases{WarmupS: 1, BaselineS: 2, FaultWindowTimeoutS: 2, CooldownS: 0}
	cfg.Fault.TInjectOffsetS = 2
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Target.Replicas = 1
	cfg.Mock.ListenAddr = "127.0.0.1:0"
	cfg.Mock.TTFT = config.LatencyDist{Distribution: "fixed", FixedMs: 20}
	cfg.Mock.ITL = config.LatencyDist{Distribution: "fixed", FixedMs: 2}
	cfg.Mock.FaultSchedule = nil
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	cfg.Target.BaseURL = "http://" + srv.Addr()

	for variant, want := range map[string][]string{
		config.VariantMock:        detectRows(config.VariantMock),
		config.VariantProcessKill: detectRows(config.VariantProcessKill),
	} {
		c := *cfg
		c.Fault.Variant = variant
		art, err := Execute(context.Background(), &c, Options{Injector: &fakeInjector{}})
		if err != nil {
			t.Fatalf("%s: execute: %v", variant, err)
		}
		var got []string
		for _, s := range art.Decomposition.Segments {
			got = append(got, s.Name)
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s: rows %v, want %v", variant, got, want)
		}
	}
	if n := len(detectRows(config.VariantProcessKill)); n != 14 {
		t.Fatalf("process_kill has %d rows, want 14", n)
	}
}

func detectRows(variant string) []string {
	var names []string
	for _, s := range detect.NewDecomposition(variant).Segments {
		names = append(names, s.Name)
	}
	return names
}
