package run

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itsveems/chaosserve/internal/config"
	"github.com/itsveems/chaosserve/internal/mock"
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
// orchestration — the seam the Phase 1 injectors reach the run through.
// Without this the clean-delete/node-partition injectors would be
// unreachable from any binary (the defect this test pins shut).
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

// TestExecuteEndToEndUnderRace runs the complete pipeline — load
// generator (pacer, workers, monitors), orchestrator (pre-armed fault),
// collector, detector, probes — in-process at a small scale, so the
// safety-critical concurrency runs under the race detector in the unit
// suite (`make test-unit` uses -race; the AC suite deliberately does not,
// for timing fidelity, and this test is what makes that split sound).
func TestExecuteEndToEndUnderRace(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Run.Phases = config.Phases{WarmupS: 1, BaselineS: 6, FaultWindowTimeoutS: 10, CooldownS: 1}
	cfg.Fault.TInjectOffsetS = 6
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
	for _, wname := range []string{"baseline", "fault"} {
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
	// The artifacts must be JSON-serializable in full (report embedding).
	if _, err := json.Marshal(art); err != nil {
		t.Fatalf("artifacts must marshal: %v", err)
	}
}
