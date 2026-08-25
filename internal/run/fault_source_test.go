package run

import (
	"context"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

func quickCfg(t *testing.T) *config.Config {
	t.Helper()
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
	return cfg
}

func startMock(t *testing.T, cfg *config.Config) string {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return "http://" + srv.Addr()
}

// A fault-labelled run with nothing armed and nothing attested is invalid.
func TestFaultVariantWithoutSourceInvalid(t *testing.T) {
	cfg := quickCfg(t)
	cfg.Mock.FaultSchedule = nil
	cfg.Target.BaseURL = startMock(t, cfg)

	art, err := Execute(context.Background(), cfg, Options{})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if art.RunValid {
		t.Fatalf("a %q run with no fault source must be invalid", cfg.Fault.Variant)
	}
}

// A schedule-driven run with an admin URL reads its fires back and is valid.
func TestScheduleDrivenRunAttested(t *testing.T) {
	cfg := quickCfg(t)
	cfg.Mock.FaultSchedule = []config.MockFault{{Mode: config.MockFaultError, StartOffsetS: 5, DurationS: 1}}
	base := startMock(t, cfg)
	cfg.Target.BaseURL = base

	art, err := Execute(context.Background(), cfg, Options{AdminURL: base})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if art.ScheduleFired == nil || *art.ScheduleFired == 0 {
		t.Fatalf("schedule fires must be read back, got %v", art.ScheduleFired)
	}
	if !art.RunValid {
		t.Fatalf("an attested schedule-driven run must be valid, reasons: %v", art.InvalidReasons)
	}
	if art.Orchestration != nil {
		t.Fatal("a schedule-driven run must not arm an extra admin fault")
	}
}

// The same run without the admin URL cannot be attested.
func TestScheduleDrivenRunUnattestedInvalid(t *testing.T) {
	cfg := quickCfg(t)
	cfg.Mock.FaultSchedule = []config.MockFault{{Mode: config.MockFaultError, StartOffsetS: 5, DurationS: 1}}
	cfg.Target.BaseURL = startMock(t, cfg)

	art, err := Execute(context.Background(), cfg, Options{})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if art.RunValid {
		t.Fatal("an unattested schedule-driven run must be invalid")
	}
}
