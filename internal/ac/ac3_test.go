package ac

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/itsveems/chaosserve/internal/config"
	"github.com/itsveems/chaosserve/internal/orchestrator"
)

// TestAC3InjectionTiming: the pre-armed fault fires within the pinned
// +-500 ms of T_inject, and armed/fire/expiry timestamps are recorded.
func TestAC3InjectionTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	cfg := buildConfig(t, scenario{
		warmupS: 1, baselineS: 3, windowS: 8, cooldownS: 1, tInjectS: 3,
		ttft: fixed(20), itl: fixed(2),
	})
	base := startMockProcess(t, cfg)

	epoch := time.Now()
	tInject := time.Duration(cfg.Run.Phases.WarmupS+cfg.Fault.TInjectOffsetS) * time.Second // 4s into the run

	inj := orchestrator.NewMockInjector(base, config.MockFaultError, 0)
	ts, err := orchestrator.Execute(context.Background(), inj, epoch, tInject, 2.0)
	if err != nil {
		t.Fatalf("AC3: orchestrator: %v", err)
	}

	if ts.ArmedAt.IsZero() || ts.ObservedFire == nil || ts.ObservedExpiry == nil {
		t.Fatalf("AC3: armed/fire/expiry must all be recorded: %+v", ts)
	}
	if !ts.ArmedAt.Before(ts.PlannedFireAt) {
		t.Errorf("AC3: injection must be pre-armed: armed %v, planned fire %v", ts.ArmedAt, ts.PlannedFireAt)
	}
	errMs, err := ts.FireErrorMs()
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(errMs) > float64(cfg.Fault.InjectionToleranceMs) {
		t.Errorf("AC3: fault fired %.1fms from T_inject; pinned tolerance is +-%dms", errMs, cfg.Fault.InjectionToleranceMs)
	}
	expiryErrMs := float64(ts.ObservedExpiry.Sub(ts.PlannedExpiry).Microseconds()) / 1000
	if math.Abs(expiryErrMs) > float64(cfg.Fault.InjectionToleranceMs) {
		t.Errorf("AC3: expiry off by %.1fms; pre-armed auto-expiry must hold the same tolerance", expiryErrMs)
	}
	t.Logf("AC3: fire error %.1fms, expiry error %.1fms (tolerance +-%dms)", errMs, expiryErrMs, cfg.Fault.InjectionToleranceMs)
}

// TestAC3RefusesPostHocInjection: arming after the fire time violates
// pre-armed semantics and must be an error, not a late fire.
func TestAC3RefusesPostHocInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	cfg := buildConfig(t, scenario{
		warmupS: 1, baselineS: 2, windowS: 4, cooldownS: 1, tInjectS: 2,
		ttft: fixed(20), itl: fixed(2),
	})
	base := startMockProcess(t, cfg)

	epoch := time.Now().Add(-10 * time.Second) // T_inject already passed
	inj := orchestrator.NewMockInjector(base, config.MockFaultError, 0)
	if _, err := orchestrator.Execute(context.Background(), inj, epoch, 3*time.Second, 1.0); err == nil {
		t.Fatal("AC3: arming a fault whose fire time has passed must fail (pre-armed semantics)")
	}
}
