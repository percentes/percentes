package run

import (
	"context"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// A throttle window puts status_429 in the fault window's error classes
// and nowhere else (§3).
func TestThrottleWindowCountsStatus429(t *testing.T) {
	cfg := quickCfg(t)
	cfg.Mock.FaultSchedule = []config.MockFault{{Mode: config.MockFaultThrottle, StartOffsetS: 5, DurationS: 1}}
	base := startMock(t, cfg)
	cfg.Target.BaseURL = base

	art, err := Execute(context.Background(), cfg, Options{AdminURL: base})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if n := art.Windows["fault"].ErrClasses["status_429"]; n == 0 {
		t.Fatalf("fault window must count status_429, got classes %v", art.Windows["fault"].ErrClasses)
	}
	if n := art.Windows["baseline"].ErrClasses["status_429"]; n != 0 {
		t.Fatalf("baseline must carry no status_429, got %d", n)
	}
	if hostContended(art) {
		t.Skipf("host contended the client: %+v", art.Loadgen.Gates)
	}
	if !art.RunValid {
		t.Fatalf("a throttle run must stay valid, reasons: %v", art.InvalidReasons)
	}
}
