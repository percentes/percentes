package detect

import (
	"context"
	"testing"
	"time"

	"github.com/itsveems/chaosserve/internal/config"
	"github.com/itsveems/chaosserve/internal/mock"
)

// Against the mock (whose /health and inference become ready together at
// the end of slow_reload), the calibration reports both boundaries and a
// near-zero gap — proving the instrument measures the relationship rather
// than assuming it.
func TestCalibrateHealthAgainstMock(t *testing.T) {
	m := config.Mock{
		ListenAddr: "127.0.0.1:0",
		Seed:       1,
		TTFT:       config.LatencyDist{Distribution: "fixed", FixedMs: 10},
		ITL:        config.LatencyDist{Distribution: "fixed", FixedMs: 2},
		SlowReload: config.SlowReload{Enabled: true, DurationS: 1.0},
	}
	srv := mock.New(m)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := "http://" + srv.Addr()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cal, err := CalibrateHealth(ctx, base, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("calibration: %v", err)
	}
	if !cal.HealthOK || !cal.InferenceOK {
		t.Fatalf("both boundaries must be measured: %+v", cal)
	}
	if cal.HealthReadyAfterS < 0.9 || cal.InferenceReadyAfterS < 0.9 {
		t.Errorf("both should become ready after the ~1s slow reload: %+v", cal)
	}
	if cal.GapS < -0.4 || cal.GapS > 0.4 {
		t.Errorf("mock health and serve readiness coincide; gap %.3fs too large", cal.GapS)
	}
	if cal.Note == "" {
		t.Error("calibration must document the relationship")
	}
}
