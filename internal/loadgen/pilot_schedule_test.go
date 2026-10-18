package loadgen

import (
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// The pilot reference schedules six warm-up requests, then 120 measured,
// one every 10 s from the epoch.
func TestPilotReferenceSchedule(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s := BuildSchedule(cfg)
	if len(s) != 126 {
		t.Fatalf("%d scheduled requests, want 126", len(s))
	}
	warmup := 0
	for i, off := range s {
		if want := int64(i) * 10e9; off != want {
			t.Fatalf("offset %d = %d ns, want %d", i, off, want)
		}
		if off < int64(cfg.Run.Phases.WarmupS*1e9) {
			warmup++
		}
	}
	if warmup != 6 {
		t.Errorf("%d warm-up requests, want 6", warmup)
	}
}
