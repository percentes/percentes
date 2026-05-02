package loadgen

import (
	"math"
	"math/rand"

	"github.com/itsveems/chaosserve/internal/config"
)

// BuildSchedule fixes every intended dispatch time in advance (§3), as
// nanosecond offsets from the run epoch on the monotonic clock, covering
// warm-up + baseline + fault window + cooldown. The schedule is immutable:
// every entry is a sample, and dispatching all of them is a run gate
// (AC2c). Poisson uses the run seed, so a run's schedule is reproducible.
func BuildSchedule(cfg *config.Config) []int64 {
	p := cfg.Run.Phases
	totalS := p.WarmupS + p.BaselineS + p.FaultWindowTimeoutS + p.CooldownS
	rate := cfg.Load.RateRPS

	var offsets []int64
	switch cfg.Load.ArrivalProcess {
	case "deterministic":
		n := int64(math.Floor(totalS * rate))
		interval := 1e9 / rate // ns
		offsets = make([]int64, 0, n)
		for i := int64(0); i < n; i++ {
			offsets = append(offsets, int64(float64(i)*interval))
		}
	case "poisson":
		rng := rand.New(rand.NewSource(cfg.Run.Seed))
		totalNs := int64(totalS * 1e9)
		t := int64(0)
		for {
			t += int64(rng.ExpFloat64() / rate * 1e9)
			if t >= totalNs {
				break
			}
			offsets = append(offsets, t)
		}
	}
	return offsets
}
