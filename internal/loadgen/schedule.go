package loadgen

import (
	"math"
	"math/rand"

	"github.com/percentes/percentes/internal/config"
)

// BuildSchedule fixes every intended dispatch time in advance (§3), as
// nanosecond offsets from the run epoch on the monotonic clock, covering
// warm-up + baseline + fault window + cooldown, or a release cell's
// ladder (BuildLadder). The schedule is immutable: every entry is a
// sample, and dispatching all of them is a run gate (AC2c). Poisson uses
// the run seed, so a run's schedule is reproducible.
func BuildSchedule(cfg *config.Config) []int64 {
	if cfg.Profile == config.ProfileRelease {
		if l := BuildLadder(cfg); l != nil {
			return l.Offsets
		}
		return nil
	}
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

// Ladder is a release cell's dispatch schedule: the warm-up at the first
// step's rate from the epoch, then per step a measured window followed
// by a drain in which nothing is scheduled (SPEC-HOSTED HA3 point 5).
type Ladder struct {
	// Offsets holds every intended dispatch time in ascending order; the
	// warm-up requests are Offsets[:Steps[0].First].
	Offsets []int64
	Steps   []LadderStep
	// EndNs is the last drain's end, the cell's length.
	EndNs int64
}

// LadderStep is one step: requests [First, End) of the schedule fall
// in [StartNs, EndNs), and the drain runs from EndNs to DrainEndNs.
type LadderStep struct {
	RateRPS    float64
	StartNs    int64
	EndNs      int64
	DrainEndNs int64
	First, End int
}

// BuildLadder returns the release ladder of cfg, or nil when cfg has no
// release section or no steps. Arrivals restart at each window's start
// from one generator seeded by the run seed.
func BuildLadder(cfg *config.Config) *Ladder {
	if cfg.Release == nil || len(cfg.Release.Ladder.StepsRPS) == 0 {
		return nil
	}
	ld := cfg.Release.Ladder
	rng := rand.New(rand.NewSource(cfg.Run.Seed))
	l := &Ladder{}
	l.Offsets = arrivals(l.Offsets, cfg.Load.ArrivalProcess, rng, ld.StepsRPS[0], 0, secondsNs(ld.WarmupS))
	t := secondsNs(ld.WarmupS)
	for _, rate := range ld.StepsRPS {
		st := LadderStep{RateRPS: rate, StartNs: t, EndNs: t + secondsNs(ld.StepS), First: len(l.Offsets)}
		st.DrainEndNs = st.EndNs + secondsNs(ld.DrainS)
		l.Offsets = arrivals(l.Offsets, cfg.Load.ArrivalProcess, rng, rate, st.StartNs, st.EndNs)
		st.End = len(l.Offsets)
		l.Steps = append(l.Steps, st)
		t = st.DrainEndNs
	}
	l.EndNs = t
	return l
}

// arrivals appends the dispatch times of one window [startNs, endNs) at
// rate requests per second.
func arrivals(offsets []int64, process string, rng *rand.Rand, rate float64, startNs, endNs int64) []int64 {
	switch process {
	case "deterministic":
		n := int64(math.Floor(float64(endNs-startNs) / 1e9 * rate))
		for i := int64(0); i < n; i++ {
			offsets = append(offsets, startNs+int64(float64(i)*1e9/rate))
		}
	case "poisson":
		for t := startNs; ; {
			t += int64(rng.ExpFloat64() / rate * 1e9)
			if t >= endNs {
				break
			}
			offsets = append(offsets, t)
		}
	}
	return offsets
}

func secondsNs(s float64) int64 { return int64(s * 1e9) }
