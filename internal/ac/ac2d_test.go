package ac

import (
	"math"
	"runtime"
	"testing"
)

// TestAC2dClientValidityGateFires: under injected client CPU pressure
// (busy-loops on 80% of cores for the whole run), the CPU gate fires on
// the pinned thresholds (sustained > 70% over a 5 s window) rather than
// silently degrading.
func TestAC2dClientValidityGateFires(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}

	hogs := int(math.Ceil(float64(runtime.NumCPU()) * 0.8))
	stop := make(chan struct{})
	for i := 0; i < hogs; i++ {
		go func() {
			x := 0.0
			for {
				select {
				case <-stop:
					return
				default:
					x += math.Sqrt(float64(int(x) % 97))
				}
			}
		}()
	}
	defer close(stop)

	cfg := buildConfig(t, scenario{
		warmupS: 2, baselineS: 10, windowS: 6, cooldownS: 2, tInjectS: 10,
		ttft: fixed(50), itl: fixed(5),
	})
	res, _ := runScenario(t, cfg)

	g := res.Gates
	if g.CPUPass {
		t.Errorf("AC2d: CPU gate must fire under %d busy cores (peak=%.1f%%, worst 5s window=%.1f%%)",
			hogs, g.CPUPeakPct, g.CPUWorstWindowPct)
	}
	if g.CPUWorstWindowPct <= float64(cfg.ClientValidity.MaxCPUPct) {
		t.Errorf("AC2d: worst 5s window %.1f%% should exceed the pinned %d%% threshold under pressure",
			g.CPUWorstWindowPct, cfg.ClientValidity.MaxCPUPct)
	}
	if g.Pass {
		t.Error("AC2d: overall client-validity gate must fail, not silently degrade")
	}
	t.Logf("AC2d: gate fired as required (peak=%.1f%%, worst window=%.1f%%, skew p99=%dus)",
		g.CPUPeakPct, g.CPUWorstWindowPct, g.SendSkewP99Us)
}
