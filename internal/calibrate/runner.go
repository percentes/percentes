package calibrate

import (
	"context"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/serverstats"
)

// LoadRunner drives the load generator against one replica addressed
// directly (§10: no Service routing), samples that replica's waiting-queue
// gauge, and reduces the measured portion to the step's numbers.
type LoadRunner struct {
	// Base is the validated experiment configuration. Each step derives
	// its own copy: the phases become settle then measure, the rate and
	// seed are the step's, and the target is the one replica.
	Base       *config.Config
	TargetURL  string
	MetricsURL string
	Gauge      string
	// SampleInterval zero selects the pinned cadence.
	SampleInterval time.Duration
}

func (r *LoadRunner) RunStep(ctx context.Context, sp Spec) (Step, error) {
	cfg := *r.Base
	cfg.Target.BaseURL = r.TargetURL
	cfg.Target.Replicas = 1
	cfg.Target.MetricsURLs = nil
	cfg.Run.Seed = sp.Seed
	cfg.Run.Phases = config.Phases{WarmupS: sp.SettleS, BaselineS: sp.MeasureS}
	cfg.Load.RateRPS = sp.RateRPS
	// Nothing is armed; T_inject is recorded at the measured end.
	cfg.Fault.TInjectOffsetS = sp.MeasureS

	step := Step{Spec: sp}
	interval := r.SampleInterval
	if interval == 0 {
		interval = time.Duration(config.PinnedQueueSampleIntervalS) * time.Second
	}
	var sampler *serverstats.Sampler
	if r.MetricsURL != "" {
		sampler = serverstats.ForRun([]string{r.MetricsURL}, r.Gauge, interval)
	}
	res, err := loadgen.Run(ctx, &cfg, &loadgen.Hooks{OnEpoch: func(e time.Time) {
		step.StartedWall = e
		if sampler != nil {
			sampler.Start(ctx)
		}
	}})
	if err != nil {
		if sampler != nil {
			sampler.Stop()
		}
		return step, err
	}

	w := collect.Window{Name: "measured", StartNs: res.WarmupEndNs, EndNs: res.BaselineEndNs}
	step.MeasuredStartWall = res.EpochWall.Add(time.Duration(w.StartNs))
	step.MeasuredEndWall = res.EpochWall.Add(time.Duration(w.EndNs))
	st, err := collect.Collect(&cfg, res.Requests, w)
	if err != nil {
		if sampler != nil {
			sampler.Stop()
		}
		return step, err
	}
	step.Stats, step.Goodput, step.Gates = st, st.GoodputFrac, res.Gates

	if sampler != nil {
		samples, errs := sampler.Stop()
		step.ScrapeErrors = len(errs)
		step.QueueSeries = samples
		step.QueueIntervalS = interval.Seconds()
		step.QueueExpected = int(sp.MeasureS / step.QueueIntervalS)
		if m, ok := serverstats.BaselineMeans(samples, res.EpochWall, w.StartNs, w.EndNs)["r0"]; ok {
			step.QueueMean, step.QueueSamples = m.Value, m.Samples
		}
	}
	return step, nil
}
