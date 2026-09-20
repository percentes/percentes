package run_test

import (
	"context"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/serverstats"
	"github.com/percentes/percentes/internal/validity"
)

// External test package: validity imports run, so the wiring from
// target.metrics_urls through the sampler to the G7 verdict is exercised
// from outside package run.

func g7Cfg(t *testing.T) *config.Config {
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
	// No mock-scripted schedule: its offsets count from the mock's start,
	// before the run epoch. The tests arm through the admin endpoint at
	// T_inject, as the command does.
	cfg.Mock.FaultSchedule = nil
	return cfg
}

func armed(base string) run.Options {
	return run.Options{AdminURL: base, InjectMode: config.MockFaultError, InjectDurationS: 1}
}

func startMockFor(t *testing.T, cfg *config.Config) string {
	t.Helper()
	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return "http://" + srv.Addr()
}

// With target.metrics_urls configured, the sampler starts on the run epoch
// and G7 evaluates the mock's waiting gauge over the §3 baseline window,
// guard excluded.
func TestG7EvaluatesFromMockScrape(t *testing.T) {
	cfg := g7Cfg(t)
	// The §3 guard window is one pinned 30 s client timeout before
	// T_inject, so a shorter baseline has no measured portion; 40 s
	// leaves 10 s for G6 and for the samples G7 averages.
	cfg.Run.Phases.BaselineS = 40
	cfg.Fault.TInjectOffsetS = 40
	base := startMockFor(t, cfg)
	cfg.Target.BaseURL = base
	cfg.Target.MetricsURLs = []string{base + "/metrics"}
	cfg.Target.QueueGauge = "percentes_mock_requests_waiting"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	sampler := serverstats.ForRun(cfg.Target.MetricsURLs, cfg.Target.QueueGauge, 200*time.Millisecond)
	opts := armed(base)
	opts.OnEpoch = func(time.Time) { sampler.Start(context.Background()) }
	art, err := run.Execute(context.Background(), cfg, opts)
	if err != nil {
		sampler.Stop()
		t.Fatalf("execute: %v", err)
	}
	startNs, endNs := art.BaselineNs()
	means, scrapeErrs := sampler.Reduce(art.Loadgen.EpochWall, startNs, endNs)
	obs := validity.Observations{Queue: &validity.QueueObservation{Gauge: cfg.Target.QueueGauge, IntervalS: 0.2, Means: means, ScrapeErrors: scrapeErrs}}
	rep := validity.Evaluate(art, obs)

	// The §2 gate measures this machine and host load inflates every term,
	// including the CPU gate, whose 5 s windowed mean can pass while the
	// peak is near saturation. A failed gate cannot judge the sampler.
	if g := art.Loadgen.Gates; !g.Pass {
		t.Skipf("host contended the client: %+v", g)
	}
	var g7 validity.Gate
	for _, g := range rep.Gates {
		if g.ID == "G7" {
			g7 = g
		}
	}
	if !g7.Applicable || !g7.Observed || !g7.Pass {
		t.Fatalf("G7 must be applicable, observed and passing from the mock scrape, got %+v", g7)
	}
	t.Logf("G7: %s", g7.Detail)
	// The measured baseline is [1 s, 11 s): 40 s configured, the last 30 s
	// guard. A 200 ms ticker cannot place more than 51 samples in it, so a
	// count above that means the guard window leaked into the mean, and G7
	// coverage needs 45 of the 50 the cadence expects.
	m, ok := means["r0"]
	if !ok || m.Samples < 45 || m.Samples > 51 || m.Value != 0 {
		t.Fatalf("expected 45 to 51 baseline samples at 0 for r0 over the 10 s window [%d ns, %d ns), got %+v (errors %d)", startNs, endNs, m, scrapeErrs)
	}
	if !rep.AllPass {
		b := art.Windows["baseline"]
		t.Fatalf("run must be valid.\nbaseline window: %+v\ndetector: %+v\ngates: %+v", b, art.Detector, rep.Gates)
	}
}

// Unset, the field leaves G7 not applicable, as in the §8 profile.
func TestG7NotApplicableWhenUnset(t *testing.T) {
	cfg := g7Cfg(t)
	base := startMockFor(t, cfg)
	cfg.Target.BaseURL = base
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	art, err := run.Execute(context.Background(), cfg, armed(base))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	for _, g := range validity.Evaluate(art, validity.Observations{}).Gates {
		if g.ID == "G7" && (g.Applicable || g.Observed || g.Pass) {
			t.Fatalf("G7 must be not applicable when metrics_urls is unset, got %+v", g)
		}
	}
}
