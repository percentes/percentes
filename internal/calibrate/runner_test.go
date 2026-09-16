package calibrate

import (
	"context"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

// One step against the mock proves the wiring: the load generator holds
// the rate, the measured window is collected, the gauge is sampled over
// the whole step and averaged over the measured window only. A stall
// scheduled inside the settle raises the gauge there, so a mean taken
// over the wrong window is not zero. The measured window exceeds the
// pinned CPU window.
func TestLoadRunnerStepAgainstMock(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Mock.ListenAddr = "127.0.0.1:0"
	cfg.Mock.TTFT = config.LatencyDist{Distribution: "fixed", FixedMs: 20}
	cfg.Mock.ITL = config.LatencyDist{Distribution: "fixed", FixedMs: 2}
	cfg.Mock.FaultSchedule = []config.MockFault{{Mode: config.MockFaultStall, StartOffsetS: 0.5, DurationS: 2}}
	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	base := "http://" + srv.Addr()

	r := &LoadRunner{Base: cfg, TargetURL: base, MetricsURL: base + "/metrics", Gauge: "percentes_mock_requests_waiting", SampleInterval: 200 * time.Millisecond}
	s, err := r.RunStep(context.Background(), Spec{RateRPS: 5, SettleS: 4, MeasureS: 7, Seed: 3})
	if err != nil {
		t.Fatal(err)
	}
	if s.QueueIntervalS != 0.2 || s.QueueExpected != 35 {
		t.Fatalf("cadence %v s, %d expected samples over 7 s", s.QueueIntervalS, s.QueueExpected)
	}
	Judge(&s)
	if !s.Passed() && !s.Gates.Pass && s.Goodput >= config.PinnedCalibrationGoodputMin && s.QueueSamples > 0 {
		t.Skipf("host contended the client: %+v", s.Gates)
	}
	if !s.Passed() {
		t.Fatalf("step must pass on a quiet host: reasons %v gates %+v", s.Reasons, s.Gates)
	}
	if s.Stats == nil || s.Stats.Completed < 10 {
		t.Fatalf("measured window stats %+v", s.Stats)
	}
	if s.QueueMean != 0 || s.QueueSamples < 2 || s.ScrapeErrors != 0 {
		t.Fatalf("gauge over the measured window: mean %v samples %d errors %d", s.QueueMean, s.QueueSamples, s.ScrapeErrors)
	}
	held := 0
	for _, smp := range s.QueueSeries {
		if smp.At.Before(s.MeasuredStartWall) && smp.Value > 0 {
			held++
		}
	}
	if held == 0 {
		t.Fatalf("the settle's stall must show in the full series: %d samples, measured from %v", len(s.QueueSeries), s.MeasuredStartWall)
	}
	if len(s.QueueSeries) <= s.QueueSamples {
		t.Fatalf("series %d must carry the settle samples beyond the %d measured", len(s.QueueSeries), s.QueueSamples)
	}
	if s.StartedWall.IsZero() || s.MeasuredStartWall.Sub(s.StartedWall) != 4*time.Second || s.MeasuredEndWall.Sub(s.MeasuredStartWall) != 7*time.Second {
		t.Fatalf("measured bounds: start %v measured %v to %v", s.StartedWall, s.MeasuredStartWall, s.MeasuredEndWall)
	}
	if s.RateRPS != 5 || s.Seed != 3 {
		t.Fatalf("step did not record its spec: %+v", s.Spec)
	}
}
