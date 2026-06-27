package campaign_test

import (
	"context"
	"strings"
	"testing"

	"github.com/itsveems/chaosserve/internal/campaign"
	"github.com/itsveems/chaosserve/internal/config"
	"github.com/itsveems/chaosserve/internal/mock"
	"github.com/itsveems/chaosserve/internal/report"
	"github.com/itsveems/chaosserve/internal/run"
	"github.com/itsveems/chaosserve/internal/validity"
)

// A small real campaign through run.Execute against the mock proves the
// whole stack composes: the Runner seam works with the real runner,
// per-run seeds vary, the §7 summaries render, and the campaign report
// generates. Kept short (2 runs, tiny phases) so it lives in the -race
// unit suite.
func TestCampaignThroughRealRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in -short mode")
	}
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Run.Repetitions = 2
	cfg.Run.Phases = config.Phases{WarmupS: 1, BaselineS: 5, FaultWindowTimeoutS: 8, CooldownS: 1}
	cfg.Fault.Variant = config.VariantMock
	cfg.Fault.TInjectOffsetS = 5
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Target.Replicas = 1
	cfg.Mock.ListenAddr = "127.0.0.1:0"
	cfg.Mock.TTFT = config.LatencyDist{Distribution: "fixed", FixedMs: 20}
	cfg.Mock.ITL = config.LatencyDist{Distribution: "fixed", FixedMs: 2}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := "http://" + srv.Addr()
	cfg.Target.BaseURL = base

	var gates []validity.Report
	var seeds []int64
	runner := func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		seeds = append(seeds, c.Run.Seed)
		c.Target.BaseURL = base
		art, err := run.Execute(ctx, c, o)
		if err != nil {
			return nil, err
		}
		gates = append(gates, validity.Evaluate(art, validity.Observations{}))
		return art, nil
	}

	opts := run.Options{AdminURL: base, InjectMode: config.MockFaultError, InjectDurationS: 3}
	rep, err := campaign.Run(context.Background(), cfg, opts, config.VariantMock, runner)
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}

	if len(rep.PerRun) != 2 {
		t.Fatalf("want 2 runs, got %d", len(rep.PerRun))
	}
	if seeds[0] == seeds[1] {
		t.Error("per-run seeds must differ (independent repetitions)")
	}
	if len(rep.Endpoints) == 0 {
		t.Error("endpoint summaries must be produced")
	}

	rawJSON, humanText, err := report.GenerateCampaign(rep, gates)
	if err != nil {
		t.Fatalf("generate campaign: %v", err)
	}
	if len(rawJSON) == 0 || !strings.Contains(humanText, "Per-run scalars") {
		t.Error("campaign report must render")
	}
	if !strings.Contains(humanText, "G1") || !strings.Contains(humanText, "G6") {
		t.Error("campaign report must include the §10 validity gates")
	}
	// G2 (client-validity) should be observable and reported per run.
	for _, gr := range gates {
		found := false
		for _, g := range gr.Gates {
			if g.ID == "G2" {
				found = true
			}
		}
		if !found {
			t.Error("every run must evaluate G2")
		}
	}
}
