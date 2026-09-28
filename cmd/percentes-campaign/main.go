// percentes-campaign runs a campaign of N runs of one (variant, config),
// the SPEC.md §5 repetition unit, where the repetition count N is pinned
// to 5 by the experiment profile, and writes the campaign report pair
// (§5/§7 statistics + §10 validity gates). Phase 0 drives it against the
// mock; Phase 1 swaps in the clean-delete or node-partition injector and
// supplies the graphics processing unit (GPU) cluster observations.
// Exit codes: 0 = every run valid, 2 = campaign completed but at least
// one run failed a run-validity gate, 1 = error.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/serverstats"
	"github.com/percentes/percentes/internal/validity"
)

func main() {
	configPath := flag.String("config", "", "path to the run config YAML (required)")
	outDir := flag.String("out", "results/campaign", "output directory")
	adminURL := flag.String("admin-url", "", "victim replica admin endpoint (mock variant)")
	injectMode := flag.String("inject-mode", config.MockFaultError, "mock fault mode to arm")
	injectDuration := flag.Float64("inject-duration-s", 10, "armed fault window duration (mock injector only)")
	victim := flag.String("victim", "", "victim replica identity (mock: hostname; clean_delete: pod name)")
	namespace := flag.String("namespace", "percentes", "victim pod namespace (clean_delete variant)")
	victimNode := flag.String("victim-node", "", "victim node name (black_hole variant)")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("percentes-campaign: --config is required")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}

	opts := run.Options{
		AdminURL:        *adminURL,
		InjectMode:      *injectMode,
		InjectDurationS: *injectDuration,
		VictimReplica:   *victim,
	}
	// §1: the black-hole partition expires at the pinned configuration
	// duration; --inject-duration-s serves the mock injector.
	if cfg.Fault.Variant == config.VariantBlackHole {
		opts.InjectDurationS = float64(cfg.Fault.PartitionDurationS)
	}

	// Route fault.variant to an injector: mock uses the admin injector at
	// AdminURL (default); clean_delete does a grace=0 pod delete via
	// kubectl; black_hole needs a real NodeOps and a multi-node cluster and
	// is refused here (SPEC.md §10); none arms nothing (§6). The run engine
	// stays agnostic beyond timestamps (§2).
	switch cfg.Fault.Variant {
	case config.VariantNone:
		opts.AdminURL = ""
	case config.VariantCleanDelete:
		if *victim == "" {
			log.Fatal("percentes-campaign: clean_delete requires --victim (pod name)")
		}
		opts.Injector = orchestrator.NewCleanDeleteInjector(orchestrator.KubectlPodOps{}, *namespace, *victim)
	case config.VariantBlackHole:
		if *victimNode == "" {
			log.Fatal("percentes-campaign: black_hole requires --victim-node")
		}
		log.Fatal("percentes-campaign: black_hole requires a multi-node GPU cluster and a real NodeOps; see SPEC.md §10")
	}

	// Evaluate the §10 validity gates per run and fold the verdict into the
	// run's validity: campaign counts and endpoint summaries key off it.
	// G5 with no fingerprint collector reports not applicable; a failed G4
	// strips the node-loss-representative label without invalidating (§10).
	//
	// gates[i] pairs with run i+1; campaign.Run calls the runner
	// sequentially. Parallelizing it would race this append.
	var gates []validity.Report
	runner := func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		// §10 G7: a fresh sampler per run, started on that run's epoch.
		sampler := serverstats.ForRun(c.Target.MetricsURLs, c.Target.QueueGauge, c.Target.MetricsFamilies, time.Duration(config.PinnedQueueSampleIntervalS)*time.Second)
		var canary *loadgen.Canary
		var canaryErr error
		o.OnEpoch = func(e time.Time) {
			if sampler != nil {
				sampler.Start(ctx)
			}
			canary, canaryErr = loadgen.StartCanary(ctx, e)
		}
		art, err := run.Execute(ctx, c, o)
		var streams []loadgen.CanaryStream
		if canary != nil {
			streams = canary.Stop()
		}
		if err != nil {
			if sampler != nil {
				sampler.Stop()
			}
			return nil, err
		}
		if canaryErr != nil {
			log.Printf("percentes-campaign: loopback canary did not run: %v", canaryErr)
		}
		obs := validity.Observations{}
		observed := run.Observed{TTFTFamily: c.Target.TTFTHistogram, Canary: streams, CanaryErr: canaryErr}
		if sampler != nil {
			samples, errs := sampler.Stop()
			observed.Samples, observed.FamilyErrors = samples, len(sampler.FamilyErrors())
			startNs, endNs := art.BaselineNs()
			means := serverstats.BaselineMeans(samples, art.Loadgen.EpochWall, startNs, endNs)
			obs.Queue = &validity.QueueObservation{Gauge: c.Target.QueueGauge, IntervalS: config.PinnedQueueSampleIntervalS, Means: means, ScrapeErrors: len(errs)}
		}
		art.AttachObservations(observed)
		rep := validity.Evaluate(art, obs)
		gates = append(gates, rep)
		if reasons := rep.FailReasons("G1", "G2"); len(reasons) > 0 {
			art.RunValid = false
			art.InvalidReasons = append(art.InvalidReasons, reasons...)
		}
		return art, nil
	}

	// Interrupt-safe teardown: a SIGINT/SIGTERM cancels the campaign context
	// so an in-flight run.Execute unwinds cleanly rather than being killed
	// mid-fault. A cancelled run surfaces as an error and exits 1, preserving
	// the 0/1/2 contract (2 is reserved for a completed campaign with a
	// failed run-validity gate).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A wrong endpoint or metric name fails here, before the first run.
	for _, u := range cfg.Target.MetricsURLs {
		if err := serverstats.Preflight(ctx, &http.Client{Timeout: 5 * time.Second}, u, cfg.Target.QueueGauge, cfg.Target.MetricsFamilies, cfg.Target.TTFTHistogram); err != nil {
			log.Fatalf("percentes-campaign: %v", err)
		}
	}

	rep, err := campaign.Run(ctx, cfg, opts, cfg.Fault.Variant, runner)
	if err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}

	rawJSON, humanText, err := report.GenerateCampaign(rep, gates)
	if err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}
	jsonPath := filepath.Join(*outDir, "campaign.json")
	txtPath := filepath.Join(*outDir, "campaign.txt")
	if err := os.WriteFile(jsonPath, rawJSON, 0o644); err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}
	if err := os.WriteFile(txtPath, []byte(humanText), 0o644); err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}

	fmt.Println(humanText)
	fmt.Printf("\ncampaign reports: %s, %s\n", jsonPath, txtPath)

	allValid := rep.ValidRuns == rep.Repetitions
	for _, g := range gates {
		if !g.AllPass {
			allValid = false
		}
	}
	if !allValid {
		fmt.Println("CAMPAIGN NOTE: at least one run is invalid (§10 gate or run audit); see per-run gates and invalid_reasons above.")
		os.Exit(2)
	}
}
