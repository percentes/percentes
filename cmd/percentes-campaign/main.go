// percentes-campaign runs an N-run (variant, config) campaign: the
// SPEC.md §5 repetition unit, N pinned to 5 by the experiment profile,
// and writes the campaign report pair (§5/§7 statistics + §10 validity
// gates). Phase 0 drives it against the mock; Phase 1 swaps in the
// clean-delete or node-partition injector and supplies the GPU-cluster
// observations. Exit codes: 0 = every run valid, 2 = campaign completed
// but at least one run failed a run-validity gate, 1 = error.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/run"
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

	// Evaluate the §10 validity gates per run. G3 derives from the run's own
	// artifacts (§1(i)); Phase 0 supplies no injected observations, so G5 (and
	// G4 under black_hole) fail unobserved, and a failed G4 strips the
	// node-loss-representative label without invalidating the run (§10).
	//
	// gates[i] pairs with run i+1; campaign.Run calls the runner
	// sequentially. Parallelizing it would race this append.
	var gates []validity.Report
	runner := func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		art, err := run.Execute(ctx, c, o)
		if err != nil {
			return nil, err
		}
		gates = append(gates, validity.Evaluate(art, validity.Observations{}))
		return art, nil
	}

	// Interrupt-safe teardown: a SIGINT/SIGTERM cancels the campaign context
	// so an in-flight run.Execute unwinds cleanly rather than being killed
	// mid-fault. A cancelled run surfaces as an error and exits 1, preserving
	// the 0/1/2 contract (2 is reserved for a completed campaign with a
	// failed run-validity gate).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	allValid := true
	for _, g := range gates {
		if !g.AllPass {
			allValid = false
		}
	}
	if !allValid {
		fmt.Println("CAMPAIGN NOTE: at least one run failed a run-validity gate (§10); see per-run gates above.")
		os.Exit(2)
	}
}
