// chaoscampaign runs an N=5 (variant, config) campaign — the SPEC.md §5
// repetition unit — and writes the campaign report pair (§5/§7 statistics
// + §10 validity gates). Phase 0 drives it against the mock; Phase 1
// swaps in the clean-delete or node-partition injector and supplies the
// GPU-cluster observations. Exit codes: 0 = every run valid, 2 = campaign
// completed but at least one run failed a run-validity gate, 1 = error.
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

	"github.com/itsveems/chaosserve/internal/campaign"
	"github.com/itsveems/chaosserve/internal/config"
	"github.com/itsveems/chaosserve/internal/orchestrator"
	"github.com/itsveems/chaosserve/internal/report"
	"github.com/itsveems/chaosserve/internal/run"
	"github.com/itsveems/chaosserve/internal/validity"
)

func main() {
	configPath := flag.String("config", "", "path to the run config YAML (required)")
	outDir := flag.String("out", "results/campaign", "output directory")
	adminURL := flag.String("admin-url", "", "victim replica admin endpoint (mock variant)")
	injectMode := flag.String("inject-mode", config.MockFaultError, "mock fault mode to arm")
	injectDuration := flag.Float64("inject-duration-s", 10, "armed fault window duration")
	victim := flag.String("victim", "", "victim replica identity (mock: hostname; clean_delete: pod name)")
	namespace := flag.String("namespace", "chaosserve", "victim pod namespace (clean_delete variant)")
	victimNode := flag.String("victim-node", "", "victim node name (black_hole variant)")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("chaoscampaign: --config is required")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("chaoscampaign: %v", err)
	}

	opts := run.Options{
		AdminURL:        *adminURL,
		InjectMode:      *injectMode,
		InjectDurationS: *injectDuration,
		VictimReplica:   *victim,
	}

	// Route the fault variant to its injector. mock → the admin injector
	// via AdminURL (default). clean_delete → grace=0 pod delete through
	// kubectl (runs on any cluster). black_hole → the pre-armed node
	// partition, which needs a real NodeOps and a multi-node cluster; it
	// is refused here with a pointer to the runbook rather than
	// half-armed. The run engine stays agnostic beyond timestamps (§2).
	switch cfg.Fault.Variant {
	case config.VariantCleanDelete:
		if *victim == "" {
			log.Fatal("chaoscampaign: clean_delete requires --victim (pod name)")
		}
		opts.Injector = orchestrator.NewCleanDeleteInjector(orchestrator.KubectlPodOps{}, *namespace, *victim)
	case config.VariantBlackHole:
		if *victimNode == "" {
			log.Fatal("chaoscampaign: black_hole requires --victim-node")
		}
		log.Fatal("chaoscampaign: black_hole needs the pre-armed node-partition NodeOps and a multi-node GPU cluster; " +
			"see docs/PHASE1-RUNBOOK.md §4 (the injector and manifest exist; wiring the real NodeOps is a Phase-1 bring-up step, not a Phase-0 code path)")
	}

	// Evaluate the §10 validity gates per run as the campaign proceeds.
	// Phase 0 supplies no GPU observations, so G5 (and G3/G4 for
	// black-hole) will fail-unobserved — correct: a real characterization
	// campaign must run on hardware.
	//
	// gates is appended to positionally by the runner and consumed by index
	// below (report.GenerateCampaign, the exit-code scan). This is sound
	// only because campaign.Run invokes the runner sequentially, once per
	// run in order; the gate at index i therefore aligns with run i+1. If
	// campaign.Run were ever parallelized this append would race and the
	// index alignment would break.
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
		log.Fatalf("chaoscampaign: %v", err)
	}

	rawJSON, humanText, err := report.GenerateCampaign(rep, gates)
	if err != nil {
		log.Fatalf("chaoscampaign: %v", err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("chaoscampaign: %v", err)
	}
	jsonPath := filepath.Join(*outDir, "campaign.json")
	txtPath := filepath.Join(*outDir, "campaign.txt")
	if err := os.WriteFile(jsonPath, rawJSON, 0o644); err != nil {
		log.Fatalf("chaoscampaign: %v", err)
	}
	if err := os.WriteFile(txtPath, []byte(humanText), 0o644); err != nil {
		log.Fatalf("chaoscampaign: %v", err)
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
