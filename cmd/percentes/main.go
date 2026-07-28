// percentes runs one full experiment from one config file and writes the
// report pair (JSON + human-readable). Exit codes: 0 = run valid, 2 = run
// completed but a run-failing gate marked it invalid, 1 = execution error.
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

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/run"
)

func main() {
	configPath := flag.String("config", "", "path to the run config YAML (required)")
	outDir := flag.String("out", "results", "output directory for report.json and report.txt")
	adminURL := flag.String("admin-url", "", "victim replica admin endpoint; when set the orchestrator pre-arms the fault there")
	injectMode := flag.String("inject-mode", config.MockFaultError, "mock fault mode to arm (stall|error|stream_abort|silent_hang)")
	injectDuration := flag.Float64("inject-duration-s", 10, "armed fault window duration")
	victim := flag.String("victim", "", "victim replica identity (pod name) for in-flight attribution")
	probeDirect := flag.String("probe-direct", "", "victim-direct inference URL for the replica-ready probe")
	probeService := flag.String("probe-service", "", "service inference URL for the traffic-restored probe")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("percentes: --config is required")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("percentes: %v", err)
	}

	// Wire an interrupt-aware context so Ctrl-C / SIGTERM cancels the in-flight
	// experiment, letting Execute unwind and disarm a fault already armed on the
	// victim replica rather than leaving it armed. Cancellation surfaces as an
	// Execute error and terminates via the exit-code-1 (execution error) path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	art, err := run.Execute(ctx, cfg, run.Options{
		AdminURL:        *adminURL,
		InjectMode:      *injectMode,
		InjectDurationS: *injectDuration,
		VictimReplica:   *victim,
		ProbeDirectURL:  *probeDirect,
		ProbeServiceURL: *probeService,
	})
	if err != nil {
		log.Fatalf("percentes: %v", err)
	}

	rawJSON, humanText, err := report.Generate(art)
	if err != nil {
		log.Fatalf("percentes: %v", err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("percentes: %v", err)
	}
	jsonPath := filepath.Join(*outDir, "report.json")
	txtPath := filepath.Join(*outDir, "report.txt")
	if err := os.WriteFile(jsonPath, rawJSON, 0o644); err != nil {
		log.Fatalf("percentes: %v", err)
	}
	if err := os.WriteFile(txtPath, []byte(humanText), 0o644); err != nil {
		log.Fatalf("percentes: %v", err)
	}

	fmt.Println(humanText)
	fmt.Printf("\nreports written: %s, %s\n", jsonPath, txtPath)
	if !art.RunValid {
		fmt.Println("RUN INVALID (run-failing gate):", art.InvalidReasons)
		os.Exit(2)
	}
}
