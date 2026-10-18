// percentes runs one full experiment from one config file and writes the
// JavaScript Object Notation (JSON) and human-readable report pair, and
// the per-attempt JSON Lines file of every run that dispatched, failed
// runs included. A hosted run also writes its raw-frame store under
// --frames, a directory outside the instrument's source tree. Exit codes:
// 0 = run valid, 2 = run completed but a run-failing gate marked it
// invalid, 1 = execution error.
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

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/serverstats"
	"github.com/percentes/percentes/internal/validity"
)

func main() {
	configPath := flag.String("config", "", "path to the run config YAML (required)")
	outDir := flag.String("out", "results", "output directory for report.json, report.txt and "+evidence.AttemptsFile)
	adminURL := flag.String("admin-url", "", "victim replica admin endpoint; when set the orchestrator pre-arms the fault there")
	injectMode := flag.String("inject-mode", config.MockFaultError, "mock fault mode to arm (stall|error|throttle|stream_abort|silent_hang)")
	injectDuration := flag.Float64("inject-duration-s", 10, "armed fault window duration")
	victim := flag.String("victim", "", "victim replica identity (pod name) for in-flight attribution")
	probeDirect := flag.String("probe-direct", "", "victim-direct inference URL for the replica-ready probe")
	probeService := flag.String("probe-service", "", "service inference URL for the traffic-restored probe")
	framesRoot := flag.String("frames", "", "raw-frame store directory outside the source tree (hosted targets, required); each run writes <UTC epoch>/"+evidence.FramesFile)
	flag.Parse()
	for name, u := range map[string]string{"--probe-direct": *probeDirect, "--probe-service": *probeService} {
		if u == "" {
			continue
		}
		if err := config.CheckBaseURL(u); err != nil {
			log.Fatalf("percentes: %s: %v", name, err)
		}
	}

	if *configPath == "" {
		log.Fatal("percentes: --config is required")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("percentes: %v", err)
	}
	if cfg.Target.Hosted {
		if err := evidence.CheckFramesRoot(*framesRoot); err != nil {
			log.Fatalf("percentes: %v", err)
		}
	} else if *framesRoot != "" {
		log.Fatal("percentes: --frames applies to hosted targets only")
	}

	// Wire an interrupt-aware context so Ctrl-C / SIGTERM cancels the in-flight
	// experiment, letting Execute unwind and disarm a fault already armed on the
	// victim replica rather than leaving it armed. Cancellation surfaces as an
	// Execute error and terminates via the exit-code-1 (execution error) path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A wrong endpoint or metric name fails here, before the load.
	for _, u := range cfg.Target.MetricsURLs {
		if err := serverstats.Preflight(ctx, &http.Client{Timeout: 5 * time.Second}, u, cfg.Target.QueueGauge, cfg.Target.MetricsFamilies, cfg.Target.TTFTHistogram); err != nil {
			log.Fatalf("percentes: %v", err)
		}
	}
	// §10 G7: sample each replica's waiting-queue gauge from the run
	// epoch when target.metrics_urls is configured.
	sampler := serverstats.ForRun(cfg.Target.MetricsURLs, cfg.Target.QueueGauge, cfg.Target.MetricsFamilies, time.Duration(config.PinnedQueueSampleIntervalS)*time.Second)
	// The §2 loopback canary runs through the same read path from the epoch.
	var canary *loadgen.Canary
	var canaryErr error
	onEpoch := func(e time.Time) {
		if sampler != nil {
			sampler.Start(ctx)
		}
		canary, canaryErr = loadgen.StartCanary(ctx, e)
	}
	art, err := run.Execute(ctx, cfg, run.Options{
		AdminURL:        *adminURL,
		InjectMode:      *injectMode,
		InjectDurationS: *injectDuration,
		VictimReplica:   *victim,
		ProbeDirectURL:  *probeDirect,
		ProbeServiceURL: *probeService,
		OnEpoch:         onEpoch,
	})
	var streams []loadgen.CanaryStream
	if canary != nil {
		streams = canary.Stop()
	}
	if art != nil && cfg.Target.Hosted {
		if path, ferr := evidence.WriteRunFrames(*framesRoot, art.Loadgen); ferr != nil {
			if err == nil {
				log.Fatalf("percentes: %v", ferr)
			}
			log.Printf("percentes: %v", ferr)
		} else if path != "" {
			log.Printf("percentes: frames written: %s", path)
		}
	}
	if err != nil {
		if sampler != nil {
			sampler.Stop()
		}
		if art != nil {
			if path, werr := evidence.WriteRun(*outDir, cfg, art.Loadgen, evidence.Options{RunFailures: art.InvalidReasons}); werr != nil {
				log.Printf("percentes: %v", werr)
			} else if path != "" {
				log.Printf("percentes: attempts written: %s", path)
			}
		}
		log.Fatalf("percentes: %v", err)
	}
	if canaryErr != nil {
		log.Printf("percentes: loopback canary did not run: %v", canaryErr)
	}
	obs := validity.Observations{}
	observed := run.Observed{TTFTFamily: cfg.Target.TTFTHistogram, Canary: streams, CanaryErr: canaryErr}
	if sampler != nil {
		samples, errs := sampler.Stop()
		observed.Samples, observed.FamilyErrors = samples, len(sampler.FamilyErrors())
		startNs, endNs := art.BaselineNs()
		means := serverstats.BaselineMeans(samples, art.Loadgen.EpochWall, startNs, endNs)
		obs.Queue = &validity.QueueObservation{Gauge: cfg.Target.QueueGauge, IntervalS: config.PinnedQueueSampleIntervalS, Means: means, ScrapeErrors: len(errs)}
	}
	art.AttachObservations(observed)

	// Run validity includes the §10 gate evaluation; a failed gate
	// reaches the exit code and the report. G1 and G2 are skipped here
	// because run.Execute already records their failures.
	gates := validity.Evaluate(art, obs)
	if reasons := gates.FailReasons("G1", "G2"); len(reasons) > 0 {
		art.RunValid = false
		art.InvalidReasons = append(art.InvalidReasons, reasons...)
	}

	attemptsPath, err := evidence.WriteRun(*outDir, cfg, art.Loadgen, evidence.Options{RunFailures: art.InvalidReasons})
	if err != nil {
		log.Fatalf("percentes: %v", err)
	}

	rawJSON, humanText, err := report.Generate(art, &gates)
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
	fmt.Printf("\nreports written: %s, %s\nattempts written: %s\n", jsonPath, txtPath, attemptsPath)
	if !art.RunValid {
		fmt.Println("RUN INVALID (run-failing gate):", art.InvalidReasons)
		os.Exit(2)
	}
}
