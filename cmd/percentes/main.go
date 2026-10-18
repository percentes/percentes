// percentes runs one full experiment from one config file and writes the
// JavaScript Object Notation (JSON) and human-readable report pair, and
// the per-attempt JSON Lines file of every run that dispatched, failed
// runs included. A hosted run also writes its raw-frame store under
// --frames, a directory outside the instrument's source tree, the client
// clock offset that --clock-sync reads at run start, the endpoint status
// that --status-cmd reads at run start and end, and the run manifest,
// which hashes those files, the config, the --operator records and the
// --ledger position. Exit codes:
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
	"runtime/debug"
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
	clockSync := flag.String("clock-sync", "", "host clock-sync command run at run start, split on spaces with no shell: sntp [options] server, or chronyc [options] tracking (hosted targets, required); writes "+evidence.ClockFile+" to --out")
	statusCmd := flag.String("status-cmd", "", "endpoint-status command run at run start and at run end, split on spaces with no shell (hosted targets, required); writes "+evidence.StatusStartFile+" and "+evidence.StatusEndFile+" to --out")
	operatorPath := flag.String("operator", "", "operator file naming the provider, account tier, client host and the record files the run relies on, hashed at start and copied beside the frames (hosted targets, required)")
	ledgerPath := flag.String("ledger", "", "admission ledger whose position at run start "+evidence.ManifestFile+" records (hosted targets)")
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
	var (
		clockCmd   evidence.ClockCommand
		statusArgs []string
		op         *evidence.Operator
	)
	build, _ := debug.ReadBuildInfo()
	if cfg.Target.Hosted {
		if err := evidence.CheckFramesRoot(*framesRoot); err != nil {
			log.Fatalf("percentes: %v", err)
		}
		if clockCmd, err = evidence.ParseClockCommand(*clockSync); err != nil {
			log.Fatalf("percentes: %v", err)
		}
		if statusArgs, err = evidence.ParseStatusCommand(*statusCmd); err != nil {
			log.Fatalf("percentes: %v", err)
		}
		if op, err = evidence.LoadOperator(*operatorPath); err != nil {
			log.Fatalf("percentes: %v", err)
		}
		if err := evidence.CheckSource(build, op); err != nil {
			log.Fatalf("percentes: %v", err)
		}
	} else if *framesRoot != "" || *clockSync != "" || *statusCmd != "" || *operatorPath != "" || *ledgerPath != "" {
		log.Fatal("percentes: --frames, --clock-sync, --status-cmd, --operator and --ledger apply to hosted targets only")
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
	// A flagged clock reading invalidates the run without stopping it.
	var (
		clock           *evidence.ClockRecord
		clockPath       string
		ledger          *evidence.LedgerRef
		statusStartPath string
	)
	if cfg.Target.Hosted {
		if *ledgerPath != "" {
			if ledger, err = evidence.ReadLedger(*ledgerPath); err != nil {
				log.Fatalf("percentes: %v", err)
			}
		}
		if statusStartPath, err = evidence.WriteStatus(*outDir, evidence.StatusStartFile, evidence.ReadStatus(ctx, statusArgs)); err != nil {
			log.Fatalf("percentes: %v", err)
		}
		log.Printf("percentes: endpoint status written: %s", statusStartPath)
		clock = evidence.ReadClock(ctx, clockCmd)
		var cerr error
		if clockPath, cerr = evidence.WriteClock(*outDir, clock); cerr != nil {
			log.Fatalf("percentes: %v", cerr)
		}
		log.Printf("percentes: clock offset written: %s", clockPath)
		if clock.Flag != "" {
			log.Printf("percentes: %s", clock.Flag)
		}
	}
	flagClock := func(a *run.Artifacts) {
		if clock != nil && clock.Flag != "" {
			a.RunValid = false
			a.InvalidReasons = append(a.InvalidReasons, clock.Flag)
		}
	}
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
	var statusEndPath, framesPath string
	if art != nil && cfg.Target.Hosted {
		// A failed write leaves status_end out of the manifest.
		if path, serr := evidence.WriteStatus(*outDir, evidence.StatusEndFile, evidence.ReadStatus(ctx, statusArgs)); serr != nil {
			log.Printf("percentes: %v", serr)
		} else {
			statusEndPath = path
			log.Printf("percentes: endpoint status written: %s", path)
		}
		var ferr error
		if framesPath, ferr = evidence.WriteRunFrames(*framesRoot, art.Loadgen); ferr == nil && framesPath != "" {
			ferr = op.WriteCopies(filepath.Dir(framesPath))
		}
		if ferr != nil {
			if err == nil {
				log.Fatalf("percentes: %v", ferr)
			}
			log.Printf("percentes: %v", ferr)
		} else if framesPath != "" {
			log.Printf("percentes: frames and operator records written: %s", filepath.Dir(framesPath))
		}
	}
	writeManifest := func(gates *validity.Report, artifacts ...string) (string, error) {
		m, merr := evidence.NewManifest(evidence.ManifestInput{
			Config:          cfg,
			ConfigName:      filepath.Base(*configPath),
			Target:          &cfg.Target,
			Result:          art.Loadgen,
			Build:           build,
			Operator:        op,
			Ledger:          ledger,
			Clock:           clock,
			ClockPath:       clockPath,
			StatusStartPath: statusStartPath,
			StatusEndPath:   statusEndPath,
			ArtifactPaths:   artifacts,
			FramesRoot:      *framesRoot,
			FramesPath:      framesPath,
			Gates:           gates,
			RunValid:        art.RunValid,
			InvalidReasons:  art.InvalidReasons,
		})
		if merr != nil {
			return "", merr
		}
		return evidence.WriteManifest(*outDir, m)
	}
	if err != nil {
		if sampler != nil {
			sampler.Stop()
		}
		if art != nil {
			flagClock(art)
			path, werr := evidence.WriteRun(*outDir, cfg, art.Loadgen, evidence.Options{RunFailures: art.InvalidReasons})
			if werr != nil {
				log.Printf("percentes: %v", werr)
			} else if path != "" {
				log.Printf("percentes: attempts written: %s", path)
			}
			if cfg.Target.Hosted && art.Loadgen != nil {
				var written []string
				if path != "" {
					written = append(written, path)
				}
				if mpath, merr := writeManifest(nil, written...); merr != nil {
					log.Printf("percentes: %v", merr)
				} else {
					log.Printf("percentes: manifest written: %s", mpath)
				}
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
	flagClock(art)

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

	if cfg.Target.Hosted {
		mpath, merr := writeManifest(&gates, attemptsPath, jsonPath, txtPath)
		if merr != nil {
			log.Fatalf("percentes: %v", merr)
		}
		log.Printf("percentes: manifest written: %s", mpath)
	}

	fmt.Println(humanText)
	fmt.Printf("\nreports written: %s, %s\nattempts written: %s\n", jsonPath, txtPath, attemptsPath)
	if !art.RunValid {
		fmt.Println("RUN INVALID (run-failing gate):", art.InvalidReasons)
		os.Exit(2)
	}
}
