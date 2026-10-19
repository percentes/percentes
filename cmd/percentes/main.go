// percentes runs one full experiment from one config file and writes the
// JavaScript Object Notation (JSON) and human-readable report pair, and
// the per-attempt JSON Lines file of every run that dispatched, failed
// runs included. It refuses a hosted target (target.hosted): hosted
// traffic runs under percentes-release or percentes-pilot, which count
// each request in the admission ledger before it is sent. --replay DIR
// renders a finished hosted run's report pair again from its manifest,
// its per-attempt file and the config file alone, writes it to --out and
// compares it with the pair DIR's manifest names. --check-release DIR
// refuses a hosted run's release unless the commit record, the config,
// the per-attempt file, the report pair, the manifest, the scorer output
// and the frame store under --frames all exist with the hashes DIR's
// manifest records, and prints the label of a release cell that does not
// count. --release-key names the release key, which --check-release
// needs on a route with a confidential limit, whose frame store and
// operator records the manifest names by HMAC-SHA256 under it.
// Exit codes: 0 = run valid, the replayed pair matches byte for byte, or
// the release check passed; 2 = run completed but a run-failing gate
// marked it invalid; 1 = execution error, a replayed pair that differs,
// or a refused release.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	"github.com/percentes/percentes/internal/releasekey"
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
	framesRoot := flag.String("frames", "", "raw-frame store directory, outside the source tree, holding the frames of the run --check-release reads")
	replayDir := flag.String("replay", "", "output directory of a finished hosted run: render its report pair again from "+evidence.ManifestFile+", "+evidence.AttemptsFile+" and --config into --out, and compare it with the pair the manifest names")
	keyPath := flag.String("release-key", "", "the release key file, which --check-release needs on a route with a confidential limit")
	checkDir := flag.String("check-release", "", "output directory of a finished hosted run: refuse its release unless the commit record, --config, "+evidence.AttemptsFile+", the report pair, "+evidence.ManifestFile+", the scorer output and the frame store under --frames exist with the hashes the manifest records")
	flag.Parse()
	if *checkDir != "" {
		if *replayDir != "" {
			log.Fatal("percentes: --check-release and --replay are separate commands")
		}
		if *configPath == "" || *framesRoot == "" {
			log.Fatal("percentes: --check-release needs --config, the run's configuration file, and --frames, the frame store root")
		}
		raw, err := os.ReadFile(*configPath)
		if err != nil {
			log.Fatalf("percentes: %v", err)
		}
		var key *releasekey.Key
		if *keyPath != "" {
			if key, err = releasekey.Read(*keyPath); err != nil {
				log.Fatalf("percentes: %v", err)
			}
		}
		m, err := evidence.CheckRelease(evidence.Release{Dir: *checkDir, Config: raw, FramesRoot: *framesRoot, Pair: [2]string{report.JSONFile, report.TextFile}, Key: key})
		if err != nil {
			log.Fatalf("percentes: %v", err)
		}
		fmt.Printf("release check passed: %s\n", *checkDir)
		if c := m.Release; c != nil && !m.Counted() {
			fmt.Printf("labelled %s, outside the route's frame: %q\n", c.Outcome, c.Reasons)
		}
		return
	}
	if *replayDir != "" {
		if *configPath == "" {
			log.Fatal("percentes: --replay needs --config, the run's configuration file")
		}
		jsonPath, txtPath, err := replay(*replayDir, *configPath, *outDir)
		if err != nil {
			log.Fatalf("percentes: %v", err)
		}
		fmt.Printf("report pair reproduced byte for byte: %s, %s\n", jsonPath, txtPath)
		return
	}
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
	if err := run.CheckConfig(cfg); err != nil {
		log.Fatalf("percentes: %v", err)
	}
	if cfg.Target.Hosted {
		log.Fatal("percentes: a hosted target runs under percentes-release or percentes-pilot only, whose ledger admits each request before it is sent")
	}
	if *framesRoot != "" {
		log.Fatal("percentes: --frames applies to --check-release only")
	}
	if *keyPath != "" {
		log.Fatal("percentes: --release-key applies to --check-release only")
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
	if err != nil {
		if sampler != nil {
			sampler.Stop()
		}
		if art != nil {
			path, werr := evidence.WriteRun(*outDir, cfg, art.Loadgen, evidence.Options{RunFailures: art.InvalidReasons})
			if werr != nil {
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
	jsonPath, txtPath := filepath.Join(*outDir, report.JSONFile), filepath.Join(*outDir, report.TextFile)
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

// replay renders the report pair of the hosted run in dir from its
// manifest, its attempts file and the config at cfgPath, writes it to
// out, and returns an error unless both files match the manifest's
// references byte for byte.
func replay(dir, cfgPath, out string) (string, string, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", "", err
	}
	absOut, err := filepath.Abs(out)
	if err != nil {
		return "", "", err
	}
	if absDir == absOut {
		return "", "", errors.New("--out must differ from the --replay directory")
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return "", "", err
	}
	rawJSON, text, err := report.FromRun(dir, raw)
	if err != nil {
		return "", "", err
	}
	m, err := evidence.ReadManifest(filepath.Join(dir, evidence.ManifestFile))
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return "", "", err
	}
	var differ []string
	for _, f := range []struct {
		name string
		b    []byte
	}{{report.JSONFile, rawJSON}, {report.TextFile, []byte(text)}} {
		if err := os.WriteFile(filepath.Join(out, f.name), f.b, 0o644); err != nil {
			return "", "", err
		}
		sum := sha256.Sum256(f.b)
		if ref := m.Artifact(f.name); ref == nil || ref.SHA256 != hex.EncodeToString(sum[:]) {
			differ = append(differ, f.name)
		}
	}
	if len(differ) > 0 {
		return "", "", fmt.Errorf("replayed %v in %s differ from the manifest's references", differ, out)
	}
	return filepath.Join(out, report.JSONFile), filepath.Join(out, report.TextFile), nil
}
