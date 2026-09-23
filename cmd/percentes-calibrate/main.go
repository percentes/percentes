// percentes-calibrate measures one replica's capacity lambda_max by the
// SPEC §10 ramp, freezes lambda_r, runs the §5 reference step at
// 2 lambda_r, and writes the full trace as calibration.json and
// calibration.txt. The output directory is created and written before the
// first step, and both files are rewritten after every step, so a
// completed step survives an interrupt or a crash. Exit codes: 0 =
// calibration valid, 2 = procedure completed but invalid (no lambda_max
// recorded), 1 = execution error or a file that could not be written. The
// reference step's numbers enter no gate (§5); an execution error in it
// exits 1 like any other.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/calibrate"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/serverstats"
)

func main() {
	configPath := flag.String("config", "", "path to the experiment config YAML (required); its target and metrics are overridden by the flags, phases and rate are derived per step")
	target := flag.String("target", "", "the one replica's direct inference URL, no Service in front (required)")
	metrics := flag.String("metrics", "", "the same replica's Prometheus endpoint (required)")
	gauge := flag.String("gauge", "", "waiting-queue gauge name (default: target.queue_gauge from the config)")
	outDir := flag.String("out", "results", "output directory for calibration.json and calibration.txt")
	maxRate := flag.Float64("max-rate", 0, "ceiling on the rate sent; a coarse candidate at or above it runs at the ceiling, and a ramp that reaches the ceiling without a failing step ends invalid (0 = no ceiling)")
	skipReference := flag.Bool("skip-reference", false, "do not run the §5 reference step at 2 lambda_r")
	check := flag.Bool("check", false, "load and validate --config, list the PIN-AT-PHASE1 placeholders it still carries, and exit")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("percentes-calibrate: --config is required")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("percentes-calibrate: %v", err)
	}
	if err := calibrate.CheckConfig(cfg); err != nil {
		log.Fatalf("percentes-calibrate: %v", err)
	}
	placeholders := placeholderLines(cfg.Raw)
	if *check {
		fmt.Printf("%s: loads and validates; %d PIN-AT-PHASE1 placeholder(s) outside comments\n", *configPath, len(placeholders))
		for _, l := range placeholders {
			fmt.Println("  " + l)
		}
		return
	}
	if *target == "" || *metrics == "" {
		log.Fatal("percentes-calibrate: --target and --metrics are required")
	}
	if *gauge == "" {
		*gauge = cfg.Target.QueueGauge
	}
	if *gauge == "" {
		log.Fatal("percentes-calibrate: --gauge or target.queue_gauge must name the waiting-queue gauge (§6)")
	}
	if n := len(placeholders); n > 0 {
		log.Fatalf("percentes-calibrate: the config still carries %d PIN-AT-PHASE1 placeholder(s) outside comments; fill them (--check lists them)", n)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A wrong endpoint or gauge name fails here, in seconds; after a 150 s
	// step it would read as the replica failing at 2 rps.
	if _, err := serverstats.Scrape(ctx, &http.Client{Timeout: 5 * time.Second}, *metrics, *gauge); err != nil {
		log.Fatalf("percentes-calibrate: %v", err)
	}
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err == nil {
		log.Printf("percentes-calibrate: open-file soft limit %d; a failing step holds up to 2 x rate x %d connections", rl.Cur, config.PinnedClientTimeoutS)
		if rl.Cur < 65536 {
			log.Printf("percentes-calibrate: raise it with `ulimit -n 65536` in this shell before a ramp that may pass %d rps", int(rl.Cur)/(2*config.PinnedClientTimeoutS))
		}
	}

	out := &calibrate.Output{
		InstrumentCommit: report.InstrumentCommit(),
		TargetURL:        *target,
		MetricsURL:       *metrics,
		QueueGauge:       *gauge,
		MaxRateRPS:       *maxRate,
		SkipReference:    *skipReference,
		StartedWall:      time.Now(),
		Config:           cfg,
	}
	out.Redact()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("percentes-calibrate: %v", err)
	}
	jsonPath, txtPath := filepath.Join(*outDir, "calibration.json"), filepath.Join(*outDir, "calibration.txt")
	write := func() error {
		var first error
		raw, err := json.MarshalIndent(out, "", "  ")
		if err == nil {
			err = os.WriteFile(jsonPath, raw, 0o644)
		}
		if err != nil {
			first = fmt.Errorf("%s: %w", jsonPath, err)
			log.Printf("percentes-calibrate: %v", first)
		}
		if err := os.WriteFile(txtPath, []byte(calibrate.Human(out)), 0o644); err != nil {
			log.Printf("percentes-calibrate: %s: %v", txtPath, err)
			if first == nil {
				first = err
			}
		}
		return first
	}
	if err := write(); err != nil {
		log.Fatalf("percentes-calibrate: output directory not writable: %v", err)
	}

	runner := &calibrate.LoadRunner{Base: cfg, TargetURL: *target, MetricsURL: *metrics, Gauge: *gauge}
	opts := calibrate.Options{MaxRateRPS: *maxRate, Seed: cfg.Run.Seed, Progress: func(p *calibrate.Result) {
		out.Calibration = p
		write() //nolint:errcheck
	}}
	res, err := calibrate.Calibrate(ctx, runner, opts)
	if err == nil && res.Valid && !*skipReference {
		err = calibrate.Reference(ctx, runner, res, cfg.Run.Seed+int64(len(res.Ramps))*1000)
	}
	out.Calibration, out.FinishedWall = res, time.Now()
	if err != nil {
		out.Error = err.Error()
	}
	werr := write()
	fmt.Print(calibrate.Human(out))
	switch {
	case err != nil:
		log.Printf("percentes-calibrate: %v", err)
		os.Exit(1)
	case werr != nil:
		os.Exit(1)
	case !res.Valid:
		os.Exit(2)
	}
}

// placeholderLines returns the config lines outside comments that still
// carry a PIN-AT-PHASE1 placeholder, numbered.
func placeholderLines(raw []byte) []string {
	var out []string
	for i, l := range strings.Split(string(raw), "\n") {
		t := strings.TrimSpace(l)
		if strings.HasPrefix(t, "#") || !strings.Contains(t, "PIN-AT-PHASE1") {
			continue
		}
		out = append(out, fmt.Sprintf("line %d: %s", i+1, t))
	}
	return out
}
