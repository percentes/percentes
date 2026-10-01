// percentes-campaign runs a campaign of N runs of one (variant, config),
// the SPEC.md §5 repetition unit, where the repetition count N is pinned
// to 5 by the experiment profile, and writes the campaign report pair
// (§5/§7 statistics + §10 validity gates). Phase 0 drives it against the
// mock; Phase 1 swaps in the clean-delete, node-partition or process-kill
// injector and supplies the graphics processing unit (GPU) cluster
// observations. Each run's report pair is written as the run ends.
// Exit codes: 0 = every run valid, 2 = at least one run failed a
// run-validity gate or the campaign halted after one, 1 = error. On an
// error or a halt the campaign report pair still holds the runs so far.
package main

import (
	"context"
	"crypto/sha256"
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

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/redact"
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
	victim := flag.String("victim", "", "victim replica identity (mock: hostname; clean_delete: a pod name fixed for every run, so only a pod recreated under the same name, such as a StatefulSet pod; a Deployment needs --victim-selector)")
	victimSelector := flag.String("victim-selector", "", "label selector naming the clean_delete victim afresh on every run, its first Ready pod by name once every replica is Ready; not combined with --victim")
	readyTimeout := flag.Duration("ready-timeout", 10*time.Minute, "how long a clean_delete run waits for its victim to be Ready before failing")
	namespace := flag.String("namespace", "percentes", "victim pod namespace (clean_delete variant)")
	kubeContext := flag.String("kube-context", "", "kubeconfig context for the clean_delete injector (required for that variant; kubectl's current context is not used)")
	victimNode := flag.String("victim-node", "", "victim node name (black_hole variant)")
	sshTarget := flag.String("ssh-target", "", "user@host of the container host (process_kill); empty runs the scripts locally")
	sshIdentity := flag.String("ssh-identity", "", "private key for --ssh-target")
	sshControlPath := flag.String("ssh-control-path", "", "ssh ControlMaster socket; empty means /tmp/percentes-cm-%C")
	container := flag.String("container", "", "container name (required for process_kill)")
	killVia := flag.String("kill-via", "sudo", "how the kill runs without --ssh-target: sudo, or docker-helper (macOS Docker in a VM)")
	probeDirect := flag.String("probe-direct", "", "the replica's base URL for the replica_ready probe (required for process_kill)")
	target := flag.String("target", "", "replaces target.base_url; recorded in campaign.json")
	metrics := flag.String("metrics", "", "replaces target.metrics_urls with this one URL; recorded in campaign.json")
	dryKillOnly := flag.Bool("dry-kill", false, "process_kill: one kill with no load; writes dry-kill.json and dry-kill-server.log under --out and exits")
	haltAfterInvalid := flag.Bool("halt-after-invalid-run", false, "end the campaign after the first invalid run (default on for process_kill)")
	flag.Parse()
	haltSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "halt-after-invalid-run" {
			haltSet = true
		}
	})

	if *configPath == "" {
		log.Fatal("percentes-campaign: --config is required")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}
	overrides, err := applyOverrides(cfg, *target, *metrics)
	if err != nil {
		log.Fatalf("percentes-campaign: %v", err)
	}
	if *probeDirect != "" {
		if err := config.CheckBaseURL(*probeDirect); err != nil {
			log.Fatalf("percentes-campaign: --probe-direct %s: %v", redact.URL(*probeDirect), err)
		}
	}
	if *dryKillOnly && cfg.Fault.Variant != config.VariantProcessKill {
		log.Fatal("percentes-campaign: --dry-kill applies to process_kill only")
	}

	opts := run.Options{
		AdminURL:        *adminURL,
		InjectMode:      *injectMode,
		InjectDurationS: *injectDuration,
		VictimReplica:   *victim,
		ProbeDirectURL:  *probeDirect,
	}
	// §1: the black-hole partition expires at the pinned configuration
	// duration; --inject-duration-s serves the mock injector.
	if cfg.Fault.Variant == config.VariantBlackHole {
		opts.InjectDurationS = float64(cfg.Fault.PartitionDurationS)
	}

	// Route fault.variant to an injector: mock uses the admin injector at
	// AdminURL (default); clean_delete does a grace=0 pod delete via
	// kubectl, one injector per run; black_hole needs a real NodeOps and a
	// multi-node cluster and is refused here (SPEC.md §10); process_kill
	// SIGKILLs the container's init process from the host, one injector per
	// run; none arms nothing (§6). The run engine stays agnostic beyond
	// timestamps (§2).
	var cleanDelete orchestrator.KubectlPodOps
	var plan victimPlan
	var containerOps orchestrator.SSHContainerOps
	var pkPlan processKillPlan
	switch cfg.Fault.Variant {
	case config.VariantNone:
		opts.AdminURL = ""
	case config.VariantCleanDelete:
		if *victim == "" && *victimSelector == "" {
			log.Fatal("percentes-campaign: clean_delete requires --victim (pod name) or --victim-selector")
		}
		if *victim != "" && *victimSelector != "" {
			log.Fatal("percentes-campaign: clean_delete takes exactly one of --victim and --victim-selector")
		}
		if *kubeContext == "" {
			log.Fatal("percentes-campaign: clean_delete requires --kube-context")
		}
		if *victim != "" && cfg.Run.Repetitions > 1 {
			log.Printf("percentes-campaign: --victim %s is fixed for %d runs; a Deployment replaces it under a new name after run 1 (use --victim-selector)", *victim, cfg.Run.Repetitions)
		}
		cleanDelete = orchestrator.KubectlPodOps{Context: *kubeContext}
		plan = victimPlan{namespace: *namespace, victim: *victim, selector: *victimSelector, replicas: cfg.Target.Replicas, readyTimeout: *readyTimeout, poll: 2 * time.Second}
		checkCtx, cancel := context.WithTimeout(context.Background(), *readyTimeout)
		if _, err := resolveVictim(checkCtx, cleanDelete, plan); err != nil {
			cancel()
			log.Fatalf("percentes-campaign: victim not ready in context %s: %v", *kubeContext, err)
		}
		cancel()
	case config.VariantBlackHole:
		if *victimNode == "" {
			log.Fatal("percentes-campaign: black_hole requires --victim-node")
		}
		log.Fatal("percentes-campaign: black_hole requires a multi-node GPU cluster and a real NodeOps; see SPEC.md §10")
	case config.VariantProcessKill:
		if *container == "" {
			log.Fatal("percentes-campaign: process_kill requires --container")
		}
		if *probeDirect == "" {
			log.Fatal("percentes-campaign: process_kill requires --probe-direct")
		}
		if *killVia != "sudo" && *killVia != "docker-helper" {
			log.Fatalf("percentes-campaign: --kill-via %q: want sudo or docker-helper", *killVia)
		}
		if *sshTarget != "" && *killVia != "sudo" {
			log.Fatal("percentes-campaign: --kill-via docker-helper applies only without --ssh-target")
		}
		if *sshTarget == "" && (*sshIdentity != "" || *sshControlPath != "") {
			log.Fatal("percentes-campaign: --ssh-identity and --ssh-control-path need --ssh-target")
		}
		if pins := cfg.Pins.Container; cfg.Profile == config.ProfileExperiment && pins != nil && *container != pins.Name {
			log.Fatalf("percentes-campaign: --container %s differs from the pinned container name %s (§6)", *container, pins.Name)
		}
		opts.AdminURL = ""
		// Execute waits out the kill call's own timeout.
		opts.InjectDurationS = orchestrator.ContainerOpTimeout.Seconds()
		containerOps = orchestrator.SSHContainerOps{Target: *sshTarget, Identity: *sshIdentity, ControlPath: *sshControlPath, KillVia: *killVia}
		pkPlan = processKillPlan{container: *container, probeURL: *probeDirect, outDir: *outDir, profile: cfg.Profile, readyTimeout: *readyTimeout, poll: 2 * time.Second}
	}

	// Evaluate the §10 validity gates per run and fold the verdict into the
	// run's validity: campaign counts and endpoint summaries key off it.
	// G5 with no fingerprint collector reports not applicable; a failed G4
	// strips the node-loss-representative label without invalidating (§10).
	// Each run's report pair is written and its gate table logged as it ends.
	//
	// gates[i] pairs with run i+1; campaign.Run calls the runner
	// sequentially. Parallelizing it would race this append.
	var gates []validity.Report
	execute := campaign.Runner(run.Execute)
	if cfg.Fault.Variant == config.VariantProcessKill {
		execute = processKillRunner(containerOps, pkPlan, run.Execute)
	}
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
		art, err := execute(ctx, c, o)
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
		if err := writeRun(*outDir, len(gates), art, &rep); err != nil {
			return nil, fmt.Errorf("writing run %d: %w", len(gates), err)
		}
		log.Printf("percentes-campaign: run %d valid=%v\n%s", len(gates), art.RunValid, gateTable(rep, art.InvalidReasons))
		return art, nil
	}

	// Interrupt-safe teardown: a SIGINT, SIGTERM or SIGHUP cancels the campaign context
	// so an in-flight run.Execute unwinds cleanly rather than being killed
	// mid-fault. A cancelled run surfaces as an error and exits 1, preserving
	// the 0/1/2 contract (2 is reserved for a completed campaign with a
	// failed run-validity gate or a halt).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	if *dryKillOnly {
		path, err := dryKill(ctx, containerOps, pkPlan, cfg)
		if path != "" {
			fmt.Printf("dry kill record: %s\n", path)
		}
		if err != nil {
			log.Fatalf("percentes-campaign: %v", err)
		}
		return
	}

	// A wrong endpoint or metric name fails here, before the first run.
	for _, u := range cfg.Target.MetricsURLs {
		if err := serverstats.Preflight(ctx, &http.Client{Timeout: 5 * time.Second}, u, cfg.Target.QueueGauge, cfg.Target.MetricsFamilies, cfg.Target.TTFTHistogram); err != nil {
			log.Fatalf("percentes-campaign: %v", err)
		}
	}

	if cfg.Fault.Variant == config.VariantCleanDelete {
		runner = cleanDeleteRunner(cleanDelete, plan, runner)
	}
	pol := campaign.Policy{HaltAfterInvalidRun: haltPolicy(haltSet, *haltAfterInvalid, cfg.Fault.Variant)}
	rep, runErr := campaign.RunWith(ctx, cfg, opts, cfg.Fault.Variant, runner, pol)
	if rep == nil {
		log.Fatalf("percentes-campaign: %v", runErr)
	}

	meta := report.CampaignMeta{InstrumentCommit: report.InstrumentCommit(), ConfigSHA256: fmt.Sprintf("%x", sha256.Sum256(cfg.Raw)), Profile: cfg.Profile, Overrides: overrides}
	rawJSON, humanText, err := report.GenerateCampaignWith(rep, gates, meta)
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

	if runErr != nil {
		log.Printf("percentes-campaign: run %d failed; the reports hold the %d runs before it", rep.FailedRun, len(rep.PerRun))
		log.Fatalf("percentes-campaign: %v", runErr)
	}
	if rep.Halted {
		fmt.Printf("CAMPAIGN HALTED: run %d was invalid and --halt-after-invalid-run ended the campaign.\n", rep.HaltedAfterRun)
		os.Exit(2)
	}

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

// applyOverrides replaces the base and metrics URLs named on the command
// line, revalidates the config and returns the replacements as recorded.
func applyOverrides(cfg *config.Config, target, metrics string) ([]string, error) {
	var out []string
	if target != "" {
		if err := config.CheckBaseURL(target); err != nil {
			return nil, fmt.Errorf("--target %s: %w", redact.URL(target), err)
		}
		cfg.Target.BaseURL = target
		out = append(out, "target.base_url="+redact.URL(target))
	}
	if metrics != "" {
		cfg.Target.MetricsURLs = []string{metrics}
		out = append(out, "target.metrics_urls="+redact.URL(metrics))
	}
	if len(out) > 0 {
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("after %s: %w", strings.Join(out, ", "), err)
		}
	}
	return out, nil
}

// haltPolicy is --halt-after-invalid-run as given, or on for process_kill
// when the flag is absent.
func haltPolicy(set, value bool, variant string) bool {
	if set {
		return value
	}
	return variant == config.VariantProcessKill
}

// gateTable renders one run's §10 gates and invalid reasons for the log.
func gateTable(rep validity.Report, reasons []string) string {
	var b strings.Builder
	for _, g := range rep.Gates {
		status := "n/a"
		if g.Applicable {
			switch {
			case !g.Observed:
				status = "UNOBSERVED->FAIL"
			case g.Pass:
				status = "pass"
			default:
				status = "FAIL"
			}
		}
		fmt.Fprintf(&b, "  %s %-16s %s\n", g.ID, status, g.Detail)
	}
	for _, r := range reasons {
		fmt.Fprintf(&b, "  invalid: %s\n", r)
	}
	return strings.TrimRight(b.String(), "\n")
}
