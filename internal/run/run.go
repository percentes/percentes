// Package run executes one full Percentes run from one config: load
// generation, fault orchestration, collection, recovery detection, and
// (optionally) the §5 decomposition probes. Its Artifacts feed the report
// generator unchanged.
package run

import (
	"context"
	"fmt"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/orchestrator"
)

// Options wires the run to its environment.
type Options struct {
	// Injector, when set, is the fault mechanism the orchestrator
	// pre-arms at T_inject — the Phase 1 clean-delete or node-partition
	// injector, or any test double. When nil, AdminURL selects the mock
	// admin injector (Phase 0). The run stays agnostic beyond timestamps
	// (§2): it never inspects which mechanism this is.
	Injector orchestrator.Injector
	// AdminURL is the victim replica's out-of-band admin endpoint; when
	// set (and Injector is nil), the orchestrator pre-arms InjectMode
	// there at T_inject.
	AdminURL   string
	InjectMode string // config.MockFault* mode for the mock variant
	// InjectDurationS is the armed fault window length.
	InjectDurationS float64
	// VictimReplica is the killed replica's identity (hostname header)
	// for in-flight attribution.
	VictimReplica string
	// ProbeDirectURL/ProbeServiceURL, when set, run the §5 replica-ready
	// and traffic-restored probes from fire time.
	ProbeDirectURL  string
	ProbeServiceURL string
}

// ShareGateResult is the §1 per-replica request-share gate, computed in
// Phase 0 from per-request replica attribution (the server-sent replica
// header recorded on every response); Phase 1 must additionally read the
// server-side Prometheus counters.
type ShareGateResult struct {
	Applicable bool               `json:"applicable"`
	Shares     map[string]float64 `json:"shares,omitempty"`
	// BandEnforced is false when the dataplane routes per connection: the
	// share is then a binomial draw over the connection count, not a
	// property of the target, so it is reported without gating the run.
	BandEnforced        bool    `json:"band_enforced"`
	VictimShareAtInject float64 `json:"victim_share_at_inject,omitempty"`
	Pass                bool    `json:"pass"`
	Note                string  `json:"note,omitempty"`
}

// Artifacts is everything one run produces.
type Artifacts struct {
	Config        *config.Config           `json:"config"`
	Loadgen       *loadgen.Result          `json:"loadgen"`
	Orchestration *orchestrator.Timestamps `json:"orchestration,omitempty"`
	ActualFireNs  int64                    `json:"actual_fire_ns"`
	// VictimReplica records which pod was killed (§1: "Record which pod
	// is killed and its share at T_inject").
	VictimReplica string                     `json:"victim_replica,omitempty"`
	Windows       map[string]*collect.Stats  `json:"windows"`
	InFlight      collect.InFlightAccounting `json:"in_flight_at_fire"`
	Detector      *detect.Result             `json:"detector"`
	Decomposition *detect.Decomposition      `json:"decomposition"`
	ShareGate     ShareGateResult            `json:"share_gate"`
	// ThresholdAnalysis is the §4 modal/baseline-SD statement.
	ThresholdAnalysis collect.ThresholdAnalysis `json:"threshold_analysis"`
	RunValid          bool                      `json:"run_valid"`
	InvalidReasons    []string                  `json:"invalid_reasons,omitempty"`
}

// Execute performs one full run.
func Execute(ctx context.Context, cfg *config.Config, opts Options) (*Artifacts, error) {
	art := &Artifacts{Config: cfg, Windows: map[string]*collect.Stats{}}

	var epoch time.Time
	epochReady := make(chan struct{})
	orchDone := make(chan error, 1)
	var orch *orchestrator.Timestamps
	tInject := time.Duration((cfg.Run.Phases.WarmupS + cfg.Fault.TInjectOffsetS) * float64(time.Second))

	inj := opts.Injector
	if inj == nil && opts.AdminURL != "" {
		inj = orchestrator.NewMockInjector(opts.AdminURL, opts.InjectMode, 0)
	}
	if inj != nil {
		go func() {
			<-epochReady
			ts, err := orchestrator.Execute(ctx, inj, epoch, tInject, opts.InjectDurationS)
			orch = ts
			orchDone <- err
		}()
	} else {
		orchDone <- nil // schedule-driven mock faults, nothing to arm
	}

	// §5 decomposition probes run DURING the fault window: they start at
	// the planned fire time and poll until first success or the window
	// timeout. Launched here so their clocks share the run epoch.
	type probeOutcome struct {
		name string
		at   time.Time
		err  error
	}
	probeCh := make(chan probeOutcome, 2)
	probes := 0
	launchProbe := func(name, url, requireReplica string) {
		if url == "" {
			return
		}
		probes++
		go func() {
			<-epochReady
			fireWall := epoch.Add(tInject)
			if wait := time.Until(fireWall); wait > 0 {
				time.Sleep(wait)
			}
			probeCtx, cancel := context.WithDeadline(ctx, epoch.Add(time.Duration((cfg.Run.Phases.WarmupS+cfg.Run.Phases.BaselineS+cfg.Run.Phases.FaultWindowTimeoutS)*float64(time.Second))))
			defer cancel()
			at, err := detect.ProbeRecovery(probeCtx, url, 500*time.Millisecond, requireReplica)
			probeCh <- probeOutcome{name: name, at: at, err: err}
		}()
	}
	// replica_ready polls the victim directly (every response is the
	// victim's); traffic_restored polls the Service but only counts a
	// success served BY the recovered victim — the surviving replica
	// answering the Service does not restore the victim's traffic (§5's
	// routing-propagation segment would otherwise be meaningless).
	launchProbe("replica_ready", opts.ProbeDirectURL, "")
	launchProbe("traffic_restored", opts.ProbeServiceURL, opts.VictimReplica)

	res, err := loadgen.Run(ctx, cfg, &loadgen.Hooks{OnEpoch: func(e time.Time) { epoch = e; close(epochReady) }})
	if err != nil {
		return nil, fmt.Errorf("run: loadgen: %w", err)
	}
	art.Loadgen = res
	if err := <-orchDone; err != nil {
		return nil, fmt.Errorf("run: orchestrator: %w", err)
	}
	art.Orchestration = orch

	// Actual fire time: injector-observed when orchestrated, planned
	// otherwise.
	art.ActualFireNs = res.TInjectNs
	if orch != nil && orch.ObservedFire != nil {
		art.ActualFireNs = orch.ObservedFire.Sub(res.EpochWall).Nanoseconds()
	}

	// Windows (§3): aligned to phases, never straddling T_inject. The
	// baseline ends one pinned client timeout before the fire anchor and
	// the guard window runs from there to T_inject (§3).
	fireAnchorNs := collect.FireAnchorNs(res.TInjectNs, art.ActualFireNs)
	guardStartNs := collect.GuardStartNs(cfg, fireAnchorNs, res.WarmupEndNs)
	windows := []collect.Window{
		{Name: "baseline", StartNs: res.WarmupEndNs, EndNs: guardStartNs},
		{Name: "guard", StartNs: guardStartNs, EndNs: res.TInjectNs},
		{Name: "fault", StartNs: res.TInjectNs, EndNs: res.FaultEndNs},
	}
	buckets := detect.BuildSeries(cfg, res.Requests, res.WarmupEndNs, res.RunEndNs)
	art.Detector = detect.Run(cfg, buckets, res.WarmupEndNs, fireAnchorNs, res.FaultEndNs)

	// The recovery point splits the fault window into degraded and
	// recovered sub-windows for reporting.
	if at := art.Detector.ToPreFault.RecoveredAtNs; at != nil && *at > res.TInjectNs && *at < res.FaultEndNs {
		windows = append(windows,
			collect.Window{Name: "fault_degraded", StartNs: res.TInjectNs, EndNs: *at},
			collect.Window{Name: "fault_recovered", StartNs: *at, EndNs: res.FaultEndNs})
	}
	for _, w := range windows {
		st, err := collect.Collect(cfg, res.Requests, w)
		if err != nil {
			return nil, fmt.Errorf("run: collect %s: %w", w.Name, err)
		}
		art.Windows[w.Name] = st
	}

	art.VictimReplica = opts.VictimReplica
	art.InFlight = collect.AccountInFlight(res.Requests, art.ActualFireNs, opts.VictimReplica)
	art.ShareGate = shareGate(cfg, res, guardStartNs, opts.VictimReplica)
	art.ThresholdAnalysis = collect.AnalyzeThresholds(cfg, art.Windows["baseline"], art.Windows["fault"])

	// §5 decomposition: the probes launched at fire time report their
	// first-success timestamps (or their failure, leaving the segment
	// N/A); goodput-restored comes from the detector.
	art.Decomposition = detect.NewPhase0Decomposition()
	fireWall := res.EpochWall.Add(time.Duration(art.ActualFireNs))
	for i := 0; i < probes; i++ {
		select {
		case p := <-probeCh:
			if p.err == nil {
				art.Decomposition.SetMeasured(p.name, fireWall, p.at)
			}
		case <-time.After(10 * time.Second):
			// Probe goroutines are deadline-bounded before run end; this
			// is a defensive backstop only.
		}
	}
	if at := art.Detector.ToPreFault.RecoveredAtNs; at != nil {
		art.Decomposition.SetMeasured("goodput_restored", fireWall, res.EpochWall.Add(time.Duration(*at)))
	}
	// Routing propagation = traffic_restored minus replica_ready, its own
	// segment (§5).
	var ready, traffic *detect.Segment
	for i := range art.Decomposition.Segments {
		switch art.Decomposition.Segments[i].Name {
		case "replica_ready":
			ready = &art.Decomposition.Segments[i]
		case "traffic_restored":
			traffic = &art.Decomposition.Segments[i]
		}
	}
	if ready != nil && traffic != nil && ready.Measured && traffic.Measured {
		art.Decomposition.SetMeasured("routing_propagation", *ready.EndAt, *traffic.EndAt)
	}

	art.RunValid, art.InvalidReasons = validity(art)
	return art, nil
}

// shareGate computes per-replica request share over the guard-bounded
// baseline window (§1: 45-55% pre-fault, run-failing for multi-replica
// targets). Membership ends at the guard start: the share gate is a
// baseline-derived quantity, so no guard-window request enters it.
func shareGate(cfg *config.Config, res *loadgen.Result, guardStartNs int64, victim string) ShareGateResult {
	out := ShareGateResult{Shares: map[string]float64{}}
	if cfg.Target.Replicas < 2 {
		out.Note = "single-replica target: §1 share gate applies to multi-replica topologies"
		out.Pass = true
		return out
	}
	out.Applicable = true
	counts := map[string]int{}
	total := 0
	for i := range res.Requests {
		r := &res.Requests[i]
		if r.IntendedNs < res.WarmupEndNs || r.IntendedNs >= guardStartNs || r.Replica == "" {
			continue
		}
		counts[r.Replica]++
		total++
	}
	if total == 0 {
		out.Note = "no replica-attributed requests in the baseline window"
		return out
	}
	out.Pass = true
	out.BandEnforced = config.BalancesPerRequest(cfg.Pins.Kubernetes.DataplaneMode)
	outOfBand := false
	for rep, c := range counts {
		share := float64(c) / float64(total)
		out.Shares[rep] = share
		if share < float64(cfg.ShareGate.MinPct)/100 || share > float64(cfg.ShareGate.MaxPct)/100 {
			outOfBand = true
		}
	}
	if outOfBand && out.BandEnforced {
		out.Pass = false
	}
	if !out.BandEnforced {
		out.Note = fmt.Sprintf("dataplane %q routes per connection: share is descriptive, band %d-%d%% not enforced (§1)",
			cfg.Pins.Kubernetes.DataplaneMode, cfg.ShareGate.MinPct, cfg.ShareGate.MaxPct)
	}
	if len(counts) != cfg.Target.Replicas {
		out.Pass = false
		out.Note = fmt.Sprintf("saw %d distinct replicas, expected %d", len(counts), cfg.Target.Replicas)
	}
	if victim != "" {
		out.VictimShareAtInject = out.Shares[victim]
	}
	return out
}

func validity(art *Artifacts) (bool, []string) {
	var reasons []string
	if !art.Loadgen.Gates.Pass {
		reason := "client-validity gate failed (§2)"
		if !art.Loadgen.Gates.CPUMeasured {
			reason += ": host CPU unmeasured on this platform build"
		}
		reasons = append(reasons, reason)
	}
	if art.ShareGate.Applicable && !art.ShareGate.Pass {
		reasons = append(reasons, "per-replica share gate failed (§1)")
	}
	if art.Orchestration != nil {
		if errMs, err := art.Orchestration.FireErrorMs(); err != nil {
			reasons = append(reasons, "fault never fired")
		} else if errMs > float64(art.Config.Fault.InjectionToleranceMs) || errMs < -float64(art.Config.Fault.InjectionToleranceMs) {
			reasons = append(reasons, fmt.Sprintf("injection timing off by %.0fms (tolerance %dms)", errMs, art.Config.Fault.InjectionToleranceMs))
		}
	}
	return len(reasons) == 0, reasons
}
