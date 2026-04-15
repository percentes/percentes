package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Validate enforces the schema. Pre-registered numbers (SPEC.md §§1-6, 8)
// are enforced as equalities: a config that widens a tolerance or weakens
// a gate is rejected, not warned about.
func (c *Config) Validate() error {
	v := &validator{}

	if c.SchemaVersion != 1 {
		v.errf("schema_version: must be 1, got %d", c.SchemaVersion)
	}
	switch c.Profile {
	case ProfileExperiment, ProfileAC:
	default:
		v.errf("profile: must be %q or %q, got %q", ProfileExperiment, ProfileAC, c.Profile)
	}

	c.validateRun(v)
	c.validateLoad(v)
	c.validateClient(v)
	c.validateSLO(v)
	c.validateHistogram(v)
	c.validateDetector(v)
	c.validateClientValidity(v)
	c.validateShareGate(v)
	c.validateTarget(v)
	c.validateFault(v)
	c.validatePins(v)
	c.validateMock(v)

	return v.result()
}

func (c *Config) validateRun(v *validator) {
	if c.Run.Name == "" {
		v.errf("run.name: required")
	}
	p := c.Run.Phases
	if c.Profile == ProfileExperiment {
		v.pinF("run.phases.warmup_s", p.WarmupS, PinnedWarmupS)
		v.pinF("run.phases.baseline_s", p.BaselineS, PinnedBaselineS)
		v.pinF("run.phases.fault_window_timeout_s", p.FaultWindowTimeoutS, PinnedFaultWindowTimeoutS)
		v.pinF("run.phases.cooldown_s", p.CooldownS, PinnedCooldownS)
		v.pinI("run.repetitions", c.Run.Repetitions, PinnedRepetitions)
	} else {
		if p.WarmupS < 0 || p.BaselineS <= 0 || p.FaultWindowTimeoutS <= 0 || p.CooldownS < 0 {
			v.errf("run.phases: ac profile requires warmup_s >= 0, baseline_s > 0, fault_window_timeout_s > 0, cooldown_s >= 0")
		}
		if c.Run.Repetitions < 1 {
			v.errf("run.repetitions: must be >= 1, got %d", c.Run.Repetitions)
		}
	}
}

func (c *Config) validateLoad(v *validator) {
	if c.Profile == ProfileAC {
		// §8: reference conditions for the AC suite are lambda=20 rps.
		v.pinF("load.rate_rps", c.Load.RateRPS, PinnedAmbientRateRPS)
	} else if c.Load.RateRPS <= 0 {
		v.errf("load.rate_rps: must be > 0, got %g", c.Load.RateRPS)
	}
	switch c.Load.ArrivalProcess {
	case "poisson", "deterministic":
	default:
		v.errf("load.arrival_process: must be \"poisson\" or \"deterministic\", got %q", c.Load.ArrivalProcess)
	}
	if c.Load.InputLengthTokens <= 0 {
		v.errf("load.input_length_tokens: must be > 0 (fixed input length, §1), got %d", c.Load.InputLengthTokens)
	}
	v.pinI("load.max_tokens", c.Load.MaxTokens, PinnedMaxTokens)
	if !c.Load.IgnoreEOS {
		v.errf("load.ignore_eos: pinned true (§6)")
	}
	if !c.Load.UniquePrefixes {
		v.errf("load.unique_prefixes: pinned true (§6: unique per-request prefixes regardless)")
	}
	if minConns := PinnedConnMultiplier * c.Target.Replicas; c.Load.Connections < minConns {
		v.errf("load.connections: must be >= %d (4 x %d replicas, §1), got %d", minConns, c.Target.Replicas, c.Load.Connections)
	}
}

func (c *Config) validateClient(v *validator) {
	v.pinI("client.http_timeout_s", c.Client.HTTPTimeoutS, PinnedClientTimeoutS)
	v.pinI("client.retries", c.Client.Retries, PinnedClientRetries)
	if c.Profile == ProfileExperiment {
		if !c.Client.Placement.DedicatedNode {
			v.errf("client.placement.dedicated_node: pinned true in experiment profile (§6)")
		}
		if c.Client.Placement.NodeClass != NodeClassNonSpot {
			v.errf("client.placement.node_class: pinned \"non-spot\" in experiment profile (§6), got %q", c.Client.Placement.NodeClass)
		}
		if !c.Client.Placement.RecordRTT {
			v.errf("client.placement.record_rtt: pinned true in experiment profile (§6)")
		}
		if c.Client.Placement.Zone == "" || c.Client.Placement.Subnet == "" {
			v.errf("client.placement: zone and subnet required in experiment profile (§6)")
		}
	}
}

func (c *Config) validateSLO(v *validator) {
	v.pinI("slo.ttft_ms", c.SLO.TTFTMs, PinnedSLOTTFTMs)
	v.pinI("slo.e2e_ms", c.SLO.E2EMs, PinnedSLOE2EMs)
	v.pinList("slo.sweep.ttft_ms", c.SLO.Sweep.TTFTMs, PinnedSLOSweepTTFTMs)
	v.pinList("slo.sweep.e2e_ms", c.SLO.Sweep.E2EMs, PinnedSLOSweepE2EMs)
}

func (c *Config) validateHistogram(v *validator) {
	h := c.Histogram
	if h.Unit != PinnedHistogramUnit {
		v.errf("histogram.unit: pinned %q, got %q", PinnedHistogramUnit, h.Unit)
	}
	if h.LowestDiscernibleValue != PinnedHistogramLowest {
		v.errf("histogram.lowest_discernible_value: pinned %d, got %d", PinnedHistogramLowest, h.LowestDiscernibleValue)
	}
	if h.SignificantFigures != PinnedHistogramSigFigs {
		v.errf("histogram.significant_figures: pinned %d, got %d", PinnedHistogramSigFigs, h.SignificantFigures)
	}
	// §3: highestTrackableValue at least the run timeout, one configuration
	// across ALL runs and windows — so the bound is the pinned 600 s
	// experiment timeout regardless of profile (lossless merge requires
	// identical configuration everywhere).
	if h.HighestTrackableValue < PinnedHistogramMinHighest {
		v.errf("histogram.highest_trackable_value: must be >= %d us (run timeout, §3), got %d", PinnedHistogramMinHighest, h.HighestTrackableValue)
	}
}

func (c *Config) validateDetector(v *validator) {
	d := c.RecoveryDetector
	v.pinI("recovery_detector.window_s", d.WindowS, PinnedDetectorWindowS)
	v.pinI("recovery_detector.entry_pct", d.EntryPct, PinnedDetectorEntryPct)
	v.pinI("recovery_detector.exit_pct", d.ExitPct, PinnedDetectorExitPct)
	v.pinI("recovery_detector.hold_s", d.HoldS, PinnedDetectorHoldS)
	v.pinList("recovery_detector.sensitivity.entry_pct", d.Sensitivity.EntryPct, PinnedSensitivityEntryPct)
	v.pinList("recovery_detector.sensitivity.window_s", d.Sensitivity.WindowS, PinnedSensitivityWindowS)
	v.pinList("recovery_detector.sensitivity.hold_s", d.Sensitivity.HoldS, PinnedSensitivityHoldS)
}

func (c *Config) validateClientValidity(v *validator) {
	g := c.ClientValidity
	v.pinI("client_validity.send_skew_p99_ms", g.SendSkewP99Ms, PinnedSendSkewP99Ms)
	v.pinI("client_validity.send_skew_max_ms", g.SendSkewMaxMs, PinnedSendSkewMaxMs)
	v.pinI("client_validity.max_cpu_pct", g.MaxCPUPct, PinnedClientCPUPct)
	v.pinI("client_validity.cpu_window_s", g.CPUWindowS, PinnedCPUWindowS)
	v.pinI("client_validity.go_gc_pause_p99_ms", g.GoGCPauseP99Ms, PinnedGoGCPauseP99Ms)
}

func (c *Config) validateShareGate(v *validator) {
	v.pinI("share_gate.min_pct", c.ShareGate.MinPct, PinnedShareMinPct)
	v.pinI("share_gate.max_pct", c.ShareGate.MaxPct, PinnedShareMaxPct)
}

func (c *Config) validateTarget(v *validator) {
	if c.Target.BaseURL == "" {
		v.errf("target.base_url: required")
	}
	if c.Profile == ProfileExperiment {
		v.pinI("target.replicas", c.Target.Replicas, PinnedExperimentReplicas)
	} else if c.Target.Replicas < 1 {
		v.errf("target.replicas: must be >= 1, got %d", c.Target.Replicas)
	}
}

func (c *Config) validateFault(v *validator) {
	switch c.Fault.Variant {
	case VariantCleanDelete, VariantBlackHole:
		if c.Profile == ProfileAC {
			v.errf("fault.variant: ac profile (Phase 0) is mock-only, got %q", c.Fault.Variant)
		}
		if c.Mock != nil {
			v.errf("mock: must be absent when fault.variant is %q", c.Fault.Variant)
		}
	case VariantMock:
		if c.Mock == nil {
			v.errf("mock: required when fault.variant is \"mock\"")
		}
	default:
		v.errf("fault.variant: must be one of %q, %q, %q; got %q", VariantCleanDelete, VariantBlackHole, VariantMock, c.Fault.Variant)
	}
	if c.Profile == ProfileExperiment {
		// §1 phase sequence has no gap: the fault immediately follows the
		// pinned 300 s baseline.
		v.pinF("fault.t_inject_offset_s", c.Fault.TInjectOffsetS, c.Run.Phases.BaselineS)
	} else if c.Fault.TInjectOffsetS < c.Run.Phases.BaselineS {
		v.errf("fault.t_inject_offset_s: must be >= baseline_s (%g) so windows never straddle T_inject (§3), got %g", c.Run.Phases.BaselineS, c.Fault.TInjectOffsetS)
	}
	v.pinI("fault.injection_tolerance_ms", c.Fault.InjectionToleranceMs, PinnedInjectionToleranceMs)
	v.pinI("fault.endpoint_staleness_min_s", c.Fault.EndpointStalenessMinS, PinnedEndpointStalenessMinS)
}

func (c *Config) validatePins(v *validator) {
	p := c.Pins
	req := func(field, val string) {
		if strings.TrimSpace(val) == "" {
			v.errf("pins.%s: required (§6 pin list; Phase 0 mock configs record an explicit placeholder such as \"n/a-phase0-mock\")", field)
		}
	}
	req("vllm.version", p.VLLM.Version)
	req("vllm.image_digest", p.VLLM.ImageDigest)
	req("model.name", p.Model.Name)
	req("model.revision", p.Model.Revision)
	req("model.quantization", p.Model.Quantization)
	// The mock has no KV cache or scheduler: numeric engine pins take real
	// values in the experiment profile and explicit zeros (recorded n/a)
	// in the Phase 0 ac profile. The fields themselves are always carried.
	if c.Profile == ProfileExperiment {
		if p.Engine.KVCacheGB <= 0 {
			v.errf("pins.engine.kv_cache_gb: must be > 0 (absolute gigabytes, §6), got %g", p.Engine.KVCacheGB)
		}
		if p.Engine.MaxNumSeqs <= 0 {
			v.errf("pins.engine.max_num_seqs: must be > 0, got %d", p.Engine.MaxNumSeqs)
		}
	} else {
		if p.Engine.KVCacheGB < 0 {
			v.errf("pins.engine.kv_cache_gb: must be >= 0, got %g", p.Engine.KVCacheGB)
		}
		if p.Engine.MaxNumSeqs < 0 {
			v.errf("pins.engine.max_num_seqs: must be >= 0, got %d", p.Engine.MaxNumSeqs)
		}
	}
	req("engine.scheduler_settings", p.Engine.SchedulerSettings)
	if p.Engine.ChunkedPrefill != OnOffOn && p.Engine.ChunkedPrefill != OnOffOff {
		v.errf("pins.engine.chunked_prefill: must be \"on\" or \"off\", got %q", p.Engine.ChunkedPrefill)
	}
	if p.Engine.CUDAGraphs != OnOffOn && p.Engine.CUDAGraphs != OnOffOff {
		v.errf("pins.engine.cuda_graphs: must be \"on\" or \"off\", got %q", p.Engine.CUDAGraphs)
	}
	if p.Engine.PrefixCaching != OnOffOff {
		v.errf("pins.engine.prefix_caching: pinned \"off\" (§6), got %q", p.Engine.PrefixCaching)
	}
	if p.Engine.ContinuousBatching != OnOffOn {
		v.errf("pins.engine.continuous_batching: pinned \"on\" (§6), got %q", p.Engine.ContinuousBatching)
	}
	req("gpu.sku", p.GPU.SKU)
	req("gpu.driver", p.GPU.Driver)
	req("gpu.cuda", p.GPU.CUDA)
	req("gpu.cudnn", p.GPU.CUDNN)
	req("gpu.nccl", p.GPU.NCCL)
	req("gpu.clock_power_policy", p.GPU.ClockPowerPolicy)
	req("kubernetes.version", p.Kubernetes.Version)
	req("kubernetes.cni", p.Kubernetes.CNI)
	req("kubernetes.dataplane_mode", p.Kubernetes.DataplaneMode)
	req("kubernetes.kube_proxy_mode", p.Kubernetes.KubeProxyMode)
	if p.Kubernetes.NodeMonitorGracePeriodS <= 0 {
		v.errf("pins.kubernetes.node_monitor_grace_period_s: must be > 0 (record the cluster's actual value, §1), got %d", p.Kubernetes.NodeMonitorGracePeriodS)
	}
	req("readiness_probe.path", p.Readiness.Path)
	if p.Readiness.PeriodS <= 0 || p.Readiness.TimeoutS <= 0 || p.Readiness.FailureThreshold <= 0 {
		v.errf("pins.readiness_probe: period_s, timeout_s, failure_threshold must all be > 0")
	}
	req("storage.weights_medium", p.Storage.WeightsMedium)
}

func (c *Config) validateMock(v *validator) {
	m := c.Mock
	if m == nil {
		return
	}
	if m.ListenAddr == "" {
		v.errf("mock.listen_addr: required")
	}
	validateDist(v, "mock.ttft", m.TTFT)
	validateDist(v, "mock.itl", m.ITL)
	if m.SlowReload.Enabled && m.SlowReload.DurationS <= 0 {
		v.errf("mock.slow_reload.duration_s: must be > 0 when enabled, got %g", m.SlowReload.DurationS)
	}

	type window struct{ start, end float64 }
	windows := make([]window, 0, len(m.FaultSchedule))
	for i, f := range m.FaultSchedule {
		field := fmt.Sprintf("mock.fault_schedule[%d]", i)
		switch f.Mode {
		case MockFaultStall, MockFaultError, MockFaultStreamAbort, MockFaultSilentHang:
		default:
			v.errf("%s.mode: must be one of %q, %q, %q, %q; got %q", field,
				MockFaultStall, MockFaultError, MockFaultStreamAbort, MockFaultSilentHang, f.Mode)
			continue
		}
		if f.StartOffsetS < 0 {
			v.errf("%s.start_offset_s: must be >= 0, got %g", field, f.StartOffsetS)
		}
		if f.DurationS <= 0 {
			v.errf("%s.duration_s: must be > 0, got %g", field, f.DurationS)
		}
		if f.AbortAfterTokens != 0 && f.Mode != MockFaultStreamAbort {
			v.errf("%s.abort_after_tokens: only valid for mode %q", field, MockFaultStreamAbort)
		}
		if f.AbortAfterTokens < 0 {
			v.errf("%s.abort_after_tokens: must be >= 0, got %d", field, f.AbortAfterTokens)
		}
		windows = append(windows, window{f.StartOffsetS, f.StartOffsetS + f.DurationS})
	}
	sort.Slice(windows, func(a, b int) bool { return windows[a].start < windows[b].start })
	for i := 1; i < len(windows); i++ {
		if windows[i].start < windows[i-1].end {
			v.errf("mock.fault_schedule: windows overlap ([%g,%g) and [%g,%g)); one fault at a time",
				windows[i-1].start, windows[i-1].end, windows[i].start, windows[i].end)
		}
	}
}

func validateDist(v *validator, field string, d LatencyDist) {
	switch d.Distribution {
	case DistributionFixed:
		if d.FixedMs <= 0 {
			v.errf("%s.fixed_ms: must be > 0 for fixed distribution, got %g", field, d.FixedMs)
		}
		if d.MinMs != 0 || d.MaxMs != 0 {
			v.errf("%s: min_ms/max_ms not valid for fixed distribution", field)
		}
	case DistributionUniform:
		if d.MinMs <= 0 || d.MaxMs < d.MinMs {
			v.errf("%s: uniform distribution requires 0 < min_ms <= max_ms, got [%g, %g]", field, d.MinMs, d.MaxMs)
		}
		if d.FixedMs != 0 {
			v.errf("%s: fixed_ms not valid for uniform distribution", field)
		}
	default:
		v.errf("%s.distribution: must be \"fixed\" or \"uniform\", got %q", field, d.Distribution)
	}
}

// validator accumulates field-scoped errors.
type validator struct{ errs []string }

func (v *validator) errf(format string, args ...any) {
	v.errs = append(v.errs, fmt.Sprintf(format, args...))
}

// pinI enforces a pre-registered integer equality.
func (v *validator) pinI(field string, got, want int) {
	if got != want {
		v.errf("%s: pre-registered value is %d (SPEC.md), got %d", field, want, got)
	}
}

// pinF enforces a pre-registered float equality.
func (v *validator) pinF(field string, got, want float64) {
	if got != want {
		v.errf("%s: pre-registered value is %g (SPEC.md), got %g", field, want, got)
	}
}

// pinList enforces a pre-registered list, order included.
func (v *validator) pinList(field string, got, want []int) {
	if len(got) != len(want) {
		v.errf("%s: pre-registered grid is %v (SPEC.md), got %v", field, want, got)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			v.errf("%s: pre-registered grid is %v (SPEC.md), got %v", field, want, got)
			return
		}
	}
}

func (v *validator) result() error {
	if len(v.errs) == 0 {
		return nil
	}
	return errors.New("invalid config:\n  - " + strings.Join(v.errs, "\n  - "))
}
