// Package config defines the single ChaosServe run-configuration schema
// (SPEC.md is authoritative). One config file drives a run end to end:
// load generator, chaos orchestrator, mock server, metrics collector,
// recovery detector, and report generator all read sections of this file.
//
// The schema carries the full SPEC.md §6 pin list. Every gate, tolerance,
// and detector parameter that SPEC.md pre-registers is enforced by
// Validate(); a config that weakens a pinned number does not load.
package config

// Profile selects which validation regime applies.
//
//   - "experiment": the Phase 1 real-run profile. Phase durations (§1),
//     repetitions (§5), and topology (§1) are pinned exactly.
//   - "ac": the Phase 0 acceptance-criteria profile against the mock
//     (§8 reference conditions: lambda=20 rps). Phase durations may be
//     scenario-scale, but every normative pin (client timeout, retries,
//     max_tokens, SLO, detector numbers, validity gates) is still enforced.
type Profile string

const (
	ProfileExperiment Profile = "experiment"
	ProfileAC         Profile = "ac"
)

// Fault variants (§1). VariantMock is the Phase 0 injector family; the
// mock's fault modes are configured in Mock.FaultSchedule.
const (
	VariantCleanDelete = "clean_delete"
	VariantBlackHole   = "black_hole"
	VariantMock        = "mock"
)

// Pre-registered constants. These are the SPEC.md numbers; Validate()
// rejects configs that deviate. They are exported so other modules assert
// against the same single source.
const (
	// §6 / §2: client HTTP timeout is pinned at 30 s, retries at zero.
	PinnedClientTimeoutS = 30
	PinnedClientRetries  = 0

	// §6: output budget.
	PinnedMaxTokens = 256

	// §4: SLO thresholds.
	PinnedSLOTTFTMs = 1000
	PinnedSLOE2EMs  = 14000

	// §2: client-validity gate.
	PinnedSendSkewP99Ms  = 5
	PinnedSendSkewMaxMs  = 50
	PinnedClientCPUPct   = 70
	PinnedCPUWindowS     = 5
	PinnedGoGCPauseP99Ms = 1

	// §1: per-replica share gate.
	PinnedShareMinPct = 45
	PinnedShareMaxPct = 55

	// §5: recovery detector.
	PinnedDetectorWindowS  = 10
	PinnedDetectorEntryPct = 90
	PinnedDetectorExitPct  = 85
	PinnedDetectorHoldS    = 30

	// §1: experiment-profile phase durations (seconds).
	PinnedWarmupS             = 60
	PinnedBaselineS           = 300
	PinnedFaultWindowTimeoutS = 600
	PinnedCooldownS           = 60

	// §5: repetitions per (variant, config).
	PinnedRepetitions = 5

	// §1: topology, and connection provisioning multiplier.
	PinnedExperimentReplicas   = 2
	PinnedConnMultiplier       = 4
	PinnedAmbientRateRPS       = 20  // §8 AC-suite reference lambda
	PinnedInjectionToleranceMs = 500 // §8 AC3

	// §1 black-hole runtime assertion (ii) / §10 G4: the dead pod must
	// remain in ready EndpointSlices for at least this long or the run is
	// downgraded to clean-variant-equivalent.
	PinnedEndpointStalenessMinS = 20

	// §3: one pinned HdrHistogram configuration across all runs and windows.
	PinnedHistogramUnit       = "us"
	PinnedHistogramLowest     = 1
	PinnedHistogramSigFigs    = 3
	PinnedHistogramMinHighest = int64(PinnedFaultWindowTimeoutS) * 1_000_000 // ≥ run timeout
)

// Pre-registered sweep grids (§4 SLO sweep, §5 detector sensitivity).
var (
	PinnedSLOSweepTTFTMs = []int{800, 1000, 1500}
	PinnedSLOSweepE2EMs  = []int{12000, 14000, 18000}

	PinnedSensitivityEntryPct = []int{85, 90, 95}
	PinnedSensitivityWindowS  = []int{5, 10, 20}
	PinnedSensitivityHoldS    = []int{15, 30, 60}
)

// Config is the root of the run configuration.
type Config struct {
	SchemaVersion int     `yaml:"schema_version" json:"schema_version"`
	Profile       Profile `yaml:"profile" json:"profile"`

	Run              Run              `yaml:"run" json:"run"`
	Load             Load             `yaml:"load" json:"load"`
	Client           Client           `yaml:"client" json:"client"`
	SLO              SLO              `yaml:"slo" json:"slo"`
	Histogram        Histogram        `yaml:"histogram" json:"histogram"`
	RecoveryDetector RecoveryDetector `yaml:"recovery_detector" json:"recovery_detector"`
	ClientValidity   ClientValidity   `yaml:"client_validity" json:"client_validity"`
	ShareGate        ShareGate        `yaml:"share_gate" json:"share_gate"`
	Target           Target           `yaml:"target" json:"target"`
	Fault            Fault            `yaml:"fault" json:"fault"`
	Pins             Pins             `yaml:"pins" json:"pins"`

	// Mock configures the Phase 0 mock inference server. Required when
	// fault.variant is "mock"; must be absent otherwise.
	Mock *Mock `yaml:"mock,omitempty" json:"mock,omitempty"`
}

// Run identifies the run and fixes its phase structure (§1).
type Run struct {
	Name        string `yaml:"name" json:"name"`
	Seed        int64  `yaml:"seed" json:"seed"`
	Repetitions int    `yaml:"repetitions" json:"repetitions"`
	Phases      Phases `yaml:"phases" json:"phases"`
}

// Phases are the §1 load-profile phases, in seconds. Warm-up is discarded.
type Phases struct {
	WarmupS             float64 `yaml:"warmup_s" json:"warmup_s"`
	BaselineS           float64 `yaml:"baseline_s" json:"baseline_s"`
	FaultWindowTimeoutS float64 `yaml:"fault_window_timeout_s" json:"fault_window_timeout_s"`
	CooldownS           float64 `yaml:"cooldown_s" json:"cooldown_s"`
}

// Load is the open-loop load profile (§1).
type Load struct {
	RateRPS           float64 `yaml:"rate_rps" json:"rate_rps"`
	ArrivalProcess    string  `yaml:"arrival_process" json:"arrival_process"` // "poisson" | "deterministic"
	InputLengthTokens int     `yaml:"input_length_tokens" json:"input_length_tokens"`
	MaxTokens         int     `yaml:"max_tokens" json:"max_tokens"`           // pinned 256 (§6)
	IgnoreEOS         bool    `yaml:"ignore_eos" json:"ignore_eos"`           // pinned true (§6)
	UniquePrefixes    bool    `yaml:"unique_prefixes" json:"unique_prefixes"` // pinned true (§6)
	// Connections is the number of independent HTTP/1.1 connections the
	// client maintains; must be ≥ 4 × replica count (§1).
	Connections int `yaml:"connections" json:"connections"`
}

// Client pins client behavior and placement (§2, §6).
type Client struct {
	HTTPTimeoutS int       `yaml:"http_timeout_s" json:"http_timeout_s"` // pinned 30
	Retries      int       `yaml:"retries" json:"retries"`               // pinned 0
	Placement    Placement `yaml:"placement" json:"placement"`
}

// NodeClass is the load-generator node's provisioning class (§6). The
// experiment profile pins it to NodeClassNonSpot so preemption and
// scheduling jitter on the client host cannot perturb the send timeline.
type NodeClass string

const (
	NodeClassSpot    NodeClass = "spot"
	NodeClassNonSpot NodeClass = "non-spot"
)

// Placement records where the load generator runs (§6: dedicated non-spot
// node, same zone and subnet, RTT recorded in the environment table).
type Placement struct {
	DedicatedNode bool      `yaml:"dedicated_node" json:"dedicated_node"`
	NodeClass     NodeClass `yaml:"node_class" json:"node_class"` // "non-spot" in experiment profile
	Zone          string    `yaml:"zone" json:"zone"`
	Subnet        string    `yaml:"subnet" json:"subnet"`
	RecordRTT     bool      `yaml:"record_rtt" json:"record_rtt"`
}

// SLO is the pre-registered SLO (§4) plus the goodput-versus-threshold sweep.
type SLO struct {
	TTFTMs int      `yaml:"ttft_ms" json:"ttft_ms"` // pinned 1000
	E2EMs  int      `yaml:"e2e_ms" json:"e2e_ms"`   // pinned 14000
	Sweep  SLOSweep `yaml:"sweep" json:"sweep"`
}

type SLOSweep struct {
	TTFTMs []int `yaml:"ttft_ms" json:"ttft_ms"` // pinned {800,1000,1500}
	E2EMs  []int `yaml:"e2e_ms" json:"e2e_ms"`   // pinned {12000,14000,18000}
}

// Histogram is the one pinned HdrHistogram configuration used for every
// window of every run (§3), enabling lossless merge. Values are recorded
// via recordValue() only; coordinated-omission correction APIs forbidden.
type Histogram struct {
	Unit                   string `yaml:"unit" json:"unit"` // pinned "us"
	LowestDiscernibleValue int64  `yaml:"lowest_discernible_value" json:"lowest_discernible_value"`
	HighestTrackableValue  int64  `yaml:"highest_trackable_value" json:"highest_trackable_value"` // ≥ run timeout
	SignificantFigures     int    `yaml:"significant_figures" json:"significant_figures"`
}

// RecoveryDetector carries the §5 pre-registered detector parameters and
// the sensitivity sweep grid.
type RecoveryDetector struct {
	WindowS     int              `yaml:"window_s" json:"window_s"`   // R, pinned 10
	EntryPct    int              `yaml:"entry_pct" json:"entry_pct"` // X, pinned 90
	ExitPct     int              `yaml:"exit_pct" json:"exit_pct"`   // pinned 85
	HoldS       int              `yaml:"hold_s" json:"hold_s"`       // H, pinned 30
	Sensitivity SensitivitySweep `yaml:"sensitivity" json:"sensitivity"`
}

type SensitivitySweep struct {
	EntryPct []int `yaml:"entry_pct" json:"entry_pct"` // pinned {85,90,95}
	WindowS  []int `yaml:"window_s" json:"window_s"`   // pinned {5,10,20}
	HoldS    []int `yaml:"hold_s" json:"hold_s"`       // pinned {15,30,60}
}

// ClientValidity is the §2 run-failing client-validity gate.
type ClientValidity struct {
	SendSkewP99Ms  int `yaml:"send_skew_p99_ms" json:"send_skew_p99_ms"`     // pinned 5
	SendSkewMaxMs  int `yaml:"send_skew_max_ms" json:"send_skew_max_ms"`     // pinned 50
	MaxCPUPct      int `yaml:"max_cpu_pct" json:"max_cpu_pct"`               // pinned 70
	CPUWindowS     int `yaml:"cpu_window_s" json:"cpu_window_s"`             // pinned 5
	GoGCPauseP99Ms int `yaml:"go_gc_pause_p99_ms" json:"go_gc_pause_p99_ms"` // pinned 1
}

// ShareGate is the §1 per-replica request-share validity gate.
type ShareGate struct {
	MinPct int `yaml:"min_pct" json:"min_pct"` // pinned 45
	MaxPct int `yaml:"max_pct" json:"max_pct"` // pinned 55
}

// Target describes the system under test.
type Target struct {
	BaseURL  string `yaml:"base_url" json:"base_url"`
	Replicas int    `yaml:"replicas" json:"replicas"`
}

// Fault is the orchestrator plan (§1, §2). Timestamps for armed/fire/expiry
// are recorded by the orchestrator at run time, not configured.
type Fault struct {
	Variant string `yaml:"variant" json:"variant"`
	// TInjectOffsetS is T_inject as an offset in seconds from the start of
	// the measured run (end of warm-up). §1 pins the phase sequence with
	// the fault immediately after the baseline: in the experiment profile
	// this must equal baseline_s exactly; in the ac profile it must be
	// ≥ baseline_s so windows never straddle T_inject (§3).
	TInjectOffsetS float64 `yaml:"t_inject_offset_s" json:"t_inject_offset_s"`
	// InjectionToleranceMs is the AC3 assertion bound; pinned 500.
	InjectionToleranceMs int `yaml:"injection_tolerance_ms" json:"injection_tolerance_ms"`
	// EndpointStalenessMinS is the §1(ii)/§10 G4 black-hole gate: minimum
	// observed ready-EndpointSlice staleness window; pinned 20.
	EndpointStalenessMinS int `yaml:"endpoint_staleness_min_s" json:"endpoint_staleness_min_s"`
}

// Pins is the full §6 configuration-control pin list. Every field is
// required. Phase 0 (mock) configs record explicit "n/a-phase0-mock"
// values rather than omitting fields: the schema carries the list in full
// from day one, and Phase 1 replaces the placeholders with real pins.
type Pins struct {
	VLLM       VLLMPins       `yaml:"vllm" json:"vllm"`
	Model      ModelPins      `yaml:"model" json:"model"`
	Engine     EnginePins     `yaml:"engine" json:"engine"`
	GPU        GPUPins        `yaml:"gpu" json:"gpu"`
	Kubernetes KubernetesPins `yaml:"kubernetes" json:"kubernetes"`
	Readiness  ReadinessProbe `yaml:"readiness_probe" json:"readiness_probe"`
	Storage    StoragePins    `yaml:"storage" json:"storage"`
}

type VLLMPins struct {
	Version     string `yaml:"version" json:"version"`
	ImageDigest string `yaml:"image_digest" json:"image_digest"`
}

type ModelPins struct {
	Name         string `yaml:"name" json:"name"`
	Revision     string `yaml:"revision" json:"revision"`
	Quantization string `yaml:"quantization" json:"quantization"`
}

// OnOff is an engine feature toggle carried in the §6 pin list. The recorded
// value is verified against server metrics at run time, not merely trusted
// from configuration, because vLLM defaults are version-dependent.
type OnOff string

const (
	OnOffOn  OnOff = "on"
	OnOffOff OnOff = "off"
)

type EnginePins struct {
	// KVCacheGB is the KV-cache budget in absolute gigabytes (§6).
	KVCacheGB         float64 `yaml:"kv_cache_gb" json:"kv_cache_gb"`
	MaxNumSeqs        int     `yaml:"max_num_seqs" json:"max_num_seqs"`
	SchedulerSettings string  `yaml:"scheduler_settings" json:"scheduler_settings"`
	ChunkedPrefill    OnOff   `yaml:"chunked_prefill" json:"chunked_prefill"` // "on" | "off"
	CUDAGraphs        OnOff   `yaml:"cuda_graphs" json:"cuda_graphs"`         // "on" | "off"
	// PrefixCaching must be "off" (§6: verified OFF via server metrics at
	// run time; defaults are version-dependent, verify, do not assume).
	PrefixCaching OnOff `yaml:"prefix_caching" json:"prefix_caching"`
	// ContinuousBatching must be "on" (§6: verified active from server
	// metrics at run time).
	ContinuousBatching OnOff `yaml:"continuous_batching" json:"continuous_batching"`
}

type GPUPins struct {
	SKU    string `yaml:"sku" json:"sku"`
	Driver string `yaml:"driver" json:"driver"`
	CUDA   string `yaml:"cuda" json:"cuda"`
	CUDNN  string `yaml:"cudnn" json:"cudnn"`
	NCCL   string `yaml:"nccl" json:"nccl"`
	// ClockPowerPolicy is asserted equal across runs via a per-replica
	// per-run nvidia-smi fingerprint at Phase 1 (§6).
	ClockPowerPolicy string `yaml:"clock_power_policy" json:"clock_power_policy"`
}

type KubernetesPins struct {
	Version       string `yaml:"version" json:"version"`
	CNI           string `yaml:"cni" json:"cni"`
	DataplaneMode string `yaml:"dataplane_mode" json:"dataplane_mode"` // e.g. "kube-proxy-iptables", "kube-proxy-ipvs", "ebpf-cilium"
	KubeProxyMode string `yaml:"kube_proxy_mode" json:"kube_proxy_mode"`
	// NodeMonitorGracePeriodS records the cluster's actual value (§1).
	NodeMonitorGracePeriodS int `yaml:"node_monitor_grace_period_s" json:"node_monitor_grace_period_s"`
}

type ReadinessProbe struct {
	Path             string `yaml:"path" json:"path"`
	PeriodS          int    `yaml:"period_s" json:"period_s"`
	TimeoutS         int    `yaml:"timeout_s" json:"timeout_s"`
	FailureThreshold int    `yaml:"failure_threshold" json:"failure_threshold"`
}

type StoragePins struct {
	WeightsMedium string `yaml:"weights_medium" json:"weights_medium"`
}

// ---------------------------------------------------------------------------
// Mock inference server (Phase 0, §2 "Local-first").
// ---------------------------------------------------------------------------

// Mock fault modes (§2): stall, error, stream-abort, slow-reload-on-
// reschedule (see Mock.SlowReload), and silent-hang (no RST).
const (
	MockFaultStall       = "stall"
	MockFaultError       = "error"
	MockFaultStreamAbort = "stream_abort"
	MockFaultSilentHang  = "silent_hang"
)

// Mock configures the mock inference server: an OpenAI-compatible SSE
// server with configurable TTFT and per-token latency and scriptable
// fault modes.
type Mock struct {
	ListenAddr string `yaml:"listen_addr" json:"listen_addr"`
	Seed       int64  `yaml:"seed" json:"seed"`

	TTFT LatencyDist `yaml:"ttft" json:"ttft"`
	ITL  LatencyDist `yaml:"itl" json:"itl"`

	// SlowReload models slow weight-load on reschedule: for DurationS
	// after process start the server is not ready (health and inference
	// return 503). It is a startup property, not a scheduled window: a
	// rescheduled pod exhibits it on every start.
	SlowReload SlowReload `yaml:"slow_reload" json:"slow_reload"`

	// ServeHealthDuringSilentHang, when true, keeps /health answering
	// during a silent_hang window. The zero value (false) is the faithful
	// full-node-partition behavior — /health blackholes with the data
	// plane — so an omitted field defaults to fidelity. The /admin and
	// /metrics endpoints always stay reachable: they are out-of-band
	// harness instrumentation, not the emulated data plane.
	ServeHealthDuringSilentHang bool `yaml:"serve_health_during_silent_hang" json:"serve_health_during_silent_hang"`

	// FaultSchedule is the config-scripted fault plan, offsets measured
	// on the monotonic clock from server start. Windows must not overlap.
	// Faults may also be armed at run time via POST /admin/faults (the
	// chaos orchestrator's path); armed/fire/expiry timestamps are
	// recorded for both sources.
	FaultSchedule []MockFault `yaml:"fault_schedule" json:"fault_schedule"`
}

// Distribution is the latency-distribution kind for a LatencyDist (§2 AC1).
// Only distributions with closed-form analytic quantiles are admitted, so
// the harness can be validated against a known injected latency.
type Distribution string

const (
	DistributionFixed   Distribution = "fixed"
	DistributionUniform Distribution = "uniform"
)

// LatencyDist is a known injected latency distribution (AC1 requires the
// harness to be checked against one). Distributions with analytic
// quantiles only.
type LatencyDist struct {
	Distribution Distribution `yaml:"distribution" json:"distribution"` // "fixed" | "uniform"
	FixedMs      float64      `yaml:"fixed_ms,omitempty" json:"fixed_ms,omitempty"`
	MinMs        float64      `yaml:"min_ms,omitempty" json:"min_ms,omitempty"`
	MaxMs        float64      `yaml:"max_ms,omitempty" json:"max_ms,omitempty"`
}

type SlowReload struct {
	Enabled   bool    `yaml:"enabled" json:"enabled"`
	DurationS float64 `yaml:"duration_s" json:"duration_s"`
}

// MockFault is one scheduled fault window.
type MockFault struct {
	Mode         string  `yaml:"mode" json:"mode"`
	StartOffsetS float64 `yaml:"start_offset_s" json:"start_offset_s"`
	DurationS    float64 `yaml:"duration_s" json:"duration_s"`
	// AbortAfterTokens applies to stream_abort only: new streams admitted
	// during the window are RST after this many tokens (in-flight streams
	// are RST immediately at fire time).
	AbortAfterTokens int `yaml:"abort_after_tokens,omitempty" json:"abort_after_tokens,omitempty"`
}
