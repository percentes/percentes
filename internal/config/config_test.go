package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	acRef         = "../../configs/ac.reference.yaml"
	experimentRef = "../../configs/experiment.reference.yaml"
)

func loadRef(t *testing.T, path string) *Config {
	t.Helper()
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load %s: %v", path, err)
	}
	return cfg
}

func TestReferenceConfigsLoadAndValidate(t *testing.T) {
	for _, p := range []string{acRef, experimentRef} {
		if _, err := LoadFile(p); err != nil {
			t.Errorf("reference config %s must load and validate: %v", p, err)
		}
	}
}

// TestSection6PinCoverage is the literal SPEC.md §6 checklist: every pin
// item in the section maps to a populated schema field. If a pin is ever
// dropped from the schema, this test names it.
func TestSection6PinCoverage(t *testing.T) {
	c := loadRef(t, experimentRef)
	strPins := map[string]string{
		"vLLM version":                      c.Pins.VLLM.Version,
		"vLLM image digest":                 c.Pins.VLLM.ImageDigest,
		"model":                             c.Pins.Model.Name,
		"model revision":                    c.Pins.Model.Revision,
		"quantization":                      c.Pins.Model.Quantization,
		"scheduler settings":                c.Pins.Engine.SchedulerSettings,
		"chunked-prefill setting":           string(c.Pins.Engine.ChunkedPrefill),
		"CUDA-graph enablement":             string(c.Pins.Engine.CUDAGraphs),
		"prefix caching (verified OFF)":     string(c.Pins.Engine.PrefixCaching),
		"continuous batching (verified on)": string(c.Pins.Engine.ContinuousBatching),
		"GPU SKU":                           c.Pins.GPU.SKU,
		"GPU driver":                        c.Pins.GPU.Driver,
		"CUDA":                              c.Pins.GPU.CUDA,
		"cuDNN":                             c.Pins.GPU.CUDNN,
		"NCCL":                              c.Pins.GPU.NCCL,
		"GPU clock and power policy":        c.Pins.GPU.ClockPowerPolicy,
		"Kubernetes version":                c.Pins.Kubernetes.Version,
		"CNI":                               c.Pins.Kubernetes.CNI,
		"dataplane mode":                    c.Pins.Kubernetes.DataplaneMode,
		"kube-proxy mode":                   c.Pins.Kubernetes.KubeProxyMode,
		"readiness probe path":              c.Pins.Readiness.Path,
		"storage medium for weights":        c.Pins.Storage.WeightsMedium,
		"client placement node class":       string(c.Client.Placement.NodeClass),
		"client placement zone":             c.Client.Placement.Zone,
		"client placement subnet":           c.Client.Placement.Subnet,
	}
	for name, val := range strPins {
		if strings.TrimSpace(val) == "" {
			t.Errorf("§6 pin %q: schema field empty in experiment reference", name)
		}
	}

	if c.Pins.Engine.KVCacheGB <= 0 {
		t.Error("§6 pin \"KV-cache budget in absolute gigabytes\": not carried")
	}
	if c.Pins.Engine.MaxNumSeqs <= 0 {
		t.Error("§6 pin \"max-num-seqs\": not carried")
	}
	if c.Pins.Kubernetes.NodeMonitorGracePeriodS <= 0 {
		t.Error("§6 pin \"node-monitor-grace-period\": not carried")
	}
	if c.Pins.Readiness.PeriodS <= 0 || c.Pins.Readiness.TimeoutS <= 0 || c.Pins.Readiness.FailureThreshold <= 0 {
		t.Error("§6 pin \"readiness probe configuration\": not carried")
	}
	if c.Client.HTTPTimeoutS != 30 {
		t.Error("§6 pin \"client HTTP timeout (30 s)\": not carried")
	}
	if c.Client.Retries != 0 {
		t.Error("§6 pin \"retries (zero)\": not carried")
	}
	if c.Load.MaxTokens != 256 || !c.Load.IgnoreEOS {
		t.Error("§6 pin \"max_tokens=256 with ignore_eos\": not carried")
	}
	if !c.Load.UniquePrefixes {
		t.Error("§6 pin \"unique per-request prefixes\": not carried")
	}
	if !c.Client.Placement.DedicatedNode || !c.Client.Placement.RecordRTT {
		t.Error("§6 pin \"client placement: dedicated non-spot node, RTT recorded\": not carried")
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	raw, err := os.ReadFile(acRef)
	if err != nil {
		t.Fatal(err)
	}
	bad := string(raw) + "\nnot_a_real_section:\n  oops: 1\n"
	if _, err := Parse([]byte(bad)); err == nil {
		t.Fatal("config with unknown top-level field must be rejected (strict decode)")
	}
	// A typo inside a nested section must also be rejected.
	bad2 := strings.Replace(string(raw), "  retries: 0", "  retries: 0\n  retrys: 1", 1)
	if _, err := Parse([]byte(bad2)); err == nil {
		t.Fatal("config with unknown nested field must be rejected (strict decode)")
	}
}

// TestPinnedValueEnforcement mutates each pre-registered number and
// asserts validation rejects the config, naming the field. The gates are
// unweakenable by construction.
func TestPinnedValueEnforcement(t *testing.T) {
	cases := []struct {
		name    string
		ref     string
		mutate  func(*Config)
		wantErr string
	}{
		{"client timeout widened", acRef, func(c *Config) { c.Client.HTTPTimeoutS = 60 }, "client.http_timeout_s"},
		{"client timeout narrowed", acRef, func(c *Config) { c.Client.HTTPTimeoutS = 29 }, "client.http_timeout_s"},
		{"retries enabled", acRef, func(c *Config) { c.Client.Retries = 1 }, "client.retries"},
		{"max_tokens changed", acRef, func(c *Config) { c.Load.MaxTokens = 128 }, "load.max_tokens"},
		{"ignore_eos disabled", acRef, func(c *Config) { c.Load.IgnoreEOS = false }, "load.ignore_eos"},
		{"unique prefixes disabled", acRef, func(c *Config) { c.Load.UniquePrefixes = false }, "load.unique_prefixes"},
		{"connections under-provisioned", acRef, func(c *Config) { c.Load.Connections = 7 }, "load.connections"},
		{"ac lambda off reference", acRef, func(c *Config) { c.Load.RateRPS = 25 }, "load.rate_rps"},
		{"slo ttft weakened", acRef, func(c *Config) { c.SLO.TTFTMs = 1500 }, "slo.ttft_ms"},
		{"slo e2e weakened", acRef, func(c *Config) { c.SLO.E2EMs = 20000 }, "slo.e2e_ms"},
		{"slo sweep grid altered", acRef, func(c *Config) { c.SLO.Sweep.TTFTMs = []int{800, 1000} }, "slo.sweep.ttft_ms"},
		{"histogram too small", acRef, func(c *Config) { c.Histogram.HighestTrackableValue = 30_000_000 }, "histogram.highest_trackable_value"},
		{"histogram unit changed", acRef, func(c *Config) { c.Histogram.Unit = "ms" }, "histogram.unit"},
		{"histogram sigfigs changed", acRef, func(c *Config) { c.Histogram.SignificantFigures = 2 }, "histogram.significant_figures"},
		{"detector entry weakened", acRef, func(c *Config) { c.RecoveryDetector.EntryPct = 80 }, "recovery_detector.entry_pct"},
		{"detector exit weakened", acRef, func(c *Config) { c.RecoveryDetector.ExitPct = 70 }, "recovery_detector.exit_pct"},
		{"detector window changed", acRef, func(c *Config) { c.RecoveryDetector.WindowS = 5 }, "recovery_detector.window_s"},
		{"detector hold changed", acRef, func(c *Config) { c.RecoveryDetector.HoldS = 15 }, "recovery_detector.hold_s"},
		{"sensitivity grid truncated", acRef, func(c *Config) { c.RecoveryDetector.Sensitivity.EntryPct = []int{90} }, "recovery_detector.sensitivity.entry_pct"},
		{"slo e2e sweep grid altered", acRef, func(c *Config) { c.SLO.Sweep.E2EMs = []int{12000, 14000} }, "slo.sweep.e2e_ms"},
		{"histogram lowest changed", acRef, func(c *Config) { c.Histogram.LowestDiscernibleValue = 2 }, "histogram.lowest_discernible_value"},
		{"sensitivity window grid truncated", acRef, func(c *Config) { c.RecoveryDetector.Sensitivity.WindowS = []int{10} }, "recovery_detector.sensitivity.window_s"},
		{"sensitivity hold grid altered", acRef, func(c *Config) { c.RecoveryDetector.Sensitivity.HoldS = []int{15, 30, 90} }, "recovery_detector.sensitivity.hold_s"},
		{"send skew p99 widened", acRef, func(c *Config) { c.ClientValidity.SendSkewP99Ms = 10 }, "client_validity.send_skew_p99_ms"},
		{"cpu window changed", acRef, func(c *Config) { c.ClientValidity.CPUWindowS = 10 }, "client_validity.cpu_window_s"},
		{"send skew max widened", acRef, func(c *Config) { c.ClientValidity.SendSkewMaxMs = 100 }, "client_validity.send_skew_max_ms"},
		{"cpu gate widened", acRef, func(c *Config) { c.ClientValidity.MaxCPUPct = 90 }, "client_validity.max_cpu_pct"},
		{"gc gate widened", acRef, func(c *Config) { c.ClientValidity.GoGCPauseP99Ms = 5 }, "client_validity.go_gc_pause_p99_ms"},
		{"share gate widened", acRef, func(c *Config) { c.ShareGate.MinPct = 40 }, "share_gate.min_pct"},
		{"share gate max widened", acRef, func(c *Config) { c.ShareGate.MaxPct = 60 }, "share_gate.max_pct"},
		{"injection tolerance widened", acRef, func(c *Config) { c.Fault.InjectionToleranceMs = 1000 }, "fault.injection_tolerance_ms"},
		{"staleness gate weakened", acRef, func(c *Config) { c.Fault.EndpointStalenessMinS = 10 }, "fault.endpoint_staleness_min_s"},
		{"t_inject inside baseline", acRef, func(c *Config) { c.Fault.TInjectOffsetS = 30 }, "fault.t_inject_offset_s"},
		{"prefix caching on", acRef, func(c *Config) { c.Pins.Engine.PrefixCaching = "on" }, "pins.engine.prefix_caching"},
		{"continuous batching off", acRef, func(c *Config) { c.Pins.Engine.ContinuousBatching = "off" }, "pins.engine.continuous_batching"},
		{"missing pin (empty digest)", acRef, func(c *Config) { c.Pins.VLLM.ImageDigest = "" }, "pins.vllm.image_digest"},
		{"missing pin (empty CNI)", acRef, func(c *Config) { c.Pins.Kubernetes.CNI = " " }, "pins.kubernetes.cni"},
		{"node monitor grace unset", acRef, func(c *Config) { c.Pins.Kubernetes.NodeMonitorGracePeriodS = 0 }, "node_monitor_grace_period_s"},
		{"ac profile with real variant", acRef, func(c *Config) { c.Fault.Variant = VariantCleanDelete }, "fault.variant"},
		{"mock section missing", acRef, func(c *Config) { c.Mock = nil }, "mock: required"},

		{"experiment t_inject gap after baseline", experimentRef, func(c *Config) { c.Fault.TInjectOffsetS = 500 }, "fault.t_inject_offset_s"},
		{"experiment phases shortened", experimentRef, func(c *Config) { c.Run.Phases.BaselineS = 60 }, "run.phases.baseline_s"},
		{"experiment warmup shortened", experimentRef, func(c *Config) { c.Run.Phases.WarmupS = 10 }, "run.phases.warmup_s"},
		{"experiment timeout shortened", experimentRef, func(c *Config) { c.Run.Phases.FaultWindowTimeoutS = 300 }, "run.phases.fault_window_timeout_s"},
		{"experiment repetitions cut", experimentRef, func(c *Config) { c.Run.Repetitions = 3 }, "run.repetitions"},
		{"experiment replicas changed", experimentRef, func(c *Config) { c.Target.Replicas = 3 }, "target.replicas"},
		{"experiment spot client", experimentRef, func(c *Config) { c.Client.Placement.NodeClass = NodeClassSpot }, "client.placement.node_class"},
		{"experiment rtt not recorded", experimentRef, func(c *Config) { c.Client.Placement.RecordRTT = false }, "client.placement.record_rtt"},
		{"experiment kv cache unpinned", experimentRef, func(c *Config) { c.Pins.Engine.KVCacheGB = 0 }, "pins.engine.kv_cache_gb"},
		{"experiment with mock section", experimentRef, func(c *Config) { c.Mock = &Mock{} }, "mock: must be absent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := loadRef(t, tc.ref)
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("mutation %q must be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error must name %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestMockScheduleValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"overlapping windows", func(c *Config) {
			c.Mock.FaultSchedule = []MockFault{
				{Mode: MockFaultStall, StartOffsetS: 10, DurationS: 10},
				{Mode: MockFaultError, StartOffsetS: 15, DurationS: 5},
			}
		}, "overlap"},
		{"unknown mode", func(c *Config) {
			c.Mock.FaultSchedule = []MockFault{{Mode: "explode", StartOffsetS: 1, DurationS: 1}}
		}, "mode"},
		{"zero duration", func(c *Config) {
			c.Mock.FaultSchedule = []MockFault{{Mode: MockFaultStall, StartOffsetS: 1, DurationS: 0}}
		}, "duration_s"},
		{"abort_after_tokens on stall", func(c *Config) {
			c.Mock.FaultSchedule = []MockFault{{Mode: MockFaultStall, StartOffsetS: 1, DurationS: 1, AbortAfterTokens: 3}}
		}, "abort_after_tokens"},
		{"bad ttft distribution", func(c *Config) {
			c.Mock.TTFT = LatencyDist{Distribution: "lognormal", FixedMs: 5}
		}, "mock.ttft.distribution"},
		{"uniform bounds inverted", func(c *Config) {
			c.Mock.ITL = LatencyDist{Distribution: "uniform", MinMs: 20, MaxMs: 10}
		}, "mock.itl"},
		{"slow reload without duration", func(c *Config) {
			c.Mock.SlowReload = SlowReload{Enabled: true, DurationS: 0}
		}, "mock.slow_reload.duration_s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := loadRef(t, acRef)
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("mutation %q must be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error must mention %q, got: %v", tc.wantErr, err)
			}
		})
	}

	// Adjacent (non-overlapping) windows are legal: flapping scenarios.
	c := loadRef(t, acRef)
	c.Mock.FaultSchedule = []MockFault{
		{Mode: MockFaultStall, StartOffsetS: 10, DurationS: 5},
		{Mode: MockFaultStall, StartOffsetS: 15, DurationS: 5},
		{Mode: MockFaultSilentHang, StartOffsetS: 30, DurationS: 10},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("adjacent windows must be legal: %v", err)
	}
}

// TestConfigJSONRecord: the report generator embeds the full config as
// JSON (§2: "full metric set as JSON ... from one config"). Round-trip
// must preserve every pin.
func TestConfigJSONRecord(t *testing.T) {
	c := loadRef(t, acRef)
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Config
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := back.Validate(); err != nil {
		t.Fatalf("JSON round-trip must preserve a valid config: %v", err)
	}
	if back.Client.HTTPTimeoutS != 30 || back.Load.MaxTokens != 256 || back.Mock == nil {
		t.Fatal("JSON round-trip lost pinned fields")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := LoadFile(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("missing file must error")
	}
}
