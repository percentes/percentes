package config

import (
	"os"
	"strings"
	"testing"
)

const releaseRef = "../../configs/release.reference.yaml"

func TestReleaseReferenceLoads(t *testing.T) {
	c := loadRef(t, releaseRef)
	if c.Profile != ProfileRelease || c.Release == nil || c.Pilot != nil {
		t.Fatalf("release reference: profile %q, release %v, pilot %v", c.Profile, c.Release, c.Pilot)
	}
	if c.Target.Name != "groq" || c.Target.Adapter != AdapterChatCompletions || !c.Target.Hosted {
		t.Errorf("target: %+v", c.Target)
	}
	if got := c.Target.Limits[0].Retrieved; got != "2026-10-07" {
		t.Errorf("retrieved %q", got)
	}
	if got := c.ReleaseConnectionProvision(); got != 752 {
		t.Errorf("connection provision %d, want 2 x 4 rps x 90 s + 32 = 752", got)
	}
	c.Release.Shape.CeilingTokens = 1024
	c.Target.Body = strings.Replace(c.Target.Body, `"max_completion_tokens": 512`, `"max_completion_tokens": 1024`, 1)
	if err := c.Validate(); err != nil {
		t.Errorf("fallback ceiling 1024: %v", err)
	}
}

// Each mutation breaks one release pin; the error names the field.
func TestReleaseRefusals(t *testing.T) {
	limit := func(c *Config) *Limit { return &c.Target.Limits[0] }
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no release section", func(c *Config) { c.Release = nil }, "release: required"},
		{"four steps", func(c *Config) { c.Release.Ladder.StepsRPS = []float64{0.25, 0.5, 1, 2} }, "release.ladder.steps_rps: pinned [0.25 0.5 1 2 4]"},
		{"descending steps", func(c *Config) { c.Release.Ladder.StepsRPS = []float64{4, 2, 1, 0.5, 0.25} }, "release.ladder.steps_rps"},
		{"step 8 rps", func(c *Config) { c.Release.Ladder.StepsRPS[4] = 8 }, "release.ladder.steps_rps"},
		{"warm-up", func(c *Config) { c.Release.Ladder.WarmupS = 30 }, "release.ladder.warmup_s: pinned 60"},
		{"step window", func(c *Config) { c.Release.Ladder.StepS = 300 }, "release.ladder.step_s: pinned 480"},
		{"drain", func(c *Config) { c.Release.Ladder.DrainS = 30 }, "release.ladder.drain_s: pinned 90"},
		{"shape name", func(c *Config) { c.Release.Shape.Name = "L" }, "release.shape.name: pinned \"S\""},
		{"input bound", func(c *Config) { c.Release.Shape.InputBoundTokens = 8000 }, "release.shape.input_bound_tokens: pinned 1200"},
		{"ceiling 256", func(c *Config) { c.Release.Shape.CeilingTokens = 256 }, "release.shape.ceiling_tokens: must be one of [512 1024]"},
		{"ceiling 2048", func(c *Config) { c.Release.Shape.CeilingTokens = 2048 }, "release.shape.ceiling_tokens"},
		{"deadline", func(c *Config) { c.Release.Shape.DeadlineS = 60 }, "release.shape.deadline_s: pinned 30"},
		{"hard timeout 30", func(c *Config) { c.Client.HTTPTimeoutS = 30 }, "client.http_timeout_s: the hard timeout, pinned 90"},
		{"retries", func(c *Config) { c.Client.Retries = 1 }, "client.retries"},
		{"deterministic", func(c *Config) { c.Load.ArrivalProcess = "deterministic" }, "load.arrival_process: pinned \"poisson\""},
		{"single rate", func(c *Config) { c.Load.RateRPS = 0.5 }, "load.rate_rps: must be absent"},
		{"load max_tokens", func(c *Config) { c.Load.MaxTokens = 512 }, "load.max_tokens"},
		{"load input length", func(c *Config) { c.Load.InputLengthTokens = 1200 }, "load.input_length_tokens"},
		{"ignore_eos", func(c *Config) { c.Load.IgnoreEOS = true }, "load.ignore_eos: pinned false"},
		{"no unique prefixes", func(c *Config) { c.Load.UniquePrefixes = false }, "load.unique_prefixes: pinned true"},
		{"connections", func(c *Config) { c.Load.Connections = 64 }, "load.connections: pinned 32"},
		{"phases", func(c *Config) { c.Run.Phases.BaselineS = 1200 }, "run.phases: must be absent"},
		{"repetitions", func(c *Config) { c.Run.Repetitions = 1 }, "run.repetitions: must be absent"},
		{"no run name", func(c *Config) { c.Run.Name = "" }, "run.name: required"},
		{"histogram", func(c *Config) { c.Histogram.SignificantFigures = 2 }, "histogram.significant_figures"},
		{"client validity", func(c *Config) { c.ClientValidity.MaxCPUPct = 90 }, "client_validity.max_cpu_pct"},
		{"slo", func(c *Config) { c.SLO.TTFTMs = PinnedSLOTTFTMs }, "slo: must be absent"},
		{"detector", func(c *Config) { c.RecoveryDetector.WindowS = PinnedDetectorWindowS }, "recovery_detector: must be absent"},
		{"share gate", func(c *Config) { c.ShareGate.MinPct = PinnedShareMinPct }, "share_gate: must be absent"},
		{"fault", func(c *Config) { c.Fault.Variant = VariantNone }, "fault: must be absent"},
		{"pins", func(c *Config) { c.Pins.VLLM.Version = "v0" }, "pins: must be absent"},
		{"mock", func(c *Config) { c.Mock = &Mock{} }, "mock: must be absent"},
		{"calibration", func(c *Config) { c.Calibration = &Calibration{} }, "calibration: must be absent"},
		{"pilot", func(c *Config) { c.Pilot = &Pilot{} }, "pilot: must be absent"},
		{"not hosted", func(c *Config) { c.Target.Hosted = false }, "target.hosted: pinned true"},
		{"no name", func(c *Config) { c.Target.Name = "" }, "target.name"},
		{"no model", func(c *Config) { c.Target.ModelName = "" }, "target.model_name: required"},
		{"no key env", func(c *Config) { c.Target.APIKeyEnv = "" }, "target.api_key_env: required"},
		{"unknown key header", func(c *Config) { c.Target.AuthHeader = "bearer" }, "target.auth_header: must be one of"},
		{"no endpoint", func(c *Config) { c.Target.EndpointURL = "" }, "target.endpoint_url"},
		{"endpoint without path", func(c *Config) { c.Target.EndpointURL = "https://api.groq.com" }, "complete request path"},
		{"base url", func(c *Config) { c.Target.BaseURL = "https://api.groq.com/openai" }, "target.base_url: must be absent"},
		{"replicas", func(c *Config) { c.Target.Replicas = 1 }, "target.replicas: must be absent"},
		{"metrics", func(c *Config) { c.Target.MetricsURLs = []string{"http://x/metrics"} }, "not read for a hosted target"},
		{"params", func(c *Config) { c.Target.Params = &Params{MaxTokens: 512} }, "params and not_applied are set on pilot.targets entries only"},
		{"no adapter", func(c *Config) { c.Target.Adapter = "" }, "target.adapter: must be one of"},
		{"unknown adapter", func(c *Config) { c.Target.Adapter = "responses" }, "target.adapter: must be one of"},
		{"no body", func(c *Config) { c.Target.Body = "" }, "target.body: required under the \"release\" profile"},
		{"body without slot", func(c *Config) { c.Target.Body = `{"stream":true,"max_completion_tokens":512}` }, "target.body: \"{{case}}\" must appear once"},
		{"body ceiling off the shape", func(c *Config) { c.Release.Shape.CeilingTokens = 1024 }, "\"max_completion_tokens\" must be release.shape.ceiling_tokens (1024), got 512"},
		{"probe status", func(c *Config) { c.Target.Probe = &TargetProbe{AccountLimitStatuses: []int{401}} }, "target.probe.account_limit_statuses[0]"},
		{"credential header recorded", func(c *Config) { c.Target.ResponseHeaders = []string{"Authorization"} }, "never recorded"},
		{"no limits", func(c *Config) { c.Target.Limits = nil }, "target.limits: required"},
		{"limit dimension", func(c *Config) { limit(c).Dimension = "rpm" }, "target.limits[0].dimension: must be one of"},
		{"limit value", func(c *Config) { limit(c).Value = 0 }, "target.limits[0].value: must be > 0"},
		{"limit window off a window dimension", func(c *Config) { limit(c).WindowS = 600 }, "target.limits[0].window_s: set for \"spend_per_window_usd\" only"},
		{"spend window without window", func(c *Config) { limit(c).Dimension = LimitSpendWindow }, "target.limits[0].window_s: must be > 0"},
		{"limit source", func(c *Config) { limit(c).Source = " " }, "target.limits[0].source: required"},
		{"limit source scheme", func(c *Config) { limit(c).Source = "ftp://example.com/limits" }, "target.limits[0].source: \"ftp://example.com/limits\" is not an http"},
		{"limit date", func(c *Config) { limit(c).Retrieved = "7 October 2026" }, "target.limits[0].retrieved: a date as YYYY-MM-DD"},
		{"limit no date", func(c *Config) { limit(c).Retrieved = "" }, "target.limits[0].retrieved"},
		{"limit hash", func(c *Config) { limit(c).SHA256 = strings.Repeat("A", 64) }, "target.limits[0].sha256: 64 lowercase hex"},
		{"limit short hash", func(c *Config) { limit(c).SHA256 = "abc" }, "target.limits[0].sha256"},
		{"no naming", func(c *Config) { c.Target.Naming = nil }, "target.naming: required"},
		{"naming provider", func(c *Config) { c.Target.Naming.Provider = "" }, "target.naming.provider: required"},
		{"naming lines", func(c *Config) { c.Target.Naming.Lines = nil }, "target.naming.lines: required"},
		{"naming empty line", func(c *Config) { c.Target.Naming.Lines[1] = " " }, "target.naming.lines[1]: empty"},
		{"naming repeated line", func(c *Config) { c.Target.Naming.Lines[1] = c.Target.Naming.Lines[0] }, "target.naming.lines[1]: listed twice"},
		{"permission unrecorded", func(c *Config) { c.Target.Naming.PermissionRequired = true }, "target.naming.permission_record: required"},
	} {
		c := loadRef(t, releaseRef)
		tc.mutate(c)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

// Operator-set limits, rolling spend windows, URL sources and recorded
// permission load.
func TestReleaseLimitAndNamingForms(t *testing.T) {
	c := loadRef(t, releaseRef)
	c.Target.Limits = append(c.Target.Limits,
		Limit{Dimension: LimitSpendWindow, Value: 10, WindowS: 600, Source: "https://ai.google.dev/gemini-api/docs/rate-limits", Retrieved: "2026-10-07", SHA256: strings.Repeat("ab", 32)},
		Limit{Dimension: LimitSpendMonth, Value: 10, OperatorSet: true, Source: "spend-limit.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64)},
	)
	c.Target.Naming.PermissionRequired, c.Target.Naming.PermissionRecord = true, "manifest: written permission"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseBody(t *testing.T) {
	const chat = `{"model":"m","messages":[{"role":"user","content":"{{case}}"}],"stream":true,%s}`
	const messages = `{"model":"m","system":"s","messages":[{"role":"user","content":"{{case}}"}],"stream":true,%s}`
	for _, tc := range []struct {
		name, adapter, body, want string
	}{
		{"chat max_completion_tokens", AdapterChatCompletions, `"max_completion_tokens":512`, ""},
		{"chat max_tokens", AdapterChatCompletions, `"max_tokens":512`, ""},
		{"messages max_tokens", AdapterMessages, `"max_tokens":512`, ""},
		{"messages max_completion_tokens", AdapterMessages, `"max_completion_tokens":512`, `"max_completion_tokens" is not the ceiling key of the "messages" adapter`},
		{"chat both keys", AdapterChatCompletions, `"max_tokens":512,"max_completion_tokens":512`, `["max_tokens" "max_completion_tokens"] both set`},
		{"chat no ceiling", AdapterChatCompletions, `"reasoning_effort":"low"`, `one ceiling key of ["max_tokens" "max_completion_tokens"] required`},
		{"messages no ceiling", AdapterMessages, `"thinking":{"type":"between_tools"}`, `one ceiling key of ["max_tokens"] required`},
		{"ceiling off", AdapterChatCompletions, `"max_tokens":513`, `"max_tokens" must be release.shape.ceiling_tokens (512), got 513`},
		{"ceiling as a string", AdapterChatCompletions, `"max_tokens":"512"`, `"max_tokens" must be release.shape.ceiling_tokens (512), got "512"`},
		{"temperature", AdapterChatCompletions, `"max_tokens":512,"temperature":0`, `"temperature" is not sent`},
		{"top_p", AdapterMessages, `"max_tokens":512,"top_p":1`, `"top_p" is not sent`},
		{"ignore_eos", AdapterChatCompletions, `"max_tokens":512,"ignore_eos":false`, `"ignore_eos" is not sent`},
		{"min_tokens", AdapterChatCompletions, `"max_tokens":512,"min_tokens":512`, `"min_tokens" is not sent`},
	} {
		tmpl := chat
		if tc.adapter == AdapterMessages {
			tmpl = messages
		}
		c := loadRef(t, releaseRef)
		c.Target.Adapter = tc.adapter
		c.Target.Body = strings.Replace(tmpl, "%s", tc.body, 1)
		err := c.Validate()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: must validate: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), "target.body: "+tc.want)):
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

// The release section and the release-only target fields refuse to load
// under every other profile.
func TestReleaseFieldsRefusedElsewhere(t *testing.T) {
	rel := loadRef(t, releaseRef)
	for _, ref := range []string{acRef, experimentRef, pilotRef} {
		c := loadRef(t, ref)
		c.Release = rel.Release
		want := "release: must be absent unless profile is \"release\""
		if ref == pilotRef {
			want = "release: must be absent under the pilot profile"
		}
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: release section: got %v", ref, err)
		}
	}
	for _, f := range []func(*Target){
		func(t *Target) { t.Adapter = AdapterChatCompletions },
		func(t *Target) { t.Limits = rel.Target.Limits },
		func(t *Target) { t.Naming = rel.Target.Naming },
	} {
		for _, ref := range []string{acRef, experimentRef} {
			c := loadRef(t, ref)
			f(&c.Target)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "target: adapter, limits and naming are set on the release profile's target only") {
				t.Errorf("%s: got %v", ref, err)
			}
		}
		p := loadRef(t, pilotRef)
		f(&p.Pilot.Targets[1])
		if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "pilot.targets[1]: adapter, limits and naming are set on the release profile's target only") {
			t.Errorf("pilot: got %v", err)
		}
	}
}

// A typo in a release field never silently drops the pin.
func TestReleaseUnknownFieldRejected(t *testing.T) {
	raw, err := os.ReadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	for _, from := range []string{"    drain_s: 90", "    deadline_s: 30", "      retrieved: 2026-10-07"} {
		bad := strings.Replace(string(raw), from, from+"\n"+strings.Repeat(" ", len(from)-len(strings.TrimLeft(from, " ")))+"drain_ss: 90", 1)
		if bad == string(raw) {
			t.Fatalf("%q not in the reference", from)
		}
		if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "drain_ss") {
			t.Errorf("unknown field after %q: got %v", from, err)
		}
	}
}
