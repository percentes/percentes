package config

import (
	"os"
	"strings"
	"testing"
)

const pilotRef = "../../configs/pilot.reference.yaml"

func TestPilotReferenceLoads(t *testing.T) {
	c := loadRef(t, pilotRef)
	if c.Profile != ProfilePilot || c.Pilot == nil || len(c.Pilot.Targets) != 2 {
		t.Fatalf("pilot reference: profile %q, pilot section %v", c.Profile, c.Pilot)
	}
	groq, baseten := c.Pilot.Targets[0], c.Pilot.Targets[1]
	if groq.EndpointURL != "https://api.groq.com/openai/v1/chat/completions" ||
		baseten.EndpointURL != "https://inference.baseten.co/v1/chat/completions" {
		t.Errorf("endpoint urls %q, %q", groq.EndpointURL, baseten.EndpointURL)
	}
	if groq.Params.MaxCompletionTokens != 4096 || groq.Params.MaxTokens != 0 ||
		baseten.Params.MaxTokens != 4096 || baseten.Params.MaxCompletionTokens != 0 {
		t.Errorf("ceilings: groq %+v, baseten %+v", groq.Params, baseten.Params)
	}
	if c.Target.BaseURL != "" || c.Target.Replicas != 0 {
		t.Error("the single target block must be absent")
	}
}

// Each mutation breaks one pilot pin; the error names the field.
func TestPilotRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"ceiling 4095", func(c *Config) { c.Pilot.Targets[0].Params.MaxCompletionTokens = 4095 }, "pilot.targets[0].params: completion ceiling pinned 4096"},
		{"ceiling 4097", func(c *Config) { c.Pilot.Targets[1].Params.MaxTokens = 4097 }, "pilot.targets[1].params: completion ceiling pinned 4096"},
		{"ceiling 256", func(c *Config) { c.Pilot.Targets[1].Params.MaxTokens = 256 }, "pilot.targets[1].params: completion ceiling pinned 4096"},
		{"both ceiling keys", func(c *Config) { c.Pilot.Targets[0].Params.MaxTokens = 4096 }, "both set"},
		{"no ceiling", func(c *Config) { c.Pilot.Targets[1].Params.MaxTokens = 0 }, "max_completion_tokens or max_tokens required"},
		{"no params", func(c *Config) { c.Pilot.Targets[0].Params = nil }, "pilot.targets[0].params: required"},
		{"ignore_eos true", func(c *Config) { c.Load.IgnoreEOS = true }, "load.ignore_eos"},
		{"unique_prefixes true", func(c *Config) { c.Load.UniquePrefixes = true }, "load.unique_prefixes"},
		{"load max_tokens", func(c *Config) { c.Load.MaxTokens = 256 }, "load.max_tokens"},
		{"load input length", func(c *Config) { c.Load.InputLengthTokens = 512 }, "load.input_length_tokens"},
		{"slo block", func(c *Config) { c.SLO.TTFTMs = PinnedSLOTTFTMs }, "slo: must be absent"},
		{"slo sweep", func(c *Config) { c.SLO.Sweep.E2EMs = PinnedSLOSweepE2EMs }, "slo: must be absent"},
		{"fault window", func(c *Config) { c.Run.Phases.FaultWindowTimeoutS = 600 }, "run.phases.fault_window_timeout_s"},
		{"fault block", func(c *Config) { c.Fault.Variant = VariantNone }, "fault: must be absent"},
		{"cooldown", func(c *Config) { c.Run.Phases.CooldownS = 60 }, "run.phases.cooldown_s"},
		{"replicas on a pilot target", func(c *Config) { c.Pilot.Targets[0].Replicas = 2 }, "pilot.targets[0].replicas"},
		{"single target block", func(c *Config) { c.Target.Replicas = 2 }, "target: must be absent"},
		{"rate 0.5", func(c *Config) { c.Load.RateRPS = 0.5 }, "load.rate_rps: pinned 0.1"},
		{"poisson", func(c *Config) { c.Load.ArrivalProcess = "poisson" }, "load.arrival_process"},
		{"warm-up", func(c *Config) { c.Run.Phases.WarmupS = 30 }, "run.phases.warmup_s: pinned 60"},
		{"measured window", func(c *Config) { c.Run.Phases.BaselineS = 300 }, "run.phases.baseline_s: pinned 1200"},
		{"connections", func(c *Config) { c.Load.Connections = 8 }, "load.connections: pinned 32"},
		{"retries", func(c *Config) { c.Client.Retries = 1 }, "client.retries"},
		{"client validity", func(c *Config) { c.ClientValidity.MaxCPUPct = 90 }, "client_validity.max_cpu_pct"},
		{"share gate", func(c *Config) { c.ShareGate.MinPct = PinnedShareMinPct }, "share_gate: must be absent"},
		{"detector", func(c *Config) { c.RecoveryDetector.WindowS = PinnedDetectorWindowS }, "recovery_detector: must be absent"},
		{"pins", func(c *Config) { c.Pins.VLLM.Version = "v0" }, "pins: must be absent"},
		{"mock", func(c *Config) { c.Mock = &Mock{} }, "mock: must be absent"},
		{"repetitions", func(c *Config) { c.Run.Repetitions = 5 }, "run.repetitions"},
		{"one target", func(c *Config) { c.Pilot.Targets = c.Pilot.Targets[:1] }, "pilot.targets: pinned 2"},
		{"no pilot section", func(c *Config) { c.Pilot = nil }, "pilot: required"},
		{"not hosted", func(c *Config) { c.Pilot.Targets[1].Hosted = false }, "pilot.targets[1].hosted"},
		{"no model", func(c *Config) { c.Pilot.Targets[0].ModelName = "" }, "pilot.targets[0].model_name"},
		{"no key env", func(c *Config) { c.Pilot.Targets[1].APIKeyEnv = "" }, "pilot.targets[1].api_key_env"},
		{"base url", func(c *Config) { c.Pilot.Targets[0].BaseURL = "https://api.groq.com/openai" }, "pilot.targets[0].base_url"},
		{"metrics", func(c *Config) { c.Pilot.Targets[0].MetricsURLs = []string{"http://x/metrics"} }, "not read for a hosted target"},
		{"empty name", func(c *Config) { c.Pilot.Targets[0].Name = "" }, "pilot.targets[0].name"},
		{"repeated name", func(c *Config) { c.Pilot.Targets[1].Name = "groq" }, "pilot.targets[1].name: \"groq\" listed twice"},
		{"repeated endpoint", func(c *Config) { c.Pilot.Targets[1].EndpointURL = c.Pilot.Targets[0].EndpointURL }, "pilot.targets[1].endpoint_url"},
		{"endpoint without path", func(c *Config) { c.Pilot.Targets[0].EndpointURL = "https://api.groq.com" }, "complete request path"},
		{"endpoint with userinfo", func(c *Config) { c.Pilot.Targets[0].EndpointURL = "https://u:p@api.groq.com/v1/chat/completions" }, "no userinfo"},
		{"endpoint with query", func(c *Config) { c.Pilot.Targets[0].EndpointURL = "https://api.groq.com/v1/chat/completions?k=v" }, "no userinfo, query"},
		{"endpoint not http", func(c *Config) { c.Pilot.Targets[0].EndpointURL = "ftp://api.groq.com/v1" }, "pilot.targets[0].endpoint_url: not an http"},
		{"reasoning_format value", func(c *Config) { c.Pilot.Targets[0].Params.ReasoningFormat = "visible" }, "params.reasoning_format: must be one of"},
		{"usage request false", func(c *Config) { c.Pilot.Targets[0].Params.StreamOptions.IncludeUsage = false }, "include_usage must be true"},
		{"negative temperature", func(c *Config) {
			x := -1.0
			c.Pilot.Targets[0].Params.Temperature, c.Pilot.Targets[1].Params.Temperature = &x, &x
		}, "params.temperature: must be >= 0"},
		{"header allowlist", func(c *Config) { c.Pilot.Targets[1].ResponseHeaders = []string{"Authorization"} }, "pilot.targets[1].response_headers[0]"},
		{"response shape", func(c *Config) {
			c.Pilot.Targets[0].ResponseShape = &ResponseShape{Refusal: []string{"choices..refusal"}}
		}, "pilot.targets[0].response_shape.refusal[0]"},
	} {
		c := loadRef(t, pilotRef)
		tc.mutate(c)
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

func TestPilotUnknownParamKeyRefused(t *testing.T) {
	raw, err := os.ReadFile(pilotRef)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"include_reasoning: true", "top_p: 1", "ignore_eos: false", "max_output_tokens: 4096"} {
		bad := strings.Replace(string(raw), "        reasoning_format: parsed\n", "        reasoning_format: parsed\n        "+key+"\n", 1)
		if bad == string(raw) {
			t.Fatal("reference no longer carries the reasoning_format line this test edits")
		}
		if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "not found in type config.Params") {
			t.Errorf("params key %q must be refused, got: %v", key, err)
		}
	}
}

func TestPilotSettingsMatchAcrossTargets(t *testing.T) {
	half, one := 0.5, 1.0
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string // "" = valid
	}{
		{"reference", func(c *Config) {}, ""},
		{"same temperature on both", func(c *Config) {
			c.Pilot.Targets[0].Params.Temperature, c.Pilot.Targets[1].Params.Temperature = &half, &half
		}, ""},
		{"temperature on one", func(c *Config) { c.Pilot.Targets[0].Params.Temperature = &half }, "pilot.targets[1].params.temperature: unset"},
		{"temperature differs", func(c *Config) {
			c.Pilot.Targets[0].Params.Temperature, c.Pilot.Targets[1].Params.Temperature = &half, &one
		}, "pilot.targets[1].params.temperature: 1 differs"},
		{"temperature not applied", func(c *Config) {
			c.Pilot.Targets[0].Params.Temperature = &half
			c.Pilot.Targets[1].NotApplied = append(c.Pilot.Targets[1].NotApplied, "temperature")
		}, ""},
		{"reasoning effort differs", func(c *Config) { c.Pilot.Targets[1].Params.ReasoningEffort = "medium" }, "pilot.targets[1].params.reasoning_effort: medium differs"},
		{"reasoning effort on one", func(c *Config) { c.Pilot.Targets[1].Params.ReasoningEffort = "" }, "pilot.targets[1].params.reasoning_effort: unset"},
		{"reasoning format without not_applied", func(c *Config) { c.Pilot.Targets[1].NotApplied = nil }, "pilot.targets[1].params.reasoning_format: unset"},
		{"reasoning format on both", func(c *Config) {
			c.Pilot.Targets[1].NotApplied = nil
			c.Pilot.Targets[1].Params.ReasoningFormat = "parsed"
		}, ""},
		{"usage request on one", func(c *Config) { c.Pilot.Targets[1].Params.StreamOptions = nil }, "pilot.targets[1].params.stream_options: unset"},
		{"usage request not applied", func(c *Config) {
			c.Pilot.Targets[1].Params.StreamOptions = nil
			c.Pilot.Targets[1].NotApplied = append(c.Pilot.Targets[1].NotApplied, "stream_options")
		}, ""},
		{"not_applied unknown", func(c *Config) { c.Pilot.Targets[1].NotApplied = append(c.Pilot.Targets[1].NotApplied, "top_p") }, "not_applied[1]: \"top_p\" is not a pilot setting"},
		{"not_applied ceiling", func(c *Config) { c.Pilot.Targets[1].NotApplied = append(c.Pilot.Targets[1].NotApplied, "max_tokens") }, "is not a pilot setting"},
		{"not_applied repeated", func(c *Config) {
			c.Pilot.Targets[1].NotApplied = append(c.Pilot.Targets[1].NotApplied, "reasoning_format")
		}, "listed twice"},
		{"not_applied but set", func(c *Config) { c.Pilot.Targets[1].Params.ReasoningFormat = "parsed" }, "is set in this target's params"},
		{"not_applied with nothing to excuse", func(c *Config) { c.Pilot.Targets[0].NotApplied = []string{"temperature"} }, "pilot.targets[0].not_applied[0]: \"temperature\" is not set on any other target"},
	} {
		c := loadRef(t, pilotRef)
		tc.mutate(c)
		err := c.Validate()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: must validate: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

func TestPilotBodyTemplate(t *testing.T) {
	const sys = `{"role": "system", "content": "Route the ticket."}`
	const user = `{"role": "user", "content": "{{case}}"}`
	for _, tc := range []struct {
		name, body, want string // want "" = valid
	}{
		{"minimal", `{"messages":[` + sys + `,` + user + `],"stream":true}`, ""},
		{"stream first, whitespace", "\n {\"stream\": true,\n \"messages\": [" + sys + ", " + user + "]}\n", ""},
		{"not json", `{"messages":`, "not valid JSON"},
		{"array", `[` + sys + `]`, "not a JSON object"},
		{"model in body", `{"model":"x","messages":[` + sys + `,` + user + `],"stream":true}`, "keys must be exactly"},
		{"ceiling in body", `{"max_tokens":4096,"messages":[` + sys + `,` + user + `],"stream":true}`, "keys must be exactly"},
		{"no stream", `{"messages":[` + sys + `,` + user + `]}`, "keys must be exactly"},
		{"stream false", `{"messages":[` + sys + `,` + user + `],"stream":false}`, "\"stream\" must be true"},
		{"stream repeated", `{"messages":[` + sys + `,` + user + `],"stream":true,"stream":false}`, "key \"stream\" repeated"},
		{"nested key repeated", `{"messages":[{"role":"system","content":"a","content":"b"},` + user + `],"stream":true}`, "key \"content\" repeated"},
		{"user only", `{"messages":[` + user + `],"stream":true}`, "one system message, then one user message"},
		{"user first", `{"messages":[` + user + `,` + sys + `],"stream":true}`, "messages[0].role"},
		{"empty system text", `{"messages":[{"role":"system","content":" "},` + user + `],"stream":true}`, "system text is empty"},
		{"extra message key", `{"messages":[` + sys + `,{"role":"user","content":"{{case}}","name":"x"}],"stream":true}`, "messages[1]: keys"},
		{"user text literal", `{"messages":[` + sys + `,{"role":"user","content":"a ticket"}],"stream":true}`, "messages[1].content: must be"},
		{"slot escaped", `{"messages":[` + sys + `,{"role":"user","content":"\u007b{case}}"}],"stream":true}`, "messages[1].content: must be"},
		{"slot twice", `{"messages":[{"role":"system","content":"{{case}}"},` + user + `],"stream":true}`, "must appear once, got 2"},
	} {
		c := loadRef(t, pilotRef)
		c.Pilot.Body = tc.body
		err := c.Validate()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: must validate: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), "pilot.body: ") || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: must fail on pilot.body naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

func TestPilotRequestBody(t *testing.T) {
	c := loadRef(t, pilotRef)
	ticket := "Refund <order 7> & \"urgent\"\nthanks"
	want := map[string]string{
		"groq": `{"model":"openai/gpt-oss-120b","max_completion_tokens":4096,"reasoning_effort":"low","reasoning_format":"parsed","stream_options":{"include_usage":true},"messages": [
  {"role": "system", "content": "System text: frozen at L1."},
  {"role": "user", "content": "Refund <order 7> & \"urgent\"\nthanks"}
], "stream": true}`,
		"baseten": `{"model":"openai/gpt-oss-120b","max_tokens":4096,"reasoning_effort":"low","stream_options":{"include_usage":true},"messages": [
  {"role": "system", "content": "System text: frozen at L1."},
  {"role": "user", "content": "Refund <order 7> & \"urgent\"\nthanks"}
], "stream": true}`,
	}
	for i := range c.Pilot.Targets {
		tg := &c.Pilot.Targets[i]
		if got := string(c.Pilot.RequestBody(tg, ticket)); got != want[tg.Name] {
			t.Errorf("%s body:\n%s\nwant:\n%s", tg.Name, got, want[tg.Name])
		}
	}

	half := 0.5
	tg := c.Pilot.Targets[1]
	tg.Params = &Params{MaxTokens: 4096, Temperature: &half}
	if got, want := string(c.Pilot.RequestBody(&tg, "{{case}}")), `{"model":"openai/gpt-oss-120b","max_tokens":4096,"temperature":0.5,"messages": [`; !strings.HasPrefix(got, want) || !strings.Contains(got, `"content": "{{case}}"`) {
		t.Errorf("body %s: want prefix %s and the case text in place", got, want)
	}
}

func TestPilotFieldsRefusedOutsidePilot(t *testing.T) {
	p := loadRef(t, pilotRef)
	for name, mutate := range map[string]func(*Config){
		"pilot section": func(c *Config) { c.Pilot = p.Pilot },
		"endpoint_url":  func(c *Config) { c.Target.EndpointURL = "http://127.0.0.1:18000/v1/chat/completions" },
		"params":        func(c *Config) { c.Target.Params = &Params{MaxTokens: 256} },
		"name":          func(c *Config) { c.Target.Name = "mock" },
		"not_applied":   func(c *Config) { c.Target.NotApplied = []string{"temperature"} },
		"probe":         func(c *Config) { c.Target.Probe = &TargetProbe{AccountLimitStatuses: []int{402}} },
	} {
		c := loadRef(t, acRef)
		mutate(c)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "pilot") {
			t.Errorf("%s under the ac profile must be refused, got: %v", name, err)
		}
	}
}

func TestPilotTargetProbe(t *testing.T) {
	limits := "https://api.example.test/v1/model_apis/gpt-oss-120b"
	for _, tc := range []struct {
		name  string
		probe TargetProbe
		want  string // "" = valid
	}{
		{"empty", TargetProbe{}, ""},
		{"limits read", TargetProbe{LimitsURL: limits, LimitsAuthScheme: "Api-Key"}, ""},
		{"account-limit statuses", TargetProbe{AccountLimitStatuses: []int{402, 429, 529}}, ""},
		{"scheme without url", TargetProbe{LimitsAuthScheme: "Bearer"}, "pilot.targets[1].probe.limits_auth_scheme: set without limits_url"},
		{"url without scheme", TargetProbe{LimitsURL: limits}, "pilot.targets[1].probe.limits_auth_scheme: one or more"},
		{"scheme with a space", TargetProbe{LimitsURL: limits, LimitsAuthScheme: "Api Key"}, "pilot.targets[1].probe.limits_auth_scheme: one or more"},
		{"url with query", TargetProbe{LimitsURL: limits + "?key=x", LimitsAuthScheme: "Bearer"}, "pilot.targets[1].probe.limits_url: no userinfo, query"},
		{"url with userinfo", TargetProbe{LimitsURL: "https://u:p@api.example.test/v1/model_apis/x", LimitsAuthScheme: "Bearer"}, "pilot.targets[1].probe.limits_url: no userinfo"},
		{"url without path", TargetProbe{LimitsURL: "https://api.example.test", LimitsAuthScheme: "Bearer"}, "pilot.targets[1].probe.limits_url: the complete request path"},
		{"status 399", TargetProbe{AccountLimitStatuses: []int{399}}, "pilot.targets[1].probe.account_limit_statuses[0]: 399 is not a 4xx or 5xx status"},
		{"status 600", TargetProbe{AccountLimitStatuses: []int{402, 600}}, "pilot.targets[1].probe.account_limit_statuses[1]: 600 is not"},
		{"setup-defect status", TargetProbe{AccountLimitStatuses: []int{401}}, "pilot.targets[1].probe.account_limit_statuses[0]: 401 already blocks the start"},
		{"repeated status", TargetProbe{AccountLimitStatuses: []int{402, 402}}, "pilot.targets[1].probe.account_limit_statuses[1]: 402 listed twice"},
		{"limits retained", TargetProbe{LimitsURL: limits, LimitsAuthScheme: "Bearer", LimitsRetained: true}, "pilot.targets[1].probe.limits_retained: set on the release profile's target only"},
		{"limits retained without url", TargetProbe{LimitsRetained: true}, "pilot.targets[1].probe.limits_retained: set without limits_url"},
	} {
		c := loadRef(t, pilotRef)
		p := tc.probe
		c.Pilot.Targets[1].Probe = &p
		err := c.Validate()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: must load, got: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}

	raw, err := os.ReadFile(pilotRef)
	if err != nil {
		t.Fatal(err)
	}
	line := "      not_applied: [reasoning_format]\n"
	edited := strings.Replace(string(raw), line, line+"      probe:\n        limits_url: "+limits+"\n        limits_auth_scheme: Api-Key\n        account_limit_statuses: [402]\n", 1)
	if edited == string(raw) {
		t.Fatal("reference no longer carries the not_applied line this test edits")
	}
	c, err := Parse([]byte(edited))
	if err != nil {
		t.Fatal(err)
	}
	if p := c.Pilot.Targets[1].Probe; p == nil || p.LimitsURL != limits || p.LimitsAuthScheme != "Api-Key" || len(p.AccountLimitStatuses) != 1 || p.AccountLimitStatuses[0] != 402 {
		t.Errorf("parsed probe %+v", p)
	}
}
