package config

import (
	"os"
	"slices"
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
	if want := []ModelHost{{Route: "baseten", Model: "openai/gpt-oss-120b"}}; !slices.Equal(c.Release.OtherHosts, want) {
		t.Errorf("other hosts %+v", c.Release.OtherHosts)
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
	doc := func(c *Config) *StatusDoc {
		c.Target.StatusDocs = []StatusDoc{{Status: 529, Meaning: "overloaded", Source: "errors.png", Retrieved: "2026-10-07"}}
		return &c.Target.StatusDocs[0]
	}
	tpm := func(c *Config) *Limit { return &c.Target.Limits[1] }
	edoc := func(c *Config) *ErrorDoc {
		c.Target.ErrorDocs = []ErrorDoc{{Type: "overloaded_error", Status: 529, Meaning: "overloaded", Source: "errors.png", Retrieved: "2026-10-07"}}
		return &c.Target.ErrorDocs[0]
	}
	unpub := func(c *Config) *UnpublishedLimit {
		c.Target.UnpublishedLimits = []UnpublishedLimit{{Dimension: "acceleration", Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64), Ruled: "2026-10-08", Ruling: "rulings.md"}}
		return &c.Target.UnpublishedLimits[0]
	}
	bal := func(c *Config) *BalanceUnavailable {
		c.Target.Limits = append(c.Target.Limits, Limit{Dimension: LimitSpendMonth, Value: 10, OperatorSet: true, Marking: MarkingPublic, Source: "spend-limit.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64)})
		c.Target.BalanceUnavailable = &BalanceUnavailable{Kind: BalancePostpaid, Reason: "postpaid account", Ruled: "2026-10-07", Ruling: "rulings.md"}
		return c.Target.BalanceUnavailable
	}
	bill := func(c *Config) *BillingRefusal {
		c.Target.BillingRefusals = []BillingRefusal{{Status: 400, BodyContains: "blocked_api_access", Meaning: "spend limit reached", Source: "errors.png", Retrieved: "2026-10-07"}}
		return &c.Target.BillingRefusals[0]
	}
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
		{"no caps", func(c *Config) { c.Release.Caps = LedgerCaps{} }, "release.caps.route_usd: a positive decimal number of US dollars required"},
		{"no global cap", func(c *Config) { c.Release.Caps.GlobalUSD = "" }, "release.caps.global_usd: a positive decimal number"},
		{"zero route cap", func(c *Config) { c.Release.Caps.RouteUSD = "0" }, "release.caps.route_usd"},
		{"cap in exponent form", func(c *Config) { c.Release.Caps.RouteUSD = "1e1" }, "release.caps.route_usd"},
		{"cap with a currency sign", func(c *Config) { c.Release.Caps.GlobalUSD = "$1300" }, "release.caps.global_usd"},
		{"route cap over global", func(c *Config) { c.Release.Caps.RouteUSD = "1300.01" }, "release.caps.route_usd: 1300.01 exceeds global_usd 1300"},
		{"other host is the route", func(c *Config) { c.Release.OtherHosts[0].Route = "groq" }, `release.other_hosts[0].route: "groq" is target.name or listed twice`},
		{"other host twice", func(c *Config) { c.Release.OtherHosts = append(c.Release.OtherHosts, c.Release.OtherHosts[0]) }, `release.other_hosts[1].route: "baseten" is target.name or listed twice`},
		{"other host name", func(c *Config) { c.Release.OtherHosts[0].Route = "base ten" }, "release.other_hosts[0].route: one or more of"},
		{"other host model", func(c *Config) { c.Release.OtherHosts[0].Model = " " }, "release.other_hosts[0].model: required"},
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
		{"params", func(c *Config) { c.Target.Params = &Params{MaxTokens: 512} }, "target.params: set on pilot.targets entries only"},
		{"not_applied sampling control", func(c *Config) { c.Target.NotApplied = []string{"temperature"} }, `target.not_applied[0]: "temperature" is not one of the "chat_completions" adapter's parameters`},
		{"not_applied ceiling key", func(c *Config) { c.Target.NotApplied = []string{"max_completion_tokens"} }, `target.not_applied[0]: "max_completion_tokens" is not one of`},
		{"not_applied of the other adapter", func(c *Config) { c.Target.NotApplied = []string{"thinking"} }, `target.not_applied[0]: "thinking" is not one of the "chat_completions" adapter's parameters`},
		{"not_applied sent", func(c *Config) { c.Target.NotApplied = []string{"reasoning_format"} }, `target.not_applied[0]: "reasoning_format" is sent in target.body`},
		{"not_applied listed twice", func(c *Config) {
			c.Target.Body = strings.Replace(c.Target.Body, `, "reasoning_format": "parsed"`, "", 1)
			c.Target.NotApplied = []string{"reasoning_format", "reasoning_format"}
		}, `target.not_applied[1]: "reasoning_format" listed twice`},
		{"no adapter", func(c *Config) { c.Target.Adapter = "" }, "target.adapter: must be one of"},
		{"unknown adapter", func(c *Config) { c.Target.Adapter = "responses" }, "target.adapter: must be one of"},
		{"response shape on messages", func(c *Config) {
			c.Target.Adapter = AdapterMessages
			c.Target.Body = strings.Replace(c.Target.Body, `"max_completion_tokens"`, `"max_tokens"`, 1)
			c.Target.ResponseShape = &ResponseShape{Reasoning: []string{"thinking"}}
		}, "target.response_shape: must be absent for the \"messages\" adapter"},
		{"no body", func(c *Config) { c.Target.Body = "" }, "target.body: required under the \"release\" profile"},
		{"body without slot", func(c *Config) { c.Target.Body = `{"stream":true,"max_completion_tokens":512}` }, "target.body: \"{{case}}\" must appear once"},
		{"body ceiling off the shape", func(c *Config) { c.Release.Shape.CeilingTokens = 1024 }, "\"max_completion_tokens\" must be release.shape.ceiling_tokens (1024), got 512"},
		{"probe status", func(c *Config) { c.Target.Probe = &TargetProbe{AccountLimitStatuses: []int{401}} }, "target.probe.account_limit_statuses[0]"},
		{"credential header recorded", func(c *Config) { c.Target.ResponseHeaders = []string{"Authorization"} }, "never recorded"},
		{"no response headers", func(c *Config) { c.Target.ResponseHeaders = nil }, "target.response_headers: must record Retry-After"},
		{"Retry-After unrecorded", func(c *Config) { c.Target.ResponseHeaders = []string{"x-ratelimit-*"} }, "target.response_headers: must record Retry-After"},
		{"no limits", func(c *Config) { c.Target.Limits = nil }, "target.limits: required"},
		{"limit dimension", func(c *Config) { limit(c).Dimension = "rpm" }, "target.limits[0].dimension: must be one of"},
		{"limit value", func(c *Config) { limit(c).Value = 0 }, "target.limits[0].value: must be > 0"},
		{"limit with no marking", func(c *Config) { limit(c).Marking = "" }, "target.limits[0].marking: must be one of [\"public\" \"confidential\"], got \"\""},
		{"limit marking", func(c *Config) { limit(c).Marking = "private" }, "target.limits[0].marking: must be one of"},
		{"limit window off a window dimension", func(c *Config) { limit(c).WindowS = 600 }, "target.limits[0].window_s: set for \"spend_per_window_usd\" only"},
		{"spend window without window", func(c *Config) { limit(c).Dimension = LimitSpendWindow }, "target.limits[0].window_s: must be > 0"},
		{"token limit without counting", func(c *Config) { tpm(c).Counting = "" }, "target.limits[1].counting: must be one of"},
		{"unknown counting", func(c *Config) { tpm(c).Counting = "actual" }, "target.limits[1].counting: must be one of"},
		{"counting on a request limit", func(c *Config) { limit(c).Counting = CountingInput }, "target.limits[0].counting: set for \"tokens_per_minute\" and \"tokens_per_day\" only"},
		{"burndown without a factor", func(c *Config) { tpm(c).Counting = CountingOutputBurndown }, "target.limits[1].output_burndown: must be >= 1"},
		{"burndown below one", func(c *Config) { tpm(c).Counting, tpm(c).OutputBurndown = CountingOutputBurndown, 0.5 }, "target.limits[1].output_burndown: must be >= 1"},
		{"factor without burndown", func(c *Config) { tpm(c).OutputBurndown = 15 }, "target.limits[1].output_burndown: set for counting \"output_burndown\" only"},
		{"limit source", func(c *Config) { limit(c).Source = " " }, "target.limits[0].source: required"},
		{"limit source scheme", func(c *Config) { limit(c).Source = "ftp://example.com/limits" }, "target.limits[0].source: \"ftp://example.com/limits\" is not an http"},
		{"limit date", func(c *Config) { limit(c).Retrieved = "7 October 2026" }, "target.limits[0].retrieved: a date as YYYY-MM-DD"},
		{"limit no date", func(c *Config) { limit(c).Retrieved = "" }, "target.limits[0].retrieved"},
		{"limit hash", func(c *Config) { limit(c).SHA256 = strings.Repeat("A", 64) }, "target.limits[0].sha256: 64 lowercase hex"},
		{"limit short hash", func(c *Config) { limit(c).SHA256 = "abc" }, "target.limits[0].sha256"},
		{"unpublished dimension unset", func(c *Config) { unpub(c).Dimension = "" }, "target.unpublished_limits[0].dimension: one or more of a-z, 0-9 and _, starting with a letter, required, got \"\""},
		{"unpublished dimension spaced", func(c *Config) { unpub(c).Dimension = "acceleration limit" }, "target.unpublished_limits[0].dimension: one or more"},
		{"unpublished dimension upper case", func(c *Config) { unpub(c).Dimension = "Acceleration" }, "target.unpublished_limits[0].dimension: one or more"},
		{"unpublished dimension with a value", func(c *Config) { unpub(c).Dimension = LimitTPM }, "target.unpublished_limits[0].dimension: \"tokens_per_minute\" is recorded with a value or listed twice"},
		{"unpublished dimension listed twice", func(c *Config) {
			unpub(c)
			c.Target.UnpublishedLimits = append(c.Target.UnpublishedLimits, c.Target.UnpublishedLimits[0])
		}, "target.unpublished_limits[1].dimension: \"acceleration\" is recorded with a value or listed twice"},
		{"unpublished source", func(c *Config) { unpub(c).Source = " " }, "target.unpublished_limits[0].source: required"},
		{"unpublished source scheme", func(c *Config) { unpub(c).Source = "ftp://example.com/limits" }, "target.unpublished_limits[0].source: \"ftp://example.com/limits\" is not an http"},
		{"unpublished retrieved", func(c *Config) { unpub(c).Retrieved = "7 October 2026" }, "target.unpublished_limits[0].retrieved: a date as YYYY-MM-DD"},
		{"unpublished hash", func(c *Config) { unpub(c).SHA256 = "abc" }, "target.unpublished_limits[0].sha256: 64 lowercase hex"},
		{"unpublished ruling undated", func(c *Config) { unpub(c).Ruled = "" }, "target.unpublished_limits[0].ruled: a date as YYYY-MM-DD required, got \"\""},
		{"unpublished ruling date", func(c *Config) { unpub(c).Ruled = "8 October 2026" }, "target.unpublished_limits[0].ruled: a date as YYYY-MM-DD"},
		{"unpublished ruling before retrieval", func(c *Config) { unpub(c).Ruled = "2026-10-06" }, "target.unpublished_limits[0].ruled: 2026-10-06 is before retrieved 2026-10-07"},
		{"unpublished ruling unrecorded", func(c *Config) { unpub(c).Ruling = " " }, "target.unpublished_limits[0].ruling: required"},
		{"retained header name", func(c *Config) { c.Target.RetainedHeaders = []string{"X Ratelimit"} }, `target.retained_headers[0]: "X Ratelimit" is not a header name, optionally ending in *`},
		{"retained header listed twice", func(c *Config) {
			c.Target.RetainedHeaders = []string{"x-ratelimit-limit-requests", "X-Ratelimit-Limit-Requests"}
		}, `target.retained_headers[1]: "X-Ratelimit-Limit-Requests" listed twice`},
		{"retained header not recorded", func(c *Config) { c.Target.RetainedHeaders = []string{"x-request-id"} }, `target.retained_headers[0]: "x-request-id" matches names target.response_headers does not record`},
		{"retained pattern wider than recorded", func(c *Config) { c.Target.RetainedHeaders = []string{"x-rate*"} }, `target.retained_headers[0]: "x-rate*" matches names target.response_headers does not record`},
		{"retained pattern over a literal", func(c *Config) {
			c.Target.ResponseHeaders = []string{"Retry-After", "x-ratelimit-limit-requests"}
			c.Target.RetainedHeaders = []string{"x-ratelimit-limit-*"}
		}, `target.retained_headers[0]: "x-ratelimit-limit-*" matches names target.response_headers does not record`},
		{"retained Retry-After", func(c *Config) { c.Target.RetainedHeaders = []string{"retry-after"} }, `target.retained_headers[0]: "retry-after" matches Retry-After, whose values the per-step tables print`},
		{"retained pattern over Retry-After", func(c *Config) {
			c.Target.ResponseHeaders = []string{"Retry-*", "x-ratelimit-*"}
			c.Target.RetainedHeaders = []string{"x-ratelimit-*", "retry-*"}
		}, `target.retained_headers[1]: "retry-*" matches Retry-After, whose values the per-step tables print`},
		{"no naming", func(c *Config) { c.Target.Naming = nil }, "target.naming: required"},
		{"naming provider", func(c *Config) { c.Target.Naming.Provider = "" }, "target.naming.provider: required"},
		{"naming lines", func(c *Config) { c.Target.Naming.Lines = nil }, "target.naming.lines: required"},
		{"naming empty line", func(c *Config) { c.Target.Naming.Lines[1] = " " }, "target.naming.lines[1]: empty"},
		{"naming repeated line", func(c *Config) { c.Target.Naming.Lines[1] = c.Target.Naming.Lines[0] }, "target.naming.lines[1]: listed twice"},
		{"permission unrecorded", func(c *Config) { c.Target.Naming.PermissionRequired = true }, "target.naming.permission_record: required"},
		{"status 200 documented", func(c *Config) { doc(c).Status = 200 }, "target.status_docs[0].status: an HTTP status from 100 to 599 other than 200 required, got 200"},
		{"status 600 documented", func(c *Config) { doc(c).Status = 600 }, "target.status_docs[0].status: an HTTP status"},
		{"status unset", func(c *Config) { doc(c).Status = 0 }, "target.status_docs[0].status: an HTTP status"},
		{"status documented twice", func(c *Config) {
			doc(c)
			c.Target.StatusDocs = append(c.Target.StatusDocs, c.Target.StatusDocs[0])
		}, "target.status_docs[1].status: 529 documented twice"},
		{"status meaning", func(c *Config) { doc(c).Meaning = " " }, "target.status_docs[0].meaning: required"},
		{"status source", func(c *Config) { doc(c).Source = "" }, "target.status_docs[0].source: required"},
		{"status source scheme", func(c *Config) { doc(c).Source = "ftp://example.com/errors" }, "target.status_docs[0].source: \"ftp://example.com/errors\" is not an http"},
		{"status date", func(c *Config) { doc(c).Retrieved = "7 October 2026" }, "target.status_docs[0].retrieved: a date as YYYY-MM-DD"},
		{"error type unset", func(c *Config) { edoc(c).Type = "" }, "target.error_docs[0].type: an error type with no surrounding space required, got \"\""},
		{"error type spaced", func(c *Config) { edoc(c).Type = " overloaded_error" }, "target.error_docs[0].type: an error type"},
		{"error type documented twice", func(c *Config) {
			edoc(c)
			c.Target.ErrorDocs = append(c.Target.ErrorDocs, c.Target.ErrorDocs[0])
		}, "target.error_docs[1].type: \"overloaded_error\" documented twice"},
		{"error status 200", func(c *Config) { edoc(c).Status = 200 }, "target.error_docs[0].status: 0 or an HTTP status from 100 to 599 other than 200 required, got 200"},
		{"error status 99", func(c *Config) { edoc(c).Status = 99 }, "target.error_docs[0].status: 0 or an HTTP status"},
		{"error meaning", func(c *Config) { edoc(c).Meaning = "" }, "target.error_docs[0].meaning: required"},
		{"error source", func(c *Config) { edoc(c).Source = " " }, "target.error_docs[0].source: required"},
		{"error source scheme", func(c *Config) { edoc(c).Source = "ftp://example.com/errors" }, "target.error_docs[0].source: \"ftp://example.com/errors\" is not an http"},
		{"error date", func(c *Config) { edoc(c).Retrieved = "2026-13-01" }, "target.error_docs[0].retrieved: a date as YYYY-MM-DD"},
		{"billing status 200", func(c *Config) { bill(c).Status = 200 }, "target.billing_refusals[0].status: a 4xx or 5xx status required, got 200"},
		{"billing status 600", func(c *Config) { bill(c).Status = 600 }, "target.billing_refusals[0].status: a 4xx or 5xx status required"},
		{"billing body spaced", func(c *Config) { bill(c).BodyContains = "blocked_api_access " }, "target.billing_refusals[0].body_contains: surrounding space"},
		{"billing recorded twice", func(c *Config) {
			b := bill(c)
			c.Target.BillingRefusals = append(c.Target.BillingRefusals, *b)
			c.Target.BillingRefusals[1].Meaning = "other words"
		}, "target.billing_refusals[1]: status 400 with body \"blocked_api_access\" recorded twice"},
		{"billing meaning", func(c *Config) { bill(c).Meaning = "" }, "target.billing_refusals[0].meaning: required"},
		{"billing source", func(c *Config) { bill(c).Source = "" }, "target.billing_refusals[0].source: required"},
		{"billing date", func(c *Config) { bill(c).Retrieved = "2026-10" }, "target.billing_refusals[0].retrieved: a date as YYYY-MM-DD"},
		{"balance kind", func(c *Config) { bal(c).Kind = "console_only" }, "target.balance_unavailable.kind: must be one of [\"prepaid\" \"postpaid\"], got \"console_only\""},
		{"balance postpaid uncovered", func(c *Config) {
			bal(c)
			c.Target.Limits = c.Target.Limits[:len(c.Target.Limits)-1]
		}, "target.balance_unavailable.kind: postpaid needs target.limits to record spend_per_month_usd"},
		{"balance prepaid uncovered", func(c *Config) { bal(c).Kind = BalancePrepaid }, "target.balance_unavailable.kind: prepaid needs target.limits to record auto_reload_threshold_usd"},
		{"balance reason", func(c *Config) { bal(c).Reason = " " }, "target.balance_unavailable.reason: required"},
		{"balance ruled", func(c *Config) { bal(c).Ruled = "7 October 2026" }, "target.balance_unavailable.ruled: a date as YYYY-MM-DD required, got \"7 October 2026\""},
		{"balance ruling", func(c *Config) { bal(c).Ruling = "" }, "target.balance_unavailable.ruling: required"},
	} {
		c := loadRef(t, releaseRef)
		tc.mutate(c)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

// Operator-set limits, rolling spend windows, token countings, URL
// sources, recorded permission, documented statuses, documented in-band
// error types, with and without a status, and billing refusals, with and
// without a body string, load.
func TestReleaseLimitAndNamingForms(t *testing.T) {
	c := loadRef(t, releaseRef)
	c.Target.Limits = append(c.Target.Limits,
		Limit{Dimension: LimitSpendWindow, Value: 10, WindowS: 600, Marking: MarkingPublic, Source: "https://ai.google.dev/gemini-api/docs/rate-limits", Retrieved: "2026-10-07", SHA256: strings.Repeat("ab", 32)},
		Limit{Dimension: LimitSpendMonth, Value: 10, OperatorSet: true, Marking: MarkingPublic, Source: "spend-limit.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64)},
		Limit{Dimension: LimitTPD, Value: 7e8, Marking: MarkingPublic, Counting: CountingOutputBurndown, OutputBurndown: 15, Source: "quotas.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64)},
		Limit{Dimension: LimitTPM, Value: 1e6, Marking: MarkingConfidential, Counting: CountingInput, Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64)},
	)
	c.Target.RetainedHeaders = []string{"x-ratelimit-*"}
	c.Target.Naming.PermissionRequired, c.Target.Naming.PermissionRecord = true, "manifest: written permission"
	c.Target.StatusDocs = []StatusDoc{
		{Status: 529, Meaning: "overloaded", Source: "https://example.test/errors", Retrieved: "2026-10-07"},
		{Status: 503, Meaning: "service unavailable", Source: "errors.png", Retrieved: "2026-10-07"},
	}
	c.Target.ErrorDocs = []ErrorDoc{
		{Type: "overloaded_error", Status: 529, Meaning: "overloaded", Source: "https://example.test/errors", Retrieved: "2026-10-07"},
		{Type: "server_error", Meaning: "server error", Source: "errors.png", Retrieved: "2026-10-07"},
	}
	c.Target.BillingRefusals = []BillingRefusal{
		{Status: 400, BodyContains: "blocked_api_access", Meaning: "spend limit reached", Source: "https://example.test/errors", Retrieved: "2026-10-07"},
		{Status: 400, BodyContains: "usage limits", Meaning: "spend limit reached", Source: "errors.png", Retrieved: "2026-10-07"},
		{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"},
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

// target.probe.limits_retained loads from YAML on a release target with
// a limits read, and is refused without one.
func TestReleaseLimitsRetained(t *testing.T) {
	raw, err := os.ReadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	line := "  response_headers: [Retry-After, x-ratelimit-*]\n"
	read := "    limits_url: https://api.groq.com/openai/v1/models\n    limits_auth_scheme: Bearer\n"
	for _, tc := range []struct {
		name, probe, want string
	}{
		{"with a limits read", read + "    limits_retained: true\n", ""},
		{"without a limits read", "    limits_retained: true\n", "target.probe.limits_retained: set without limits_url"},
	} {
		edited := strings.Replace(string(raw), line, line+"  probe:\n"+tc.probe, 1)
		if edited == string(raw) {
			t.Fatal("reference no longer carries the response_headers line this test edits")
		}
		c, err := Parse([]byte(edited))
		switch {
		case tc.want == "" && (err != nil || c.Target.Probe == nil || !c.Target.Probe.LimitsRetained):
			t.Errorf("%s: must load with limits_retained set, got %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

// withBalanceRuling is the release reference with a limit of dimension
// added and a balance_unavailable ruling of kind, its kind line left out
// where kind is empty.
func withBalanceRuling(t *testing.T, kind, dimension string) string {
	t.Helper()
	raw, err := os.ReadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	const limits, naming = "  limits:\n", "  naming:\n"
	if strings.Count(string(raw), limits) != 1 || strings.Count(string(raw), naming) != 1 {
		t.Fatal("reference no longer carries the limits and naming lines this test edits")
	}
	limit := "    - dimension: " + dimension + "\n      value: 10\n      marking: public\n      operator_set: true\n" +
		"      source: spend-limit.png\n      retrieved: 2026-10-07\n      sha256: \"" + strings.Repeat("0", 64) + "\"\n"
	ruling := "  balance_unavailable:\n"
	if kind != "" {
		ruling += "    kind: " + kind + "\n"
	}
	ruling += "    reason: shown in the console only\n    ruled: 2026-10-07\n    ruling: rulings.md\n"
	edited := strings.Replace(string(raw), limits, limits+limit, 1)
	return strings.Replace(edited, naming, ruling+naming, 1)
}

// A balance_unavailable ruling loads from YAML beside the limit its kind
// names, and is refused beside the other kind's limit or without a kind.
func TestReleaseBalanceRuling(t *testing.T) {
	for _, tc := range []struct {
		name, kind, dimension, want string
	}{
		{"postpaid with a monthly spend limit", BalancePostpaid, LimitSpendMonth, ""},
		{"prepaid with an auto-reload threshold", BalancePrepaid, LimitReloadThreshold, ""},
		{"postpaid with an auto-reload threshold", BalancePostpaid, LimitReloadThreshold, "target.balance_unavailable.kind: postpaid needs target.limits to record spend_per_month_usd"},
		{"prepaid with a monthly spend limit", BalancePrepaid, LimitSpendMonth, "target.balance_unavailable.kind: prepaid needs target.limits to record auto_reload_threshold_usd"},
		{"no kind", "", LimitSpendMonth, "target.balance_unavailable.kind: must be one of [\"prepaid\" \"postpaid\"], got \"\""},
	} {
		c, err := Parse([]byte(withBalanceRuling(t, tc.kind, tc.dimension)))
		switch {
		case tc.want == "" && (err != nil || c.Target.BalanceUnavailable == nil || c.Target.BalanceUnavailable.Kind != tc.kind):
			t.Errorf("%s: must load with kind %q, got %v", tc.name, tc.kind, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

// Unpublished limits load from YAML, with URL and file sources and a
// ruling on the retrieval date or later, beside a recorded limit of
// another dimension.
func TestReleaseUnpublishedLimits(t *testing.T) {
	raw, err := os.ReadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	const entries = `  unpublished_limits:
    - dimension: acceleration
      source: https://example.test/rate-limits
      retrieved: 2026-10-07
      sha256: "abababababababababababababababababababababababababababababababab"
      ruled: 2026-10-07
      ruling: rulings/2026-10-07-acceleration.md
    - dimension: concurrent_requests
      source: limits.png
      retrieved: 2026-10-07
      sha256: "0000000000000000000000000000000000000000000000000000000000000000"
      ruled: 2026-10-09
      ruling: rulings/2026-10-09-concurrency.md
  naming:
`
	with := strings.Replace(string(raw), "  naming:\n", entries, 1)
	if with == string(raw) {
		t.Fatal("no naming block in the reference")
	}
	c, err := Parse([]byte(with))
	if err != nil {
		t.Fatal(err)
	}
	want := []UnpublishedLimit{
		{Dimension: "acceleration", Source: "https://example.test/rate-limits", Retrieved: "2026-10-07", SHA256: strings.Repeat("ab", 32), Ruled: "2026-10-07", Ruling: "rulings/2026-10-07-acceleration.md"},
		{Dimension: LimitConcurrency, Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64), Ruled: "2026-10-09", Ruling: "rulings/2026-10-09-concurrency.md"},
	}
	if !slices.Equal(c.Target.UnpublishedLimits, want) {
		t.Errorf("unpublished limits %+v", c.Target.UnpublishedLimits)
	}
	if _, err := Parse([]byte(strings.Replace(with, "      ruled: 2026-10-09\n", "      ruled: 2026-10-09\n      cap_rps: 1\n", 1))); err == nil || !strings.Contains(err.Error(), "cap_rps") {
		t.Errorf("an unknown field in an entry: got %v", err)
	}
}

// Retained headers load from YAML as literal names and prefixes inside
// the recorded allowlist, and match without regard to case.
func TestReleaseRetainedHeaders(t *testing.T) {
	raw, err := os.ReadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	with := strings.Replace(string(raw), "  naming:\n", "  retained_headers: [x-ratelimit-limit-*, X-Ratelimit-Remaining-Tokens]\n  naming:\n", 1)
	if with == string(raw) {
		t.Fatal("no naming block in the reference")
	}
	c, err := Parse([]byte(with))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"x-ratelimit-limit-*", "X-Ratelimit-Remaining-Tokens"}; !slices.Equal(c.Target.RetainedHeaders, want) {
		t.Errorf("retained headers %q, want %q", c.Target.RetainedHeaders, want)
	}
	for name, want := range map[string]bool{
		"X-Ratelimit-Limit-Requests":     true,
		"x-ratelimit-limit-tokens":       true,
		"X-Ratelimit-Remaining-Tokens":   true,
		"X-Ratelimit-Remaining-Requests": false,
		"X-Ratelimit-Limit":              false,
		"Retry-After":                    false,
	} {
		if got := c.Target.HeaderRetained(name); got != want {
			t.Errorf("HeaderRetained(%q) = %v, want %v", name, got, want)
		}
	}
}

// A target with a confidential limit retains each recorded header that
// can carry a rate limit, and the limits body where it reads one.
func TestReleaseConfidentialRetention(t *testing.T) {
	read := &TargetProbe{LimitsURL: "https://api.groq.com/openai/v1/models", LimitsAuthScheme: "Bearer"}
	kept := &TargetProbe{LimitsURL: read.LimitsURL, LimitsAuthScheme: read.LimitsAuthScheme, LimitsRetained: true}
	for _, tc := range []struct {
		name               string
		confidential       bool
		recorded, retained []string
		probe              *TargetProbe
		want               string
	}{
		{"public limits", false, []string{"Retry-After", "x-ratelimit-*"}, nil, read, ""},
		{"prefix retained", true, []string{"Retry-After", "x-ratelimit-*"}, []string{"x-ratelimit-*"}, nil, ""},
		{"name retained in another case", true, []string{"Retry-After", "x-ratelimit-limit-tokens", "x-request-id"}, []string{"X-RateLimit-Limit-Tokens"}, nil, ""},
		{"limits body retained", true, []string{"Retry-After"}, nil, kept, ""},
		{"prefix not retained", true, []string{"Retry-After", "x-ratelimit-*"}, nil, nil, `target.response_headers[1]: "x-ratelimit-*" can match a name containing "ratelimit"`},
		{"prefix retained in part", true, []string{"Retry-After", "x-ratelimit-*"}, []string{"x-ratelimit-limit-*"}, nil, `target.response_headers[1]: "x-ratelimit-*" can match`},
		{"name not retained", true, []string{"Retry-After", "anthropic-RateLimit-tokens-limit"}, nil, nil, `target.response_headers[1]: "anthropic-RateLimit-tokens-limit" can match`},
		{"another prefix", true, []string{"Retry-After", "x-request-*"}, nil, nil, `target.response_headers[1]: "x-request-*" can match`},
		{"limits body published", true, []string{"Retry-After"}, nil, read, "target.probe.limits_retained: required with limits_url on a target with a confidential limit"},
	} {
		c := loadRef(t, releaseRef)
		if tc.confidential {
			c.Target.Limits[1].Marking = MarkingConfidential
		}
		c.Target.ResponseHeaders, c.Target.RetainedHeaders, c.Target.Probe = tc.recorded, tc.retained, tc.probe
		err := c.Validate()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: must load, got: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

// Retry-After is recorded by a literal name or a prefix, in any case; a
// near miss does not record it.
func TestReleaseRetryAfterRecorded(t *testing.T) {
	for _, tc := range []struct {
		allow    []string
		recorded bool
	}{
		{[]string{"retry-after"}, true},
		{[]string{"RETRY-AFTER"}, true},
		{[]string{"x-ratelimit-*", "Retry-*"}, true},
		{[]string{"Retry-After-*"}, false},
		{[]string{"Retry"}, false},
		{[]string{"x-retry-after"}, false},
	} {
		c := loadRef(t, releaseRef)
		c.Target.ResponseHeaders = tc.allow
		err := c.Validate()
		refused := err != nil && strings.Contains(err.Error(), "target.response_headers: must record Retry-After")
		if refused == tc.recorded || (tc.recorded && err != nil) {
			t.Errorf("%q: recorded %v, got: %v", tc.allow, tc.recorded, err)
		}
	}
}

// A parameter the target rejects loads as not applied, from YAML for a
// chat-completions target and for each Messages parameter, when the body
// omits it.
func TestReleaseNotApplied(t *testing.T) {
	raw, err := os.ReadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	with := strings.Replace(string(raw), `, "reasoning_format": "parsed"}`, "}", 1)
	with = strings.Replace(with, "  naming:\n", "  not_applied: [reasoning_format]\n  naming:\n", 1)
	if strings.Contains(with, "reasoning_format\": ") || !strings.Contains(with, "not_applied: [reasoning_format]") {
		t.Fatal("the reference no longer carries the body line or the naming block this test edits")
	}
	c, err := Parse([]byte(with))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.Target.NotApplied, []string{"reasoning_format"}) {
		t.Errorf("not_applied %q", c.Target.NotApplied)
	}

	const messages = `{"model":"m","system":"s","messages":[{"role":"user","content":"{{case}}"}],"stream":true,"max_tokens":512%s}`
	for _, tc := range []struct{ body, notApplied string }{
		{`,"output_config":{"effort":"low"}`, "thinking"},
		{`,"thinking":{"type":"between_tools"}`, "output_config"},
	} {
		c := loadRef(t, releaseRef)
		c.Target.Adapter = AdapterMessages
		c.Target.Body = strings.Replace(messages, "%s", tc.body, 1)
		c.Target.NotApplied = []string{tc.notApplied}
		if err := c.Validate(); err != nil {
			t.Errorf("messages, %s not applied: %v", tc.notApplied, err)
		}
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

// The reference keeps model output under a Groq terms record; none
// stops the frame store keeping it; each part of the record is required
// under the release profile. A target with no retention keeps output.
func TestReleaseRetention(t *testing.T) {
	c := loadRef(t, releaseRef)
	r := c.Target.Retention
	if r == nil || r.ModelOutput != RetainProtocolPeriod || r.Source != "https://console.groq.com/docs/legal/services-agreement" || r.Retrieved != "2026-10-12" ||
		!strings.HasPrefix(r.Provision, "Groq Services Agreement, section 1:") || !strings.HasPrefix(r.Deletion, "None on the customer:") || !c.Target.KeepsModelOutput() {
		t.Fatalf("reference retention %+v", r)
	}
	c.Target.Retention.ModelOutput = RetainNone
	if err := c.Validate(); err != nil || c.Target.KeepsModelOutput() {
		t.Fatalf("none: %v, keeps %v", err, c.Target.KeepsModelOutput())
	}
	if !(&Target{}).KeepsModelOutput() {
		t.Error("a target with no retention keeps no output")
	}
	for _, tc := range []struct {
		name string
		edit func(*Target)
		want string
	}{
		{"absent", func(t *Target) { t.Retention = nil }, "target.retention: required under the release profile"},
		{"model_output empty", func(t *Target) { t.Retention.ModelOutput = "" }, `target.retention.model_output: must be one of ["protocol_period" "none"], got ""`},
		{"model_output unknown", func(t *Target) { t.Retention.ModelOutput = "12_months" }, `target.retention.model_output: must be one of ["protocol_period" "none"], got "12_months"`},
		{"no provision", func(t *Target) { t.Retention.Provision = " " }, "target.retention.provision: required"},
		{"no deletion", func(t *Target) { t.Retention.Deletion = "" }, "target.retention.deletion: required"},
		{"no source", func(t *Target) { t.Retention.Source = "" }, "target.retention.source: required"},
		{"bad source", func(t *Target) { t.Retention.Source = "https://" }, `target.retention.source: "https://" is not an http or https URL with a host`},
		{"bad date", func(t *Target) { t.Retention.Retrieved = "12 Oct 2026" }, `target.retention.retrieved: a date as YYYY-MM-DD required, got "12 Oct 2026"`},
		{"bad hash", func(t *Target) { t.Retention.SHA256 = strings.Repeat("A", 64) }, "target.retention.sha256: 64 lowercase hex characters required"},
	} {
		c := loadRef(t, releaseRef)
		tc.edit(&c.Target)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want %q, got %v", tc.name, tc.want, err)
		}
	}
	c = loadRef(t, releaseRef)
	c.Target.Retention.Source = "groq-services-agreement.html"
	if err := c.Validate(); err != nil {
		t.Errorf("retained copy as source: %v", err)
	}
}

// A recorded reported_model becomes the expected identifier; without one
// it is model_name. Each part of the record is required, and its name
// must differ from model_name.
func TestReleaseReportedModel(t *testing.T) {
	c := loadRef(t, releaseRef)
	if c.Target.ReportedModel != nil || c.Target.ExpectedModel() != c.Target.ModelName {
		t.Fatalf("reference: reported_model %+v, expected %q", c.Target.ReportedModel, c.Target.ExpectedModel())
	}
	c.Target.ReportedModel = &ReportedModel{Name: "m-2026-10-01", Source: "https://example.com/models", Retrieved: "2026-10-12", SHA256: strings.Repeat("0", 64)}
	if err := c.Validate(); err != nil || c.Target.ExpectedModel() != "m-2026-10-01" {
		t.Fatalf("recorded: %v, expected %q", err, c.Target.ExpectedModel())
	}
	for _, tc := range []struct {
		name string
		edit func(*ReportedModel)
		want string
	}{
		{"no name", func(r *ReportedModel) { r.Name = "" }, `target.reported_model.name: a model identifier with no surrounding space required, got ""`},
		{"spaced name", func(r *ReportedModel) { r.Name = " m" }, `target.reported_model.name: a model identifier with no surrounding space required, got " m"`},
		{"model_name", func(r *ReportedModel) { r.Name = "openai/gpt-oss-120b" }, `target.reported_model.name: "openai/gpt-oss-120b" is target.model_name`},
		{"no source", func(r *ReportedModel) { r.Source = " " }, "target.reported_model.source: required"},
		{"bad source", func(r *ReportedModel) { r.Source = "https://" }, `target.reported_model.source: "https://" is not an http or https URL with a host`},
		{"bad date", func(r *ReportedModel) { r.Retrieved = "12 Oct 2026" }, `target.reported_model.retrieved: a date as YYYY-MM-DD required, got "12 Oct 2026"`},
		{"bad hash", func(r *ReportedModel) { r.SHA256 = "" }, "target.reported_model.sha256: 64 lowercase hex characters required"},
	} {
		c := loadRef(t, releaseRef)
		c.Target.ReportedModel = &ReportedModel{Name: "m-2026-10-01", Source: "models.html", Retrieved: "2026-10-12", SHA256: strings.Repeat("0", 64)}
		tc.edit(c.Target.ReportedModel)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want %q, got %v", tc.name, tc.want, err)
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
		func(t *Target) {
			t.UnpublishedLimits = []UnpublishedLimit{{Dimension: "acceleration", Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64), Ruled: "2026-10-07", Ruling: "rulings.md"}}
		},
		func(t *Target) {
			t.ResponseHeaders = []string{"x-ratelimit-*"}
			t.RetainedHeaders = []string{"x-ratelimit-*"}
		},
		func(t *Target) { t.Naming = rel.Target.Naming },
		func(t *Target) {
			t.StatusDocs = []StatusDoc{{Status: 529, Meaning: "overloaded", Source: "errors.png", Retrieved: "2026-10-07"}}
		},
		func(t *Target) {
			t.ErrorDocs = []ErrorDoc{{Type: "overloaded_error", Meaning: "overloaded", Source: "errors.png", Retrieved: "2026-10-07"}}
		},
		func(t *Target) {
			t.BillingRefusals = []BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
		},
		func(t *Target) {
			t.BalanceUnavailable = &BalanceUnavailable{Kind: BalancePostpaid, Reason: "postpaid account", Ruled: "2026-10-07", Ruling: "rulings.md"}
		},
		func(t *Target) { t.Retention = rel.Target.Retention },
		func(t *Target) {
			t.ReportedModel = &ReportedModel{Name: "m-2026-10-01", Source: "models.html", Retrieved: "2026-10-12", SHA256: strings.Repeat("0", 64)}
		},
	} {
		for _, ref := range []string{acRef, experimentRef} {
			c := loadRef(t, ref)
			f(&c.Target)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "target: adapter, limits, unpublished_limits, retained_headers, naming, status_docs, error_docs, billing_refusals, balance_unavailable, retention and reported_model are set on the release profile's target only") {
				t.Errorf("%s: got %v", ref, err)
			}
		}
		p := loadRef(t, pilotRef)
		f(&p.Pilot.Targets[1])
		if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "pilot.targets[1]: adapter, limits, unpublished_limits, retained_headers, naming, status_docs, error_docs, billing_refusals, balance_unavailable, retention and reported_model are set on the release profile's target only") {
			t.Errorf("pilot: got %v", err)
		}
	}
}

// A typo in a release field never silently drops the pin.
func TestReleaseUnknownFieldRejected(t *testing.T) {
	raw := withBalanceRuling(t, BalancePostpaid, LimitSpendMonth)
	for _, from := range []string{"    drain_s: 90", "    deadline_s: 30", "    route_usd: \"10\"", "      retrieved: 2026-10-07", "    ruled: 2026-10-07"} {
		bad := strings.Replace(raw, from, from+"\n"+strings.Repeat(" ", len(from)-len(strings.TrimLeft(from, " ")))+"drain_ss: 90", 1)
		if bad == raw {
			t.Fatalf("%q not in the reference", from)
		}
		if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "drain_ss") {
			t.Errorf("unknown field after %q: got %v", from, err)
		}
	}
}

// A billing refusal matches its status and, where it names one, a body
// carrying its string.
func TestBillingRefusalMatches(t *testing.T) {
	body := []byte(`{"error":{"message":"You have reached your specified API usage limits."}}`)
	for _, tc := range []struct {
		b      BillingRefusal
		status int
		want   bool
	}{
		{BillingRefusal{Status: 400, BodyContains: "specified API usage limits"}, 400, true},
		{BillingRefusal{Status: 400, BodyContains: "specified API usage limits"}, 429, false},
		{BillingRefusal{Status: 400, BodyContains: "blocked_api_access"}, 400, false},
		{BillingRefusal{Status: 402}, 402, true},
		{BillingRefusal{Status: 402}, 400, false},
	} {
		if got := tc.b.Matches(tc.status, body); got != tc.want {
			t.Errorf("%+v on %d: %v, want %v", tc.b, tc.status, got, tc.want)
		}
	}
}
