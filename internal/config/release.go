package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Release is the release profile's ladder, request shape (SPEC-HOSTED
// HA3 points 5 and 7) and ledger caps. One configuration file holds one
// route: its target is the top-level target.
type Release struct {
	Ladder Ladder     `yaml:"ladder" json:"ladder"`
	Shape  Shape      `yaml:"shape" json:"shape"`
	Caps   LedgerCaps `yaml:"caps" json:"caps"`
}

// LedgerCaps are the release ledger's liability caps in US dollars, as
// decimal numbers: this route's, and the global cap over every route
// that shares the ledger, which each route's file repeats.
type LedgerCaps struct {
	RouteUSD  string `yaml:"route_usd" json:"route_usd"`
	GlobalUSD string `yaml:"global_usd" json:"global_usd"`
}

// Ladder is the offered-load ladder: open-loop Poisson arrivals at each
// step in ascending order, a warm-up at the first step, then per step a
// measured window followed by a drain that dispatches nothing.
type Ladder struct {
	StepsRPS []float64 `yaml:"steps_rps" json:"steps_rps"`
	WarmupS  float64   `yaml:"warmup_s" json:"warmup_s"`
	StepS    float64   `yaml:"step_s" json:"step_s"`
	DrainS   float64   `yaml:"drain_s" json:"drain_s"`
}

// Shape is the request shape. The hard timeout is client.http_timeout_s;
// the deadline is a reporting cut inside completed.
type Shape struct {
	Name             string `yaml:"name" json:"name"`
	InputBoundTokens int    `yaml:"input_bound_tokens" json:"input_bound_tokens"`
	CeilingTokens    int    `yaml:"ceiling_tokens" json:"ceiling_tokens"`
	DeadlineS        int    `yaml:"deadline_s" json:"deadline_s"`
}

// Values of target.adapter.
const (
	// AdapterChatCompletions is the OpenAI chat-completions stream, ending
	// at "data: [DONE]".
	AdapterChatCompletions = "chat_completions"
	// AdapterMessages is the Anthropic Messages stream, ending at
	// message_stop.
	AdapterMessages = "messages"
)

var adapters = []string{AdapterChatCompletions, AdapterMessages}

// Limit is one recorded limit: a value the provider publishes, the
// account console or a limits API shows, or the operator set.
type Limit struct {
	// Dimension is one of the Limit* values.
	Dimension string  `yaml:"dimension" json:"dimension"`
	Value     float64 `yaml:"value" json:"value"`
	// WindowS is the rolling window of a LimitSpendWindow, and absent for
	// every other dimension.
	WindowS     float64 `yaml:"window_s,omitempty" json:"window_s,omitempty"`
	OperatorSet bool    `yaml:"operator_set,omitempty" json:"operator_set,omitempty"`
	// Counting is how the provider counts a request against a
	// tokens_per_minute or tokens_per_day limit, one of the Counting*
	// values, and absent for every other dimension. OutputBurndown is
	// the factor output tokens count at under CountingOutputBurndown.
	Counting       string  `yaml:"counting,omitempty" json:"counting,omitempty"`
	OutputBurndown float64 `yaml:"output_burndown,omitempty" json:"output_burndown,omitempty"`
	// Source is the page's URL or the retained screenshot's file name.
	Source string `yaml:"source" json:"source"`
	// Retrieved is the retrieval date, YYYY-MM-DD in UTC.
	Retrieved string `yaml:"retrieved" json:"retrieved"`
	// SHA256 is the hash of the retained copy, in lowercase hex.
	SHA256 string `yaml:"sha256" json:"sha256"`
}

// Values of a limit's dimension. Spend values are in US dollars.
const (
	LimitRPM             = "requests_per_minute"
	LimitRPS             = "requests_per_second"
	LimitTPM             = "tokens_per_minute"
	LimitInputTPM        = "input_tokens_per_minute"
	LimitOutputTPM       = "output_tokens_per_minute"
	LimitConcurrency     = "concurrent_requests"
	LimitRPD             = "requests_per_day"
	LimitTPD             = "tokens_per_day"
	LimitSpendWindow     = "spend_per_window_usd"
	LimitSpendMonth      = "spend_per_month_usd"
	LimitReloadThreshold = "auto_reload_threshold_usd"
)

var limitDimensions = []string{
	LimitRPM, LimitRPS, LimitTPM, LimitInputTPM, LimitOutputTPM, LimitConcurrency,
	LimitRPD, LimitTPD, LimitSpendWindow, LimitSpendMonth, LimitReloadThreshold,
}

// Values of a limit's counting. The ceiling rule takes a request at the
// shape's input bound and output ceiling (SPEC-HOSTED HA3 point 6).
const (
	// CountingInputOutput counts input plus output tokens, or input plus
	// max_tokens where the provider estimates at admission.
	CountingInputOutput = "input_and_output"
	// CountingInput counts input tokens only.
	CountingInput = "input"
	// CountingOutputBurndown counts input plus output tokens times
	// output_burndown.
	CountingOutputBurndown = "output_burndown"
)

var countings = []string{CountingInputOutput, CountingInput, CountingOutputBurndown}

// Naming is the route's provider-naming record.
type Naming struct {
	// Provider is the word mark printed for the route, as plain text.
	Provider string `yaml:"provider" json:"provider"`
	// Lines are printed verbatim beside the route.
	Lines []string `yaml:"lines" json:"lines"`
	// PermissionRequired marks a provider that may be named only with its
	// written permission; PermissionRecord then names where it is kept.
	PermissionRequired bool   `yaml:"permission_required,omitempty" json:"permission_required,omitempty"`
	PermissionRecord   string `yaml:"permission_record,omitempty" json:"permission_record,omitempty"`
}

// releaseUnsentKeys may not appear at the top level of a release body:
// sampling controls stay at each provider's default, and no output-length
// forcing is sent (SPEC-HOSTED HA3 points 3 and 7).
var releaseUnsentKeys = []string{
	"temperature", "top_p", "top_k", "min_p", "frequency_penalty", "presence_penalty",
	"repetition_penalty", "ignore_eos", "min_tokens",
}

// ReleaseConnectionProvision is the release transport provision: two
// times the top step's rate times the hard timeout, plus
// load.connections. The config must have passed Validate under the
// release profile.
func (c *Config) ReleaseConnectionProvision() int {
	top := slices.Max(c.Release.Ladder.StepsRPS)
	return int(2*top*float64(c.Client.HTTPTimeoutS)) + c.Load.Connections
}

func (c *Config) validateRelease(v *validator) {
	if c.Run.Name == "" {
		v.errf("run.name: required")
	}
	if c.Run.Repetitions != 0 {
		v.errf("run.repetitions: must be absent under the release profile, got %d", c.Run.Repetitions)
	}
	if c.Run.Phases != (Phases{}) {
		v.errf("run.phases: must be absent under the release profile; release.ladder carries the warm-up, steps and drains")
	}

	l := c.Load
	if l.RateRPS != 0 {
		v.errf("load.rate_rps: must be absent under the release profile; release.ladder.steps_rps sets the rates, got %g", l.RateRPS)
	}
	if l.ArrivalProcess != "poisson" {
		v.errf("load.arrival_process: pinned \"poisson\" under the release profile (SPEC-HOSTED HA3 point 5), got %q", l.ArrivalProcess)
	}
	if l.InputLengthTokens != 0 || l.MaxTokens != 0 {
		v.errf("load.input_length_tokens, load.max_tokens: must be absent under the release profile; release.shape and target.body carry the input bound and the ceiling")
	}
	if l.IgnoreEOS {
		v.errf("load.ignore_eos: pinned false under the release profile")
	}
	if !l.UniquePrefixes {
		v.errf("load.unique_prefixes: pinned true under the release profile (SPEC-HOSTED HA3 point 7)")
	}
	if l.Connections != PinnedReleaseConnections {
		v.errf("load.connections: pinned %d under the release profile (SPEC-HOSTED HA3 point 5), got %d", PinnedReleaseConnections, l.Connections)
	}

	if c.Client.HTTPTimeoutS != PinnedReleaseHardTimeoutS {
		v.errf("client.http_timeout_s: the hard timeout, pinned %d under the release profile (SPEC-HOSTED HA3 point 7), got %d", PinnedReleaseHardTimeoutS, c.Client.HTTPTimeoutS)
	}
	v.pinI("client.retries", c.Client.Retries, PinnedClientRetries)
	c.validateHistogram(v)
	c.validateClientValidity(v)
	for _, b := range []struct {
		field string
		val   any
	}{
		{"slo", c.SLO},
		{"recovery_detector", c.RecoveryDetector},
		{"share_gate", c.ShareGate},
		{"fault", c.Fault},
		{"pins", c.Pins},
		{"mock", c.Mock},
		{"calibration", c.Calibration},
		{"pilot", c.Pilot},
	} {
		if !reflect.ValueOf(b.val).IsZero() {
			v.errf("%s: must be absent under the release profile", b.field)
		}
	}

	r := c.Release
	if r == nil {
		v.errf("release: required under the release profile")
		return
	}
	pin := func(field string, got, want float64) {
		if got != want {
			v.errf("%s: pinned %g under the release profile (SPEC-HOSTED HA3), got %g", field, want, got)
		}
	}
	if !slices.Equal(r.Ladder.StepsRPS, PinnedReleaseStepsRPS) {
		v.errf("release.ladder.steps_rps: pinned %v under the release profile (SPEC-HOSTED HA3 point 5), got %v", PinnedReleaseStepsRPS, r.Ladder.StepsRPS)
	}
	pin("release.ladder.warmup_s", r.Ladder.WarmupS, PinnedReleaseWarmupS)
	pin("release.ladder.step_s", r.Ladder.StepS, PinnedReleaseStepS)
	pin("release.ladder.drain_s", r.Ladder.DrainS, PinnedReleaseDrainS)
	if r.Shape.Name != PinnedReleaseShape {
		v.errf("release.shape.name: pinned %q under the release profile (SPEC-HOSTED HA3 point 7), got %q", PinnedReleaseShape, r.Shape.Name)
	}
	pin("release.shape.input_bound_tokens", float64(r.Shape.InputBoundTokens), PinnedReleaseInputBoundTokens)
	if !slices.Contains(PinnedReleaseCeilings, r.Shape.CeilingTokens) {
		v.errf("release.shape.ceiling_tokens: must be one of %v (SPEC-HOSTED HA3 point 7), got %d", PinnedReleaseCeilings, r.Shape.CeilingTokens)
	}
	pin("release.shape.deadline_s", float64(r.Shape.DeadlineS), PinnedReleaseDeadlineS)
	validateLedgerCaps(v, "release.caps", r.Caps)

	c.validateReleaseTarget(v, &c.Target)
}

// validateLedgerCaps requires both caps as positive decimal numbers, the
// route's within the global cap.
func validateLedgerCaps(v *validator, field string, c LedgerCaps) {
	route, rok := usdDecimal(c.RouteUSD)
	global, gok := usdDecimal(c.GlobalUSD)
	if !rok {
		v.errf("%s.route_usd: a positive decimal number of US dollars required, got %q", field, c.RouteUSD)
	}
	if !gok {
		v.errf("%s.global_usd: a positive decimal number of US dollars required, got %q", field, c.GlobalUSD)
	}
	if rok && gok && route.Cmp(global) > 0 {
		v.errf("%s.route_usd: %s exceeds global_usd %s", field, c.RouteUSD, c.GlobalUSD)
	}
}

var usdDecimalRe = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// usdDecimal parses a positive decimal number such as "400" or "0.5".
func usdDecimal(s string) (*big.Rat, bool) {
	if !usdDecimalRe.MatchString(s) {
		return nil, false
	}
	r, ok := new(big.Rat).SetString(s)
	return r, ok && r.Sign() > 0
}

func (c *Config) validateReleaseTarget(v *validator, t *Target) {
	const field = "target"
	if !fieldKey(t.Name) {
		v.errf("%s.name: one or more of a-z, A-Z, 0-9, _ and -, got %q", field, t.Name)
	}
	if !t.Hosted {
		v.errf("%s.hosted: pinned true under the release profile", field)
	}
	if t.ModelName == "" {
		v.errf("%s.model_name: required", field)
	}
	if t.APIKeyEnv == "" {
		v.errf("%s.api_key_env: required (name of the env var holding the key; never the key itself)", field)
	}
	if err := checkEndpointURL(t.EndpointURL); err != nil {
		v.errf("%s.endpoint_url: %v", field, err)
	}
	if t.BaseURL != "" {
		v.errf("%s.base_url: must be absent; endpoint_url is the complete request URL", field)
	}
	if t.Replicas != 0 {
		v.errf("%s.replicas: must be absent under the release profile, got %d", field, t.Replicas)
	}
	if len(t.MetricsURLs) > 0 || t.QueueGauge != "" || len(t.MetricsFamilies) > 0 || t.TTFTHistogram != "" {
		v.errf("%s: metrics_urls, queue_gauge, metrics_families and ttft_histogram are not read for a hosted target (§6)", field)
	}
	if t.Params != nil || len(t.NotApplied) > 0 {
		v.errf("%s: params and not_applied are set on pilot.targets entries only; target.body is the whole request", field)
	}
	if !slices.Contains(adapters, t.Adapter) {
		v.errf("%s.adapter: must be one of %q, got %q", field, adapters, t.Adapter)
	}
	validateTargetRequest(v, field, ProfileRelease, t)
	if t.Body != "" && CheckLiteralBody(t.Body) == nil && c.Release != nil && slices.Contains(adapters, t.Adapter) {
		if err := checkReleaseBody(t.Body, t.Adapter, c.Release.Shape.CeilingTokens); err != nil {
			v.errf("%s.body: %v", field, err)
		}
	}
	validateResponseHeaders(v, field, t)
	validateResponseShape(v, field, t)
	if t.Probe != nil {
		validateTargetProbe(v, field+".probe", t.Probe)
	}
	if len(t.Limits) == 0 {
		v.errf("%s.limits: required under the release profile (SPEC-HOSTED HA3 point 6)", field)
	}
	for i := range t.Limits {
		validateLimit(v, fmt.Sprintf("%s.limits[%d]", field, i), &t.Limits[i])
	}
	if t.Naming == nil {
		v.errf("%s.naming: required under the release profile", field)
	} else {
		validateNaming(v, field+".naming", t.Naming)
	}
}

// checkReleaseBody reports why a literal body breaks the release shape:
// the adapter's ceiling key must carry the shape's ceiling, and no
// sampling control or output-length forcing is sent.
func checkReleaseBody(body, adapter string, ceiling int) error {
	var top map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &top) != nil {
		return errors.New("not a JSON object")
	}
	keys := []string{"max_tokens"}
	if adapter == AdapterChatCompletions {
		keys = append(keys, "max_completion_tokens")
	}
	var set []string
	for _, k := range keys {
		if top[k] != nil {
			set = append(set, k)
		}
	}
	for _, k := range []string{"max_tokens", "max_completion_tokens"} {
		if top[k] != nil && !slices.Contains(keys, k) {
			return fmt.Errorf("%q is not the ceiling key of the %q adapter", k, adapter)
		}
	}
	switch {
	case len(set) == 0:
		return fmt.Errorf("one ceiling key of %q required", keys)
	case len(set) > 1:
		return fmt.Errorf("%q both set; set one", set)
	}
	if want := fmt.Sprint(ceiling); string(top[set[0]]) != want {
		return fmt.Errorf("%q must be release.shape.ceiling_tokens (%s), got %s", set[0], want, top[set[0]])
	}
	for _, k := range releaseUnsentKeys {
		if top[k] != nil {
			return fmt.Errorf("%q is not sent under the release profile (SPEC-HOSTED HA3 points 3 and 7)", k)
		}
	}
	return nil
}

func validateLimit(v *validator, field string, l *Limit) {
	if !slices.Contains(limitDimensions, l.Dimension) {
		v.errf("%s.dimension: must be one of %q, got %q", field, limitDimensions, l.Dimension)
	}
	if l.Value <= 0 {
		v.errf("%s.value: must be > 0, got %g", field, l.Value)
	}
	switch {
	case l.Dimension == LimitSpendWindow && l.WindowS <= 0:
		v.errf("%s.window_s: must be > 0 for %q, got %g", field, LimitSpendWindow, l.WindowS)
	case l.Dimension != LimitSpendWindow && l.WindowS != 0:
		v.errf("%s.window_s: set for %q only", field, LimitSpendWindow)
	}
	tokens := l.Dimension == LimitTPM || l.Dimension == LimitTPD
	switch {
	case tokens && !slices.Contains(countings, l.Counting):
		v.errf("%s.counting: must be one of %q for %q, got %q", field, countings, l.Dimension, l.Counting)
	case !tokens && l.Counting != "":
		v.errf("%s.counting: set for %q and %q only", field, LimitTPM, LimitTPD)
	}
	switch {
	case l.Counting == CountingOutputBurndown && l.OutputBurndown < 1:
		v.errf("%s.output_burndown: must be >= 1 for counting %q, got %g", field, CountingOutputBurndown, l.OutputBurndown)
	case l.Counting != CountingOutputBurndown && l.OutputBurndown != 0:
		v.errf("%s.output_burndown: set for counting %q only", field, CountingOutputBurndown)
	}
	switch src := strings.TrimSpace(l.Source); {
	case src == "":
		v.errf("%s.source: required (the page's URL or the retained screenshot's file name)", field)
	case strings.Contains(src, "://") && !parsesAsURL(src):
		v.errf("%s.source: %q is not an http or https URL with a host", field, l.Source)
	}
	if _, err := time.Parse(time.DateOnly, l.Retrieved); err != nil {
		v.errf("%s.retrieved: a date as YYYY-MM-DD required, got %q", field, l.Retrieved)
	}
	if !sha256Hex(l.SHA256) {
		v.errf("%s.sha256: 64 lowercase hex characters required, got %q", field, l.SHA256)
	}
}

func validateNaming(v *validator, field string, n *Naming) {
	if strings.TrimSpace(n.Provider) == "" {
		v.errf("%s.provider: required", field)
	}
	if len(n.Lines) == 0 {
		v.errf("%s.lines: required; at least the line that Percentes is not affiliated with or endorsed by the provider", field)
	}
	seen := map[string]bool{}
	for i, l := range n.Lines {
		switch {
		case strings.TrimSpace(l) == "":
			v.errf("%s.lines[%d]: empty", field, i)
		case seen[l]:
			v.errf("%s.lines[%d]: listed twice", field, i)
		}
		seen[l] = true
	}
	if n.PermissionRequired && strings.TrimSpace(n.PermissionRecord) == "" {
		v.errf("%s.permission_record: required when permission_required is true", field)
	}
}

// refuseReleaseFields refuses the release-only target fields on a target
// outside the release profile.
func refuseReleaseFields(v *validator, field string, t *Target) {
	if t.Adapter != "" || len(t.Limits) > 0 || t.Naming != nil {
		v.errf("%s: adapter, limits and naming are set on the release profile's target only", field)
	}
}

func sha256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
