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
	// OtherHosts are the release's other routes of the target's model.
	// The ceiling rule chooses one ceiling from a check of every host
	// (SPEC-HOSTED HA3 point 7).
	OtherHosts []ModelHost `yaml:"other_hosts,omitempty" json:"other_hosts,omitempty"`
}

// ModelHost is a route of the target's model: its route name and the
// model name its requests carry.
type ModelHost struct {
	Route string `yaml:"route" json:"route"`
	Model string `yaml:"model" json:"model"`
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

// MessagesVersion is the anthropic-version header value every Messages
// request carries (SPEC-HOSTED HA3 point 3).
const MessagesVersion = "2023-06-01"

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

// UnpublishedLimit is the operator's dated ruling on a dimension the
// provider documents but neither publishes a value for nor shows in the
// console: the route runs at most PinnedReleaseUnpublishedCapRPS.
type UnpublishedLimit struct {
	// Dimension names the documented dimension: one or more of a-z, 0-9
	// and _, starting with a letter, and no dimension of target.limits.
	Dimension string `yaml:"dimension" json:"dimension"`
	// Source, Retrieved and SHA256 record the page that documents the
	// dimension, as a Limit's.
	Source    string `yaml:"source" json:"source"`
	Retrieved string `yaml:"retrieved" json:"retrieved"`
	SHA256    string `yaml:"sha256" json:"sha256"`
	// Ruled is the ruling's date, YYYY-MM-DD in UTC, on or after
	// Retrieved; Ruling names where the ruling is kept.
	Ruled  string `yaml:"ruled" json:"ruled"`
	Ruling string `yaml:"ruling" json:"ruling"`
}

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

// StatusDoc is the provider's documented meaning of one HTTP status.
type StatusDoc struct {
	Status  int    `yaml:"status" json:"status"`
	Meaning string `yaml:"meaning" json:"meaning"`
	// Source is the page's URL or the retained screenshot's file name;
	// Retrieved is the retrieval date, YYYY-MM-DD in UTC.
	Source    string `yaml:"source" json:"source"`
	Retrieved string `yaml:"retrieved" json:"retrieved"`
}

// ErrorDoc is the provider's documented meaning of one in-band error
// type: the string at the error object's "type" key. Status is the HTTP
// status the provider documents for the same condition, 0 where it
// documents none.
type ErrorDoc struct {
	Type    string `yaml:"type" json:"type"`
	Status  int    `yaml:"status,omitempty" json:"status,omitempty"`
	Meaning string `yaml:"meaning" json:"meaning"`
	// Source and Retrieved are as a StatusDoc's.
	Source    string `yaml:"source" json:"source"`
	Retrieved string `yaml:"retrieved" json:"retrieved"`
}

// BillingRefusal is a response the provider documents as its spend-limit
// or exhausted-balance refusal: a status and, where the status alone does
// not mark it, a string its body carries.
type BillingRefusal struct {
	Status       int    `yaml:"status" json:"status"`
	BodyContains string `yaml:"body_contains,omitempty" json:"body_contains,omitempty"`
	Meaning      string `yaml:"meaning" json:"meaning"`
	// Source and Retrieved are as a StatusDoc's.
	Source    string `yaml:"source" json:"source"`
	Retrieved string `yaml:"retrieved" json:"retrieved"`
}

// Matches reports whether a response with status and body is b.
func (b *BillingRefusal) Matches(status int, body []byte) bool {
	return status == b.Status && strings.Contains(string(body), b.BodyContains)
}

// releaseUnsentKeys may not appear at the top level of a release body:
// sampling controls stay at each provider's default, and no output-length
// forcing is sent (SPEC-HOSTED HA3 points 3 and 7).
var releaseUnsentKeys = []string{
	"temperature", "top_p", "top_k", "min_p", "frequency_penalty", "presence_penalty",
	"repetition_penalty", "ignore_eos", "min_tokens",
}

// releaseParams are the body parameters SPEC-HOSTED HA3 point 3 names
// per adapter other than the model, the texts, stream and the ceiling; a
// target that rejects one lists it in not_applied.
var releaseParams = map[string][]string{
	AdapterChatCompletions: {"stream_options", "reasoning_effort", "reasoning_format"},
	AdapterMessages:        {"thinking", "output_config"},
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
	hosts := map[string]bool{c.Target.Name: true}
	for i, h := range r.OtherHosts {
		field := fmt.Sprintf("release.other_hosts[%d]", i)
		switch {
		case !fieldKey(h.Route):
			v.errf("%s.route: one or more of a-z, A-Z, 0-9, _ and -, got %q", field, h.Route)
		case hosts[h.Route]:
			v.errf("%s.route: %q is target.name or listed twice", field, h.Route)
		}
		hosts[h.Route] = true
		if strings.TrimSpace(h.Model) == "" {
			v.errf("%s.model: required", field)
		}
	}

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
	if t.Params != nil {
		v.errf("%s.params: set on pilot.targets entries only; target.body is the whole request", field)
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
	validateReleaseNotApplied(v, field, t)
	validateResponseHeaders(v, field, t)
	validateRetainedHeaders(v, field, t)
	validateResponseShape(v, field, t)
	if t.Adapter == AdapterMessages && t.ResponseShape != nil {
		v.errf("%s.response_shape: must be absent for the %q adapter, which reads the Messages event fields", field, AdapterMessages)
	}
	if t.Probe != nil {
		validateTargetProbe(v, field+".probe", t.Probe)
	}
	if len(t.Limits) == 0 {
		v.errf("%s.limits: required under the release profile (SPEC-HOSTED HA3 point 6)", field)
	}
	for i := range t.Limits {
		validateLimit(v, fmt.Sprintf("%s.limits[%d]", field, i), &t.Limits[i])
	}
	dims := map[string]bool{}
	for _, l := range t.Limits {
		dims[l.Dimension] = true
	}
	for i := range t.UnpublishedLimits {
		validateUnpublishedLimit(v, fmt.Sprintf("%s.unpublished_limits[%d]", field, i), &t.UnpublishedLimits[i], dims)
	}
	if t.Naming == nil {
		v.errf("%s.naming: required under the release profile", field)
	} else {
		validateNaming(v, field+".naming", t.Naming)
	}
	seen := map[int]bool{}
	for i := range t.StatusDocs {
		validateStatusDoc(v, fmt.Sprintf("%s.status_docs[%d]", field, i), &t.StatusDocs[i], seen)
	}
	types := map[string]bool{}
	for i := range t.ErrorDocs {
		validateErrorDoc(v, fmt.Sprintf("%s.error_docs[%d]", field, i), &t.ErrorDocs[i], types)
	}
	refusals := map[BillingRefusal]bool{}
	for i := range t.BillingRefusals {
		validateBillingRefusal(v, fmt.Sprintf("%s.billing_refusals[%d]", field, i), &t.BillingRefusals[i], refusals)
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

// validateReleaseNotApplied refuses a not_applied entry that is not one
// of the adapter's releaseParams, repeats another, or is a key target.body
// sends.
func validateReleaseNotApplied(v *validator, field string, t *Target) {
	params, ok := releaseParams[t.Adapter]
	if !ok {
		return
	}
	var top map[string]json.RawMessage
	_ = json.Unmarshal([]byte(t.Body), &top)
	seen := map[string]bool{}
	for i, k := range t.NotApplied {
		switch {
		case !slices.Contains(params, k):
			v.errf("%s.not_applied[%d]: %q is not one of the %q adapter's parameters %q (SPEC-HOSTED HA3 point 3)", field, i, k, t.Adapter, params)
		case seen[k]:
			v.errf("%s.not_applied[%d]: %q listed twice", field, i, k)
		case top[k] != nil:
			v.errf("%s.not_applied[%d]: %q is sent in target.body", field, i, k)
		}
		seen[k] = true
	}
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

var unpublishedDimensionRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// validateUnpublishedLimit requires a dimension name that no recorded
// limit and no earlier entry carries, the documenting page's source,
// retrieval date and hash, a ruling date no earlier than the retrieval,
// and where the ruling is kept. seen holds target.limits' dimensions.
func validateUnpublishedLimit(v *validator, field string, u *UnpublishedLimit, seen map[string]bool) {
	switch {
	case !unpublishedDimensionRe.MatchString(u.Dimension):
		v.errf("%s.dimension: one or more of a-z, 0-9 and _, starting with a letter, required, got %q", field, u.Dimension)
	case seen[u.Dimension]:
		v.errf("%s.dimension: %q is recorded with a value or listed twice", field, u.Dimension)
	}
	seen[u.Dimension] = true
	switch src := strings.TrimSpace(u.Source); {
	case src == "":
		v.errf("%s.source: required (the page's URL or the retained screenshot's file name)", field)
	case strings.Contains(src, "://") && !parsesAsURL(src):
		v.errf("%s.source: %q is not an http or https URL with a host", field, u.Source)
	}
	retrieved, rerr := time.Parse(time.DateOnly, u.Retrieved)
	if rerr != nil {
		v.errf("%s.retrieved: a date as YYYY-MM-DD required, got %q", field, u.Retrieved)
	}
	if !sha256Hex(u.SHA256) {
		v.errf("%s.sha256: 64 lowercase hex characters required, got %q", field, u.SHA256)
	}
	switch ruled, err := time.Parse(time.DateOnly, u.Ruled); {
	case err != nil:
		v.errf("%s.ruled: a date as YYYY-MM-DD required, got %q", field, u.Ruled)
	case rerr == nil && ruled.Before(retrieved):
		v.errf("%s.ruled: %s is before retrieved %s", field, u.Ruled, u.Retrieved)
	}
	if strings.TrimSpace(u.Ruling) == "" {
		v.errf("%s.ruling: required (where the dated ruling is kept)", field)
	}
}

// validateStatusDoc requires a status other than 200 that no earlier
// entry documents, a meaning, a source and a retrieval date.
func validateStatusDoc(v *validator, field string, d *StatusDoc, seen map[int]bool) {
	switch {
	case d.Status < 100 || d.Status > 599 || d.Status == 200:
		v.errf("%s.status: an HTTP status from 100 to 599 other than 200 required, got %d", field, d.Status)
	case seen[d.Status]:
		v.errf("%s.status: %d documented twice", field, d.Status)
	}
	seen[d.Status] = true
	validateDoc(v, field, d.Meaning, d.Source, d.Retrieved)
}

// validateErrorDoc requires a type that no earlier entry documents, a
// status that is 0 or as a StatusDoc's, a meaning, a source and a
// retrieval date.
func validateErrorDoc(v *validator, field string, d *ErrorDoc, seen map[string]bool) {
	switch {
	case d.Type == "" || strings.TrimSpace(d.Type) != d.Type:
		v.errf("%s.type: an error type with no surrounding space required, got %q", field, d.Type)
	case seen[d.Type]:
		v.errf("%s.type: %q documented twice", field, d.Type)
	}
	seen[d.Type] = true
	if d.Status != 0 && (d.Status < 100 || d.Status > 599 || d.Status == 200) {
		v.errf("%s.status: 0 or an HTTP status from 100 to 599 other than 200 required, got %d", field, d.Status)
	}
	validateDoc(v, field, d.Meaning, d.Source, d.Retrieved)
}

// validateBillingRefusal requires a 4xx or 5xx status, a body string with
// no surrounding space, a status and string that no earlier entry
// records, a meaning, a source and a retrieval date.
func validateBillingRefusal(v *validator, field string, b *BillingRefusal, seen map[BillingRefusal]bool) {
	if b.Status < 400 || b.Status > 599 {
		v.errf("%s.status: a 4xx or 5xx status required, got %d", field, b.Status)
	}
	if strings.TrimSpace(b.BodyContains) != b.BodyContains {
		v.errf("%s.body_contains: surrounding space in %q", field, b.BodyContains)
	}
	key := BillingRefusal{Status: b.Status, BodyContains: b.BodyContains}
	if seen[key] {
		v.errf("%s: status %d with body %q recorded twice", field, b.Status, b.BodyContains)
	}
	seen[key] = true
	validateDoc(v, field, b.Meaning, b.Source, b.Retrieved)
}

func validateDoc(v *validator, field, meaning, source, retrieved string) {
	if strings.TrimSpace(meaning) == "" {
		v.errf("%s.meaning: required", field)
	}
	switch src := strings.TrimSpace(source); {
	case src == "":
		v.errf("%s.source: required (the page's URL or the retained screenshot's file name)", field)
	case strings.Contains(src, "://") && !parsesAsURL(src):
		v.errf("%s.source: %q is not an http or https URL with a host", field, source)
	}
	if _, err := time.Parse(time.DateOnly, retrieved); err != nil {
		v.errf("%s.retrieved: a date as YYYY-MM-DD required, got %q", field, retrieved)
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

// validateRetainedHeaders refuses a retained_headers entry that is not a
// header name (with an optional trailing "*"), repeats another, could
// match a name target.response_headers does not record, or could match
// Retry-After, whose values the per-step tables print.
func validateRetainedHeaders(v *validator, field string, t *Target) {
	seen := map[string]bool{}
	for i, e := range t.RetainedHeaders {
		if !headerToken(strings.TrimSuffix(e, "*")) {
			v.errf("%s.retained_headers[%d]: %q is not a header name, optionally ending in *", field, i, e)
			continue
		}
		key := strings.ToLower(e)
		if seen[key] {
			v.errf("%s.retained_headers[%d]: %q listed twice", field, i, e)
		}
		seen[key] = true
		if !slices.ContainsFunc(t.ResponseHeaders, func(a string) bool { return headerCovers(a, e) }) {
			v.errf("%s.retained_headers[%d]: %q matches names target.response_headers does not record", field, i, e)
		}
		if headerMatch(e, "Retry-After") {
			v.errf("%s.retained_headers[%d]: %q matches Retry-After, whose values the per-step tables print", field, i, e)
		}
	}
}

// refuseReleaseFields refuses the release-only target fields on a target
// outside the release profile.
func refuseReleaseFields(v *validator, field string, t *Target) {
	if t.Adapter != "" || len(t.Limits) > 0 || len(t.UnpublishedLimits) > 0 || len(t.RetainedHeaders) > 0 || t.Naming != nil || len(t.StatusDocs) > 0 || len(t.ErrorDocs) > 0 || len(t.BillingRefusals) > 0 {
		v.errf("%s: adapter, limits, unpublished_limits, retained_headers, naming, status_docs, error_docs and billing_refusals are set on the release profile's target only", field)
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
