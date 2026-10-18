package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
)

// PilotCaseSlot is the JSON string in pilot.body that the case text
// replaces. It appears once, unescaped, as the user message content.
const PilotCaseSlot = `"{{case}}"`

// Pilot is the pilot profile's request definition (SPEC-HOSTED HA2 §2):
// one literal body for every endpoint, and the endpoints.
type Pilot struct {
	// Body is a JSON object holding only "messages" (one system message,
	// then one user message whose content is PilotCaseSlot) and "stream"
	// (true). RequestBody keeps its bytes, trimmed of surrounding
	// whitespace, apart from the slot, and adds the target's model and
	// params first.
	Body    string   `yaml:"body" json:"body"`
	Targets []Target `yaml:"targets" json:"targets"`
}

// Params are a pilot target's generation settings, sent as top-level body
// keys in field order. Exactly one ceiling key is set.
type Params struct {
	MaxCompletionTokens int            `yaml:"max_completion_tokens,omitempty" json:"max_completion_tokens,omitempty"`
	MaxTokens           int            `yaml:"max_tokens,omitempty" json:"max_tokens,omitempty"`
	ReasoningEffort     string         `yaml:"reasoning_effort,omitempty" json:"reasoning_effort,omitempty"`
	ReasoningFormat     string         `yaml:"reasoning_format,omitempty" json:"reasoning_format,omitempty"`
	Temperature         *float64       `yaml:"temperature,omitempty" json:"temperature,omitempty"`
	StreamOptions       *StreamOptions `yaml:"stream_options,omitempty" json:"stream_options,omitempty"`
}

// StreamOptions is the usage request.
type StreamOptions struct {
	IncludeUsage bool `yaml:"include_usage" json:"include_usage"`
}

// reasoningFormats are the reasoning_format values the pilot accepts.
var reasoningFormats = []string{"hidden", "raw", "parsed"}

// pilotSettings are the params other than the ceiling. A setting one
// target sends is sent with the same value by every other target, or
// listed in that target's not_applied (HA2 §2).
var pilotSettings = []struct {
	key string
	get func(*Params) (any, bool)
}{
	{"reasoning_effort", func(p *Params) (any, bool) { return p.ReasoningEffort, p.ReasoningEffort != "" }},
	{"reasoning_format", func(p *Params) (any, bool) { return p.ReasoningFormat, p.ReasoningFormat != "" }},
	{"temperature", func(p *Params) (any, bool) {
		if p.Temperature == nil {
			return nil, false
		}
		return *p.Temperature, true
	}},
	{"stream_options", func(p *Params) (any, bool) {
		if p.StreamOptions == nil {
			return nil, false
		}
		return *p.StreamOptions, true
	}},
}

// RequestBody returns the body sent to t for one case: pilot.body with
// the slot replaced by the JSON-encoded case text, and "model" and t's
// params inserted after the opening brace. The config must have passed
// Validate.
func (p *Pilot) RequestBody(t *Target, caseText string) []byte {
	body := bytes.TrimSpace([]byte(p.Body))
	body = bytes.Replace(body, []byte(PilotCaseSlot), jsonString(caseText), 1)
	var b bytes.Buffer
	b.WriteString(`{"model":`)
	b.Write(jsonString(t.ModelName))
	if t.Params != nil {
		params, _ := json.Marshal(t.Params)
		if inner := params[1 : len(params)-1]; len(inner) > 0 {
			b.WriteByte(',')
			b.Write(inner)
		}
	}
	b.WriteByte(',')
	b.Write(body[1:])
	return b.Bytes()
}

// jsonString encodes s as a JSON string without HTML escaping.
func jsonString(s string) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}

func (c *Config) validatePilot(v *validator) {
	if c.Run.Name == "" {
		v.errf("run.name: required")
	}
	if c.Run.Repetitions != 0 {
		v.errf("run.repetitions: must be absent under the pilot profile, got %d", c.Run.Repetitions)
	}
	ph := c.Run.Phases
	pin := func(field string, got, want float64) {
		if got != want {
			v.errf("%s: pinned %g under the pilot profile (SPEC-HOSTED HA2 §3), got %g", field, want, got)
		}
	}
	pin("run.phases.warmup_s", ph.WarmupS, PinnedPilotWarmupS)
	pin("run.phases.baseline_s", ph.BaselineS, PinnedPilotMeasuredS)
	if ph.FaultWindowTimeoutS != 0 {
		v.errf("run.phases.fault_window_timeout_s: no fault window under the pilot profile, got %g", ph.FaultWindowTimeoutS)
	}
	if ph.CooldownS != 0 {
		v.errf("run.phases.cooldown_s: no cooldown under the pilot profile, got %g", ph.CooldownS)
	}

	l := c.Load
	pin("load.rate_rps", l.RateRPS, PinnedPilotRateRPS)
	if l.ArrivalProcess != "deterministic" {
		v.errf("load.arrival_process: pinned \"deterministic\" under the pilot profile (SPEC-HOSTED HA2 §3), got %q", l.ArrivalProcess)
	}
	if l.InputLengthTokens != 0 || l.MaxTokens != 0 {
		v.errf("load.input_length_tokens, load.max_tokens: must be absent under the pilot profile; the input and the ceiling come from pilot.body and params")
	}
	if l.IgnoreEOS {
		v.errf("load.ignore_eos: pinned false under the pilot profile")
	}
	if l.UniquePrefixes {
		v.errf("load.unique_prefixes: pinned false under the pilot profile (SPEC-HOSTED HA2 §3: no unique prefix)")
	}
	if l.Connections != PinnedPilotConnections {
		v.errf("load.connections: pinned %d under the pilot profile (SPEC-HOSTED §3), got %d", PinnedPilotConnections, l.Connections)
	}

	c.validateClient(v)
	c.validateHistogram(v)
	c.validateClientValidity(v)
	for _, b := range []struct {
		field string
		val   any
	}{
		{"slo", c.SLO},
		{"recovery_detector", c.RecoveryDetector},
		{"share_gate", c.ShareGate},
		{"target", c.Target},
		{"fault", c.Fault},
		{"pins", c.Pins},
		{"mock", c.Mock},
		{"calibration", c.Calibration},
	} {
		if !reflect.ValueOf(b.val).IsZero() {
			v.errf("%s: must be absent under the pilot profile", b.field)
		}
	}

	if c.Pilot == nil {
		v.errf("pilot: required under the pilot profile")
		return
	}
	if err := checkPilotBody(c.Pilot.Body); err != nil {
		v.errf("pilot.body: %v", err)
	}
	ts := c.Pilot.Targets
	if len(ts) != PinnedPilotTargets {
		v.errf("pilot.targets: pinned %d endpoints (SPEC-HOSTED HA2), got %d", PinnedPilotTargets, len(ts))
	}
	names, urls := map[string]bool{}, map[string]bool{}
	for i := range ts {
		t := &ts[i]
		field := fmt.Sprintf("pilot.targets[%d]", i)
		validatePilotTarget(v, field, t)
		if names[t.Name] {
			v.errf("%s.name: %q listed twice", field, t.Name)
		}
		names[t.Name] = true
		if urls[t.EndpointURL] {
			v.errf("%s.endpoint_url: %q listed twice", field, t.EndpointURL)
		}
		urls[t.EndpointURL] = true
	}
	validatePilotSettings(v, ts)
}

func validatePilotTarget(v *validator, field string, t *Target) {
	if !fieldKey(t.Name) {
		v.errf("%s.name: one or more of a-z, A-Z, 0-9, _ and -, got %q", field, t.Name)
	}
	if !t.Hosted {
		v.errf("%s.hosted: pinned true under the pilot profile", field)
	}
	if t.ModelName == "" {
		v.errf("%s.model_name: required", field)
	}
	if t.APIKeyEnv == "" {
		v.errf("%s.api_key_env: required (name of the env var holding the bearer token; never the token itself)", field)
	}
	if err := checkEndpointURL(t.EndpointURL); err != nil {
		v.errf("%s.endpoint_url: %v", field, err)
	}
	if t.BaseURL != "" {
		v.errf("%s.base_url: must be absent; endpoint_url is the complete request URL", field)
	}
	if t.Replicas != 0 {
		v.errf("%s.replicas: must be absent under the pilot profile, got %d", field, t.Replicas)
	}
	if len(t.MetricsURLs) > 0 || t.QueueGauge != "" || len(t.MetricsFamilies) > 0 || t.TTFTHistogram != "" {
		v.errf("%s: metrics_urls, queue_gauge, metrics_families and ttft_histogram are not read for a hosted target (§6)", field)
	}
	validateResponseHeaders(v, field, t)
	validateResponseShape(v, field, t)

	p := t.Params
	if p == nil {
		v.errf("%s.params: required", field)
		return
	}
	switch {
	case p.MaxCompletionTokens != 0 && p.MaxTokens != 0:
		v.errf("%s.params: max_completion_tokens and max_tokens both set; set one", field)
	case p.MaxCompletionTokens == 0 && p.MaxTokens == 0:
		v.errf("%s.params: max_completion_tokens or max_tokens required, pinned %d", field, PinnedPilotCompletionCeiling)
	case p.MaxCompletionTokens+p.MaxTokens != PinnedPilotCompletionCeiling:
		v.errf("%s.params: completion ceiling pinned %d (SPEC-HOSTED HA2 §3), got %d", field, PinnedPilotCompletionCeiling, p.MaxCompletionTokens+p.MaxTokens)
	}
	if p.ReasoningEffort != "" && !fieldKey(p.ReasoningEffort) {
		v.errf("%s.params.reasoning_effort: %q is not a setting value", field, p.ReasoningEffort)
	}
	if p.ReasoningFormat != "" && !contains(reasoningFormats, p.ReasoningFormat) {
		v.errf("%s.params.reasoning_format: must be one of %q, got %q", field, reasoningFormats, p.ReasoningFormat)
	}
	if p.Temperature != nil && *p.Temperature < 0 {
		v.errf("%s.params.temperature: must be >= 0, got %g", field, *p.Temperature)
	}
	if p.StreamOptions != nil && !p.StreamOptions.IncludeUsage {
		v.errf("%s.params.stream_options: include_usage must be true when set", field)
	}
}

// validatePilotSettings holds every setting equal across the targets,
// except where a target lists it in not_applied.
func validatePilotSettings(v *validator, ts []Target) {
	known := map[string]bool{}
	for _, s := range pilotSettings {
		known[s.key] = true
	}
	for i := range ts {
		field := fmt.Sprintf("pilot.targets[%d].not_applied", i)
		seen := map[string]bool{}
		for j, k := range ts[i].NotApplied {
			switch {
			case !known[k]:
				v.errf("%s[%d]: %q is not a pilot setting", field, j, k)
			case seen[k]:
				v.errf("%s[%d]: %q listed twice", field, j, k)
			case ts[i].Params != nil && isSet(ts[i].Params, k):
				v.errf("%s[%d]: %q is set in this target's params", field, j, k)
			case !setElsewhere(ts, i, k):
				v.errf("%s[%d]: %q is not set on any other target", field, j, k)
			}
			seen[k] = true
		}
	}
	for _, s := range pilotSettings {
		for a := range ts {
			if ts[a].Params == nil {
				continue
			}
			va, ok := s.get(ts[a].Params)
			if !ok {
				continue
			}
			for b := range ts {
				if b == a || ts[b].Params == nil || contains(ts[b].NotApplied, s.key) {
					continue
				}
				vb, ok := s.get(ts[b].Params)
				switch {
				case !ok:
					v.errf("pilot.targets[%d].params.%s: unset while pilot.targets[%d] sets it; set the same value or list it in not_applied (SPEC-HOSTED HA2 §2)", b, s.key, a)
				case b > a && vb != va:
					v.errf("pilot.targets[%d].params.%s: %v differs from pilot.targets[%d] (%v); the settings are the same on every endpoint (SPEC-HOSTED HA2 §2)", b, s.key, vb, a, va)
				}
			}
		}
	}
}

func isSet(p *Params, key string) bool {
	for _, s := range pilotSettings {
		if s.key == key {
			_, ok := s.get(p)
			return ok
		}
	}
	return false
}

func setElsewhere(ts []Target, i int, key string) bool {
	for j := range ts {
		if j != i && ts[j].Params != nil && isSet(ts[j].Params, key) {
			return true
		}
	}
	return false
}

func contains(list []string, s string) bool {
	for _, e := range list {
		if e == s {
			return true
		}
	}
	return false
}

// checkEndpointURL reports why s is not a complete request URL: http or
// https, a host and a path, and no userinfo, query or fragment.
func checkEndpointURL(s string) error {
	if !parsesAsURL(s) {
		return errors.New("not an http or https URL with a host")
	}
	u, _ := url.Parse(s)
	if u.User != nil || strings.ContainsAny(s, "?#") {
		return errors.New("no userinfo, query or fragment")
	}
	if u.Path == "" || u.Path == "/" {
		return errors.New("the complete request path is required")
	}
	return nil
}

// checkPilotBody reports why body is not a pilot body template.
func checkPilotBody(body string) error {
	raw := []byte(body)
	if !json.Valid(raw) {
		return errors.New("not valid JSON")
	}
	if k, err := duplicateKey(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return err
	} else if k != "" {
		return fmt.Errorf("key %q repeated in one object", k)
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil {
		return errors.New("not a JSON object")
	}
	if len(top) != 2 || top["messages"] == nil || top["stream"] == nil {
		return fmt.Errorf("keys must be exactly \"messages\" and \"stream\", got %q", sortedKeys(top))
	}
	if string(top["stream"]) != "true" {
		return errors.New("\"stream\" must be true")
	}
	var msgs []map[string]json.RawMessage
	if json.Unmarshal(top["messages"], &msgs) != nil || len(msgs) != 2 {
		return errors.New("\"messages\" must be one system message, then one user message")
	}
	for i, want := range []string{"system", "user"} {
		var role, content string
		if len(msgs[i]) != 2 || json.Unmarshal(msgs[i]["role"], &role) != nil || json.Unmarshal(msgs[i]["content"], &content) != nil {
			return fmt.Errorf("messages[%d]: keys must be exactly \"role\" and \"content\", both strings", i)
		}
		if role != want {
			return fmt.Errorf("messages[%d].role: must be %q, got %q", i, want, role)
		}
		if want == "system" && strings.TrimSpace(content) == "" {
			return errors.New("messages[0].content: the system text is empty")
		}
		if want == "user" && string(msgs[i]["content"]) != PilotCaseSlot {
			return fmt.Errorf("messages[1].content: must be %s", PilotCaseSlot)
		}
	}
	if n := strings.Count(body, PilotCaseSlot); n != 1 {
		return fmt.Errorf("%s must appear once, got %d", PilotCaseSlot, n)
	}
	return nil
}

// duplicateKey walks one JSON value and returns the first key repeated
// within a single object.
func duplicateKey(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	switch tok {
	case json.Delim('{'):
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return "", err
			}
			k, _ := kt.(string)
			if seen[k] {
				return k, nil
			}
			seen[k] = true
			if d, err := duplicateKey(dec); d != "" || err != nil {
				return d, err
			}
		}
		_, err = dec.Token()
	case json.Delim('['):
		for dec.More() {
			if d, err := duplicateKey(dec); d != "" || err != nil {
				return d, err
			}
		}
		_, err = dec.Token()
	}
	return "", err
}

func sortedKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
