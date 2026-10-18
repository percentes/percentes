package config

import (
	"bytes"
	"strings"
	"testing"
)

// hostedAC is the ac reference with a hosted target.
func hostedAC(t *testing.T) *Config {
	t.Helper()
	c := loadRef(t, acRef)
	c.Target.Hosted, c.Target.ModelName, c.Target.APIKeyEnv = true, "m", "SOME_KEY_ENV"
	c.Load.IgnoreEOS = false
	return c
}

func TestAuthHeaderValidation(t *testing.T) {
	for _, h := range []string{"", AuthBearer, AuthXAPIKey, AuthAPIKey} {
		c := hostedAC(t)
		c.Target.AuthHeader = h
		if err := c.Validate(); err != nil {
			t.Errorf("target.auth_header %q must validate: %v", h, err)
		}
		p := loadRef(t, pilotRef)
		p.Pilot.Targets[1].AuthHeader = h
		if err := p.Validate(); err != nil {
			t.Errorf("pilot.targets[1].auth_header %q must validate: %v", h, err)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"unknown target header", func(c *Config) { c.Target.AuthHeader = "Authorization" }, "target.auth_header: must be one of"},
		{"no key env", func(c *Config) { c.Target.AuthHeader, c.Target.Hosted, c.Target.APIKeyEnv = AuthXAPIKey, false, "" }, "target.auth_header: set without api_key_env"},
	} {
		c := hostedAC(t)
		tc.mutate(c)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
	p := loadRef(t, pilotRef)
	p.Pilot.Targets[0].AuthHeader = "bearer"
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "pilot.targets[0].auth_header: must be one of") {
		t.Errorf("unknown pilot header: got %v", err)
	}
}

// No profile in this tree sends a target's literal body.
func TestTargetBodyRefused(t *testing.T) {
	const body = `{"stream":true,"messages":[{"role":"user","content":"{{case}}"}]}`
	for _, ref := range []string{acRef, experimentRef} {
		c := loadRef(t, ref)
		c.Target.Body = body
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "target.body: a literal request body is not accepted") {
			t.Errorf("%s: got %v", ref, err)
		}
	}
	p := loadRef(t, pilotRef)
	p.Pilot.Targets[0].Body = body
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "pilot.targets[0].body: a literal request body is not accepted under the \"pilot\" profile") {
		t.Errorf("pilot target body: got %v", err)
	}
}

func TestKeyHeader(t *testing.T) {
	for _, tc := range []struct{ kind, name, value string }{
		{"", "Authorization", "Bearer k1"},
		{AuthBearer, "Authorization", "Bearer k1"},
		{AuthXAPIKey, "X-Api-Key", "k1"},
		{AuthAPIKey, "Api-Key", "k1"},
	} {
		name, value := (&Target{AuthHeader: tc.kind}).KeyHeader("k1")
		if name != tc.name || value != tc.value {
			t.Errorf("%q: %s: %s, want %s: %s", tc.kind, name, value, tc.name, tc.value)
		}
	}
}

// Every header that can carry the key is never recorded.
func TestKeyHeadersNeverRecorded(t *testing.T) {
	for _, h := range []string{"x-api-key", "Api-Key", "X-API-KEY"} {
		if HeaderAllowed([]string{h, "*"}, h) {
			t.Errorf("%s allowed", h)
		}
	}
	c := hostedAC(t)
	c.Target.ResponseHeaders = []string{"x-api-*"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "matches X-Api-Key, which is never recorded") {
		t.Errorf("x-api-* allowlist: got %v", err)
	}
}

const messagesBody = ` {"model":"claude-sonnet-5-5","system":"Summarise.","messages":[{"role":"user","content":"{{case}}"}],
  "stream":true,"max_tokens":512,"thinking":{"type":"between_tools"}} `

func TestCheckLiteralBody(t *testing.T) {
	if err := CheckLiteralBody(messagesBody); err != nil {
		t.Fatalf("messages body: %v", err)
	}
	for _, tc := range []struct{ name, body, want string }{
		{"not JSON", `{"stream":true,`, "not valid JSON"},
		{"array", `["{{case}}"]`, "not a JSON object"},
		{"repeated key", `{"stream":true,"stream":true,"x":"{{case}}"}`, `key "stream" repeated`},
		{"stream absent", `{"x":"{{case}}"}`, `"stream" must be true`},
		{"stream false", `{"stream":false,"x":"{{case}}"}`, `"stream" must be true`},
		{"stream string", `{"stream":"true","x":"{{case}}"}`, `"stream" must be true`},
		{"no slot", `{"stream":true,"x":"case"}`, "must appear once, got 0"},
		{"two slots", `{"stream":true,"x":"{{case}}","y":["{{case}}"]}`, "must appear once, got 2"},
		{"slot as key", `{"stream":true,"{{case}}":1}`, "one whole string value"},
		{"slot inside a string", `{"stream":true,"x":"a \"{{case}}"}`, "one whole string value"},
	} {
		if err := CheckLiteralBody(tc.body); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got: %v", tc.name, tc.want, err)
		}
	}
}

// Every byte but the slot is sent as configured, whitespace included.
func TestLiteralBodyBytes(t *testing.T) {
	text := "ticket \"7\" <b> & é\n"
	got := LiteralBody(messagesBody, text)
	i := strings.Index(messagesBody, PilotCaseSlot)
	enc := `"ticket \"7\" <b> & é\n"`
	want := messagesBody[:i] + enc + messagesBody[i+len(PilotCaseSlot):]
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}
