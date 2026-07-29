package loadgen

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

func testGen(hosted bool, model, key string) *gen {
	cfg := &config.Config{}
	cfg.Run.Seed = 42
	cfg.Load.MaxTokens = 256
	cfg.Target.BaseURL = "http://example.invalid"
	cfg.Target.Hosted = hosted
	return &gen{cfg: cfg, filler: "xyz", model: model, apiKey: key}
}

// The mock/self-hosted body keeps the vLLM output-forcing extension
// (ignore_eos, SPEC.md §6); a hosted body must omit it — hosted
// OpenAI-compatible endpoints reject or ignore vLLM extensions.
func TestRequestBodyMockVsHosted(t *testing.T) {
	r := &Request{Index: 7}

	var m map[string]any
	body := testGen(false, "percentes-mock", "").requestBody(r)
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("mock body is not valid JSON: %v\n%s", err, body)
	}
	if m["model"] != "percentes-mock" || m["ignore_eos"] != true || m["stream"] != true {
		t.Fatalf("mock body wrong: %s", body)
	}
	if m["max_tokens"].(float64) != 256 {
		t.Fatalf("max_tokens must carry the pinned budget: %s", body)
	}

	m = map[string]any{}
	body = testGen(true, "llama-3.1-8b-instant", "k").requestBody(r)
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("hosted body is not valid JSON: %v\n%s", err, body)
	}
	if m["model"] != "llama-3.1-8b-instant" {
		t.Fatalf("hosted body must carry the configured model: %s", body)
	}
	if _, has := m["ignore_eos"]; has {
		t.Fatalf("hosted body must omit ignore_eos: %s", body)
	}
}

// The bearer token rides only in the Authorization header, and only
// when a key is resolved; the mock path sends no auth at all.
func TestAuthorizationHeader(t *testing.T) {
	g := testGen(true, "m", "sk-test-value")
	req, err := g.newRequest(context.Background(), "{}")
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer sk-test-value" {
		t.Fatalf("want bearer header, got %q", got)
	}
	if req.Header.Get("Content-Type") != "application/json" {
		t.Fatal("content-type lost in refactor")
	}

	g = testGen(false, "percentes-mock", "")
	req, err = g.newRequest(context.Background(), "{}")
	if err != nil {
		t.Fatal(err)
	}
	if _, has := req.Header["Authorization"]; has {
		t.Fatal("no-key path must send no Authorization header")
	}
}
