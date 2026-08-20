package config

import (
	"strings"
	"testing"
)

// Hosted targets (SPEC.md §2's OpenAI-compatible client pointed at a
// managed endpoint) must name the model and the environment variable
// carrying the bearer token; the config itself never holds a secret.
func TestHostedTargetValidation(t *testing.T) {
	c := loadRef(t, acRef)
	c.Target.Hosted = true

	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "target.model_name") {
		t.Fatalf("hosted target without model_name must fail on it, got: %v", err)
	}

	c.Target.ModelName = "llama-3.1-8b-instant"
	err = c.Validate()
	if err == nil || !strings.Contains(err.Error(), "target.api_key_env") {
		t.Fatalf("hosted target without api_key_env must fail on it, got: %v", err)
	}

	c.Target.APIKeyEnv = "SOME_KEY_ENV"
	c.Load.IgnoreEOS = false // pinned false for a hosted target (§6)
	if err := c.Validate(); err != nil {
		t.Fatalf("fully-specified hosted target must validate: %v", err)
	}
}

// Non-hosted targets require neither model_name nor api_key_env, and
// model_name alone is legal: a self-hosted vLLM deployment needs one.
func TestNonHostedTargetUnchanged(t *testing.T) {
	c := loadRef(t, acRef)
	if err := c.Validate(); err != nil {
		t.Fatalf("reference config must still validate: %v", err)
	}
	c.Target.ModelName = "some-pinned-model"
	if err := c.Validate(); err != nil {
		t.Fatalf("model_name without hosted must be legal: %v", err)
	}
}
