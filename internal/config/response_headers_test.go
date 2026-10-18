package config

import (
	"strings"
	"testing"
)

func TestResponseHeadersValidation(t *testing.T) {
	c := loadRef(t, acRef)
	c.Target.ResponseHeaders = []string{"Retry-After", "x-ratelimit-*", "X-Request-Id"}
	if err := c.Validate(); err != nil {
		t.Fatalf("allowlist must validate: %v", err)
	}
	for _, bad := range [][]string{
		{"Authorization"},
		{"proxy-authorization"},
		{"Set-Cookie"},
		{"cookie"},
		{"Auth*"},
		{"Set-*"},
		{"*"},
		{""},
		{"X Request"},
		{"X-*-Id"},
		{"Retry-After", "retry-after"},
	} {
		c.Target.ResponseHeaders = bad
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "target.response_headers") {
			t.Errorf("%q must fail on target.response_headers, got: %v", bad, err)
		}
	}
}

func TestHeaderAllowed(t *testing.T) {
	allow := []string{"Retry-After", "X-Ratelimit-*", "Authorization", "Set-*"}
	for _, c := range []struct {
		name string
		want bool
	}{
		{"Retry-After", true},
		{"retry-after", true},
		{"X-Ratelimit-Remaining-Tokens", true},
		{"X-Ratelimit-", true},
		{"X-Ratelimit", false},
		{"Authorization", false},
		{"Set-Cookie", false},
	} {
		if got := HeaderAllowed(allow, c.name); got != c.want {
			t.Errorf("HeaderAllowed(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}
