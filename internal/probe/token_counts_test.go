package probe

import (
	"encoding/json"
	"math"
	"testing"
)

// A reasoning_tokens count past int64 is math.MaxInt and an exponent
// count its value.
func TestReasoningTokensNotation(t *testing.T) {
	for _, c := range []struct {
		usage string
		want  int
	}{
		{`{"completion_tokens_details":{"reasoning_tokens":99999999999999999999}}`, math.MaxInt},
		{`{"completion_tokens_details":{"reasoning_tokens":9e3}}`, 9000},
		{`{"completion_tokens_details":{"reasoning_tokens":4.5}}`, 0},
	} {
		if got := reasoningTokens(json.RawMessage(c.usage)); got != c.want {
			t.Errorf("%s: %d, want %d", c.usage, got, c.want)
		}
	}
}
