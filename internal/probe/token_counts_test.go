package probe

import (
	"encoding/json"
	"math"
	"testing"
)

// A total_tokens past int64 makes the output math.MaxInt, whatever the
// prompt count; an unsaturated total less the prompt still counts.
func TestReportedOutputSaturatedTotal(t *testing.T) {
	for _, c := range []struct {
		prompt, total string
		want          int
	}{
		{"1000", "1e20", math.MaxInt},
		{"1e20", "1e20", math.MaxInt},
		{"1000", "99999999999999999999", math.MaxInt},
		{"1000", "1300", 300},
	} {
		s := &Shape{usage: map[string]json.RawMessage{"prompt_tokens": json.RawMessage(c.prompt), "completion_tokens": json.RawMessage("100"), "total_tokens": json.RawMessage(c.total)}}
		if _, o := s.Reported(); o == nil || *o != c.want {
			t.Errorf("prompt %s total %s: output %v, want %d", c.prompt, c.total, o, c.want)
		}
	}
}

// A record's reported counts survive its JSON form; Shape's usage does
// not.
func TestRecordReportedCountsRoundTrip(t *testing.T) {
	s := &Shape{usage: map[string]json.RawMessage{"prompt_tokens": json.RawMessage("5"), "completion_tokens": json.RawMessage("8")}}
	rec := &Record{Shape: s}
	rec.PromptTokens, rec.OutputTokens = s.Reported()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	var got Record
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.PromptTokens == nil || *got.PromptTokens != 5 || got.OutputTokens == nil || *got.OutputTokens != 8 {
		t.Errorf("decoded counts %v %v, want 5 and 8", got.PromptTokens, got.OutputTokens)
	}
}

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
