package report

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

// A saturated count prints as at least math.MaxInt in the step's token
// lines.
func TestTokenCountsTextSaturated(t *testing.T) {
	c := Counts{Of: 3, N: 2, Min: 1000, P50: 1000, P90: math.MaxInt, Max: math.MaxInt}
	want := "on 2 of 3: min 1000, median 1000, 90th percentile at least 9223372036854775807, max at least 9223372036854775807"
	if got := tokenCountsText(c); got != want {
		t.Errorf("%q, want %q", got, want)
	}
	if got := tokenCountsText(Counts{Of: 2}); got != countsText(Counts{Of: 2}) {
		t.Errorf("no count: %q", got)
	}
}

// A record's negative completion or reasoning count enters no step
// figure: the throughput and the token counts read as if absent.
func TestNegativeTokenCountsEnterNoStepFigure(t *testing.T) {
	neg, negR, pos := -500, -900, 300
	tl := newStepTally()
	for _, r := range []evidence.Record{
		{IntendedNs: 1, FirstTokNs: 2, DoneNs: 3, Outcome: loadgen.OutcomeCompleted, InTime: true, CompletionTokens: &neg, ReasoningTokens: &negR},
		{IntendedNs: 1, FirstTokNs: 2, DoneNs: 3, Outcome: loadgen.OutcomeCompleted, InTime: true, CompletionTokens: &pos},
	} {
		if err := tl.timing.add(LineInTime, &r, 0); err != nil {
			t.Fatal(err)
		}
	}
	if len(tl.tokens) != 1 || tl.tokens[0] != 300 || len(tl.reasoning) != 0 {
		t.Errorf("tokens %v, reasoning %v", tl.tokens, tl.reasoning)
	}
	if tp := throughput([]*stepTally{&tl}, 60); tp.TokenCompletions != 1 || tp.Tokens != 300 {
		t.Errorf("throughput %+v", tp)
	}
}

// A prompt count or prompt-plus-cache sum past math.MaxInt reads as
// math.MaxInt, a count, so it stays above any input bound.
func TestPromptTokensPastMaxInt(t *testing.T) {
	for _, u := range []string{
		`{"input_tokens":9223372036854775000,"cache_read_input_tokens":1000}`,
		`{"prompt_tokens":99999999999999999999}`,
	} {
		if n, ok := promptTokens(json.RawMessage(u)); n != math.MaxInt || !ok {
			t.Errorf("%s: %d %t", u, n, ok)
		}
	}
}
