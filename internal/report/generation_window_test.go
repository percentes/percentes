package report

import (
	"slices"
	"testing"

	"github.com/percentes/percentes/internal/evidence"
)

// A completed record's nonnegative reasoning count enters the step's
// reasoning figures.
func TestStepFiguresKeepReasoningTokens(t *testing.T) {
	reasoning, tokens := 40, 300
	tl := newStepTally()
	r := evidence.Record{IntendedNs: 1, FirstTokNs: 2, DoneNs: 3, InTime: true, CompletionTokens: &tokens, ReasoningTokens: &reasoning}
	if err := tl.timing.add(LineInTime, &r, 0); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tl.reasoning, []int{40}) || !slices.Equal(tl.tokens, []int{300}) {
		t.Fatalf("tokens %v, reasoning %v", tl.tokens, tl.reasoning)
	}
}
