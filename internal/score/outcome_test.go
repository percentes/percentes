package score

import (
	"testing"
	"time"
)

const sec = int64(time.Second)

var rule = Rule{
	Cutoff:   10 * time.Second,
	Accepted: []string{"stop"},
	Length:   []string{"length"},
	Refusal:  []string{"content_filter"},
}

func scorer(t *testing.T) *Scorer {
	t.Helper()
	s, err := NewScorer(contract(t), rule)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const billing = `{"route":"billing"}`

var outcomeFixtures = []struct {
	name string
	in   Attempt
	want Verdict
}{
	{"correct at the cutoff",
		Attempt{Expected: "billing", Ended: true, EndNs: 10 * sec, FinishReason: "stop", Answer: billing},
		Verdict{Class: Success, Route: "billing"}},
	{"cutoff from intended dispatch",
		Attempt{Expected: "billing", IntendedNs: 5 * sec, Ended: true, EndNs: 15 * sec, FinishReason: "stop", Answer: billing},
		Verdict{Class: Success, Route: "billing"}},
	{"correct early",
		Attempt{Expected: "escalation", Ended: true, EndNs: sec, FinishReason: "stop", Answer: `{"route":"escalation"}`},
		Verdict{Class: Success, Route: "escalation"}},
	{"correct with whitespace before the cutoff",
		Attempt{Expected: "account", Ended: true, EndNs: 10*sec - 1, FinishReason: "stop", Answer: "\n {\"route\":\"account\"} \n"},
		Verdict{Class: Success, Route: "account"}},
	{"correct one nanosecond late",
		Attempt{Expected: "billing", Ended: true, EndNs: 10*sec + 1, FinishReason: "stop", Answer: billing},
		Verdict{Class: LateCorrect, Route: "billing", Late: true}},
	{"correct late from intended dispatch",
		Attempt{Expected: "billing", IntendedNs: 5 * sec, Ended: true, EndNs: 25 * sec, FinishReason: "stop", Answer: billing},
		Verdict{Class: LateCorrect, Route: "billing", Late: true}},
	{"wrong label",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "stop", Answer: `{"route":"technical"}`},
		Verdict{Class: WrongLabel, Route: "technical"}},
	{"wrong label late",
		Attempt{Expected: "billing", Ended: true, EndNs: 12 * sec, FinishReason: "stop", Answer: `{"route":"escalation"}`},
		Verdict{Class: WrongLabel, Route: "escalation", Late: true}},
	{"duplicate key",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "stop", Answer: `{"route":"billing","route":"billing"}`},
		Verdict{Class: SchemaFailure, Reason: DuplicateKey}},
	{"refusal wording without a refusal field",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "stop", Answer: "I cannot help with that request."},
		Verdict{Class: SchemaFailure, Reason: NotObject}},
	{"empty answer with an accepted finish",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "stop"},
		Verdict{Class: SchemaFailure, Reason: Empty}},
	{"cut object with an accepted finish",
		Attempt{Expected: "billing", Ended: true, EndNs: 11 * sec, FinishReason: "stop", Answer: `{"route":"bil`},
		Verdict{Class: SchemaFailure, Reason: InvalidJSON, Late: true}},
	{"refusal field",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "stop", Refusal: "no"},
		Verdict{Class: Refusal}},
	{"refusal finish reason",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "content_filter"},
		Verdict{Class: Refusal}},
	{"refusal field beside a correct answer",
		Attempt{Expected: "billing", Ended: true, EndNs: 12 * sec, FinishReason: "stop", Answer: billing, Refusal: "no"},
		Verdict{Class: Refusal, Late: true}},
	{"length limit mid-object",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "length", Answer: `{"rou`},
		Verdict{Class: LengthLimit}},
	{"length limit with no visible content",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "length"},
		Verdict{Class: LengthLimit}},
	{"length limit after a complete object",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "length", Answer: billing},
		Verdict{Class: LengthLimit}},
	{"partial object, no end of stream",
		Attempt{Expected: "billing", EndNs: 3 * sec, Answer: `{"route":"bill`},
		Verdict{Class: PartialAnswer}},
	{"complete object and finish reason, no end of stream",
		Attempt{Expected: "billing", EndNs: 3 * sec, FinishReason: "stop", Answer: billing},
		Verdict{Class: PartialAnswer}},
	{"no content, no end of stream",
		Attempt{Expected: "billing", EndNs: 3 * sec},
		Verdict{Class: NoAnswer}},
	{"length finish, no content, no end of stream",
		Attempt{Expected: "billing", EndNs: 3 * sec, FinishReason: "length"},
		Verdict{Class: NoAnswer}},
	{"end of stream with no finish reason",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, Answer: billing},
		Verdict{Class: UnknownTermination}},
	{"empty stream with no finish reason",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec},
		Verdict{Class: UnknownTermination}},
	{"flagged incompatible",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "stop", Answer: billing, Incompatible: true},
		Verdict{Class: InstrumentIncompatible}},
	{"finish reason the rule does not name",
		Attempt{Expected: "billing", Ended: true, EndNs: 2 * sec, FinishReason: "tool_calls", Answer: billing},
		Verdict{Class: InstrumentIncompatible}},
	{"flagged incompatible, no end of stream",
		Attempt{Expected: "billing", EndNs: 3 * sec, Answer: billing, Incompatible: true},
		Verdict{Class: InstrumentIncompatible}},
}

var frozenCounts = map[Class]int{
	Success:                4,
	LateCorrect:            2,
	WrongLabel:             2,
	SchemaFailure:          4,
	Refusal:                3,
	LengthLimit:            3,
	PartialAnswer:          2,
	NoAnswer:               2,
	UnknownTermination:     2,
	InstrumentIncompatible: 3,
}

func TestScoreClassifiesEachFixture(t *testing.T) {
	s := scorer(t)
	for _, f := range outcomeFixtures {
		got, err := s.Score(f.in)
		if err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		if got != f.want {
			t.Errorf("%s: got %+v, want %+v", f.name, got, f.want)
		}
	}
}

func TestTallyMatchesFrozenCounts(t *testing.T) {
	s := scorer(t)
	vs := make([]Verdict, 0, len(outcomeFixtures))
	for _, f := range outcomeFixtures {
		v, err := s.Score(f.in)
		if err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		vs = append(vs, v)
	}
	got := Tally(vs)
	if len(got) != len(Classes) || len(frozenCounts) != len(Classes) {
		t.Fatalf("tally has %d classes, frozen counts %d, want %d", len(got), len(frozenCounts), len(Classes))
	}
	total := 0
	for _, c := range Classes {
		total += got[c]
		if got[c] != frozenCounts[c] {
			t.Errorf("%s: got %d, want %d", c, got[c], frozenCounts[c])
		}
	}
	if total != len(outcomeFixtures) {
		t.Errorf("tally total %d, want %d", total, len(outcomeFixtures))
	}
}

func TestScoreRefusesAnUnconfiguredExpectedLabel(t *testing.T) {
	s := scorer(t)
	for _, want := range []string{"", "refund", "Billing"} {
		if _, err := s.Score(Attempt{Expected: want, Ended: true, FinishReason: "stop", Answer: billing}); err == nil {
			t.Errorf("Score with expected %q accepted", want)
		}
	}
}

func TestNewScorerRefusesBadRules(t *testing.T) {
	c := contract(t)
	if _, err := NewScorer(nil, rule); err == nil {
		t.Error("nil contract accepted")
	}
	for name, r := range map[string]Rule{
		"zero cutoff":       {Accepted: []string{"stop"}},
		"negative cutoff":   {Cutoff: -time.Second, Accepted: []string{"stop"}},
		"no accepted":       {Cutoff: time.Second, Length: []string{"length"}},
		"empty name":        {Cutoff: time.Second, Accepted: []string{"stop", ""}},
		"repeated in a set": {Cutoff: time.Second, Accepted: []string{"stop", "stop"}},
		"in two sets":       {Cutoff: time.Second, Accepted: []string{"stop"}, Refusal: []string{"stop"}},
		"length as refusal": {Cutoff: time.Second, Accepted: []string{"stop"}, Length: []string{"length"}, Refusal: []string{"length"}},
	} {
		if _, err := NewScorer(c, r); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
