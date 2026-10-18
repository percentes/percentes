package score

import (
	"errors"
	"fmt"
	"time"
)

// Class is an attempt's task outcome, recorded beside its transport
// outcome.
type Class string

// Task outcome classes, in report order.
const (
	// Success: a complete correct object, an accepted finish reason and
	// the recognised end of stream, ending no later than the cutoff from
	// intended dispatch.
	Success Class = "success"
	// LateCorrect: as Success, ending after the cutoff.
	LateCorrect Class = "late_correct"
	// WrongLabel: as Success or LateCorrect, with a configured label other
	// than the expected one.
	WrongLabel Class = "wrong_label"
	// SchemaFailure: an accepted finish reason and the end of stream, with
	// an answer that fails the contract.
	SchemaFailure Class = "schema_failure"
	// Refusal: a non-empty refusal field, or a finish reason the rule
	// names as a refusal.
	Refusal Class = "refusal"
	// LengthLimit: a finish reason the rule names as a length limit.
	LengthLimit Class = "length_limit"
	// PartialAnswer: visible content, then no recognised end of stream.
	PartialAnswer Class = "partial_answer"
	// NoAnswer: no visible content and no recognised end of stream.
	NoAnswer Class = "no_answer"
	// UnknownTermination: the recognised end of stream with no finish
	// reason.
	UnknownTermination Class = "unknown_termination"
	// InstrumentIncompatible: a response the client does not understand,
	// as flagged by the caller or shown by a finish reason the rule does
	// not name. Unscorable; never a provider row.
	InstrumentIncompatible Class = "instrument_incompatible"
)

// Classes lists every class in report order.
var Classes = []Class{
	Success, LateCorrect, WrongLabel, SchemaFailure, Refusal, LengthLimit,
	PartialAnswer, NoAnswer, UnknownTermination, InstrumentIncompatible,
}

// Rule is the frozen part of scoring beside the contract. Refusal is
// decided by fields only, never by the answer text.
type Rule struct {
	// Cutoff is the scoring deadline from intended dispatch, inclusive.
	Cutoff time.Duration
	// Accepted lists the finish reasons of an ordinary complete answer;
	// at least one.
	Accepted []string
	// Length and Refusal list the finish reasons classed as a length
	// limit and as a refusal.
	Length  []string
	Refusal []string
}

// Attempt is what the scorer reads from one scheduled request. Times are
// nanosecond offsets on the run's monotonic clock.
type Attempt struct {
	// Expected is the case's frozen label.
	Expected   string
	IntendedNs int64
	// Ended says the stream reached the adapter's recognised end, at
	// EndNs, with no in-band error and no undecodable event.
	Ended bool
	EndNs int64
	// FinishReason is the first choice's last finish reason.
	FinishReason string
	// Answer is the visible content; reasoning text never enters it.
	Answer string
	// Refusal is the value of the target's refusal field.
	Refusal string
	// Incompatible says the client did not understand the response
	// contract.
	Incompatible bool
}

// Verdict is one attempt's task outcome. Route is the parsed label for
// Success, LateCorrect and WrongLabel; Reason is the contract failure for
// SchemaFailure. Late says the stream ended after the cutoff.
type Verdict struct {
	Class  Class  `json:"class"`
	Route  string `json:"route,omitempty"`
	Reason Reason `json:"reason,omitempty"`
	Late   bool   `json:"late,omitempty"`
}

type finishKind int

const (
	finishAccepted finishKind = iota + 1
	finishLength
	finishRefusal
)

// Scorer classifies attempts under one contract and one rule.
type Scorer struct {
	contract *Contract
	cutoffNs int64
	finish   map[string]finishKind
}

// NewScorer returns a scorer for c and r. The cutoff must be positive and
// every finish reason non-empty and named once across the three lists.
func NewScorer(c *Contract, r Rule) (*Scorer, error) {
	if c == nil {
		return nil, errors.New("score: no contract")
	}
	if r.Cutoff <= 0 {
		return nil, fmt.Errorf("score: cutoff %v is not positive", r.Cutoff)
	}
	if len(r.Accepted) == 0 {
		return nil, errors.New("score: no accepted finish reason")
	}
	finish := make(map[string]finishKind)
	for _, set := range []struct {
		names []string
		kind  finishKind
	}{{r.Accepted, finishAccepted}, {r.Length, finishLength}, {r.Refusal, finishRefusal}} {
		for _, n := range set.names {
			if n == "" {
				return nil, errors.New("score: empty finish reason")
			}
			if _, dup := finish[n]; dup {
				return nil, fmt.Errorf("score: finish reason %q named twice", n)
			}
			finish[n] = set.kind
		}
	}
	return &Scorer{contract: c, cutoffNs: int64(r.Cutoff), finish: finish}, nil
}

// Score classifies a. The first rule that applies decides: incompatible,
// no recognised end, refusal, length limit, missing finish reason,
// unnamed finish reason, then the contract and the cutoff. It fails only
// when a.Expected is not a configured label.
func (s *Scorer) Score(a Attempt) (Verdict, error) {
	if _, ok := s.contract.labels[a.Expected]; !ok {
		return Verdict{}, fmt.Errorf("score: expected label %q is not configured", a.Expected)
	}
	late := a.Ended && a.EndNs-a.IntendedNs > s.cutoffNs
	kind, named := s.finish[a.FinishReason]
	switch {
	case a.Incompatible:
		return Verdict{Class: InstrumentIncompatible, Late: late}, nil
	case !a.Ended && a.Answer != "":
		return Verdict{Class: PartialAnswer}, nil
	case !a.Ended:
		return Verdict{Class: NoAnswer}, nil
	case a.Refusal != "" || kind == finishRefusal:
		return Verdict{Class: Refusal, Late: late}, nil
	case kind == finishLength:
		return Verdict{Class: LengthLimit, Late: late}, nil
	case a.FinishReason == "":
		return Verdict{Class: UnknownTermination, Late: late}, nil
	case !named:
		return Verdict{Class: InstrumentIncompatible, Late: late}, nil
	}
	p := s.contract.Parse(a.Answer)
	switch {
	case !p.OK():
		return Verdict{Class: SchemaFailure, Reason: p.Reason, Late: late}, nil
	case p.Route != a.Expected:
		return Verdict{Class: WrongLabel, Route: p.Route, Late: late}, nil
	case late:
		return Verdict{Class: LateCorrect, Route: p.Route, Late: true}, nil
	}
	return Verdict{Class: Success, Route: p.Route}, nil
}

// Tally counts verdicts by class; every class in Classes is present.
func Tally(vs []Verdict) map[Class]int {
	n := make(map[Class]int, len(Classes))
	for _, c := range Classes {
		n[c] = 0
	}
	for _, v := range vs {
		n[v.Class]++
	}
	return n
}
