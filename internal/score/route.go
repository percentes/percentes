// Package score applies the task's output contract to a visible answer.
// The contract is exactly one JSON object whose one key is "route" and whose
// value is a string in the configured label set, with JSON whitespace
// (space, tab, line feed, carriage return) allowed around it. Anything else
// is a schema failure with one named reason. Keys and labels compare as
// exact strings after JSON unescaping: "Route" is an extra key and
// "Billing" an unknown label.
package score

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Reason names why an answer failed the contract.
type Reason string

// Failure reasons, in the order Parse checks them; the first that applies
// is reported.
const (
	InvalidUTF8  Reason = "invalid_utf8"
	Empty        Reason = "empty"
	CodeFence    Reason = "code_fence"
	NotObject    Reason = "not_object"
	InvalidJSON  Reason = "invalid_json"
	TrailingText Reason = "trailing_text"
	DuplicateKey Reason = "duplicate_key"
	ExtraKey     Reason = "extra_key"
	MissingRoute Reason = "missing_route"
	WrongType    Reason = "wrong_type"
	UnknownLabel Reason = "unknown_label"
)

// routeKey is the contract's one key.
const routeKey = "route"

// jsonSpace is the whitespace RFC 8259 allows between tokens.
const jsonSpace = " \t\n\r"

// Parsed is the contract's verdict on one answer. Reason is empty exactly
// when Route holds a configured label.
type Parsed struct {
	Route  string
	Reason Reason
}

// OK reports whether the answer met the contract.
func (p Parsed) OK() bool { return p.Reason == "" }

// Contract holds the configured label set.
type Contract struct {
	labels map[string]struct{}
}

// NewContract returns a contract over labels, which must be non-empty,
// distinct, valid UTF-8 strings.
func NewContract(labels []string) (*Contract, error) {
	if len(labels) == 0 {
		return nil, errors.New("score: no labels")
	}
	set := make(map[string]struct{}, len(labels))
	for _, l := range labels {
		switch {
		case l == "":
			return nil, errors.New("score: empty label")
		case !utf8.ValidString(l):
			return nil, fmt.Errorf("score: label %q is not valid UTF-8", l)
		}
		if _, dup := set[l]; dup {
			return nil, fmt.Errorf("score: duplicate label %q", l)
		}
		set[l] = struct{}{}
	}
	return &Contract{labels: set}, nil
}

// Parse applies the contract to answer. The object is walked token by token
// because encoding/json.Unmarshal keeps the last of duplicate keys without
// an error.
func (c *Contract) Parse(answer string) Parsed {
	if !utf8.ValidString(answer) {
		return Parsed{Reason: InvalidUTF8}
	}
	body := strings.Trim(answer, jsonSpace)
	switch {
	case body == "":
		return Parsed{Reason: Empty}
	case strings.HasPrefix(body, "```"):
		return Parsed{Reason: CodeFence}
	case body[0] != '{':
		return Parsed{Reason: NotObject}
	}

	dec := json.NewDecoder(strings.NewReader(body))
	if _, err := dec.Token(); err != nil {
		return Parsed{Reason: InvalidJSON}
	}
	seen := make(map[string]bool)
	var dup, extra bool
	var value json.RawMessage
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return Parsed{Reason: InvalidJSON}
		}
		key, ok := tok.(string)
		if !ok {
			return Parsed{Reason: InvalidJSON}
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return Parsed{Reason: InvalidJSON}
		}
		switch {
		case seen[key]:
			dup = true
		case key != routeKey:
			extra = true
		default:
			value = raw
		}
		seen[key] = true
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return Parsed{Reason: InvalidJSON}
	}
	if strings.Trim(body[dec.InputOffset():], jsonSpace) != "" {
		return Parsed{Reason: TrailingText}
	}

	switch {
	case dup:
		return Parsed{Reason: DuplicateKey}
	case extra:
		return Parsed{Reason: ExtraKey}
	case value == nil:
		return Parsed{Reason: MissingRoute}
	}
	var v interface{}
	if err := json.Unmarshal(value, &v); err != nil {
		return Parsed{Reason: InvalidJSON}
	}
	route, ok := v.(string)
	if !ok {
		return Parsed{Reason: WrongType}
	}
	if _, ok := c.labels[route]; !ok {
		return Parsed{Reason: UnknownLabel}
	}
	return Parsed{Route: route}
}
