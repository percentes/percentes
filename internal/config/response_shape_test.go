package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestResponseShapeParsesAndDefaults(t *testing.T) {
	raw, err := os.ReadFile(acRef)
	if err != nil {
		t.Fatal(err)
	}
	withShape := strings.Replace(string(raw), "target:\n", "target:\n  response_shape:\n    reasoning: [reasoning]\n    usage: [usage, x_groq.usage]\n    refusal: [choices.0.delta.refusal]\n", 1)
	c, err := Parse([]byte(withShape))
	if err != nil {
		t.Fatalf("config with a response shape must load: %v", err)
	}
	want := ResponseShape{Content: "content", Reasoning: []string{"reasoning"}, Model: "model", Usage: []string{"usage", "x_groq.usage"}, Error: []string{"error"}, Refusal: []string{"choices.0.delta.refusal"}}
	if got := c.Target.Shape(); !reflect.DeepEqual(got, want) {
		t.Fatalf("shape %+v, want %+v", got, want)
	}

	unset := loadRef(t, acRef)
	want = ResponseShape{Content: "content", Model: "model", Usage: []string{"usage"}, Error: []string{"error"}}
	if got := unset.Target.Shape(); !reflect.DeepEqual(got, want) {
		t.Fatalf("default shape %+v, want %+v", got, want)
	}

	bad := strings.Replace(string(raw), "target:\n", "target:\n  response_shape:\n    reasonning: [reasoning]\n", 1)
	if _, err := Parse([]byte(bad)); err == nil {
		t.Fatal("a misspelt response_shape key must be rejected")
	}
}

func TestResponseShapeValidation(t *testing.T) {
	c := loadRef(t, acRef)
	c.Target.ResponseShape = &ResponseShape{
		Content:   "text",
		Reasoning: []string{"reasoning", "reasoning_content"},
		Model:     "x_groq.model",
		Usage:     []string{"usage", "x_groq.usage", "choices.0.usage"},
		Error:     []string{"error", "x_groq.error"},
		Refusal:   []string{"choices.0.delta.refusal", "refusal"},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("response shape must validate: %v", err)
	}
	for name, s := range map[string]ResponseShape{
		"content with a dot":          {Content: "delta.content"},
		"content with a space":        {Content: "the content"},
		"reasoning equal to content":  {Reasoning: []string{"content"}},
		"reasoning equal to override": {Content: "text", Reasoning: []string{"text"}},
		"reasoning repeated":          {Reasoning: []string{"reasoning", "reasoning"}},
		"reasoning empty":             {Reasoning: []string{""}},
		"model empty segment":         {Model: "x_groq..model"},
		"usage trailing dot":          {Usage: []string{"usage."}},
		"usage repeated":              {Usage: []string{"usage", "usage"}},
		"error empty":                 {Error: []string{""}},
		"error with a slash":          {Error: []string{"x/error"}},
		"refusal empty segment":       {Refusal: []string{"choices.0..refusal"}},
		"refusal repeated":            {Refusal: []string{"refusal", "refusal"}},
	} {
		s := s
		c.Target.ResponseShape = &s
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "target.response_shape") {
			t.Errorf("%s: must fail on target.response_shape, got: %v", name, err)
		}
	}
}
