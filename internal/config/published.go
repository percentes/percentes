package config

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// RetainedLimitValue is the YAML scalar that stands in a published
// configuration for the value of a limit marked MarkingConfidential.
const RetainedLimitValue = `"` + RetainedHeaderValue + `"`

// Confidential reports whether t records a limit marked
// MarkingConfidential.
func (t *Target) Confidential() bool {
	return slices.ContainsFunc(t.Limits, func(l Limit) bool { return l.Marking == MarkingConfidential })
}

// Published returns the configuration file as published: Raw with the
// value of each limit marked MarkingConfidential replaced by
// RetainedLimitValue, every other byte as written. Without such a limit
// it is Raw. It refuses a confidential value that is not a plain scalar
// written in target.limits itself, a comment inside that limit, and a
// comment anywhere in the file that carries the value's text.
func (c *Config) Published() ([]byte, error) {
	if !c.Target.Confidential() {
		return c.Raw, nil
	}
	fail := func(format string, a ...any) ([]byte, error) {
		return nil, fmt.Errorf("config: published: "+format, a...)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(c.Raw, &doc); err != nil {
		return fail("%v", err)
	}
	var root *yaml.Node
	if doc.Kind == yaml.DocumentNode && len(doc.Content) == 1 {
		root = doc.Content[0]
	}
	limits := mapValue(mapValue(root, "target"), "limits")
	if limits == nil || limits.Kind != yaml.SequenceNode || len(limits.Content) != len(c.Target.Limits) {
		return fail("target.limits is not a sequence the file writes out under target")
	}
	lines := bytes.SplitAfter(c.Raw, []byte("\n"))
	starts := make([]int, len(lines))
	for i := 1; i < len(lines); i++ {
		starts[i] = starts[i-1] + len(lines[i-1])
	}
	var comments []string
	walk(&doc, func(n *yaml.Node) { comments = append(comments, n.HeadComment, n.LineComment, n.FootComment) })

	type span struct{ from, to int }
	var edits []span
	for i, l := range c.Target.Limits {
		if l.Marking != MarkingConfidential {
			continue
		}
		field := fmt.Sprintf("target.limits[%d]", i)
		item := limits.Content[i]
		commented := false
		walk(item, func(n *yaml.Node) {
			commented = commented || n.HeadComment != "" || n.LineComment != "" || n.FootComment != ""
		})
		if commented {
			return fail("%s: a confidential limit carries no comment", field)
		}
		v := mapValue(item, "value")
		var f float64
		if v == nil || v.Kind != yaml.ScalarNode || v.Style != 0 || v.Anchor != "" || v.Decode(&f) != nil || f != l.Value {
			return fail("%s.value: a confidential value is a plain number written in the limit", field)
		}
		at := -1
		if k := v.Line - 1; k >= 0 && k < len(lines) {
			at = runeOffset(lines[k], v.Column-1)
			if at >= 0 {
				at += starts[k]
			}
		}
		if at < 0 || !bytes.HasPrefix(c.Raw[at:], []byte(v.Value)) {
			return fail("%s.value: not found at line %d, column %d", field, v.Line, v.Column)
		}
		word := regexp.MustCompile(`(^|[^0-9.])` + regexp.QuoteMeta(v.Value) + `($|[^0-9.])`)
		for _, cm := range comments {
			if word.MatchString(cm) {
				return fail("%s.value: a comment in the file carries %s", field, v.Value)
			}
		}
		edits = append(edits, span{at, at + len(v.Value)})
	}
	slices.SortFunc(edits, func(a, b span) int { return b.from - a.from })
	out := slices.Clone(c.Raw)
	for _, e := range edits {
		out = slices.Replace(out, e.from, e.to, []byte(RetainedLimitValue)...)
	}

	var want, got any
	if err := yaml.Unmarshal(c.Raw, &want); err != nil {
		return fail("%v", err)
	}
	if err := yaml.Unmarshal(out, &got); err != nil {
		return fail("%v", err)
	}
	items, _ := mapAt(mapAt(want, "target"), "limits").([]any)
	for i, l := range c.Target.Limits {
		if m, ok := itemAt(items, i).(map[string]any); ok && l.Marking == MarkingConfidential {
			m["value"] = RetainedHeaderValue
		}
	}
	if !reflect.DeepEqual(want, got) {
		return nil, errors.New("config: published: the file as published differs from the file beyond the confidential values")
	}
	return out, nil
}

// mapValue is the value node of key in mapping n, or nil.
func mapValue(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if k := n.Content[i]; k.Kind == yaml.ScalarNode && k.Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

func walk(n *yaml.Node, f func(*yaml.Node)) {
	f(n)
	for _, c := range n.Content {
		walk(c, f)
	}
}

// runeOffset is the byte offset of rune col in line, or -1.
func runeOffset(line []byte, col int) int {
	at := 0
	for ; col > 0; col-- {
		if at >= len(line) {
			return -1
		}
		_, size := utf8.DecodeRune(line[at:])
		at += size
	}
	return at
}

func mapAt(v any, key string) any {
	m, _ := v.(map[string]any)
	return m[key]
}

func itemAt(items []any, i int) any {
	if i < len(items) {
		return items[i]
	}
	return nil
}
