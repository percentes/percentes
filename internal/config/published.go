package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
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

// RetainedError stands in the attempts file and the probe records for an
// in-band error object's text on a target with a confidential limit: the
// object with its "type" string alone, {} for an object without one, and
// RetainedHeaderValue for text that is not a JSON object. A stand-in is
// its own stand-in.
func RetainedError(text string) string {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &obj) != nil || obj == nil {
		return RetainedHeaderValue
	}
	var typ string
	if json.Unmarshal(obj["type"], &typ) != nil || typ == "" {
		return "{}"
	}
	b, _ := json.Marshal(map[string]string{"type": typ})
	return string(b)
}

// Published returns the configuration file as published: Raw with the
// value of each limit marked MarkingConfidential replaced by
// RetainedLimitValue, every other byte as written. Without such a limit
// it is Raw. It refuses a confidential value that is not a plain scalar
// written in target.limits itself, a comment inside that limit, and a
// comment or string scalar anywhere in the file that carries one of the
// value's forms (valueForms) outside a hex run, a date or the text of an
// identifier the provider fixes (FixedIdentifiers), and skips the fields
// that hold those identifiers (fixedNodes). It refuses a file of more than
// one YAML document or with text after its document-end marker, and a
// file with a YAML directive or a tag outside coreTags.
func (c *Config) Published() ([]byte, error) {
	if !c.Target.Confidential() {
		return c.Raw, nil
	}
	fail := func(format string, a ...any) ([]byte, error) {
		return nil, fmt.Errorf("config: published: "+format, a...)
	}
	var doc yaml.Node
	if err := decodeOne(c.Raw, &doc, false); err != nil {
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
	fixed, ids := fixedNodes(root), FixedIdentifiers(&c.Target, c.Release)
	var comments, strs []string
	walk(&doc, func(n *yaml.Node) {
		comments = append(comments, n.HeadComment, n.LineComment, n.FootComment)
		if n.Kind == yaml.ScalarNode && n.ShortTag() == "!!str" && !slices.Contains(fixed, n) {
			strs = append(strs, n.Value)
		}
	})

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
		forms := valueForms(v.Value, l.Value)
		for _, cm := range comments {
			if form, ok := CarriesOutside(cm, forms, ids); ok {
				return fail("%s.value: a comment in the file carries %s", field, form)
			}
		}
		for _, s := range strs {
			if form, ok := CarriesOutside(s, forms, ids); ok {
				return fail("%s.value: a string in the file carries %s", field, form)
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
	if err := decodeOne(c.Raw, &want, false); err != nil {
		return fail("%v", err)
	}
	if err := decodeOne(out, &got, false); err != nil {
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

// fixedNodes are the scalars of root, the file's top-level mapping, that
// hold an identifier the provider fixes and the route publishes:
// target.model_name, target.reported_model.name, target.endpoint_url and
// each release.other_hosts[].model.
func fixedNodes(root *yaml.Node) []*yaml.Node {
	target := mapValue(root, "target")
	fixed := []*yaml.Node{mapValue(target, "model_name"), mapValue(mapValue(target, "reported_model"), "name"), mapValue(target, "endpoint_url")}
	if hosts := mapValue(mapValue(root, "release"), "other_hosts"); hosts != nil && hosts.Kind == yaml.SequenceNode {
		for _, h := range hosts.Content {
			fixed = append(fixed, mapValue(h, "model"))
		}
	}
	return slices.DeleteFunc(fixed, func(n *yaml.Node) bool { return n == nil || n.Kind != yaml.ScalarNode })
}

// FixedIdentifiers are the texts of the identifiers the provider fixes
// and a route of t in release r publishes: t's endpoint_url, model_name
// and reported_model name and the model of each of r's other_hosts, each
// model also without its provider prefix (the text through its last /).
// Only texts holding a letter other than e and E, which no value form
// holds, are kept. r may be nil.
func FixedIdentifiers(t *Target, r *Release) []string {
	models := []string{t.ModelName}
	if m := t.ReportedModel; m != nil {
		models = append(models, m.Name)
	}
	if r != nil {
		for _, h := range r.OtherHosts {
			models = append(models, h.Model)
		}
	}
	ids := []string{t.EndpointURL}
	for _, m := range models {
		ids = append(ids, m)
		if i := strings.LastIndex(m, "/"); i >= 0 {
			ids = append(ids, m[i+1:])
		}
	}
	ids = slices.DeleteFunc(ids, func(id string) bool {
		return !strings.ContainsFunc(id, func(c rune) bool { return unicode.IsLetter(c) && c != 'e' && c != 'E' })
	})
	slices.Sort(ids)
	return slices.Compact(ids)
}

// thousandsSeparators group the whole part of a number in threes.
var thousandsSeparators = []string{",", " ", "\u00a0", "\u2009", "\u202f"}

// paddedDigits is the most digits in a decimal form with zeros added.
const paddedDigits = 17

// ValueForms are the texts valueForms gives for value written in decimal.
func ValueForms(value float64) []string {
	return valueForms(strconv.FormatFloat(value, 'f', -1, 64), value)
}

// valueForms are the texts that write a value: lit as written, the value
// in decimal and that decimal with zeros added after its last digit up to
// paddedDigits digits in all, each also with its whole part grouped by
// each of thousandsSeparators; each of those with a fractional part also
// with a decimal comma, ungrouped unless that reads as a whole number
// grouped in threes, or grouped by a full stop or by a thousands
// separator other than the comma; and the value as
// strconv.FormatFloat prints it in format 'e' and in format 'E'.
func valueForms(lit string, value float64) []string {
	dec := strconv.FormatFloat(value, 'f', -1, 64)
	bases := []string{lit, dec}
	units, places, _ := strings.Cut(dec, ".")
	for n := len(places) + 1; len(units)+n <= paddedDigits; n++ {
		bases = append(bases, units+"."+places+strings.Repeat("0", n-len(places)))
	}
	forms := []string{strconv.FormatFloat(value, 'e', -1, 64), strconv.FormatFloat(value, 'E', -1, 64)}
	for _, s := range bases {
		forms = append(forms, s)
		whole, frac, dot := strings.Cut(s, ".")
		if strings.Trim(whole, digits) != "" {
			continue
		}
		comma := whole != "" && frac != "" && strings.Trim(frac, digits) == ""
		// A comma before three digits after at most three groups thousands.
		if comma && (len(frac) != 3 || len(whole) > 3) {
			forms = append(forms, whole+","+frac)
		}
		if len(whole) <= 3 {
			continue
		}
		for _, sep := range thousandsSeparators {
			g := grouped(whole, sep)
			if !dot {
				forms = append(forms, g)
				continue
			}
			forms = append(forms, g+"."+frac)
			if comma && sep != "," {
				forms = append(forms, g+","+frac)
			}
		}
		if comma {
			forms = append(forms, grouped(whole, ".")+","+frac)
		}
	}
	slices.Sort(forms)
	return slices.Compact(forms)
}

// digits are the decimal digits.
const digits = "0123456789"

// grouped is the digits whole grouped in threes from the right by sep.
func grouped(whole, sep string) string {
	g := whole[:(len(whole)-1)%3+1]
	for i := len(g); i < len(whole); i += 3 {
		g += sep + whole[i:i+3]
	}
	return g
}

var (
	hexRun  = regexp.MustCompile(`[0-9A-Fa-f]{32,}`)
	dateRun = regexp.MustCompile(`[0-9]{4}-[0-9]{2}-[0-9]{2}`)
)

// Carries returns the first of forms that text holds as a number of its
// own: not within a longer number, a run of 32 or more hex digits or a
// YYYY-MM-DD date.
func Carries(text string, forms []string) (string, bool) {
	return CarriesOutside(text, forms, nil)
}

// CarriesOutside is Carries counting no form that lies wholly within an
// occurrence of one of ids in text; a form that runs on past it counts.
func CarriesOutside(text string, forms, ids []string) (string, bool) {
	var within [][2]int
	for _, id := range ids {
		for at := 0; id != ""; {
			i := strings.Index(text[at:], id)
			if i < 0 {
				break
			}
			within = append(within, [2]int{at + i, at + i + len(id)})
			at += i + 1
		}
	}
	text = hexRun.ReplaceAllStringFunc(text, func(run string) string { return strings.Repeat(" ", len(run)) })
	b := []byte(text)
	for _, m := range dateRun.FindAllStringIndex(text, -1) {
		if _, err := time.Parse(time.DateOnly, text[m[0]:m[1]]); err == nil && !digitAt(text, m[0]-1) && !digitAt(text, m[1]) {
			copy(b[m[0]:m[1]], strings.Repeat(" ", m[1]-m[0]))
		}
	}
	text = string(b)
	for _, f := range forms {
		for at := 0; at < len(text); {
			i := strings.Index(text[at:], f)
			if i < 0 {
				break
			}
			from, to := at+i, at+i+len(f)
			before := digitAt(text, from-1) || from > 0 && (text[from-1] == '.' || text[from-1] == ',' && digitAt(text, from-2))
			after := digitAt(text, to) || to < len(text) && (text[to] == '.' || text[to] == ',') && digitAt(text, to+1)
			if !before && !after && !slices.ContainsFunc(within, func(w [2]int) bool { return w[0] <= from && to <= w[1] }) {
				return f, true
			}
			at = from + 1
		}
	}
	return "", false
}

func digitAt(s string, i int) bool {
	return i >= 0 && i < len(s) && s[i] >= '0' && s[i] <= '9'
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
