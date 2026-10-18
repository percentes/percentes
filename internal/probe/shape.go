package probe

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/redact"
	"github.com/percentes/percentes/internal/sse"
)

// Shape is a 200 stream's form, read without assuming the target's
// configured response shape, except where a field says so. Every name and
// value taken from the stream has the key masked.
type Shape struct {
	// Events counts data events before [DONE]; Done says [DONE] arrived.
	Events int  `json:"events"`
	Done   bool `json:"done"`
	// Undecodable counts events that are not a JSON object.
	Undecodable int `json:"undecodable,omitempty"`
	// ReadError is the redacted error that ended the stream early.
	ReadError     string `json:"read_error,omitempty"`
	DroppedEvents int    `json:"dropped_events,omitempty"`
	// TopLevelKeys counts the events carrying each top-level key.
	TopLevelKeys map[string]int `json:"top_level_keys"`
	// DeltaKeys describes each key seen in any choice's delta.
	DeltaKeys map[string]*DeltaKey `json:"delta_keys"`
	// FinishReasons are the distinct non-null finish_reason values of any
	// choice, in arrival order.
	FinishReasons []string `json:"finish_reasons"`
	// Models are the distinct values at the configured model path;
	// SystemFingerprints the distinct top-level system_fingerprint values.
	Models             []string `json:"models"`
	SystemFingerprints []string `json:"system_fingerprints,omitempty"`
	// Usage lists each path at which an object keyed "usage" appeared.
	Usage []UsageAt `json:"usage"`
	// LastUsage is the last usage object, compacted.
	LastUsage string `json:"last_usage,omitempty"`
	// Error is the first in-band error object at a configured error path,
	// compacted and cut at errorBodyLimit bytes.
	Error string `json:"error,omitempty"`
	// ContentEvents and ReasoningEvents count events whose first choice
	// carries text in the configured content key, or in a configured
	// reasoning key; the first such event's arrival is in Ns.
	ContentEvents    int   `json:"content_events"`
	FirstContentNs   int64 `json:"first_content_ns,omitempty"`
	ReasoningEvents  int   `json:"reasoning_events"`
	FirstReasoningNs int64 `json:"first_reasoning_ns,omitempty"`
	// DroppedDistinct counts distinct keys and values past distinctLimit,
	// not kept.
	DroppedDistinct int `json:"dropped_distinct,omitempty"`
}

// DeltaKey is one delta key: the events carrying a non-null value under
// it, the JSON types seen, and the bytes of its string values.
type DeltaKey struct {
	Events      int      `json:"events"`
	Types       []string `json:"types"`
	StringBytes int      `json:"string_bytes"`
}

// UsageAt is one usage path: the events carrying an object there, the
// first and last of them by event index, and how many of them had no
// choice.
type UsageAt struct {
	Path         string `json:"path"`
	Events       int    `json:"events"`
	FirstEvent   int    `json:"first_event"`
	LastEvent    int    `json:"last_event"`
	EmptyChoices int    `json:"empty_choices"`
}

// usageSearchDepth bounds the object nesting searched for a usage key.
const usageSearchDepth = 3

// readShape reads r's events to [DONE] or the stream's end.
func readShape(r io.Reader, sh config.ResponseShape, m masker, since func() int64) *Shape {
	s := &Shape{TopLevelKeys: map[string]int{}, DeltaKeys: map[string]*DeltaKey{}, FinishReasons: []string{}, Models: []string{}, Usage: []UsageAt{}}
	_, dropped, err := sse.Events(r, eventLimit, func(payload []byte) bool {
		if string(bytes.TrimSpace(payload)) == "[DONE]" {
			s.Done = true
			return true
		}
		s.event(payload, sh, m, since)
		return false
	})
	s.DroppedEvents = dropped
	if err != nil {
		s.ReadError = redact.ErrorText(err, requestTimeout)
	}
	return s
}

func (s *Shape) event(payload []byte, sh config.ResponseShape, m masker, since func() int64) {
	idx := s.Events
	s.Events++
	var root map[string]json.RawMessage
	if json.Unmarshal(payload, &root) != nil || root == nil {
		s.Undecodable++
		return
	}
	for k := range root {
		k = m.str(k)
		if _, ok := s.TopLevelKeys[k]; !ok && len(s.TopLevelKeys) >= distinctLimit {
			s.DroppedDistinct++
			continue
		}
		s.TopLevelKeys[k]++
	}

	var choices []map[string]json.RawMessage
	json.Unmarshal(orNull(root["choices"]), &choices) //nolint:errcheck
	for i, choice := range choices {
		var delta map[string]json.RawMessage
		json.Unmarshal(orNull(choice["delta"]), &delta) //nolint:errcheck
		for k, v := range delta {
			s.deltaKey(m.str(k), v)
		}
		var finish string
		if json.Unmarshal(orNull(choice["finish_reason"]), &finish) == nil && finish != "" {
			s.FinishReasons = s.distinct(s.FinishReasons, m.str(finish))
		}
		if i > 0 {
			continue
		}
		if text(delta[sh.Content]) != "" {
			s.ContentEvents++
			if s.FirstContentNs == 0 {
				s.FirstContentNs = since()
			}
		}
		for _, k := range sh.Reasoning {
			if text(delta[k]) != "" {
				s.ReasoningEvents++
				if s.FirstReasoningNs == 0 {
					s.FirstReasoningNs = since()
				}
				break
			}
		}
	}

	var model, fingerprint string
	if json.Unmarshal(orNull(lookup(root, sh.Model)), &model) == nil && model != "" {
		s.Models = s.distinct(s.Models, m.str(model))
	}
	if json.Unmarshal(orNull(root["system_fingerprint"]), &fingerprint) == nil && fingerprint != "" {
		s.SystemFingerprints = s.distinct(s.SystemFingerprints, m.str(fingerprint))
	}
	found := usagePaths(root, "", usageSearchDepth)
	paths := make([]string, 0, len(found))
	for p := range found {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		s.usage(m.str(p), idx, len(choices) == 0)
		s.LastUsage = m.str(compact(found[p]))
	}
	if s.Error == "" {
		for _, p := range sh.Error {
			if v := lookup(root, p); v != nil {
				e := m.str(compact(v))
				if len(e) > errorBodyLimit {
					e = e[:errorBodyLimit]
				}
				s.Error = e
				break
			}
		}
	}
}

func (s *Shape) deltaKey(k string, v json.RawMessage) {
	t := jsonType(v)
	if t == "null" {
		return
	}
	d, ok := s.DeltaKeys[k]
	if !ok {
		if len(s.DeltaKeys) >= distinctLimit {
			s.DroppedDistinct++
			return
		}
		d = &DeltaKey{Types: []string{}}
		s.DeltaKeys[k] = d
	}
	d.Events++
	if !slices.Contains(d.Types, t) {
		d.Types = append(d.Types, t)
	}
	d.StringBytes += len(text(v))
}

func (s *Shape) usage(path string, idx int, emptyChoices bool) {
	for i := range s.Usage {
		if u := &s.Usage[i]; u.Path == path {
			u.Events++
			u.LastEvent = idx
			if emptyChoices {
				u.EmptyChoices++
			}
			return
		}
	}
	if len(s.Usage) >= distinctLimit {
		s.DroppedDistinct++
		return
	}
	u := UsageAt{Path: path, Events: 1, FirstEvent: idx, LastEvent: idx}
	if emptyChoices {
		u.EmptyChoices = 1
	}
	s.Usage = append(s.Usage, u)
	sort.Slice(s.Usage, func(i, j int) bool { return s.Usage[i].Path < s.Usage[j].Path })
}

// distinct appends v to list unless present or the list is full.
func (s *Shape) distinct(list []string, v string) []string {
	if slices.Contains(list, v) {
		return list
	}
	if len(list) >= distinctLimit {
		s.DroppedDistinct++
		return list
	}
	return append(list, v)
}

// usagePaths returns every non-null object value under a key named
// "usage", searching objects and arrays to depth levels below v.
func usagePaths(v map[string]json.RawMessage, prefix string, depth int) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, raw := range v {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		if k == "usage" && jsonType(raw) == "object" {
			out[path] = raw
		}
		if depth > 1 {
			for p, u := range nested(raw, path, depth-1) {
				out[p] = u
			}
		}
	}
	return out
}

func nested(raw json.RawMessage, path string, depth int) map[string]json.RawMessage {
	switch jsonType(raw) {
	case "object":
		var m map[string]json.RawMessage
		if json.Unmarshal(raw, &m) == nil {
			return usagePaths(m, path, depth)
		}
	case "array":
		var a []json.RawMessage
		if json.Unmarshal(raw, &a) == nil {
			out := map[string]json.RawMessage{}
			for i, e := range a {
				for p, u := range nested(e, path+"."+strconv.Itoa(i), depth) {
					out[p] = u
				}
			}
			return out
		}
	}
	return nil
}

// lookup returns the non-null value at a configured path, segments joined
// by ".", where a segment of digits indexes an array.
func lookup(root map[string]json.RawMessage, path string) json.RawMessage {
	segs := strings.Split(path, ".")
	v := root[segs[0]]
	for _, seg := range segs[1:] {
		if v == nil {
			return nil
		}
		if i, err := strconv.Atoi(seg); err == nil {
			var a []json.RawMessage
			if json.Unmarshal(v, &a) != nil || i < 0 || i >= len(a) {
				return nil
			}
			v = a[i]
			continue
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(v, &m) != nil {
			return nil
		}
		v = m[seg]
	}
	if jsonType(v) == "null" {
		return nil
	}
	return v
}

// jsonType names a JSON value's type from its first byte; a missing
// value is "null".
func jsonType(v json.RawMessage) string {
	b := bytes.TrimSpace(v)
	if len(b) == 0 {
		return "null"
	}
	switch b[0] {
	case '{':
		return "object"
	case '[':
		return "array"
	case '"':
		return "string"
	case 't', 'f':
		return "boolean"
	case 'n':
		return "null"
	}
	return "number"
}

// text returns a string value, or "" for any other type.
func text(v json.RawMessage) string {
	var s string
	if json.Unmarshal(orNull(v), &s) != nil {
		return ""
	}
	return s
}

func compact(v json.RawMessage) string {
	var b bytes.Buffer
	if json.Compact(&b, v) != nil {
		return string(v)
	}
	return b.String()
}

func orNull(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return json.RawMessage("null")
	}
	return raw
}

func sortedKeys(m map[string]*DeltaKey) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
