package probe

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/redact"
	"github.com/percentes/percentes/internal/sse"
)

// Shape is a 200 stream's form, read without assuming the target's
// configured response shape, except where a field says so. Every name and
// value taken from the stream has the key masked. On a Messages stream
// the delta keys are those of content_block_delta and message_delta, the
// finish reasons are stop_reason values, and the configured content,
// reasoning and error fields are the adapter's own.
type Shape struct {
	// Events counts data events before the terminator ([DONE], or
	// message_stop on a Messages stream); Done says it arrived.
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
	// EventTypes counts a Messages stream's events by type; DeltaTypes
	// counts its content_block_delta delta types and content_block_start
	// block types, the latter prefixed "block:".
	EventTypes map[string]int `json:"event_types,omitempty"`
	DeltaTypes map[string]int `json:"delta_types,omitempty"`
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
	// Refusals lists each path at which a non-null value keyed "refusal"
	// appeared.
	Refusals []string `json:"refusals,omitempty"`
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

	// finish is the last finish reason of the first choice, or the last
	// stop_reason; usage is the last usage object at a configured path, or
	// a Messages stream's usage objects merged in order. Neither is masked.
	finish string
	usage  map[string]json.RawMessage
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

// readShape reads r's events to the adapter's terminator or the
// stream's end; timeout is the exchange's, for the error text.
func readShape(r io.Reader, adapter string, sh config.ResponseShape, m masker, since func() int64, timeout time.Duration) *Shape {
	s := &Shape{TopLevelKeys: map[string]int{}, DeltaKeys: map[string]*DeltaKey{}, FinishReasons: []string{}, Models: []string{}, Usage: []UsageAt{}}
	messages := adapter == config.AdapterMessages
	if messages {
		s.EventTypes, s.DeltaTypes = map[string]int{}, map[string]int{}
	}
	_, dropped, err := sse.Events(r, eventLimit, func(payload []byte) bool {
		if messages {
			return s.messagesEvent(payload, m, since)
		}
		if string(bytes.TrimSpace(payload)) == "[DONE]" {
			s.Done = true
			return true
		}
		s.event(payload, sh, m, since)
		return false
	})
	s.DroppedEvents = dropped
	if err != nil {
		s.ReadError = redact.ErrorText(err, timeout)
	}
	return s
}

// root decodes one event as a JSON object and counts its top-level keys
// and refusal paths; nil when it is not an object.
func (s *Shape) root(payload []byte, m masker) map[string]json.RawMessage {
	s.Events++
	var root map[string]json.RawMessage
	if json.Unmarshal(payload, &root) != nil || root == nil {
		s.Undecodable++
		return nil
	}
	for k := range root {
		k = m.str(k)
		if _, ok := s.TopLevelKeys[k]; !ok && len(s.TopLevelKeys) >= distinctLimit {
			s.DroppedDistinct++
			continue
		}
		s.TopLevelKeys[k]++
	}
	found := keyPaths(root, "refusal", "", usageSearchDepth, func(v json.RawMessage) bool { return jsonType(v) != "null" })
	for _, p := range sortedPaths(found) {
		s.Refusals = s.distinct(s.Refusals, m.str(p))
	}
	return root
}

func (s *Shape) event(payload []byte, sh config.ResponseShape, m masker, since func() int64) {
	idx := s.Events
	root := s.root(payload, m)
	if root == nil {
		return
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
		if finish != "" {
			s.finish = finish
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
	s.usages(root, idx, len(choices) == 0, m)
	for _, p := range sh.Usage {
		if u, ok := object(lookup(root, p)); ok && u != nil {
			s.usage = u
		}
	}
	for _, p := range sh.Error {
		if v := lookup(root, p); v != nil {
			s.inBandError(v, m)
			break
		}
	}
}

// messagesEvent reads one Messages event by its data's type and reports
// whether it is the terminator.
func (s *Shape) messagesEvent(payload []byte, m masker, since func() int64) bool {
	var head map[string]json.RawMessage
	if json.Unmarshal(payload, &head) == nil && text(head["type"]) == "message_stop" {
		s.Done = true
		return true
	}
	idx := s.Events
	root := s.root(payload, m)
	if root == nil {
		return false
	}
	typ := text(root["type"])
	s.count(s.EventTypes, m.str(typ))
	switch typ {
	case "message_start":
		msg, _ := object(root["message"])
		if mdl := text(msg["model"]); mdl != "" {
			s.Models = s.distinct(s.Models, m.str(mdl))
		}
	case "content_block_start":
		b, _ := object(root["content_block"])
		s.count(s.DeltaTypes, "block:"+m.str(text(b["type"])))
	case "content_block_delta", "message_delta":
		d, _ := object(root["delta"])
		for k, v := range d {
			s.deltaKey(m.str(k), v)
		}
		if typ == "message_delta" {
			if stop := text(d["stop_reason"]); stop != "" {
				s.FinishReasons = s.distinct(s.FinishReasons, m.str(stop))
				s.finish = stop
			}
			break
		}
		dt := text(d["type"])
		s.count(s.DeltaTypes, m.str(dt))
		switch {
		case dt == "text_delta" && text(d["text"]) != "":
			s.ContentEvents++
			if s.FirstContentNs == 0 {
				s.FirstContentNs = since()
			}
		case dt == "thinking_delta" && text(d["thinking"]) != "":
			s.ReasoningEvents++
			if s.FirstReasoningNs == 0 {
				s.FirstReasoningNs = since()
			}
		}
	case "error":
		v := root["error"]
		if jsonType(v) == "null" {
			v = payload
		}
		s.inBandError(v, m)
	}
	s.usages(root, idx, false, m)
	for _, p := range []string{"message.usage", "usage"} {
		if u, ok := object(lookup(root, p)); ok && u != nil {
			if s.usage == nil {
				s.usage = map[string]json.RawMessage{}
			}
			for k, v := range u {
				if jsonType(v) != "null" {
					s.usage[k] = v
				}
			}
		}
	}
	return false
}

// usages records every usage object in one event.
func (s *Shape) usages(root map[string]json.RawMessage, idx int, emptyChoices bool, m masker) {
	found := keyPaths(root, "usage", "", usageSearchDepth, func(v json.RawMessage) bool { return jsonType(v) == "object" })
	for _, p := range sortedPaths(found) {
		s.usageAt(m.str(p), idx, emptyChoices)
		s.LastUsage = m.str(compact(found[p]))
	}
}

// inBandError keeps the first in-band error object, compacted and cut at
// errorBodyLimit bytes.
func (s *Shape) inBandError(v json.RawMessage, m masker) {
	if s.Error != "" {
		return
	}
	e := m.str(compact(v))
	if len(e) > errorBodyLimit {
		e = e[:errorBodyLimit]
	}
	s.Error = e
}

func (s *Shape) count(c map[string]int, k string) {
	if _, ok := c[k]; !ok && len(c) >= distinctLimit {
		s.DroppedDistinct++
		return
	}
	c[k]++
}

// tokens returns the integer at key in the stream's usage; it is false
// when the key is missing or null.
func (s *Shape) tokens(key string) (int, bool) {
	var n *int
	if v, ok := s.usage[key]; !ok || json.Unmarshal(v, &n) != nil || n == nil {
		return 0, false
	}
	return *n, true
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

func (s *Shape) usageAt(path string, idx int, emptyChoices bool) {
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

// keyPaths returns every value under a key named name that want
// accepts, searching objects and arrays to depth levels below v.
func keyPaths(v map[string]json.RawMessage, name, prefix string, depth int, want func(json.RawMessage) bool) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, raw := range v {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		if k == name && want(raw) {
			out[path] = raw
		}
		if depth > 1 {
			for p, u := range nested(raw, name, path, depth-1, want) {
				out[p] = u
			}
		}
	}
	return out
}

func nested(raw json.RawMessage, name, path string, depth int, want func(json.RawMessage) bool) map[string]json.RawMessage {
	switch jsonType(raw) {
	case "object":
		var m map[string]json.RawMessage
		if json.Unmarshal(raw, &m) == nil {
			return keyPaths(m, name, path, depth, want)
		}
	case "array":
		var a []json.RawMessage
		if json.Unmarshal(raw, &a) == nil {
			out := map[string]json.RawMessage{}
			for i, e := range a {
				for p, u := range nested(e, name, path+"."+strconv.Itoa(i), depth, want) {
					out[p] = u
				}
			}
			return out
		}
	}
	return nil
}

func sortedPaths(m map[string]json.RawMessage) []string {
	paths := make([]string, 0, len(m))
	for p := range m {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

// object decodes a JSON object; ok is false for any other type, and m is
// nil for null or a missing value.
func object(raw json.RawMessage) (m map[string]json.RawMessage, ok bool) {
	return m, json.Unmarshal(orNull(raw), &m) == nil
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
