package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// LoadFile reads, strictly parses, and validates a run configuration.
// Unknown fields are errors: a typo in a pin name must never silently
// drop the pin.
func LoadFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	cfg, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return cfg, nil
}

// Parse strictly decodes and validates a run configuration.
func Parse(raw []byte) (*Config, error) {
	cfg, err := Decode(raw)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Decode strictly decodes a run configuration without validating it.
func Decode(raw []byte) (*Config, error) {
	var cfg Config
	if err := decodeOne(raw, &cfg, true); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	cfg.Raw = raw
	return &cfg, nil
}

// errOneDocument is the refusal of a file of more than one YAML document
// or with text after its document-end marker.
var errOneDocument = errors.New("one YAML document required")

// coreTags are the tags yaml.v3 gives a node written without one.
var coreTags = []string{"!!null", "!!bool", "!!int", "!!float", "!!str", "!!timestamp", "!!merge", "!!seq", "!!map"}

// decodeOne decodes raw into v and refuses a YAML document after the
// first, any text but whitespace after a document-end marker, a file not
// in UTF-8, a YAML directive and a tag outside coreTags; with known set,
// a field v does not declare is an error.
func decodeOne(raw []byte, v any, known bool) error {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(known)
	if err := dec.Decode(v); err != nil {
		return err
	}
	var extra yaml.Node
	switch err := dec.Decode(&extra); {
	case err == nil:
		return errOneDocument
	case !errors.Is(err, io.EOF):
		return fmt.Errorf("%w: %w", errOneDocument, err)
	}
	if !utf8.Valid(raw) {
		return errors.New("the file is not UTF-8")
	}
	if tail, ok := afterEnd(raw); ok && !blank(tail) {
		return fmt.Errorf("%w: text after the document-end marker", errOneDocument)
	}
	if directive(raw) {
		return errors.New("a YAML directive (a line starting with %) is not accepted")
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return err
	}
	var tagged *yaml.Node
	walk(&doc, func(n *yaml.Node) {
		if tagged == nil && n.Kind != yaml.DocumentNode && n.Kind != yaml.AliasNode && !slices.Contains(coreTags, n.Tag) {
			tagged = n
		}
	})
	if tagged != nil {
		return fmt.Errorf("line %d, column %d: a YAML tag outside the core types (%s) is not accepted", tagged.Line, tagged.Column, strings.Join(coreTags, ", "))
	}
	return nil
}

// directive reports whether a line of raw starts with "%".
func directive(raw []byte) bool {
	raw = bytes.TrimPrefix(raw, []byte("\ufeff"))
	for more := true; more; raw, more = nextLine(raw) {
		if bytes.HasPrefix(raw, []byte("%")) {
			return true
		}
	}
	return false
}

// afterEnd returns the bytes after raw's first document-end marker: "..."
// at the start of a line, followed by a space, a tab, a line break or the
// end of raw.
func afterEnd(raw []byte) ([]byte, bool) {
	raw = bytes.TrimPrefix(raw, []byte("\ufeff"))
	for more := true; more; raw, more = nextLine(raw) {
		if rest, ok := bytes.CutPrefix(raw, []byte("...")); ok &&
			(len(rest) == 0 || rest[0] == ' ' || rest[0] == '\t' || lineBreak(rest) > 0) {
			return rest, true
		}
	}
	return nil, false
}

// nextLine is raw after its first line break, or false without one.
func nextLine(raw []byte) ([]byte, bool) {
	for i := range raw {
		if n := lineBreak(raw[i:]); n > 0 {
			return raw[i+n:], true
		}
	}
	return nil, false
}

// blank reports whether b holds only spaces, tabs and line breaks.
func blank(b []byte) bool {
	for len(b) > 0 {
		switch n := lineBreak(b); {
		case n > 0:
			b = b[n:]
		case b[0] == ' ' || b[0] == '\t':
			b = b[1:]
		default:
			return false
		}
	}
	return true
}

// lineBreak is the length of the YAML line break b starts with, or 0.
func lineBreak(b []byte) int {
	for _, br := range []string{"\r\n", "\r", "\n", "\u0085", "\u2028", "\u2029"} {
		if bytes.HasPrefix(b, []byte(br)) {
			return len(br)
		}
	}
	return 0
}
