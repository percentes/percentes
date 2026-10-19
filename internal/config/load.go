package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

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

// decodeOne decodes raw into v and refuses a YAML document after the
// first and any text but whitespace after a document-end marker; with
// known set, a field v does not declare is an error.
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
	if tail, ok := afterEnd(raw); ok && !blank(tail) {
		return fmt.Errorf("%w: text after the document-end marker", errOneDocument)
	}
	return nil
}

// afterEnd returns the bytes after raw's first document-end marker: "..."
// at the start of a line, followed by a space, a tab, a line break or the
// end of raw.
func afterEnd(raw []byte) ([]byte, bool) {
	raw = bytes.TrimPrefix(raw, []byte("\ufeff"))
	for {
		if rest, ok := bytes.CutPrefix(raw, []byte("...")); ok &&
			(len(rest) == 0 || rest[0] == ' ' || rest[0] == '\t' || lineBreak(rest) > 0) {
			return rest, true
		}
		i := 0
		for i < len(raw) && lineBreak(raw[i:]) == 0 {
			i++
		}
		if i == len(raw) {
			return nil, false
		}
		raw = raw[i+lineBreak(raw[i:]):]
	}
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
