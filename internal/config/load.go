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

// errOneDocument is the refusal of a file of more than one YAML document.
var errOneDocument = errors.New("one YAML document required")

// decodeOne decodes raw into v and refuses a YAML document after the
// first; with known set, a field v does not declare is an error.
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
	return nil
}
