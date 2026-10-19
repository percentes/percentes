package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// public-config writes the file as published: "[retained]" in place of a
// confidential value and every other byte as loaded. It never writes over
// the configuration.
func TestPublicConfigCmd(t *testing.T) {
	cfgPath := routeConfigFile(t, "groq", "https://api.groq.com/openai/v1/chat/completions", "10")
	markConfidential(t, cfgPath)
	loaded, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "groq.yaml")
	if code, err := publicConfigCmd([]string{"--config", cfgPath, "--out", out}); err != nil || code != exitOK {
		t.Fatalf("public-config: %d, %v", code, err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if want := bytes.Replace(loaded, []byte("250013.5"), []byte(`"[retained]"`), 1); !bytes.Equal(b, want) {
		t.Errorf("file as published:\n%s", b)
	}
	for _, args := range [][]string{
		{"--config", cfgPath, "--out", cfgPath},
		{"--config", cfgPath, "--out", filepath.Join(filepath.Dir(cfgPath), ".", filepath.Base(cfgPath))},
		{"--config", cfgPath},
	} {
		if code, err := publicConfigCmd(args); err == nil || code != exitError || !strings.Contains(err.Error(), "public-config: --") {
			t.Errorf("%q: %d, %v", args, code, err)
		}
	}
	if after, err := os.ReadFile(cfgPath); err != nil || !bytes.Equal(after, loaded) {
		t.Errorf("the configuration changed (%v)", err)
	}
}
