package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

func writeEnv(t *testing.T, body string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key.env")
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

// Each target's variable is set from the env files: comments, export
// and one pair of quotes are read; nothing else in the files is set.
func TestLoadKeys(t *testing.T) {
	a := &config.Target{Name: "a", APIKeyEnv: "PILOT_TEST_KEY_A"}
	b := &config.Target{Name: "b", APIKeyEnv: "PILOT_TEST_KEY_B"}
	for _, v := range []string{a.APIKeyEnv, b.APIKeyEnv, "PILOT_TEST_OTHER"} {
		t.Setenv(v, "")
	}
	fa := writeEnv(t, "# key a\n\nexport PILOT_TEST_KEY_A=\"secret-a-123\"\nPILOT_TEST_OTHER=x\n", 0o600)
	fb := writeEnv(t, "PILOT_TEST_KEY_B='secret-b=456'\n", 0o600)
	if err := loadKeys([]string{fa, fb}, []*config.Target{a, b}); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(a.APIKeyEnv) != "secret-a-123" || os.Getenv(b.APIKeyEnv) != "secret-b=456" {
		t.Errorf("keys %q %q", os.Getenv(a.APIKeyEnv), os.Getenv(b.APIKeyEnv))
	}
	if os.Getenv("PILOT_TEST_OTHER") != "" {
		t.Error("a variable no target names was set")
	}
}

// A refused env file is named by path and line; the error never carries
// a line's content.
func TestLoadKeysRefusals(t *testing.T) {
	a := &config.Target{Name: "a", APIKeyEnv: "PILOT_TEST_KEY_A"}
	t.Setenv(a.APIKeyEnv, "inherited-key-value")
	const secret = "sk-very-secret-value-0123456789"
	for _, c := range []struct {
		name  string
		files []string
		msg   string
	}{
		{"no file", nil, "--env is required"},
		{"malformed", []string{writeEnv(t, "PILOT_TEST_KEY_A "+secret+"\n", 0o600)}, "line 1: not NAME=value"},
		{"bad name", []string{writeEnv(t, "\n1KEY="+secret+"\n", 0o600)}, "line 2: not NAME=value"},
		{"readable by others", []string{writeEnv(t, "PILOT_TEST_KEY_A="+secret+"\n", 0o644)}, "chmod 600"},
		{"missing", []string{writeEnv(t, "OTHER="+secret+"\n", 0o600)}, "no env file sets PILOT_TEST_KEY_A"},
		{"empty", []string{writeEnv(t, "PILOT_TEST_KEY_A=\n", 0o600)}, "no env file sets PILOT_TEST_KEY_A"},
		{"twice in one file", []string{writeEnv(t, "PILOT_TEST_KEY_A="+secret+"\nPILOT_TEST_KEY_A="+secret+"\n", 0o600)}, "set twice"},
		{"two files", []string{writeEnv(t, "PILOT_TEST_KEY_A="+secret+"\n", 0o600), writeEnv(t, "PILOT_TEST_KEY_A="+secret+"\n", 0o600)}, "both set"},
	} {
		err := loadKeys(c.files, []*config.Target{a})
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s: %v, want %q", c.name, err, c.msg)
			continue
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "very-secret") {
			t.Errorf("%s: the error carries the value: %v", c.name, err)
		}
	}
	if os.Getenv(a.APIKeyEnv) != "inherited-key-value" {
		t.Error("a refused load changed the variable")
	}
}
