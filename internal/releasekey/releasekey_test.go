package releasekey

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Create writes the key in hex with a newline, readable by its owner
// only, and refuses an existing file; Read returns the key it wrote, and
// Sum is the HMAC-SHA256 under it.
func TestCreateRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release.key")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("mode %#o, want 0600", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2*Size+1 || raw[2*Size] != '\n' || !lowerHex(raw[:2*Size]) {
		t.Fatalf("key file %q", raw)
	}
	if err := Create(path); err == nil {
		t.Fatal("Create replaced an existing key")
	}
	if b, _ := os.ReadFile(path); string(b) != string(raw) {
		t.Fatal("the refused Create changed the key file")
	}
	k, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := hex.DecodeString(string(raw[:2*Size]))
	m := hmac.New(sha256.New, b)
	m.Write([]byte("withheld"))
	if got, want := k.Sum([]byte("withheld")), hex.EncodeToString(m.Sum(nil)); got != want {
		t.Fatalf("Sum %s, want %s", got, want)
	}
	plain := sha256.Sum256([]byte("withheld"))
	if k.Sum([]byte("withheld")) == hex.EncodeToString(plain[:]) {
		t.Fatal("Sum is the plain SHA-256")
	}
	other, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if other.Sum([]byte("withheld")) == k.Sum([]byte("withheld")) {
		t.Fatal("two keys give one sum")
	}
}

// Read refuses a file other users can read and a malformed file, and its
// errors carry none of the file's content.
func TestReadRefuses(t *testing.T) {
	dir := t.TempDir()
	good := strings.Repeat("ab", Size) + "\n"
	for name, c := range map[string]struct {
		content string
		mode    os.FileMode
		want    string
	}{
		"group readable": {good, 0o640, "lets other users read it"},
		"no newline":     {strings.Repeat("ab", Size), 0o600, "lowercase hex digits"},
		"upper case":     {strings.Repeat("AB", Size) + "\n", 0o600, "lowercase hex digits"},
		"short":          {strings.Repeat("ab", Size-1) + "\n", 0o600, "lowercase hex digits"},
		"not hex":        {strings.Repeat("zq", Size) + "\n", 0o600, "lowercase hex digits"},
		"trailing data":  {good + "ab\n", 0o600, "lowercase hex digits"},
	} {
		path := filepath.Join(dir, strings.ReplaceAll(name, " ", "-"))
		if err := os.WriteFile(path, []byte(c.content), c.mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, c.mode); err != nil {
			t.Fatal(err)
		}
		k, err := Read(path)
		if err == nil || k != nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, %v", name, k, err)
			continue
		}
		if strings.Contains(err.Error(), "abab") || strings.Contains(err.Error(), "zq") || strings.Contains(strings.ToLower(err.Error()), "abab") {
			t.Errorf("%s: the error carries the file's content: %v", name, err)
		}
	}
	if _, err := Read(filepath.Join(dir, "missing")); err == nil {
		t.Error("read a missing file")
	}
}

// The key prints as its name under every verb and marshals to none of
// its bytes.
func TestKeyNeverPrints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release.key")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	k, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	hexKey := strings.TrimSpace(string(raw))
	j, err := json.Marshal(struct{ K *Key }{k})
	if err != nil {
		t.Fatal(err)
	}
	outs := []string{string(j)}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x", "%X", "%q", "%d"} {
		outs = append(outs, fmt.Sprintf(verb, k), fmt.Sprintf(verb, *k), fmt.Sprintf(verb, struct{ K *Key }{k}))
	}
	for _, s := range outs {
		if strings.Contains(strings.ToLower(s), hexKey[:16]) || strings.Contains(s, string(k.b[:8])) {
			t.Fatalf("printed the key: %s", s)
		}
	}
	if s := fmt.Sprint(k); s != "[release key]" {
		t.Errorf("printed %q", s)
	}
}
