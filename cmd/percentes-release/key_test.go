package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/releasekey"
)

// key create writes a key file readable by its owner only, prints its
// path and none of the key, and refuses an existing file; key sum prints
// each file's HMAC-SHA256 under the key and refuses a key file other
// users can read.
func TestKeyCmd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release.key")
	var code int
	var err error
	out := stdoutOf(t, func() { code, err = keyCmd([]string{"create", "--key", path}) })
	if err != nil || code != exitOK || out != "release key written: "+path+", mode 0600\n" {
		t.Fatalf("key create: %d, %v, %q", code, err, out)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v, %v", fi, err)
	}
	hexKey := strings.TrimSpace(string(raw))
	if len(hexKey) != 2*releasekey.Size || strings.Contains(out, hexKey) {
		t.Fatalf("key file %q, output %q", raw, out)
	}
	if code, err := keyCmd([]string{"create", "--key", path}); err == nil || code != exitError {
		t.Fatalf("an existing key replaced: %d, %v", code, err)
	}
	if after, _ := os.ReadFile(path); string(after) != string(raw) {
		t.Fatal("the existing key changed")
	}

	k, err := releasekey.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	a, b := writeFile(t, dir, "a.png", []byte("first")), writeFile(t, dir, "b.png", []byte("second"))
	out = stdoutOf(t, func() { code, err = keyCmd([]string{"sum", "--key", path, a, b}) })
	if want := k.Sum([]byte("first")) + "  " + a + "\n" + k.Sum([]byte("second")) + "  " + b + "\n"; err != nil || code != exitOK || out != want || strings.Contains(out, hexKey) {
		t.Fatalf("key sum: %d, %v, %q, want %q", code, err, out, want)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if code, err := keyCmd([]string{"sum", "--key", path, a}); err == nil || code != exitError || !strings.Contains(err.Error(), "chmod 600") || strings.Contains(err.Error(), hexKey) {
		t.Fatalf("key sum, key readable by others: %d, %v", code, err)
	}

	for name, args := range map[string][]string{
		"no action":      nil,
		"unknown action": {"rotate", "--key", path},
		"no key flag":    {"create"},
		"create extra":   {"create", "--key", filepath.Join(t.TempDir(), "k"), "x"},
		"sum no files":   {"sum", "--key", path},
	} {
		if code, err := keyCmd(args); err == nil || code != exitError {
			t.Errorf("%s: %d, %v", name, code, err)
		}
	}
}

// run refuses to start without --release-key, before reading anything
// but its configuration.
func TestRunNeedsReleaseKey(t *testing.T) {
	args := []string{"--config", "../../configs/release.reference.yaml", "--ledger", "l", "--schedule", "s", "--out", t.TempDir(),
		"--frames", t.TempDir(), "--operator", "o", "--account-created", "2026-09-15"}
	if code, err := runCmd(context.Background(), args); err == nil || code != exitError || err.Error() != "--release-key is required: the file key create wrote" {
		t.Fatalf("run: %d, %v", code, err)
	}
}
