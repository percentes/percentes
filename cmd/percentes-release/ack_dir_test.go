package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// run refuses an acknowledgement directory under --out or one the route's
// user can write, and accepts a read-only one outside it.
func TestAckDirLocationRules(t *testing.T) {
	out := t.TempDir()
	under := filepath.Join(out, "acks")
	writable := t.TempDir()
	readOnly := t.TempDir()
	if err := os.MkdirAll(under, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{under, readOnly} {
		d := d
		if err := os.Chmod(d, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(d, 0o755) }) //nolint:errcheck
	}
	for d, want := range map[string]string{under: "is under --out", writable: "is writable by this process", filepath.Join(out, "missing"): "no such file"} {
		if err := checkAckDir(d, out); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", d, err, want)
		}
	}
	if err := checkAckDir(readOnly, out); err != nil {
		t.Fatal(err)
	}
}

// run refuses an acknowledgement directory it cannot read or search, and
// one under --out reached through a symbolic link.
func TestAckDirAccessRefusals(t *testing.T) {
	out := t.TempDir()
	for _, mode := range []os.FileMode{0o444, 0o111} {
		d := t.TempDir()
		if err := os.Chmod(d, mode); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(d, 0o755) }) //nolint:errcheck
		if err := checkAckDir(d, out); err == nil || !strings.Contains(err.Error(), "not readable and searchable") {
			t.Errorf("mode %o: %v", mode, err)
		}
	}
	under := filepath.Join(out, "acks")
	if err := os.MkdirAll(under, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(under, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(under, 0o755) }) //nolint:errcheck
	links := t.TempDir()
	outLink, ackLink := filepath.Join(links, "out"), filepath.Join(links, "ack")
	for link, target := range map[string]string{outLink: out, ackLink: under} {
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range [][2]string{{under, outLink}, {ackLink, out}, {under, filepath.Join(outLink, ".")}} {
		if err := checkAckDir(c[0], c[1]); err == nil || !strings.Contains(err.Error(), "is under --out") {
			t.Errorf("ack %s, out %s: %v", c[0], c[1], err)
		}
	}
	if err := checkAckDir(under, filepath.Join(t.TempDir(), "new", "out")); err != nil {
		t.Errorf("--out not yet made: %v", err)
	}
}
