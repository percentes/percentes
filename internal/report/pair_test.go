package report

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/evidence"
)

// A pair that cannot be written leaves no manifest; once written, the
// manifest names both files and FromRun renders the same pair from disk.
func TestHostedPairWritesManifestLast(t *testing.T) {
	dir := t.TempDir()
	m, cfg := cellManifest(t, evidence.CellStopped, []string{"test stop"}, false)
	cfg.Raw = bytes.Replace(cfg.Raw, []byte("value: 250000"), []byte("value: 80000"), 1)
	sum := sha256.Sum256(cfg.Raw)
	m.Config.SHA256 = hex.EncodeToString(sum[:])
	p := filepath.Join(dir, evidence.AttemptsFile)
	if err := os.WriteFile(p, nil, 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := evidence.HashFile(p, evidence.AttemptsFile)
	if err != nil {
		t.Fatal(err)
	}
	m.Artifacts = []evidence.FileRef{ref}
	blocker := filepath.Join(dir, JSONFile)
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := WriteHostedPair(dir, cfg.Raw, m); err == nil || !strings.Contains(err.Error(), JSONFile) {
		t.Fatalf("unwritable report: err %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, evidence.ManifestFile)); !os.IsNotExist(err) {
		t.Fatalf("failed pair left a manifest: stat %v", err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	m.Artifacts = []evidence.FileRef{ref}
	jsonPath, txtPath, text, err := WriteHostedPair(dir, cfg.Raw, m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := evidence.ReadManifest(filepath.Join(dir, evidence.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	if got.Artifact(JSONFile) == nil || got.Artifact(TextFile) == nil {
		t.Fatalf("manifest artifacts %+v", got.Artifacts)
	}
	raw, rtext, err := FromRun(dir, cfg.Raw)
	if err != nil {
		t.Fatal(err)
	}
	onDisk, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	txt, err := os.ReadFile(txtPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, onDisk) || rtext != text || string(txt) != text {
		t.Fatal("FromRun does not reproduce the written pair")
	}
}
