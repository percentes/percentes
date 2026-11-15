package probe

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Write replaces the record through a synced temporary file and leaves
// none behind; WriteRetained refuses an existing file and leaves it as
// it was; a failed write leaves no record.
func TestWriteSynced(t *testing.T) {
	dir := t.TempDir()
	for _, rec := range []*ReleaseRecord{{Route: "groq"}, {Route: "azure"}} {
		if _, err := Write(dir, rec); err != nil {
			t.Fatal(err)
		}
	}
	names := func() []string {
		es, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range es {
			out = append(out, e.Name())
		}
		return out
	}
	if got := names(); !reflect.DeepEqual(got, []string{File}) {
		t.Fatalf("files %v", got)
	}
	if b, err := os.ReadFile(filepath.Join(dir, File)); err != nil || !strings.Contains(string(b), `"route": "azure"`) {
		t.Fatalf("record %s, %v", b, err)
	}
	held := &ReleaseRecord{Route: "groq", Requests: []CheckRequest{{Exchange: Exchange{Retained: &Retained{}}}}}
	path, err := WriteRetained(dir, held)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if _, err := WriteRetained(dir, held); err == nil {
		t.Fatal("an existing retained file replaced")
	}
	if after, _ := os.ReadFile(path); string(after) != string(before) {
		t.Fatal("the existing retained file changed")
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("retained file mode %v, %v", fi.Mode(), err)
	}
	if got := names(); !reflect.DeepEqual(got, []string{RetainedFile, File}) {
		t.Fatalf("files %v", got)
	}
	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	if _, err := Write(locked, &ReleaseRecord{}); err == nil {
		t.Fatal("a write into a read-only directory succeeded")
	}
	if es, _ := os.ReadDir(locked); len(es) != 0 {
		t.Fatalf("a failed write left %v", es)
	}
}
