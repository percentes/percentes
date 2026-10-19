package evidence

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// tarOf writes entries, each a name and content, as a tar in the order
// given; a name ending in "/" is a directory.
func tarOf(t *testing.T, entries ...[2]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		h := &tar.Header{Typeflag: tar.TypeReg, Name: e[0], Mode: 0o600, Size: int64(len(e[1])), ModTime: time.Now()}
		if strings.HasSuffix(e[0], "/") {
			h.Typeflag, h.Size = tar.TypeDir, 0
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(e[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// A pack is the same bytes from the same files in any map order, its
// entries in name order with fixed modes, owners and times, and its
// inventory names each other file by SHA-256 in path order.
func TestBuildPack(t *testing.T) {
	files := map[string][]byte{"pages/routes/groq.txt": []byte("page"), "out/groq/c01-a1/manifest.json": []byte("m1"),
		"pages/routes/groq.json": []byte("{}"), "out/groq/c02-a1/manifest.json": []byte("m2")}
	first, err := BuildPack("groq", files)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		again := map[string][]byte{}
		for k, v := range files {
			again[k] = v
		}
		b, err := BuildPack("groq", again)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, first) {
			t.Fatal("two builds of one pack differ")
		}
	}
	tr := tar.NewReader(bytes.NewReader(first))
	var names []string
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		names = append(names, h.Name)
		if h.Typeflag != tar.TypeReg || h.Mode != 0o644 || h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" || h.ModTime.Unix() != 0 {
			t.Errorf("%s: header %+v", h.Name, h)
		}
	}
	want := []string{"inventory.json", "out/groq/c01-a1/manifest.json", "out/groq/c02-a1/manifest.json", "pages/routes/groq.json", "pages/routes/groq.txt"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("entries %q, want %q", names, want)
	}
	got, err := ReadPack(first)
	if err != nil {
		t.Fatal(err)
	}
	var inv PackInventory
	if err := json.Unmarshal(got[PackInventoryFile], &inv); err != nil {
		t.Fatal(err)
	}
	wantInv := PackInventory{Route: "groq"}
	for _, name := range want[1:] {
		wantInv.Files = append(wantInv.Files, PublishedFile{Path: name, SHA256: shaOf(files[name])})
		if !bytes.Equal(got[name], files[name]) {
			t.Errorf("%s: %q, want %q", name, got[name], files[name])
		}
	}
	if !reflect.DeepEqual(inv, wantInv) {
		t.Errorf("inventory %+v, want %+v", inv, wantInv)
	}
	if b, err := BuildPack("baseten", nil); err != nil || !strings.Contains(string(tarFile(t, b)), `"files": []`) {
		t.Errorf("empty pack: %v: %s", err, tarFile(t, b))
	}
	for _, name := range []string{"inventory.json", "../x", "/abs", "a/../b", "a//b", ""} {
		if _, err := BuildPack("groq", map[string][]byte{name: nil}); err == nil {
			t.Errorf("%q: built", name)
		}
	}
}

// tarFile is the inventory of the pack b.
func tarFile(t *testing.T, b []byte) []byte {
	t.Helper()
	files, err := ReadPack(b)
	if err != nil {
		t.Fatal(err)
	}
	return files[PackInventoryFile]
}

// ReadPack skips directories and cleans names, and refuses links, a
// path outside the pack and a path given twice.
func TestReadPack(t *testing.T) {
	got, err := ReadPack(tarOf(t, [2]string{"pack/", ""}, [2]string{"./pack/a.txt", "a"}))
	if err != nil || len(got) != 1 || string(got["pack/a.txt"]) != "a" {
		t.Fatalf("%v: %q", err, got)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeSymlink, Name: "inventory.json", Linkname: "/etc/passwd"}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		b    []byte
		want string
	}{
		{buf.Bytes(), "inventory.json is not a regular file"},
		{tarOf(t, [2]string{"../a", "a"}), `"../a" is not a path inside the pack`},
		{tarOf(t, [2]string{"a", "1"}, [2]string{"./a", "2"}), "a given twice"},
		{[]byte("tables and evidence"), "evidence: pack: "},
	} {
		if _, err := ReadPack(c.b); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v, want %q", err, c.want)
		}
	}
}

// A pack carries another when it holds each of the other's files, its
// inventory among them, with the same bytes, with or without more.
func TestPackCarries(t *testing.T) {
	want, err := BuildPack("bedrock", map[string][]byte{"pages/routes/bedrock.txt": []byte("page"), "out/bedrock/c01-a1/manifest.json": []byte("m1")})
	if err != nil {
		t.Fatal(err)
	}
	files, err := ReadPack(want)
	if err != nil {
		t.Fatal(err)
	}
	entries := [][2]string{{"replication/README", "how to run it"}}
	for _, name := range sortedKeys(files) {
		entries = append(entries, [2]string{name, string(files[name])})
	}
	if err := PackCarries(tarOf(t, entries...), want); err != nil {
		t.Errorf("with an extra file: %v", err)
	}
	if err := PackCarries(want, want); err != nil {
		t.Errorf("itself: %v", err)
	}
	other, err := BuildPack("bedrock", map[string][]byte{"pages/routes/bedrock.txt": []byte("page"), "out/bedrock/c01-a1/manifest.json": []byte("m1, earlier")})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		got  []byte
		want string
	}{
		{other, "[inventory.json out/bedrock/c01-a1/manifest.json] differ from the route's evidence pack"},
		{tarOf(t, entries[1:2]...), "holds no [out/bedrock/c01-a1/manifest.json pages/routes/bedrock.txt] of the route's evidence pack"},
		{[]byte("tables and evidence"), "evidence: pack: "},
	} {
		if err := PackCarries(c.got, want); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v, want %q", err, c.want)
		}
	}
	if err := PackCarries(want, tarOf(t, [2]string{"a", "a"})); err == nil || !strings.Contains(err.Error(), "has no inventory.json") {
		t.Errorf("want pack without inventory: %v", err)
	}
}
