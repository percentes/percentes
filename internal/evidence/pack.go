package evidence

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// PackInventoryFile is the file in an evidence pack that names each of
// the pack's other files by SHA-256.
const PackInventoryFile = "inventory.json"

// PackInventory is an evidence pack's inventory: the route and each
// other file of the pack by its slash path in the pack and SHA-256, in
// path order.
type PackInventory struct {
	Route string          `json:"route"`
	Files []PublishedFile `json:"files"`
}

// BuildPack builds route's evidence pack (SPEC-HOSTED HA3 point 14): a
// tar holding PackInventoryFile, which names each of files by SHA-256,
// and files, by their slash paths. Entries are in name order, each a
// regular file with mode 0644, owner and group 0 and the Unix epoch as
// its modification time, so the same files give the same bytes.
func BuildPack(route string, files map[string][]byte) ([]byte, error) {
	inv := PackInventory{Route: route, Files: []PublishedFile{}}
	entries := map[string][]byte{}
	for name, b := range files {
		if err := packName(name); err != nil {
			return nil, err
		}
		if name == PackInventoryFile {
			return nil, fmt.Errorf("evidence: pack: %s is the inventory's name", name)
		}
		inv.Files = append(inv.Files, PublishedFile{Path: name, SHA256: refOf(name, b).SHA256})
		entries[name] = b
	}
	slices.SortFunc(inv.Files, func(a, b PublishedFile) int { return strings.Compare(a.Path, b.Path) })
	ib, err := json.MarshalIndent(inv, "", "  ")
	if err != nil {
		return nil, err
	}
	entries[PackInventoryFile] = append(ib, '\n')
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	slices.Sort(names)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range names {
		b := entries[name]
		h := &tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(b)), ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}
		if err := tw.WriteHeader(h); err != nil {
			return nil, fmt.Errorf("evidence: pack: %s: %w", name, err)
		}
		if _, err := tw.Write(b); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadPack reads the regular files of the tar b by slash path, skipping
// directories. It refuses any other entry type, a path outside the pack
// and a path given twice.
func ReadPack(b []byte) (map[string][]byte, error) {
	tr := tar.NewReader(bytes.NewReader(b))
	files := map[string][]byte{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return files, nil
		}
		if err != nil {
			return nil, fmt.Errorf("evidence: pack: %w", err)
		}
		if h.Typeflag == tar.TypeDir {
			continue
		}
		if h.Typeflag != tar.TypeReg {
			return nil, fmt.Errorf("evidence: pack: %s is not a regular file", h.Name)
		}
		name := path.Clean(h.Name)
		if err := packName(name); err != nil {
			return nil, err
		}
		if _, ok := files[name]; ok {
			return nil, fmt.Errorf("evidence: pack: %s given twice", name)
		}
		if files[name], err = io.ReadAll(tr); err != nil {
			return nil, fmt.Errorf("evidence: pack: %s: %w", name, err)
		}
	}
}

// PackCarries returns nil when the tar got holds every file of the pack
// want, its inventory among them, with the same bytes. Other files may
// accompany them.
func PackCarries(got, want []byte) error {
	wf, err := ReadPack(want)
	if err != nil {
		return err
	}
	if _, ok := wf[PackInventoryFile]; !ok {
		return fmt.Errorf("evidence: pack: the route's evidence pack has no %s", PackInventoryFile)
	}
	gf, err := ReadPack(got)
	if err != nil {
		return err
	}
	var missing, differ []string
	for _, name := range sortedKeys(wf) {
		switch b, ok := gf[name]; {
		case !ok:
			missing = append(missing, name)
		case !bytes.Equal(b, wf[name]):
			differ = append(differ, name)
		}
	}
	switch {
	case len(missing) > 0:
		return fmt.Errorf("evidence: pack: holds no %v of the route's evidence pack", missing)
	case len(differ) > 0:
		return fmt.Errorf("evidence: pack: %v differ from the route's evidence pack", differ)
	}
	return nil
}

func packName(name string) error {
	if name == "" || name == "." || !filepath.IsLocal(filepath.FromSlash(name)) || path.Clean(name) != name {
		return fmt.Errorf("evidence: pack: %q is not a path inside the pack", name)
	}
	return nil
}
