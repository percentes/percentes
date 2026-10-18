package evidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// Release is a finished hosted run as it is checked before release: its
// output directory, the configuration file's bytes, the frame store root
// and the report pair's two file names in Dir.
type Release struct {
	Dir        string
	Config     []byte
	FramesRoot string
	Pair       [2]string
}

// CheckRelease reads the manifest in r.Dir and returns it when the run
// has every SPEC-HOSTED §9 artifact: the commit record (a revision, and
// the source diff when the build was modified), the verbatim config, the
// attempts file, the report pair, the manifest, the scorer output on
// every measured attempt of a pilot run, the cell record with a
// consistent label and an attempts file without answer text for a
// release run, and the frame store with one line per attempt. A release
// cell that does not count passes as a labelled record, which never
// enters its route's frame (Manifest.Counted); one whose ladder never
// started has empty attempts and frame files. Every file the manifest
// names must exist with the SHA-256 and size it records: the config, the
// attempts file, the report pair and the clock and status records in
// r.Dir, and the frame store and the operator copies in the run's frame
// directory. Otherwise it returns an error naming every missing item and
// every mismatch.
func CheckRelease(r Release) (*Manifest, error) {
	m, err := ReadManifest(filepath.Join(r.Dir, ManifestFile))
	if err != nil {
		return nil, fmt.Errorf("evidence: release refused: %w", err)
	}
	var problems []string
	fail := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	check := func(base string, ref FileRef, what string) []byte {
		b, err := readRef(base, ref)
		if err != nil {
			fail("%s: %v", what, err)
		}
		return b
	}

	if m.Source.Revision == "" {
		fail("commit: the manifest records no revision")
	}
	if m.Source.Modified && m.Source.Diff == nil {
		fail("commit: built from a modified tree and the manifest names no source diff")
	}
	if sum := sha256.Sum256(r.Config); hex.EncodeToString(sum[:]) != m.Config.SHA256 || int64(len(r.Config)) != m.Config.Bytes {
		fail("config: not %s, the file the manifest names", m.Config.Name)
	}

	var attempts []byte
	if ref := m.Artifact(AttemptsFile); ref == nil {
		fail("%s: not in the manifest", AttemptsFile)
	} else {
		attempts = check(r.Dir, *ref, AttemptsFile)
	}
	for _, name := range r.Pair {
		if m.Artifact(name) == nil {
			fail("report pair: %s not in the manifest", name)
		}
	}
	for _, ref := range m.Artifacts {
		if ref.Name != AttemptsFile {
			check(r.Dir, ref, ref.Name)
		}
	}
	check(r.Dir, m.Clock.File, "clock record")
	check(r.Dir, m.StatusStart, "start status")
	if m.StatusEnd != nil {
		check(r.Dir, *m.StatusEnd, "end status")
	}

	var recs []Record
	if attempts != nil {
		if recs, err = ReadRecords(bytes.NewReader(attempts)); err != nil {
			fail("%s: %v", AttemptsFile, err)
			attempts = nil
		}
	}
	if attempts != nil && m.Profile == config.ProfilePilot {
		measuredFrom := int64(config.PinnedPilotWarmupS) * int64(time.Second)
		measured := 0
		for i := range recs {
			if recs[i].IntendedNs < measuredFrom {
				continue
			}
			measured++
			switch s := recs[i].Score; {
			case s == nil:
				fail("scorer output: measured attempt %d has no score", recs[i].Index)
			case s.Error != "":
				fail("scorer output: measured attempt %d: scorer error %q", recs[i].Index, s.Error)
			}
		}
		if measured == 0 {
			fail("scorer output: no measured attempt")
		}
	}
	if m.Profile == config.ProfileRelease {
		switch c := m.Release; {
		case c == nil:
			fail("cell record: not in the manifest")
		default:
			if err := c.checkLabel(m.RunValid, m.InvalidReasons); err != nil {
				fail("cell record: %v", err)
			}
			if !c.LadderStarted && len(recs) > 0 {
				fail("%s: %d attempts in a cell whose ladder never started", AttemptsFile, len(recs))
			}
		}
		for i := range recs {
			if recs[i].Answer != nil {
				fail("%s: attempt %d carries answer text", AttemptsFile, recs[i].Index)
				break
			}
		}
	}

	switch {
	case m.Frames == nil:
		fail("frame store: not in the manifest")
	case r.FramesRoot == "":
		fail("frame store: no frame store root given")
	default:
		frames := check(r.FramesRoot, *m.Frames, "frame store")
		if frames != nil && attempts != nil {
			if n := bytes.Count(frames, []byte("\n")); n != len(recs) {
				fail("frame store: %d lines for %d attempts", n, len(recs))
			}
		}
		runDir := filepath.Join(r.FramesRoot, filepath.Dir(filepath.FromSlash(m.Frames.Name)))
		op := m.Operator
		op.Name = OperatorCopyFile
		check(runDir, op, "operator copy")
		copies := filepath.Join(runDir, RecordsCopyDir)
		diffChecked := false
		for _, rec := range m.Records {
			check(copies, rec.FileRef, rec.Kind+" record copy")
			diffChecked = diffChecked || (m.Source.Diff != nil && rec.FileRef == *m.Source.Diff)
		}
		if d := m.Source.Diff; d != nil && !diffChecked {
			check(copies, *d, "commit: source diff copy")
		}
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("evidence: release refused: %s", strings.Join(problems, "; "))
	}
	return m, nil
}

// readRef reads the file ref names under base and returns its bytes when
// their SHA-256 and size are the ones ref records.
func readRef(base string, ref FileRef) ([]byte, error) {
	name := filepath.FromSlash(ref.Name)
	if !filepath.IsLocal(name) {
		return nil, fmt.Errorf("%q is not a path inside %s", ref.Name, base)
	}
	b, err := os.ReadFile(filepath.Join(base, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%s is missing", ref.Name)
	}
	if err != nil {
		return nil, err
	}
	if got := refOf(ref.Name, b); got != ref {
		return nil, fmt.Errorf("%s has SHA-256 %s and %d bytes, the manifest records %s and %d", ref.Name, got.SHA256, got.Bytes, ref.SHA256, ref.Bytes)
	}
	return b, nil
}
