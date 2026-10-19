package evidence

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/releasekey"
)

// Release is a finished hosted run as it is checked before release: its
// output directory, the configuration file's bytes, the frame store root
// and the report pair's two file names in Dir. Key is the release key,
// which a target with a confidential limit requires.
type Release struct {
	Dir        string
	Config     []byte
	FramesRoot string
	Pair       [2]string
	Key        *releasekey.Key
}

// ConfigCopyFile is the configuration file as loaded, kept in a release
// run's frame directory when a limit marked config.MarkingConfidential
// makes it differ from the file as published.
const ConfigCopyFile = "config.yaml"

// WriteConfigCopy writes cfg's file as loaded to ConfigCopyFile in dir,
// readable by the owner only, when it differs from the file as published
// (config.Config.Published). It refuses an existing file.
func WriteConfigCopy(dir string, cfg *config.Config) error {
	published, err := cfg.Published()
	if err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	if bytes.Equal(published, cfg.Raw) {
		return nil
	}
	return writeNew(filepath.Join(dir, ConfigCopyFile), cfg.Raw)
}

// CheckRelease reads the manifest in r.Dir and returns it when the run
// has every SPEC-HOSTED §9 artifact: the commit record (a revision, and
// the source diff when the build was modified), the verbatim config,
// the attempts file, the report pair, the manifest, the scorer output
// on every measured attempt of a pilot run, the cell record with a
// consistent label and aggregate dispatch series, whose own run's part
// counts the attempts file's dispatches, and inter-chunk gap histograms
// that agree with the attempts file's completions, model fields that
// agree with the attempts file's records and the configuration's
// expected model, and an attempts file without answer text for a
// release run, and the frame store with one line per attempt. A release
// cell that does not count passes as a labelled record, which never
// enters its route's frame (Manifest.Counted); one whose ladder never
// started, and an interrupted one, has empty attempts and frame files,
// and an interrupted cell's own dispatch series need not be final or
// match its attempts file. Every file the manifest names must exist
// with the SHA-256 and size it records: the config, the attempts file,
// the report pair and the clock, status and balance records in r.Dir, a
// release run's start balance among them, and the frame store and the
// operator copies in the run's frame directory. On a target with a
// confidential limit the frame store and the operator records must be
// keyed, with the HMAC-SHA256 under r.Key the manifest records, and
// elsewhere none may be. r.Config is the file as
// loaded: the manifest names the file as published
// (config.Config.Published), a release cell's ceiling-rule verdict must
// be the one published from r.Config's limits
// (admission.CheckPublished), and where the two files differ the run's
// frame directory keeps r.Config as ConfigCopyFile. Otherwise it
// returns an error naming every missing item and every mismatch.
func CheckRelease(r Release) (*Manifest, error) {
	m, err := ReadManifest(filepath.Join(r.Dir, ManifestFile))
	if err != nil {
		return nil, fmt.Errorf("evidence: release refused: %w", err)
	}
	var problems []string
	fail := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	// held is whether the frame store and the operator records are keyed.
	held := false
	check := func(base string, ref FileRef, what string) []byte {
		b, err := readRef(base, ref, nil)
		if err != nil {
			fail("%s: %v", what, err)
		}
		return b
	}
	checkHeld := func(base string, ref FileRef, what string) []byte {
		switch {
		case held && !ref.Keyed():
			fail("%s: %s is named by its SHA-256 on a target with a confidential limit", what, ref.Name)
			return nil
		case !held && ref.Keyed():
			fail("%s: %s is keyed on a target with public limits only", what, ref.Name)
			return nil
		case held && r.Key == nil:
			return nil
		}
		b, err := readRef(base, ref, r.Key)
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
	cfg, err := config.Decode(r.Config)
	var published []byte
	if err == nil {
		published, err = cfg.Published()
	}
	switch {
	case err != nil:
		cfg = nil
		fail("config: %v", err)
	case refOf(m.Config.Name, published) != m.Config:
		fail("config: not %s, the file the manifest names", m.Config.Name)
	default:
		held = cfg.Target.Confidential()
		if held && r.Key == nil {
			fail("release key: the target has a confidential limit, whose frame store and operator records are keyed, and no release key was given")
		}
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
	switch {
	case m.BalanceStart != nil:
		check(r.Dir, *m.BalanceStart, "start balance")
	case m.Profile == config.ProfileRelease:
		fail("start balance: not in the manifest")
	}
	if m.BalanceEnd != nil {
		check(r.Dir, *m.BalanceEnd, "end balance")
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
			if err := c.checkLabel(m.RunValid, m.InvalidReasons, m.Gates, m.Endpoint.ModelMismatches); err != nil {
				fail("cell record: %v", err)
			}
			if cfg != nil && m.Endpoint.ExpectedModel != cfg.Target.ExpectedModel() {
				fail("expected model %q, the configuration's %q", m.Endpoint.ExpectedModel, cfg.Target.ExpectedModel())
			}
			if attempts != nil {
				for _, p := range modelProblems(m.Endpoint, recs) {
					fail("%s", p)
				}
			}
			switch {
			case c.Admission == nil:
				fail("cell record: admission: no verdict")
			case cfg != nil:
				if err := admission.CheckPublished(cfg, c.Admission); err != nil {
					fail("cell record: %v", err)
				}
			}
			if !c.LadderStarted && len(recs) > 0 {
				fail("%s: %d attempts in a cell whose ladder never started", AttemptsFile, len(recs))
			}
			if c.Outcome == CellInterrupted && len(recs) > 0 {
				fail("%s: %d attempts in an interrupted cell", AttemptsFile, len(recs))
			}
			for _, p := range c.dispatchProblems() {
				fail("cell record: %s", p)
			}
			if d := c.Dispatch; d != nil && len(d.Sources) > 0 && attempts != nil && c.Outcome != CellInterrupted {
				sent := 0
				for i := range recs {
					if recs[i].DispatchNs != 0 {
						sent++
					}
				}
				if d.Sources[0].Dispatches != sent {
					fail("cell record: dispatch_aggregate: the cell's own series counts %d dispatches, %s %d", d.Sources[0].Dispatches, AttemptsFile, sent)
				}
			}
			if c.Gaps == nil {
				fail("cell record: inter_chunk_gaps: none")
			} else if attempts != nil {
				if err := CheckGaps(c.Gaps, recs); err != nil {
					fail("cell record: %v", err)
				}
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
		frames := checkHeld(r.FramesRoot, *m.Frames, "frame store")
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
			checkHeld(copies, rec.FileRef, rec.Kind+" record copy")
			diffChecked = diffChecked || (m.Source.Diff != nil && rec.FileRef == *m.Source.Diff)
		}
		if d := m.Source.Diff; d != nil && !diffChecked {
			checkHeld(copies, *d, "commit: source diff copy")
		}
		if cfg != nil && !bytes.Equal(published, r.Config) {
			switch b, err := os.ReadFile(filepath.Join(runDir, ConfigCopyFile)); {
			case errors.Is(err, os.ErrNotExist):
				fail("config copy: %s is missing", ConfigCopyFile)
			case err != nil:
				fail("config copy: %v", err)
			case !bytes.Equal(b, r.Config):
				fail("config copy: %s is not the configuration given", ConfigCopyFile)
			}
		}
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("evidence: release refused: %s", strings.Join(problems, "; "))
	}
	return m, nil
}

// modelProblems reports every way e's model fields disagree with the
// attempts' records: each record whose first reported model differs from
// the expected one lists it as a mismatch, no mismatch is the expected
// one, and e counts the records with a mismatch and lists every model
// they report, in record order.
func modelProblems(e Endpoint, recs []Record) []string {
	var problems []string
	var reported []string
	n := 0
	for i := range recs {
		r := &recs[i]
		if r.ReportedModel != "" && r.ReportedModel != e.ExpectedModel && !slices.Contains(r.ModelMismatch, r.ReportedModel) {
			problems = append(problems, fmt.Sprintf("%s: attempt %d reported model %q, the expected %q, with no mismatch recorded", AttemptsFile, r.Index, r.ReportedModel, e.ExpectedModel))
		}
		if slices.Contains(r.ModelMismatch, e.ExpectedModel) {
			problems = append(problems, fmt.Sprintf("%s: attempt %d records the expected model %q as a mismatch", AttemptsFile, r.Index, e.ExpectedModel))
		}
		if len(r.ModelMismatch) > 0 {
			n++
		}
		for _, m := range append([]string{r.ReportedModel}, r.ModelMismatch...) {
			if m != "" && !slices.Contains(reported, m) {
				reported = append(reported, m)
			}
		}
	}
	if n != e.ModelMismatches || !slices.Equal(reported, e.ReportedModels) {
		problems = append(problems, fmt.Sprintf("models: %s gives %d mismatches over reported %q, the manifest %d over %q", AttemptsFile, n, reported, e.ModelMismatches, e.ReportedModels))
	}
	return problems
}

// readRef reads the file ref names under base and returns its bytes when
// their SHA-256 and size, or for a keyed ref their HMAC-SHA256 under key,
// are the ones ref records.
func readRef(base string, ref FileRef, key *releasekey.Key) ([]byte, error) {
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
	if ref.Keyed() {
		switch {
		case ref.SHA256 != "" || ref.Bytes != "":
			return nil, fmt.Errorf("%s: a keyed reference with a SHA-256 or a size", ref.Name)
		case key == nil:
			return nil, fmt.Errorf("%s is keyed and no release key was given", ref.Name)
		case key.Sum(b) != ref.HMACSHA256:
			return nil, fmt.Errorf("%s has another HMAC-SHA256 under the release key than the manifest records", ref.Name)
		}
		return b, nil
	}
	if got := refOf(ref.Name, b); got != ref {
		return nil, fmt.Errorf("%s has SHA-256 %s and %s bytes, the manifest records %s and %s", ref.Name, got.SHA256, got.Bytes, ref.SHA256, ref.Bytes)
	}
	return b, nil
}
