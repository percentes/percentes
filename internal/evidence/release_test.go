package evidence

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/score"
)

// releaseRun is a finished run's files on disk: dir holds the published
// files, root the frame store, cfg the configuration file's bytes.
type releaseRun struct {
	dir, root, runDir string
	cfg               []byte
	m                 *Manifest
}

const releaseRunDir = "20261010T200000.000000000Z"

var releasePair = [2]string{"report.json", "report.txt"}

// newReleaseRun writes a complete run of three attempts: one warm-up and
// two measured, every measured attempt scored. A release run's
// configuration is the reference's, whose limits a cell's verdict names.
func newReleaseRun(t *testing.T, profile config.Profile) *releaseRun {
	t.Helper()
	r := &releaseRun{dir: t.TempDir(), root: t.TempDir(), cfg: []byte("profile: " + string(profile) + "\n")}
	if profile == config.ProfileRelease {
		r.cfg = releaseCfg(t).Raw
	}
	r.runDir = filepath.Join(r.root, releaseRunDir)
	measured := int64(config.PinnedPilotWarmupS) * int64(time.Second)
	answer := `{"route":"billing"}`
	recs := []Record{
		{Index: 0, IntendedNs: 0, DispatchNs: 1, DoneNs: 2, Outcome: loadgen.OutcomeCompleted, Answer: &answer},
		{Index: 1, IntendedNs: measured, DispatchNs: measured + 1, DoneNs: measured + 2, Outcome: loadgen.OutcomeCompleted, Answer: &answer,
			Score: &Score{Expected: "billing", Class: score.Success, Route: "billing"}},
		{Index: 2, IntendedNs: measured + 10, DispatchNs: measured + 11, DoneNs: measured + 12, Outcome: loadgen.OutcomeCompleted, Answer: &answer,
			Score: &Score{Expected: "account", Class: score.WrongLabel, Route: "billing"}},
	}
	var attempts bytes.Buffer
	if err := Write(&attempts, recs); err != nil {
		t.Fatal(err)
	}
	put := func(dir, name string, b []byte) FileRef {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return refOf(name, b)
	}
	m := &Manifest{
		SchemaVersion: 1,
		Profile:       profile,
		Source:        Source{Revision: "0123456789abcdef0123456789abcdef01234567", Modified: true},
		Config:        refOf("pilot.yaml", r.cfg),
		Endpoint:      Endpoint{Protocols: map[string]int{"HTTP/1.1": 3}},
		Artifacts: []FileRef{
			put(r.dir, AttemptsFile, attempts.Bytes()),
			put(r.dir, releasePair[0], []byte("{}\n")),
			put(r.dir, releasePair[1], []byte("report\n")),
		},
		StatusStart: put(r.dir, StatusStartFile, []byte(`{"start":true}`)),
		RunValid:    true,
	}
	m.Clock.File = put(r.dir, ClockFile, []byte(`{"clock":true}`))
	end := put(r.dir, StatusEndFile, []byte(`{"end":true}`))
	m.StatusEnd = &end
	balanceStart := put(r.dir, BalanceStartFile, []byte(`{"balance":"start"}`))
	balanceEnd := put(r.dir, BalanceEndFile, []byte(`{"balance":"end"}`))
	m.BalanceStart, m.BalanceEnd = &balanceStart, &balanceEnd
	frames := put(r.runDir, FramesFile, []byte("{\"index\":0}\n{\"index\":1}\n{\"index\":2}\n"))
	frames.Name = releaseRunDir + "/" + FramesFile
	m.Frames = &frames
	m.Operator = refOf("operator-2026-10-10.json", []byte(`{"provider":"p"}`))
	put(r.runDir, OperatorCopyFile, []byte(`{"provider":"p"}`))
	copies := filepath.Join(r.runDir, RecordsCopyDir)
	m.Records = []OperatorRecord{
		{Kind: RecordLimits, FileRef: put(copies, "limits/groq.txt", []byte("limits")), URL: "https://example.com/limits", Retrieved: "2026-10-10"},
		{Kind: RecordSourceDiff, FileRef: put(copies, "source.diff", []byte("diff --git a/x b/x\n")), Retrieved: "2026-10-10"},
	}
	diff := m.Records[1].FileRef
	m.Source.Diff = &diff
	r.m = m
	r.write(t)
	return r
}

func (r *releaseRun) write(t *testing.T) {
	t.Helper()
	if _, err := WriteManifest(r.dir, r.m); err != nil {
		t.Fatal(err)
	}
}

func (r *releaseRun) check() (*Manifest, error) {
	return CheckRelease(Release{Dir: r.dir, Config: r.cfg, FramesRoot: r.root, Pair: releasePair})
}

func TestCheckReleaseComplete(t *testing.T) {
	for _, p := range []config.Profile{config.ProfilePilot, config.ProfileAC} {
		r := newReleaseRun(t, p)
		m, err := r.check()
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if m.Source.Revision != r.m.Source.Revision || len(m.Artifacts) != 3 {
			t.Fatalf("%s: manifest read back as %+v", p, m)
		}
	}
	// A run outside the pilot profile carries no scorer output.
	r := newReleaseRun(t, config.ProfileAC)
	recs := []Record{{Index: 0}, {Index: 1, IntendedNs: int64(config.PinnedPilotWarmupS) * int64(time.Second)}, {Index: 2}}
	var b bytes.Buffer
	if err := Write(&b, recs); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir, AttemptsFile), b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	r.m.Artifacts[0] = refOf(AttemptsFile, b.Bytes())
	r.write(t)
	if _, err := r.check(); err != nil {
		t.Fatalf("unscored run outside the pilot profile: %v", err)
	}
	// An unmodified build needs no source diff.
	r = newReleaseRun(t, config.ProfilePilot)
	r.m.Source.Modified, r.m.Source.Diff, r.m.Records = false, nil, r.m.Records[:1]
	r.write(t)
	if _, err := r.check(); err != nil {
		t.Fatalf("unmodified build: %v", err)
	}
}

func TestCheckReleaseRefusals(t *testing.T) {
	rewrite := func(t *testing.T, path string, b []byte) {
		t.Helper()
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// attempts replaces the attempts file and its manifest reference.
	attempts := func(t *testing.T, r *releaseRun, recs []Record) {
		t.Helper()
		var b bytes.Buffer
		if err := Write(&b, recs); err != nil {
			t.Fatal(err)
		}
		rewrite(t, filepath.Join(r.dir, AttemptsFile), b.Bytes())
		r.m.Artifacts[0] = refOf(AttemptsFile, b.Bytes())
	}
	measured := int64(config.PinnedPilotWarmupS) * int64(time.Second)
	cases := []struct {
		name string
		edit func(t *testing.T, r *releaseRun)
		want string
	}{
		{"no manifest", func(t *testing.T, r *releaseRun) { os.Remove(filepath.Join(r.dir, ManifestFile)) }, ManifestFile},
		{"manifest with an unknown field", func(t *testing.T, r *releaseRun) {
			rewrite(t, filepath.Join(r.dir, ManifestFile), []byte(`{"schema_version":1,"extra":1}`))
		}, `unknown field "extra"`},
		{"no revision", func(t *testing.T, r *releaseRun) { r.m.Source.Revision = ""; r.write(t) }, "commit: the manifest records no revision"},
		{"modified build without a diff", func(t *testing.T, r *releaseRun) {
			r.m.Source.Diff, r.m.Records = nil, r.m.Records[:1]
			r.write(t)
		}, "commit: built from a modified tree and the manifest names no source diff"},
		{"source diff copy edited", func(t *testing.T, r *releaseRun) {
			rewrite(t, filepath.Join(r.runDir, RecordsCopyDir, "source.diff"), []byte("diff --git a/y b/y\n"))
		}, "source_diff record copy: source.diff has SHA-256"},
		{"source diff copy missing, no record", func(t *testing.T, r *releaseRun) {
			r.m.Records = r.m.Records[:1]
			r.write(t)
			os.Remove(filepath.Join(r.runDir, RecordsCopyDir, "source.diff"))
		}, "commit: source diff copy: source.diff is missing"},
		{"config edited", func(t *testing.T, r *releaseRun) { r.cfg = append(r.cfg, '#') }, "config: not pilot.yaml, the file the manifest names"},
		{"attempts not in the manifest", func(t *testing.T, r *releaseRun) { r.m.Artifacts = r.m.Artifacts[1:]; r.write(t) }, AttemptsFile + ": not in the manifest"},
		{"attempts missing", func(t *testing.T, r *releaseRun) { os.Remove(filepath.Join(r.dir, AttemptsFile)) }, AttemptsFile + ": " + AttemptsFile + " is missing"},
		{"attempts edited", func(t *testing.T, r *releaseRun) {
			b, _ := os.ReadFile(filepath.Join(r.dir, AttemptsFile))
			rewrite(t, filepath.Join(r.dir, AttemptsFile), bytes.Replace(b, []byte(`"billing"}`), []byte(`"account"}`), 1))
		}, AttemptsFile + ": " + AttemptsFile + " has SHA-256"},
		{"attempts unreadable", func(t *testing.T, r *releaseRun) {
			rewrite(t, filepath.Join(r.dir, AttemptsFile), []byte("{\"index\":0,\"bogus\":1}\n"))
			r.m.Artifacts[0] = refOf(AttemptsFile, []byte("{\"index\":0,\"bogus\":1}\n"))
			r.write(t)
		}, `unknown field "bogus"`},
		{"report json not in the manifest", func(t *testing.T, r *releaseRun) {
			r.m.Artifacts = append(r.m.Artifacts[:1], r.m.Artifacts[2])
			r.write(t)
		}, "report pair: report.json not in the manifest"},
		{"report text edited", func(t *testing.T, r *releaseRun) { rewrite(t, filepath.Join(r.dir, "report.txt"), []byte("report!\n")) }, "report.txt: report.txt has SHA-256"},
		{"report json missing", func(t *testing.T, r *releaseRun) { os.Remove(filepath.Join(r.dir, "report.json")) }, "report.json: report.json is missing"},
		{"artifact outside the directory", func(t *testing.T, r *releaseRun) {
			r.m.Artifacts[1].Name = "../report.json"
			r.write(t)
		}, `"../report.json" is not a path inside`},
		{"clock record edited", func(t *testing.T, r *releaseRun) { rewrite(t, filepath.Join(r.dir, ClockFile), []byte(`{}`)) }, "clock record: " + ClockFile + " has SHA-256"},
		{"start status missing", func(t *testing.T, r *releaseRun) { os.Remove(filepath.Join(r.dir, StatusStartFile)) }, "start status: " + StatusStartFile + " is missing"},
		{"end status edited", func(t *testing.T, r *releaseRun) { rewrite(t, filepath.Join(r.dir, StatusEndFile), []byte(`{}`)) }, "end status: " + StatusEndFile + " has SHA-256"},
		{"start balance missing", func(t *testing.T, r *releaseRun) { os.Remove(filepath.Join(r.dir, BalanceStartFile)) }, "start balance: " + BalanceStartFile + " is missing"},
		{"end balance edited", func(t *testing.T, r *releaseRun) { rewrite(t, filepath.Join(r.dir, BalanceEndFile), []byte(`{}`)) }, "end balance: " + BalanceEndFile + " has SHA-256"},
		{"measured attempt unscored", func(t *testing.T, r *releaseRun) {
			attempts(t, r, []Record{{Index: 0}, {Index: 1, IntendedNs: measured}, {Index: 2, IntendedNs: measured, Score: &Score{Expected: "billing", Class: score.Success}}})
			r.write(t)
		}, "scorer output: measured attempt 1 has no score"},
		{"measured attempt with a scorer error", func(t *testing.T, r *releaseRun) {
			attempts(t, r, []Record{{Index: 0}, {Index: 1, IntendedNs: measured, Score: &Score{Expected: "x", Error: "label not configured"}}, {Index: 2, IntendedNs: measured, Score: &Score{Expected: "billing", Class: score.Success}}})
			r.write(t)
		}, `scorer output: measured attempt 1: scorer error "label not configured"`},
		{"no measured attempt", func(t *testing.T, r *releaseRun) {
			attempts(t, r, []Record{{Index: 0}, {Index: 1, IntendedNs: measured - 1}, {Index: 2, IntendedNs: measured - 1}})
			r.write(t)
		}, "scorer output: no measured attempt"},
		{"no frame store in the manifest", func(t *testing.T, r *releaseRun) { r.m.Frames = nil; r.write(t) }, "frame store: not in the manifest"},
		{"no frame store root", func(t *testing.T, r *releaseRun) { r.root = "" }, "frame store: no frame store root given"},
		{"frame store missing", func(t *testing.T, r *releaseRun) { os.Remove(filepath.Join(r.runDir, FramesFile)) }, "frame store: " + releaseRunDir + "/" + FramesFile + " is missing"},
		{"frame store edited", func(t *testing.T, r *releaseRun) {
			rewrite(t, filepath.Join(r.runDir, FramesFile), []byte("{\"index\":0}\n{\"index\":1}\n{\"index\":9}\n"))
		}, "frame store: " + releaseRunDir + "/" + FramesFile + " has SHA-256"},
		{"frame store short of a line", func(t *testing.T, r *releaseRun) {
			b := []byte("{\"index\":0}\n{\"index\":1}\n")
			rewrite(t, filepath.Join(r.runDir, FramesFile), b)
			ref := refOf(releaseRunDir+"/"+FramesFile, b)
			r.m.Frames = &ref
			r.write(t)
		}, "frame store: 2 lines for 3 attempts"},
		{"frame store under another root", func(t *testing.T, r *releaseRun) { r.root = t.TempDir() }, "frame store: " + releaseRunDir + "/" + FramesFile + " is missing"},
		{"operator copy edited", func(t *testing.T, r *releaseRun) {
			rewrite(t, filepath.Join(r.runDir, OperatorCopyFile), []byte(`{"provider":"q"}`))
		}, "operator copy: " + OperatorCopyFile + " has SHA-256"},
		{"record copy missing", func(t *testing.T, r *releaseRun) {
			os.Remove(filepath.Join(r.runDir, RecordsCopyDir, "limits", "groq.txt"))
		}, "limits record copy: limits/groq.txt is missing"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newReleaseRun(t, config.ProfilePilot)
			c.edit(t, r)
			m, err := r.check()
			if err == nil {
				t.Fatalf("release passed: %+v", m)
			}
			if m != nil {
				t.Fatal("a refused release returned its manifest")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %q", err, c.want)
			}
		})
	}
}

// One refusal names every missing item and every mismatch.
func TestCheckReleaseNamesEveryProblem(t *testing.T) {
	r := newReleaseRun(t, config.ProfilePilot)
	r.cfg = append(r.cfg, '#')
	os.Remove(filepath.Join(r.dir, "report.txt"))
	os.Remove(filepath.Join(r.runDir, FramesFile))
	_, err := r.check()
	if err == nil {
		t.Fatal("release passed")
	}
	for _, want := range []string{"config: not pilot.yaml", "report.txt is missing", FramesFile + " is missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}
