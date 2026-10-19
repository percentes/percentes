package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/validity"
)

func sha(b string) string {
	s := sha256.Sum256([]byte(b))
	return hex.EncodeToString(s[:])
}

// recordFiles are the operator's record files, by path.
var recordFiles = map[string]string{
	"limits.png":         "\x89PNG limits page",
	"prices.html":        "<html>prices</html>",
	"quota.txt":          "quota at start: 15 RPM",
	"terms/verdict.md":   "IN, 6 Oct 2026",
	"status-mapping.md":  "429 = rate limit; 529 = overloaded",
	"eligibility.md":     "production model",
	"account.md":         "dedicated account",
	"affiliation.md":     "no affiliation",
	"source.diff":        "diff --git a/x b/x\n",
	"operator-notes.txt": "unused",
}

const operatorJSON = `{
  "provider": "Example Cloud",
  "account_tier": "Basic",
  "client_host": "one Mac, London",
  "cell": "pilot session 1",
  "roles": {"task": "Percentes", "cases_labels": "generator", "rubric": "Claude, approved by Varun", "pair_interest": "none"},
  "records": [
    {"kind": "limits", "path": "limits.png", "url": "https://docs.example/limits", "retrieved": "2026-10-06"},
    {"kind": "prices", "path": "prices.html", "url": "https://docs.example/pricing", "retrieved": "2026-10-06T19:40:00Z"},
    {"kind": "quota", "path": "quota.txt", "retrieved": "2026-10-06"},
    {"kind": "terms", "path": "terms/verdict.md", "url": "https://example/terms", "retrieved": "2026-10-06", "note": "T&C of 15 Sep 2026"},
    {"kind": "status_mapping", "path": "status-mapping.md", "url": "https://docs.example/errors", "retrieved": "2026-10-06"},
    {"kind": "eligibility", "path": "eligibility.md", "retrieved": "2026-10-06"},
    {"kind": "dedicated_account", "path": "account.md", "retrieved": "2026-10-06"},
    {"kind": "affiliation", "path": "affiliation.md", "retrieved": "2026-10-06"},
    {"kind": "source_diff", "path": "source.diff", "retrieved": "2026-10-06"}
  ]
}`

// writeOperator writes body as operator.json beside the record files, and
// outside.txt in the directory above, and returns the operator file's
// path.
func writeOperator(t *testing.T, body string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "operator")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "..", "outside.txt"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, content := range recordFiles {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "operator.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func loadOperator(t *testing.T) *Operator {
	t.Helper()
	op, err := LoadOperator(writeOperator(t, operatorJSON))
	if err != nil {
		t.Fatal(err)
	}
	return op
}

// Every record file is hashed as read, named by its path from the
// operator file's directory.
func TestLoadOperator(t *testing.T) {
	op := loadOperator(t)
	if op.Provider != "Example Cloud" || op.AccountTier != "Basic" || op.ClientHost != "one Mac, London" || op.Cell != "pilot session 1" || op.Roles == nil || op.Roles.Rubric != "Claude, approved by Varun" {
		t.Errorf("operator %+v", op)
	}
	if op.File != (FileRef{Name: "operator.json", SHA256: sha(operatorJSON), Bytes: json.Number(strconv.Itoa(len(operatorJSON)))}) {
		t.Errorf("file %+v", op.File)
	}
	if len(op.Records) != 9 {
		t.Fatalf("%d records", len(op.Records))
	}
	for _, r := range op.Records {
		want := recordFiles[r.Name]
		if r.SHA256 != sha(want) || r.Bytes != json.Number(strconv.Itoa(len(want))) {
			t.Errorf("%s: %+v", r.Name, r.FileRef)
		}
	}
	terms := op.find(RecordTerms)
	if terms.Name != "terms/verdict.md" || terms.URL != "https://example/terms" || terms.Retrieved != "2026-10-06" || terms.Note != "T&C of 15 Sep 2026" {
		t.Errorf("terms %+v", terms)
	}
}

func TestLoadOperatorRefusals(t *testing.T) {
	edit := func(old, new string) string {
		if !strings.Contains(operatorJSON, old) {
			t.Fatalf("fixture lacks %q", old)
		}
		return strings.Replace(operatorJSON, old, new, 1)
	}
	limits := `{"kind": "limits", "path": "limits.png", "url": "https://docs.example/limits", "retrieved": "2026-10-06"},`
	for name, body := range map[string]string{
		"provider":           edit(`"provider": "Example Cloud"`, `"provider": ""`),
		"account tier":       edit(`"account_tier": "Basic",`, ``),
		"client host":        edit(`"client_host": "one Mac, London",`, ``),
		"unknown field":      edit(`"cell":`, `"region": "x", "cell":`),
		"unknown kind":       edit(`"kind": "quota"`, `"kind": "balance"`),
		"trailing data":      operatorJSON + `{}`,
		"parent path":        edit(`"path": "quota.txt"`, `"path": "../outside.txt"`),
		"absolute path":      edit(`"path": "quota.txt"`, `"path": "/etc/hosts"`),
		"empty path":         edit(`"path": "quota.txt"`, `"path": ""`),
		"missing file":       edit(`"path": "quota.txt"`, `"path": "nothing.txt"`),
		"directory":          edit(`"path": "quota.txt"`, `"path": "terms"`),
		"bad retrieved":      edit(`"retrieved": "2026-10-06T19:40:00Z"`, `"retrieved": "6 Oct 2026"`),
		"no retrieved":       edit(`, "retrieved": "2026-10-06"}`, `}`),
		"limits without url": edit(limits, `{"kind": "limits", "path": "limits.png", "retrieved": "2026-10-06"},`),
		"non-http url":       edit(`"url": "https://example/terms"`, `"url": "file:///etc/terms"`),
		"bad optional url":   edit(`"path": "quota.txt",`, `"path": "quota.txt", "url": "quota",`),
		"no limits record":   edit(limits, ``),
		"two source diffs":   edit(`{"kind": "source_diff"`, `{"kind": "source_diff", "path": "source.diff", "retrieved": "2026-10-06"}, {"kind": "source_diff"`),
		"partial roles":      edit(`"pair_interest": "none"`, `"pair_interest": ""`),
	} {
		if _, err := LoadOperator(writeOperator(t, body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := LoadOperator(""); err == nil {
		t.Error("empty path accepted")
	}
}

// On a target with a confidential limit, an operator file that holds the
// limit's value as loaded, in any printed form, in its own name or a
// published text is refused with an error that names the field and the
// limit's dimension and carries no value; a window-close record of it is
// refused too. A public target accepts the same file, and the
// confidential target accepts the digits inside a longer number.
func TestOperatorConfidential(t *testing.T) {
	cfg, public := confidentialCfg(t), releaseCfg(t)
	key, err := releasekey.New()
	if err != nil {
		t.Fatal(err)
	}
	edit := func(old, new string) string {
		if !strings.Contains(operatorJSON, old) {
			t.Fatalf("fixture lacks %q", old)
		}
		return strings.Replace(operatorJSON, old, new, 1)
	}
	if err := loadOperator(t).CheckConfidential(&cfg.Target); err != nil {
		t.Fatalf("an operator file without the value: %v", err)
	}
	for _, tc := range []struct {
		name, body, field, value string
		// tpm, where set, is the confidential limit's value in place of
		// the configuration's.
		tpm float64
		// file, where set, is the operator file's name.
		file string
	}{
		{"note", edit(`"note": "T&C of 15 Sep 2026"`, `"note": "TPM 300001 per console"`), "records[3].note", "300001", 0, ""},
		{"path", edit(`"path": "quota.txt"`, `"path": "tpm-300001/../quota.txt"`), "records[2].path", "300001", 0, ""},
		{"url", edit(`"url": "https://docs.example/limits"`, `"url": "https://docs.example/limits?tpm=300001"`), "records[0].url", "300001", 0, ""},
		{"client host", edit(`"client_host": "one Mac, London"`, `"client_host": "one Mac, 300001"`), "client_host", "300001", 0, ""},
		{"e form", edit(`"note": "T&C of 15 Sep 2026"`, `"note": "TPM 3.00001e+05"`), "records[3].note", "3.00001e+05", 0, ""},
		{"%g form", edit(`"note": "T&C of 15 Sep 2026"`, `"note": "TPM 3.00001e+06"`), "records[3].note", "3.00001e+06", 3000010, ""},
		{"comma", edit(`"note": "T&C of 15 Sep 2026"`, `"note": "TPM 300,001 per console"`), "records[3].note", "300,001", 0, ""},
		{"narrow space", edit(`"note": "T&C of 15 Sep 2026"`, "\"note\": \"TPM 300\u202f001\""), "records[3].note", "300\u202f001", 0, ""},
		{"grouped decimal", edit(`"note": "T&C of 15 Sep 2026"`, `"note": "TPM 3,000,010"`), "records[3].note", "3,000,010", 3000010, ""},
		{"file name", operatorJSON, "file path", "300001", 0, "operator-tpm-300001.json"},
	} {
		path := writeOperator(t, tc.body)
		if tc.file != "" {
			named := filepath.Join(filepath.Dir(path), tc.file)
			if err := os.Rename(path, named); err != nil {
				t.Fatal(err)
			}
			path = named
		}
		op, err := LoadOperator(path)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if err := op.CheckConfidential(&public.Target); err != nil {
			t.Errorf("%s: public target: %v", tc.name, err)
		}
		target := cfg.Target
		target.Limits = append([]config.Limit(nil), target.Limits...)
		for i := range target.Limits {
			if target.Limits[i].Dimension == config.LimitTPM && tc.tpm != 0 {
				target.Limits[i].Value = tc.tpm
			}
		}
		want := "a confidential limit's value is in " + tc.field + " (tokens_per_minute)"
		err = op.CheckConfidential(&target)
		if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), tc.value) {
			t.Errorf("%s: %v; want %q", tc.name, err, want)
		}
		if _, err := NewWindowClose(&target, time.Now(), refreshRun(`{"limits": []}`), op, key); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: window-close record: %v", tc.name, err)
		}
	}
	for _, note := range []string{"ticket 13000017", "build 3000012", "v1.300001"} {
		op, err := LoadOperator(writeOperator(t, edit(`"note": "T&C of 15 Sep 2026"`, `"note": "`+note+`"`)))
		if err != nil {
			t.Fatal(err)
		}
		if err := op.CheckConfidential(&cfg.Target); err != nil {
			t.Errorf("%q: %v", note, err)
		}
	}
}

// A confidential limit's value a refresh command read, other than the one
// recorded, refuses an operator file that holds it once the operator
// withholds it, in a check and in a manifest's or window-close record's
// records; a public limit's value read is not withheld.
func TestOperatorWithhold(t *testing.T) {
	cfg := confidentialCfg(t)
	key, err := releasekey.New()
	if err != nil {
		t.Fatal(err)
	}
	op, err := LoadOperator(writeOperator(t, strings.Replace(operatorJSON, `"note": "T&C of 15 Sep 2026"`, `"note": "TPM now 310,007; RPM 1000"`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := op.CheckConfidential(&cfg.Target); err != nil {
		t.Fatalf("before the read: %v", err)
	}
	read, err := ReadRefreshedLimits(refreshRun(`{"limits": [{"dimension": "requests_per_minute", "value": 1000}, {"dimension": "tokens_per_minute", "value": 310007}]}`), &cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	op.Withhold(read[:1])
	if err := op.CheckConfidential(&cfg.Target); err != nil {
		t.Fatalf("a public limit's value withheld: %v", err)
	}
	op.Withhold(read)
	op.Withhold(read)
	const want = "evidence: operator file: a confidential limit's value is in records[3].note (tokens_per_minute)"
	if err := op.CheckConfidential(&cfg.Target); err == nil || err.Error() != want {
		t.Fatalf("%v; want %q", err, want)
	}
	if _, err := NewWindowClose(&cfg.Target, time.Now(), refreshRun(`{"limits": []}`), op, key); err == nil || err.Error() != want {
		t.Fatalf("window-close record: %v", err)
	}
	if _, err := op.records(&cfg.Target, key); err == nil || err.Error() != want {
		t.Fatalf("manifest records: %v", err)
	}
}

// A copy holds the bytes hashed at load, though the file has changed
// since, and an existing copy is never overwritten.
func TestOperatorCopies(t *testing.T) {
	path := writeOperator(t, operatorJSON)
	op, err := LoadOperator(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "quota.txt"), []byte("changed after load"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "frames", "20261006T200000.000000000Z")
	if err := op.WriteCopies(dir); err != nil {
		t.Fatal(err)
	}
	check := func(rel, want string) {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(rel))
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != want {
			t.Errorf("%s: %q, want %q", rel, b, want)
		}
		if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: mode %v", rel, fi.Mode())
		}
	}
	check(OperatorCopyFile, operatorJSON)
	for _, r := range op.Records {
		check(RecordsCopyDir+"/"+r.Name, recordFiles[r.Name])
	}
	if _, err := os.Stat(filepath.Join(dir, RecordsCopyDir, "operator-notes.txt")); err == nil {
		t.Error("a file the operator file does not name was copied")
	}
	if err := op.WriteCopies(dir); err == nil {
		t.Error("existing copies overwritten")
	}
}

// Refreshed refuses the records whose limits or eligibility file was
// last modified before the refresh started, to the second, naming each
// and dating none; otherwise it dates those two by their files'
// modification times and leaves the other records' dates as written.
func TestOperatorRefreshed(t *testing.T) {
	path := writeOperator(t, operatorJSON)
	start := time.Date(2026, 10, 17, 6, 0, 0, 500_000_000, time.UTC)
	touch := func(name string, at time.Time) {
		t.Helper()
		if err := os.Chtimes(filepath.Join(filepath.Dir(path), name), at, at); err != nil {
			t.Fatal(err)
		}
	}
	refreshed := func() (*Operator, error) {
		t.Helper()
		op, err := LoadOperator(path)
		if err != nil {
			t.Fatal(err)
		}
		return op, op.Refreshed(start)
	}
	for name := range recordFiles {
		touch(name, start.Add(-5*time.Second))
	}
	op, err := refreshed()
	if err == nil || !strings.Contains(err.Error(), "limits record limits.png last modified 2026-10-17T05:59:55Z") ||
		!strings.Contains(err.Error(), "eligibility record eligibility.md") || strings.Contains(err.Error(), "quota") {
		t.Fatalf("all stale: %v", err)
	}
	if r := op.find(RecordLimits); r.Retrieved != "2026-10-06" {
		t.Errorf("stale limits dated %s", r.Retrieved)
	}
	touch("limits.png", start.Add(3*time.Second))
	if _, err := refreshed(); err == nil || strings.Contains(err.Error(), "limits") || !strings.Contains(err.Error(), "eligibility") {
		t.Fatalf("eligibility stale: %v", err)
	}
	touch("eligibility.md", start.Add(-300*time.Millisecond))
	op, err = refreshed()
	if err != nil {
		t.Fatal(err)
	}
	for kind, want := range map[string]string{RecordLimits: "2026-10-17T06:00:03Z", RecordEligibility: "2026-10-17T06:00:00Z", RecordQuota: "2026-10-06", RecordPrices: "2026-10-06T19:40:00Z"} {
		if r := op.find(kind); r.Retrieved != want {
			t.Errorf("%s retrieved %s, want %s", kind, r.Retrieved, want)
		}
	}
}

func buildInfo(settings ...string) *debug.BuildInfo {
	info := &debug.BuildInfo{GoVersion: runtime.Version()}
	for i := 0; i+1 < len(settings); i += 2 {
		info.Settings = append(info.Settings, debug.BuildSetting{Key: settings[i], Value: settings[i+1]})
	}
	return info
}

func TestCheckSource(t *testing.T) {
	op := loadOperator(t)
	noDiff := *op
	noDiff.Records = nil
	for _, r := range op.Records {
		if r.Kind != RecordSourceDiff {
			noDiff.Records = append(noDiff.Records, r)
		}
	}
	clean := buildInfo("vcs.revision", "bce0628", "vcs.modified", "false")
	dirty := buildInfo("vcs.revision", "bce0628", "vcs.modified", "true")
	for _, tc := range []struct {
		name string
		info *debug.BuildInfo
		op   *Operator
		ok   bool
	}{
		{"clean", clean, &noDiff, true},
		{"dirty with diff", dirty, op, true},
		{"dirty without diff", dirty, &noDiff, false},
		{"no stamp", buildInfo("GOOS", "darwin"), op, false},
		{"no build info", nil, op, false},
	} {
		if err := CheckSource(tc.info, tc.op); (err == nil) != tc.ok {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}

func TestReadLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	body := "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := (LedgerRef{FileRef: FileRef{Name: "ledger.jsonl", SHA256: sha(body), Bytes: json.Number(strconv.Itoa(len(body)))}, Lines: 3}); *got != want {
		t.Errorf("ledger %+v, want %+v", *got, want)
	}
	if _, err := ReadLedger(filepath.Join(t.TempDir(), "none")); err == nil {
		t.Error("missing ledger accepted")
	}
}

// The status command's output is kept verbatim; it sees PATH and LC_ALL=C
// only, and a failing command is recorded with its error.
func TestReadStatus(t *testing.T) {
	t.Setenv("PERCENTES_STATUS_TEST_KEY", "status-secret-0123456789")
	body := `{"status":{"indicator":"none"}}` + "\n"
	ok := fakeScript(t, "status", func(string) string { return "printf '%s' '" + body + "'\nenv >&2\n" })
	args, err := ParseStatusCommand(ok + "  --json ")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(args, []string{ok, "--json"}) {
		t.Errorf("args %q", args)
	}
	rec := ReadStatus(context.Background(), args)
	if rec.Stdout.Text != body || rec.Error != "" || rec.ExitCode == nil || *rec.ExitCode != 0 {
		t.Errorf("record %+v", rec)
	}
	if strings.Contains(rec.Stderr.Text, "status-secret-0123456789") || !strings.Contains(rec.Stderr.Text, "LC_ALL=C\n") {
		t.Errorf("environment %q", rec.Stderr.Text)
	}
	if rec.StartUTC.IsZero() || rec.EndUTC.Before(rec.StartUTC) || rec.StartUTC.Location() != time.UTC {
		t.Errorf("start %v end %v", rec.StartUTC, rec.EndUTC)
	}

	rec = ReadStatus(context.Background(), []string{fakeTool(t, "status", "partial", "outage", 7)})
	if rec.Error == "" || *rec.ExitCode != 7 || rec.Stdout.Text != "partial" || rec.Stderr.Text != "outage" {
		t.Errorf("failing command: %+v", rec)
	}
	if _, err := ParseStatusCommand("  "); err == nil {
		t.Error("empty command accepted")
	}

	path, err := WriteStatus(t.TempDir(), StatusEndFile, rec)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	var back StatusRecord
	if err := json.Unmarshal(b, &back); err != nil || !reflect.DeepEqual(&back, rec) {
		t.Errorf("round trip %v:\n%s", err, b)
	}
}

// manifestRun writes the files one hosted run leaves before its manifest
// and returns the manifest's input.
func manifestRun(t *testing.T) (ManifestInput, string) {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	epoch := time.Date(2026, 10, 10, 20, 0, 0, 0, time.UTC)
	res := &loadgen.Result{
		EpochWall:       epoch,
		RequestedModel:  "openai/gpt-oss-120b",
		ReportedModels:  []string{"openai/gpt-oss-120b", "openai/gpt-oss-120b-v2"},
		ModelMismatches: 1,
		SplitSeries:     true,
		Requests:        []loadgen.Request{{Index: 0, Proto: "HTTP/1.1"}, {Index: 1}, {Index: 2, Proto: "HTTP/1.1"}, {Index: 3, Proto: "HTTP/2.0"}},
	}
	write := func(dir, name, body string) string {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	off := 0.25
	clock := &ClockRecord{Tool: ClockSNTP, StartUTC: epoch.Add(-3 * time.Second), EndUTC: epoch.Add(-2 * time.Second), AbsOffsetS: &off}
	frames := t.TempDir()
	in := ManifestInput{
		Config:           cfg,
		ConfigName:       "pilot.reference.yaml",
		Target:           &cfg.Pilot.Targets[1],
		Result:           res,
		Build:            buildInfo("CGO_ENABLED", "1", "vcs.revision", "bce0628", "vcs.time", "2026-10-06T12:00:00Z", "vcs.modified", "true"),
		Operator:         loadOperator(t),
		Ledger:           &LedgerRef{FileRef: FileRef{Name: "ledger.jsonl", SHA256: sha("x\n"), Bytes: "2"}, Lines: 1},
		Clock:            clock,
		ClockPath:        write(out, ClockFile, "clock"),
		StatusStartPath:  write(out, StatusStartFile, "start"),
		StatusEndPath:    write(out, StatusEndFile, "end"),
		ArtifactPaths:    []string{write(out, AttemptsFile, "attempts"), write(out, "report.json", "{}")},
		BalanceStartPath: write(out, BalanceStartFile, "balance at start"),
		BalanceEndPath:   write(out, BalanceEndFile, "balance at end"),
		FramesRoot:       frames,
		FramesPath:       write(frames, "20261010T200000.000000000Z/"+FramesFile, "frames"),
		Gates: &validity.Report{Gates: []validity.Gate{
			{ID: "G1", Name: "share"}, {ID: "G2", Name: "client", Applicable: true, Observed: true, Pass: true}, {ID: "G6", Name: "hosted-G6", Applicable: true, Observed: true, ReportedOnly: true, Detail: "118/120"},
		}},
		RunValid:       false,
		InvalidReasons: []string{"clock offset: 75 s from sntp exceeds 60 s"},
	}
	return in, out
}

func TestManifestFields(t *testing.T) {
	in, out := manifestRun(t)
	m, err := NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	if !m.StartUTC.Equal(in.Result.EpochWall) || m.WrittenUTC.IsZero() || m.Profile != config.ProfilePilot || m.Cell != "pilot session 1" {
		t.Errorf("header %+v", m)
	}
	diff := in.Operator.find(RecordSourceDiff).FileRef
	if want := (Source{Revision: "bce0628", Modified: true, VCSTime: "2026-10-06T12:00:00Z", Diff: &diff}); !reflect.DeepEqual(m.Source, want) {
		t.Errorf("source %+v", m.Source)
	}
	if want := refOf("pilot.reference.yaml", in.Config.Raw); m.Config != want {
		t.Errorf("config %+v, want %+v", m.Config, want)
	}
	e := m.Endpoint
	if e.Provider != "Example Cloud" || e.AccountTier != "Basic" || e.Name != "baseten" || e.BaseURL != "" ||
		e.EndpointURL != "https://inference.baseten.co/v1/chat/completions" || e.RequestedModel != "openai/gpt-oss-120b" ||
		!reflect.DeepEqual(e.ReportedModels, in.Result.ReportedModels) || e.ModelMismatches != 1 || !e.SplitSeries ||
		e.Params != in.Target.Params || e.MaxTokens != 0 || !reflect.DeepEqual(e.NotApplied, []string{"reasoning_format"}) {
		t.Errorf("endpoint %+v", e)
	}
	if want := map[string]int{"HTTP/1.1": 2, "HTTP/2.0": 1}; !reflect.DeepEqual(e.Protocols, want) {
		t.Errorf("protocols %v", e.Protocols)
	}
	if want := (Client{Host: "one Mac, London", OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), GoVersion: runtime.Version(), CgoEnabled: "1"}); m.Client != want {
		t.Errorf("client %+v", m.Client)
	}
	if m.Clock.File != refOf(ClockFile, []byte("clock")) || *m.Clock.AbsOffsetS != 0.25 || m.Clock.ReadBeforeEpochNs != int64(2*time.Second) {
		t.Errorf("clock %+v", m.Clock)
	}
	if m.StatusStart != refOf(StatusStartFile, []byte("start")) || m.StatusEnd == nil || *m.StatusEnd != refOf(StatusEndFile, []byte("end")) {
		t.Errorf("status %+v %+v", m.StatusStart, m.StatusEnd)
	}
	if want := refOf(BalanceStartFile, []byte("balance at start")); m.BalanceStart == nil || *m.BalanceStart != want {
		t.Errorf("start balance %+v", m.BalanceStart)
	}
	if want := refOf(BalanceEndFile, []byte("balance at end")); m.BalanceEnd == nil || *m.BalanceEnd != want {
		t.Errorf("end balance %+v", m.BalanceEnd)
	}
	if m.Operator != in.Operator.File || m.Roles != in.Operator.Roles || !reflect.DeepEqual(m.Records, in.Operator.Records) || m.Ledger != in.Ledger {
		t.Errorf("operator %+v %+v %+v", m.Operator, m.Roles, m.Ledger)
	}
	if want := []FileRef{refOf(AttemptsFile, []byte("attempts")), refOf("report.json", []byte("{}"))}; !reflect.DeepEqual(m.Artifacts, want) {
		t.Errorf("artifacts %+v", m.Artifacts)
	}
	if want := refOf("20261010T200000.000000000Z/"+FramesFile, []byte("frames")); m.Frames == nil || *m.Frames != want {
		t.Errorf("frames %+v", m.Frames)
	}
	if len(m.Gates) != 2 || m.Gates[0].ID != "G2" || m.Gates[1].ID != "G6" || m.Gates[1].Detail != "118/120" {
		t.Errorf("gates %+v", m.Gates)
	}
	if m.RunValid || !reflect.DeepEqual(m.InvalidReasons, in.InvalidReasons) {
		t.Errorf("validity %v %q", m.RunValid, m.InvalidReasons)
	}

	path, err := WriteManifest(out, m)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{out, in.FramesRoot, os.TempDir()} {
		if strings.Contains(string(b), dir) {
			t.Errorf("manifest names the local directory %s", dir)
		}
	}
	for _, field := range []string{
		`"name": "terms/verdict.md"`,
		`"sha256": "` + sha(recordFiles["terms/verdict.md"]) + `"`,
		`"url": "https://example/terms"`,
		`"retrieved": "2026-10-06T19:40:00Z"`,
		`"read_before_epoch_ns": 2000000000`,
		`"max_tokens": 4096`,
		`"not_applied": [`,
	} {
		if !strings.Contains(string(b), field) {
			t.Errorf("manifest lacks %s:\n%s", field, b)
		}
	}
	if strings.Contains(string(b), "max_completion_tokens") {
		t.Errorf("the other target's ceiling key is in the manifest:\n%s", b)
	}
	var back Manifest
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Config != m.Config || back.Records[3].SHA256 != m.Records[3].SHA256 {
		t.Errorf("round trip %+v", back)
	}
}

// A single hosted target records the body's ceiling and its base URL
// without credentials; a run that ended in an error has no gates, end
// status or frames.
func TestManifestSingleTargetAndFailedRun(t *testing.T) {
	in, _ := manifestRun(t)
	cfg := hostedCfg("https://user:secret-password@api.example/v1?key=secret-query-value")
	cfg.Raw = []byte("target: hosted\n")
	in.Config, in.Target = cfg, &cfg.Target
	in.Gates, in.StatusEndPath, in.FramesPath, in.ArtifactPaths = nil, "", "", nil
	m, err := NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	if m.Endpoint.BaseURL != "https://api.example/v1" || m.Endpoint.EndpointURL != "" || m.Endpoint.MaxTokens != 64 || m.Endpoint.Params != nil {
		t.Errorf("endpoint %+v", m.Endpoint)
	}
	if m.Gates != nil || m.StatusEnd != nil || m.Frames != nil || len(m.Artifacts) != 0 || m.Artifacts == nil {
		t.Errorf("failed run %+v", m)
	}
	b, _ := json.Marshal(m)
	if strings.Contains(string(b), "secret-") {
		t.Errorf("credential in manifest: %s", b)
	}

	in.ClockPath = filepath.Join(t.TempDir(), "none")
	if _, err := NewManifest(in); err == nil {
		t.Error("missing clock file accepted")
	}
}
