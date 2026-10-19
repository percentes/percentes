package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/schedule"
)

// releaseLedger is a release ledger ledger create starts for the
// configurations at paths.
func releaseLedger(t *testing.T, paths ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	args := []string{"create", "--ledger", path}
	for _, p := range paths {
		args = append(args, "--config", p)
	}
	if code, err := ledgerCmd(args); err != nil || code != exitOK {
		t.Fatalf("ledger create: %d, %v", code, err)
	}
	return path
}

// schedule writes the schedule its inputs derive for the configurations'
// routes and the ledger, with the ledger's line count and the SHA-256 of
// its bytes, which run loads, and never replaces a file.
func TestScheduleCmd(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	ledger := releaseLedger(t, groq, baseten)
	if code, err := ledgerCmd([]string{"price", "--ledger", ledger, "--config", groq, "--input", "0.15", "--output", "0.60",
		"--source", "https://example.test/pricing", "--retrieved", "2026-10-06"}); err != nil || code != exitOK {
		t.Fatalf("ledger price: %d, %v", code, err)
	}
	path := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groq, "--config", baseten, "--ledger", ledger, "--release-key", testReleaseKeyPath, "--schedule", path}
	if code, err := scheduleCmd(args); err != nil || code != exitOK {
		t.Fatalf("schedule: %d, %v", code, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfgs, err := loadReleases([]string{groq, baseten})
	if err != nil {
		t.Fatal(err)
	}
	want := ledgerSchedule(t, ledger, cfgs, "groq", "baseten")
	if !bytes.Equal(b, want) {
		t.Fatalf("written schedule differs from the derived one:\n%s", b)
	}
	s, err := schedule.Load(b)
	if err != nil || !slices.Equal(s.Input.Routes, []string{"baseten", "groq"}) {
		t.Fatalf("load: %v, routes %v", err, s.Input.Routes)
	}
	for _, cfg := range cfgs {
		sum := sha256.Sum256(cfg.Raw)
		got, ok := s.Config(cfg.Target.Name)
		if !ok || got.ConfigSHA256 != hex.EncodeToString(sum[:]) || got.ConfigHMACSHA256 != testReleaseKey.Sum(cfg.Raw) || !slices.Equal(got.HostGroup, []string{"baseten", "groq"}) {
			t.Errorf("%s: frozen %+v, %t", cfg.Target.Name, got, ok)
		}
	}
	lb, err := os.ReadFile(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if open := sha256.Sum256(lb[:bytes.IndexByte(lb, '\n')+1]); s.Input.LedgerSHA256 != hex.EncodeToString(open[:]) {
		t.Errorf("frozen ledger %q, want the open line's %x", s.Input.LedgerSHA256, open)
	}
	head := sha256.Sum256(lb)
	if s.Input.LedgerLines != bytes.Count(lb, []byte("\n")) || s.Input.LedgerHeadSHA256 != hex.EncodeToString(head[:]) ||
		!bytes.Contains(b, []byte(`"ledger_lines": `+strconv.Itoa(s.Input.LedgerLines)+`,`)) || !bytes.Contains(b, []byte(`"ledger_head_sha256": "`+hex.EncodeToString(head[:])+`"`)) {
		t.Errorf("frozen ledger position %d, %q; want %d lines, %x, in\n%s", s.Input.LedgerLines, s.Input.LedgerHeadSHA256, bytes.Count(lb, []byte("\n")), head, b)
	}

	other := append(slices.Clone(args[:len(args)-1]), path)
	other[1] = "another-seed"
	if code, err := scheduleCmd(other); err == nil || code != exitError {
		t.Fatalf("an existing schedule replaced: %d, %v", code, err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(after, want) {
		t.Fatal("the existing schedule changed")
	}
}

// The schedule of a route with a confidential limit freezes the file as
// published and the file as loaded by its HMAC-SHA256 under the release
// key, which run accepts, and carries no SHA-256 of the file as loaded
// and no byte of the key. Run refuses the configuration with its
// confidential value changed, whose file as published is the same, and
// the frozen configuration under another key.
func TestScheduleCmdConfidential(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	markConfidential(t, groq)
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	path := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groq, "--config", baseten, "--ledger", releaseLedger(t, groq, baseten), "--release-key", testReleaseKeyPath, "--schedule", path}
	var code int
	var err error
	printed := stdoutOf(t, func() { code, err = scheduleCmd(args) })
	if err != nil || code != exitOK {
		t.Fatalf("schedule: %d, %v", code, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := schedule.Load(b)
	if err != nil {
		t.Fatal(err)
	}
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	cfg := cfgs[0]
	published, err := cfg.Published()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(published, cfg.Raw) {
		t.Fatal("the file as published is the file as loaded")
	}
	sum, loaded := sha256.Sum256(published), sha256.Sum256(cfg.Raw)
	if got, ok := s.Config("groq"); !ok || got.ConfigSHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("frozen %+v, %t; want the file as published, %x", got, ok, sum)
	}
	if bytes.Contains(b, []byte(hex.EncodeToString(loaded[:]))) {
		t.Errorf("the schedule carries the hash of the file as loaded:\n%s", b)
	}
	if got, _ := s.Config("groq"); got.ConfigHMACSHA256 != testReleaseKey.Sum(cfg.Raw) {
		t.Errorf("frozen keyed hash %q, want the file as loaded's", got.ConfigHMACSHA256)
	}
	key, err := os.ReadFile(testReleaseKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	hexKey := strings.TrimSpace(string(key))
	if bytes.Contains(b, []byte(hexKey)) || strings.Contains(printed, hexKey) {
		t.Errorf("the schedule or its output carries the release key:\n%s\n%s", b, printed)
	}
	if err := checkFrozen(cfg, s, testReleaseKey); err != nil {
		t.Errorf("run refuses the frozen configuration: %v", err)
	}

	raw, err := os.ReadFile(groq)
	if err != nil {
		t.Fatal(err)
	}
	const frozen = "      value: 250013.5\n"
	if !bytes.Contains(raw, []byte(frozen)) {
		t.Fatal("configuration: the confidential value this test edits not found")
	}
	changed := writeFile(t, t.TempDir(), "groq.yaml", bytes.Replace(raw, []byte(frozen), []byte("      value: 500027\n"), 1))
	cfgs, err = loadReleases([]string{changed})
	if err != nil {
		t.Fatal(err)
	}
	edited, err := cfgs[0].Published()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(edited, published) {
		t.Fatal("the confidential value's change reached the file as published")
	}
	if err := checkFrozen(cfgs[0], s, testReleaseKey); err == nil || !strings.Contains(err.Error(), "the configuration as loaded, or the release key, is not the one the schedule froze") {
		t.Errorf("a confidential value changed after the freeze: %v", err)
	}
	other, err := releasekey.New()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkFrozen(cfg, s, other); err == nil || !strings.Contains(err.Error(), "the configuration as loaded, or the release key, is not the one the schedule froze") {
		t.Errorf("another release key: %v", err)
	}
	unkeyed := *s
	unkeyed.Input.Configs = slices.Clone(s.Input.Configs)
	for i := range unkeyed.Input.Configs {
		unkeyed.Input.Configs[i].ConfigHMACSHA256 = ""
	}
	if err := checkFrozen(cfg, &unkeyed, testReleaseKey); err == nil || !strings.Contains(err.Error(), "the schedule freezes no hmac_sha256") {
		t.Errorf("a schedule with no keyed hash: %v", err)
	}
}

// schedule refuses a window the frame does not allow, a configuration
// outside the release profile, a ledger that does not cap a route as its
// configuration does or is missing, and a missing flag, and writes no
// file.
func TestScheduleCmdRefusals(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	ledger := releaseLedger(t, groq, baseten)
	other := releaseLedger(t, routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "20"), baseten)
	key := []string{"--release-key", testReleaseKeyPath}
	for name, args := range map[string][]string{
		"no weekend":       append([]string{"--master-seed", "s", "--first", "2026-10-19", "--last", "2026-10-23", "--config", groq, "--ledger", ledger}, key...),
		"window length":    append([]string{"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-31", "--config", groq, "--ledger", ledger}, key...),
		"no seed":          append([]string{"--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--ledger", ledger}, key...),
		"pilot profile":    append([]string{"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--config", "../../configs/pilot.reference.yaml", "--ledger", ledger}, key...),
		"no config":        append([]string{"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--ledger", ledger}, key...),
		"no release key":   {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--config", baseten, "--ledger", ledger},
		"release key mode": {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--config", baseten, "--ledger", ledger, "--release-key", readableKey(t)},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schedule.json")
			if code, err := scheduleCmd(append(args, "--schedule", path)); err == nil || code != exitError {
				t.Fatalf("%d, %v", code, err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("a refused schedule wrote %s: %v", path, err)
			}
		})
	}
	both := []string{"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--config", baseten, "--release-key", testReleaseKeyPath}
	for name, tc := range map[string]struct {
		ledger []string
		want   string
	}{
		"no ledger":      {nil, "schedule: --ledger and --schedule are required"},
		"missing ledger": {[]string{"--ledger", filepath.Join(t.TempDir(), "none.jsonl")}, "no such file or directory"},
		"another cap":    {[]string{"--ledger", other}, `admission: ledger cap for route groq "20", its configuration "10"`},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schedule.json")
			args := append(append(slices.Clone(both), tc.ledger...), "--schedule", path)
			if code, err := scheduleCmd(args); err == nil || code != exitError || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%d, %v; want %q", code, err, tc.want)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("a refused schedule wrote %s: %v", path, err)
			}
		})
	}
	if code, err := scheduleCmd([]string{"--master-seed", "s", "--config", groq, "--ledger", ledger, "--release-key", testReleaseKeyPath}); err == nil || code != exitError {
		t.Fatalf("no --schedule: %d, %v", code, err)
	}
	path := filepath.Join(t.TempDir(), "schedule.json")
	if code, err := scheduleCmd(append(both, "--ledger", ledger, "--schedule", path)); err != nil || code != exitOK {
		t.Fatalf("the frozen ledger: %d, %v", code, err)
	}
}

// schedule refuses routes whose host groups disagree: an other host not
// given, at another model, or naming other hosts.
func TestScheduleCmdHostGroups(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	edited := func(path string, from, to string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(b), from) != 1 {
			t.Fatalf("%q not found once", from)
		}
		return writeFile(t, t.TempDir(), filepath.Base(path), []byte(strings.Replace(string(b), from, to, 1)))
	}
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	for name, tc := range map[string]struct {
		configs []string
		want    string
	}{
		"other host not given": {[]string{groq}, "schedule: route groq: other host baseten has no configuration given"},
		"other host's model": {[]string{groq, edited(baseten, "model_name: openai/gpt-oss-120b", "model_name: openai/gpt-oss-20b")},
			`schedule: route groq: other host baseten at model "openai/gpt-oss-120b"; its configuration sends "openai/gpt-oss-20b"`},
		"groups differ": {[]string{edited(groq, "  other_hosts:", "  other_hosts:\n    - route: azure\n      model: openai/gpt-oss-120b"), baseten,
			edited(routeConfigFile(t, "azure", "https://api.example.test/v1/chat/completions", "10"), "    - route: baseten\n", "    - route: groq\n      model: openai/gpt-oss-120b\n    - route: baseten\n")},
			"schedule: routes groq and baseten name different hosts of their model: [azure baseten groq] and [baseten groq]"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schedule.json")
			args := []string{"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--ledger", releaseLedger(t, tc.configs...), "--release-key", testReleaseKeyPath, "--schedule", path}
			for _, c := range tc.configs {
				args = append(args, "--config", c)
			}
			if code, err := scheduleCmd(args); err == nil || code != exitError || err.Error() != tc.want {
				t.Fatalf("%d, %v; want %q", code, err, tc.want)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("a refused schedule wrote %s: %v", path, err)
			}
		})
	}
}

// readableKey is a copy of the test release key in a file other users can
// read.
func readableKey(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(testReleaseKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	p := writeFile(t, t.TempDir(), "release.key", b)
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}
