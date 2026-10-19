package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
// routes and the ledger, which run loads, and never replaces a file.
func TestScheduleCmd(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	ledger := releaseLedger(t, groq, baseten)
	path := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groq, "--config", baseten, "--ledger", ledger, "--schedule", path}
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
		if !ok || got.ConfigSHA256 != hex.EncodeToString(sum[:]) || !slices.Equal(got.HostGroup, []string{"baseten", "groq"}) {
			t.Errorf("%s: frozen %+v, %t", cfg.Target.Name, got, ok)
		}
	}
	lb, err := os.ReadFile(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if open := sha256.Sum256(lb); s.Input.LedgerSHA256 != hex.EncodeToString(open[:]) {
		t.Errorf("frozen ledger %q, want the open line's %x", s.Input.LedgerSHA256, open)
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
// published, which run accepts, and carries no hash of the file as
// loaded.
func TestScheduleCmdConfidential(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	markConfidential(t, groq)
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	path := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groq, "--config", baseten, "--ledger", releaseLedger(t, groq, baseten), "--schedule", path}
	if code, err := scheduleCmd(args); err != nil || code != exitOK {
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
	if err := checkFrozen(cfg, s); err != nil {
		t.Errorf("run refuses the frozen configuration: %v", err)
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
	for name, args := range map[string][]string{
		"no weekend":    {"--master-seed", "s", "--first", "2026-10-19", "--last", "2026-10-23", "--config", groq, "--ledger", ledger},
		"window length": {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-31", "--config", groq, "--ledger", ledger},
		"no seed":       {"--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--ledger", ledger},
		"pilot profile": {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--config", "../../configs/pilot.reference.yaml", "--ledger", ledger},
		"no config":     {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--ledger", ledger},
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
	both := []string{"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--config", baseten}
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
	if code, err := scheduleCmd([]string{"--master-seed", "s", "--config", groq, "--ledger", ledger}); err == nil || code != exitError {
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
			args := []string{"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--ledger", releaseLedger(t, tc.configs...), "--schedule", path}
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
