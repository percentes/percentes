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

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/schedule"
)

// releaseLedger is a release ledger ledger create starts for the
// configurations at paths, with ledger price recording each route's
// prices.
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
	for _, p := range paths {
		if code, err := ledgerCmd([]string{"price", "--ledger", path, "--config", p, "--input", "0.15", "--output", "0.60",
			"--source", "https://example.test/pricing", "--retrieved", "2026-10-06"}); err != nil || code != exitOK {
			t.Fatalf("ledger price: %d, %v", code, err)
		}
	}
	return path
}

// schedule writes the schedule its inputs derive for the configurations'
// routes and the ledger, with the ledger's line count and the SHA-256 of
// its bytes and each route's binding rate, which run loads, and never
// replaces a file. The reference route at a US$10 cap and US$0.15 and
// US$0.60 per million tokens is held by its route cap at 1.755 requests
// per second, below its 2.429 at the tokens-per-minute limit.
func TestScheduleCmd(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	ledger := releaseLedger(t, groq, baseten)
	path := filepath.Join(t.TempDir(), "schedule.json")
	args := append(passingChecks(t, ledger, groq, baseten), "--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groq, "--config", baseten, "--ledger", ledger, "--release-key", testReleaseKeyPath, "--schedule", path)
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
		if got.BindingRPS != 1.755 || got.BindingLimit != admission.RouteCapDimension || got.BindingWindowS != 0 {
			t.Errorf("%s: binding rate %g, limit %q over %g s; want 1.755 at %s", cfg.Target.Name, got.BindingRPS, got.BindingLimit, got.BindingWindowS, admission.RouteCapDimension)
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
	other[slices.Index(other, "--master-seed")+1] = "another-seed"
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
	ledger := releaseLedger(t, groq, baseten)
	args := append(passingChecks(t, ledger, groq, baseten), "--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groq, "--config", baseten, "--ledger", ledger, "--release-key", testReleaseKeyPath, "--schedule", path)
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
	if code, err := scheduleCmd(append(append(both, passingChecks(t, ledger, groq, baseten)...), "--ledger", ledger, "--schedule", path)); err != nil || code != exitOK {
		t.Fatalf("the frozen ledger: %d, %v", code, err)
	}
}

// schedule refuses routes whose host groups disagree: an other host not
// given, at another model, of another request shape or permitted
// ceilings, or naming other hosts.
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
		"other host's model": {[]string{groq, edited(edited(baseten, "model_name: openai/gpt-oss-120b", "model_name: openai/gpt-oss-20b"), `"model": "openai/gpt-oss-120b"`, `"model": "openai/gpt-oss-20b"`)},
			`schedule: route groq: other host baseten at model "openai/gpt-oss-120b"; its configuration sends "openai/gpt-oss-20b"`},
		"groups differ": {[]string{edited(groq, "  other_hosts:", "  other_hosts:\n    - route: azure\n      model: openai/gpt-oss-120b"), baseten,
			edited(routeConfigFile(t, "azure", "https://api.example.test/v1/chat/completions", "10"), "    - route: baseten\n", "    - route: groq\n      model: openai/gpt-oss-120b\n    - route: baseten\n")},
			"schedule: routes groq and baseten name different hosts of their host group: [azure baseten groq] and [baseten groq]"},
		"other host's shape": {[]string{groq, shapeRFile(t, baseten, true)},
			"schedule: route groq: other host baseten sends Shape R at ceilings [16 256], the route Shape S at [512 1024]; a host group has one request shape"},
		"other host's fallback": {[]string{shapeRFile(t, groq, false), shapeRFile(t, baseten, true)},
			"schedule: route groq: other host baseten sends Shape R at ceilings [16 256], the route Shape R at [16]; a host group has one request shape"},
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

// schedule refuses an account group whose routes' caps sum above the
// spend_per_month_usd limit they record, or whose routes record different
// ones, and takes each group's caps apart; a group recording none has no
// test.
func TestScheduleCmdAccountGroups(t *testing.T) {
	edited := func(path string, reps ...string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		for i := 0; i < len(reps); i += 2 {
			if strings.Count(s, reps[i]) != 1 {
				t.Fatalf("%q not found once", reps[i])
			}
			s = strings.Replace(s, reps[i], reps[i+1], 1)
		}
		return writeFile(t, t.TempDir(), filepath.Base(path), []byte(s))
	}
	spend := func(value string) string {
		return "  limits:\n    - dimension: spend_per_month_usd\n      value: " + value + "\n      marking: public\n      operator_set: true\n" +
			"      source: budget.png\n      retrieved: 2026-10-07\n      sha256: \"" + strings.Repeat("0", 64) + "\"\n"
	}
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	apart := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10.5")
	baseten := edited(apart, "account_group: baseten", "account_group: groq")
	// A third route's offset lets the group's two start 80 minutes apart.
	third := edited(routeConfigFile(t, "anthropic", "https://api.example.test/v1/chat/completions", "10"),
		"  other_hosts:              # the host group's other routes: same model and shape\n    - route: baseten\n      model: openai/gpt-oss-120b\n", "")
	for name, tc := range map[string]struct {
		configs []string
		want    string
	}{
		"caps within the limit": {[]string{edited(groq, "  limits:\n", spend("20.5")), edited(baseten, "  limits:\n", spend("20.5"))}, ""},
		"caps above the limit": {[]string{edited(groq, "  limits:\n", spend("20.49")), edited(baseten, "  limits:\n", spend("20.49"))},
			"schedule: account group groq: route caps groq US$10, baseten US$10.5 sum to US$20.5, above the group's spend_per_month_usd limit (US$20.49, source budget.png)"},
		"limits differ": {[]string{edited(groq, "  limits:\n", spend("20.5")), edited(baseten, "  limits:\n", spend("30"))},
			"schedule: account group groq: routes groq and baseten record different spend_per_month_usd limits"},
		"one route records none": {[]string{edited(groq, "  limits:\n", spend("30")), baseten},
			"schedule: account group groq: routes groq and baseten record different spend_per_month_usd limits"},
		"groups apart":      {[]string{edited(groq, "  limits:\n", spend("10")), edited(apart, "  limits:\n", spend("10.5"))}, ""},
		"no limit recorded": {[]string{groq, baseten}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schedule.json")
			ledger := releaseLedger(t, append(tc.configs, third)...)
			args := append(passingChecks(t, ledger, append(tc.configs, third)...), "--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--ledger", ledger, "--release-key", testReleaseKeyPath, "--schedule", path)
			for _, c := range append(tc.configs, third) {
				args = append(args, "--config", c)
			}
			code, err := scheduleCmd(args)
			if tc.want == "" {
				if err != nil || code != exitOK {
					t.Fatalf("%d, %v", code, err)
				}
				return
			}
			if err == nil || code != exitError || err.Error() != tc.want {
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
