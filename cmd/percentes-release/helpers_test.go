package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// testBuild is a build stamped with a clean revision.
var testBuild = &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "0123456789abcdef0123456789abcdef01234567"}, {Key: "vcs.modified", Value: "false"}}}

const testKey = "test-key-release-0123456789abcdef"

// operatorJSON is an operator file naming every required record kind.
const operatorJSON = `{
  "provider": "Example Cloud",
  "account_tier": "Developer",
  "client_host": "test host",
  "records": [
    {"kind": "limits", "path": "r.txt", "url": "https://example.test/limits", "retrieved": "2026-10-07"},
    {"kind": "prices", "path": "r.txt", "url": "https://example.test/pricing", "retrieved": "2026-10-07"},
    {"kind": "quota", "path": "r.txt", "retrieved": "2026-10-07"},
    {"kind": "terms", "path": "r.txt", "url": "https://example.test/terms", "retrieved": "2026-10-07"},
    {"kind": "status_mapping", "path": "r.txt", "url": "https://example.test/errors", "retrieved": "2026-10-07"},
    {"kind": "eligibility", "path": "r.txt", "retrieved": "2026-10-07"},
    {"kind": "dedicated_account", "path": "r.txt", "retrieved": "2026-10-07"},
    {"kind": "affiliation", "path": "r.txt", "retrieved": "2026-10-07"}
  ]
}`

// fixture is a route's input files in temporary directories.
type fixture struct {
	in     routeInput
	ledger string
}

// writeFile writes b to name in dir and returns its path.
func writeFile(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// fakeSNTP puts an sntp on PATH that prints one reading of offset, and
// returns the clock-sync command.
func fakeSNTP(t *testing.T, offset string) string {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\necho '" + offset + " +/- 0.010257 time.example.test 192.0.2.1'\n"
	if err := os.WriteFile(filepath.Join(dir, "sntp"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return "sntp time.example.test"
}

// releaseConfig is the release reference at endpoint, with the test key
// set in its variable.
func releaseConfig(t *testing.T, endpoint string) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Target.EndpointURL = endpoint
	t.Setenv(cfg.Target.APIKeyEnv, testKey)
	return cfg
}

// routeConfigFile writes the release reference as route name at endpoint
// with routeUSD as its cap, and returns the file's path.
func routeConfigFile(t *testing.T, name, endpoint, routeUSD string) string {
	t.Helper()
	b, err := os.ReadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, r := range [][2]string{
		{"\n  name: groq\n", "\n  name: " + name + "\n"},
		{"endpoint_url: https://api.groq.com/openai/v1/chat/completions", "endpoint_url: " + endpoint},
		{`route_usd: "10"`, `route_usd: "` + routeUSD + `"`},
	} {
		if strings.Count(s, r[0]) != 1 {
			t.Fatalf("reference config: %q not found once", r[0])
		}
		s = strings.Replace(s, r[0], r[1], 1)
	}
	return writeFile(t, t.TempDir(), name+".yaml", []byte(s))
}

// testSchedule is a schedule over 17 to 26 October 2026 for routes.
func testSchedule(t *testing.T, routes ...string) *schedule.Schedule {
	t.Helper()
	s, err := schedule.New(schedule.Input{MasterSeed: "percentes-release-test", First: "2026-10-17", Last: "2026-10-26", Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// newFixture writes every input of cfg's route: its schedule beside
// "other", a ledger with the route's caps and price, an operator file, a
// ceiling probe that passed at 512, and a clock reading of offset.
func newFixture(t *testing.T, cfg *config.Config, offset string) *fixture {
	t.Helper()
	dir := t.TempDir()
	sb, err := testSchedule(t, cfg.Target.Name, "other").JSON()
	if err != nil {
		t.Fatal(err)
	}
	ledger := filepath.Join(dir, "ledger.jsonl")
	caps, err := admission.ReleaseCaps(cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := admission.Create(ledger, caps)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.RecordPrice(cfg.Target.Name, admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.60", InputTokens: 1200, OutputTokens: 512,
		Source: "https://example.test/pricing", Retrieved: "2026-10-06"}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "r.txt", []byte("record"))
	cp, err := json.Marshal(&probe.ReleaseRecord{SchemaVersion: 1, Route: cfg.Target.Name, Ceiling: probe.CeilingCheck{Ceiling: 512, LengthValue: "length", Sent: 10, Passed: 10, Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{
		in: routeInput{
			cfg: cfg, configName: "groq.yaml", schedulePath: writeFile(t, dir, "schedule.json", sb), out: t.TempDir(), ledgerPath: ledger,
			framesRoot: t.TempDir(), clockSync: fakeSNTP(t, offset), statusCmd: "true", opPath: writeFile(t, dir, "operator.json", []byte(operatorJSON)),
			probeFiles: []string{writeFile(t, dir, "ceiling.json", cp)}, byteCap: 2000, accountCreated: "2026-09-15", build: testBuild,
		},
		ledger: ledger,
	}
}

// enoughOpenFiles stubs the open-file limit at the required value.
func enoughOpenFiles(t *testing.T) {
	t.Helper()
	orig := openFilesLimit
	openFilesLimit = func() (uint64, error) { return minOpenFiles, nil }
	t.Cleanup(func() { openFilesLimit = orig })
}

// shortLadder scales r's ladder to a 1 s warm-up and 2 s windows with 1 s
// drains at steps of 2, 4, 8, 16 and 32 requests a second, with a 1 s
// CPU window, once the ceiling rule has read the pinned ladder.
func shortLadder(r *route) {
	r.cfg.Release.Ladder = config.Ladder{StepsRPS: []float64{2, 4, 8, 16, 32}, WarmupS: 1, StepS: 2, DrainS: 1}
	r.cfg.ClientValidity.CPUWindowS = 1
}

// fastDispatch shortens the running dispatch series' timing for a test.
func fastDispatch(t *testing.T) {
	t.Helper()
	flush, lag, wait := dispatchFlush, dispatchLag, dispatchWaitMax
	dispatchFlush, dispatchLag, dispatchWaitMax = 50*time.Millisecond, 50*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { dispatchFlush, dispatchLag, dispatchWaitMax = flush, lag, wait })
}

// readCell reads a cell's record.
func readCell(t *testing.T, r *route, run schedule.Run) *cellRecord {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(r.cellDir(run), cellFile))
	if err != nil {
		t.Fatal(err)
	}
	var rec cellRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return &rec
}

// noWait returns at once.
func noWait(context.Context, time.Time) error { return nil }
