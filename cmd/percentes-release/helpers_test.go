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

// fakeBalance puts a balance command on PATH that prints a balance of
// US$30.00 and, on stderr, its own read count, kept in a file beside it,
// and returns the command.
func fakeBalance(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	count := filepath.Join(dir, "count")
	script := "#!/bin/sh\nn=$(($(cat '" + count + "' 2>/dev/null || echo 0) + 1))\necho $n > '" + count + "'\necho \"read $n\" >&2\necho '" + fakeBalanceOut + "'\n"
	if err := os.WriteFile(filepath.Join(dir, "balance"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return "balance --account test"
}

// fakeBalanceOut is fakeBalance's standard output, less its newline.
const fakeBalanceOut = `{"amount_usd": "30.00"}`

// rateLimitHeaders is the release reference's response_headers line.
const rateLimitHeaders = "  response_headers: [Retry-After, x-ratelimit-*]\n"

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
// with routeUSD as its cap, and returns the file's path. The reference's
// other host, baseten, becomes groq when name is baseten.
func routeConfigFile(t *testing.T, name, endpoint, routeUSD string) string {
	t.Helper()
	b, err := os.ReadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	other := "baseten"
	if name == other {
		other = "groq"
	}
	for _, r := range [][2]string{
		{"\n  name: groq\n", "\n  name: " + name + "\n"},
		{"\n    - route: baseten\n", "\n    - route: " + other + "\n"},
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
// ceiling probe of each host of its model that passed at 512, a clock
// reading of offset, a counting balance command and a refresh command
// that touches the record file and prints cfg's recorded limits.
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
	if _, err := recordPrice(l, cfg, "0.15", "0.60", "https://example.test/pricing", "2026-10-06"); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "r.txt", []byte("record"))
	refresh := filepath.Join(dir, "refresh")
	if err := writeMockRefresh(refresh, cfg); err != nil {
		t.Fatal(err)
	}
	var probeFiles []string
	for _, route := range append([]string{cfg.Target.Name}, otherHosts(cfg)...) {
		probeFiles = append(probeFiles, writeCeiling(t, hostCeiling(cfg, 2000, route, 512, true)))
	}
	return &fixture{
		in: routeInput{
			cfg: cfg, configName: "groq.yaml", schedulePath: writeFile(t, dir, "schedule.json", sb), out: t.TempDir(), ledgerPath: ledger,
			framesRoot: t.TempDir(), clockSync: fakeSNTP(t, offset), statusCmd: "true", balanceCmd: fakeBalance(t), opPath: writeFile(t, dir, "operator.json", []byte(operatorJSON)),
			refreshCmd: refresh + " " + filepath.Join(dir, "r.txt"), probeFiles: probeFiles, byteCap: 2000, accountCreated: "2026-09-15", build: testBuild,
		},
		ledger: ledger,
	}
}

// otherHosts are the routes of cfg's release.other_hosts.
func otherHosts(cfg *config.Config) []string {
	var out []string
	for _, h := range cfg.Release.OtherHosts {
		out = append(out, h.Route)
	}
	return out
}

// hostCeiling is host route's ceiling check record at ceiling, passed or
// not, of cfg's model at byteCap: cfg's request when route is cfg's, else
// route's model from release.other_hosts, expected as requested.
func hostCeiling(cfg *config.Config, byteCap int, route string, ceiling int, pass bool) probe.ReleaseRecord {
	rec := *passedCeiling(cfg, byteCap)
	rec.Route, rec.Ceiling.Ceiling = route, ceiling
	for _, h := range cfg.Release.OtherHosts {
		if h.Route == route {
			rec.RequestedModel, rec.ExpectedModel = h.Model, h.Model
		}
	}
	if !pass {
		rec.Ceiling.Passed, rec.Ceiling.Pass = probe.CeilingRequests-1, false
	}
	return rec
}

// writeCeiling writes rec to a new file and returns its path.
func writeCeiling(t *testing.T, rec probe.ReleaseRecord) string {
	t.Helper()
	b, err := json.Marshal(&rec)
	if err != nil {
		t.Fatal(err)
	}
	return writeFile(t, t.TempDir(), probe.File, b)
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
