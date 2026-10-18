package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/pilot"
)

// testBuild is a build stamped with a clean revision.
var testBuild = &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "0123456789abcdef0123456789abcdef01234567"}, {Key: "vcs.modified", Value: "false"}}}

// routeMock starts a shaped mock that answers every fixture ticket with
// its frozen label, and returns its endpoint URL.
func routeMock(t *testing.T) string {
	t.Helper()
	fx, err := fixture()
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]string{}
	for _, set := range [][]pilot.Ticket{fx.Measured, fx.Development} {
		for _, c := range set {
			routes[c.Text] = fmt.Sprintf(`{"route":%q}`, c.Label)
		}
	}
	fixed := config.LatencyDist{Distribution: config.DistributionFixed, FixedMs: 1}
	s := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed}, mock.Shape{Routes: routes})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return "http://" + s.Addr() + "/v1/chat/completions"
}

// statusServer answers every request with status and returns its
// endpoint URL.
func statusServer(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/v1/chat/completions"
}

// testConfig is the pilot reference at the two endpoint URLs, with each
// key set, scaled to rate requests a second over warmupS and measuredS.
func testConfig(t *testing.T, urls [2]string, rate, warmupS, measuredS float64) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for i := range cfg.Pilot.Targets {
		cfg.Pilot.Targets[i].EndpointURL = urls[i]
		t.Setenv(cfg.Pilot.Targets[i].APIKeyEnv, "test-key-"+cfg.Pilot.Targets[i].Name+"-0123456789")
	}
	cfg.Load.RateRPS = rate
	cfg.Run.Phases.WarmupS, cfg.Run.Phases.BaselineS = warmupS, measuredS
	return cfg
}

// testLedger creates a ledger with a price for each endpoint of cfg.
func testLedger(t *testing.T, cfg *config.Config) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	l, err := admission.Create(path, admission.PilotCaps())
	if err != nil {
		t.Fatal(err)
	}
	for _, tg := range cfg.Pilot.Targets {
		if err := l.RecordPrice(tg.EndpointURL, admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.60", Source: "https://example.test/pricing", Retrieved: "2026-10-06"}); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

// operatorJSON is an operator file naming every record kind a session
// window requires.
const operatorJSON = `{
  "provider": "Example Cloud",
  "account_tier": "Basic",
  "client_host": "test host",
  "cell": "pilot test",
  "roles": {"task": "Percentes", "cases_labels": "generator", "rubric": "draft", "pair_interest": "none"},
  "records": [
    {"kind": "limits", "path": "r.txt", "url": "https://example.test/limits", "retrieved": "2026-10-06"},
    {"kind": "prices", "path": "r.txt", "url": "https://example.test/pricing", "retrieved": "2026-10-06"},
    {"kind": "quota", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "terms", "path": "r.txt", "url": "https://example.test/terms", "retrieved": "2026-10-06"},
    {"kind": "status_mapping", "path": "r.txt", "url": "https://example.test/errors", "retrieved": "2026-10-06"},
    {"kind": "eligibility", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "dedicated_account", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "affiliation", "path": "r.txt", "retrieved": "2026-10-06"}%s
  ]
}`

const pilotRecordsJSON = `,
    {"kind": "cases", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "labels", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "rubric", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "scorer", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "prompt", "path": "r.txt", "retrieved": "2026-10-06"},
    {"kind": "schedule", "path": "r.txt", "retrieved": "2026-10-06"}`

// writeOperator writes an operator file and its record file, with the
// pilot record kinds when pilotRecords is set, and returns its path.
func writeOperator(t *testing.T, pilotRecords bool) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "r.txt"), []byte("record"), 0o600); err != nil {
		t.Fatal(err)
	}
	extra := ""
	if pilotRecords {
		extra = pilotRecordsJSON
	}
	path := filepath.Join(dir, "operator.json")
	if err := os.WriteFile(path, []byte(fmt.Sprintf(operatorJSON, extra)), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeSNTP puts an sntp on PATH that prints one reading, and returns the
// clock-sync command.
func fakeSNTP(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\necho '+0.005032 +/- 0.010257 time.apple.com 17.253.108.125'\n"
	if err := os.WriteFile(filepath.Join(dir, "sntp"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return "sntp time.apple.com"
}

// testSchedule writes the schedule input for cfg's targets and returns
// the file and the derived schedule.
func testSchedule(t *testing.T, cfg *config.Config) (string, *pilot.Schedule) {
	t.Helper()
	in := pilot.ScheduleInput{MasterSeed: "percentes-pilot-test", Freeze: "2026-10-07", Last: "2026-10-31", Endpoints: [2]string{cfg.Pilot.Targets[0].Name, cfg.Pilot.Targets[1].Name}}
	s, err := pilot.NewSchedule(in)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "schedule.json")
	body := fmt.Sprintf(`{"input": {"master_seed": %q, "freeze": %q, "last": %q, "endpoints": [%q, %q]}}`, in.MasterSeed, in.Freeze, in.Last, in.Endpoints[0], in.Endpoints[1])
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, s
}

// sessionFixture is a session's inputs against local endpoints.
type sessionFixture struct {
	in     *sessionInput
	ledger string
	frames string
}

func newSessionFixture(t *testing.T, cfg *config.Config, label string) *sessionFixture {
	t.Helper()
	schedPath, _ := testSchedule(t, cfg)
	sched, sum, err := readSchedule(schedPath, cfg)
	if err != nil {
		t.Fatal(err)
	}
	targets, err := selectTargets(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	var opFlags []string
	for _, tg := range targets {
		opFlags = append(opFlags, tg.Name+"="+writeOperator(t, true))
	}
	ops, err := loadOperators(opFlags, targets, testBuild)
	if err != nil {
		t.Fatal(err)
	}
	ledger := testLedger(t, cfg)
	frames := t.TempDir()
	deps, err := newWindowDeps(ledger, frames, fakeSNTP(t), "true", "pilot.yaml", testBuild)
	if err != nil {
		t.Fatal(err)
	}
	return &sessionFixture{
		in: &sessionInput{
			cfg: cfg, configName: "pilot.yaml", schedule: sched, scheduleSH: sum,
			label: label, out: t.TempDir(), local: true, operators: ops, deps: deps,
			now: func() time.Time { return time.Now() },
		},
		ledger: ledger,
		frames: frames,
	}
}

// ledgerUsage returns the ledger's charge for endpoint.
func ledgerUsage(t *testing.T, ledger, endpoint string) admission.Usage {
	t.Helper()
	l, err := admission.Open(ledger)
	if err != nil {
		t.Fatal(err)
	}
	st, err := l.State()
	if err != nil {
		t.Fatal(err)
	}
	u := st.Routes[endpoint]
	if u == nil {
		return admission.Usage{}
	}
	return *u
}
