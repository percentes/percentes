// Package ac is the SPEC.md §8 acceptance-criteria suite. Reference
// conditions: lambda = 20 rps (pinned by the ac profile), stall D = 10 s
// where used. These tests run real load through the real generator
// against the in-process mock; they are skipped in -short mode and run
// without the race detector (timing fidelity).
//
// Printed by TestMain, per §8: passing AC1-AC7 certifies the instrument,
// not any real-GPU claim.
package ac

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/itsveems/chaosserve/internal/config"
	"github.com/itsveems/chaosserve/internal/loadgen"
	"github.com/itsveems/chaosserve/internal/mock"
)

// mockBin is the mockserver binary, built once per suite run. AC
// scenarios run the mock as a SEPARATE PROCESS: §6 pins the client to a
// dedicated node precisely so the generator and the system under test
// never share a scheduler, and the AC setup honors that boundary — a
// server-side burst must not be able to contaminate client send skew.
var mockBin string

func TestMain(m *testing.M) {
	fmt.Println("CAVEAT (SPEC.md §8): passing AC1-AC7 certifies the instrument against the mock, not any claim about real GPU behavior.")
	dir, err := os.MkdirTemp("", "chaosserve-ac-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)
	mockBin = filepath.Join(dir, "mockserver")
	build := exec.Command("go", "build", "-o", mockBin, "../../cmd/mockserver")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building mockserver: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// startMockProcess launches the mockserver binary on an ephemeral port
// with the given run config, registers its teardown on t, and returns its
// base URL.
func startMockProcess(t *testing.T, cfg *config.Config) string {
	t.Helper()
	base, stop, err := launchMock(cfg)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(stop)
	return base
}

// launchMock builds the run config to a temp file, starts the mockserver
// binary on an ephemeral port, and returns its base URL plus a stop func
// that terminates the process and removes the temp dir. It takes no
// *testing.T so the shared stall setup (getStallRun) can record a setup
// failure once and fail every consumer deterministically, rather than
// binding a fatal to whichever test first triggered the sync.Once.
func launchMock(cfg *config.Config) (baseURL string, stop func(), err error) {
	dir, err := os.MkdirTemp("", "chaosserve-mock-*")
	if err != nil {
		return "", nil, fmt.Errorf("ac: mock temp dir: %w", err)
	}
	cleanup := func() { os.RemoveAll(dir) } //nolint:errcheck
	cfgPath := filepath.Join(dir, "run.yaml")
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("ac: marshal run config: %w", err)
	}
	if err := os.WriteFile(cfgPath, raw, 0o644); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("ac: write run config: %w", err)
	}
	addrPath := filepath.Join(dir, "addr")

	cmd := exec.Command(mockBin, "--config", cfgPath, "--addr-file", addrPath)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("ac: start mockserver process: %w", err)
	}
	stop = func() {
		cmd.Process.Signal(syscall.SIGTERM) //nolint:errcheck
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }() //nolint:errcheck
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill() //nolint:errcheck
			<-done
		}
		cleanup()
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if raw, err := os.ReadFile(addrPath); err == nil && len(raw) > 0 {
			return "http://" + string(raw), stop, nil
		}
		if time.Now().After(deadline) {
			stop()
			return "", nil, fmt.Errorf("ac: mockserver never wrote its address")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// scenario builds a validated ac-profile config from the reference file
// with scenario-scale phases and the given mock behavior.
type scenario struct {
	warmupS, baselineS, windowS, cooldownS float64
	tInjectS                               float64 // offset from measurement start; >= baselineS
	ttft, itl                              config.LatencyDist
	schedule                               []config.MockFault
}

func buildConfig(t *testing.T, sc scenario) *config.Config {
	t.Helper()
	cfg, err := makeScenarioConfig(sc)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return cfg
}

// makeScenarioConfig builds a validated ac-profile config from the reference
// file off any *testing.T, returning an error rather than failing a test, so
// the shared stall setup can record a config failure once and fail every
// consumer deterministically.
func makeScenarioConfig(sc scenario) (*config.Config, error) {
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		return nil, fmt.Errorf("ac: load reference config: %w", err)
	}
	cfg.Run.Phases = config.Phases{WarmupS: sc.warmupS, BaselineS: sc.baselineS, FaultWindowTimeoutS: sc.windowS, CooldownS: sc.cooldownS}
	cfg.Fault.TInjectOffsetS = sc.tInjectS
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Mock.ListenAddr = "127.0.0.1:0"
	cfg.Mock.TTFT = sc.ttft
	cfg.Mock.ITL = sc.itl
	cfg.Mock.FaultSchedule = sc.schedule
	// AC scenarios run against a single mock process; the §1 share gate
	// applies to multi-replica topologies (the kind e2e exercises it).
	cfg.Target.Replicas = 1
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("ac: scenario config must validate (the AC suite runs pinned configs): %w", err)
	}
	return cfg, nil
}

// runScenario starts the mock as a separate process, points the generator
// at it, and runs the full schedule to terminal states. It returns the
// mock's base URL for out-of-band /admin queries.
func runScenario(t *testing.T, cfg *config.Config) (*loadgen.Result, string) {
	t.Helper()
	base := startMockProcess(t, cfg)
	cfg.Target.BaseURL = base

	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("loadgen run: %v", err)
	}
	return res, base
}

func fixed(ms float64) config.LatencyDist {
	return config.LatencyDist{Distribution: "fixed", FixedMs: ms}
}

// ---------------------------------------------------------------------------
// Shared reference stall run (used by AC2, AC2b, AC2c): fixed TTFT 500 ms,
// ITL 10 ms (nominal e2e = 500 + 255*10 = 3050 ms), stall D = 10 s
// starting 5 s into the fault window.
// ---------------------------------------------------------------------------

const (
	stallDS        = 10.0
	stallRunTTFTMs = 500.0
	stallRunITLMs  = 10.0
)

type stallRun struct {
	cfg     *config.Config
	res     *loadgen.Result
	fireNs  int64  // stall fire, run-relative ns (from mock's own record)
	invalid string // non-empty reason if the shared setup failed (checked by every consumer)
}

var (
	stallOnce sync.Once
	theStall  *stallRun
)

// getStallRun returns the memoized reference stall run, building it exactly
// once. Setup runs off any *testing.T (see buildStallRun): a setup failure is
// recorded into stallRun.invalid so EVERY consumer fails deterministically
// via t.Fatal, rather than only the test that first triggered the sync.Once.
func getStallRun(t *testing.T) *stallRun {
	stallOnce.Do(func() { theStall = buildStallRun() })
	if theStall.invalid != "" {
		t.Fatal(theStall.invalid)
	}
	return theStall
}

// buildStallRun performs the shared stall setup without a test-bound
// *testing.T, returning a stallRun whose invalid field carries any failure
// reason so the memoized value is never left partially initialized.
func buildStallRun() *stallRun {
	r := &stallRun{}
	// warmup 5 | baseline 30 | fault window 40 | cooldown 2; stall at
	// run offset 40 s (5 s into the fault window) so pre-stall
	// in-flight streams are fault-window traffic too.
	cfg, err := makeScenarioConfig(scenario{
		warmupS: 5, baselineS: 30, windowS: 40, cooldownS: 2, tInjectS: 35,
		ttft: fixed(stallRunTTFTMs), itl: fixed(stallRunITLMs),
		schedule: []config.MockFault{{Mode: config.MockFaultStall, StartOffsetS: 40, DurationS: stallDS}},
	})
	if err != nil {
		r.invalid = err.Error()
		return r
	}
	r.cfg = cfg

	base, stop, err := launchMock(cfg)
	if err != nil {
		r.invalid = err.Error()
		return r
	}
	defer stop()
	cfg.Target.BaseURL = base

	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		r.invalid = fmt.Sprintf("ac: stall loadgen run: %v", err)
		return r
	}
	r.res = res

	// The mock records fired/expired wall timestamps; convert to
	// run-relative offsets for exact window arithmetic.
	recs, err := fetchFaults(base)
	if err != nil {
		r.invalid = err.Error()
		return r
	}
	if len(recs) != 1 || recs[0].FiredAt == nil || recs[0].ExpiredAt == nil {
		r.invalid = fmt.Sprintf("stall record incomplete: %+v", recs)
		return r
	}
	r.fireNs = recs[0].FiredAt.Sub(res.EpochWall).Nanoseconds()
	return r
}

func adminFaults(t *testing.T, baseURL string) []mock.FaultRecord {
	t.Helper()
	recs, err := fetchFaults(baseURL)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return recs
}

// fetchFaults queries the mock's /admin/faults endpoint off any *testing.T,
// returning an error rather than failing a test so the shared stall setup can
// record the failure deterministically.
func fetchFaults(baseURL string) ([]mock.FaultRecord, error) {
	resp, err := http.Get(baseURL + "/admin/faults")
	if err != nil {
		return nil, fmt.Errorf("ac: admin faults: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		Faults []mock.FaultRecord `json:"faults"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("ac: decode admin faults: %w", err)
	}
	return out.Faults, nil
}

// completedIn returns completed requests with intended time in [fromNs, toNs).
func completedIn(res *loadgen.Result, fromNs, toNs int64) []loadgen.Request {
	var out []loadgen.Request
	for _, r := range res.Requests {
		if r.Outcome == loadgen.OutcomeCompleted && r.IntendedNs >= fromNs && r.IntendedNs < toNs {
			out = append(out, r)
		}
	}
	return out
}
