package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/validity"
)

// mainEnv makes the test binary run main in place of the tests.
const mainEnv = "PERCENTES_TEST_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(mainEnv) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fakeResolver answers readiness from a script of per-call Ready lists.
type fakeResolver struct {
	ready   [][]string
	calls   int
	deleted []string
}

func (f *fakeResolver) DeletePodGrace0(_ context.Context, ns, pod string) (time.Time, error) {
	f.deleted = append(f.deleted, ns+"/"+pod)
	return time.Now(), nil
}

func (f *fakeResolver) next() []string {
	if f.calls >= len(f.ready) {
		return f.ready[len(f.ready)-1]
	}
	r := f.ready[f.calls]
	f.calls++
	return r
}

func (f *fakeResolver) PodReady(_ context.Context, _, pod string) (bool, error) {
	for _, n := range f.next() {
		if n == pod {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeResolver) ReadyPods(_ context.Context, _, _ string) ([]string, error) {
	return f.next(), nil
}

func plan(victim, selector string) victimPlan {
	return victimPlan{namespace: "percentes", victim: victim, selector: selector, readyTimeout: time.Second, poll: time.Millisecond}
}

// Each run waits for every replica to be Ready, resolves the selector
// afresh and arms its own injector.
func TestCleanDeleteRunnerResolvesEveryRun(t *testing.T) {
	ops := &fakeResolver{ready: [][]string{{"mock-a", "mock-b"}, {"mock-b"}, {"mock-b", "mock-c"}}}
	var victims []string
	var injectors []orchestrator.Injector
	inner := func(_ context.Context, _ *config.Config, o run.Options) (*run.Artifacts, error) {
		victims = append(victims, o.VictimReplica)
		injectors = append(injectors, o.Injector)
		return &run.Artifacts{}, nil
	}
	preset := orchestrator.NewCleanDeleteInjector(ops, "percentes", "stale")
	r := cleanDeleteRunner(ops, plan("", "app=percentes-mock"), inner)
	cfg := &config.Config{}
	cfg.Target.Replicas = 2
	for i := 0; i < 2; i++ {
		if _, err := r(context.Background(), cfg, run.Options{Injector: preset, VictimReplica: "stale"}); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	if victims[0] != "mock-a" || victims[1] != "mock-b" {
		t.Fatalf("victims not resolved per run once two pods were Ready: %v", victims)
	}
	if ops.calls != 3 {
		t.Fatalf("run 2 must wait for the second Ready pod: %d readiness reads", ops.calls)
	}
	if injectors[0] == injectors[1] || injectors[0] == preset || injectors[1] == preset {
		t.Fatal("an injector was reused across runs")
	}
	for i, inj := range injectors {
		cd, ok := inj.(*orchestrator.CleanDeleteInjector)
		if !ok || cd.Pod != victims[i] {
			t.Fatalf("run %d injector does not target its victim: %+v", i+1, inj)
		}
	}
}

// A run whose pods never become Ready fails at the timeout.
func TestCleanDeleteRunnerFailsWhenNeverReady(t *testing.T) {
	ops := &fakeResolver{ready: [][]string{{"mock-b"}}}
	inner := func(_ context.Context, _ *config.Config, _ run.Options) (*run.Artifacts, error) {
		return &run.Artifacts{}, nil
	}
	p := plan("", "app=percentes-mock")
	p.readyTimeout = 20 * time.Millisecond
	cfg := &config.Config{}
	cfg.Target.Replicas = 2
	if _, err := cleanDeleteRunner(ops, p, inner)(context.Background(), cfg, run.Options{}); err == nil {
		t.Fatal("one Ready pod of two was accepted")
	}
}

// A fixed victim name is used only once its pod is Ready again.
func TestCleanDeleteRunnerWaitsForTheFixedName(t *testing.T) {
	ops := &fakeResolver{ready: [][]string{{}, {"mock-a"}}}
	var victims []string
	inner := func(_ context.Context, _ *config.Config, o run.Options) (*run.Artifacts, error) {
		victims = append(victims, o.VictimReplica)
		return &run.Artifacts{}, nil
	}
	if _, err := cleanDeleteRunner(ops, plan("mock-a", ""), inner)(context.Background(), &config.Config{}, run.Options{}); err != nil {
		t.Fatalf("a pod that became Ready was refused: %v", err)
	}
	if victims[0] != "mock-a" || ops.calls != 2 {
		t.Fatalf("victim %v after %d reads", victims, ops.calls)
	}
}

// writeRun writes the run's JSON and text reports under its number.
func TestWriteRunWritesReportPair(t *testing.T) {
	dir := t.TempDir()
	art := &run.Artifacts{Config: &config.Config{}, Loadgen: &loadgen.Result{}, Windows: map[string]*collect.Stats{}, Detector: &detect.Result{}, Decomposition: detect.NewDecomposition(config.VariantProcessKill)}
	gate := &validity.Report{Gates: []validity.Gate{{ID: "G1", Detail: "single-replica target"}}}
	if err := writeRun(dir, 3, art, gate); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "run-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rep struct {
		InstrumentCommit string           `json:"instrument_commit"`
		ConfigSHA256     string           `json:"config_sha256"`
		ValidityGates    *validity.Report `json:"validity_gates"`
	}
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.InstrumentCommit == "" || rep.ConfigSHA256 == "" || rep.ValidityGates == nil || len(rep.ValidityGates.Gates) != 1 {
		t.Fatalf("run-3.json %+v", rep)
	}
	txt, err := os.ReadFile(filepath.Join(dir, "run-3.txt"))
	if err != nil || !strings.Contains(string(txt), "Percentes run report") || !strings.Contains(string(txt), "single-replica target") {
		t.Fatalf("run-3.txt: %v\n%s", err, txt)
	}
}

// The halt policy is on for process_kill unless the flag says otherwise,
// and off for every other variant unless the flag turns it on.
func TestHaltPolicyDefaultsOnForProcessKill(t *testing.T) {
	if !haltPolicy(false, false, config.VariantProcessKill) || haltPolicy(true, false, config.VariantProcessKill) {
		t.Fatal("process_kill default on, explicit =false off")
	}
	if haltPolicy(false, false, config.VariantCleanDelete) || !haltPolicy(true, true, config.VariantCleanDelete) {
		t.Fatal("other variants default off, explicit =true on")
	}
}

// The base and metrics overrides replace the file's values, are recorded,
// and a malformed base URL is refused.
func TestApplyOverridesRecordsReplacements(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/process-kill-mock.yaml")
	if err != nil {
		t.Fatal(err)
	}
	got, err := applyOverrides(cfg, "http://10.0.0.7:8000", "http://10.0.0.7:8000/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Target.BaseURL != "http://10.0.0.7:8000" || len(cfg.Target.MetricsURLs) != 1 || cfg.Target.MetricsURLs[0] != "http://10.0.0.7:8000/metrics" {
		t.Fatalf("config not replaced: %+v", cfg.Target)
	}
	if strings.Join(got, ",") != "target.base_url=http://10.0.0.7:8000,target.metrics_urls=http://10.0.0.7:8000/metrics" {
		t.Fatalf("recorded %v", got)
	}
	if got, err := applyOverrides(cfg, "", ""); err != nil || got != nil {
		t.Fatalf("no flags: %v %v", got, err)
	}
	if _, err := applyOverrides(cfg, "http://10.0.0.7:8000/", ""); err == nil {
		t.Fatal("a trailing slash was accepted")
	}
}

// The command refuses a hosted target before any request or setup; a
// self-hosted config passes that check.
func TestRefusesHostedTarget(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	ref, err := os.ReadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, pairs ...string) string {
		s := string(ref)
		for i := 0; i < len(pairs); i += 2 {
			if strings.Count(s, pairs[i]) != 1 {
				t.Fatalf("%q not once in the reference", pairs[i])
			}
			s = strings.Replace(s, pairs[i], pairs[i+1], 1)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	baseURL := "  base_url: http://127.0.0.1:18000\n"
	self := write("self.yaml", baseURL, "  base_url: "+srv.URL+"\n")
	hosted := write("hosted.yaml", baseURL, "  base_url: "+srv.URL+"\n  hosted: true\n  model_name: test-model\n  api_key_env: PERCENTES_TEST_KEY\n",
		"  ignore_eos: true        # pinned (§6)\n", "  ignore_eos: false\n")
	for _, path := range []string{self, hosted} {
		cfg, err := config.LoadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := run.CheckConfig(cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.Target.Hosted != (path == hosted) {
			t.Fatalf("%s: hosted %v", path, cfg.Target.Hosted)
		}
	}

	const refusal = "percentes-campaign: a hosted target runs under percentes-release or percentes-pilot only"
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"hosted", []string{"--config", hosted}, refusal},
		{"hosted with dry kill", []string{"--config", hosted, "--dry-kill"}, refusal},
		{"self-hosted with dry kill", []string{"--config", self, "--dry-kill"}, "percentes-campaign: --dry-kill applies to process_kill only"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], append(tc.args, "--out", filepath.Join(dir, "out"))...)
		cmd.Env = append(os.Environ(), mainEnv+"=1", "PERCENTES_TEST_KEY=test")
		cmd.WaitDelay = 5 * time.Second
		out, err := cmd.CombinedOutput()
		cancel()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), tc.want) {
			t.Errorf("%s: %v\n%s", tc.name, err, out)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("%d requests sent", n)
	}
	if _, err := os.Stat(filepath.Join(dir, "out")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("out directory: %v", err)
	}
}
