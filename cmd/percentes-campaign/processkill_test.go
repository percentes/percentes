package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/run"
)

// hostAhead is the fake host clock's lead over the client clock.
const hostAhead = 2 * time.Second

// fakeContainerOps scripts a container that reports not running for its
// first notRunningFor inspections and restarts on a SIGKILL, advancing
// its restart count by restartDelta. Its host clock runs hostAhead.
type fakeContainerOps struct {
	mu            sync.Mutex
	state         orchestrator.ContainerState
	notRunningFor int
	restartDelta  int
	fpErr         error
	inspectErr    error
	inspectErrs   int // the first inspectErrs inspections fail with inspectErr
	killErr       error
	inspects      int
	kills         []orchestrator.KillRecord
	killHost      time.Time
	logsSince     time.Time
}

func newFakeOps() *fakeContainerOps {
	return &fakeContainerOps{
		state:        orchestrator.ContainerState{Running: true, Pid: 4242, ID: "c0ffee", StartedAt: time.Now().Add(hostAhead - time.Hour), RestartCount: 1, RestartPolicy: "on-failure"},
		restartDelta: 1,
	}
}

func (f *fakeContainerOps) Inspect(context.Context, string) (orchestrator.ContainerState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspects++
	if f.inspectErr != nil && (f.inspectErrs == 0 || f.inspects <= f.inspectErrs) {
		return orchestrator.ContainerState{}, f.inspectErr
	}
	st := f.state
	if f.inspects <= f.notRunningFor {
		st.Running = false
	}
	return st, nil
}

func (f *fakeContainerOps) KillInit(_ context.Context, pid int, cid string, sig int) (orchestrator.KillRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if pid != f.state.Pid || cid != f.state.ID {
		return orchestrator.KillRecord{}, fmt.Errorf("pid %d is not in container %s", pid, cid)
	}
	if sig == 9 && f.killErr != nil {
		return orchestrator.KillRecord{}, f.killErr
	}
	sent := time.Now()
	host := sent.Add(hostAhead)
	rec := orchestrator.KillRecord{Pid: pid, Signal: sig, SentAt: sent, ReturnedAt: sent.Add(3 * time.Millisecond),
		RemoteBeforeNs: host.Add(time.Millisecond).UnixNano(), RemoteAfterNs: host.Add(2 * time.Millisecond).UnixNano()}
	f.kills = append(f.kills, rec)
	if sig == 9 {
		f.killHost = host.Add(1500 * time.Microsecond)
		f.state.RestartCount += f.restartDelta
		f.state.StartedAt = f.killHost.Add(200 * time.Millisecond)
		f.state.Pid++
	}
	return rec, nil
}

func (f *fakeContainerOps) Logs(_ context.Context, _ string, since time.Time) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logsSince = since
	stamp := func(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000000Z") }
	return []byte(stamp(f.killHost.Add(-time.Second)) + " 2026/10/03 12:00:00 mockserver: serving on [::]:8000 (old boot)\n" +
		stamp(f.killHost.Add(10*time.Millisecond)) + " 2026/10/03 12:00:01 mockserver: shutting down\n" +
		stamp(f.killHost.Add(300*time.Millisecond)) + " 2026/10/03 12:00:01 mockserver: serving on [::]:8000 (seed=42, scheduled faults=0, slow_reload=true)\n"), nil
}

func (f *fakeContainerOps) Events(context.Context, string, time.Time, time.Time) ([]orchestrator.ContainerEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []orchestrator.ContainerEvent{{Action: "die", At: f.killHost.Add(10 * time.Millisecond)}, {Action: "start", At: f.killHost.Add(200 * time.Millisecond)}}, nil
}

func (f *fakeContainerOps) ClockOffset(context.Context) (orchestrator.ClockOffset, error) {
	return orchestrator.ClockOffset{OffsetNs: int64(hostAhead), BoundNs: int64(time.Millisecond), Samples: 5, At: time.Now()}, nil
}

func (f *fakeContainerOps) Fingerprint(context.Context, string) (string, error) {
	if f.fpErr != nil {
		return "", f.fpErr
	}
	return "# fingerprint fake\n", nil
}

// replicaServer answers /health with 503 for its first healthFails calls
// and serves a one-token stream on /v1/chat/completions.
type replicaServer struct {
	*httptest.Server
	health, inference atomic.Int64
}

func newReplicaServer(t *testing.T, healthFails int64) *replicaServer {
	t.Helper()
	rs := &replicaServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			if rs.health.Add(1) <= healthFails {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "/v1/chat/completions":
			rs.inference.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(rs.Close)
	return rs
}

func pkPlanFor(t *testing.T, url string, profile config.Profile) processKillPlan {
	return processKillPlan{container: "pct-mock", probeURL: url, outDir: t.TempDir(), profile: profile, readyTimeout: 2 * time.Second, poll: time.Millisecond}
}

func pkConfig() *config.Config {
	c := &config.Config{Profile: config.ProfileAC}
	c.Fault.Variant = config.VariantProcessKill
	c.Target.Replicas = 1
	return c
}

// fakeInner fires the supplied injector 30 ms after its epoch and returns
// one request ending inside the fire uncertainty and one outside it.
func fakeInner(called *int) campaign.Runner {
	return func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		*called++
		epoch := time.Now()
		ts, err := orchestrator.Execute(ctx, o.Injector, epoch, 30*time.Millisecond, 0)
		if err != nil {
			return nil, err
		}
		fireNs := ts.ObservedFire.Sub(epoch).Nanoseconds()
		reqs := []loadgen.Request{
			{Index: 0, DispatchNs: fireNs - 1e9, DoneNs: fireNs + 1e5, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset},
			{Index: 1, DispatchNs: fireNs - 1e9, DoneNs: fireNs + 1e9, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset},
		}
		return &run.Artifacts{Config: c, Loadgen: &loadgen.Result{EpochWall: epoch, Requests: reqs}, Orchestration: ts, ActualFireNs: fireNs,
			Decomposition: detect.NewDecomposition(c.Fault.Variant), RunValid: true}, nil
	}
}

// The run starts only once the container runs, /health answers 200 and
// one inference is served, and it is armed against the inspected target.
func TestProcessKillRunnerWaitsForHealthAndInference(t *testing.T) {
	ops := newFakeOps()
	ops.notRunningFor = 2
	srv := newReplicaServer(t, 3)
	var health, inference int64
	var target orchestrator.ContainerState
	calls := 0
	inner := func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		health, inference = srv.health.Load(), srv.inference.Load()
		if pk, ok := o.Injector.(*orchestrator.ProcessKillInjector); ok {
			target = pk.Target
		}
		return fakeInner(&calls)(ctx, c, o)
	}
	art, err := processKillRunner(ops, pkPlanFor(t, srv.URL, config.ProfileAC), inner)(context.Background(), pkConfig(), run.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if health < 4 || inference < 1 {
		t.Fatalf("run started after %d health and %d inference requests", health, inference)
	}
	if ops.inspects < 3 || !target.Running || target.Pid != 4242 || target.ID != "c0ffee" {
		t.Fatalf("armed against %+v after %d inspections", target, ops.inspects)
	}
	if !art.RunValid {
		t.Fatalf("invalid: %v", art.InvalidReasons)
	}
}

// A container that never runs fails the run at the ready timeout before
// any load; an inspection finding no such container fails it at once.
func TestProcessKillRunnerFailsWhenNeverReady(t *testing.T) {
	ops := newFakeOps()
	ops.notRunningFor = 1 << 30
	srv := newReplicaServer(t, 0)
	p := pkPlanFor(t, srv.URL, config.ProfileAC)
	p.readyTimeout = 20 * time.Millisecond
	calls := 0
	_, err := processKillRunner(ops, p, fakeInner(&calls))(context.Background(), pkConfig(), run.Options{})
	if err == nil || !strings.Contains(err.Error(), "container not running") || calls != 0 {
		t.Fatalf("err %v after %d runs", err, calls)
	}
	ops = newFakeOps()
	ops.inspectErr = errors.New("docker inspect pct-mock: exit status 1: no such object")
	p.readyTimeout = time.Hour
	start := time.Now()
	_, err = processKillRunner(ops, p, fakeInner(&calls))(context.Background(), pkConfig(), run.Options{})
	if err == nil || !strings.Contains(err.Error(), "no such object") || ops.inspects != 1 || time.Since(start) > 5*time.Second {
		t.Fatalf("inspect failure: err %v after %d inspections", err, ops.inspects)
	}
}

// A transient inspection failure is retried until the container is ready.
func TestProcessKillRunnerRetriesTransientInspectErrors(t *testing.T) {
	ops := newFakeOps()
	ops.inspectErr = errors.New("docker inspect pct-mock: timed out after 10s: context deadline exceeded")
	ops.inspectErrs = 2
	srv := newReplicaServer(t, 0)
	calls := 0
	art, err := processKillRunner(ops, pkPlanFor(t, srv.URL, config.ProfileAC), fakeInner(&calls))(context.Background(), pkConfig(), run.Options{})
	if err != nil || calls != 1 || !art.RunValid {
		t.Fatalf("err %v after %d runs, art %+v", err, calls, art)
	}
}

// A restart policy or container name other than the §6 pins invalidates
// the run, as does a restart between runs.
func TestProcessKillRunnerChecksPinsAndRestartsBetweenRuns(t *testing.T) {
	srv := newReplicaServer(t, 0)
	c := pkConfig()
	c.Pins.Container = &config.ContainerPins{RestartPolicy: config.PinnedContainerRestartPolicy, Name: "pct-mock"}
	ops := newFakeOps()
	ops.state.RestartPolicy = "always"
	calls := 0
	art, err := processKillRunner(ops, pkPlanFor(t, srv.URL, config.ProfileAC), fakeInner(&calls))(context.Background(), c, run.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if art.RunValid || !strings.Contains(strings.Join(art.InvalidReasons, ";"), `restart policy "always", pinned "on-failure"`) {
		t.Fatalf("restart policy always kept the run valid: %v", art.InvalidReasons)
	}
	p := pkPlanFor(t, srv.URL, config.ProfileAC)
	p.container = "other"
	art, err = processKillRunner(newFakeOps(), p, fakeInner(&calls))(context.Background(), c, run.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if art.RunValid || !strings.Contains(strings.Join(art.InvalidReasons, ";"), `container "other", pinned "pct-mock"`) {
		t.Fatalf("container name: %v", art.InvalidReasons)
	}

	ops = newFakeOps()
	runner := processKillRunner(ops, pkPlanFor(t, srv.URL, config.ProfileAC), fakeInner(&calls))
	if art, err = runner(context.Background(), c, run.Options{}); err != nil || !art.RunValid {
		t.Fatalf("run 1: %v %v", err, art.InvalidReasons)
	}
	ops.mu.Lock()
	ops.state.RestartCount++
	ops.mu.Unlock()
	if art, err = runner(context.Background(), c, run.Options{}); err != nil {
		t.Fatal(err)
	}
	if art.RunValid || len(art.InvalidReasons) != 1 || art.InvalidReasons[0] != "container restarted 1 times between runs" {
		t.Fatalf("run 2: valid %v reasons %v", art.RunValid, art.InvalidReasons)
	}
}

// The runner splits the in-flight requests at the fire and collects the
// requests scheduled in [fire, replica_ready) as the outage window.
func TestProcessKillRunnerSplitsInFlightAndCollectsOutage(t *testing.T) {
	c, err := config.LoadFile(filepath.Join("..", "..", "configs", "process-kill-mock.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	srv := newReplicaServer(t, 0)
	inner := func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		epoch := time.Now()
		ts, err := orchestrator.Execute(ctx, o.Injector, epoch, 30*time.Millisecond, 0)
		if err != nil {
			return nil, err
		}
		fireNs := ts.ObservedFire.Sub(epoch).Nanoseconds()
		reqs := []loadgen.Request{
			{Index: 0, IntendedNs: fireNs - 2e9, DispatchNs: fireNs - 2e9, DoneNs: fireNs + 1e5, Outcome: loadgen.OutcomeCompleted},
			{Index: 1, IntendedNs: fireNs - 2e9, DispatchNs: fireNs - 2e9, DoneNs: fireNs - 1e5, Outcome: loadgen.OutcomeCompleted},
			{Index: 2, IntendedNs: fireNs - 1e9, DispatchNs: fireNs - 1e9, DoneNs: fireNs + 1e9, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset},
			{Index: 3, IntendedNs: fireNs + 1e9, DispatchNs: fireNs + 1e9, DoneNs: fireNs + 1e9 + 1e6, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrConnect},
			{Index: 4, IntendedNs: fireNs + 3e9, DispatchNs: fireNs + 3e9, DoneNs: fireNs + 3e9 + 1e6, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrConnect},
		}
		d := detect.NewDecomposition(c.Fault.Variant)
		fireWall := epoch.Add(time.Duration(fireNs))
		d.SetMeasured("replica_ready", fireWall, fireWall.Add(2*time.Second))
		return &run.Artifacts{Config: c, Loadgen: &loadgen.Result{EpochWall: epoch, Requests: reqs}, Orchestration: ts, ActualFireNs: fireNs,
			Decomposition: d, RunValid: true}, nil
	}
	art, err := processKillRunner(newFakeOps(), pkPlanFor(t, srv.URL, config.ProfileAC), inner)(context.Background(), c, run.Options{})
	if err != nil {
		t.Fatal(err)
	}
	det := art.InFlight.Determinate
	if art.InFlight.IndeterminateAtFire != 1 || det == nil || det.Total != 1 || det.Completed != 0 || det.Errored != 1 {
		t.Fatalf("indeterminate %d, determinate %+v", art.InFlight.IndeterminateAtFire, det)
	}
	if want := art.Container.FireUncertaintyNs + int64(time.Millisecond); art.Container.IndeterminateZoneNs != want {
		t.Fatalf("zone %d, want the uncertainty plus the offset bound %d", art.Container.IndeterminateZoneNs, want)
	}
	o := art.Windows["outage"]
	if o == nil || o.Scheduled != 1 || o.Errored != 1 || o.ErrClasses[loadgen.ErrConnect] != 1 {
		t.Fatalf("outage window %+v", o)
	}
	if got := o.Window.EndNs - o.Window.StartNs; got != int64(2*time.Second) {
		t.Fatalf("outage window spans %d ns, want replica_ready", got)
	}
}

// A restart count that did not advance by exactly one invalidates the run.
func TestProcessKillRunnerRestartCountMustAdvanceByOne(t *testing.T) {
	srv := newReplicaServer(t, 0)
	for delta, want := range map[int]string{0: "restart count advanced by 0, expected 1", 2: "restart count advanced by 2, expected 1", 1: ""} {
		ops := newFakeOps()
		ops.restartDelta = delta
		calls := 0
		art, err := processKillRunner(ops, pkPlanFor(t, srv.URL, config.ProfileAC), fakeInner(&calls))(context.Background(), pkConfig(), run.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if want == "" {
			if !art.RunValid {
				t.Fatalf("delta 1 invalid: %v", art.InvalidReasons)
			}
			continue
		}
		if art.RunValid || len(art.InvalidReasons) != 1 || art.InvalidReasons[0] != want {
			t.Fatalf("delta %d: valid %v reasons %v", delta, art.RunValid, art.InvalidReasons)
		}
	}
}

// The run carries the restart record, the converted container start, the
// mock boundary and the indeterminate count, and its files are written.
func TestProcessKillRunnerAttachesContainerRecordAndFiles(t *testing.T) {
	ops := newFakeOps()
	srv := newReplicaServer(t, 0)
	p := pkPlanFor(t, srv.URL, config.ProfileAC)
	calls := 0
	runner := processKillRunner(ops, p, fakeInner(&calls))
	if _, err := runner(context.Background(), pkConfig(), run.Options{}); err != nil {
		t.Fatal(err)
	}
	art, err := runner(context.Background(), pkConfig(), run.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rec := art.Container
	if rec == nil || rec.Kill == nil || rec.Kill.Signal != 9 || rec.Before.RestartCount != 2 || rec.After.RestartCount != 3 {
		t.Fatalf("restart record %+v", rec)
	}
	if rec.OffsetAtArm.OffsetNs != int64(hostAhead) || rec.OffsetAfter.Samples != 5 || rec.FireUncertaintyNs != 1_500_000 {
		t.Fatalf("offsets %+v %+v, uncertainty %d", rec.OffsetAtArm, rec.OffsetAfter, rec.FireUncertaintyNs)
	}
	if len(rec.Events) != 2 || rec.DieToStartS == nil || *rec.DieToStartS < 0.189 || *rec.DieToStartS > 0.191 {
		t.Fatalf("events %+v die to start %v", rec.Events, rec.DieToStartS)
	}
	fire := *art.Orchestration.ObservedFire
	if got := ops.logsSince; !got.Equal(fire.Add(hostAhead - captureLead)) {
		t.Fatalf("logs read from %s, want 5 s before the fire in host time %s", got, fire.Add(hostAhead-captureLead))
	}
	m, ok := rec.Boundaries.Matches["server_start"]
	if !ok || !strings.Contains(m.Text, "seed=42") || m.Stamp != "docker" {
		t.Fatalf("server_start must come from the restart's line: %+v", rec.Boundaries)
	}
	for _, s := range art.Decomposition.Segments {
		switch s.Name {
		case "container_start":
			if d := s.DurationS(); d == nil || *d < 0.19 || *d > 0.21 || s.Note != "" {
				t.Fatalf("container_start %+v", s)
			}
		case "log_bringup", "engine_ready":
			if s.Measured || !strings.Contains(s.Note, "not found in the log after the fire") {
				t.Fatalf("%s under the mock pattern set: %+v", s.Name, s)
			}
		}
	}
	if art.InFlight.IndeterminateAtFire != 1 {
		t.Fatalf("indeterminate %d, want 1", art.InFlight.IndeterminateAtFire)
	}
	for _, name := range []string{"run-2-server.log", "run-2-fingerprint-before.txt", "run-2-fingerprint-after.txt"} {
		b, err := os.ReadFile(filepath.Join(p.outDir, name))
		if err != nil || len(b) == 0 {
			t.Fatalf("%s: %v (%d bytes)", name, err, len(b))
		}
	}
	if b, _ := os.ReadFile(rec.LogPath); !strings.Contains(string(b), "mockserver: serving on") || rec.LogBytes != len(b) {
		t.Fatalf("server log %s: %d bytes recorded, %q", rec.LogPath, rec.LogBytes, b)
	}

	// A fingerprint failure invalidates the run under the experiment
	// profile and is written into the file under the ac profile.
	ops.fpErr = errors.New("fingerprint: nvidia-smi: not found")
	art, err = processKillRunner(ops, p, fakeInner(&calls))(context.Background(), pkConfig(), run.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !art.RunValid {
		t.Fatalf("ac profile invalidated on a fingerprint failure: %v", art.InvalidReasons)
	}
	if b, _ := os.ReadFile(filepath.Join(p.outDir, "run-1-fingerprint-before.txt")); !strings.Contains(string(b), "nvidia-smi: not found") {
		t.Fatalf("fingerprint file %q", b)
	}
	exp := pkPlanFor(t, srv.URL, config.ProfileExperiment)
	art, err = processKillRunner(ops, exp, fakeInner(&calls))(context.Background(), pkConfig(), run.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if art.RunValid || !strings.Contains(strings.Join(art.InvalidReasons, ";"), "nvidia-smi: not found") {
		t.Fatalf("experiment profile kept a run without a fingerprint: %v", art.InvalidReasons)
	}
}

// The dry kill records five signal-0 brackets, one SIGKILL through the
// orchestrator, the /health calibration and the restart's log.
func TestDryKillWritesRecord(t *testing.T) {
	defer func(d time.Duration) { dryKillTInject = d }(dryKillTInject)
	dryKillTInject = 50 * time.Millisecond
	ops := newFakeOps()
	srv := newReplicaServer(t, 0)
	p := pkPlanFor(t, srv.URL, config.ProfileAC)
	path, err := dryKill(context.Background(), ops, p, pkConfig())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rec dryKillRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.SignalZero) != 5 || len(rec.SignalZeroRoundTripMs) != 5 || len(rec.SignalZeroBracketMs) != 5 {
		t.Fatalf("signal-0 brackets %+v", rec.SignalZero)
	}
	for i, k := range rec.SignalZero {
		if k.Signal != 0 || rec.SignalZeroBracketMs[i] == nil {
			t.Fatalf("bracket %d: signal %d, %v ms", i, k.Signal, rec.SignalZeroBracketMs[i])
		}
	}
	if rec.Kill == nil || rec.Kill.Signal != 9 || rec.FireErrorMs == nil || rec.RestartCountAdvanced != 1 || rec.DieToStartS == nil {
		t.Fatalf("kill record %+v", rec)
	}
	if rec.HealthCalibration == nil || !rec.HealthCalibration.HealthOK || !rec.HealthCalibration.InferenceOK {
		t.Fatalf("health calibration %+v", rec.HealthCalibration)
	}
	lag := rec.CalibrationLagS
	if lag == nil || *lag <= 0 || rec.HealthReadyAfterFireS == nil || *rec.HealthReadyAfterFireS < *lag ||
		rec.InferenceReadyAfterFireS == nil || *rec.InferenceReadyAfterFireS < *lag {
		t.Fatalf("calibration from the fire: lag %v, health %v, inference %v", lag, rec.HealthReadyAfterFireS, rec.InferenceReadyAfterFireS)
	}
	if _, ok := rec.Boundaries.Matches["server_start"]; !ok || len(rec.Errors) != 0 {
		t.Fatalf("boundaries %+v errors %v", rec.Boundaries, rec.Errors)
	}
	if b, err := os.ReadFile(filepath.Join(p.outDir, "dry-kill-server.log")); err != nil || !strings.Contains(string(b), "mockserver: serving on") {
		t.Fatalf("dry-kill-server.log: %v", err)
	}
	if len(ops.kills) != 6 {
		t.Fatalf("%d kill-script calls, want five brackets and one kill", len(ops.kills))
	}
}

// A kill that fails still leaves dry-kill.json with the error.
func TestDryKillWritesRecordOnFailure(t *testing.T) {
	defer func(d time.Duration) { dryKillTInject = d }(dryKillTInject)
	dryKillTInject = 50 * time.Millisecond
	ops := newFakeOps()
	ops.killErr = errors.New("kill: timed out after 10s")
	srv := newReplicaServer(t, 0)
	p := pkPlanFor(t, srv.URL, config.ProfileAC)
	path, err := dryKill(context.Background(), ops, p, pkConfig())
	if err == nil || path == "" {
		t.Fatalf("path %q err %v", path, err)
	}
	raw, rerr := os.ReadFile(path)
	if rerr != nil {
		t.Fatal(rerr)
	}
	var rec dryKillRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.SignalZero) != 5 || len(rec.Errors) != 1 || !strings.Contains(rec.Errors[0], "timed out after 10s") {
		t.Fatalf("record %+v", rec)
	}
}
