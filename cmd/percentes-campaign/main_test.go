package main

import (
	"context"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/run"
)

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
