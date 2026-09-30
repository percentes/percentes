package main

import (
	"context"
	"fmt"
	"time"

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/run"
)

// victimResolver names the pod a clean-delete run kills.
type victimResolver interface {
	orchestrator.PodOps
	PodReady(ctx context.Context, namespace, pod string) (bool, error)
	ReadyPods(ctx context.Context, namespace, selector string) ([]string, error)
}

// victimPlan is how each run picks its victim: a fixed pod name, or a
// selector whose first Ready pod by name dies once all replicas are Ready.
type victimPlan struct {
	namespace    string
	victim       string
	selector     string
	replicas     int
	readyTimeout time.Duration
	poll         time.Duration
}

// resolveVictim waits, up to readyTimeout, for the plan's pods to be Ready
// and returns the run's victim.
func resolveVictim(ctx context.Context, ops victimResolver, p victimPlan) (string, error) {
	deadline := time.Now().Add(p.readyTimeout)
	for {
		pod, ready, err := p.pick(ctx, ops)
		if err != nil {
			return "", err
		}
		if ready {
			return pod, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("not ready within %s", p.readyTimeout)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(p.poll):
		}
	}
}

func (p victimPlan) pick(ctx context.Context, ops victimResolver) (pod string, ready bool, err error) {
	if p.selector == "" {
		ok, err := ops.PodReady(ctx, p.namespace, p.victim)
		return p.victim, ok, err
	}
	names, err := ops.ReadyPods(ctx, p.namespace, p.selector)
	if err != nil {
		return "", false, err
	}
	want := p.replicas
	if want < 1 {
		want = 1
	}
	if len(names) < want {
		return "", false, nil
	}
	return names[0], true, nil
}

// cleanDeleteRunner wraps inner so every run resolves its own victim and
// arms its own injector.
func cleanDeleteRunner(ops victimResolver, p victimPlan, inner campaign.Runner) campaign.Runner {
	return func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		p.replicas = c.Target.Replicas
		pod, err := resolveVictim(ctx, ops, p)
		if err != nil {
			return nil, fmt.Errorf("clean_delete victim: %w", err)
		}
		o.VictimReplica = pod
		o.Injector = orchestrator.NewCleanDeleteInjector(ops, p.namespace, pod)
		return inner(ctx, c, o)
	}
}
