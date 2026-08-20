package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// The Phase 1 injectors implement the same Injector contract the mock
// injector does (Arm/Observed), so the orchestrator stays agnostic beyond
// timestamps (§2). Cluster operations go through narrow seams (PodOps,
// nodeOps) with a real client (kubectl-backed) and a fake for tests, so
// the pre-armed lifecycle and timestamp recording are verified without a
// cluster.

// PodOps is the cluster operation the clean-delete injector needs.
type PodOps interface {
	// DeletePodGrace0 deletes the pod with grace period 0 (abrupt
	// deletion: endpoint withdrawn near-instantly, kernel RSTs in-flight
	// connections, §1). Returns the observed deletion time.
	DeletePodGrace0(ctx context.Context, namespace, pod string) (time.Time, error)
}

// CleanDeleteInjector fires a grace=0 pod deletion at T_inject (§1
// "abrupt replica deletion", never called node loss). A clean delete is a
// point event: the fault window is effectively zero (the endpoint is
// withdrawn near-instantly), so expiry is reported at fire — recovery is
// measured by the detector and probes, not by the orchestrator.
type CleanDeleteInjector struct {
	Ops       PodOps
	Namespace string
	Pod       string

	mu     sync.Mutex
	fired  *time.Time
	expiry *time.Time
	armErr error
}

// NewCleanDeleteInjector constructs a CleanDeleteInjector that deletes pod in
// namespace with grace=0 at T_inject (§1).
func NewCleanDeleteInjector(ops PodOps, namespace, pod string) *CleanDeleteInjector {
	return &CleanDeleteInjector{Ops: ops, Namespace: namespace, Pod: pod}
}

func (c *CleanDeleteInjector) Arm(ctx context.Context, fireIn time.Duration, _ float64) error {
	if c.Pod == "" {
		return fmt.Errorf("clean-delete injector: no victim pod")
	}
	go func() {
		timer := time.NewTimer(fireIn)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			c.mu.Lock()
			c.armErr = ctx.Err()
			c.mu.Unlock()
			return
		}
		at, err := c.Ops.DeletePodGrace0(ctx, c.Namespace, c.Pod)
		c.mu.Lock()
		if err != nil {
			c.armErr = err
		} else {
			c.fired, c.expiry = &at, &at // point event: expiry == fire
		}
		c.mu.Unlock()
	}()
	return nil
}

func (c *CleanDeleteInjector) Observed(context.Context) (fired, expired *time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fired, c.expiry, c.armErr
}

// nodeOps is the cluster operations the node-partition injector needs.
type nodeOps interface {
	// ArmNodePartition installs a PRE-ARMED full-node DROP-all that fires
	// after fireIn and auto-removes durationS later (Chaos Mesh
	// NetworkChaos with a duration, or an iptables DROP-all installed by
	// a pre-armed job with a scheduled removal, §1). Because the node
	// becomes unreachable the instant it fires, arming MUST complete
	// before the fire time — nothing may depend on reaching the node
	// after fire.
	ArmNodePartition(ctx context.Context, node string, fireIn time.Duration, durationS float64) error
	// PartitionStatus reports the observed fire/expiry of a previously
	// armed partition (from the pre-armed job's own records / rule
	// presence), nil until they happen.
	PartitionStatus(ctx context.Context, node string) (fired, expired *time.Time, err error)
}

// nodePartitionInjector fires the pre-armed full-node partition,
// node-loss-representative for the degradation window only (§1).
// It records armed/fire/expiry via the pre-armed job's own timestamps,
// so the orchestrator never needs to reach the partitioned node.
// Unexported Phase 1 scaffolding: no real nodeOps exists until the
// multi-node bring-up.
type nodePartitionInjector struct {
	Ops  nodeOps
	Node string

	armed bool
}

// newNodePartitionInjector constructs a nodePartitionInjector for node behind
// the given nodeOps seam (Phase 1 scaffolding, §1).
func newNodePartitionInjector(ops nodeOps, node string) *nodePartitionInjector {
	return &nodePartitionInjector{Ops: ops, Node: node}
}

func (n *nodePartitionInjector) Arm(ctx context.Context, fireIn time.Duration, durationS float64) error {
	if n.Node == "" {
		return fmt.Errorf("node-partition injector: no victim node")
	}
	if fireIn <= 0 {
		return fmt.Errorf("node-partition injector: partition must be pre-armed (fireIn=%s)", fireIn)
	}
	// Defense in depth: config validation upstream already requires a
	// positive duration, but a partition without automatic expiry
	// permanently kills the node — the §1 MUST lives here too.
	if durationS <= 0 {
		return fmt.Errorf("node-partition injector: automatic expiry is mandatory (§1), got duration %.3fs", durationS)
	}
	if err := n.Ops.ArmNodePartition(ctx, n.Node, fireIn, durationS); err != nil {
		return err
	}
	n.armed = true
	return nil
}

func (n *nodePartitionInjector) Observed(ctx context.Context) (fired, expired *time.Time, err error) {
	if !n.armed {
		return nil, nil, fmt.Errorf("node-partition injector: not armed")
	}
	return n.Ops.PartitionStatus(ctx, n.Node)
}
