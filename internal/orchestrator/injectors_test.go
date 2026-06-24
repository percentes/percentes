package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakePodOps records deletions and stamps a controllable deletion time.
type fakePodOps struct {
	mu       sync.Mutex
	deleted  []string
	failWith error
}

func (f *fakePodOps) DeletePodGrace0(ctx context.Context, ns, pod string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return time.Time{}, f.failWith
	}
	f.deleted = append(f.deleted, ns+"/"+pod)
	return time.Now(), nil
}

// A clean-delete injector, driven through the real orchestrator.Execute,
// fires within tolerance and records armed/fire/expiry (expiry==fire for
// a point event).
func TestCleanDeleteThroughExecute(t *testing.T) {
	ops := &fakePodOps{}
	inj := NewCleanDeleteInjector(ops, "chaosserve", "victim-pod")

	epoch := time.Now()
	ts, err := Execute(context.Background(), inj, epoch, 300*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if ts.ObservedFire == nil || ts.ObservedExpiry == nil {
		t.Fatalf("clean-delete must record fire and expiry: %+v", ts)
	}
	errMs, _ := ts.FireErrorMs()
	if errMs < -500 || errMs > 500 {
		t.Errorf("clean-delete fire error %.1fms exceeds tolerance", errMs)
	}
	if len(ops.deleted) != 1 || ops.deleted[0] != "chaosserve/victim-pod" {
		t.Errorf("exactly the victim pod must be deleted grace=0: %v", ops.deleted)
	}
	// Point event: expiry coincides with fire (no armed window).
	if !ts.ObservedExpiry.Equal(*ts.ObservedFire) {
		t.Errorf("clean-delete expiry must coincide with fire: fire=%v expiry=%v", ts.ObservedFire, ts.ObservedExpiry)
	}
}

// A grace=0 deletion that fails is a terminal injection failure: Execute
// must surface the real cause immediately, not swallow it into the generic
// "fault did not expire" timeout (Observed's error contract, §2).
func TestCleanDeleteSurfacesTerminalFailure(t *testing.T) {
	wantErr := errors.New(`pods "victim-pod" is forbidden`)
	ops := &fakePodOps{failWith: wantErr}
	inj := NewCleanDeleteInjector(ops, "chaosserve", "victim-pod")

	start := time.Now()
	ts, err := Execute(context.Background(), inj, start, 200*time.Millisecond, 1.0)
	if err == nil {
		t.Fatalf("execute must fail when the grace=0 delete fails: %+v", ts)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("execute must surface the delete cause, got: %v", err)
	}
	// It must abort on the terminal error, not wait out planned-expiry+5s.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("execute must abort promptly on terminal failure, took %v", elapsed)
	}
}

// fakeNodeOps models a pre-armed partition with its own fire/expiry
// schedule (as the on-node job would record them).
type fakeNodeOps struct {
	mu            sync.Mutex
	armed         bool
	armedFireIn   time.Duration
	armedDuration float64
	fireAt, expAt time.Time
}

func (f *fakeNodeOps) ArmNodePartition(ctx context.Context, node string, fireIn time.Duration, durationS float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = true
	f.armedFireIn, f.armedDuration = fireIn, durationS
	f.fireAt = time.Now().Add(fireIn)
	f.expAt = f.fireAt.Add(time.Duration(durationS * float64(time.Second)))
	return nil
}

func (f *fakeNodeOps) PartitionStatus(ctx context.Context, node string) (fired, expired *time.Time, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	if !f.armed || now.Before(f.fireAt) {
		return nil, nil, nil
	}
	fr := f.fireAt
	if now.Before(f.expAt) {
		return &fr, nil, nil
	}
	ex := f.expAt
	return &fr, &ex, nil
}

// The node-partition injector is genuinely pre-armed (arming completes
// before fire) and reports fire/expiry from the on-node schedule via
// Execute, within tolerance.
func TestNodePartitionThroughExecute(t *testing.T) {
	ops := &fakeNodeOps{}
	inj := newNodePartitionInjector(ops, "gpu-node-1")

	epoch := time.Now()
	ts, err := Execute(context.Background(), inj, epoch, 400*time.Millisecond, 1.0)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !ops.armed {
		t.Fatal("partition must be armed")
	}
	// Arming happened before the fire time (pre-armed).
	if ts.ArmedAt.After(ts.PlannedFireAt) {
		t.Errorf("partition must be pre-armed: armed %v, planned fire %v", ts.ArmedAt, ts.PlannedFireAt)
	}
	if ts.ObservedFire == nil || ts.ObservedExpiry == nil {
		t.Fatalf("partition must record fire and expiry: %+v", ts)
	}
	errMs, _ := ts.FireErrorMs()
	if errMs < -500 || errMs > 500 {
		t.Errorf("partition fire error %.1fms exceeds tolerance", errMs)
	}
	if d := ts.ObservedExpiry.Sub(*ts.ObservedFire).Seconds(); d < 0.8 || d > 1.3 {
		t.Errorf("partition window ~1s (auto-expiry), got %.2fs", d)
	}
}

// A node partition must be pre-armed: a non-positive fireIn is refused
// (nothing may reach the node after fire).
func TestNodePartitionRefusesImmediate(t *testing.T) {
	inj := newNodePartitionInjector(&fakeNodeOps{}, "gpu-node-1")
	if err := inj.Arm(context.Background(), 0, 1.0); err == nil {
		t.Error("node partition with fireIn<=0 must be refused (pre-armed only)")
	}
}

// A node partition without automatic expiry permanently kills the node;
// §1 makes pre-armed expiry a MUST. Config validation prevents this
// upstream, but the injector encodes the invariant itself.
func TestNodePartitionRequiresExpiry(t *testing.T) {
	inj := newNodePartitionInjector(&fakeNodeOps{}, "gpu-node-1")
	if err := inj.Arm(context.Background(), time.Second, 0); err == nil {
		t.Error("node partition with duration<=0 must be refused (automatic expiry is mandatory, §1)")
	}
}

// Both injectors satisfy the Injector interface.
var (
	_ Injector = (*CleanDeleteInjector)(nil)
	_ Injector = (*nodePartitionInjector)(nil)
	_ PodOps   = KubectlPodOps{}
)
