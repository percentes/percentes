package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeContainerOps scripts inspect states, the clock offset, the remote
// stamps around a kill and the kill's outcome.
type fakeContainerOps struct {
	mu        sync.Mutex
	states    []ContainerState // Inspect returns these in turn; the last repeats
	offset    ClockOffset
	stampLag  time.Duration // remote stamps trail the local send by this much
	bracket   time.Duration // remote after minus remote before
	killDelay time.Duration // KillInit returns this long after stamping
	noStamps  bool
	killErr   error
	kills     []KillRecord
	killIDs   []string
}

func (f *fakeContainerOps) Inspect(context.Context, string) (ContainerState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.states) == 0 {
		return ContainerState{}, errors.New("no state scripted")
	}
	st := f.states[0]
	if len(f.states) > 1 {
		f.states = f.states[1:]
	}
	return st, nil
}

func (f *fakeContainerOps) KillInit(_ context.Context, pid int, containerID string, signal int) (KillRecord, error) {
	rec := KillRecord{Pid: pid, Signal: signal, SentAt: time.Now()}
	if f.killErr != nil {
		rec.ReturnedAt = time.Now()
		return rec, f.killErr
	}
	if !f.noStamps {
		before := rec.SentAt.Add(f.stampLag).UnixNano() + f.offset.OffsetNs
		rec.RemoteBeforeNs, rec.RemoteAfterNs = before, before+int64(f.bracket)
	}
	time.Sleep(f.killDelay)
	rec.ReturnedAt = time.Now()
	f.mu.Lock()
	f.kills = append(f.kills, rec)
	f.killIDs = append(f.killIDs, containerID)
	f.mu.Unlock()
	return rec, nil
}

func (f *fakeContainerOps) Logs(context.Context, string, time.Time) ([]byte, error) { return nil, nil }

func (f *fakeContainerOps) Events(context.Context, string, time.Time, time.Time) ([]ContainerEvent, error) {
	return nil, nil
}

func (f *fakeContainerOps) ClockOffset(context.Context) (ClockOffset, error) { return f.offset, nil }

func (f *fakeContainerOps) Fingerprint(context.Context, string) (string, error) { return "", nil }

func (f *fakeContainerOps) killCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.kills)
}

var liveTarget = ContainerState{Running: true, Pid: 4242, ID: "c0ffee", RestartCount: 1, RestartPolicy: "on-failure"}

// Interface assertions for the process-kill seam.
var (
	_ Injector     = (*ProcessKillInjector)(nil)
	_ ContainerOps = (*fakeContainerOps)(nil)
	_ ContainerOps = SSHContainerOps{}
)

func TestProcessKillThroughExecute(t *testing.T) {
	ops := &fakeContainerOps{offset: ClockOffset{OffsetNs: int64(3 * time.Second), BoundNs: int64(time.Millisecond), Samples: 5}, bracket: 2 * time.Millisecond}
	inj := NewProcessKillInjector(ops, "vllm", liveTarget, ops.offset)

	ts, err := Execute(context.Background(), inj, time.Now(), 300*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if ts.ObservedFire == nil || ts.ObservedExpiry == nil || !ts.ObservedExpiry.Equal(*ts.ObservedFire) {
		t.Fatalf("expiry must equal fire: %+v", ts)
	}
	k := inj.Kill()
	if k == nil || k.RemoteBeforeNs == 0 || k.RemoteAfterNs == 0 || k.Pid != 4242 || k.Signal != 9 {
		t.Fatalf("kill record: %+v", k)
	}
	if ops.killIDs[0] != "c0ffee" {
		t.Errorf("kill guarded by %q, want the target id", ops.killIDs[0])
	}
	want := time.Unix(0, (k.RemoteBeforeNs+k.RemoteAfterNs)/2-ops.offset.OffsetNs)
	if !ts.ObservedFire.Equal(want) {
		t.Errorf("observed fire %v, want the converted bracket midpoint %v", ts.ObservedFire, want)
	}
	if !strings.Contains(ts.ObservedFire.String(), " m=") {
		t.Errorf("observed fire %v carries no monotonic reading", ts.ObservedFire)
	}
	if errMs, _ := ts.FireErrorMs(); errMs < -500 || errMs > 500 {
		t.Errorf("fire error %.1fms exceeds tolerance", errMs)
	}
}

func TestObservedFireFromRemoteBracket(t *testing.T) {
	off := ClockOffset{OffsetNs: -int64(7 * time.Second), BoundNs: int64(3 * time.Millisecond), Samples: 5}
	ops := &fakeContainerOps{offset: off, stampLag: 50 * time.Millisecond, bracket: 4 * time.Millisecond}
	inj := NewProcessKillInjector(ops, "vllm", liveTarget, off)

	ts, err := Execute(context.Background(), inj, time.Now(), 200*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	k := inj.Kill()
	want := k.SentAt.Add(50*time.Millisecond + 2*time.Millisecond)
	if d := ts.ObservedFire.Sub(want); d < -time.Microsecond || d > time.Microsecond {
		t.Errorf("observed fire %v follows the local send, want the stamps' %v", ts.ObservedFire, want)
	}
	if got, want := inj.FireUncertainty(), 2*time.Millisecond+3*time.Millisecond; got != want {
		t.Errorf("fire uncertainty %v, want half bracket plus bound %v", got, want)
	}
}

func TestFireUncertaintyOfAddsBoundAndDrift(t *testing.T) {
	k := KillRecord{RemoteBeforeNs: 1_000_000_000, RemoteAfterNs: 1_004_000_000}
	at := ClockOffset{OffsetNs: 5_000_000, BoundNs: 1_000_000}
	after := ClockOffset{OffsetNs: 2_000_000, BoundNs: 3_000_000}
	if got, want := FireUncertaintyOf(k, at, after), 2*time.Millisecond+3*time.Millisecond+3*time.Millisecond; got != want {
		t.Errorf("uncertainty %v, want half bracket plus larger bound plus drift %v", got, want)
	}
	if got, want := FireUncertaintyOf(k, at, at), 3*time.Millisecond; got != want {
		t.Errorf("uncertainty %v without an after offset, want %v", got, want)
	}
}

func TestObservedFireFallsBackToLocalMidpoint(t *testing.T) {
	ops := &fakeContainerOps{noStamps: true, killDelay: 20 * time.Millisecond}
	inj := NewProcessKillInjector(ops, "vllm", liveTarget, ClockOffset{OffsetNs: int64(time.Hour)})
	ts, err := Execute(context.Background(), inj, time.Now(), 200*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	k := inj.Kill()
	if want := k.SentAt.Add(k.ReturnedAt.Sub(k.SentAt) / 2); !ts.ObservedFire.Equal(want) {
		t.Errorf("observed fire %v, want the local midpoint %v", ts.ObservedFire, want)
	}
	if got, want := inj.FireUncertainty(), k.ReturnedAt.Sub(k.SentAt)/2; got != want {
		t.Errorf("fire uncertainty %v, want half the local round trip %v", got, want)
	}
}

func TestProcessKillArmLatencyDoesNotDelayFire(t *testing.T) {
	ops := &fakeContainerOps{killDelay: 400 * time.Millisecond, bracket: time.Millisecond}
	inj := NewProcessKillInjector(ops, "vllm", liveTarget, ops.offset)

	ts, err := Execute(context.Background(), inj, time.Now(), 200*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	errMs, err := ts.FireErrorMs()
	if err != nil {
		t.Fatal(err)
	}
	if errMs < -50 || errMs > 50 {
		t.Errorf("fire error from the stamps %.1fms, want under 50ms", errMs)
	}
}

func TestProcessKillRefusesDeadTarget(t *testing.T) {
	for name, target := range map[string]ContainerState{
		"stopped": {Running: false, Pid: 4242, ID: "c0ffee"},
		"pid 0":   {Running: true, Pid: 0, ID: "c0ffee"},
		"pid 1":   {Running: true, Pid: 1, ID: "c0ffee"},
		"no id":   {Running: true, Pid: 4242},
	} {
		ops := &fakeContainerOps{}
		inj := NewProcessKillInjector(ops, "vllm", target, ClockOffset{})
		if err := inj.Arm(context.Background(), 50*time.Millisecond, 0); err == nil {
			t.Errorf("%s: arm accepted %+v", name, target)
		}
		time.Sleep(100 * time.Millisecond)
		if n := ops.killCount(); n != 0 {
			t.Errorf("%s: %d kills sent", name, n)
		}
	}
}

func TestProcessKillSurfacesTerminalFailure(t *testing.T) {
	wantErr := errors.New("kill: exit status 3: pid 4242 is not in container c0ffee")
	inj := NewProcessKillInjector(&fakeContainerOps{killErr: wantErr}, "vllm", liveTarget, ClockOffset{})

	start := time.Now()
	ts, err := Execute(context.Background(), inj, start, 200*time.Millisecond, 0)
	if !errors.Is(err, wantErr) {
		t.Fatalf("execute must surface the kill cause, got %v (%+v)", err, ts)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("execute must abort promptly on terminal failure, took %v", elapsed)
	}
	if inj.Kill() != nil {
		t.Error("a failed kill left a kill record")
	}
}

func TestProcessKillArmClearsThePreviousRun(t *testing.T) {
	ops := &fakeContainerOps{}
	inj := NewProcessKillInjector(ops, "vllm", liveTarget, ClockOffset{})
	var first *KillRecord
	for i := 0; i < 2; i++ {
		epoch := time.Now()
		ts, err := Execute(context.Background(), inj, epoch, 200*time.Millisecond, 0)
		if err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
		if ts.ObservedFire == nil || ts.ObservedFire.Before(epoch) {
			t.Fatalf("run %d reported a fire from before its own epoch: %v", i+1, ts.ObservedFire)
		}
		if i == 0 {
			first = inj.Kill()
		} else if k := inj.Kill(); k == nil || !k.SentAt.After(first.SentAt) {
			t.Fatalf("run 2 kept run 1's kill record: %+v", k)
		}
	}
	if n := ops.killCount(); n != 2 {
		t.Fatalf("each run must kill once, got %d", n)
	}
}

func TestClockOffsetMedianAndBound(t *testing.T) {
	base := time.Unix(1_790_000_000, 0)
	var samples []clockSample
	// offsets in ms, half round trips in ms
	for i, s := range []struct{ off, half int64 }{{5, 1}, {1, 2}, {3, 4}, {100, 1}, {2, 3}} {
		sent := base.Add(time.Duration(i) * time.Second)
		mid := sent.Add(time.Duration(s.half) * time.Millisecond)
		samples = append(samples, clockSample{
			sent:     sent,
			returned: mid.Add(time.Duration(s.half) * time.Millisecond),
			remoteNs: mid.UnixNano() + s.off*int64(time.Millisecond),
		})
	}
	got := offsetFrom(samples)
	if got.OffsetNs != int64(3*time.Millisecond) {
		t.Errorf("offset %v, want the median 3ms", time.Duration(got.OffsetNs))
	}
	if got.BoundNs != int64(4*time.Millisecond) {
		t.Errorf("bound %v, want the largest half round trip 4ms", time.Duration(got.BoundNs))
	}
	if got.Samples != 5 {
		t.Errorf("samples %d, want 5", got.Samples)
	}
}
