package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ContainerState is one docker inspect of the replica container.
type ContainerState struct {
	Running       bool      `json:"running"`
	Pid           int       `json:"pid"`
	ID            string    `json:"id"`
	StartedAt     time.Time `json:"started_at"`  // host clock
	FinishedAt    time.Time `json:"finished_at"` // host clock
	RestartCount  int       `json:"restart_count"`
	RestartPolicy string    `json:"restart_policy"`
}

// KillRecord is one run of the kill script: local send and return, remote
// stamps around the kill. Remote stamps that do not parse are 0.
type KillRecord struct {
	Pid            int       `json:"pid"`
	Signal         int       `json:"signal"`
	SentAt         time.Time `json:"sent_at"`
	ReturnedAt     time.Time `json:"returned_at"`
	RemoteBeforeNs int64     `json:"remote_before_ns"`
	RemoteAfterNs  int64     `json:"remote_after_ns"`
}

// ClockOffset is host minus client from round trips; Samples is 0 locally.
type ClockOffset struct {
	OffsetNs int64     `json:"offset_ns"`
	BoundNs  int64     `json:"bound_ns"`
	Samples  int       `json:"samples"`
	At       time.Time `json:"at"`
}

// ContainerEvent is one die or start event from docker events.
type ContainerEvent struct {
	Action string    `json:"action"` // "die" | "start"
	At     time.Time `json:"at"`     // host clock
}

const (
	ContainerOpTimeout   = 10 * time.Second
	ContainerLogsTimeout = 60 * time.Second
)

// ContainerOps is the seam the process-kill injector and runner use. Times
// passed in are host clock; the caller converts with the offset. Every
// method bounds its own call with the timeouts above.
type ContainerOps interface {
	Inspect(ctx context.Context, container string) (ContainerState, error)
	// KillInit sends signal to pid after checking containerID in /proc/pid/cgroup.
	KillInit(ctx context.Context, pid int, containerID string, signal int) (KillRecord, error)
	Logs(ctx context.Context, container string, since time.Time) ([]byte, error)
	Events(ctx context.Context, container string, since, until time.Time) ([]ContainerEvent, error)
	ClockOffset(ctx context.Context) (ClockOffset, error)
	Fingerprint(ctx context.Context, container string) (string, error)
}

// ProcessKillInjector fires a host-side SIGKILL at T_inject. Target and
// Offset are resolved by the caller before Arm.
type ProcessKillInjector struct {
	Ops       ContainerOps
	Container string
	Target    ContainerState
	Offset    ClockOffset

	mu     sync.Mutex
	fired  *time.Time
	expiry *time.Time
	armErr error
	gen    int
	cancel context.CancelFunc
	kill   *KillRecord
}

// NewProcessKillInjector constructs a ProcessKillInjector that kills
// target's init process in container, converting host stamps with offset.
func NewProcessKillInjector(ops ContainerOps, container string, target ContainerState, offset ClockOffset) *ProcessKillInjector {
	return &ProcessKillInjector{Ops: ops, Container: container, Target: target, Offset: offset}
}

// Arm takes the fire time on entry and starts a fresh injection; state from
// an earlier Arm is cleared and its goroutine cancelled.
func (p *ProcessKillInjector) Arm(ctx context.Context, fireIn time.Duration, _ float64) error {
	fireAt := time.Now().Add(fireIn)
	if !p.Target.Running || p.Target.Pid <= 1 || p.Target.ID == "" {
		return fmt.Errorf("process-kill injector: container %s not killable (running=%t pid=%d id=%q)", p.Container, p.Target.Running, p.Target.Pid, p.Target.ID)
	}
	p.mu.Lock()
	if p.cancel != nil {
		p.cancel()
	}
	armCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.gen++
	gen := p.gen
	p.fired, p.expiry, p.armErr, p.kill = nil, nil, nil, nil
	p.mu.Unlock()
	go func() {
		timer := time.NewTimer(time.Until(fireAt))
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-armCtx.Done():
			p.record(gen, nil, armCtx.Err())
			return
		}
		rec, err := p.Ops.KillInit(armCtx, p.Target.Pid, p.Target.ID, 9)
		p.record(gen, &rec, err)
	}()
	return nil
}

// record stores the outcome of the arm generation gen, or drops it when a
// later Arm has superseded it.
func (p *ProcessKillInjector) record(gen int, rec *KillRecord, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen != p.gen {
		return
	}
	if err != nil {
		p.armErr = err
		return
	}
	at := observedFire(*rec, p.Offset)
	p.kill = rec
	p.fired, p.expiry = &at, &at // point event: expiry == fire
}

// observedFire is the remote bracket midpoint on the client clock, or the
// local midpoint when a remote stamp is missing. It is SentAt shifted by a
// wall-clock difference, so it keeps SentAt's monotonic reading.
func observedFire(rec KillRecord, off ClockOffset) time.Time {
	if rec.RemoteBeforeNs != 0 && rec.RemoteAfterNs != 0 {
		mid := (rec.RemoteBeforeNs + rec.RemoteAfterNs) / 2
		return rec.SentAt.Add(time.Duration(mid - off.OffsetNs - rec.SentAt.UnixNano()))
	}
	return rec.SentAt.Add(rec.ReturnedAt.Sub(rec.SentAt) / 2)
}

func (p *ProcessKillInjector) Observed(context.Context) (fired, expired *time.Time, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fired, p.expiry, p.armErr
}

// Kill returns the fired kill's record, nil until fired.
func (p *ProcessKillInjector) Kill() *KillRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.kill == nil {
		return nil
	}
	rec := *p.kill
	return &rec
}

// FireUncertainty is FireUncertaintyOf the fired kill with the arm offset
// alone; 0 until fired.
func (p *ProcessKillInjector) FireUncertainty() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.kill == nil {
		return 0
	}
	return FireUncertaintyOf(*p.kill, p.Offset, p.Offset)
}

// FireUncertaintyOf is half the remote bracket plus the larger of the two
// offset bounds plus the offset's change from at to after, or half the
// local round trip when a remote stamp is missing.
func FireUncertaintyOf(k KillRecord, at, after ClockOffset) time.Duration {
	if k.RemoteBeforeNs == 0 || k.RemoteAfterNs == 0 {
		return k.ReturnedAt.Sub(k.SentAt) / 2
	}
	drift := after.OffsetNs - at.OffsetNs
	if drift < 0 {
		drift = -drift
	}
	return time.Duration((k.RemoteAfterNs-k.RemoteBeforeNs)/2 + max(at.BoundNs, after.BoundNs) + drift)
}
