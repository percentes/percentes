package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// action is what the fault engine tells a stream handler to do at a gate.
type action int

const (
	actProceed action = iota // keep serving
	actAbort                 // RST the connection now (stream_abort)
	actHang                  // blackhole: no bytes, no FIN, no RST, ever
	actAbandon               // client is gone; return quietly
)

// admitVerdict is the engine's decision for a newly arrived request.
type admitVerdict struct {
	act action
	// httpError, when non-zero, means reject with this status (error mode).
	httpError int
	// abortAfterTokens >= 0 means RST after that many content tokens
	// (stream_abort window); -1 means no scheduled abort.
	abortAfterTokens int
}

// FaultRecord is the auditable lifecycle of one fault: armed at schedule
// load or admin call, fired at window start, expired at window end.
// Offsets are measured on the monotonic clock from server start; wall
// times are informational.
type FaultRecord struct {
	ID               int    `json:"id"`
	Mode             string `json:"mode"`
	Source           string `json:"source"` // "schedule" | "admin"
	AbortAfterTokens int    `json:"abort_after_tokens,omitempty"`

	PlannedStartOffsetS float64 `json:"planned_start_offset_s"`
	PlannedEndOffsetS   float64 `json:"planned_end_offset_s"`

	ArmedAt        time.Time  `json:"armed_at"`
	ArmedOffsetS   float64    `json:"armed_offset_s"`
	FiredAt        *time.Time `json:"fired_at,omitempty"`
	FiredOffsetS   *float64   `json:"fired_offset_s,omitempty"`
	ExpiredAt      *time.Time `json:"expired_at,omitempty"`
	ExpiredOffsetS *float64   `json:"expired_offset_s,omitempty"`

	State string `json:"state"` // "armed" | "active" | "expired"
}

// engine owns fault state. Exactly one fault window may be active at a
// time (config validation and admin arming both enforce non-overlap).
type engine struct {
	start time.Time     // monotonic anchor (server start)
	done  chan struct{} // closed on server shutdown; releases hung handlers

	mu       sync.Mutex
	records  []*FaultRecord
	active   *FaultRecord
	changeCh chan struct{} // closed and replaced on every fire/expire

	onFire func(mode string) // metrics hook, may be nil
}

func newEngine(start time.Time, done chan struct{}) *engine {
	return &engine{start: start, done: done, changeCh: make(chan struct{})}
}

func (e *engine) offsetS() float64 { return time.Since(e.start).Seconds() }

// arm registers a fault to fire at startOffsetS on the monotonic clock and
// expire durationS later. Overlap with any non-expired record is an error.
// It returns a copy: the fire/expire goroutine mutates the live record's
// timestamp and State fields, which must only be read under the engine
// lock; Mode and AbortAfterTokens are immutable after arm and may be read
// lock-free.
func (e *engine) arm(mode, source string, startOffsetS, durationS float64, abortAfterTokens int) (FaultRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	endOffsetS := startOffsetS + durationS
	for _, r := range e.records {
		if r.State == "expired" {
			continue
		}
		if startOffsetS < r.PlannedEndOffsetS && r.PlannedStartOffsetS < endOffsetS {
			return FaultRecord{}, fmt.Errorf("fault window [%gs,%gs) overlaps %s fault %d [%gs,%gs); one fault at a time",
				startOffsetS, endOffsetS, r.Mode, r.ID, r.PlannedStartOffsetS, r.PlannedEndOffsetS)
		}
	}

	now := time.Now()
	rec := &FaultRecord{
		ID:                  len(e.records) + 1,
		Mode:                mode,
		Source:              source,
		AbortAfterTokens:    abortAfterTokens,
		PlannedStartOffsetS: startOffsetS,
		PlannedEndOffsetS:   endOffsetS,
		ArmedAt:             now,
		ArmedOffsetS:        e.offsetS(),
		State:               "armed",
	}
	e.records = append(e.records, rec)

	fireDelay := time.Duration((startOffsetS - e.offsetS()) * float64(time.Second))
	if fireDelay < 0 {
		fireDelay = 0
	}
	go e.runWindow(rec, fireDelay, time.Duration(durationS*float64(time.Second)))
	return *rec, nil // copied while still under the lock
}

func (e *engine) runWindow(rec *FaultRecord, fireDelay, duration time.Duration) {
	fireTimer := time.NewTimer(fireDelay)
	defer fireTimer.Stop()
	select {
	case <-fireTimer.C:
	case <-e.done:
		return
	}
	e.fire(rec)

	expireTimer := time.NewTimer(duration)
	defer expireTimer.Stop()
	select {
	case <-expireTimer.C:
	case <-e.done:
		return
	}
	e.expire(rec)
}

func (e *engine) fire(rec *FaultRecord) {
	e.mu.Lock()
	now := time.Now()
	off := e.offsetS()
	rec.FiredAt, rec.FiredOffsetS = &now, &off
	rec.State = "active"
	e.active = rec
	e.broadcastLocked()
	hook := e.onFire
	e.mu.Unlock()
	if hook != nil {
		hook(rec.Mode)
	}
}

func (e *engine) expire(rec *FaultRecord) {
	e.mu.Lock()
	now := time.Now()
	off := e.offsetS()
	rec.ExpiredAt, rec.ExpiredOffsetS = &now, &off
	rec.State = "expired"
	if e.active == rec {
		e.active = nil
	}
	e.broadcastLocked()
	e.mu.Unlock()
}

// broadcastLocked wakes every gated/sleeping handler to re-check state.
func (e *engine) broadcastLocked() {
	close(e.changeCh)
	e.changeCh = make(chan struct{})
}

func (e *engine) snapshot() []FaultRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]FaultRecord, len(e.records))
	for i, r := range e.records {
		out[i] = *r
	}
	return out
}

func (e *engine) current() (*FaultRecord, chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.active, e.changeCh
}

// admit decides the fate of a newly arrived inference request.
func (e *engine) admit() admitVerdict {
	a, _ := e.current()
	if a == nil {
		return admitVerdict{act: actProceed, abortAfterTokens: -1}
	}
	switch a.Mode {
	case config.MockFaultError:
		return admitVerdict{act: actProceed, httpError: 500, abortAfterTokens: -1}
	case config.MockFaultSilentHang:
		return admitVerdict{act: actHang}
	case config.MockFaultStreamAbort:
		return admitVerdict{act: actProceed, abortAfterTokens: a.AbortAfterTokens}
	default: // stall: admitted; the emission gates will hold it
		return admitVerdict{act: actProceed, abortAfterTokens: -1}
	}
}

// silentHangActive reports whether a silent_hang window is active (used
// by /health when hang_health_during_silent_hang is set).
func (e *engine) silentHangActive() bool {
	a, _ := e.current()
	return a != nil && a.Mode == config.MockFaultSilentHang
}

// gateEmit blocks while a stall is active, and converts an active
// stream_abort or silent_hang into the corresponding action. Called
// before every write to the stream. Streams admitted DURING a
// stream_abort window set exemptFromAbort: they abort via their own
// token counter and must otherwise be served with normal pacing.
// The second return reports whether the call blocked on a stall — the
// handler staggers resume by a small per-stream jitter so hundreds of
// frozen streams do not release their backlogs in the same instant.
func (e *engine) gateEmit(ctx context.Context, exemptFromAbort bool) (action, bool) {
	stalled := false
	for {
		a, ch := e.current()
		if a == nil {
			return actProceed, stalled
		}
		switch a.Mode {
		case config.MockFaultStall:
			stalled = true
			select {
			case <-ch: // state changed; re-check
			case <-ctx.Done():
				return actAbandon, stalled
			case <-e.done:
				return actAbandon, stalled
			}
		case config.MockFaultStreamAbort:
			if exemptFromAbort {
				return actProceed, stalled
			}
			return actAbort, stalled
		case config.MockFaultSilentHang:
			return actHang, stalled
		default: // error mode leaves in-flight streams alone
			return actProceed, stalled
		}
	}
}

// sleep waits d on the monotonic clock but wakes early if a fault fires
// that demands immediate action on in-flight streams (abort or hang).
// A stall does not cut the sleep short: the follow-up gateEmit holds the
// stream instead. exemptFromAbort as in gateEmit: exempt streams sleep
// their full latency even while the abort window is active.
func (e *engine) sleep(ctx context.Context, d time.Duration, exemptFromAbort bool) action {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		a, ch := e.current()
		if a != nil {
			switch a.Mode {
			case config.MockFaultStreamAbort:
				if !exemptFromAbort {
					return actAbort
				}
			case config.MockFaultSilentHang:
				return actHang
			}
		}
		select {
		case <-timer.C:
			return actProceed
		case <-ch: // fault state changed; re-evaluate
		case <-ctx.Done():
			return actAbandon
		case <-e.done:
			return actAbandon
		}
	}
}

// hangForever parks until the server shuts down. It is only the fallback
// for Server.blackhole when the connection cannot be hijacked; blackhole is
// the real silent-hang mechanism (returning from a handler normally would
// let net/http write an implicit 200).
func (e *engine) hangForever() {
	<-e.done
}
