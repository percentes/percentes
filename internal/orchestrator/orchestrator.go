// Package orchestrator fires the configured fault variant at T_inject
// (SPEC.md §2). Injection is always PRE-ARMED with automatic expiry: the
// injector is told the fire time and duration in advance, because a
// black-hole partition makes the victim unreachable the moment it fires
// (§1) — nothing may depend on talking to the victim after T_inject.
// The orchestrator records armed/fire/expiry timestamps and is agnostic
// to the injection mechanism beyond them.
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Timestamps is the §2 audit record. Wall times cross process boundaries
// (mock records its own); run-relative offsets are derived by the caller
// from the run epoch.
type Timestamps struct {
	ArmedAt        time.Time  `json:"armed_at"`
	PlannedFireAt  time.Time  `json:"planned_fire_at"`
	PlannedExpiry  time.Time  `json:"planned_expiry_at"`
	ObservedFire   *time.Time `json:"observed_fire_at,omitempty"`
	ObservedExpiry *time.Time `json:"observed_expiry_at,omitempty"`
}

// FireErrorMs returns observed-minus-planned fire time in milliseconds
// (AC3 asserts |error| <= the pinned 500 ms tolerance).
func (ts *Timestamps) FireErrorMs() (float64, error) {
	if ts.ObservedFire == nil {
		return 0, fmt.Errorf("orchestrator: fault never fired")
	}
	return float64(ts.ObservedFire.Sub(ts.PlannedFireAt).Microseconds()) / 1000, nil
}

// Injector is a pluggable fault mechanism. Phase 0 ships the mock admin
// injector; Phase 1 adds clean pod deletion and the pre-armed node
// partition behind the same contract.
type Injector interface {
	// Arm schedules the fault to fire after fireIn and auto-expire
	// durationS later. Must be called before the fire time (pre-armed).
	Arm(ctx context.Context, fireIn time.Duration, durationS float64) error
	// Observed reports the injector-side fired/expired wall timestamps,
	// nil until they happen. A non-nil error is TERMINAL: the injector
	// could not carry out or track the injection (e.g. a failed grace=0
	// delete), and Execute aborts the run with it. Transient conditions —
	// the fault not observed yet, a retryable poll failure — return
	// (nil, nil, nil), never an error.
	Observed(ctx context.Context) (fired, expired *time.Time, err error)
}

// Execute pre-arms the injector to fire at epoch+tInject, waits for
// expiry (or ctx cancellation), and returns the audit record.
func Execute(ctx context.Context, inj Injector, epoch time.Time, tInject time.Duration, durationS float64) (*Timestamps, error) {
	ts := &Timestamps{
		PlannedFireAt: epoch.Add(tInject),
		PlannedExpiry: epoch.Add(tInject + time.Duration(durationS*float64(time.Second))),
	}
	fireIn := time.Until(ts.PlannedFireAt)
	if fireIn <= 0 {
		return nil, fmt.Errorf("orchestrator: T_inject is %s in the past; injection must be pre-armed", -fireIn)
	}
	if err := inj.Arm(ctx, fireIn, durationS); err != nil {
		return nil, fmt.Errorf("orchestrator: arming: %w", err)
	}
	ts.ArmedAt = time.Now()

	// Poll for observed fire/expiry until past planned expiry plus slack.
	// A non-nil Observed error is terminal — the injector could not carry
	// out or track the injection (e.g. a failed grace=0 delete) — so
	// surface it immediately rather than masking it as an expiry timeout.
	deadline := ts.PlannedExpiry.Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fired, expired, err := inj.Observed(ctx)
		if err != nil {
			return ts, fmt.Errorf("orchestrator: injection failed: %w", err)
		}
		ts.ObservedFire, ts.ObservedExpiry = fired, expired
		if expired != nil {
			return ts, nil
		}
		select {
		case <-ctx.Done():
			return ts, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return ts, fmt.Errorf("orchestrator: fault did not expire by %s (fired=%v)", deadline, ts.ObservedFire)
}

// MockInjector arms fault windows on a mock replica's out-of-band admin
// API.
type MockInjector struct {
	AdminBaseURL     string
	Mode             string
	AbortAfterTokens int

	faultID int
	client  *http.Client
}

// NewMockInjector constructs a MockInjector that arms fault windows on the
// mock replica's out-of-band admin API at adminBaseURL (§2).
func NewMockInjector(adminBaseURL, mode string, abortAfterTokens int) *MockInjector {
	return &MockInjector{
		AdminBaseURL:     adminBaseURL,
		Mode:             mode,
		AbortAfterTokens: abortAfterTokens,
		client:           &http.Client{Timeout: 5 * time.Second},
	}
}

func (m *MockInjector) Arm(ctx context.Context, fireIn time.Duration, durationS float64) error {
	body, err := json.Marshal(map[string]any{
		"mode":               m.Mode,
		"delay_s":            fireIn.Seconds(),
		"duration_s":         durationS,
		"abort_after_tokens": m.AbortAfterTokens,
	})
	if err != nil {
		return fmt.Errorf("mock injector: encoding arm request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.AdminBaseURL+"/admin/faults", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("mock injector: arm returned %d", resp.StatusCode)
	}
	var rec struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return err
	}
	m.faultID = rec.ID
	return nil
}

// Observed polls the mock's /admin/faults for this fault's recorded
// fire/expiry. A poll failure — transport, decode, or the fault not yet
// listed — is transient, not a terminal injection failure, so it returns
// (nil, nil, nil) and lets Execute retry; the mock injector has no
// terminal observe-time failure (arming already validated the fault) and
// so never returns a non-nil error.
func (m *MockInjector) Observed(ctx context.Context) (fired, expired *time.Time, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.AdminBaseURL+"/admin/faults", nil)
	if err != nil {
		return nil, nil, nil
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, nil, nil
	}
	defer resp.Body.Close()
	var out struct {
		Faults []struct {
			ID        int        `json:"id"`
			FiredAt   *time.Time `json:"fired_at"`
			ExpiredAt *time.Time `json:"expired_at"`
		} `json:"faults"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, nil
	}
	for _, f := range out.Faults {
		if f.ID == m.faultID {
			return f.FiredAt, f.ExpiredAt, nil
		}
	}
	return nil, nil, nil
}
