package collect

import (
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

func testCfg(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// Synthetic three-state accounting: the normative exclusions hold —
// errored and censored never enter latency histograms, every scheduled
// request lands in exactly one state, window assignment is by intended
// time.
func TestCollectThreeStateAccounting(t *testing.T) {
	cfg := testCfg(t)
	sec := int64(1e9)
	reqs := []loadgen.Request{
		// completed in-window: TTFT 0.5s, e2e 2s, meets SLO
		{Index: 0, IntendedNs: 10 * sec, DispatchNs: 10*sec + 1e6, FirstTokNs: 10*sec + 5e8, DoneNs: 12 * sec, Outcome: loadgen.OutcomeCompleted, ITLsUs: []int64{5000, 5000}},
		// completed but SLO-violating TTFT (1.5s)
		{Index: 1, IntendedNs: 11 * sec, DispatchNs: 11*sec + 1e6, FirstTokNs: 11*sec + 15*1e8, DoneNs: 13 * sec, Outcome: loadgen.OutcomeCompleted},
		// errored (reset) at 0.3s
		{Index: 2, IntendedNs: 12 * sec, DispatchNs: 12*sec + 1e6, DoneNs: 12*sec + 3e8, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset},
		// censored at 30s
		{Index: 3, IntendedNs: 13 * sec, DispatchNs: 13*sec + 1e6, DoneNs: 43 * sec, Outcome: loadgen.OutcomeCensored},
		// outside window (intended before start): ignored
		{Index: 4, IntendedNs: 5 * sec, DispatchNs: 5*sec + 1e6, FirstTokNs: 6 * sec, DoneNs: 7 * sec, Outcome: loadgen.OutcomeCompleted},
	}
	st, err := Collect(cfg, reqs, Window{Name: "w", StartNs: 10 * sec, EndNs: 20 * sec})
	if err != nil {
		t.Fatal(err)
	}

	if st.Scheduled != 4 || st.Completed != 2 || st.Errored != 1 || st.Censored != 1 {
		t.Fatalf("counts: %+v", st)
	}
	if st.TTFTConditional.Count != 2 || st.E2EConditional.Count != 2 {
		t.Errorf("latency histograms must hold completed only: ttft=%d e2e=%d", st.TTFTConditional.Count, st.E2EConditional.Count)
	}
	if st.ITLPooled.Count != 2 {
		t.Errorf("pooled ITL must have the completed request's 2 gaps, got %d", st.ITLPooled.Count)
	}
	if st.ErrorRate != 0.25 || st.CensoredRate != 0.25 {
		t.Errorf("failure rates: err=%v cens=%v, want 0.25/0.25", st.ErrorRate, st.CensoredRate)
	}
	if !st.ConditionalCaveat {
		t.Error("error+censored = 50% > 5%: conditional caveat must be set")
	}
	if st.Incidence.N != 4 {
		t.Errorf("incidence curve over ALL scheduled: n=%d, want 4", st.Incidence.N)
	}
	if st.GoodputFrac != 0.25 {
		t.Errorf("goodput: one of four meets SLO, got %v", st.GoodputFrac)
	}
	if st.ErrClasses[loadgen.ErrReset] != 1 {
		t.Errorf("error classes: %+v", st.ErrClasses)
	}
}

// Guard-window arithmetic on the pinned §1 profile: warm-up ends at 60 s and
// T_inject is at 360 s, so with the fault firing on time the baseline runs
// [60 s, 330 s), the guard [330 s, 360 s), and the fault window from 360 s.
func TestGuardWindowBoundsNominalRun(t *testing.T) {
	cfg := testCfg(t)
	sec := int64(1e9)
	warmupEnd, tInject, actualFire := 60*sec, 360*sec, 360*sec

	anchor := FireAnchorNs(tInject, actualFire)
	if anchor != 360*sec {
		t.Fatalf("on-time fire: anchor %v, want 360s", anchor)
	}
	guardStart := GuardStartNs(cfg, anchor, warmupEnd)
	if guardStart != 330*sec {
		t.Errorf("baseline must end one pinned 30 s timeout before the anchor: got %v, want 330s", guardStart)
	}
	if got := (guardStart - warmupEnd) / sec; got != 270 {
		t.Errorf("baseline statistics must cover 270 s of the 300 s phase, got %ds", got)
	}
	if got := (tInject - guardStart) / sec; got != 30 {
		t.Errorf("guard window must span the pinned 30 s timeout, got %ds", got)
	}
}

// The anchor is the EARLIER of T_inject and the recorded actual fire, so a
// fault firing inside the 500 ms injection tolerance moves the baseline end
// with it (§3). Oracle: firing 400 ms early puts the anchor at 359.6 s
// and the baseline end at 329.6 s; a late fire leaves the anchor at T_inject.
func TestGuardWindowBoundsEarlyAndLateFire(t *testing.T) {
	cfg := testCfg(t)
	sec := int64(1e9)
	warmupEnd, tInject := 60*sec, 360*sec

	early := FireAnchorNs(tInject, 360*sec-4e8)
	if early != 360*sec-4e8 {
		t.Fatalf("early fire must anchor the windows: got %v", early)
	}
	if got := GuardStartNs(cfg, early, warmupEnd); got != 330*sec-4e8 {
		t.Errorf("baseline end under an early fire: got %v, want 329.6s", got)
	}
	late := FireAnchorNs(tInject, 360*sec+4e8)
	if late != tInject {
		t.Errorf("a late fire must not extend the baseline past T_inject: got %v", late)
	}
}

// A pre-fault phase shorter than the pinned timeout cannot carry a baseline
// at all: the guard start floors at the measurement start, leaving the
// baseline window empty and every pre-fault request in the guard.
func TestGuardStartFloorsAtMeasurementStart(t *testing.T) {
	cfg := testCfg(t)
	sec := int64(1e9)
	warmupEnd := 3 * sec
	if got := GuardStartNs(cfg, warmupEnd+15*sec, warmupEnd); got != warmupEnd {
		t.Errorf("guard start must floor at the measurement start: got %v, want %v", got, warmupEnd)
	}
}

func TestAccountInFlight(t *testing.T) {
	sec := int64(1e9)
	tInject := 20 * sec
	reqs := []loadgen.Request{
		// dispatched before, done after: in flight, errored, on victim
		{Index: 0, DispatchNs: 19 * sec, DoneNs: 20*sec + 3e8, Outcome: loadgen.OutcomeErrored, Replica: "pod-a"},
		// in flight, survived to completion on the other replica
		{Index: 1, DispatchNs: 19 * sec, DoneNs: 21 * sec, Outcome: loadgen.OutcomeCompleted, Replica: "pod-b"},
		// finished before T_inject: not in flight
		{Index: 2, DispatchNs: 18 * sec, DoneNs: 19 * sec, Outcome: loadgen.OutcomeCompleted, Replica: "pod-a"},
		// dispatched after T_inject: not in flight
		{Index: 3, DispatchNs: 21 * sec, DoneNs: 22 * sec, Outcome: loadgen.OutcomeCompleted, Replica: "pod-b"},
		// never dispatched: not in flight
		{Index: 4},
	}
	acc := AccountInFlight(reqs, tInject, "pod-a")
	if acc.Total != 2 || acc.Errored != 1 || acc.Completed != 1 || acc.OnVictim != 1 {
		t.Fatalf("in-flight accounting: %+v", acc)
	}
}
