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

// Synthetic three-state accounting: the normative exclusions hold;
// errored and censored never enter latency histograms, every scheduled
// request lands in exactly one state, window assignment is by intended
// time.
func TestCollectThreeStateAccounting(t *testing.T) {
	cfg := testCfg(t)
	sec := int64(1e9)
	reqs := []loadgen.Request{
		// completed in-window: time to first token (TTFT) 0.5s, e2e 2s,
		// meets the service-level objective (SLO)
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

// A window naming a replica keeps only the requests attributed to it.
func TestWindowReplicaFilter(t *testing.T) {
	cfg := testCfg(t)
	reqs := []loadgen.Request{
		{Index: 0, IntendedNs: 1e9, DispatchNs: 1e9, FirstTokNs: 1.1e9, DoneNs: 2e9, Outcome: loadgen.OutcomeCompleted, Replica: "a"},
		{Index: 1, IntendedNs: 2e9, DispatchNs: 2e9, FirstTokNs: 2.1e9, DoneNs: 3e9, Outcome: loadgen.OutcomeCompleted, Replica: "b"},
		{Index: 2, IntendedNs: 3e9, DispatchNs: 3e9, DoneNs: 4e9, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset, Replica: "b"},
	}
	all, err := Collect(cfg, reqs, Window{Name: "fault", StartNs: 0, EndNs: 10e9})
	if err != nil {
		t.Fatal(err)
	}
	only, err := Collect(cfg, reqs, Window{Name: "fault_survivor", StartNs: 0, EndNs: 10e9, Replica: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if all.Scheduled != 3 || only.Scheduled != 2 || only.Completed != 1 || only.Errored != 1 {
		t.Fatalf("pooled %+v, replica b %+v", all.Scheduled, only)
	}
}

// Completion length is reported as the content event count over every
// completed request and the usage count over those that carried one; the
// §10 check compares the two where both exist.
func TestCollectCompletionLengthAndTokenCheck(t *testing.T) {
	cfg := testCfg(t)
	sec := int64(1e9)
	done := func(i int64, tokens int, usage int, seen bool) loadgen.Request {
		return loadgen.Request{Index: i, IntendedNs: (10 + i) * sec, DispatchNs: (10+i)*sec + 1e6, FirstTokNs: (10+i)*sec + 5e8, DoneNs: (12 + i) * sec,
			Outcome: loadgen.OutcomeCompleted, Tokens: tokens, CompletionTokens: usage, UsageSeen: seen}
	}
	reqs := []loadgen.Request{done(0, 3, 3, true), done(1, 5, 6, true), done(2, 4, 4, true), done(3, 2, 0, false),
		{Index: 4, IntendedNs: 14 * sec, DispatchNs: 14*sec + 1e6, DoneNs: 14*sec + 3e8, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset, Tokens: 1}}
	st, err := Collect(cfg, reqs, Window{Name: "w", StartNs: 10 * sec, EndNs: 20 * sec})
	if err != nil {
		t.Fatal(err)
	}
	if st.ContentEvents != (CountSummary{N: 4, Mean: 3.5, P50: 3, P95: 5, Max: 5}) {
		t.Fatalf("content events over completed requests: %+v", st.ContentEvents)
	}
	if st.CompletionTokens != (CountSummary{N: 3, Mean: 13.0 / 3, P50: 4, P95: 6, Max: 6}) {
		t.Fatalf("completion tokens over requests with usage: %+v", st.CompletionTokens)
	}
	if st.TokenCheck != (TokenCheck{Sampled: 3, Matched: 2}) {
		t.Fatalf("token check: %+v", st.TokenCheck)
	}
}

// Errored in-flight requests are split by error class; completed and
// censored ones are not.
func TestInFlightErroredByClass(t *testing.T) {
	sec := int64(1e9)
	fire := 20 * sec
	reqs := []loadgen.Request{
		{Index: 0, DispatchNs: 19 * sec, DoneNs: fire + 1e8, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset},
		{Index: 1, DispatchNs: 19 * sec, DoneNs: fire + 2e8, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset},
		{Index: 2, DispatchNs: 19 * sec, DoneNs: fire + 3e8, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrConnect},
		{Index: 3, DispatchNs: 19 * sec, DoneNs: fire + 30*sec, Outcome: loadgen.OutcomeCensored},
		{Index: 4, DispatchNs: 19 * sec, DoneNs: 21 * sec, Outcome: loadgen.OutcomeCompleted},
		{Index: 5, DispatchNs: 18 * sec, DoneNs: 19 * sec, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrConnect},
	}
	acc := AccountInFlight(reqs, fire, "")
	if acc.Errored != 3 || len(acc.ErroredByClass) != 2 || acc.ErroredByClass[loadgen.ErrReset] != 2 || acc.ErroredByClass[loadgen.ErrConnect] != 1 {
		t.Fatalf("class split: %+v", acc)
	}
	if acc := AccountInFlight(reqs[3:5], fire, ""); acc.ErroredByClass != nil {
		t.Fatalf("no errored request produced a split: %v", acc.ErroredByClass)
	}
}

// A request dispatched before the fire whose terminal time lies inside
// the uncertainty interval on either side is indeterminate.
func TestSplitAtFire(t *testing.T) {
	ms := int64(1e6)
	fire := 20_000 * ms
	reqs := []loadgen.Request{
		{Index: 0, DispatchNs: fire - 500*ms, DoneNs: fire - 4*ms, Outcome: loadgen.OutcomeErrored},   // ended before the fire
		{Index: 1, DispatchNs: fire - 500*ms, DoneNs: fire + 1*ms, Outcome: loadgen.OutcomeCompleted}, // inside the zone
		{Index: 2, DispatchNs: fire - 500*ms, DoneNs: fire + 5*ms, Outcome: loadgen.OutcomeCompleted}, // on the zone's edge
		{Index: 3, DispatchNs: fire - 500*ms, DoneNs: fire + 6*ms, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset},
		{Index: 4, DispatchNs: fire - 500*ms, DoneNs: fire + 30_000*ms, Outcome: loadgen.OutcomeCensored},
		{Index: 5, DispatchNs: fire + 1*ms, DoneNs: fire + 2*ms, Outcome: loadgen.OutcomeErrored}, // dispatched after the fire
		{Index: 6, DoneNs: fire}, // never dispatched
	}
	n, det := SplitAtFire(reqs, fire, 5*ms)
	if n != 2 || det.Total != 2 || det.Completed != 0 || det.Errored != 1 || det.Censored != 1 || det.ErroredByClass[loadgen.ErrReset] != 1 {
		t.Fatalf("indeterminate %d, determinate %+v", n, det)
	}
	if inf := AccountInFlight(reqs, fire, ""); n+det.Total != inf.Total {
		t.Fatalf("indeterminate %d plus determinate %d must partition the %d in flight", n, det.Total, inf.Total)
	}
	// One completion just after the fire and one request ending just before
	// it: no determinate completion, one indeterminate.
	pair := []loadgen.Request{
		{Index: 0, DispatchNs: fire - 500*ms, DoneNs: fire - 1*ms, Outcome: loadgen.OutcomeCompleted},
		{Index: 1, DispatchNs: fire - 500*ms, DoneNs: fire + 1*ms, Outcome: loadgen.OutcomeCompleted},
	}
	if n, det := SplitAtFire(pair, fire, 2*ms); n != 1 || det.Completed != 0 || det.Total != 0 {
		t.Fatalf("indeterminate %d, determinate %+v", n, det)
	}
	if n, det := SplitAtFire(reqs, fire, 0); n != 0 || det.Total != 4 {
		t.Fatalf("zero zone: indeterminate %d, determinate %+v", n, det)
	}
}
