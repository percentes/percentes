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
	if st.KM.N != 4 {
		t.Errorf("KM over ALL scheduled: n=%d, want 4", st.KM.N)
	}
	if st.GoodputFrac != 0.25 {
		t.Errorf("goodput: one of four meets SLO, got %v", st.GoodputFrac)
	}
	if st.ErrClasses[loadgen.ErrReset] != 1 {
		t.Errorf("error classes: %+v", st.ErrClasses)
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
