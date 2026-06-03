package ac

import (
	"testing"
	"time"

	"github.com/itsveems/chaosserve/internal/collect"
	"github.com/itsveems/chaosserve/internal/config"
	"github.com/itsveems/chaosserve/internal/loadgen"
)

// ac4Scenario runs load through a fault window of the given mode fired at
// T_inject (run offset 18 s, 4 s duration) and returns the results plus
// the baseline/fault window stats.
func ac4Scenario(t *testing.T, mode string) (*loadgen.Result, *collect.Stats, *collect.Stats, *config.Config, int64) {
	t.Helper()
	cfg := buildConfig(t, scenario{
		warmupS: 3, baselineS: 15, windowS: 15, cooldownS: 2, tInjectS: 15,
		ttft: fixed(100), itl: fixed(5), // nominal e2e = 100 + 255*5 = 1375 ms
		schedule: []config.MockFault{{Mode: mode, StartOffsetS: 18, DurationS: 4}},
	})
	res, base := runScenario(t, cfg)

	// The mock's schedule runs on its own start anchor, slightly before
	// the run epoch; in-flight accounting is against the ACTUAL fire
	// time, which the injector side records.
	recs := adminFaults(t, base)
	if len(recs) != 1 || recs[0].FiredAt == nil {
		t.Fatalf("fault record incomplete: %+v", recs)
	}
	fireNs := recs[0].FiredAt.Sub(res.EpochWall).Nanoseconds()

	baseline, err := collect.Collect(cfg, res.Requests, collect.Window{Name: "baseline", StartNs: res.WarmupEndNs, EndNs: res.TInjectNs})
	if err != nil {
		t.Fatal(err)
	}
	fault, err := collect.Collect(cfg, res.Requests, collect.Window{Name: "fault", StartNs: res.TInjectNs, EndNs: res.FaultEndNs})
	if err != nil {
		t.Fatal(err)
	}
	return res, baseline, fault, cfg, fireNs
}

// TestAC4LossAccounting: in-flight requests on a killed mock replica are
// classified errored, appear in the failure rates, and are absent from
// latency histograms.
func TestAC4LossAccounting(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	res, baseline, fault, _, fireNs := ac4Scenario(t, config.MockFaultStreamAbort)

	acc := collect.AccountInFlight(res.Requests, fireNs, "")
	if acc.Total < 15 {
		t.Fatalf("AC4: expected a meaningful in-flight population at T_inject, got %d", acc.Total)
	}
	// At most one request may straddle the fire boundary (its [DONE] was
	// written server-side just before fire and read just after); every
	// other in-flight request must be classified errored.
	if acc.Errored < acc.Total-1 || acc.Completed > 1 || acc.Censored != 0 {
		t.Errorf("AC4: in-flight requests on the killed replica must be classified errored: %+v", acc)
	}

	// The killed in-flight requests were scheduled pre-fault: they appear
	// in the baseline window's error rate; window admissions during the
	// abort window appear in the fault window's. Both are first-class.
	if baseline.Errored < acc.Total {
		t.Errorf("AC4: in-flight losses must surface in their window's failure rate: baseline errored %d < in-flight %d", baseline.Errored, acc.Total)
	}
	if fault.ErrorRate < 0.15 {
		t.Errorf("AC4: fault-window error rate must reflect the abort window, got %.3f", fault.ErrorRate)
	}
	for _, st := range []*collect.Stats{baseline, fault} {
		if st.E2EConditional.Count != int64(st.Completed) || st.TTFTConditional.Count != int64(st.Completed) {
			t.Errorf("AC4: latency histograms must contain exactly the completed requests (window %s): e2e=%d ttft=%d completed=%d",
				st.Window.Name, st.E2EConditional.Count, st.TTFTConditional.Count, st.Completed)
		}
		if st.Scheduled != st.Completed+st.Errored+st.Censored {
			t.Errorf("AC4: three-state totals must partition scheduled (window %s): %+v", st.Window.Name, st)
		}
	}
	if fault.ErrClasses[loadgen.ErrReset] == 0 {
		t.Errorf("AC4: abrupt deletion presents as connection resets, got classes %+v", fault.ErrClasses)
	}
	if !fault.ConditionalCaveat {
		t.Error("AC4: fault window exceeds the 5% failure threshold; conditional caveat must be set")
	}
}

// TestAC4bCensoringAccounting: in silent-hang mode, affected requests are
// censored at exactly the 30 s pinned timeout, appear in the censored
// rate and the KM curve, and are absent from latency histograms.
func TestAC4bCensoringAccounting(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	res, baseline, fault, cfg, fireNs := ac4Scenario(t, config.MockFaultSilentHang)

	// Every in-flight request at fire time hangs to the timeout.
	acc := collect.AccountInFlight(res.Requests, fireNs, "")
	if acc.Censored != acc.Total || acc.Total < 15 {
		t.Errorf("AC4b: in-flight requests at fire must be censored, got %+v", acc)
	}

	// Censoring time is the pinned 30 s client timeout, measured from
	// intended dispatch; the send-skew gate bounds the divergence.
	timeoutNs := int64(cfg.Client.HTTPTimeoutS) * 1e9
	censored := 0
	for _, r := range res.Requests {
		if r.Outcome != loadgen.OutcomeCensored {
			continue
		}
		censored++
		obs := r.DoneNs - r.IntendedNs
		if obs < timeoutNs || obs > timeoutNs+500*int64(time.Millisecond) {
			t.Errorf("AC4b: censoring must occur at exactly the pinned timeout: request %d observed at %.3fs", r.Index, float64(obs)/1e9)
		}
	}
	if censored < 60 {
		t.Fatalf("AC4b: expected ~lambda x (window + nominal) censored requests, got %d", censored)
	}

	if fault.CensoredRate < 0.15 {
		t.Errorf("AC4b: fault-window censored rate must be first-class, got %.3f", fault.CensoredRate)
	}
	for _, st := range []*collect.Stats{baseline, fault} {
		if st.E2EConditional.Count != int64(st.Completed) {
			t.Errorf("AC4b: censored requests must not enter latency histograms (window %s)", st.Window.Name)
		}
	}

	// KM: censored requests are censored observations at the timeout —
	// the fault-window curve must not cross high quantiles inside the
	// horizon ("p_q > 30 s"), while the median still exists.
	if _, ok := fault.KM.Quantile(0.5); !ok {
		t.Error("AC4b: fault-window KM median should exist (most requests complete)")
	}
	if q, ok := fault.KM.Quantile(0.9); ok {
		t.Errorf("AC4b: with ~27%% censored, KM p90 must be reported as beyond the horizon, got %.3fs", float64(q)/1e6)
	}
	if got := fault.KM.N; got != fault.Scheduled {
		t.Errorf("AC4b: KM must cover ALL scheduled requests: n=%d scheduled=%d", got, fault.Scheduled)
	}
}
