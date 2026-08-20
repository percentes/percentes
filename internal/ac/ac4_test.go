package ac

import (
	"testing"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// ac4Run is one scenario run with its guard window set: the baseline ends one
// pinned client timeout before the fire anchor and the guard window runs
// from there to T_inject (§3).
type ac4Run struct {
	cfg          *config.Config
	res          *loadgen.Result
	fireNs       int64
	guardStartNs int64
	baseline     *collect.Stats
	guard        *collect.Stats
	fault        *collect.Stats
}

// ac4Scenario runs load through a fault window of the given mode fired at
// T_inject (run offset 40 s, 4 s duration) and collects the §3 windows.
// The pre-fault phase exceeds the pinned 30 s timeout so the baseline
// window is non-empty and the guard sits inside the phase.
func ac4Scenario(t *testing.T, mode string) ac4Run {
	t.Helper()
	cfg := buildConfig(t, scenario{
		warmupS: 2, baselineS: 38, windowS: 8, cooldownS: 1, tInjectS: 38,
		ttft: fixed(100), itl: fixed(5), // nominal e2e = 100 + 255*5 = 1375 ms
		schedule: []config.MockFault{{Mode: mode, StartOffsetS: 40, DurationS: 4}},
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

	out := ac4Run{cfg: cfg, res: res, fireNs: fireNs}
	out.guardStartNs = collect.GuardStartNs(cfg, collect.FireAnchorNs(res.TInjectNs, fireNs), res.WarmupEndNs)
	for _, w := range []collect.Window{
		{Name: "baseline", StartNs: res.WarmupEndNs, EndNs: out.guardStartNs},
		{Name: "guard", StartNs: out.guardStartNs, EndNs: res.TInjectNs},
		{Name: "fault", StartNs: res.TInjectNs, EndNs: res.FaultEndNs},
	} {
		st, err := collect.Collect(cfg, res.Requests, w)
		if err != nil {
			t.Fatal(err)
		}
		switch w.Name {
		case "baseline":
			out.baseline = st
		case "guard":
			out.guard = st
		case "fault":
			out.fault = st
		}
	}
	if out.baseline.Scheduled == 0 {
		t.Fatalf("AC4: the scenario must produce a non-empty baseline window [%.1fs, %.1fs)",
			float64(res.WarmupEndNs)/1e9, float64(out.guardStartNs)/1e9)
	}
	return out
}

// assertBaselineResolvedAtFire is the guard clause of AC4 (§8): no request
// whose intended dispatch time lies in the baseline window is unresolved at
// actual fire time, within the pinned 50 ms maximum send skew.
func assertBaselineResolvedAtFire(t *testing.T, r ac4Run) {
	t.Helper()
	skewNs := int64(r.cfg.ClientValidity.SendSkewMaxMs) * int64(time.Millisecond)
	unresolved, worstNs := 0, int64(0)
	for i := range r.res.Requests {
		req := &r.res.Requests[i]
		if req.IntendedNs < r.res.WarmupEndNs || req.IntendedNs >= r.guardStartNs {
			continue
		}
		if over := req.DoneNs - r.fireNs; over > skewNs {
			unresolved++
			if over > worstNs {
				worstNs = over
			}
		}
	}
	if unresolved > 0 {
		t.Errorf("AC4: %d of %d baseline requests were still unresolved at fire (worst by %.3fs, allowance %dms): a fault-caused outcome would be booked to the baseline",
			unresolved, r.baseline.Scheduled, float64(worstNs)/1e9, r.cfg.ClientValidity.SendSkewMaxMs)
	}
}

// TestAC4LossAccounting: in-flight requests on a killed mock replica are
// classified errored, appear in the failure rates, and are absent from
// latency histograms; and the baseline window holds no request that the
// fault could still have touched (§3).
func TestAC4LossAccounting(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	r := ac4Scenario(t, config.MockFaultStreamAbort)

	acc := collect.AccountInFlight(r.res.Requests, r.fireNs, "")
	if acc.Total < 15 {
		t.Fatalf("AC4: expected a meaningful in-flight population at T_inject, got %d", acc.Total)
	}
	// At most one request may straddle the fire boundary (its [DONE] was
	// written server-side just before fire and read just after); every
	// other in-flight request must be classified errored.
	if acc.Errored < acc.Total-1 || acc.Completed > 1 || acc.Censored != 0 {
		t.Errorf("AC4: in-flight requests on the killed replica must be classified errored: %+v", acc)
	}

	// In-flight kills were scheduled inside the guard window, so they land
	// in the guard's error rate; requests admitted during the mock's
	// stream_abort window land in the fault window's error rate (§3
	// membership by intended time).
	if r.guard.Errored < acc.Total {
		t.Errorf("AC4: in-flight losses must surface in their window's failure rate: guard errored %d < in-flight %d", r.guard.Errored, acc.Total)
	}
	if r.fault.ErrorRate < 0.15 {
		t.Errorf("AC4: fault-window error rate must reflect the stream_abort window, got %.3f", r.fault.ErrorRate)
	}
	// §3: the guard absorbs every fault-caused outcome, so the baseline
	// carries none.
	if r.baseline.Errored != 0 || r.baseline.Censored != 0 {
		t.Errorf("AC4: the guard-bounded baseline must carry no fault-caused outcome: errored=%d censored=%d", r.baseline.Errored, r.baseline.Censored)
	}
	assertBaselineResolvedAtFire(t, r)

	for _, st := range []*collect.Stats{r.baseline, r.guard, r.fault} {
		if st.E2EConditional.Count != int64(st.Completed) || st.TTFTConditional.Count != int64(st.Completed) {
			t.Errorf("AC4: latency histograms must contain exactly the completed requests (window %s): e2e=%d ttft=%d completed=%d",
				st.Window.Name, st.E2EConditional.Count, st.TTFTConditional.Count, st.Completed)
		}
		if st.Scheduled != st.Completed+st.Errored+st.Censored {
			t.Errorf("AC4: three-state totals must partition scheduled (window %s): %+v", st.Window.Name, st)
		}
	}
	if r.fault.ErrClasses[loadgen.ErrReset] == 0 {
		t.Errorf("AC4: abrupt deletion presents as connection resets, got classes %+v", r.fault.ErrClasses)
	}
	if !r.fault.ConditionalCaveat {
		t.Error("AC4: fault window exceeds the 5% failure threshold; conditional caveat must be set")
	}
}

// TestAC4bCensoringAccounting: in silent-hang mode, affected requests are
// censored at exactly the 30 s pinned timeout, appear in the censored
// rate and the incidence curve, and are absent from latency histograms.
func TestAC4bCensoringAccounting(t *testing.T) {
	if testing.Short() {
		t.Skip("AC suite skipped in -short mode")
	}
	r := ac4Scenario(t, config.MockFaultSilentHang)

	// Every in-flight request at fire time hangs to the timeout.
	acc := collect.AccountInFlight(r.res.Requests, r.fireNs, "")
	if acc.Censored != acc.Total || acc.Total < 15 {
		t.Errorf("AC4b: in-flight requests at fire must be censored, got %+v", acc)
	}

	// Censoring time is the pinned 30 s client timeout, measured from
	// intended dispatch; the send-skew gate bounds the divergence.
	timeoutNs := int64(r.cfg.Client.HTTPTimeoutS) * 1e9
	censored := 0
	for _, req := range r.res.Requests {
		if req.Outcome != loadgen.OutcomeCensored {
			continue
		}
		censored++
		obs := req.DoneNs - req.IntendedNs
		if obs < timeoutNs || obs > timeoutNs+500*int64(time.Millisecond) {
			t.Errorf("AC4b: censoring must occur at exactly the pinned timeout: request %d observed at %.3fs", req.Index, float64(obs)/1e9)
		}
	}
	if censored < 60 {
		t.Fatalf("AC4b: expected ~lambda x (window + nominal) censored requests, got %d", censored)
	}

	// This mode is where the boundary is tested: a hung request resolves one
	// full timeout after its intended time, which is exactly the guard's width.
	assertBaselineResolvedAtFire(t, r)
	if r.baseline.Censored != 0 {
		t.Errorf("AC4: hangs must land in the guard window, not the baseline: baseline censored=%d", r.baseline.Censored)
	}

	if r.fault.CensoredRate < 0.15 {
		t.Errorf("AC4b: fault-window censored rate must be first-class, got %.3f", r.fault.CensoredRate)
	}
	for _, st := range []*collect.Stats{r.baseline, r.guard, r.fault} {
		if st.E2EConditional.Count != int64(st.Completed) {
			t.Errorf("AC4b: censored requests must not enter latency histograms (window %s)", st.Window.Name)
		}
	}

	// Incidence curve: censored requests are censored observations at the
	// timeout — the fault-window curve must not cross high quantiles inside
	// the horizon ("p_q > 30 s"), while the median still exists.
	if _, ok := r.fault.Incidence.Quantile(0.5); !ok {
		t.Error("AC4b: fault-window incidence median should exist (most requests complete)")
	}
	if q, ok := r.fault.Incidence.Quantile(0.9); ok {
		t.Errorf("AC4b: with ~27%% censored, incidence p90 must be reported as beyond the horizon, got %.3fs", float64(q)/1e6)
	}
	if got := r.fault.Incidence.N; got != r.fault.Scheduled {
		t.Errorf("AC4b: incidence curve must cover ALL scheduled requests: n=%d scheduled=%d", got, r.fault.Scheduled)
	}
}
