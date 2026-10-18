package loadgen

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/schedule"
)

// The reference ladder: a 60 s warm-up at 0.25 rps, then five 480 s
// windows each followed by a 90 s drain, 48.5 minutes in all.
func TestReleaseLadderSchedule(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Run.Seed = 7
	l := BuildLadder(cfg)
	if l == nil || len(l.Steps) != 5 {
		t.Fatalf("ladder %+v", l)
	}
	if time.Duration(l.EndNs) != schedule.CellLength || l.EndNs != 2910e9 {
		t.Fatalf("cell ends at %v, want %v", time.Duration(l.EndNs), schedule.CellLength)
	}
	if !slices.Equal(BuildSchedule(cfg), l.Offsets) || !slices.Equal(BuildLadder(cfg).Offsets, l.Offsets) {
		t.Fatal("the schedule is not the ladder's, or not reproducible")
	}
	if !slices.IsSorted(l.Offsets) {
		t.Fatal("offsets not ascending")
	}
	within := func(name string, got int, want float64) {
		t.Helper()
		if math.Abs(float64(got)-want) > 6*math.Sqrt(want) {
			t.Errorf("%s: %d requests, expected about %g", name, got, want)
		}
	}
	for _, off := range l.Offsets[:l.Steps[0].First] {
		if off < 0 || off >= 60e9 {
			t.Fatalf("warm-up request at %d ns", off)
		}
	}
	within("warm-up", l.Steps[0].First, 15)
	limits := []int{45, 90, 180, 360, 720}
	rec := newLadderRecord(cfg, l)
	for k, st := range l.Steps {
		start := int64(60e9) + int64(k)*570e9
		if st.RateRPS != config.PinnedReleaseStepsRPS[k] || st.StartNs != start || st.EndNs != start+480e9 || st.DrainEndNs != start+570e9 {
			t.Fatalf("step %d: %+v", k+1, st)
		}
		if k > 0 && st.First != l.Steps[k-1].End {
			t.Fatalf("step %d starts at request %d, step %d ended at %d", k+1, st.First, k, l.Steps[k-1].End)
		}
		for _, off := range l.Offsets[st.First:st.End] {
			if off < st.StartNs || off >= st.EndNs {
				t.Fatalf("step %d request at %d ns, outside [%d, %d)", k+1, off, st.StartNs, st.EndNs)
			}
		}
		within("step", st.End-st.First, st.RateRPS*480)
		if sr := rec.Steps[k]; sr.Scheduled != st.End-st.First || sr.InFlightLimit != limits[k] || sr.Attempted {
			t.Fatalf("step %d record %+v", k+1, sr)
		}
	}
	if l.Steps[4].End != len(l.Offsets) {
		t.Fatalf("%d offsets, last step ends at %d", len(l.Offsets), l.Steps[4].End)
	}
	if w := newLadderTripwire(cfg, l).report(); !slices.Equal(w.InFlightLimits, limits) || w.InFlightLimit != 0 || w.Scheduled != len(l.Offsets) {
		t.Fatalf("tripwires %+v", w)
	}

	cfg.Run.Seed = 8
	if slices.Equal(BuildSchedule(cfg), l.Offsets) {
		t.Fatal("a different seed gave the same schedule")
	}
}

// Each step bounds its own sends and its own in-flight count; the
// warm-up counts under the first step.
func TestLadderTripwireBySteps(t *testing.T) {
	cfg := &config.Config{Client: config.Client{HTTPTimeoutS: 2}}
	l := &Ladder{Steps: []LadderStep{{RateRPS: 1, First: 2, End: 5}, {RateRPS: 2, First: 5, End: 7}}}

	tw := newLadderTripwire(cfg, l)
	for i := int64(1); i <= 5; i++ {
		tw.start(i, 1)
		tw.end()
	}
	tw.start(6, 2)
	tw.end()
	if w := tw.report(); w.Fired || w.Dispatches != 6 || w.Scheduled != 7 || !slices.Equal(w.InFlightLimits, []int{4, 8}) {
		t.Fatalf("five first-step sends and one second-step: %+v", w)
	}
	tw.send(7, 1)
	if w := tw.report(); !w.Fired || w.ExtraDispatchNs != 7 {
		t.Fatalf("a sixth first-step send: %+v", w)
	}

	// The second step schedules two, so its third send is also extra.
	tw = newLadderTripwire(cfg, l)
	for i := int64(1); i <= 8; i++ {
		tw.start(i, 2)
	}
	if w := tw.report(); w.OverInFlightNs != 0 || w.MaxInFlight != 8 {
		t.Fatalf("eight in flight at the second step: %+v", w)
	}
	tw.start(9, 2)
	if w := tw.report(); !w.Fired || w.OverInFlightNs != 9 || w.ExtraDispatchNs != 3 {
		t.Fatalf("ninth in flight at the second step: %+v", w)
	}

	tw = newLadderTripwire(cfg, l)
	for i := int64(1); i <= 5; i++ {
		tw.start(i, 1)
	}
	if w := tw.report(); !w.Fired || w.OverInFlightNs != 5 || w.ExtraDispatchNs != 0 {
		t.Fatalf("fifth in flight at the first step: %+v", w)
	}
}

func TestStepEnd(t *testing.T) {
	pass, fail := &GateReport{Pass: true}, &GateReport{}
	for _, c := range []struct {
		s    StepRecord
		want string
	}{
		{StepRecord{Scheduled: 10, Status429: 5, Gates: pass}, ""},
		{StepRecord{Scheduled: 10, Status429: 6, Gates: pass}, LadderEndBackoff},
		{StepRecord{Scheduled: 9, Status429: 5, Gates: pass}, LadderEndBackoff},
		{StepRecord{Scheduled: 0, Gates: pass}, ""},
		{StepRecord{Scheduled: 10, Status429: 10, Gates: pass, Tripwire: true}, LadderEndTripwire},
		{StepRecord{Scheduled: 10, Status429: 10, Gates: fail, Tripwire: true}, LadderEndClientValidity},
	} {
		if got := stepEnd(&c.s); got != c.want {
			t.Errorf("%+v: %q, want %q", c.s, got, c.want)
		}
	}
}

type gateCall struct {
	requests   int
	start, end int64
}

// recordGates passes every evaluation except the one equal to fail, and
// records each call.
func recordGates(fail gateCall, mu *sync.Mutex, calls *[]gateCall) gateFunc {
	return func(_ *config.Config, requests []Request, _ []cpuSample, _, _ float64, start, end int64) GateReport {
		mu.Lock()
		defer mu.Unlock()
		c := gateCall{len(requests), start, end}
		*calls = append(*calls, c)
		return GateReport{Pass: c != fail}
	}
}

// A ladder of len(steps) steps: a 1 s warm-up, 2 s windows and 1.5 s
// drains, deterministic arrivals, a 1 s hard timeout.
func ladderRunCfg(baseURL string, steps ...float64) *config.Config {
	cfg := &config.Config{Profile: config.ProfileRelease, Target: config.Target{BaseURL: baseURL}}
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Load.Connections = 2
	cfg.Load.MaxTokens = 8
	cfg.Client.HTTPTimeoutS = 1
	cfg.Release = &config.Release{
		Ladder: config.Ladder{StepsRPS: steps, WarmupS: 1, StepS: 2, DrainS: 1.5},
		Shape:  config.Shape{DeadlineS: 1},
	}
	return cfg
}

// statusByHit answers the nth request with status(n), 1-based, and a
// complete stream on 200.
func statusByHit(t *testing.T, hits *atomic.Int64, status func(n int64) int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if s := status(hits.Add(1)); s != http.StatusOK {
			w.WriteHeader(s)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Two steps run to the last drain's end; each step's verdict covers its
// own window and measured requests.
func TestLadderRunsEveryStep(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
	var mu sync.Mutex
	var calls []gateCall
	begin := time.Now()
	res, err := run(context.Background(), ladderRunCfg(srv.URL, 2, 4), nil, recordGates(gateCall{}, &mu, &calls))
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(begin); took < 8*time.Second {
		t.Fatalf("returned after %v, before the last drain's end at 8 s", took)
	}
	if n := hits.Load(); n != 14 || len(res.Requests) != 14 {
		t.Fatalf("server saw %d of %d requests, want 14", n, len(res.Requests))
	}
	for i := range res.Requests {
		r := &res.Requests[i]
		step, warm := 1, i < 2
		if i >= 6 {
			step = 2
		}
		if r.Outcome != OutcomeCompleted || r.Step != step || r.Warmup != warm || r.Late {
			t.Fatalf("request %d: %+v", i, r)
		}
	}
	lr := res.Ladder
	if lr == nil || lr.End != "" || lr.EndStep != 0 || lr.WarmupScheduled != 2 || len(lr.Steps) != 2 {
		t.Fatalf("ladder %+v", lr)
	}
	for k, want := range []StepRecord{
		{RateRPS: 2, StartNs: 1e9, EndNs: 3e9, DrainEndNs: 4.5e9, Scheduled: 4, InFlightLimit: 4},
		{RateRPS: 4, StartNs: 4.5e9, EndNs: 6.5e9, DrainEndNs: 8e9, Scheduled: 8, InFlightLimit: 8},
	} {
		got := lr.Steps[k]
		if got.Gates == nil || !got.Gates.Pass || !got.Attempted || got.Tripwire || got.Status429 != 0 {
			t.Fatalf("step %d: %+v", k+1, got)
		}
		got.Gates, got.Attempted = nil, false
		if got != want {
			t.Fatalf("step %d: %+v, want %+v", k+1, got, want)
		}
	}
	if want := []gateCall{{4, 1e9, 3e9}, {8, 4.5e9, 6.5e9}, {14, 1e9, 6.5e9}}; !slices.Equal(calls, want) {
		t.Fatalf("gate calls %+v, want %+v", calls, want)
	}
	if !res.Gates.Pass || res.WarmupEndNs != 1e9 || res.RunEndNs != 8e9 {
		t.Fatalf("gates %+v, warm-up end %d, run end %d", res.Gates, res.WarmupEndNs, res.RunEndNs)
	}
	if w := res.Tripwires; w == nil || w.Fired || w.Scheduled != 14 || w.Dispatches != 14 || !slices.Equal(w.InFlightLimits, []int{4, 8}) {
		t.Fatalf("tripwires %+v", w)
	}
}

// Exactly half the first step's requests at 429 continues the ladder;
// all of the second step's ends it there, and the third is never sent.
func TestBackoffEndsLadder(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(n int64) int {
		if n == 3 || n == 4 || n >= 7 {
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	})
	var mu sync.Mutex
	var calls []gateCall
	res, err := run(context.Background(), ladderRunCfg(srv.URL, 2, 4, 4), nil, recordGates(gateCall{}, &mu, &calls))
	if err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != 14 {
		t.Fatalf("server saw %d requests, want 14", n)
	}
	lr := res.Ladder
	if lr.End != LadderEndBackoff || lr.EndStep != 2 {
		t.Fatalf("ladder ended %q at step %d", lr.End, lr.EndStep)
	}
	if s := lr.Steps[0]; !s.Attempted || s.Status429 != 2 || s.Scheduled != 4 {
		t.Fatalf("step 1: %+v", s)
	}
	if s := lr.Steps[1]; !s.Attempted || s.Status429 != 8 || s.Scheduled != 8 {
		t.Fatalf("step 2: %+v", s)
	}
	if s := lr.Steps[2]; s.Attempted || s.Gates != nil || s.Scheduled != 8 || s.Status429 != 0 {
		t.Fatalf("step 3: %+v", s)
	}
	for _, r := range res.Requests[14:] {
		if r.Step != 3 || r.DispatchNs != 0 || r.Outcome != "" {
			t.Fatalf("step 3 request sent: %+v", r)
		}
	}
	if !res.Gates.Pass || res.Gates.Undispatched != 0 || res.Interrupted != nil || res.Tripwires.Fired {
		t.Fatalf("gates %+v, interrupted %+v, tripwires %+v", res.Gates, res.Interrupted, res.Tripwires)
	}
	if last := calls[len(calls)-1]; last != (gateCall{14, 1e9, 6.5e9}) {
		t.Fatalf("run gate over %+v", last)
	}
}

// A client-validity failure at the first step ends the ladder there and
// fails the run's gate, which the run's own evaluation passed.
func TestClientValidityEndsLadder(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
	var mu sync.Mutex
	var calls []gateCall
	res, err := run(context.Background(), ladderRunCfg(srv.URL, 2, 4), nil, recordGates(gateCall{4, 1e9, 3e9}, &mu, &calls))
	if err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != 6 {
		t.Fatalf("server saw %d requests, want 6", n)
	}
	lr := res.Ladder
	if lr.End != LadderEndClientValidity || lr.EndStep != 1 || lr.Steps[0].Gates.Pass || lr.Steps[1].Attempted {
		t.Fatalf("ladder %+v", lr)
	}
	if want := []gateCall{{4, 1e9, 3e9}, {6, 1e9, 3e9}}; !slices.Equal(calls, want) {
		t.Fatalf("gate calls %+v, want %+v", calls, want)
	}
	if res.Gates.Pass {
		t.Fatal("run gate passed with a failed step")
	}
}

// A tripwire fired by a step's drain ends the ladder there.
func TestTripwireEndsLadder(t *testing.T) {
	var hits atomic.Int64
	srv := statusByHit(t, &hits, func(int64) int { return http.StatusOK })
	cfg := ladderRunCfg(srv.URL, 2, 4)
	cfg.Release.Ladder = config.Ladder{StepsRPS: []float64{2, 4}, WarmupS: 0.5, StepS: 0.5, DrainS: 0.5}
	l := BuildLadder(cfg)
	var mu sync.Mutex
	var calls []gateCall
	g := &gen{cfg: cfg, client: srv.Client(), epoch: time.Now(), filler: "x", model: "percentes-mock",
		trip: newLadderTripwire(cfg, l), gates: recordGates(gateCall{}, &mu, &calls)}
	requests := make([]Request, len(l.Offsets))
	for i := range requests {
		requests[i].Index = int64(i)
	}
	for i := 0; i < l.Steps[0].End+1; i++ {
		g.trip.send(1, 1)
	}
	pace := func(r *Request, wg *sync.WaitGroup) bool {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.execute(r)
		}()
		return true
	}
	rec := newLadderRecord(cfg, l)
	cpuMon := startCPUMonitor()
	defer cpuMon.stopAndCollect(g)
	if aborted, _ := g.runLadder(context.Background(), l, requests, rec, pace, cpuMon); aborted {
		t.Fatal("ladder reported a context stop")
	}
	if rec.End != LadderEndTripwire || rec.EndStep != 1 || !rec.Steps[0].Tripwire || rec.Steps[1].Attempted {
		t.Fatalf("ladder %+v", rec)
	}
	if n := hits.Load(); n != int64(l.Steps[0].End) {
		t.Fatalf("server saw %d requests, want %d", n, l.Steps[0].End)
	}
}

// A release completion after the deadline is late; the hard timeout is
// the configured one.
func TestReleaseDeadlineAndHardTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")) //nolint:errcheck
		w.(http.Flusher).Flush()
		select {
		case <-time.After(4 * time.Second):
		case <-r.Context().Done():
			return
		}
		w.Write([]byte("data: [DONE]\n\n")) //nolint:errcheck
	}))
	defer slow.Close()
	fast := statusByHit(t, new(atomic.Int64), func(int64) int { return http.StatusOK })

	exec := func(cfg *config.Config, srv *httptest.Server) *Request {
		cfg.Target.BaseURL = srv.URL
		g := &gen{cfg: cfg, client: srv.Client(), epoch: time.Now(), filler: "x", model: "percentes-mock"}
		r := &Request{}
		g.execute(r)
		return r
	}
	release := func(timeoutS int) *config.Config {
		cfg := ladderRunCfg("", 1)
		cfg.Client.HTTPTimeoutS = timeoutS
		return cfg
	}

	if r := exec(release(10), slow); r.Outcome != OutcomeCompleted || !r.Late {
		t.Fatalf("slow release completion: %v late=%v after %v", r.Outcome, r.Late, time.Duration(r.E2ENs()))
	}
	if r := exec(release(10), fast); r.Outcome != OutcomeCompleted || r.Late {
		t.Fatalf("fast release completion: %v late=%v after %v", r.Outcome, r.Late, time.Duration(r.E2ENs()))
	}
	experiment := release(10)
	experiment.Profile = config.ProfileExperiment
	if r := exec(experiment, slow); r.Outcome != OutcomeCompleted || r.Late {
		t.Fatalf("slow experiment completion: %v late=%v", r.Outcome, r.Late)
	}
	r := exec(release(1), slow)
	if took := time.Duration(r.DoneNs - r.DispatchNs); r.Outcome != OutcomeCensored || took < time.Second || took > 3*time.Second {
		t.Fatalf("release hard timeout 1 s: %v after %v", r.Outcome, took)
	}
}
