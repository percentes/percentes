package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

const completeStream = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n"

func countingServer(t *testing.T, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	return srv
}

func pilotRunCfg(baseURL string, baselineS float64) *config.Config {
	cfg := &config.Config{Profile: config.ProfilePilot, Target: config.Target{BaseURL: baseURL}}
	cfg.Load.RateRPS = 10
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Load.Connections = 2
	cfg.Load.MaxTokens = 8
	cfg.Run.Phases = config.Phases{BaselineS: baselineS}
	return cfg
}

func TestTripwireCounts(t *testing.T) {
	if newTripwire(&config.Config{Profile: config.ProfileExperiment}, 126).report() != nil {
		t.Fatal("a non-pilot run must carry no tripwires")
	}

	tw := newTripwire(&config.Config{Profile: config.ProfilePilot}, 126)
	for i := int64(1); i <= 4; i++ {
		tw.start(i, 0)
	}
	if r := tw.report(); r.Fired || r.MaxInFlight != 4 || r.OverInFlightNs != 0 || r.InFlightLimit != 4 {
		t.Fatalf("four in flight: %+v", r)
	}
	started := make(chan struct{})
	go func() {
		tw.start(5, 0)
		close(started)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the fifth dispatch waited on the tripwire")
	}
	if r := tw.report(); !r.Fired || r.MaxInFlight != 5 || r.OverInFlightNs != 5 {
		t.Fatalf("fifth in flight: %+v", r)
	}

	tw = newTripwire(&config.Config{Profile: config.ProfilePilot}, 126)
	for i := int64(1); i <= 126; i++ {
		tw.start(i, 0)
		tw.end()
	}
	if r := tw.report(); r.Fired || r.Dispatches != 126 || r.ExtraDispatchNs != 0 || r.MaxInFlight != 1 {
		t.Fatalf("126 dispatches: %+v", r)
	}
	tw.start(999, 0)
	if r := tw.report(); !r.Fired || r.Dispatches != 127 || r.ExtraDispatchNs != 999 || r.Scheduled != 126 {
		t.Fatalf("127th dispatch: %+v", r)
	}
}

// The 127th dispatch of a 126-request schedule is sent and recorded.
func TestExtraDispatchRecordedAndSent(t *testing.T) {
	var hits atomic.Int64
	srv := countingServer(t, &hits)
	cfg := pilotRunCfg(srv.URL, 0)
	g := &gen{cfg: cfg, client: srv.Client(), epoch: time.Now(), filler: "xyz", model: "percentes-mock", trip: newTripwire(cfg, 126)}
	reqs := make([]Request, 127)
	for i := range reqs {
		reqs[i].Index = int64(i)
		g.execute(&reqs[i])
		if i == 125 {
			if r := g.trip.report(); r.Fired {
				t.Fatalf("fired after 126 dispatches: %+v", r)
			}
		}
	}
	if n := hits.Load(); n != 127 {
		t.Fatalf("server saw %d requests, want 127", n)
	}
	for i := range reqs {
		if reqs[i].Outcome != OutcomeCompleted {
			t.Fatalf("request %d: %v/%q", i, reqs[i].Outcome, reqs[i].ErrClass)
		}
	}
	r := g.trip.report()
	if !r.Fired || r.Dispatches != 127 || r.ExtraDispatchNs != reqs[126].DispatchNs {
		t.Fatalf("tripwires %+v, 127th dispatch at %d", r, reqs[126].DispatchNs)
	}
}

// sendTwice sends every request twice and returns the second response.
type sendTwice struct{ rt http.RoundTripper }

func (s sendTwice) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	first := req.Clone(req.Context())
	first.Body = io.NopCloser(bytes.NewReader(body))
	if resp, err := s.rt.RoundTrip(first); err == nil {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
	}
	again := req.Clone(req.Context())
	again.Body = io.NopCloser(bytes.NewReader(body))
	return s.rt.RoundTrip(again)
}

// A second connection taken by one request counts as a further send.
func TestResendCountsAsDispatch(t *testing.T) {
	var hits atomic.Int64
	srv := countingServer(t, &hits)
	cfg := pilotRunCfg(srv.URL, 0)
	client := &http.Client{Transport: sendTwice{NewTransport(2)}}
	g := &gen{cfg: cfg, client: client, epoch: time.Now(), filler: "xyz", model: "percentes-mock", trip: newTripwire(cfg, 1)}
	r := &Request{}
	g.execute(r)
	if n := hits.Load(); n != 2 {
		t.Fatalf("server saw %d requests, want 2", n)
	}
	if w := g.trip.report(); !w.Fired || w.Dispatches != 2 || w.ExtraDispatchNs <= r.DispatchNs || w.MaxInFlight != 1 {
		t.Fatalf("tripwires %+v, dispatch at %d", w, r.DispatchNs)
	}
}

// The server holds every response until six requests are open at once,
// so the run completes only if the fifth and sixth are sent while four
// are in flight.
func TestInFlightTripwireNeverBlocks(t *testing.T) {
	var mu sync.Mutex
	open, maxOpen, seen := 0, 0, 0
	release := make(chan struct{})
	var byCount atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		open++
		seen++
		if open > maxOpen {
			maxOpen = open
		}
		if seen == 6 {
			byCount.Store(true)
			close(release)
		}
		mu.Unlock()
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
		mu.Lock()
		open--
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)

	res, err := Run(context.Background(), pilotRunCfg(srv.URL, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !byCount.Load() || maxOpen < 6 {
		t.Fatalf("server never held six requests at once (max %d)", maxOpen)
	}
	for i := range res.Requests {
		if r := &res.Requests[i]; r.Outcome != OutcomeCompleted {
			t.Fatalf("request %d: %v/%q", r.Index, r.Outcome, r.ErrClass)
		}
	}
	w := res.Tripwires
	if w == nil || !w.Fired || w.Scheduled != 10 || w.Dispatches != 10 || w.ExtraDispatchNs != 0 ||
		w.MaxInFlight < 6 || w.OverInFlightNs != res.Requests[4].DispatchNs {
		t.Fatalf("tripwires %+v, fifth dispatch at %d", w, res.Requests[4].DispatchNs)
	}
	if res.Interrupted != nil {
		t.Fatalf("interrupted %+v", res.Interrupted)
	}
}

func TestQuietRunTripsNothing(t *testing.T) {
	var hits atomic.Int64
	srv := countingServer(t, &hits)
	res, err := Run(context.Background(), pilotRunCfg(srv.URL, 0.5), nil)
	if err != nil {
		t.Fatal(err)
	}
	if w := res.Tripwires; w == nil || w.Fired || w.Scheduled != 5 || w.Dispatches != 5 || w.MaxInFlight < 1 || w.MaxInFlight > 4 {
		t.Fatalf("tripwires %+v", w)
	}

	cfg := pilotRunCfg(srv.URL, 0.5)
	cfg.Profile = config.ProfileExperiment
	res, err = Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res)
	if res.Tripwires != nil || strings.Contains(string(b), `"tripwires"`) || strings.Contains(string(b), `"interrupted"`) {
		t.Fatalf("non-pilot run: tripwires %+v, JSON %s", res.Tripwires, b)
	}
}

// A safety stop leaves the remaining requests unsent and records the
// counts and the cause.
func TestSafetyStopMarksInterrupted(t *testing.T) {
	for _, cause := range []error{ErrOperatorAbort, ErrLedgerCap, context.Canceled} {
		ctx, cancel := context.WithCancelCause(context.Background())
		var hits atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if hits.Add(1) == 3 {
				cancel(cause)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte(completeStream)) //nolint:errcheck
		}))

		res, err := Run(ctx, pilotRunCfg(srv.URL, 1), nil)
		srv.Close()
		cancel(nil)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("%v: err %v", cause, err)
		}
		dispatched := 0
		for i := range res.Requests {
			if res.Requests[i].DispatchNs != 0 {
				dispatched++
			}
		}
		in := res.Interrupted
		if in == nil || in.Cause != cause.Error() || in.Scheduled != 10 || in.Dispatched != dispatched ||
			int64(in.Dispatched) != hits.Load() || in.Dispatched < 3 || in.Unsent != 10-in.Dispatched || in.Unsent == 0 ||
			in.StopNs < res.Requests[2].DispatchNs {
			t.Fatalf("%v: interrupted %+v, dispatched %d, server saw %d", cause, in, dispatched, hits.Load())
		}
		if w := res.Tripwires; w == nil || w.Fired || int(w.Dispatches) != in.Dispatched {
			t.Fatalf("%v: tripwires %+v", cause, w)
		}
	}
}
