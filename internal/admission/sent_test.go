package admission

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

const (
	keyEnv  = "PERCENTES_ADMISSION_TEST_KEY"
	testKey = "gsk_ADMISSIONTESTKEY0123456789"

	answerFrame = "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"{\\\"route\\\":\\\"billing\\\"}\"}}]}\n\n"
	stopFrame   = "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	doneFrame   = "data: [DONE]\n\n"
)

// failingServer counts every request it reads. Request k gets, by k mod 5:
// a complete stream, a 500, a 429, the connection closed after the
// request is read with no response, or part of a stream and then the
// connection closed.
func failingServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) //nolint:errcheck
		k := n.Add(1) - 1
		hangUp := func() {
			c, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			c.Close()
		}
		switch k % 5 {
		case 0:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, answerFrame+stopFrame+doneFrame) //nolint:errcheck
		case 1:
			w.WriteHeader(http.StatusInternalServerError)
		case 2:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		case 3:
			hangUp()
		case 4:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, answerFrame) //nolint:errcheck
			w.(http.Flusher).Flush()
			hangUp()
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

// Every request that reaches the endpoint is in the ledger, and nothing
// else is: a window's probe and its run settle to exactly the requests
// the server read, through failures that end with no response on a
// reused connection, the case a retrying client would resend.
func TestSettlementIsEveryRequestSent(t *testing.T) {
	srv, served := failingServer(t)
	endpoint := srv.URL + "/openai/v1/chat/completions"

	pcfg, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tg := pcfg.Pilot.Targets[0]
	tg.EndpointURL, tg.APIKeyEnv = endpoint, keyEnv
	t.Setenv(keyEnv, testKey)

	l, _ := newLedger(t)
	must(t, l.RecordPrice(endpoint, groqPrice))
	a := admit(t, l, endpoint, PurposeWindow, WindowRequests)

	rec, err := probe.Run(context.Background(), probe.Input{Pilot: pcfg.Pilot, Target: &tg, CaseText: "My invoice was charged twice."})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Request.Status != http.StatusOK || served.Load() != 1 {
		t.Fatalf("probe status %d, served %d", rec.Request.Status, served.Load())
	}

	cfg := &config.Config{
		Run:    config.Run{Phases: config.Phases{WarmupS: 0.5, BaselineS: 2.5}},
		Load:   config.Load{RateRPS: 10, ArrivalProcess: "deterministic", Connections: 4, MaxTokens: 16},
		Target: config.Target{BaseURL: srv.URL},
	}
	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	reusedHangUp := false
	for _, r := range res.Requests {
		if r.Status == 0 && r.ConnReused {
			reusedHangUp = true
		}
	}
	if !reusedHangUp {
		t.Fatal("no request lost its response on a reused connection")
	}

	sent := WindowSent(rec, res)
	if want := (Counts{Probe: 1, Warmup: 5, Measured: 25}); sent != want {
		t.Fatalf("sent %+v, want %+v", sent, want)
	}
	must(t, l.Settle(a, sent))
	u := state(t, l).Endpoints[endpoint]
	if got := int64(u.Charged()); got != served.Load() || got != int64(1+len(res.Requests)) {
		t.Fatalf("ledger %d, server read %d, scheduled %d plus the probe", got, served.Load(), len(res.Requests))
	}
}

// A request that never left the client is not counted; one that did is,
// whatever came back.
func TestSentCounts(t *testing.T) {
	sentAt := time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		rec  *probe.Record
		want int
	}{
		{nil, 0},
		{&probe.Record{}, 0},
		{&probe.Record{Request: probe.Exchange{DispatchUTC: sentAt, Error: "dial tcp: connection refused"}}, 1},
		{&probe.Record{Request: probe.Exchange{DispatchUTC: sentAt, Status: 401}, WrongModel: &probe.Exchange{Error: "not sent: context canceled"}}, 1},
		{&probe.Record{Request: probe.Exchange{DispatchUTC: sentAt, Status: 200}, WrongModel: &probe.Exchange{DispatchUTC: sentAt, Status: 404},
			Limits: &probe.Exchange{DispatchUTC: sentAt, Status: 200}}, 2},
	} {
		if got := ProbeSent(c.rec); got != c.want {
			t.Errorf("ProbeSent(%+v) = %d, want %d", c.rec, got, c.want)
		}
	}
	res := &loadgen.Result{WarmupEndNs: 60e9, Requests: []loadgen.Request{
		{IntendedNs: 0, DispatchNs: 1, Outcome: loadgen.OutcomeCompleted},
		{IntendedNs: 50e9, DispatchNs: 50e9 + 1, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrConnect},
		{IntendedNs: 60e9, DispatchNs: 60e9 + 1, Outcome: loadgen.OutcomeCensored},
		{IntendedNs: 70e9, DispatchNs: 70e9 + 1, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429, Status: 429},
		{IntendedNs: 80e9},
	}}
	if w, m := RunSent(res); w != 2 || m != 2 {
		t.Errorf("RunSent = %d, %d, want 2, 2", w, m)
	}
	if w, m := RunSent(nil); w != 0 || m != 0 {
		t.Errorf("RunSent(nil) = %d, %d", w, m)
	}
}
