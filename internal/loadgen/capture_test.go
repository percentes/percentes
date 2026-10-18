package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

const testKey = "sk-capture-test-0123456789"

// captureGen is a gen holding testKey, recording the allowlisted headers.
func captureGen(srv *httptest.Server, allow ...string) *gen {
	cfg := &config.Config{}
	cfg.Run.Seed = 1
	cfg.Load.MaxTokens = 256
	cfg.Target.BaseURL = srv.URL
	cfg.Target.Hosted = true
	cfg.Target.ResponseHeaders = allow
	return &gen{cfg: cfg, client: &http.Client{Transport: NewTransport(1)}, epoch: time.Now(), filler: "xyz", model: "m", apiKey: testKey}
}

// Only allowlisted headers are kept: never a credential header, never a
// value carrying the bearer token, never a header outside the list.
func TestHeadersAllowlistOnly(t *testing.T) {
	var sawAuth atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		auth := req.Header.Get("Authorization")
		sawAuth.Store(auth == "Bearer "+testKey)
		h := w.Header()
		h.Set("Authorization", auth)
		h.Set("Set-Cookie", "s=1")
		h.Set("X-Echo", auth)
		h.Set("X-Other", "outside the list")
		h.Set("Retry-After", "7")
		h.Set("X-Ratelimit-Remaining-Requests", "9")
		h.Add("X-Ratelimit-Reset-Tokens", "1s")
		h.Add("X-Ratelimit-Reset-Tokens", "2s")
		h.Set("X-Request-Id", "req-1")
		h.Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")) //nolint:errcheck
	}))
	defer srv.Close()

	g := captureGen(srv, "authorization", "Set-Cookie", "x-echo", "retry-after", "x-ratelimit-*", "X-Request-Id")
	r := &Request{Index: 1}
	g.execute(r)
	if !sawAuth.Load() {
		t.Fatal("server did not receive the bearer token; the test proves nothing")
	}
	want := map[string][]string{
		"Retry-After":                    {"7"},
		"X-Ratelimit-Remaining-Requests": {"9"},
		"X-Ratelimit-Reset-Tokens":       {"1s", "2s"},
		"X-Request-Id":                   {"req-1"},
	}
	if r.Outcome != OutcomeCompleted || r.Status != http.StatusOK || !reflect.DeepEqual(r.Headers, want) {
		t.Fatalf("got %v/%d headers %v, want completed/200 headers %v", r.Outcome, r.Status, r.Headers, want)
	}
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{testKey, "Authorization", "Set-Cookie", "X-Echo", "X-Other"} {
		if bytes.Contains(out, []byte(s)) {
			t.Errorf("record carries %q: %s", s, out)
		}
	}
}

// The values of retained headers leave the record for RetainedHeaders,
// on a refusal and on a stream alike; every other allowlisted header
// keeps its values.
func TestRetainedHeadersLeaveTheRecord(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusOK} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			h := w.Header()
			h.Set("Retry-After", "7")
			h.Set("X-Ratelimit-Limit-Tokens", "limit-tokens-250000")
			h.Add("X-Ratelimit-Limit-Requests", "limit-requests-1000")
			h.Add("X-Ratelimit-Limit-Requests", "limit-requests-14400")
			h.Set("X-Ratelimit-Remaining-Tokens", "249000")
			h.Set("X-Request-Id", "request-id-0001")
			if status != http.StatusOK {
				w.WriteHeader(status)
				return
			}
			h.Set("Content-Type", "text/event-stream")
			w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")) //nolint:errcheck
		}))
		t.Cleanup(srv.Close)
		g := captureGen(srv, "Retry-After", "x-ratelimit-*", "X-Request-Id")
		g.cfg.Target.RetainedHeaders = []string{"x-ratelimit-limit-*", "x-request-id"}
		r := &Request{Index: 1}
		g.execute(r)

		mark := config.RetainedHeaderValue
		want := map[string][]string{
			"Retry-After":                  {"7"},
			"X-Ratelimit-Limit-Tokens":     {mark},
			"X-Ratelimit-Limit-Requests":   {mark, mark},
			"X-Ratelimit-Remaining-Tokens": {"249000"},
			"X-Request-Id":                 {mark},
		}
		kept := map[string][]string{
			"X-Ratelimit-Limit-Tokens":   {"limit-tokens-250000"},
			"X-Ratelimit-Limit-Requests": {"limit-requests-1000", "limit-requests-14400"},
			"X-Request-Id":               {"request-id-0001"},
		}
		if r.Status != status || !reflect.DeepEqual(r.Headers, want) || !reflect.DeepEqual(r.RetainedHeaders, kept) {
			t.Fatalf("status %d: headers %v retained %v, want %v and %v", r.Status, r.Headers, r.RetainedHeaders, want, kept)
		}
		out, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range []string{"limit-tokens", "limit-requests", "request-id-0001"} {
			if bytes.Contains(out, []byte(s)) {
				t.Errorf("status %d: record carries %q: %s", status, s, out)
			}
		}
	}
}

// A 429 is recorded with its status and headers, once per scheduled
// request: never retried, Retry-After never honoured.
func TestRun429RecordedNotRetried(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "7")
		w.Header().Set("X-Ratelimit-Remaining-Requests", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limited"}`)) //nolint:errcheck
	}))
	defer srv.Close()
	t.Setenv("PERCENTES_CAPTURE_TEST_KEY", testKey)

	cfg := &config.Config{Target: config.Target{
		BaseURL: srv.URL, Hosted: true, ModelName: "m", APIKeyEnv: "PERCENTES_CAPTURE_TEST_KEY",
		ResponseHeaders: []string{"Retry-After", "X-Ratelimit-*"},
	}}
	cfg.Load.RateRPS = 10
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Load.Connections = 2
	cfg.Load.MaxTokens = 8
	cfg.Run.Phases = config.Phases{BaselineS: 0.3}

	res, err := Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != int64(len(res.Requests)) || n == 0 {
		t.Fatalf("server saw %d requests for %d scheduled", n, len(res.Requests))
	}
	want := map[string][]string{"Retry-After": {"7"}, "X-Ratelimit-Remaining-Requests": {"0"}}
	for i := range res.Requests {
		r := &res.Requests[i]
		if r.ErrClass != ErrStatus429 || r.Status != http.StatusTooManyRequests || !reflect.DeepEqual(r.Headers, want) {
			t.Fatalf("request %d: %v/%q status %d headers %v", r.Index, r.Outcome, r.ErrClass, r.Status, r.Headers)
		}
		if string(r.ErrorBody) != `{"error":"rate limited"}` {
			t.Fatalf("request %d: error body %q", r.Index, r.ErrorBody)
		}
	}
}

// The peer address comes from the connection; the second request on the
// same transport reuses it.
func TestPeerAddrAndReuse(t *testing.T) {
	srv := sseServer(t, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
	defer srv.Close()

	g := captureGen(srv)
	for i, reused := range []bool{false, true} {
		r := &Request{Index: int64(i)}
		g.execute(r)
		if r.Outcome != OutcomeCompleted || r.PeerAddr != srv.Listener.Addr().String() || r.ConnReused != reused {
			t.Fatalf("request %d: %v peer %q reused %v, want completed peer %q reused %v",
				i, r.Outcome, r.PeerAddr, r.ConnReused, srv.Listener.Addr().String(), reused)
		}
	}
}

// A non-200 body is kept to errorBodyLimit bytes with the bearer token
// masked, including one that straddles the limit, and stays out of JSON.
func TestErrorBodyKeptMasked(t *testing.T) {
	body := "err " + testKey + " " + strings.Repeat("x", errorBodyLimit-10-len(testKey)-5) + testKey + strings.Repeat("y", 1000)
	srv := statusServer(http.StatusServiceUnavailable, body)
	defer srv.Close()

	r := &Request{Index: 1}
	captureGen(srv).execute(r)
	if r.ErrClass != ErrStatusOther || r.Status != http.StatusServiceUnavailable {
		t.Fatalf("got %v/%q status %d", r.Outcome, r.ErrClass, r.Status)
	}
	if len(r.ErrorBody) != errorBodyLimit {
		t.Fatalf("kept %d bytes, want %d", len(r.ErrorBody), errorBodyLimit)
	}
	if !strings.HasPrefix(string(r.ErrorBody), "err "+strings.Repeat("*", len(testKey))+" ") ||
		!strings.HasSuffix(string(r.ErrorBody), "x"+strings.Repeat("*", 10)) {
		t.Fatalf("token not masked at both positions: %q ... %q", r.ErrorBody[:40], r.ErrorBody[errorBodyLimit-20:])
	}
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(out, []byte("err ")) || bytes.Contains(out, []byte("xxxx")) {
		t.Fatalf("error body reached JSON: %s", out)
	}
}

// A non-200 body that stalls keeps the bytes that arrived, and the read
// ends errorBodyWait after the status.
func TestErrorBodyStallKeepsPartial(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("partial")) //nolint:errcheck
		w.(http.Flusher).Flush()
		<-release
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	r := &Request{Index: 1}
	captureGen(srv).execute(r)
	if string(r.ErrorBody) != "partial" {
		t.Fatalf("error body %q, want %q", r.ErrorBody, "partial")
	}
	if elapsed := time.Since(start); elapsed > errorBodyWait+time.Second {
		t.Fatalf("body read ran %s", elapsed)
	}
}

// A connect failure records its Go error string.
func TestConnectRecordsErrText(t *testing.T) {
	srv := statusServer(http.StatusOK, "")
	srv.Close()

	r := executeAgainst(srv)
	if r.ErrClass != ErrConnect || !strings.Contains(r.ErrText, "connection refused") {
		t.Fatalf("got %v/%q err_text %q", r.Outcome, r.ErrClass, r.ErrText)
	}
	if r.Status != 0 || r.PeerAddr != "" {
		t.Fatalf("no response, no connection: status %d peer %q", r.Status, r.PeerAddr)
	}
}
