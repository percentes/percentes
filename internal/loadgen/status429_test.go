package loadgen

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"
)

func statusServer(status int, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		if body != "" {
			w.Write([]byte(body)) //nolint:errcheck
		}
	}))
}

// A 429 is errored under its own class (§3).
func TestStatus429Errored(t *testing.T) {
	srv := statusServer(http.StatusTooManyRequests, "")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrStatus429 {
		t.Fatalf("429 must be errored/%s, got %v/%q", ErrStatus429, r.Outcome, r.ErrClass)
	}
	if r.DoneNs == 0 {
		t.Fatal("errored request must carry its failure time")
	}
}

// SSE-looking bytes in a 429 body are not parsed as tokens.
func TestStatus429BodyIgnored(t *testing.T) {
	srv := statusServer(http.StatusTooManyRequests,
		"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrStatus429 {
		t.Fatalf("429 with a body must stay errored/%s, got %v/%q", ErrStatus429, r.Outcome, r.ErrClass)
	}
	if r.FirstTokNs != 0 || r.Tokens != 0 {
		t.Fatalf("429 body must carry no token: first=%d tokens=%d", r.FirstTokNs, r.Tokens)
	}
}

// 428, 430, 503, 400, and an unfollowed 302 are status_other.
func TestOtherStatusIsStatusOther(t *testing.T) {
	for _, status := range []int{428, 430, http.StatusServiceUnavailable, http.StatusBadRequest, http.StatusFound} {
		srv := statusServer(status, "")
		r := executeAgainst(srv)
		srv.Close()
		if r.Outcome != OutcomeErrored || r.ErrClass != ErrStatusOther {
			t.Fatalf("%d must be errored/%s, got %v/%q", status, ErrStatusOther, r.Outcome, r.ErrClass)
		}
	}
}

// DoneNs is taken at status arrival, before the stalled body.
func TestFailureTimeAtStatusArrival(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.(http.Flusher).Flush()
		time.Sleep(2 * time.Second)
		w.Write([]byte("late")) //nolint:errcheck
	}))
	defer srv.Close()

	r := executeAgainst(srv)
	if r.ErrClass != ErrStatus429 {
		t.Fatalf("expected %s, got %q", ErrStatus429, r.ErrClass)
	}
	if r.DoneNs > int64(2*time.Second) {
		t.Fatalf("failure time must be the status arrival, got %s", time.Duration(r.DoneNs))
	}
}

// A 200 stream that closes before [DONE] is malformed_stream (§3).
func TestEOFBeforeDoneMalformed(t *testing.T) {
	srv := sseServer(t, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
		t.Fatalf("EOF before [DONE] must be errored/%s, got %v/%q", ErrMalformedStream, r.Outcome, r.ErrClass)
	}
}

// A failure before any response is connect unless the transport reports
// a reset (§3).
func TestConnectionRefusedConnect(t *testing.T) {
	srv := statusServer(http.StatusOK, "")
	srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrConnect {
		t.Fatalf("refused connection must be errored/%s, got %v/%q", ErrConnect, r.Outcome, r.ErrClass)
	}
}

// A 429 whose body never arrives does not hold the request to the
// deadline: the body is not read.
func TestStatus429NoBodyReturnsAtOnce(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.(http.Flusher).Flush()
		<-release
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	r := executeAgainst(srv)
	if r.ErrClass != ErrStatus429 {
		t.Fatalf("expected %s, got %q", ErrStatus429, r.ErrClass)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("execute waited on the body: %s", elapsed)
	}
}

// A 200 with a non-SSE body ends before [DONE]: malformed_stream, no
// token (§3).
func Test200NonSSEBodyMalformed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`)) //nolint:errcheck
	}))
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
		t.Fatalf("JSON body must be errored/%s, got %v/%q", ErrMalformedStream, r.Outcome, r.ErrClass)
	}
	if r.Tokens != 0 {
		t.Fatalf("JSON body must carry no token, got %d", r.Tokens)
	}
}

// Deadline wins over reset when one error satisfies both tests (§3).
func TestPrecedenceDeadlineOverReset(t *testing.T) {
	g := &gen{epoch: time.Now()}
	both := errors.Join(context.DeadlineExceeded, syscall.ECONNRESET)

	r := &Request{}
	g.classifyTransportErr(r, both)
	if r.Outcome != OutcomeCensored || r.ErrClass != "" {
		t.Fatalf("transport: deadline+reset must be censored with no class, got %v/%q", r.Outcome, r.ErrClass)
	}
	r = &Request{}
	g.classifyStreamErr(r, both)
	if r.Outcome != OutcomeCensored || r.ErrClass != "" {
		t.Fatalf("stream: deadline+reset must be censored with no class, got %v/%q", r.Outcome, r.ErrClass)
	}
	r = &Request{}
	g.classifyStreamErr(r, syscall.ECONNRESET)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrReset {
		t.Fatalf("reset alone must be errored/%s, got %v/%q", ErrReset, r.Outcome, r.ErrClass)
	}
}
