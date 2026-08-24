package loadgen

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

func sseServer(t *testing.T, frames ...string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, f := range frames {
			if _, err := w.Write([]byte(f)); err != nil {
				t.Errorf("sse write: %v", err)
			}
		}
	}))
}

func executeAgainst(srv *httptest.Server) *Request {
	cfg := &config.Config{}
	cfg.Run.Seed = 1
	cfg.Load.MaxTokens = 256
	cfg.Target.BaseURL = srv.URL
	g := &gen{cfg: cfg, client: srv.Client(), epoch: time.Now(), filler: "xyz", model: "percentes-mock"}
	r := &Request{Index: 1}
	g.execute(r)
	return r
}

// A stream that terminates with [DONE] and no content event carries no
// token: errored (empty_stream), never a completion (§3). A completed
// request with FirstTokNs zero would make TTFTNs negative downstream.
func TestEmptyStreamErrored(t *testing.T) {
	srv := sseServer(t, "data: [DONE]\n\n")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrEmptyStream {
		t.Fatalf("empty stream must be errored/%s, got %v/%q", ErrEmptyStream, r.Outcome, r.ErrClass)
	}
	if r.FirstTokNs != 0 || r.Tokens != 0 {
		t.Fatalf("empty stream must carry no token: first=%d tokens=%d", r.FirstTokNs, r.Tokens)
	}
	if r.DoneNs == 0 {
		t.Fatal("errored request must carry its failure time")
	}
}

// One content event then [DONE] stays a completion with a positive TTFT.
func TestContentThenDoneCompleted(t *testing.T) {
	srv := sseServer(t,
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
		"data: [DONE]\n\n")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeCompleted || r.Tokens != 1 {
		t.Fatalf("content+[DONE] must complete with one token, got %v tokens=%d", r.Outcome, r.Tokens)
	}
	if r.TTFTNs() <= 0 {
		t.Fatalf("completed TTFT must be positive, got %d", r.TTFTNs())
	}
}
