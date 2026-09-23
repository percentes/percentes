package loadgen

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

const reflected = "SYNTHETIC_BEARER_5f84e713"

func executeWith(srv *httptest.Server, hosted bool) *Request {
	cfg := &config.Config{}
	cfg.Run.Seed = 1
	cfg.Load.MaxTokens = 256
	cfg.Target.BaseURL = srv.URL
	cfg.Target.Hosted = hosted
	g := &gen{cfg: cfg, client: srv.Client(), epoch: time.Now(), filler: "xyz", model: "m"}
	r := &Request{Index: 1}
	g.execute(r)
	return r
}

// The replica header is recorded only from the mock.
func TestHostedReplicaHeaderIsNotRecorded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Percentes-Replica", reflected)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	defer srv.Close()

	if r := executeWith(srv, true); r.Replica != "" {
		t.Errorf("hosted target recorded the reflected header: %q", r.Replica)
	}
	if r := executeWith(srv, false); r.Replica != reflected {
		t.Errorf("mock target dropped its replica identity: %q", r.Replica)
	}
}

// Every framing the Server-Sent Events grammar permits completes with one
// token, and a stream that ends before [DONE] is still malformed.
func TestValidFramingsComplete(t *testing.T) {
	const one = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
	cases := []struct {
		name string
		body string
	}{
		{"plain", one},
		{"byte-order mark", "\xef\xbb\xbf" + one},
		{"carriage returns only", strings.ReplaceAll(one, "\n", "\r")},
		{"crlf", strings.ReplaceAll(one, "\n", "\r\n")},
		{"no space after the colon", strings.ReplaceAll(one, "data: ", "data:")},
		{"one event across two data lines", "data: {\"choices\":[{\"delta\":\ndata: {\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"},
		{"trailing space after [DONE]", strings.Replace(one, "[DONE]\n", "[DONE] \n", 1)},
		{"space-named field inside the event", "data: {\"choices\":[{\"delta\":\n \ndata: {\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := sseServer(t, c.body)
			defer srv.Close()
			r := executeAgainst(srv)
			if r.Outcome != OutcomeCompleted || r.Tokens != 1 {
				t.Fatalf("got %v/%q tokens=%d, want completed with one token", r.Outcome, r.ErrClass, r.Tokens)
			}
		})
	}
	t.Run("ends before [DONE]", func(t *testing.T) {
		srv := sseServer(t, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		defer srv.Close()
		if r := executeAgainst(srv); r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
			t.Fatalf("got %v/%q, want errored/%s", r.Outcome, r.ErrClass, ErrMalformedStream)
		}
	})
}

// An oversized event is dropped and the stream is malformed, so a server
// that sends no blank line cannot hold the client's body.
func TestOversizedEventIsMalformed(t *testing.T) {
	var lines strings.Builder
	for i := 0; i < 4; i++ {
		lines.WriteString("data: " + strings.Repeat("x", 512<<10) + "\n")
	}
	for name, body := range map[string]string{
		"four data lines":  lines.String() + "\ndata: [DONE]\n\n",
		"single data line": "data: " + strings.Repeat("x", 2<<20) + "\n\ndata: [DONE]\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			srv := sseServer(t, body)
			defer srv.Close()
			if r := executeAgainst(srv); r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
				t.Fatalf("got %v/%q, want errored/%s", r.Outcome, r.ErrClass, ErrMalformedStream)
			}
		})
	}
}

// The request body is built by the JSON encoder, so a model id the
// validator accepts cannot produce an invalid document.
func TestRequestBodyIsValidJSON(t *testing.T) {
	for _, model := range []string{"plain-model", "m\x01odel", "quo\"ted", "back\\slash", "unié"} {
		g := testGen(true, model, "")
		body := g.requestBody(&Request{Index: 3})
		if !json.Valid([]byte(body)) {
			t.Errorf("model %q: body is not JSON: %s", model, body)
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			t.Fatal(err)
		}
		if m["model"] != model {
			t.Errorf("model %q round-tripped as %q", model, m["model"])
		}
		if _, has := m["ignore_eos"]; has {
			t.Errorf("hosted body carries ignore_eos")
		}
	}
	if m := map[string]any{}; json.Unmarshal([]byte(testGen(false, "m", "").requestBody(&Request{})), &m) == nil && m["ignore_eos"] != true {
		t.Error("mock body must carry ignore_eos: true")
	}
}
