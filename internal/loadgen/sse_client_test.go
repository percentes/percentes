package loadgen

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

const reflected = "SYNTHETIC_BEARER_5f84e713"

func executeCfg(srv *httptest.Server, cfg *config.Config, apiKey string) *Request {
	cfg.Run.Seed = 1
	cfg.Load.MaxTokens = 256
	if cfg.Target.BaseURL == "" {
		cfg.Target.BaseURL = srv.URL
	}
	g := &gen{cfg: cfg, client: srv.Client(), epoch: time.Now(), filler: "xyz", model: "m", apiKey: apiKey}
	r := &Request{Index: 1}
	g.execute(r)
	return r
}

// The replica header is recorded from a non-hosted mock target whose value
// is hostname-shaped, and only while the client holds no credential.
func TestReplicaHeaderIsRecordedOnlyFromTheMock(t *testing.T) {
	serve := func(replica string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Percentes-Replica", replica)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))
		}))
	}
	mock := func() *config.Config { return &config.Config{Mock: &config.Mock{}} }
	const pod = "percentes-mock-7d9f-abcde"
	const password = "synthetic-password-29sep"

	srv := serve(pod)
	defer srv.Close()
	if r := executeCfg(srv, &config.Config{Target: config.Target{Hosted: true}}, ""); r.Replica != "" {
		t.Errorf("hosted target recorded the header: %q", r.Replica)
	}
	if r := executeCfg(srv, &config.Config{}, ""); r.Replica != "" {
		t.Errorf("self-hosted target without a mock recorded the header: %q", r.Replica)
	}
	if r := executeCfg(srv, mock(), ""); r.Replica != pod {
		t.Errorf("mock target dropped its replica identity: %q", r.Replica)
	}
	hostedMock := mock()
	hostedMock.Target.Hosted = true
	if r := executeCfg(srv, hostedMock, "key"); r.Replica != "" {
		t.Errorf("a hosted target with a mock section recorded the header: %q", r.Replica)
	}
	if r := executeCfg(srv, mock(), "synthetic-bearer-29sep"); r.Replica != "" {
		t.Errorf("a client holding a bearer token recorded the header: %q", r.Replica)
	}
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		user, _, _ := req.BasicAuth()
		w.Header().Set("X-Percentes-Replica", user)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	defer echo.Close()
	cfg := mock()
	cfg.Target.BaseURL = strings.Replace(echo.URL, "http://", "http://synthetic-token-29sep@", 1)
	if r := executeCfg(echo, cfg, ""); r.Replica != "" {
		t.Errorf("a URL username sent as a credential was recorded: %q", r.Replica)
	}
	cfg = mock()
	cfg.Target.BaseURL = strings.Replace(echo.URL, "http://", "http://user:"+password+"@", 1)
	if r := executeCfg(echo, cfg, ""); r.Replica != "" {
		t.Errorf("a client holding a URL password recorded the header: %q", r.Replica)
	}
	tok := serve(reflected)
	defer tok.Close()
	if r := executeCfg(tok, mock(), ""); r.Replica != "" {
		t.Errorf("a value that is not hostname-shaped was recorded from the mock: %q", r.Replica)
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

// After [DONE] the body is drained, so a second request reuses the
// connection even when the server's closing bytes arrive late.
func TestCompletedStreamKeepsTheConnection(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))
		http.NewResponseController(w).Flush() //nolint:errcheck
		time.Sleep(50 * time.Millisecond)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	cfg := &config.Config{}
	cfg.Run.Seed = 1
	cfg.Load.MaxTokens = 256
	cfg.Target.BaseURL = srv.URL
	g := &gen{cfg: cfg, client: &http.Client{Transport: &http.Transport{}}, epoch: time.Now(), filler: "xyz", model: "m"}
	for i := 0; i < 3; i++ {
		r := &Request{Index: int64(i)}
		g.execute(r)
		if r.Outcome != OutcomeCompleted {
			t.Fatalf("request %d: %v/%q", i, r.Outcome, r.ErrClass)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("three completed requests opened %d connections, want 1", n)
	}
}

// A delivered redirect is a non-200 status, whatever its Location header
// holds, and it is never followed.
func TestDeliveredRedirectIsAStatus(t *testing.T) {
	for name, location := range map[string]string{"unparseable": "http://127.0.0.1/%zz", "well-formed": "http://127.0.0.1:1/elsewhere"} {
		t.Run(name, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.Header().Set("Location", location)
				w.WriteHeader(http.StatusFound)
			}))
			defer srv.Close()
			r := executeAgainst(srv)
			if r.Outcome != OutcomeErrored || r.ErrClass != ErrStatusOther {
				t.Fatalf("got %v/%q, want errored/%s", r.Outcome, r.ErrClass, ErrStatusOther)
			}
			if hits.Load() != 1 {
				t.Fatalf("server saw %d requests, want 1", hits.Load())
			}
		})
	}
}

// Userinfo in the base URL reaches the server as basic authentication,
// and a bearer token set by the request wins over it.
func TestUserinfoBecomesBasicAuth(t *testing.T) {
	var seen atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	defer srv.Close()
	cfg := &config.Config{}
	cfg.Run.Seed = 1
	cfg.Load.MaxTokens = 256
	cfg.Target.BaseURL = strings.Replace(srv.URL, "http://", "http://user:SYNTHETIC_PW_31ab@", 1)
	g := &gen{cfg: cfg, client: srv.Client(), epoch: time.Now(), filler: "xyz", model: "m"}
	r := &Request{Index: 1}
	g.execute(r)
	if r.Outcome != OutcomeCompleted {
		t.Fatalf("got %v/%q", r.Outcome, r.ErrClass)
	}
	if got := seen.Load(); got != "Basic dXNlcjpTWU5USEVUSUNfUFdfMzFhYg==" {
		t.Fatalf("server saw Authorization %q, want the basic credential from the URL", got)
	}
	g.apiKey = "SYNTHETIC_KEY_77c0"
	g.execute(&Request{Index: 2})
	if got := seen.Load(); got != "Bearer SYNTHETIC_KEY_77c0" {
		t.Fatalf("server saw Authorization %q, want the bearer token", got)
	}
}

// A chunk's usage object yields the completion token count; chunks
// without one yield nothing.
func TestUsageTokens(t *testing.T) {
	if n, ok := usageTokens([]byte(`{"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":17,"total_tokens":26}}`)); !ok || n != 17 {
		t.Fatalf("usage chunk: %d %v", n, ok)
	}
	for _, p := range []string{`{"choices":[{"delta":{"content":"x"}}]}`, `{"usage":null}`, `not json`} {
		if _, ok := usageTokens([]byte(p)); ok {
			t.Fatalf("%s must carry no usage", p)
		}
	}
}
