package loadgen

import (
	"encoding/json"
	"math"
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

// A chunk's usage object yields the completion token count, an explicit
// zero included; a usage object whose count is missing or null, and a
// chunk without one, yield nothing.
func TestUsageTokens(t *testing.T) {
	if n, ok := usageTokens([]byte(`{"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":17,"total_tokens":26}}`)); !ok || n != 17 {
		t.Fatalf("usage chunk: %d %v", n, ok)
	}
	if n, ok := usageTokens([]byte(`{"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":0}}`)); !ok || n != 0 {
		t.Fatalf("explicit zero: %d %v", n, ok)
	}
	for _, p := range []string{
		`{"choices":[{"delta":{"content":"x"}}]}`, `{"usage":null}`, `not json`,
		`{"choices":[],"usage":{"prompt_tokens":12}}`, `{"choices":[],"usage":{}}`,
		`{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":null}}`,
	} {
		if n, ok := usageTokens([]byte(p)); ok {
			t.Fatalf("%s must carry no completion count, got %d", p, n)
		}
	}
}

// A later usage object lays its non-null fields over the earlier ones, so
// a count one object carried survives a later object without it; a stream
// whose only usage object lacks the completion count records none.
func TestUsageWithoutCompletionCount(t *testing.T) {
	usage := func(u string) string { return `data: {"choices":[],"usage":` + u + "}\n\n" }
	for _, c := range []struct {
		name   string
		frames []string
		seen   bool
		n      int
		raw    string
	}{
		{"prompt only after a count", []string{usage(`{"completion_tokens":7}`), usage(`{"prompt_tokens":12}`)}, true, 7, `{"completion_tokens":7,"prompt_tokens":12}`},
		{"count after prompt only", []string{usage(`{"prompt_tokens":12}`), usage(`{"completion_tokens":7}`)}, true, 7, `{"completion_tokens":7,"prompt_tokens":12}`},
		{"null after a count", []string{usage(`{"completion_tokens":7}`), usage(`{"completion_tokens":null}`)}, true, 7, `{"completion_tokens":7}`},
		{"null prompt after a count", []string{usage(`{"prompt_tokens":12,"completion_tokens":7}`), usage(`{"prompt_tokens":null,"completion_tokens":8}`)}, true, 8, `{"completion_tokens":8,"prompt_tokens":12}`},
		{"zero after a count", []string{usage(`{"completion_tokens":7}`), usage(`{"completion_tokens":0}`)}, true, 0, `{"completion_tokens":0}`},
		{"prompt only", []string{usage(`{"prompt_tokens":12}`)}, false, 0, `{"prompt_tokens":12}`},
		{"empty", []string{usage(`{}`)}, false, 0, `{}`},
		{"null count", []string{usage(`{"prompt_tokens":3,"completion_tokens":null}`)}, false, 0, `{"prompt_tokens":3}`},
		{"not an object", []string{usage(`{"prompt_tokens":3}`), usage(`5`)}, false, 0, `5`},
	} {
		t.Run(c.name, func(t *testing.T) {
			frames := append(append([]string{answerFrame, stopFrame}, c.frames...), doneFrame)
			r := executeShaped(t, true, nil, frames...)
			if r.Outcome != OutcomeCompleted || r.Tokens != 1 {
				t.Fatalf("got %v/%q tokens=%d", r.Outcome, r.ErrClass, r.Tokens)
			}
			if r.UsageSeen != c.seen || r.CompletionTokens != c.n || string(r.Usage) != c.raw {
				t.Fatalf("usage seen %v tokens %d raw %s, want %v %d %s", r.UsageSeen, r.CompletionTokens, r.Usage, c.seen, c.n, c.raw)
			}
		})
	}
}

// A request records when its connection was acquired and its request
// written, in that order after its dispatch; a dial that fails records
// neither.
func TestConnectionAndWriteTimes(t *testing.T) {
	srv := sseServer(t, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
	defer srv.Close()
	g := captureGen(srv)
	defer g.client.CloseIdleConnections()
	r := new(Request)
	g.execute(r)
	if r.Outcome != OutcomeCompleted || r.ConnAcquiredNs < r.DispatchNs || r.WroteRequestNs < r.ConnAcquiredNs || r.ConnAcquiredNs == 0 || r.DoneNs < r.WroteRequestNs {
		t.Fatalf("dispatch %d, connection %d, written %d, done %d", r.DispatchNs, r.ConnAcquiredNs, r.WroteRequestNs, r.DoneNs)
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	if r := executeAgainst(closed); r.ErrClass != ErrConnect || r.ConnAcquiredNs != 0 || r.WroteRequestNs != 0 {
		t.Fatalf("failed dial: %q, connection %d, written %d", r.ErrClass, r.ConnAcquiredNs, r.WroteRequestNs)
	}
}

// A non-200 body's error object gives its type, code and status string as
// received, a numeric code as its JSON text; the raw body is kept.
func TestErrorFields(t *testing.T) {
	for _, c := range []struct {
		name, body        string
		typ, code, status string
	}{
		{"messages", `{"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your rate limit"}}`, "rate_limit_error", "", ""},
		{"chat completions", `{"error":{"message":"Requests have exceeded the rate limit.","type":"requests","param":null,"code":"429"}}`, "requests", "429", ""},
		{"array of one", `[{"error":{"code":429,"message":"Resource has been exhausted.","status":"RESOURCE_EXHAUSTED"}}]`, "", "429", "RESOURCE_EXHAUSTED"},
		{"no error object", `{"code":"rate_limit_exceeded","status":"RESOURCE_EXHAUSTED"}`, "", "rate_limit_exceeded", "RESOURCE_EXHAUSTED"},
		{"fields of other types", `{"error":{"type":7,"code":{"n":1},"status":["x"]}}`, "", "", ""},
		{"not JSON", `upstream connect error`, "", "", ""},
		{"cut short", `{"error":{"type":"rate_limit_error","mes`, "", "", ""},
	} {
		typ, code, status := errorFields([]byte(c.body))
		if typ != c.typ || code != c.code || status != c.status {
			t.Errorf("%s: %q %q %q, want %q %q %q", c.name, typ, code, status, c.typ, c.code, c.status)
		}
	}
	if typ, _, _ := errorFields([]byte(`{"error":{"type":"` + strings.Repeat("x", 300) + `"}}`)); len(typ) != errorFieldLimit {
		t.Errorf("a long type kept %d bytes, want %d", len(typ), errorFieldLimit)
	}
	const body = `{"error":{"code":"insufficient_quota","status":"PAYMENT_REQUIRED"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	r := executeAgainst(srv)
	if r.ErrClass != ErrStatus429 || string(r.ErrorBody) != body || r.ErrorCode != "insufficient_quota" || r.ErrorStatus != "PAYMENT_REQUIRED" || r.ErrorType != "" {
		t.Fatalf("%q body %q, fields %q %q %q", r.ErrClass, r.ErrorBody, r.ErrorType, r.ErrorCode, r.ErrorStatus)
	}
}

// A stream a server resets leaves err_text naming the peer and no address of
// this client.
func TestCutStreamErrTextOmitsClientAddress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		c.(*net.TCPConn).SetLinger(0) //nolint:errcheck
		c.Close()
	}))
	defer srv.Close()
	r := executeAgainst(srv)
	if r.ErrClass != ErrReset || !strings.Contains(r.ErrText, "connection reset by peer") {
		t.Fatalf("%q, text %q", r.ErrClass, r.ErrText)
	}
	if strings.Contains(r.ErrText, "->") {
		t.Errorf("err_text holds a source address: %q", r.ErrText)
	}
}

// A request's reported prompt count is a chat-completions prompt_tokens or
// a Messages input_tokens with its cache counts, and its output count,
// reasoning included, is the largest of the completion count, the total
// less the prompt count and any reasoning count; nothing reported is no
// count, never zero, and neither is a negative count. A prompt count or
// sum past math.MaxInt is math.MaxInt.
func TestReportedTokens(t *testing.T) {
	for _, c := range []struct {
		name           string
		r              Request
		prompt, output int
		hasP, hasO     bool
	}{
		{"chat", Request{Usage: json.RawMessage(`{"prompt_tokens":120,"completion_tokens":16,"total_tokens":136}`), CompletionTokens: 16, UsageSeen: true}, 120, 16, true, true},
		{"reasoning outside the completion count", Request{Usage: json.RawMessage(`{"prompt_tokens":120,"completion_tokens":16,"total_tokens":400}`), CompletionTokens: 16, UsageSeen: true}, 120, 280, true, true},
		{"reasoning count", Request{Usage: json.RawMessage(`{"prompt_tokens":9,"completion_tokens":16,"completion_tokens_details":{"reasoning_tokens":30}}`), CompletionTokens: 16, UsageSeen: true}, 9, 30, true, true},
		{"messages", Request{Usage: json.RawMessage(`{"input_tokens":100,"cache_creation_input_tokens":20,"cache_read_input_tokens":8,"output_tokens":16}`), CompletionTokens: 16, UsageSeen: true}, 128, 16, true, true},
		{"completion count in usage only", Request{Usage: json.RawMessage(`{"prompt_tokens":5,"completion_tokens":12}`)}, 5, 12, true, true},
		{"prompt only", Request{Usage: json.RawMessage(`{"prompt_tokens":5}`)}, 5, 0, true, false},
		{"no usage", Request{}, 0, 0, false, false},
		{"negative prompt", Request{Usage: json.RawMessage(`{"prompt_tokens":-10000,"completion_tokens":-1}`)}, 0, 0, false, false},
		{"negative messages output", Request{Usage: json.RawMessage(`{"input_tokens":-1,"output_tokens":-5}`), CompletionTokens: -5, UsageSeen: true}, 0, 0, false, false},
		{"cache sum overflows", Request{Usage: json.RawMessage(`{"input_tokens":9223372036854775807,"cache_read_input_tokens":1}`)}, math.MaxInt, 0, true, false},
		{"prompt past int64", Request{Usage: json.RawMessage(`{"prompt_tokens":99999999999999999999}`)}, math.MaxInt, 0, true, false},
	} {
		p, okP := c.r.PromptTokens()
		o, okO := c.r.OutputTokens()
		if p != c.prompt || o != c.output || okP != c.hasP || okO != c.hasO {
			t.Errorf("%s: prompt %d %t, output %d %t; want %d %t, %d %t", c.name, p, okP, o, okO, c.prompt, c.hasP, c.output, c.hasO)
		}
	}
}
