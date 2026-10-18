package probe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

const (
	keyEnv   = "PERCENTES_PROBE_TEST_KEY"
	testKey  = "gsk_PROBETESTKEY0123456789abcdefXYZ"
	caseText = "My invoice was charged twice."
)

// reference returns the pilot reference config's body and first target,
// pointed at url with the test key's variable.
func reference(t *testing.T, url string) (*config.Pilot, *config.Target) {
	t.Helper()
	c, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tg := c.Pilot.Targets[0]
	tg.EndpointURL, tg.APIKeyEnv = url, keyEnv
	t.Setenv(keyEnv, testKey)
	return c.Pilot, &tg
}

func probe(t *testing.T, in Input) *Record {
	t.Helper()
	rec, err := Run(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// sseHandler answers every request with status 200 and the frames.
func sseHandler(frames ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, f := range frames {
			io.WriteString(w, f) //nolint:errcheck
			w.(http.Flusher).Flush()
		}
	}
}

const (
	answerFrame = "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"{\\\"route\\\":\\\"billing\\\"}\"}}]}\n\n"
	stopFrame   = "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	doneFrame   = "data: [DONE]\n\n"
)

// The probe request is the session's: the pilot body for the case with
// the target's model and params, the session's headers, POSTed to the
// endpoint URL.
func TestProbeSendsTheSessionRequest(t *testing.T) {
	var mu sync.Mutex
	var got struct {
		method, path, auth, ctype, accept string
		body                              []byte
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		got.method, got.path = r.Method, r.URL.Path
		got.auth, got.ctype, got.accept = r.Header.Get("Authorization"), r.Header.Get("Content-Type"), r.Header.Get("Accept")
		got.body, _ = io.ReadAll(r.Body)
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/openai/v1/chat/completions")
	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	mu.Lock()
	defer mu.Unlock()

	want := pilot.RequestBody(tg, caseText)
	if !bytes.Equal(got.body, want) {
		t.Fatalf("body sent:\n%s\nwant:\n%s", got.body, want)
	}
	if got.method != http.MethodPost || got.path != "/openai/v1/chat/completions" || got.auth != "Bearer "+testKey ||
		got.ctype != "application/json" || got.accept != "" {
		t.Fatalf("request %+v", got)
	}
	sum := sha256.Sum256(want)
	if rec.BodySHA256 != hex.EncodeToString(sum[:]) || rec.BodyBytes != len(want) {
		t.Errorf("body hash %s (%d bytes), want %x (%d)", rec.BodySHA256, rec.BodyBytes, sum, len(want))
	}
	if rec.Target != "groq" || rec.RequestedModel != "openai/gpt-oss-120b" || rec.Request.Model != "openai/gpt-oss-120b" {
		t.Errorf("target %q requested %q request model %q", rec.Target, rec.RequestedModel, rec.Request.Model)
	}
}

// The D1 mock's hosted stream: model id, delta keys, finish reasons,
// usage placement, protocol and the connection are recorded, and a
// reasoning key the target does not configure is a finding.
func TestProbeRecordsTheShape(t *testing.T) {
	fixed := config.LatencyDist{Distribution: "fixed", FixedMs: 1}
	srv := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed},
		mock.Shape{ReasoningKey: "reasoning", ReasoningTokens: 3, Answer: `{"route":"billing"}`})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	pilot, tg := reference(t, "http://"+srv.Addr()+"/v1/chat/completions")

	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	ex, s := rec.Request, rec.Shape
	if ex.Status != 200 || ex.Proto != "HTTP/1.1" || ex.PeerAddr != srv.Addr() || ex.DNS != nil || ex.TLS != nil {
		t.Fatalf("exchange: status %d proto %q peer %q dns %v tls %v", ex.Status, ex.Proto, ex.PeerAddr, ex.DNS, ex.TLS)
	}
	if len(ex.Connects) != 1 || ex.Connects[0].Addr != srv.Addr() || ex.Connects[0].Error != "" || ex.FirstByteNs <= 0 || ex.DoneNs < ex.FirstByteNs {
		t.Fatalf("connects %+v first byte %d done %d", ex.Connects, ex.FirstByteNs, ex.DoneNs)
	}
	if s == nil || !s.Done || s.Undecodable != 0 || s.ContentEvents == 0 || s.ReasoningEvents != 0 {
		t.Fatalf("shape %+v", s)
	}
	if d := s.DeltaKeys["reasoning"]; d == nil || d.Events != 3 || d.StringBytes != len("r1 r2 r3 ") || d.Types[0] != "string" {
		t.Fatalf("reasoning key %+v", d)
	}
	if d := s.DeltaKeys["content"]; d == nil || d.StringBytes != len(`{"route":"billing"}`) {
		t.Fatalf("content key %+v", d)
	}
	if fmt.Sprint(s.FinishReasons) != "[stop]" || fmt.Sprint(s.Models) != "[openai/gpt-oss-120b]" {
		t.Fatalf("finish reasons %q models %q", s.FinishReasons, s.Models)
	}
	if len(s.Usage) != 1 || s.Usage[0] != (UsageAt{Path: "usage", Events: 1, FirstEvent: s.Events - 1, LastEvent: s.Events - 1, EmptyChoices: 1}) {
		t.Fatalf("usage %+v over %d events", s.Usage, s.Events)
	}
	if !strings.Contains(s.LastUsage, `"completion_tokens":`) {
		t.Errorf("last usage %q", s.LastUsage)
	}
	if fmt.Sprint(rec.Findings) != `[delta key "reasoning" carries text and is neither the content key nor a configured reasoning key]` {
		t.Errorf("findings %q", rec.Findings)
	}
	if rec.Start.Blocked {
		t.Errorf("start blocked: %s", rec.Start.Reason)
	}

	tg.ResponseShape = &config.ResponseShape{Reasoning: []string{"reasoning"}}
	rec = probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	if len(rec.Findings) != 0 || rec.Shape.ReasoningEvents != 3 || rec.Shape.FirstReasoningNs > rec.Shape.FirstContentNs {
		t.Errorf("configured reasoning: findings %q, reasoning events %d at %d, content at %d",
			rec.Findings, rec.Shape.ReasoningEvents, rec.Shape.FirstReasoningNs, rec.Shape.FirstContentNs)
	}
}

// A usage object nested under a provider key, in a chunk with no
// choice, is found where it is.
func TestProbeUsagePlacement(t *testing.T) {
	srv := httptest.NewServer(sseHandler(answerFrame, stopFrame,
		"data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[],\"x_groq\":{\"id\":\"r\",\"usage\":{\"completion_tokens\":9}}}\n\n",
		doneFrame))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	s := rec.Shape
	if len(s.Usage) != 1 || s.Usage[0] != (UsageAt{Path: "x_groq.usage", Events: 1, FirstEvent: 2, LastEvent: 2, EmptyChoices: 1}) || s.LastUsage != `{"completion_tokens":9}` {
		t.Fatalf("usage %+v last %q", s.Usage, s.LastUsage)
	}
	if s.TopLevelKeys["x_groq"] != 1 || s.TopLevelKeys["choices"] != 3 {
		t.Errorf("top-level keys %v", s.TopLevelKeys)
	}
	if fmt.Sprint(rec.Findings) != `[usage object at "x_groq.usage", not a configured usage path]` {
		t.Errorf("findings %q", rec.Findings)
	}
	tg.ResponseShape = &config.ResponseShape{Usage: []string{"x_groq.usage"}}
	if rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText}); len(rec.Findings) != 0 {
		t.Errorf("configured usage path: findings %q", rec.Findings)
	}
}

// Only 400, 401, 403, 404 and the target's account-limit statuses block
// the start; every other status, and no response at all, is recorded and
// the window starts.
func TestProbeStartGate(t *testing.T) {
	var status atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status.Load() == 200 {
			sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
			return
		}
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(int(status.Load()))
		io.WriteString(w, `{"error":{"message":"refused"}}`) //nolint:errcheck
	}))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")

	for _, c := range []struct {
		status  int
		limits  []int
		blocked bool
	}{
		{200, nil, false},
		{400, nil, true},
		{401, nil, true},
		{403, nil, true},
		{404, nil, true},
		{402, nil, false},
		{402, []int{402}, true},
		{429, nil, false},
		{500, nil, false},
		{503, nil, false},
		{529, nil, false},
		{529, []int{402}, false},
		{302, nil, false},
	} {
		status.Store(int64(c.status))
		tg.Probe = nil
		if c.limits != nil {
			tg.Probe = &config.TargetProbe{AccountLimitStatuses: c.limits}
		}
		rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
		if rec.Request.Status != c.status || rec.Start.Blocked != c.blocked || (rec.Start.Reason != "") != c.blocked {
			t.Errorf("status %d, limits %v: recorded %d, start %+v", c.status, c.limits, rec.Request.Status, rec.Start)
		}
		if c.status != 200 && (rec.Request.Body == nil || *rec.Request.Body.Text != `{"error":{"message":"refused"}}` || rec.Shape != nil ||
			fmt.Sprint(rec.Request.Headers["Retry-After"]) != "[7]") {
			t.Errorf("status %d: body %+v shape %v headers %v", c.status, rec.Request.Body, rec.Shape, rec.Request.Headers)
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	_, tg = reference(t, "http://"+closed+"/v1/chat/completions")
	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	if rec.Start.Blocked || rec.Request.Status != 0 || rec.Request.Error == "" || len(rec.Request.Connects) != 1 || rec.Request.Connects[0].Error == "" {
		t.Errorf("refused dial: start %+v status %d error %q connects %+v", rec.Start, rec.Request.Status, rec.Request.Error, rec.Request.Connects)
	}
}

// A request still unanswered at the pinned timeout is recorded and does
// not block the start.
func TestProbeTimeoutStarts(t *testing.T) {
	defer func(d time.Duration) { requestTimeout = d }(requestTimeout)
	requestTimeout = 200 * time.Millisecond
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	if rec.Start.Blocked || rec.Request.Status != 0 || rec.Request.Error != "timed out after 200ms" {
		t.Errorf("start %+v status %d error %q", rec.Start, rec.Request.Status, rec.Request.Error)
	}
}

// The wrong-model request goes the session's interval after the probe
// request, records whatever status returns, and never gates the start;
// neither does the limits read.
func TestProbeWrongModelAndLimitsOutsideTheGate(t *testing.T) {
	defer func(d time.Duration) { spacing = d }(spacing)
	spacing = 300 * time.Millisecond
	var mu sync.Mutex
	var posts []time.Time
	var limitsAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			limitsAuth = r.Header.Get("Authorization")
			mu.Unlock()
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"error":"unauthorized"}`) //nolint:errcheck
			return
		}
		mu.Lock()
		posts = append(posts, time.Now())
		mu.Unlock()
		var req struct{ Model string }
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		if req.Model != "openai/gpt-oss-120b" {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"error":{"message":"model not found"}}`) //nolint:errcheck
			return
		}
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	tg.Probe = &config.TargetProbe{LimitsURL: srv.URL + "/v1/model_apis/gpt-oss-120b", LimitsAuthScheme: "Api-Key"}

	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText, WrongModel: "openai/gpt-oss-120b-nonexistent"})
	if rec.Start.Blocked || rec.Request.Status != 200 {
		t.Fatalf("start %+v request status %d", rec.Start, rec.Request.Status)
	}
	w := rec.WrongModel
	if w == nil || w.Status != 404 || w.Model != "openai/gpt-oss-120b-nonexistent" || *w.Body.Text != `{"error":{"message":"model not found"}}` {
		t.Fatalf("wrong model %+v", w)
	}
	mu.Lock()
	// Handler arrival trails dispatch by scheduling jitter of a few milliseconds.
	if len(posts) != 2 || posts[1].Sub(posts[0]) < spacing-50*time.Millisecond {
		t.Errorf("posts at %v, want two at least %s apart", posts, spacing)
	}
	sentAuth := limitsAuth
	mu.Unlock()
	l := rec.Limits
	if l == nil || l.Method != http.MethodGet || l.URL != srv.URL+"/v1/model_apis/gpt-oss-120b" || l.Status != 401 || *l.Body.Text != `{"error":"unauthorized"}` || sentAuth != "Api-Key "+testKey {
		t.Fatalf("limits %+v, authorization sent %q", l, sentAuth)
	}

	ctx, cancel := context.WithCancel(context.Background())
	spacing = time.Hour
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	rec, err := Run(ctx, Input{Pilot: pilot, Target: tg, CaseText: caseText, WrongModel: "other"})
	if err != nil || rec.WrongModel == nil || rec.WrongModel.Status != 0 || rec.WrongModel.Error != "not sent: context canceled" || rec.Start.Blocked {
		t.Errorf("cancelled before the wrong-model request: %v, %+v", err, rec.WrongModel)
	}
}

// Over TLS from a server offering h2, the probe negotiates HTTP/1.1 and
// records the handshake; a host name is resolved and the answer
// recorded.
func TestProbeTLSAndDNS(t *testing.T) {
	srv := httptest.NewUnstartedServer(sseHandler(answerFrame, stopFrame, doneFrame))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	roots := srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	session := newTransport
	defer func() { newTransport = session }()
	newTransport = func(n int) *http.Transport {
		tr := session(n)
		// A request with no URL fixes the protocol defaults before the root
		// is added; adding a TLS config first would turn h2 off by itself.
		tr.RoundTrip(&http.Request{}) //nolint:errcheck,bodyclose
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{}
		}
		tr.TLSClientConfig.RootCAs = roots
		return tr
	}
	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	ex := rec.Request
	if ex.Status != 200 || ex.Proto != "HTTP/1.1" || ex.TLS == nil || ex.TLS.Error != "" || ex.TLS.Version == "" || ex.TLS.CipherSuite == "" ||
		ex.TLS.ALPN == "h2" || ex.TLS.Certificate == nil || ex.TLS.DurationNs <= 0 {
		t.Fatalf("tls exchange: status %d proto %q tls %+v", ex.Status, ex.Proto, ex.TLS)
	}

	plain := httptest.NewServer(sseHandler(answerFrame, stopFrame, doneFrame))
	defer plain.Close()
	_, port, _ := net.SplitHostPort(plain.Listener.Addr().String())
	_, tg = reference(t, "http://localhost:"+port+"/v1/chat/completions")
	rec = probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	if d := rec.Request.DNS; d == nil || d.Host != "localhost" || len(d.Addrs) == 0 || d.Error != "" || rec.Request.Status != 200 {
		t.Fatalf("dns %+v status %d error %q", d, rec.Request.Status, rec.Request.Error)
	}
}

// An endpoint echoing the bearer token into every part of every response
// leaves neither the token nor any run of redact.SecretMin bytes of it in
// the record or the written file.
func TestProbeNeverRecordsTheKey(t *testing.T) {
	defer func(d time.Duration) { spacing = d }(spacing)
	spacing = 0
	var auths []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		auths = append(auths, auth)
		mu.Unlock()
		_, key, _ := strings.Cut(auth, " ")
		w.Header().Set("X-Ratelimit-Echo", key)
		w.Header().Set("X-Ratelimit-Remaining", "99")
		w.Header().Set("X-Ratelimit-"+key, "1")
		w.Header().Set("Retry-After", key[:12])
		if r.Method == http.MethodGet {
			fmt.Fprintf(w, `{"limits":{"rpm":15},"auth":%q,"prefix":%q}`, auth, key[:10])
			return
		}
		var req struct{ Model string }
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		if req.Model != "openai/gpt-oss-120b" {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{"error":"no model for key %s (%s...)"}`, key, key[:9])
			return
		}
		sseHandler(
			fmt.Sprintf("data: {\"model\":%q,\"system_fingerprint\":%q,%q:1,\"choices\":[{\"delta\":{%q:\"x\",\"content\":%q,\"reasoning\":%q}}]}\n\n", key, key, key, key, key, key[3:20]),
			fmt.Sprintf("data: {\"choices\":[{\"delta\":{},\"finish_reason\":%q}],\"error\":{\"message\":%q}}\n\n", key, key),
			fmt.Sprintf("data: {\"choices\":[],\"usage\":{\"completion_tokens\":1,\"note\":%q}}\n\n", key),
			doneFrame)(w, r)
	}))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	tg.Probe = &config.TargetProbe{LimitsURL: srv.URL + "/v1/model_apis/x", LimitsAuthScheme: "Api-Key"}
	tg.ResponseShape = &config.ResponseShape{Reasoning: []string{"reasoning"}}

	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText, WrongModel: "wrong"})
	path, err := Write(t.TempDir(), rec)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lower := bytes.ToLower(raw)
	for i := 0; i+8 <= len(testKey); i++ {
		if run := strings.ToLower(testKey[i : i+8]); bytes.Contains(lower, []byte(run)) {
			t.Fatalf("written record holds %q of the key, in some letter case", run)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(auths) != 3 || auths[0] != "Bearer "+testKey || auths[1] != "Api-Key "+testKey || auths[2] != "Bearer "+testKey {
		t.Fatalf("authorization sent: %q", auths)
	}
	masked := strings.Repeat("*", len(testKey))
	s := rec.Shape
	if s.Models[0] != masked || s.TopLevelKeys[masked] != 1 || s.DeltaKeys[masked] == nil || s.FinishReasons[0] != masked ||
		!strings.Contains(s.Error, masked) || !strings.Contains(s.LastUsage, masked) || s.ContentEvents != 1 || s.ReasoningEvents != 1 {
		t.Fatalf("shape %+v", s)
	}
	h := rec.Request.Headers
	if fmt.Sprint(h["X-Ratelimit-Remaining"]) != "[99]" || h["X-Ratelimit-Echo"] != nil || h["Retry-After"] != nil || len(h) != 1 {
		t.Errorf("headers %v", h)
	}
	if !strings.Contains(*rec.Limits.Body.Text, `"rpm":15`) || !strings.Contains(*rec.WrongModel.Body.Text, masked) {
		t.Errorf("limits body %q, wrong-model body %q", *rec.Limits.Body.Text, *rec.WrongModel.Body.Text)
	}
}

// With the key's variable empty nothing is sent.
func TestProbeNeedsTheKey(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	t.Setenv(keyEnv, "")
	if rec, err := Run(context.Background(), Input{Pilot: pilot, Target: tg, CaseText: caseText}); err == nil || rec != nil || hits.Load() != 0 ||
		!strings.Contains(err.Error(), keyEnv) {
		t.Fatalf("got %v, %v after %d requests", rec, err, hits.Load())
	}
}

func TestWriteRoundTrip(t *testing.T) {
	srv := httptest.NewServer(sseHandler(answerFrame, stopFrame, doneFrame))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	rec := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	path, err := Write(t.TempDir(), rec)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var back Record
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Request.Status != 200 || back.Shape.ContentEvents != 1 || back.EndpointURL != tg.EndpointURL || back.Start.Blocked ||
		!bytes.Contains(raw, []byte(`"blocked": false`)) {
		t.Errorf("round trip %+v\n%s", back, raw)
	}
}

// A body past the limit is cut there and marked; a key starting inside
// the limit is masked whole.
func TestProbeBodyCutAtLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, strings.Repeat("a", errorBodyLimit-6)+testKey+strings.Repeat("b", 100)) //nolint:errcheck
	}))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/chat/completions")
	b := probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText}).Request.Body
	if b == nil || b.Text == nil || len(*b.Text) != errorBodyLimit || !b.Truncated || !strings.HasSuffix(*b.Text, "a******") {
		t.Fatalf("body %+v", b)
	}
}

// A target's auth_header carries the key on the probe request, alone;
// the limits read keeps its own Authorization scheme.
func TestProbeKeyHeader(t *testing.T) {
	var mu sync.Mutex
	var post, get http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Method == http.MethodGet {
			get = r.Header.Clone()
			mu.Unlock()
			io.WriteString(w, `{}`) //nolint:errcheck
			return
		}
		post = r.Header.Clone()
		mu.Unlock()
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	pilot, tg := reference(t, srv.URL+"/v1/messages")
	tg.AuthHeader = config.AuthXAPIKey
	tg.Probe = &config.TargetProbe{LimitsURL: srv.URL + "/v1/limits", LimitsAuthScheme: "Api-Key"}
	probe(t, Input{Pilot: pilot, Target: tg, CaseText: caseText})
	mu.Lock()
	defer mu.Unlock()
	if v := post.Values("X-Api-Key"); len(v) != 1 || v[0] != testKey {
		t.Errorf("probe request X-Api-Key %q", v)
	}
	if v := post.Values("Authorization"); len(v) != 0 {
		t.Errorf("probe request also sent Authorization %q", v)
	}
	if v := get.Get("Authorization"); v != "Api-Key "+testKey || len(get.Values("X-Api-Key")) != 0 {
		t.Errorf("limits read Authorization %q, X-Api-Key %q", v, get.Values("X-Api-Key"))
	}
}
