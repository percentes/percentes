package probe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

const ticket = "My invoice was charged twice this month."

// byteCap stands for the ticket byte cap the texts were cut at.
const byteCap = 2000

// releaseConfig returns the release reference config, pointed at url with
// the test key's variable.
func releaseConfig(t *testing.T, url string) *config.Config {
	t.Helper()
	c, err := config.LoadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c.Target.EndpointURL, c.Target.APIKeyEnv = url, keyEnv
	t.Setenv(keyEnv, testKey)
	return c
}

// messagesBody is a Messages route's body at ceiling.
func messagesBody(ceiling int) string {
	return fmt.Sprintf(`{"model": "claude-sonnet-5-5", "system": "System text.",
 "messages": [{"role": "user", "content": "{{case}}"}],
 "stream": true, "max_tokens": %d,
 "thinking": {"type": "between_tools"}, "output_config": {"effort": "low"}}`, ceiling)
}

// messagesConfig turns c into a Messages route at ceiling with the key in
// x-api-key.
func messagesConfig(c *config.Config, ceiling int) *config.Config {
	c.Target.Name, c.Target.ModelName, c.Target.Adapter = "anthropic", "claude-sonnet-5-5", config.AdapterMessages
	c.Target.AuthHeader, c.Target.Body, c.Target.ResponseShape = config.AuthXAPIKey, messagesBody(ceiling), nil
	c.Release.Shape.CeilingTokens = ceiling
	return c
}

// atCeiling sets a chat route's ceiling and its body's ceiling key.
func atCeiling(t *testing.T, c *config.Config, ceiling int) {
	t.Helper()
	old := fmt.Sprintf(`"max_completion_tokens": %d`, c.Release.Shape.CeilingTokens)
	if !strings.Contains(c.Target.Body, old) {
		t.Fatalf("body has no %s", old)
	}
	c.Target.Body = strings.Replace(c.Target.Body, old, fmt.Sprintf(`"max_completion_tokens": %d`, ceiling), 1)
	c.Release.Shape.CeilingTokens = ceiling
}

func texts(words int) []string {
	out := make([]string, CeilingRequests)
	for i := range out {
		out[i] = fmt.Sprintf("cs-7-%06d %s", i, ticket)
		if words > 0 {
			out[i] += strings.Repeat(" w", words-len(strings.Fields(out[i])))
		}
	}
	return out
}

func releaseProbe(t *testing.T, in ReleaseInput) *ReleaseRecord {
	t.Helper()
	rec, err := RunRelease(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// noSpacing removes the spacing between dispatches for the test.
func noSpacing(t *testing.T) {
	d := releaseSpacing
	releaseSpacing = 0
	t.Cleanup(func() { releaseSpacing = d })
}

func startShaped(t *testing.T, sh mock.Shape) string {
	t.Helper()
	fixed := config.LatencyDist{Distribution: "fixed"}
	s := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed}, sh)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s.Addr()
}

// A chat route whose reasoning spends the 512 ceiling fails every request
// of the check there, with the finish reason named, and passes at 1,024;
// each request sends the configured body with its text, and the reported
// prompt tokens meet the input bound.
func TestReleaseProbeChatCeiling(t *testing.T) {
	noSpacing(t)
	answer := "The customer was billed twice."
	addr := startShaped(t, mock.Shape{ReasoningKey: "reasoning", ReasoningTokens: 510, Answer: answer})
	cfg := releaseConfig(t, "http://"+addr+"/v1/chat/completions")
	cfg.Target.ResponseShape = &config.ResponseShape{Reasoning: []string{"reasoning"}}
	in := ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}

	rec := releaseProbe(t, in)
	c := rec.Ceiling
	if c != (CeilingCheck{Ceiling: 512, LengthValue: "length", Sent: 10, Passed: 0}) || len(rec.Requests) != 10 || rec.Start.Blocked {
		t.Fatalf("at 512: check %+v, %d requests, start %+v", c, len(rec.Requests), rec.Start)
	}
	for i, r := range rec.Requests {
		if r.Index != i || r.Pass || r.Fail != `finish reason "length": stopped at the ceiling` || r.FinishReason != "length" ||
			r.Shape.ContentEvents == 0 || r.CompletionTokens == nil || *r.CompletionTokens != 512 {
			t.Fatalf("request %d at 512: %+v", i, r)
		}
	}

	atCeiling(t, cfg, 1024)
	rec = releaseProbe(t, in)
	if rec.Ceiling != (CeilingCheck{Ceiling: 1024, LengthValue: "length", Sent: 10, Passed: 10, Pass: true}) {
		t.Fatalf("at 1024: check %+v", rec.Ceiling)
	}
	want := len(strings.Fields(in.Texts[0]))
	if rec.InputBound != (InputCheck{BoundTokens: 1200, Reported: 10, MaxPromptTokens: want, Pass: true}) {
		t.Errorf("input bound %+v", rec.InputBound)
	}
	r := rec.Requests[3]
	if r.FinishReason != "stop" || *r.PromptTokens != want || r.Exchange.Status != 200 || r.Exchange.Model != "openai/gpt-oss-120b" {
		t.Errorf("request 3 at 1024: %+v", r)
	}
	sum := sha256.Sum256([]byte(cfg.Target.Body))
	if rec.BodySHA256 != hex.EncodeToString(sum[:]) || rec.Route != "groq" || rec.Adapter != config.AdapterChatCompletions || rec.Sent() != 10 || rec.ByteCap != byteCap {
		t.Errorf("record %s %s %s, sent %d, byte cap %d", rec.BodySHA256, rec.Route, rec.Adapter, rec.Sent(), rec.ByteCap)
	}
	if len(rec.Findings) != 0 {
		t.Errorf("findings %q", rec.Findings)
	}
}

// The bodies sent are the configured body with each text in place, every
// other byte unchanged.
func TestReleaseProbeSendsTheConfiguredBody(t *testing.T) {
	noSpacing(t)
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/openai/v1/chat/completions")
	in := ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}
	releaseProbe(t, in)
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 10 {
		t.Fatalf("%d bodies", len(bodies))
	}
	for i, b := range bodies {
		if want := config.LiteralBody(cfg.Target.Body, in.Texts[i]); !bytes.Equal(b, want) {
			t.Fatalf("body %d:\n%s\nwant:\n%s", i, b, want)
		}
	}
}

// A prompt over the input bound on any request fails the input check; a
// request without a reported count leaves it unverified.
func TestReleaseProbeInputBound(t *testing.T) {
	noSpacing(t)
	addr := startShaped(t, mock.Shape{Answer: "Billed twice."})
	cfg := releaseConfig(t, "http://"+addr+"/v1/chat/completions")
	in := ReleaseInput{Config: cfg, Texts: texts(1200), ByteCap: byteCap}
	if rec := releaseProbe(t, in); rec.InputBound != (InputCheck{BoundTokens: 1200, Reported: 10, MaxPromptTokens: 1200, Pass: true}) {
		t.Errorf("at the bound: %+v", rec.InputBound)
	}
	in.Texts[6] += " over"
	if rec := releaseProbe(t, in); rec.InputBound != (InputCheck{BoundTokens: 1200, Reported: 10, MaxPromptTokens: 1201, Exceeded: 1}) ||
		!rec.Ceiling.Pass {
		t.Errorf("one over: %+v, ceiling %+v", rec.InputBound, rec.Ceiling)
	}

	plain := httptest.NewServer(sseHandler(answerFrame, stopFrame, doneFrame))
	defer plain.Close()
	cfg = releaseConfig(t, plain.URL+"/v1/chat/completions")
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if rec.InputBound != (InputCheck{BoundTokens: 1200}) || !rec.Ceiling.Pass || rec.Requests[0].PromptTokens != nil {
		t.Errorf("no usage: %+v, ceiling %+v", rec.InputBound, rec.Ceiling)
	}
}

// A null prompt or completion count in a chat usage object is absent, so
// the input bound is not reported as met; an explicit zero is a count.
func TestReleaseProbeNullUsageCounts(t *testing.T) {
	noSpacing(t)
	for _, c := range []struct {
		name  string
		usage string
		want  string
		input InputCheck
	}{
		{"null", `{"prompt_tokens":null,"completion_tokens":null}`, "absent", InputCheck{BoundTokens: 1200}},
		{"zero", `{"prompt_tokens":0,"completion_tokens":0}`, "0", InputCheck{BoundTokens: 1200, Reported: 10, Pass: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			usage := `data: {"model":"openai/gpt-oss-120b","choices":[],"usage":` + c.usage + "}\n\n"
			srv := httptest.NewServer(sseHandler(answerFrame, stopFrame, usage, doneFrame))
			defer srv.Close()
			rec := releaseProbe(t, ReleaseInput{Config: releaseConfig(t, srv.URL+"/v1/chat/completions"), Texts: texts(0), ByteCap: byteCap})
			r := rec.Requests[0]
			count := func(p *int) string {
				if p == nil {
					return "absent"
				}
				return fmt.Sprint(*p)
			}
			if count(r.PromptTokens) != c.want || count(r.CompletionTokens) != c.want {
				t.Errorf("prompt %s completion %s, want %s", count(r.PromptTokens), count(r.CompletionTokens), c.want)
			}
			if rec.InputBound != c.input || !rec.Ceiling.Pass {
				t.Errorf("input %+v, ceiling %+v", rec.InputBound, rec.Ceiling)
			}
		})
	}
}

// The Messages adapter: the key travels in x-api-key with the
// anthropic-version header, the shape is read from the event types, and
// the ceiling check reads stop_reason.
func TestReleaseProbeMessages(t *testing.T) {
	noSpacing(t)
	answer := "The customer was billed twice."
	addr := startShaped(t, mock.Shape{Messages: true, Key: testKey, AuthHeader: config.AuthXAPIKey, ReasoningTokens: 3, Answer: answer})
	cfg := messagesConfig(releaseConfig(t, "http://"+addr+"/v1/messages"), 512)
	in := ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}
	rec := releaseProbe(t, in)
	if rec.Ceiling != (CeilingCheck{Ceiling: 512, LengthValue: "max_tokens", Sent: 10, Passed: 10, Pass: true}) || !rec.InputBound.Pass {
		t.Fatalf("check %+v, input %+v, first request %+v", rec.Ceiling, rec.InputBound, rec.Requests[0])
	}
	r := rec.Requests[0]
	s := r.Shape
	if r.FinishReason != "end_turn" || *r.PromptTokens != len(strings.Fields(in.Texts[0])) || *r.CompletionTokens != 3+8 {
		t.Fatalf("request %+v", r)
	}
	if !s.Done || s.ContentEvents != 8 || s.ReasoningEvents != 3 || fmt.Sprint(s.Models) != "[claude-sonnet-5-5]" || fmt.Sprint(s.FinishReasons) != "[end_turn]" {
		t.Fatalf("shape %+v", s)
	}
	if fmt.Sprint(s.EventTypes) != "map[content_block_delta:12 content_block_start:2 content_block_stop:2 message_delta:1 message_start:1]" ||
		fmt.Sprint(s.DeltaTypes) != "map[block:text:1 block:thinking:1 signature_delta:1 text_delta:8 thinking_delta:3]" {
		t.Fatalf("event types %v, delta types %v", s.EventTypes, s.DeltaTypes)
	}
	if len(s.Usage) != 2 || s.Usage[0].Path != "message.usage" || s.Usage[1].Path != "usage" || s.Events != 18 {
		t.Fatalf("usage %+v over %d events", s.Usage, s.Events)
	}
	if len(rec.Findings) != 0 {
		t.Errorf("findings %q", rec.Findings)
	}

	addr = startShaped(t, mock.Shape{Messages: true, Key: testKey, AuthHeader: config.AuthXAPIKey, ReasoningTokens: 510, Answer: answer})
	cfg.Target.EndpointURL = "http://" + addr + "/v1/messages"
	rec = releaseProbe(t, in)
	if rec.Ceiling.Passed != 0 || rec.Requests[9].Fail != `finish reason "max_tokens": stopped at the ceiling` {
		t.Errorf("at the ceiling: %+v, last request %+v", rec.Ceiling, rec.Requests[9])
	}

	cfg.Target.AuthHeader = config.AuthBearer
	rec = releaseProbe(t, in)
	if !rec.Start.Blocked || len(rec.Requests) != 1 || rec.Requests[0].Fail != "status 401" || rec.Sent() != 1 {
		t.Errorf("bearer to an x-api-key mock: start %+v, %d requests", rec.Start, len(rec.Requests))
	}
}

// A Messages prompt count adds both cache counts to input_tokens; the
// usage objects of message_start and message_delta merge.
func TestReleaseProbeMessagesPromptTokens(t *testing.T) {
	noSpacing(t)
	ev := func(v string) string { return "event: x\ndata: " + v + "\n\n" }
	srv := httptest.NewServer(sseHandler(
		ev(`{"type":"message_start","message":{"model":"claude-sonnet-5-5","usage":{"input_tokens":5,"cache_creation_input_tokens":100,"cache_read_input_tokens":1100,"output_tokens":1}}}`),
		ev(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Billed."}}`),
		ev(`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":4}}`),
		ev(`{"type":"message_stop"}`)))
	defer srv.Close()
	cfg := messagesConfig(releaseConfig(t, srv.URL+"/v1/messages"), 512)
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	r := rec.Requests[0]
	if *r.PromptTokens != 1205 || *r.CompletionTokens != 4 || rec.InputBound.Exceeded != 10 || rec.InputBound.Pass || !rec.Ceiling.Pass {
		t.Errorf("request %+v, input %+v, ceiling %+v", r, rec.InputBound, rec.Ceiling)
	}
	if len(rec.Findings) != 0 {
		t.Errorf("findings %q", rec.Findings)
	}
}

// A Messages stream outside the adapter's reading is a finding per gap.
func TestReleaseProbeMessagesFindings(t *testing.T) {
	noSpacing(t)
	ev := func(v string) string { return "event: x\ndata: " + v + "\n\n" }
	srv := httptest.NewServer(sseHandler(
		ev(`{"type":"message_start","message":{"model":"claude-sonnet-5-5-20261001"}}`),
		ev(`{"type":"content_block_delta","index":0,"delta":{"type":"citations_delta","citation":{}}}`),
		ev(`{"type":"x_new_event"}`),
		ev(`{"type":"message_delta","delta":{"stop_reason":null}}`)))
	defer srv.Close()
	cfg := messagesConfig(releaseConfig(t, srv.URL+"/v1/messages"), 512)
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if fmt.Sprint(rec.Findings) != `[the stream ended without message_stop no text_delta carried text `+
		`event type "x_new_event" is not one the Messages adapter reads delta type "citations_delta" is not one the Messages adapter reads `+
		`no usage object in the stream no stop_reason in the stream reported model "claude-sonnet-5-5-20261001" differs from the requested "claude-sonnet-5-5"]` {
		t.Errorf("findings %q", rec.Findings)
	}
	if rec.Requests[0].Fail != "no terminator" {
		t.Errorf("request %+v", rec.Requests[0])
	}
}

// Each way a request can fail the ceiling check is named, and only the
// gate's statuses stop the requests.
func TestReleaseProbeFailures(t *testing.T) {
	noSpacing(t)
	frames := map[string][]string{
		"no terminator":      {answerFrame, stopFrame},
		"in-band error":      {answerFrame, "data: {\"error\":{\"message\":\"overloaded\"}}\n\n", doneFrame},
		"no visible content": {stopFrame, doneFrame},
		"no finish reason":   {answerFrame, doneFrame},
		"malformed stream":   {answerFrame, "data: not json\n\n", stopFrame, doneFrame},
	}
	var mode atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := mode.Load().(string)
		if f, ok := frames[m]; ok {
			sseHandler(f...)(w, r)
			return
		}
		var status int
		fmt.Sscanf(m, "status %d", &status) //nolint:errcheck
		w.WriteHeader(status)
	}))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	for _, c := range []struct {
		mode    string
		sent    int
		blocked bool
	}{
		{"no terminator", 10, false},
		{"in-band error", 10, false},
		{"no visible content", 10, false},
		{"no finish reason", 10, false},
		{"malformed stream", 10, false},
		{"status 500", 10, false},
		{"status 429", 10, false},
		{"status 404", 1, true},
		{"status 403", 1, true},
	} {
		mode.Store(c.mode)
		rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
		if rec.Ceiling.Sent != c.sent || rec.Ceiling.Passed != 0 || rec.Ceiling.Pass || rec.Start.Blocked != c.blocked ||
			rec.Requests[len(rec.Requests)-1].Fail != c.mode {
			t.Errorf("%s: check %+v, start %+v, last request fail %q", c.mode, rec.Ceiling, rec.Start, rec.Requests[len(rec.Requests)-1].Fail)
		}
	}

	var hits atomic.Int64
	one := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 6 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer one.Close()
	cfg = releaseConfig(t, one.URL+"/v1/chat/completions")
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if rec.Ceiling != (CeilingCheck{Ceiling: 512, LengthValue: "length", Sent: 10, Passed: 9}) || rec.Requests[5].Fail != "status 500" {
		t.Errorf("one of ten failed: %+v, request 5 %+v", rec.Ceiling, rec.Requests[5])
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	cfg = releaseConfig(t, "http://"+closed+"/v1/chat/completions")
	rec = releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if rec.Ceiling.Sent != 10 || rec.Requests[0].Fail != "no response" || rec.Requests[0].Exchange.Error == "" || len(rec.RTTNs) != 0 || rec.Start.Blocked {
		t.Errorf("refused dial: %+v, rtt %v, start %+v", rec.Requests[0], rec.RTTNs, rec.Start)
	}
}

// Each request runs to the configured hard timeout, not the pilot
// probe's.
func TestReleaseProbeHardTimeout(t *testing.T) {
	noSpacing(t)
	defer func(d time.Duration) { requestTimeout = d }(requestTimeout)
	requestTimeout = 100 * time.Millisecond
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			time.Sleep(time.Second)
		}
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	cfg.Client.HTTPTimeoutS = 5
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if !rec.Requests[0].Pass || !rec.Ceiling.Pass {
		t.Errorf("a 1 s answer under a 5 s hard timeout: %+v", rec.Requests[0])
	}
}

// Requests go one at a time, no sooner than the spacing after the last
// dispatch; the connect durations are the RTT record.
func TestReleaseProbeSpacingAndRTT(t *testing.T) {
	d := releaseSpacing
	releaseSpacing = 300 * time.Millisecond
	defer func() { releaseSpacing = d }()
	var hits, inFlight, most atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if n := inFlight.Add(1); n > most.Load() {
			most.Store(n)
		}
		defer inFlight.Add(-1)
		w.Header().Set("Connection", "close")
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if hits.Load() != 10 || most.Load() != 1 {
		t.Fatalf("%d requests, at most %d in flight", hits.Load(), most.Load())
	}
	for i := 1; i < len(rec.Requests); i++ {
		if gap := rec.Requests[i].Exchange.DispatchUTC.Sub(rec.Requests[i-1].Exchange.DispatchUTC); gap < releaseSpacing {
			t.Fatalf("requests %d and %d dispatched %s apart, want at least %s", i-1, i, gap, releaseSpacing)
		}
	}
	if len(rec.RTTNs) != 10 {
		t.Fatalf("rtt %v", rec.RTTNs)
	}
	for i, ns := range rec.RTTNs {
		if ns <= 0 || ns != rec.Requests[i].Exchange.Connects[0].DurationNs {
			t.Fatalf("rtt %d: %d, connect %+v", i, ns, rec.Requests[i].Exchange.Connects)
		}
	}
}

// The limits read is the route's limits record when it returns 200;
// otherwise the rate-limit headers of the probe's own requests are.
func TestReleaseProbeLimits(t *testing.T) {
	noSpacing(t)
	var limitsStatus atomic.Int64
	var mu sync.Mutex
	var auth, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			auth, path = r.Header.Get("Authorization"), r.URL.Path
			mu.Unlock()
			w.WriteHeader(int(limitsStatus.Load()))
			io.WriteString(w, `{"rate_limits":{"rpm":120}}`) //nolint:errcheck
			return
		}
		w.Header().Set("X-Ratelimit-Limit-Requests", "120")
		w.Header().Set("X-Ratelimit-Remaining-Requests", "119")
		w.Header().Set("X-Other", "1")
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	cfg.Target.ResponseHeaders = []string{"X-Ratelimit-*", "X-Other"}
	cfg.Target.Probe = &config.TargetProbe{LimitsURL: srv.URL + "/v1/model_apis/gpt-oss-120b", LimitsAuthScheme: "Bearer"}

	limitsStatus.Store(200)
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	mu.Lock()
	if auth != "Bearer "+testKey || path != "/v1/model_apis/gpt-oss-120b" {
		t.Errorf("limits read: Authorization %q path %q", auth, path)
	}
	mu.Unlock()
	if rec.LimitsFrom != LimitsFromRead || rec.Limits == nil || rec.Limits.Status != 200 || !strings.Contains(*rec.Limits.Body.Text, `"rpm":120`) {
		t.Errorf("limits from %q, read %+v", rec.LimitsFrom, rec.Limits)
	}
	if fmt.Sprint(rec.RateLimitHeaders) != "map[X-Ratelimit-Limit-Requests:[120] X-Ratelimit-Remaining-Requests:[119]]" {
		t.Errorf("rate-limit headers %v", rec.RateLimitHeaders)
	}

	limitsStatus.Store(403)
	if rec = releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}); rec.LimitsFrom != LimitsFromHeaders || rec.Limits.Status != 403 || rec.Start.Blocked {
		t.Errorf("limits refused: from %q, read status %d, start %+v", rec.LimitsFrom, rec.Limits.Status, rec.Start)
	}

	cfg.Target.ResponseHeaders = []string{"X-Other"}
	if rec = releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}); rec.LimitsFrom != "" || rec.RateLimitHeaders != nil {
		t.Errorf("neither: from %q, headers %v", rec.LimitsFrom, rec.RateLimitHeaders)
	}
}

// The values a route withholds stay out of the record: each value of a
// header target.retained_headers names is replaced by the stand-in in
// every request, the rate-limit headers and the limits read, and under
// target.probe.limits_retained the limits body is replaced by its hash.
// The retained file holds the values, readable by the owner only. A
// record carrying them is exposed; a route withholding nothing writes no
// retained file.
func TestReleaseProbeRetains(t *testing.T) {
	noSpacing(t)
	const limitsBody = `{"rate_limits":{"rpm":"qz104729"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Ratelimit-Limit-Requests", "120")
		w.Header().Set("X-Ratelimit-Remaining-Requests", "qz7919")
		if r.Method == http.MethodGet {
			io.WriteString(w, limitsBody) //nolint:errcheck
			return
		}
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	cfg.Target.ResponseHeaders = []string{"Retry-After", "X-Ratelimit-*"}
	cfg.Target.RetainedHeaders = []string{"x-ratelimit-remaining-*"}
	cfg.Target.Probe = &config.TargetProbe{LimitsURL: srv.URL + "/v1/model_apis/gpt-oss-120b", LimitsAuthScheme: "Bearer", LimitsRetained: true}

	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	const published = "map[X-Ratelimit-Limit-Requests:[120] X-Ratelimit-Remaining-Requests:[[retained]]]"
	for i, r := range rec.Requests {
		if fmt.Sprint(r.Exchange.Headers) != published {
			t.Fatalf("request %d headers %v", i, r.Exchange.Headers)
		}
	}
	if fmt.Sprint(rec.RateLimitHeaders) != published || rec.Limits == nil || fmt.Sprint(rec.Limits.Headers) != published {
		t.Fatalf("rate-limit headers %v, limits read %+v", rec.RateLimitHeaders, rec.Limits)
	}
	sum := sha256.Sum256([]byte(limitsBody))
	if b := rec.Limits.Body; b == nil || b.Text != nil || b.Base64 != "" || !b.Retained || b.SHA256 != hex.EncodeToString(sum[:]) || rec.LimitsFrom != LimitsFromRead {
		t.Fatalf("limits body %+v, limits from %q", b, rec.LimitsFrom)
	}
	if e := Exposed(&cfg.Target, rec); e != nil {
		t.Fatalf("exposed %q", e)
	}
	path, err := Write(t.TempDir(), rec)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(path); err != nil || bytes.Contains(b, []byte("qz7919")) || bytes.Contains(b, []byte("qz104729")) {
		t.Fatalf("%s carries a withheld value (%v):\n%s", File, err, b)
	}

	held, err := WriteRetained(filepath.Join(t.TempDir(), "frames"), rec)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(held); err != nil || fi.Mode().Perm() != 0o600 || filepath.Base(held) != RetainedFile {
		t.Fatalf("retained file %s: %v, %v", held, fi, err)
	}
	raw, err := os.ReadFile(held)
	if err != nil {
		t.Fatal(err)
	}
	var back RetainedRecord
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	const kept = "map[X-Ratelimit-Remaining-Requests:[qz7919]]"
	if back.Target != "groq" || len(back.Exchanges) != CeilingRequests+1 {
		t.Fatalf("retained record %s", raw)
	}
	for i := 0; i < CeilingRequests; i++ {
		if e := back.Exchanges[fmt.Sprintf("requests.%d", i)]; e == nil || fmt.Sprint(e.Headers) != kept || e.Body != nil {
			t.Fatalf("retained request %d: %+v", i, e)
		}
	}
	if l := back.Exchanges["limits"]; l == nil || fmt.Sprint(l.Headers) != kept || l.Body == nil || l.Body.Text == nil || *l.Body.Text != limitsBody {
		t.Fatalf("retained limits read %+v", l)
	}
	if _, err := WriteRetained(filepath.Dir(held), rec); err == nil {
		t.Fatal("an existing retained file was overwritten")
	}

	exposed := *rec
	exposed.Requests = append([]CheckRequest(nil), rec.Requests...)
	exposed.Requests[2].Exchange.Headers = map[string][]string{"X-Ratelimit-Remaining-Requests": {"qz7919"}}
	limits := *rec.Limits
	limits.Body = rec.Limits.Retained.Body
	exposed.Limits = &limits
	if e := Exposed(&cfg.Target, &exposed); fmt.Sprint(e) != "[X-Ratelimit-Remaining-Requests limits body]" {
		t.Fatalf("exposed %q", e)
	}

	cfg.Target.RetainedHeaders, cfg.Target.Probe.LimitsRetained = nil, false
	rec = releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if fmt.Sprint(rec.RateLimitHeaders) != "map[X-Ratelimit-Limit-Requests:[120] X-Ratelimit-Remaining-Requests:[qz7919]]" || *rec.Limits.Body.Text != limitsBody {
		t.Fatalf("withholding nothing: headers %v, limits body %+v", rec.RateLimitHeaders, rec.Limits.Body)
	}
	if p, err := WriteRetained(t.TempDir(), rec); p != "" || err != nil {
		t.Fatalf("withholding nothing wrote %q, %v", p, err)
	}
}

// Each limit record is carried; a file source's retained copy is hashed
// and a mismatch or a missing copy is a finding.
func TestReleaseProbeSnapshots(t *testing.T) {
	noSpacing(t)
	srv := httptest.NewServer(sseHandler(answerFrame, stopFrame, doneFrame))
	defer srv.Close()
	dir := t.TempDir()
	shot := []byte("console limits screenshot")
	if err := os.WriteFile(filepath.Join(dir, "limits.png"), shot, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(shot)
	good := hex.EncodeToString(sum[:])
	bad := strings.Repeat("0", 64)
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	lim := func(src, sha string) config.Limit {
		return config.Limit{Dimension: config.LimitRPM, Value: 1000, Marking: config.MarkingPublic, Source: src, Retrieved: "2026-10-07", SHA256: sha}
	}
	cfg.Target.Limits = []config.Limit{lim("limits.png", good), lim("limits.png", bad), lim("missing.png", good),
		lim("https://console.example.test/limits", good), lim("../limits.png", good)}

	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap, SnapshotDir: dir})
	got := make([]string, len(rec.Snapshots))
	for i, s := range rec.Snapshots {
		got[i] = s.RetainedError
		if s.Retained != nil {
			got[i] = fmt.Sprint(*s.Retained)
		}
		if s.Limit != cfg.Target.Limits[i] {
			t.Errorf("snapshot %d carries %+v", i, s.Limit)
		}
	}
	want := `[true false no such file  the source is not a file name inside the snapshot directory]`
	if fmt.Sprint(got) != want {
		t.Errorf("snapshots %q, want %s", got, want)
	}
	if fmt.Sprint(rec.Findings) != `[no usage object in the stream retained copy "limits.png" does not match its recorded sha256 `+
		`retained copy "missing.png" not read: no such file `+
		`retained copy "../limits.png" not read: the source is not a file name inside the snapshot directory]` {
		t.Errorf("findings %q", rec.Findings)
	}

	rec = releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	for i, s := range rec.Snapshots {
		if s.Retained != nil || s.RetainedError != "" {
			t.Errorf("no directory: snapshot %d %+v", i, s)
		}
	}
	if fmt.Sprint(rec.Findings) != "[no usage object in the stream]" {
		t.Errorf("no directory: findings %q", rec.Findings)
	}
}

// A confidential limit's snapshot carries its dimension, source and hash
// and the retained copy's hash check, and no value; a snapshot that
// carries one is exposed.
func TestReleaseProbeConfidentialSnapshot(t *testing.T) {
	noSpacing(t)
	srv := httptest.NewServer(sseHandler(answerFrame, stopFrame, doneFrame))
	defer srv.Close()
	dir := t.TempDir()
	shot := []byte("console limits screenshot")
	if err := os.WriteFile(filepath.Join(dir, "console.png"), shot, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(shot)
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	tpm := config.Limit{Dimension: config.LimitTPM, Value: 9876543.25, Marking: config.MarkingConfidential, Counting: config.CountingInputOutput,
		Source: "console.png", Retrieved: "2026-10-07", SHA256: hex.EncodeToString(sum[:])}
	cfg.Target.Limits = []config.Limit{cfg.Target.Limits[0], tpm}

	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap, SnapshotDir: dir})
	want := tpm
	want.Value = 0
	if s := rec.Snapshots[1]; s.Limit != want || s.Retained == nil || !*s.Retained {
		t.Fatalf("confidential snapshot %+v", s)
	}
	if s := rec.Snapshots[0]; s.Limit != cfg.Target.Limits[0] {
		t.Errorf("public snapshot %+v", s)
	}
	path, err := Write(t.TempDir(), rec)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "9876543") || strings.Count(string(b), `"value": `) != 1 || !strings.Contains(string(b), `"marking": "confidential"`) {
		t.Errorf("%s carries the value or no marking:\n%s", File, b)
	}
	if e := Exposed(&cfg.Target, rec); e != nil {
		t.Errorf("exposed %q", e)
	}

	exposed := *rec
	exposed.Snapshots = append([]Snapshot(nil), rec.Snapshots...)
	exposed.Snapshots[1].Value = 9876543.25
	if e := Exposed(&cfg.Target, &exposed); fmt.Sprint(e) != "[snapshot 1 value]" {
		t.Errorf("value restored: exposed %q", e)
	}
	public := cfg.Target
	public.Limits = []config.Limit{cfg.Target.Limits[0]}
	if e := Exposed(&public, &exposed); fmt.Sprint(e) != "[snapshot 1 value]" {
		t.Errorf("a snapshot marked confidential beyond target.limits: exposed %q", e)
	}
}

// A refusal field is recorded at its path; unconfigured, it is a finding.
func TestReleaseProbeRefusalField(t *testing.T) {
	noSpacing(t)
	srv := httptest.NewServer(sseHandler(answerFrame,
		"data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"refusal\":\"No.\"}}]}\n\n",
		"data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"refusal\":null}}]}\n\n",
		stopFrame, doneFrame))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if fmt.Sprint(rec.Requests[0].Shape.Refusals) != "[choices.0.delta.refusal]" {
		t.Fatalf("refusals %q", rec.Requests[0].Shape.Refusals)
	}
	if fmt.Sprint(rec.Findings) != `[delta key "refusal" carries text and is neither the content key nor a configured reasoning key `+
		`no usage object in the stream refusal field at "choices.0.delta.refusal", not a configured refusal path]` {
		t.Errorf("findings %q", rec.Findings)
	}
	cfg.Target.ResponseShape = &config.ResponseShape{Refusal: []string{"choices.0.delta.refusal"}}
	if rec = releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}); fmt.Sprint(rec.Findings) != "[no usage object in the stream]" {
		t.Errorf("configured refusal path: findings %q", rec.Findings)
	}
}

// A route echoing the key into every part of its responses leaves no run
// of it in the written record.
func TestReleaseProbeNeverRecordsTheKey(t *testing.T) {
	noSpacing(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")
		if r.Method == http.MethodGet {
			key = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			fmt.Fprintf(w, `{"key":%q}`, key)
			return
		}
		w.Header().Set("Anthropic-Ratelimit-"+key, key)
		w.Header().Set("Anthropic-Ratelimit-Requests-Remaining", key[2:14])
		ev := func(v string) string { return "event: x\ndata: " + v + "\n\n" }
		sseHandler(
			ev(fmt.Sprintf(`{"type":"message_start","message":{"model":%q,"usage":{"input_tokens":3,%q:1}}}`, key, key)),
			ev(fmt.Sprintf(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%q}}`, key)),
			ev(fmt.Sprintf(`{"type":%q,"refusal":%q}`, key, key)),
			ev(fmt.Sprintf(`{"type":"message_delta","delta":{"stop_reason":%q},"usage":{"output_tokens":1}}`, key)),
			ev(`{"type":"error","error":{"message":"`+key+`"}}`),
			ev(`{"type":"message_stop"}`))(w, r)
	}))
	defer srv.Close()
	cfg := messagesConfig(releaseConfig(t, srv.URL+"/v1/messages"), 512)
	cfg.Target.ResponseHeaders = []string{"Anthropic-Ratelimit-*"}
	cfg.Target.Probe = &config.TargetProbe{LimitsURL: srv.URL + "/v1/limits", LimitsAuthScheme: "Bearer"}
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
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
	masked := strings.Repeat("*", len(testKey))
	s := rec.Requests[0].Shape
	if s.Models[0] != masked || s.EventTypes[masked] != 1 || rec.Requests[0].FinishReason != masked || len(s.Refusals) != 1 ||
		!strings.Contains(s.Error, masked) || rec.Limits.Status != 200 || rec.RateLimitHeaders != nil {
		t.Fatalf("shape %+v, finish %q, limits %+v, headers %v", s, rec.Requests[0].FinishReason, rec.Limits, rec.RateLimitHeaders)
	}
	var back ReleaseRecord
	if err := json.Unmarshal(raw, &back); err != nil || back.Ceiling != rec.Ceiling || len(back.Requests) != 10 {
		t.Errorf("round trip: %v, %+v", err, back.Ceiling)
	}
}

// Nothing is sent without the key or the byte cap, under another
// profile, or with a text count other than CeilingRequests.
func TestReleaseProbeRefusesItsInput(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	pilot, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for name, in := range map[string]ReleaseInput{
		"no config":     {Texts: texts(0), ByteCap: byteCap},
		"pilot profile": {Config: pilot, Texts: texts(0), ByteCap: byteCap},
		"nine texts":    {Config: cfg, Texts: texts(0)[:9], ByteCap: byteCap},
		"eleven texts":  {Config: cfg, Texts: append(texts(0), "x"), ByteCap: byteCap},
		"no byte cap":   {Config: cfg, Texts: texts(0)},
	} {
		if rec, err := RunRelease(context.Background(), in); err == nil || rec != nil {
			t.Errorf("%s: %v, %v", name, rec, err)
		}
	}
	t.Setenv(keyEnv, "")
	if rec, err := RunRelease(context.Background(), ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}); err == nil || rec != nil || !strings.Contains(err.Error(), keyEnv) {
		t.Errorf("empty key: %v, %v", rec, err)
	}
	if hits.Load() != 0 {
		t.Errorf("%d requests sent", hits.Load())
	}
}

// checked is route's record at ceiling with passed of the ten requests
// passing the ceiling check and every one the input bound.
func checked(route string, ceiling, passed int) *ReleaseRecord {
	return &ReleaseRecord{Route: route, Ceiling: CeilingCheck{Ceiling: ceiling, Sent: 10, Passed: passed, Pass: passed == 10},
		InputBound: InputCheck{BoundTokens: 1200, Reported: 10, MaxPromptTokens: 1100, Pass: true}}
}

// The model runs at 512 when every host passes there, at 1,024 when any
// failed at 512 and every host passes at 1,024, and is excluded when any
// host fails at 1,024. A record of no host, one whose input bound failed
// and a host with no record refuse the choice.
func TestChooseCeiling(t *testing.T) {
	blocked := checked("bedrock", 512, 0)
	blocked.Start = Start{Blocked: true, Reason: "probe request returned 401"}
	over := checked("baseten", 512, 10)
	over.InputBound = InputCheck{BoundTokens: 1200, Reported: 10, MaxPromptTokens: 1205, Exceeded: 1}
	unreported := checked("baseten", 1024, 10)
	unreported.InputBound = InputCheck{BoundTokens: 1200, Reported: 9, MaxPromptTokens: 1100}
	gpt, claude := []string{"groq", "baseten"}, []string{"anthropic", "bedrock"}
	for _, c := range []struct {
		name  string
		hosts []string
		recs  []*ReleaseRecord
		want  CeilingChoice
		err   string
	}{
		{"all pass at 512", gpt, []*ReleaseRecord{checked("groq", 512, 10), checked("baseten", 512, 10)},
			CeilingChoice{Ceiling: 512, Reason: "every host passed at 512"}, ""},
		{"one host", []string{"groq"}, []*ReleaseRecord{checked("groq", 512, 10)},
			CeilingChoice{Ceiling: 512, Reason: "every host passed at 512"}, ""},
		{"512 passes, a 1024 record is not needed", gpt, []*ReleaseRecord{checked("groq", 512, 10), checked("baseten", 512, 10), checked("groq", 1024, 0)},
			CeilingChoice{Ceiling: 512, Reason: "every host passed at 512"}, ""},
		{"one fails at 512, all pass at 1024", gpt, []*ReleaseRecord{checked("groq", 512, 10), checked("baseten", 512, 9), checked("groq", 1024, 10), checked("baseten", 1024, 10)},
			CeilingChoice{Ceiling: 1024, Reason: "baseten failed at 512; every host passed at 1024"}, ""},
		{"one fails at 1024", gpt, []*ReleaseRecord{checked("groq", 512, 0), checked("baseten", 512, 10), checked("groq", 1024, 10), checked("baseten", 1024, 9)},
			CeilingChoice{Excluded: true, Reason: "baseten failed at 1024"}, ""},
		{"a host passing at 512 still needs 1024", gpt, []*ReleaseRecord{checked("groq", 512, 0), checked("baseten", 512, 10), checked("groq", 1024, 10)},
			CeilingChoice{}, "probe: route baseten has no check at 1024; groq failed at 512"},
		{"a host without 512", gpt, []*ReleaseRecord{checked("groq", 512, 10), checked("baseten", 1024, 10)},
			CeilingChoice{}, "probe: route baseten has no check at 512"},
		{"a host without records", gpt, []*ReleaseRecord{checked("groq", 512, 10)},
			CeilingChoice{}, "probe: route baseten has no check at 512"},
		{"a record of no host", []string{"groq"}, []*ReleaseRecord{checked("groq", 512, 10), checked("baseten", 512, 0)},
			CeilingChoice{}, "probe: route baseten is not a host of the model (groq)"},
		{"input bound exceeded", gpt, []*ReleaseRecord{checked("groq", 512, 10), over},
			CeilingChoice{}, "probe: route baseten: the input bound failed at ceiling 512: 10 of 10 requests reported, 1 above 1200 tokens"},
		{"input bound unreported", gpt, []*ReleaseRecord{checked("groq", 512, 0), checked("baseten", 512, 10), checked("groq", 1024, 10), unreported},
			CeilingChoice{}, "probe: route baseten: the input bound failed at ceiling 1024: 9 of 10 requests reported, 0 above 1200 tokens"},
		{"blocked", claude, []*ReleaseRecord{checked("anthropic", 512, 10), blocked},
			CeilingChoice{}, "probe: route bedrock: the probe was blocked (probe request returned 401); it is not a ceiling check"},
		{"two at one ceiling", gpt, []*ReleaseRecord{checked("groq", 512, 10), checked("groq", 512, 0)},
			CeilingChoice{}, "probe: route groq: two records at ceiling 512"},
		{"other ceiling", gpt, []*ReleaseRecord{checked("groq", 256, 10)},
			CeilingChoice{}, "probe: route groq: ceiling 256 is not one of [512 1024]"},
		{"no records", gpt, nil, CeilingChoice{}, "probe: route baseten has no check at 512"},
		{"no hosts", nil, []*ReleaseRecord{checked("groq", 512, 10)}, CeilingChoice{}, "probe: no hosts"},
	} {
		got, err := ChooseCeiling(c.hosts, c.recs)
		if got != c.want || fmt.Sprint(err) != map[bool]string{true: "<nil>", false: c.err}[c.err == ""] {
			t.Errorf("%s: got %+v, %v; want %+v, %q", c.name, got, err, c.want, c.err)
		}
	}
}

// The route's own ceiling-check records keep the provider-model out when
// a response reported a model id other than the expected one, or when
// target.reported_model is recorded and no response reported a model.
func TestModelFault(t *testing.T) {
	noSpacing(t)
	addr := startShaped(t, mock.Shape{Answer: "The customer was billed twice."})
	cfg := releaseConfig(t, "http://"+addr+"/v1/chat/completions")
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	tg := &cfg.Target
	if got := rec.ReportedModels(); len(got) != 1 || got[0] != "openai/gpt-oss-120b" {
		t.Fatalf("reported %q", got)
	}
	if f := ModelFault(tg, []*ReleaseRecord{rec}); f != "" {
		t.Fatalf("requested model reported: %s", f)
	}
	tg.ReportedModel = &config.ReportedModel{Name: "openai/gpt-oss-120b-2026"}
	if f := ModelFault(tg, []*ReleaseRecord{rec}); f != `the ceiling check reported model "openai/gpt-oss-120b", not target.reported_model "openai/gpt-oss-120b-2026"` {
		t.Errorf("recorded another model: %s", f)
	}
	tg.ReportedModel = nil
	tg.ModelName = "openai/gpt-oss-120b-2026"
	if f := ModelFault(tg, []*ReleaseRecord{rec}); f != `the ceiling check reported model "openai/gpt-oss-120b", the requested "openai/gpt-oss-120b-2026", and target.reported_model records no documentation naming both as one build: identifier-not-documented` {
		t.Errorf("requested another model: %s", f)
	}
	tg.ModelName, tg.ReportedModel = "m", &config.ReportedModel{Name: "openai/gpt-oss-120b"}
	if f := ModelFault(tg, []*ReleaseRecord{rec}); f != "" {
		t.Errorf("recorded model reported: %s", f)
	}

	// Every record counts, in order; a record with no reported model
	// faults only a recorded reported_model.
	silent := &ReleaseRecord{Requests: []CheckRequest{{Shape: &Shape{}}, {}}}
	other := &ReleaseRecord{Requests: []CheckRequest{{Shape: &Shape{Models: []string{"m", "m-v2"}}}}}
	if got := (&ReleaseRecord{Requests: append(other.Requests, rec.Requests...)}).ReportedModels(); fmt.Sprint(got) != "[m m-v2 openai/gpt-oss-120b]" {
		t.Errorf("reported %q", got)
	}
	tg.ModelName, tg.ReportedModel = "m", nil
	if f := ModelFault(tg, []*ReleaseRecord{silent}); f != "" {
		t.Errorf("no model reported: %s", f)
	}
	if f := ModelFault(tg, []*ReleaseRecord{silent, other}); !strings.Contains(f, `reported model "m-v2", the requested "m"`) {
		t.Errorf("second record: %s", f)
	}
	tg.ReportedModel = &config.ReportedModel{Name: "m-2026"}
	if f := ModelFault(tg, []*ReleaseRecord{silent}); f != `target.reported_model "m-2026" is recorded and no response of the ceiling check reported a model` {
		t.Errorf("recorded model, none reported: %s", f)
	}
}
