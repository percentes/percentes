package loadgen

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

// shapeGen is a gen for one target: hosted or self-hosted, with the given
// response shape, holding testKey when hosted.
func shapeGen(baseURL string, client *http.Client, hosted bool, sh *config.ResponseShape) *gen {
	cfg := &config.Config{}
	cfg.Run.Seed = 1
	cfg.Load.MaxTokens = 256
	cfg.Target.BaseURL = baseURL
	cfg.Target.Hosted = hosted
	cfg.Target.ResponseShape = sh
	g := &gen{cfg: cfg, client: client, epoch: time.Now(), filler: "xyz", model: "m"}
	if hosted {
		g.apiKey = testKey
	}
	return g
}

func executeShaped(t *testing.T, hosted bool, sh *config.ResponseShape, frames ...string) *Request {
	t.Helper()
	srv := sseServer(t, frames...)
	defer srv.Close()
	r := &Request{Index: 1}
	shapeGen(srv.URL, srv.Client(), hosted, sh).execute(r)
	return r
}

const (
	answerFrame = "data: {\"choices\":[{\"delta\":{\"content\":\"{\\\"route\\\":\\\"billing\\\"}\"}}]}\n\n"
	stopFrame   = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	doneFrame   = "data: [DONE]\n\n"
)

// One event may span several data lines; every field it carries is kept,
// and reasoning stays apart from the visible answer.
func TestMultilineEventsKeepEveryField(t *testing.T) {
	sh := &config.ResponseShape{Reasoning: []string{"reasoning"}, Usage: []string{"x_groq.usage"}}
	r := executeShaped(t, true, sh,
		"data: {\"model\":\"openai/gpt-oss-120b\",\ndata: \"choices\":[{\"delta\":{\"reasoning\":\"think \"},\ndata: \"finish_reason\":null}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"reasoning\":\"more\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":\ndata: {\"content\":\"{\\\"route\\\":\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"\\\"billing\\\"}\"}}]}\n\n",
		stopFrame,
		"data: {\"choices\":[],\ndata: \"x_groq\":{\"usage\":{\"prompt_tokens\":5,\ndata: \"completion_tokens\":9}}}\n\n",
		doneFrame)
	if r.Outcome != OutcomeCompleted || r.Tokens != 2 {
		t.Fatalf("got %v/%q tokens=%d, want completed with two tokens", r.Outcome, r.ErrClass, r.Tokens)
	}
	if r.Content != `{"route":"billing"}` || r.Reasoning != "think more" || r.ReasoningChunks != 2 {
		t.Fatalf("content %q reasoning %q chunks %d", r.Content, r.Reasoning, r.ReasoningChunks)
	}
	if r.FirstReasoningNs == 0 || r.FirstReasoningNs > r.FirstTokNs {
		t.Fatalf("first reasoning %d, first token %d", r.FirstReasoningNs, r.FirstTokNs)
	}
	if r.FinishReason != "stop" || r.ReportedModel != "openai/gpt-oss-120b" || !r.UsageSeen || r.CompletionTokens != 9 {
		t.Fatalf("finish %q model %q usage %v/%d", r.FinishReason, r.ReportedModel, r.UsageSeen, r.CompletionTokens)
	}
	b, _ := json.Marshal(r)
	if !strings.Contains(string(b), `"usage":{"prompt_tokens":5,"completion_tokens":9}`) {
		t.Fatalf("usage not kept compact in JSON: %s", b)
	}
	if strings.Contains(string(b), "billing") || strings.Contains(string(b), "think") {
		t.Fatalf("answer or reasoning text in JSON: %s", b)
	}
}

// A usage-only chunk is read at the paths the shape names, and only there.
func TestUsageOnlyChunkAtConfiguredPath(t *testing.T) {
	for _, c := range []struct {
		name  string
		usage []string
		frame string
		seen  bool
	}{
		{"top level, default", nil, `{"choices":[],"usage":{"completion_tokens":4}}`, true},
		{"vendor object", []string{"x_groq.usage"}, `{"choices":[],"x_groq":{"usage":{"completion_tokens":4}}}`, true},
		{"vendor object, unconfigured", nil, `{"choices":[],"x_groq":{"usage":{"completion_tokens":4}}}`, false},
		{"inside the choice", []string{"usage", "choices.0.usage"}, `{"choices":[{"delta":{},"usage":{"completion_tokens":4}}]}`, true},
		{"null", nil, `{"choices":[],"usage":null}`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := executeShaped(t, true, &config.ResponseShape{Usage: c.usage}, answerFrame, stopFrame, "data: "+c.frame+"\n\n", doneFrame)
			if r.Outcome != OutcomeCompleted || r.Tokens != 1 {
				t.Fatalf("got %v/%q tokens=%d", r.Outcome, r.ErrClass, r.Tokens)
			}
			if r.UsageSeen != c.seen || (c.seen && r.CompletionTokens != 4) || (r.Usage != nil) != c.seen {
				t.Fatalf("usage seen %v tokens %d raw %s, want seen %v", r.UsageSeen, r.CompletionTokens, r.Usage, c.seen)
			}
			if r.UnknownEvents != 0 {
				t.Fatalf("%d unknown events", r.UnknownEvents)
			}
		})
	}
}

// A stream that spends its ceiling on reasoning has no visible answer: an
// empty stream whose finish reason and reasoning are kept.
func TestReasoningOnlyStream(t *testing.T) {
	frames := []string{
		"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"r1 \"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"r2 \"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n",
		doneFrame,
	}
	r := executeShaped(t, true, &config.ResponseShape{Reasoning: []string{"reasoning_content"}}, frames...)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrEmptyStream {
		t.Fatalf("got %v/%q, want errored/%s", r.Outcome, r.ErrClass, ErrEmptyStream)
	}
	if r.Tokens != 0 || r.FirstTokNs != 0 || r.Content != "" {
		t.Fatalf("reasoning counted as content: tokens=%d first=%d content=%q", r.Tokens, r.FirstTokNs, r.Content)
	}
	if r.ReasoningChunks != 2 || r.Reasoning != "r1 r2 " || r.FirstReasoningNs == 0 || r.FinishReason != "length" {
		t.Fatalf("chunks %d reasoning %q first %d finish %q", r.ReasoningChunks, r.Reasoning, r.FirstReasoningNs, r.FinishReason)
	}

	r = executeShaped(t, false, nil, frames...)
	if r.ErrClass != ErrEmptyStream || r.ReasoningChunks != 0 || r.Reasoning != "" {
		t.Fatalf("unconfigured reasoning key read: %q chunks=%d reasoning=%q", r.ErrClass, r.ReasoningChunks, r.Reasoning)
	}
}

// Content, then an in-band error, then [DONE]: a hosted request is not
// completed; a self-hosted one keeps its §3 class. Both keep the error
// object with the bearer token masked.
func TestContentThenErrorThenDone(t *testing.T) {
	errFrame := "data: {\"error\": {\"message\": \"upstream failed for " + testKey + "\",\ndata: \"type\": \"server_error\"}}\n\n"
	masked := `{"message":"upstream failed for ` + strings.Repeat("*", len(testKey)) + `","type":"server_error"}`

	r := executeShaped(t, true, nil, answerFrame, answerFrame, errFrame, doneFrame)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
		t.Fatalf("hosted: got %v/%q, want errored/%s", r.Outcome, r.ErrClass, ErrMalformedStream)
	}
	if r.Tokens != 2 || r.StreamError != masked {
		t.Fatalf("hosted: tokens=%d stream error %q, want %q", r.Tokens, r.StreamError, masked)
	}

	srv := sseServer(t, answerFrame, answerFrame, errFrame, doneFrame)
	defer srv.Close()
	g := shapeGen(srv.URL, srv.Client(), false, nil)
	g.apiKey = testKey
	r = &Request{Index: 1}
	g.execute(r)
	if r.Outcome != OutcomeCompleted || r.Tokens != 2 || r.StreamError != masked {
		t.Fatalf("self-hosted: got %v/%q tokens=%d stream error %q", r.Outcome, r.ErrClass, r.Tokens, r.StreamError)
	}
}

// An explicit error object with no content: a hosted request is a
// malformed stream with the object kept, at any path the shape names.
func TestExplicitErrorEvent(t *testing.T) {
	r := executeShaped(t, true, nil, "data: {\"error\":{\"message\":\"rate limited\",\"code\":429}}\n\n", doneFrame)
	if r.ErrClass != ErrMalformedStream || r.StreamError != `{"message":"rate limited","code":429}` {
		t.Fatalf("hosted: %v/%q stream error %q", r.Outcome, r.ErrClass, r.StreamError)
	}
	r = executeShaped(t, false, nil, "data: {\"error\":{\"message\":\"rate limited\"}}\n\n", doneFrame)
	if r.ErrClass != ErrEmptyStream || r.StreamError == "" {
		t.Fatalf("self-hosted: %v/%q stream error %q", r.Outcome, r.ErrClass, r.StreamError)
	}

	vendor := "data: {\"choices\":[],\"x_groq\":{\"error\":\"overloaded\"}}\n\n"
	r = executeShaped(t, true, &config.ResponseShape{Error: []string{"error", "x_groq.error"}}, answerFrame, vendor, doneFrame)
	if r.ErrClass != ErrMalformedStream || r.StreamError != `"overloaded"` {
		t.Fatalf("vendor path: %v/%q stream error %q", r.Outcome, r.ErrClass, r.StreamError)
	}
	r = executeShaped(t, true, nil, answerFrame, vendor, doneFrame)
	if r.Outcome != OutcomeCompleted || r.StreamError != "" {
		t.Fatalf("unconfigured path: %v/%q stream error %q", r.Outcome, r.ErrClass, r.StreamError)
	}

	r = executeShaped(t, true, nil, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}],\"error\":null}\n\n", doneFrame)
	if r.Outcome != OutcomeCompleted || r.StreamError != "" {
		t.Fatalf("null error: %v/%q stream error %q", r.Outcome, r.ErrClass, r.StreamError)
	}
}

// Refusal text is read only at the paths the shape names, joined across
// chunks and counted in bytes; the text stays out of JSON and is kept for
// hosted targets only; two paths in one chunk join in path order. A
// refusal of the wrong type is a malformed stream.
func TestRefusalFieldAtConfiguredPath(t *testing.T) {
	delta := &config.ResponseShape{Refusal: []string{"choices.0.delta.refusal"}}
	refusal := func(s string) string {
		return "data: {\"choices\":[{\"delta\":{\"content\":null,\"refusal\":" + s + "}}]}\n\n"
	}
	frames := []string{refusal(`"I can\u2019t "`), refusal(`null`), refusal(`"help."`), stopFrame, doneFrame}
	const text, n = "I can\u2019t help.", 15

	r := executeShaped(t, true, delta, frames...)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrEmptyStream || r.Tokens != 0 {
		t.Fatalf("got %v/%q tokens=%d, want errored/%s", r.Outcome, r.ErrClass, r.Tokens, ErrEmptyStream)
	}
	if r.Refusal != text || r.RefusalBytes != n || r.UnknownEvents != 0 || r.FinishReason != "stop" {
		t.Fatalf("refusal %q bytes %d unknown %d finish %q, want %q and %d bytes", r.Refusal, r.RefusalBytes, r.UnknownEvents, r.FinishReason, text, n)
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "help") || !strings.Contains(string(b), `"refusal_bytes":15`) {
		t.Fatalf("refusal in JSON: %s", b)
	}

	if r := executeShaped(t, false, delta, frames...); r.Refusal != "" || r.RefusalBytes != n {
		t.Fatalf("self-hosted: refusal %q bytes %d, want no text and %d bytes", r.Refusal, r.RefusalBytes, n)
	}
	if r := executeShaped(t, true, nil, frames...); r.Refusal != "" || r.RefusalBytes != 0 {
		t.Fatalf("unconfigured path: refusal %q bytes %d", r.Refusal, r.RefusalBytes)
	}

	vendor := &config.ResponseShape{Refusal: []string{"choices.0.delta.refusal", "x_refusal.text"}}
	r = executeShaped(t, true, vendor, answerFrame, "data: {\"x_refusal\":{\"text\":\"no\"}}\n\n",
		"data: {\"choices\":[{\"delta\":{\"refusal\":\" and \"}}],\"x_refusal\":{\"text\":\"no\"}}\n\n", stopFrame, doneFrame)
	if r.Outcome != OutcomeCompleted || r.Refusal != "no and no" || r.RefusalBytes != 9 || r.UnknownEvents != 0 {
		t.Fatalf("vendor path: %v/%q refusal %q bytes %d unknown %d", r.Outcome, r.ErrClass, r.Refusal, r.RefusalBytes, r.UnknownEvents)
	}

	r = executeShaped(t, true, delta, answerFrame, refusal(`{"text":"no"}`), doneFrame)
	if r.ErrClass != ErrMalformedStream || r.RefusalBytes != 0 {
		t.Fatalf("refusal object: %v/%q bytes %d, want %s", r.Outcome, r.ErrClass, r.RefusalBytes, ErrMalformedStream)
	}
}

// Events of another schema are counted and carry no token; extra keys on
// a known chunk are ignored; a configured field of the wrong type is a
// malformed stream.
func TestUnknownSchema(t *testing.T) {
	other := "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n"
	r := executeShaped(t, true, nil, other, other, doneFrame)
	if r.ErrClass != ErrEmptyStream || r.UnknownEvents != 2 || r.Tokens != 0 {
		t.Fatalf("other schema: %v/%q unknown=%d tokens=%d", r.Outcome, r.ErrClass, r.UnknownEvents, r.Tokens)
	}

	extra := "data: {\"x_mock_unknown\":\"mock\",\"choices\":[{\"delta\":{\"content\":\"hi\",\"x_mock_unknown\":\"mock\"}}]}\n\n"
	r = executeShaped(t, true, nil, extra, doneFrame)
	if r.Outcome != OutcomeCompleted || r.UnknownEvents != 0 || r.Content != "hi" {
		t.Fatalf("extra keys: %v/%q unknown=%d content=%q", r.Outcome, r.ErrClass, r.UnknownEvents, r.Content)
	}

	sh := &config.ResponseShape{Reasoning: []string{"reasoning"}}
	r = executeShaped(t, true, sh, "data: {\"choices\":[{\"delta\":{\"reasoning\":{\"text\":\"r\"}}}]}\n\n", answerFrame, doneFrame)
	if r.ErrClass != ErrMalformedStream {
		t.Fatalf("reasoning object: %v/%q, want %s", r.Outcome, r.ErrClass, ErrMalformedStream)
	}
}

// failingBody streams frames and then fails the read with err.
type failingBody struct {
	io.Reader
	err error
}

func (b *failingBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF {
		return n, b.err
	}
	return n, err
}

func (b *failingBody) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// A reset or a timeout mid-stream keeps its §3 class, and the fields read
// before it are kept: on a hosted stream with no in-band error or with one
// in an event the read error cut short, and on a self-hosted stream whose
// in-band error is kept beside the class.
func TestResetAndTimeoutKeepParsedFields(t *testing.T) {
	head := "data: {\"model\":\"reported\",\"choices\":[{\"delta\":{\"reasoning\":\"r\"}}]}\n\n" + answerFrame
	inBand := "data: {\"error\":{\"message\":\"cut\"}}\n"
	for _, c := range []struct {
		name    string
		hosted  bool
		frames  string
		err     error
		outcome Outcome
		class   string
		stream  string
	}{
		{"hosted reset", true, head, syscall.ECONNRESET, OutcomeErrored, ErrReset, ""},
		{"hosted timeout", true, head, context.DeadlineExceeded, OutcomeCensored, "", ""},
		{"hosted reset inside an error event", true, head + inBand, syscall.ECONNRESET, OutcomeErrored, ErrReset, `{"message":"cut"}`},
		{"self-hosted reset", false, head + inBand + "\n", syscall.ECONNRESET, OutcomeErrored, ErrReset, `{"message":"cut"}`},
		{"self-hosted timeout", false, head + inBand + "\n", context.DeadlineExceeded, OutcomeCensored, "", `{"message":"cut"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Proto: "HTTP/1.1", Header: http.Header{},
					Body: &failingBody{strings.NewReader(c.frames), c.err}, Request: req,
				}, nil
			})}
			r := &Request{Index: 1}
			shapeGen("http://127.0.0.1:1", client, c.hosted, &config.ResponseShape{Reasoning: []string{"reasoning"}}).execute(r)
			if r.Outcome != c.outcome || r.ErrClass != c.class {
				t.Fatalf("got %v/%q, want %v/%q", r.Outcome, r.ErrClass, c.outcome, c.class)
			}
			if r.ReportedModel != "reported" || r.ReasoningChunks != 1 || r.Tokens != 1 || r.StreamError != c.stream {
				t.Fatalf("model %q reasoning %d tokens %d error %q", r.ReportedModel, r.ReasoningChunks, r.Tokens, r.StreamError)
			}
			if c.hosted && r.Content != `{"route":"billing"}` {
				t.Fatalf("content %q", r.Content)
			}
		})
	}
}

// stagedBody returns head on the first read, then after delay tail and
// err.
type stagedBody struct {
	head, tail string
	delay      time.Duration
	err        error
	reads      int
}

func (b *stagedBody) Read(p []byte) (int, error) {
	b.reads++
	switch {
	case b.reads == 1:
		return copy(p, b.head), nil
	case b.reads == 2 && b.tail != "":
		time.Sleep(b.delay)
		return copy(p, b.tail), nil
	case b.reads == 2:
		time.Sleep(b.delay)
	}
	return 0, b.err
}

func (b *stagedBody) Close() error { return nil }

// In a hosted stream the first in-band error ends the request as
// malformed_stream at the error's arrival. A reset, a timeout or a
// terminator after it changes neither the class nor the failure time; the
// content and usage read before it are kept, and later events are not read.
func TestInBandErrorEndsHostedRequest(t *testing.T) {
	const later = 2 * time.Second
	usage := "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\n"
	chatHead := answerFrame + usage + "data: {\"error\":{\"message\":\"cut\"}}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":9}}\n\n"
	chatGen := func(c *http.Client) *gen { return shapeGen("http://127.0.0.1:1", c, true, nil) }
	msgGen := func(c *http.Client) *gen { return messagesGen("http://127.0.0.1:1/v1/messages", c, config.AuthXAPIKey) }
	msgHead := msgStart + textStart + text("x") + overload + msgDelta
	for _, c := range []struct {
		name       string
		gen        func(*http.Client) *gen
		head, tail string
		err        error
		stream     string
		usage      string
	}{
		{"reset", chatGen, chatHead, "", syscall.ECONNRESET, `{"message":"cut"}`, `{"prompt_tokens":5,"completion_tokens":1}`},
		{"timeout", chatGen, chatHead, "", context.DeadlineExceeded, `{"message":"cut"}`, `{"prompt_tokens":5,"completion_tokens":1}`},
		{"terminator", chatGen, chatHead, doneFrame, io.EOF, `{"message":"cut"}`, `{"prompt_tokens":5,"completion_tokens":1}`},
		{"messages reset", msgGen, msgHead, "", syscall.ECONNRESET,
			`{"type":"overloaded_error","message":"Overloaded"}`, `{"input_tokens":25,"output_tokens":1}`},
		{"messages terminator", msgGen, msgHead, msgStop, io.EOF,
			`{"type":"overloaded_error","message":"Overloaded"}`, `{"input_tokens":25,"output_tokens":1}`},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Proto: "HTTP/1.1", Header: http.Header{},
					Body: &stagedBody{head: c.head, tail: c.tail, delay: later, err: c.err}, Request: req,
				}, nil
			})}
			r := &Request{Index: 1}
			c.gen(client).execute(r)
			if r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
				t.Fatalf("got %v/%q, want errored/%s", r.Outcome, r.ErrClass, ErrMalformedStream)
			}
			if r.StreamError != c.stream || r.Tokens != 1 || string(r.Usage) != c.usage || r.FinishReason != "" {
				t.Fatalf("error %q tokens %d usage %s finish %q", r.StreamError, r.Tokens, r.Usage, r.FinishReason)
			}
			var errNs int64
			for _, f := range r.Frames {
				if strings.Contains(string(f.Data), `"error"`) {
					errNs = f.Ns
				}
			}
			if errNs == 0 || r.DoneNs < errNs || r.DoneNs-errNs > (later/2).Nanoseconds() {
				t.Fatalf("done at %d, error frame at %d", r.DoneNs, errNs)
			}
		})
	}
}

// The D1 mock's hosted stream form, read end to end.
func TestShapedMockStream(t *testing.T) {
	start := func(sh mock.Shape) string {
		fixed := config.LatencyDist{Distribution: "fixed", FixedMs: 1}
		s := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed}, sh)
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return "http://" + s.Addr()
	}
	run := func(base string, hosted bool) *Request {
		r := &Request{Index: 1}
		shapeGen(base, &http.Client{}, hosted, &config.ResponseShape{Reasoning: []string{"reasoning"}}).execute(r)
		return r
	}

	base := start(mock.Shape{ReasoningKey: "reasoning", ReasoningTokens: 3, Answer: `{"route":"billing"}`, Usage: mock.UsageAlways})
	r := run(base, true)
	if r.Outcome != OutcomeCompleted || r.Content != `{"route":"billing"}` || r.Reasoning != "r1 r2 r3 " {
		t.Fatalf("got %v/%q content %q reasoning %q", r.Outcome, r.ErrClass, r.Content, r.Reasoning)
	}
	if r.Tokens != 5 || r.CompletionTokens != 8 || r.FinishReason != "stop" || r.ReportedModel != "m" || r.FirstReasoningNs > r.FirstTokNs {
		t.Fatalf("tokens %d completion %d finish %q model %q reasoning at %d, content at %d",
			r.Tokens, r.CompletionTokens, r.FinishReason, r.ReportedModel, r.FirstReasoningNs, r.FirstTokNs)
	}

	base = start(mock.Shape{Answer: `{"route":"billing"}`, Error: map[string]any{"message": "boom"}, ErrorAfterContent: 2})
	if r := run(base, true); r.ErrClass != ErrMalformedStream || r.Tokens != 2 || r.StreamError != `{"message":"boom"}` {
		t.Fatalf("hosted: %v/%q tokens=%d stream error %q", r.Outcome, r.ErrClass, r.Tokens, r.StreamError)
	}
	if r := run(base, false); r.Outcome != OutcomeCompleted || r.StreamError != `{"message":"boom"}` {
		t.Fatalf("self-hosted: %v/%q stream error %q", r.Outcome, r.ErrClass, r.StreamError)
	}
}
