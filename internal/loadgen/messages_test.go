package loadgen

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// ev is one Messages stream event with its SSE event name.
func ev(name, data string) string { return "event: " + name + "\ndata: " + data + "\n\n" }

var (
	msgStart   = ev("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-sonnet-5-5","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":25,"output_tokens":1}}}`)
	thinkStart = ev("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`)
	think      = func(s string) string {
		return ev("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"`+s+`"}}`)
	}
	signature = ev("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"EqQBCgIYAh"}}`)
	stopBlock = func(i string) string { return ev("content_block_stop", `{"type":"content_block_stop","index":`+i+`}`) }
	textStart = ev("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`)
	text      = func(s string) string {
		return ev("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"`+s+`"}}`)
	}
	ping      = ev("ping", `{"type": "ping"}`)
	msgDelta  = ev("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":15}}`)
	msgStop   = ev("message_stop", `{"type":"message_stop"}`)
	overload  = ev("error", `{"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}`)
	fullReply = []string{msgStart, thinkStart, think("weigh "), think("the ticket"), signature, stopBlock("0"),
		textStart, ping, text("The customer"), text(" was billed twice."), stopBlock("1"), msgDelta, msgStop}
)

// messagesGen is a gen for a hosted Messages target at endpoint, holding
// testKey in the header auth names.
func messagesGen(endpoint string, client *http.Client, auth string) *gen {
	cfg := &config.Config{}
	cfg.Run.Seed = 1
	cfg.Target = config.Target{Hosted: true, Adapter: config.AdapterMessages, EndpointURL: endpoint, AuthHeader: auth, Body: targetBody}
	return &gen{cfg: cfg, client: client, epoch: time.Now(), model: "claude-sonnet-5-5", apiKey: testKey, cases: []string{"", "ticket one"}}
}

func executeMessages(t *testing.T, frames ...string) *Request {
	t.Helper()
	srv := sseServer(t, frames...)
	defer srv.Close()
	r := &Request{Index: 1}
	messagesGen(srv.URL+"/v1/messages", srv.Client(), config.AuthXAPIKey).execute(r)
	return r
}

// A Messages stream: text_delta text is the answer, thinking_delta text is
// reasoning kept apart, message_start carries the model, usage arrives in
// message_start and message_delta, and message_stop ends it.
func TestMessagesStream(t *testing.T) {
	r := executeMessages(t, fullReply...)
	if r.Outcome != OutcomeCompleted || r.Tokens != 2 {
		t.Fatalf("got %v/%q tokens=%d, want completed with two tokens", r.Outcome, r.ErrClass, r.Tokens)
	}
	if r.Content != "The customer was billed twice." || r.Reasoning != "weigh the ticket" || r.ReasoningChunks != 2 {
		t.Fatalf("content %q reasoning %q chunks %d", r.Content, r.Reasoning, r.ReasoningChunks)
	}
	if r.FirstReasoningNs == 0 || r.FirstReasoningNs > r.FirstTokNs || len(r.ITLsUs) != 1 {
		t.Fatalf("first reasoning %d, first token %d, gaps %d", r.FirstReasoningNs, r.FirstTokNs, len(r.ITLsUs))
	}
	if r.FinishReason != "end_turn" || r.ReportedModel != "claude-sonnet-5-5" || len(r.ModelMismatch) != 0 {
		t.Fatalf("finish %q model %q mismatch %q", r.FinishReason, r.ReportedModel, r.ModelMismatch)
	}
	if !r.UsageSeen || r.CompletionTokens != 15 || string(r.Usage) != `{"input_tokens":25,"output_tokens":15}` {
		t.Fatalf("usage seen %v tokens %d raw %s", r.UsageSeen, r.CompletionTokens, r.Usage)
	}
	if r.UnknownEvents != 0 || r.StreamError != "" || len(r.Frames) == 0 {
		t.Fatalf("unknown %d, stream error %q, %d frames", r.UnknownEvents, r.StreamError, len(r.Frames))
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "billed") || strings.Contains(string(b), "weigh") {
		t.Fatalf("answer or reasoning text in JSON: %s", b)
	}
}

// Terminators, in-band errors, empty streams, usage placements and
// wrong-typed fields on a Messages target.
func TestMessagesOutcomes(t *testing.T) {
	head := []string{msgStart, textStart, text("x")}
	with := func(frames ...string) []string { return append(append([]string{}, head...), frames...) }
	for _, tc := range []struct {
		name      string
		frames    []string
		outcome   Outcome
		class     string
		check     func(*Request) bool
		checkText string
	}{
		{"cut before message_stop", with(msgDelta), OutcomeErrored, ErrMalformedStream, nil, ""},
		{"chat terminator", with(msgDelta, "data: [DONE]\n\n"), OutcomeErrored, ErrMalformedStream, nil, ""},
		{"in-band error after content", with(overload), OutcomeErrored, ErrMalformedStream,
			func(r *Request) bool { return r.StreamError == `{"type":"overloaded_error","message":"Overloaded"}` }, "error object kept"},
		{"in-band error before content", []string{overload}, OutcomeErrored, ErrMalformedStream,
			func(r *Request) bool { return r.StreamError == `{"type":"overloaded_error","message":"Overloaded"}` }, "error object kept"},
		{"in-band error then message_stop", with(overload, msgDelta, msgStop), OutcomeErrored, ErrMalformedStream,
			func(r *Request) bool { return r.StreamError != "" }, "error object kept"},
		{"error event without an error object", with(ev("error", `{"type":"error"}`)), OutcomeErrored, ErrMalformedStream,
			func(r *Request) bool { return r.StreamError == `{"type":"error"}` }, "whole event kept"},
		{"reasoning only, stopped at the ceiling", []string{msgStart, thinkStart, think("r"), signature, stopBlock("0"),
			ev("message_delta", `{"type":"message_delta","delta":{"stop_reason":"max_tokens","stop_sequence":null},"usage":{"output_tokens":512}}`), msgStop},
			OutcomeErrored, ErrEmptyStream,
			func(r *Request) bool {
				return r.FinishReason == "max_tokens" && r.ReasoningChunks == 1 && r.CompletionTokens == 512
			},
			"finish reason, reasoning and usage kept"},
		{"thinking omitted", []string{msgStart, thinkStart, think(""), signature, stopBlock("0"), textStart, text("x"), msgDelta, msgStop},
			OutcomeCompleted, "", func(r *Request) bool { return r.ReasoningChunks == 0 && r.FirstReasoningNs == 0 }, "no reasoning chunk"},
		{"no message_delta", with(msgStop), OutcomeCompleted, "",
			func(r *Request) bool {
				return r.FinishReason == "" && !r.UsageSeen && string(r.Usage) == `{"input_tokens":25,"output_tokens":1}`
			}, "no finish reason, start usage kept unseen"},
		{"text in content_block_start", []string{msgStart, ev("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"y"}}`), msgDelta, msgStop},
			OutcomeCompleted, "", func(r *Request) bool { return r.Tokens == 1 && r.Content == "y" }, "one token"},
		{"unknown event and delta types", with(ev("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"citations_delta","citation":{}}}`),
			ev("future_event", `{"type":"future_event"}`), msgDelta, msgStop),
			OutcomeCompleted, "", func(r *Request) bool { return r.UnknownEvents == 2 && r.Tokens == 1 }, "two unknown events"},
		{"usage merged across message_delta events", with(ev("message_delta", `{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":3,"input_tokens":null}}`), msgDelta, msgStop),
			OutcomeCompleted, "", func(r *Request) bool {
				return r.CompletionTokens == 15 && string(r.Usage) == `{"input_tokens":25,"output_tokens":15}`
			}, "cumulative usage, null fields not laid over"},
		{"not JSON", with("data: hello\n\n", msgStop), OutcomeErrored, ErrMalformedStream, nil, ""},
		{"no type", with("data: {\"index\":0}\n\n", msgStop), OutcomeErrored, ErrMalformedStream, nil, ""},
		{"text of the wrong type", with(ev("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":5}}`), msgStop),
			OutcomeErrored, ErrMalformedStream, nil, ""},
		{"usage not an object", with(ev("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":3}`), msgStop),
			OutcomeErrored, ErrMalformedStream, nil, ""},
		{"output_tokens not a number", with(ev("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":"15"}}`), msgStop),
			OutcomeErrored, ErrMalformedStream, nil, ""},
		{"message_start without a message", []string{ev("message_start", `{"type":"message_start"}`), textStart, text("x"), msgStop},
			OutcomeErrored, ErrMalformedStream, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := executeMessages(t, tc.frames...)
			if r.Outcome != tc.outcome || r.ErrClass != tc.class {
				t.Fatalf("got %v/%q, want %v/%q", r.Outcome, r.ErrClass, tc.outcome, tc.class)
			}
			if tc.check != nil && !tc.check(r) {
				t.Fatalf("%s: %+v", tc.checkText, r)
			}
		})
	}
}

// A Messages request carries anthropic-version and the key in the
// target's one header, to the complete URL, direct or Bedrock's; a chat
// target sends no anthropic-version.
func TestMessagesRequestHeaders(t *testing.T) {
	for _, tc := range []struct {
		name, adapter, auth, path, keyHeader, keyValue, version string
	}{
		{"direct", config.AdapterMessages, config.AuthBearer, "/v1/messages", "Authorization", "Bearer " + testKey, config.MessagesVersion},
		{"direct, legacy key header", config.AdapterMessages, config.AuthXAPIKey, "/v1/messages", "X-Api-Key", testKey, config.MessagesVersion},
		{"bedrock", config.AdapterMessages, config.AuthBearer, "/anthropic/v1/messages", "Authorization", "Bearer " + testKey, config.MessagesVersion},
		{"chat", config.AdapterChatCompletions, config.AuthBearer, "/v1/chat/completions", "Authorization", "Bearer " + testKey, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var got http.Header
			var path, body string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				mu.Lock()
				got, path, body = r.Header.Clone(), r.URL.Path, string(b)
				mu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, strings.Join(fullReply, "")) //nolint:errcheck
			}))
			defer srv.Close()
			g := messagesGen(srv.URL+tc.path, srv.Client(), tc.auth)
			g.cfg.Target.Adapter = tc.adapter
			r := &Request{Index: 1}
			g.execute(r)
			mu.Lock()
			defer mu.Unlock()
			if v := got.Values("Anthropic-Version"); tc.version == "" && len(v) != 0 || tc.version != "" && (len(v) != 1 || v[0] != tc.version) {
				t.Errorf("anthropic-version %q, want %q", v, tc.version)
			}
			if v := got.Values(tc.keyHeader); len(v) != 1 || v[0] != tc.keyValue {
				t.Errorf("%s = %q", tc.keyHeader, v)
			}
			if path != tc.path || !strings.Contains(body, `"content":"ticket one"`) {
				t.Errorf("posted to %s: %s", path, body)
			}
			if tc.adapter == config.AdapterMessages && r.Outcome != OutcomeCompleted {
				t.Errorf("outcome %v/%q", r.Outcome, r.ErrClass)
			}
		})
	}
}
