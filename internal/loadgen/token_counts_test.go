package loadgen

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// usageStream runs one request against a stream that reports count as
// its completion count: a Messages message_delta output_tokens when
// messages is set, otherwise a chat-completion usage chunk's
// completion_tokens.
func usageStream(t *testing.T, messages bool, count string) *Request {
	t.Helper()
	body := "data: {\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":900,\"completion_tokens\":" + count + "}}\n\n" +
		"data: [DONE]\n\n"
	if messages {
		body = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"usage\":{\"input_tokens\":900,\"output_tokens\":1}}}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":" + count + "}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(body)) //nolint:errcheck
	}))
	defer srv.Close()
	g := captureGen(srv)
	if messages {
		g.cfg.Target.Adapter = config.AdapterMessages
	}
	r := &Request{Index: 1}
	g.execute(r)
	return r
}

// A Messages output_tokens and a chat-completion completion_tokens follow
// TokenCount's rule: exponent and decimal notation count at their value
// and a count past int64 is math.MaxInt. A Messages value that is no
// count and does not decode as an integer still fails the stream.
func TestUsageCountNotation(t *testing.T) {
	for _, c := range []struct {
		messages  bool
		count     string
		malformed bool
		seen      bool
		n         int
	}{
		{true, `600`, false, true, 600},
		{true, `6e2`, false, true, 600},
		{true, `600.0`, false, true, 600},
		{true, `99999999999999999999`, false, true, math.MaxInt},
		{true, `9223372036854775808`, false, true, math.MaxInt},
		{true, `"600"`, true, false, 0},
		{true, `600.5`, true, false, 0},
		{true, `true`, true, false, 0},
		{true, `-1`, false, false, 0},
		{true, `null`, false, false, 0},
		{false, `600`, false, true, 600},
		{false, `6e2`, false, true, 600},
		{false, `600.0`, false, true, 600},
		{false, `99999999999999999999`, false, true, math.MaxInt},
		{false, `9223372036854775808`, false, true, math.MaxInt},
		{false, `"600"`, false, false, 0},
		{false, `600.5`, false, false, 0},
		{false, `-1`, false, false, 0},
		{false, `true`, false, false, 0},
	} {
		r := usageStream(t, c.messages, c.count)
		form := "chat completion"
		if c.messages {
			form = "messages"
		}
		if c.malformed {
			if r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
				t.Errorf("%s %s: outcome %s %q, want malformed_stream", form, c.count, r.Outcome, r.ErrClass)
			}
			continue
		}
		if r.Outcome != OutcomeCompleted {
			t.Errorf("%s %s: outcome %s %q, want completed", form, c.count, r.Outcome, r.ErrClass)
			continue
		}
		if r.UsageSeen != c.seen || (c.seen && r.CompletionTokens != c.n) {
			t.Errorf("%s %s: usage seen %t, completion %d; want %t, %d", form, c.count, r.UsageSeen, r.CompletionTokens, c.seen, c.n)
		}
		if o, ok := r.OutputTokens(); c.seen && (o != c.n || !ok) {
			t.Errorf("%s %s: output %d %t, want %d", form, c.count, o, ok, c.n)
		}
	}
}

// A usage chunk past int64 after one reporting 10 leaves the request's
// completion count at math.MaxInt.
func TestUsageCountLaterSaturates(t *testing.T) {
	g := &gen{}
	r := &Request{}
	for _, u := range []string{`{"completion_tokens":10}`, `{"completion_tokens":99999999999999999999}`} {
		c, ok := parseChunk([]byte(`{"choices":[],"usage":`+u+`}`), (&config.Target{}).Shape())
		if !ok {
			t.Fatalf("%s: not parsed", u)
		}
		g.recordChunk(r, &c, nil)
	}
	if !r.UsageSeen || r.CompletionTokens != math.MaxInt {
		t.Errorf("completion %d seen %t, want math.MaxInt", r.CompletionTokens, r.UsageSeen)
	}
}

// A total_tokens past int64 makes the output math.MaxInt, whatever the
// prompt count.
func TestOutputTokensSaturatedTotal(t *testing.T) {
	for _, u := range []string{
		`{"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1e20}`,
		`{"prompt_tokens":1e20,"completion_tokens":100,"total_tokens":1e20}`,
		`{"completion_tokens":100,"total_tokens":99999999999999999999}`,
	} {
		r := &Request{Usage: json.RawMessage(u)}
		if o, ok := r.OutputTokens(); o != math.MaxInt || !ok {
			t.Errorf("%s: output %d %t, want math.MaxInt", u, o, ok)
		}
	}
	r := &Request{Usage: json.RawMessage(`{"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1300}`)}
	if o, ok := r.OutputTokens(); o != 300 || !ok {
		t.Errorf("unsaturated total: output %d %t, want 300", o, ok)
	}
}

// A count in exponent or decimal notation is taken at its integral
// value, saturating at math.MaxInt; a fractional one is no count.
func TestTokenCountNotation(t *testing.T) {
	for _, c := range []struct {
		raw string
		n   int
		ok  bool
	}{
		{`1.5e4`, 15000, true},
		{`5.12e3`, 5120, true},
		{`9E+3`, 9000, true},
		{`15000.0`, 15000, true},
		{`0e7`, 0, true},
		{`-0`, 0, true},
		{`1500e-2`, 15, true},
		{`9.223372036854775807e18`, math.MaxInt, true},
		{`9.223372036854775808e18`, math.MaxInt, true},
		{`1e400`, math.MaxInt, true},
		{`1e99999999999999999999`, math.MaxInt, true},
		{`1.25e1`, 0, false},
		{`15e-1`, 0, false},
		{`1e-99999999999999999999`, 0, false},
		{`-1.5e4`, 0, false},
		{`"1.5e4"`, 0, false},
		{`true`, 0, false},
		{`{}`, 0, false},
	} {
		if n, ok := TokenCount(json.RawMessage(c.raw)); n != c.n || ok != c.ok {
			t.Errorf("%s: %d %t, want %d %t", c.raw, n, ok, c.n, c.ok)
		}
	}
	if got := TokenCountText(math.MaxInt); got != "at least 9223372036854775807" {
		t.Errorf("saturated text %q", got)
	}
	if got := TokenCountText(1200); got != "1200" {
		t.Errorf("count text %q", got)
	}
}

// Output takes a reasoning_tokens or completion_tokens count past int64
// as math.MaxInt, a later usage's completion_tokens over an earlier
// chunk's count, and an exponent count at its value.
func TestOutputTokensSaturate(t *testing.T) {
	g := &gen{}
	r := &Request{}
	for _, u := range []string{`{"completion_tokens":10}`, `{"completion_tokens":99999999999999999999}`} {
		c, ok := parseChunk([]byte(`{"choices":[],"usage":`+u+`}`), (&config.Target{}).Shape())
		if !ok {
			t.Fatalf("%s: not parsed", u)
		}
		g.recordChunk(r, &c, nil)
	}
	if n, ok := r.OutputTokens(); n != math.MaxInt || !ok {
		t.Errorf("two usage chunks: output %d %t, completion %d seen %t", n, ok, r.CompletionTokens, r.UsageSeen)
	}
	for _, c := range []struct {
		usage          string
		prompt, output int
	}{
		{`{"prompt_tokens":1000,"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":99999999999999999999}}`, 1000, math.MaxInt},
		{`{"prompt_tokens":1.5e4,"completion_tokens":5.12e3}`, 15000, 5120},
		{`{"prompt_tokens":1000,"completion_tokens":100,"details":{"reasoning_tokens":9e3}}`, 1000, 9000},
	} {
		r := &Request{Usage: json.RawMessage(c.usage)}
		p, pok := r.PromptTokens()
		o, ook := r.OutputTokens()
		if p != c.prompt || o != c.output || !pok || !ook {
			t.Errorf("%s: prompt %d %t, output %d %t; want %d, %d", c.usage, p, pok, o, ook, c.prompt, c.output)
		}
	}
}

// A Messages message_delta with a negative output_tokens is read but its
// count is not seen, so the request keeps no completion count; a
// non-negative one is seen.
func TestMessagesNegativeOutputTokensUnseen(t *testing.T) {
	g := &gen{}
	for _, c := range []struct {
		usage string
		n     int
		seen  bool
	}{
		{`{"output_tokens":-500}`, 0, false},
		{`{"output_tokens":300}`, 300, true},
	} {
		c0, ok := parseMessagesEvent([]byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":` + c.usage + `}`))
		if !ok {
			t.Fatalf("%s: not parsed", c.usage)
		}
		r := &Request{}
		g.recordChunk(r, &c0, nil)
		if r.UsageSeen != c.seen || r.CompletionTokens != c.n {
			t.Errorf("%s: completion %d, seen %t; want %d, %t", c.usage, r.CompletionTokens, r.UsageSeen, c.n, c.seen)
		}
		if o, ok := r.OutputTokens(); ok != c.seen || o != c.n {
			t.Errorf("%s: output %d %t", c.usage, o, ok)
		}
	}
}

// A reported count is a non-negative JSON number of integral value; one
// past math.MaxInt is math.MaxInt, and a negative, fractional or string
// value is no count.
func TestTokenCountIntegralNonNegativeRule(t *testing.T) {
	for _, c := range []struct {
		raw string
		n   int
		ok  bool
	}{
		{`12`, 12, true},
		{`0`, 0, true},
		{`9223372036854775807`, math.MaxInt, true},
		{`99999999999999999999`, math.MaxInt, true},
		{`-1`, 0, false},
		{`-99999999999999999999`, 0, false},
		{`12.5`, 0, false},
		{`1e20`, math.MaxInt, true},
		{`"12"`, 0, false},
		{`null`, 0, false},
		{``, 0, false},
	} {
		var raw json.RawMessage
		if c.raw != "" {
			raw = json.RawMessage(c.raw)
		}
		if n, ok := TokenCount(raw); n != c.n || ok != c.ok {
			t.Errorf("%q: %d %t, want %d %t", c.raw, n, ok, c.n, c.ok)
		}
	}
}
