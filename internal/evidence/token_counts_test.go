package evidence

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// A completion count in exponent or decimal notation, or past int64,
// reaches the record's completion_tokens at its value or math.MaxInt, for
// a Messages stream and a chat-completion stream.
func TestRecordCompletionTokensNotation(t *testing.T) {
	for _, c := range []struct {
		messages bool
		count    string
		n        int
	}{
		{true, `6e2`, 600},
		{true, `600.0`, 600},
		{true, `99999999999999999999`, math.MaxInt},
		{false, `6e2`, 600},
		{false, `600.0`, 600},
		{false, `99999999999999999999`, math.MaxInt},
	} {
		body := "data: {\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":900,\"completion_tokens\":" + c.count + "}}\n\n" +
			"data: [DONE]\n\n"
		if c.messages {
			body = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"usage\":{\"input_tokens\":900,\"output_tokens\":1}}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
				"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":" + c.count + "}}\n\n" +
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte(body)) //nolint:errcheck
		}))
		cfg := hostedCfg(srv.URL)
		cfg.Target.ModelName = "m"
		if c.messages {
			cfg.Target.Adapter = config.AdapterMessages
		}
		res, err := loadgen.Run(context.Background(), cfg, nil)
		srv.Close()
		if err != nil {
			t.Fatalf("%s: %v", c.count, err)
		}
		recs := Records(cfg, res, Options{})
		if len(recs) == 0 {
			t.Fatalf("%s: no records", c.count)
		}
		for _, rec := range recs {
			if rec.Outcome != loadgen.OutcomeCompleted {
				t.Errorf("messages %t %s: record %d outcome %s %q", c.messages, c.count, rec.Index, rec.Outcome, rec.ErrClass)
				continue
			}
			if rec.CompletionTokens == nil || *rec.CompletionTokens != c.n {
				t.Errorf("messages %t %s: record %d completion_tokens %v, want %d", c.messages, c.count, rec.Index, rec.CompletionTokens, c.n)
			}
		}
	}
}

// A reasoning_tokens count past int64 is math.MaxInt, an exponent count
// its value, and two counts that disagree give none.
func TestReasoningTokensNotation(t *testing.T) {
	for _, c := range []struct {
		usage string
		want  *int
	}{
		{`{"details":{"reasoning_tokens":99999999999999999999}}`, intPtr(math.MaxInt)},
		{`{"details":{"reasoning_tokens":9e3}}`, intPtr(9000)},
		{`{"reasoning_tokens":99999999999999999999,"details":{"reasoning_tokens":4}}`, nil},
		{`{"reasoning_tokens":4,"details":{"reasoning_tokens":4.0}}`, intPtr(4)},
		{`{"reasoning_tokens":4.5}`, nil},
	} {
		got := reasoningTokens(json.RawMessage(c.usage))
		if (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
			t.Errorf("%s: %v, want %v", c.usage, intValue(got), intValue(c.want))
		}
	}
}

func intPtr(n int) *int { return &n }

func intValue(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// A negative reasoning_tokens is no count: alone it leaves the record's
// reasoning count absent, and beside a non-negative one it is skipped.
func TestReasoningTokensNegative(t *testing.T) {
	if got := reasoningTokens(json.RawMessage(`{"completion_tokens_details":{"reasoning_tokens":-900}}`)); got != nil {
		t.Errorf("negative alone: %d", *got)
	}
	if got := reasoningTokens(json.RawMessage(`{"reasoning_tokens":-1,"details":{"reasoning_tokens":4}}`)); got == nil || *got != 4 {
		t.Errorf("negative beside 4: %v", got)
	}
}
