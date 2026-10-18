package loadgen

import (
	"net/http"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

// startMessagesMock starts a Messages-shaped mock holding testKey in the
// x-api-key header, paced at ttftMs, and returns its /v1/messages URL.
func startMessagesMock(t *testing.T, sh mock.Shape, ttftMs float64) string {
	t.Helper()
	sh.Messages, sh.Key, sh.AuthHeader = true, testKey, config.AuthXAPIKey
	cfg := config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1,
		TTFT: config.LatencyDist{Distribution: "fixed", FixedMs: ttftMs}, ITL: config.LatencyDist{Distribution: "fixed"}}
	s := mock.NewShaped(cfg, sh)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return "http://" + s.Addr() + "/v1/messages"
}

func runMessagesMock(endpoint, auth string, deadlineS int) *Request {
	g := messagesGen(endpoint, &http.Client{}, auth)
	if deadlineS > 0 {
		g.cfg.Profile = config.ProfileRelease
		g.cfg.Client.HTTPTimeoutS = 90
		g.cfg.Release = &config.Release{Shape: config.Shape{DeadlineS: deadlineS}}
	}
	r := &Request{Index: 1}
	g.execute(r)
	return r
}

// The mock's Messages shape modes, read end to end by the Messages adapter.
func TestMessagesMockFixtures(t *testing.T) {
	overloaded := map[string]any{"type": "overloaded_error", "message": "Overloaded"}
	answer := "The customer was billed."
	for _, tc := range []struct {
		name    string
		sh      mock.Shape
		outcome Outcome
		class   string
		check   func(*Request) bool
	}{
		{"complete", mock.Shape{ReasoningTokens: 2, Answer: answer}, OutcomeCompleted, "", func(r *Request) bool {
			return r.Content == answer && r.Reasoning == "r1 r2 " && r.Tokens == 6 && r.FinishReason == "end_turn" &&
				r.UsageSeen && r.CompletionTokens == 8 && string(r.Usage) == `{"input_tokens":2,"output_tokens":8}` &&
				r.ReportedModel == "claude-sonnet-5-5" && len(r.ModelMismatch) == 0 && r.UnknownEvents == 0
		}},
		{"cut", mock.Shape{Answer: answer, Cut: true}, OutcomeErrored, ErrMalformedStream, func(r *Request) bool {
			return r.Content == answer && r.FinishReason == "end_turn"
		}},
		{"empty", mock.Shape{}, OutcomeErrored, ErrEmptyStream, func(r *Request) bool {
			return r.FinishReason == "end_turn" && r.Tokens == 0
		}},
		{"reasoning stopped at the ceiling", mock.Shape{ReasoningTokens: 600, Answer: answer}, OutcomeErrored, ErrEmptyStream, func(r *Request) bool {
			return r.FinishReason == "max_tokens" && r.ReasoningChunks == 512 && r.CompletionTokens == 512
		}},
		{"in-band error", mock.Shape{Answer: answer, Error: overloaded, ErrorAfterContent: 2}, OutcomeErrored, ErrMalformedStream, func(r *Request) bool {
			return r.Tokens == 2 && r.StreamError == `{"message":"Overloaded","type":"overloaded_error"}`
		}},
		{"no finish reason", mock.Shape{Answer: answer, OmitFinish: true}, OutcomeCompleted, "", func(r *Request) bool {
			return r.FinishReason == "" && r.UsageSeen
		}},
		{"usage in message_start only", mock.Shape{Answer: answer, Usage: mock.UsageStart}, OutcomeCompleted, "", func(r *Request) bool {
			return !r.UsageSeen && string(r.Usage) == `{"input_tokens":2,"output_tokens":0}`
		}},
		{"usage in message_delta only", mock.Shape{Answer: answer, Usage: mock.UsageDelta}, OutcomeCompleted, "", func(r *Request) bool {
			return r.UsageSeen && r.CompletionTokens == 6 && string(r.Usage) == `{"output_tokens":6}`
		}},
		{"no usage", mock.Shape{Answer: answer, Usage: mock.UsageNever}, OutcomeCompleted, "", func(r *Request) bool {
			return !r.UsageSeen && (len(r.Usage) == 0 || string(r.Usage) == "{}")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runMessagesMock(startMessagesMock(t, tc.sh, 0), config.AuthXAPIKey, 0)
			if r.Outcome != tc.outcome || r.ErrClass != tc.class || r.Status != http.StatusOK {
				t.Fatalf("got %d %v/%q, want %v/%q", r.Status, r.Outcome, r.ErrClass, tc.outcome, tc.class)
			}
			if !tc.check(r) {
				t.Fatalf("%+v content %q reasoning %q usage %s", r, r.Content, r.Reasoning, r.Usage)
			}
		})
	}
}

// The mock refuses a key sent in a header other than the one it names.
func TestMessagesMockCredentialHeader(t *testing.T) {
	endpoint := startMessagesMock(t, mock.Shape{Answer: "a"}, 0)
	if r := runMessagesMock(endpoint, config.AuthXAPIKey, 0); r.Outcome != OutcomeCompleted {
		t.Fatalf("x-api-key: %d %v/%q", r.Status, r.Outcome, r.ErrClass)
	}
	r := runMessagesMock(endpoint, config.AuthBearer, 0)
	if r.Status != http.StatusUnauthorized || r.ErrClass != ErrStatusOther || !strings.Contains(string(r.ErrorBody), "authentication_error") {
		t.Fatalf("bearer: %d %v/%q body %s", r.Status, r.Outcome, r.ErrClass, r.ErrorBody)
	}
}

// A Messages completion past the release deadline is late.
func TestMessagesMockLate(t *testing.T) {
	if r := runMessagesMock(startMessagesMock(t, mock.Shape{Answer: "a"}, 2000), config.AuthXAPIKey, 1); r.Outcome != OutcomeCompleted || !r.Late {
		t.Fatalf("slow: %v/%q late=%v", r.Outcome, r.ErrClass, r.Late)
	}
	if r := runMessagesMock(startMessagesMock(t, mock.Shape{Answer: "a"}, 0), config.AuthXAPIKey, 30); r.Outcome != OutcomeCompleted || r.Late {
		t.Fatalf("fast: %v/%q late=%v", r.Outcome, r.ErrClass, r.Late)
	}
}
