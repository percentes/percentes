package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/sse"
)

// execute runs one scheduled request to its terminal state. The pinned
// 30 s client timeout runs from actual dispatch (a real client timeout);
// latency re-basing to intended time happens at read-out, and the send-
// skew gate bounds the difference.
func (g *gen) execute(r *Request) {
	r.DispatchNs = g.now()
	deadline := g.epoch.Add(time.Duration(r.DispatchNs) + time.Duration(config.PinnedClientTimeoutS)*time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	req, err := g.newRequest(ctx, g.requestBody(r))
	if err != nil {
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrConnect, g.now()
		return
	}

	resp, err := g.client.Do(req)
	if err != nil {
		g.classifyTransportErr(r, err)
		return
	}
	defer resp.Body.Close()
	// A hosted endpoint controls the replica header, so only the mock's is recorded.
	if !g.cfg.Target.Hosted {
		r.Replica = resp.Header.Get("X-Percentes-Replica")
	}

	if resp.StatusCode != http.StatusOK {
		class := ErrStatusOther
		// Status 429 is classified apart from other non-200 statuses (§3).
		if resp.StatusCode == http.StatusTooManyRequests {
			class = ErrStatus429
		}
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, class, g.now()
		return
	}

	var malformed, sawDone bool
	var prevTokNs int64
	_, dropped, err := sse.Events(resp.Body, eventLimit, func(payload []byte) bool {
		// [DONE] may carry trailing whitespace.
		if string(bytes.TrimSpace(payload)) == "[DONE]" {
			sawDone = true
			return true
		}
		// A chunk counts as a token only when its decoded delta carries
		// nonempty content; an undecodable payload is a malformed
		// stream (§3).
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal(payload, &chunk) != nil {
			malformed = true
			return true
		}
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != "" {
			r.Tokens++
			now := g.now()
			if r.FirstTokNs == 0 {
				r.FirstTokNs = now
			} else {
				r.ITLsUs = append(r.ITLsUs, (now-prevTokNs)/1000)
			}
			prevTokNs = now
		}
		return false
	})
	switch {
	case err != nil:
		// A read error, or a line over the bound, leaves the partial event untrusted.
		g.classifyStreamErr(r, err)
	case malformed || dropped > 0 || !sawDone:
		// An undecodable or oversized event, or a stream that ended before
		// [DONE]: malformed stream (§3).
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrMalformedStream, g.now()
	case r.FirstTokNs == 0:
		// [DONE] with no prior content event: an empty stream is errored,
		// never a completion (§3).
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrEmptyStream, g.now()
	default:
		r.Outcome, r.DoneNs = OutcomeCompleted, g.now()
	}
}

// requestBody builds the OpenAI-compatible chat-completion request.
// ignore_eos is a vLLM extension forcing the full max_tokens output
// budget (§6); hosted endpoints reject or ignore it, so a hosted target
// omits it and accepts natural stops.
func (g *gen) requestBody(r *Request) string {
	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	body, _ := json.Marshal(struct {
		Model     string    `json:"model"`
		Messages  []message `json:"messages"`
		Stream    bool      `json:"stream"`
		MaxTokens int       `json:"max_tokens"`
		IgnoreEOS bool      `json:"ignore_eos,omitempty"`
	}{
		Model:     g.model,
		Messages:  []message{{Role: "user", Content: fmt.Sprintf("cs-%d-%d %s", g.cfg.Run.Seed, r.Index, g.filler)}},
		Stream:    true,
		MaxTokens: g.cfg.Load.MaxTokens,
		IgnoreEOS: !g.cfg.Target.Hosted,
	})
	return string(body)
}

// newRequest attaches the fixed headers. The bearer token is added only
// when resolved (hosted targets); it exists nowhere but this header.
func (g *gen) newRequest(ctx context.Context, body string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.Target.BaseURL+"/v1/chat/completions", bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if g.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+g.apiKey)
	}
	// POSTs are not replayable by net/http (non-idempotent, no idempotency
	// key), so the transport never silently retries; belt-and-braces:
	req.GetBody = nil
	return req, nil
}

// Ceiling on one scanned line and on the data fields assembled into one event.
const eventLimit = 1 << 20

func (g *gen) classifyTransportErr(r *Request, err error) {
	r.DoneNs = g.now()
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		// No terminal event by the pinned timeout: censored (§3).
		r.Outcome = OutcomeCensored
	case isReset(err):
		r.Outcome, r.ErrClass = OutcomeErrored, ErrReset
	default:
		r.Outcome, r.ErrClass = OutcomeErrored, ErrConnect
	}
}

func (g *gen) classifyStreamErr(r *Request, err error) {
	r.DoneNs = g.now()
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		r.Outcome = OutcomeCensored
	case isReset(err):
		r.Outcome, r.ErrClass = OutcomeErrored, ErrReset
	default:
		// EOF or any framing error before [DONE]: malformed stream (§3).
		r.Outcome, r.ErrClass = OutcomeErrored, ErrMalformedStream
	}
}

func isReset(err error) bool {
	return errors.Is(err, syscall.ECONNRESET)
}
