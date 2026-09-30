package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

	resp, err := g.roundTrip(req)
	if err != nil {
		g.classifyTransportErr(r, err)
		return
	}
	defer resp.Body.Close()
	// Only the mock's replica header is recorded (§1); any other endpoint controls the value.
	r.Replica = replicaFrom(resp.Header, g.cfg.Mock != nil && !g.cfg.Target.Hosted, g.cfg.Target.BaseURL, g.apiKey)

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
		content, ok := ContentDelta(payload)
		if !ok {
			malformed = true
			return true
		}
		if n, ok := usageTokens(payload); ok {
			r.CompletionTokens, r.UsageSeen = n, true
		}
		if content != "" {
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
	// The transport reuses the connection only once the body reads to
	// its end, which [DONE] precedes.
	if sawDone && err == nil {
		io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit)) //nolint:errcheck
	}
}

// ContentDelta decodes one chat-completion chunk and returns the first
// choice's delta content; ok is false for a payload that does not decode.
func ContentDelta(payload []byte) (content string, ok bool) {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(payload, &chunk) != nil {
		return "", false
	}
	if len(chunk.Choices) == 0 {
		return "", true
	}
	return chunk.Choices[0].Delta.Content, true
}

// usageTokens returns the completion token count of a chunk's usage
// object, when the chunk carries one.
func usageTokens(payload []byte) (n int, ok bool) {
	var chunk struct {
		Usage *struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(payload, &chunk) != nil || chunk.Usage == nil {
		return 0, false
	}
	return chunk.Usage.CompletionTokens, true
}

// requestBody builds the OpenAI-compatible chat-completion request.
// ignore_eos is a vLLM extension forcing the full max_tokens output
// budget (§6); a hosted target omits it, since a provider's handling of
// the field is not pinned, and accepts natural stops. A self-hosted target
// is asked for the usage object in the stream's last chunk; the hosted
// body carries no such request.
func (g *gen) requestBody(r *Request) string {
	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type streamOptions struct {
		IncludeUsage bool `json:"include_usage"`
	}
	var usage *streamOptions
	if !g.cfg.Target.Hosted {
		usage = &streamOptions{IncludeUsage: true}
	}
	body, _ := json.Marshal(struct {
		Model         string         `json:"model"`
		Messages      []message      `json:"messages"`
		Stream        bool           `json:"stream"`
		MaxTokens     int            `json:"max_tokens"`
		IgnoreEOS     bool           `json:"ignore_eos,omitempty"`
		StreamOptions *streamOptions `json:"stream_options,omitempty"`
	}{
		Model:         g.model,
		Messages:      []message{{Role: "user", Content: fmt.Sprintf("cs-%d-%d %s", g.cfg.Run.Seed, r.Index, g.filler)}},
		Stream:        true,
		MaxTokens:     g.cfg.Load.MaxTokens,
		IgnoreEOS:     !g.cfg.Target.Hosted,
		StreamOptions: usage,
	})
	return string(body)
}

// roundTrip sends the request through the client's transport, so a
// delivered redirect is a status (§3) and its Location header is never
// parsed. Userinfo in the base URL becomes basic authentication unless
// the request already carries an Authorization header.
func (g *gen) roundTrip(req *http.Request) (*http.Response, error) {
	if u := req.URL.User; u != nil && req.Header.Get("Authorization") == "" {
		p, _ := u.Password()
		req.SetBasicAuth(u.Username(), p)
	}
	rt := g.client.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	return rt.RoundTrip(req)
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
	// A POST with no GetBody is never replayed by the transport.
	req.GetBody = nil
	return req, nil
}

// Ceiling on one scanned line and on the data fields assembled into one event.
const eventLimit = 1 << 20

// Bytes read after [DONE] before the body is closed unread.
const drainLimit = 4096

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

// replicaFrom returns the mock's X-Percentes-Replica value when it is
// hostname-shaped and the client holds no credential.
func replicaFrom(h http.Header, mock bool, baseURL, apiKey string) string {
	v := h.Get("X-Percentes-Replica")
	if !mock || v == "" || !hostnameShaped(v) || apiKey != "" {
		return ""
	}
	if u, err := url.Parse(baseURL); err != nil || u.User != nil {
		return ""
	}
	return v
}

// hostnameShaped accepts 1 to 253 characters from a-z, A-Z, 0-9, hyphen and dot.
func hostnameShaped(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
