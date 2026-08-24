package loadgen

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/config"
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
	r.Replica = resp.Header.Get("X-Percentes-Replica")

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) //nolint:errcheck
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrHTTPStatus, g.now()
		return
	}

	reader := bufio.NewReader(resp.Body)
	var prevTokNs int64
	for {
		line, err := reader.ReadString('\n')
		l := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(l, "data: [DONE]"):
			// [DONE] with no prior content event: an empty stream is
			// errored, never a completion (§3).
			if r.FirstTokNs == 0 {
				r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrEmptyStream, g.now()
				return
			}
			r.Outcome, r.DoneNs = OutcomeCompleted, g.now()
			return
		case strings.HasPrefix(l, "data: ") && strings.Contains(l, `"content"`):
			r.Tokens++
			now := g.now()
			if r.FirstTokNs == 0 {
				r.FirstTokNs = now
			} else {
				r.ITLsUs = append(r.ITLsUs, (now-prevTokNs)/1000)
			}
			prevTokNs = now
		}
		if err != nil {
			g.classifyStreamErr(r, err)
			return
		}
	}
}

// requestBody builds the OpenAI-compatible chat-completion request.
// ignore_eos is a vLLM extension forcing the full max_tokens output
// budget (§6); hosted endpoints reject or ignore it, so a hosted target
// omits it and accepts natural stops.
func (g *gen) requestBody(r *Request) string {
	body := fmt.Sprintf(
		`{"model":%q,"messages":[{"role":"user","content":"cs-%d-%d %s"}],"stream":true,"max_tokens":%d`,
		g.model, g.cfg.Run.Seed, r.Index, g.filler, g.cfg.Load.MaxTokens)
	if !g.cfg.Target.Hosted {
		body += `,"ignore_eos":true`
	}
	return body + "}"
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
