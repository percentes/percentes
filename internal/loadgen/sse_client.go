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

	body := fmt.Sprintf(
		`{"model":"percentes-mock","messages":[{"role":"user","content":"cs-%d-%d %s"}],"stream":true,"max_tokens":%d,"ignore_eos":true}`,
		g.cfg.Run.Seed, r.Index, g.filler, g.cfg.Load.MaxTokens)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.Target.BaseURL+"/v1/chat/completions", bytes.NewReader([]byte(body)))
	if err != nil {
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrConnect, g.now()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	// POSTs are not replayable by net/http (non-idempotent, no idempotency
	// key), so the transport never silently retries; belt-and-braces:
	req.GetBody = nil

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
