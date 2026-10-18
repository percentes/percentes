package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/redact"
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
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			r.PeerAddr, r.ConnReused = info.Conn.RemoteAddr().String(), info.Reused
		},
	})

	req, err := g.newRequest(ctx, g.requestBody(r))
	if err != nil {
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrConnect, g.now()
		r.ErrText = redact.ErrorText(err, config.PinnedClientTimeoutS*time.Second)
		return
	}

	resp, err := g.roundTrip(req)
	if err != nil {
		g.classifyTransportErr(r, err)
		return
	}
	defer resp.Body.Close()
	r.Proto = resp.Proto
	r.Status = resp.StatusCode
	secrets := g.secrets()
	r.Headers = allowedHeaders(resp.Header, g.cfg.Target.ResponseHeaders, secrets)
	// Only the mock's replica header is recorded (§1); any other endpoint controls the value.
	r.Replica = replicaFrom(resp.Header, g.cfg.Mock != nil && !g.cfg.Target.Hosted, g.cfg.Target.BaseURL, g.apiKey)

	if resp.StatusCode != http.StatusOK {
		class := ErrStatusOther
		// Status 429 is classified apart from other non-200 statuses (§3).
		if resp.StatusCode == http.StatusTooManyRequests {
			class = ErrStatus429
		}
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, class, g.now()
		stop := time.AfterFunc(errorBodyWait, cancel)
		defer stop.Stop()
		r.ErrorBody = readErrorBody(resp.Body, secrets)
		return
	}

	shape := g.cfg.Target.Shape()
	keepText := g.cfg.Target.Hosted
	var content, reasoning strings.Builder
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
		c, ok := parseChunk(payload, shape)
		if !ok {
			malformed = true
			return true
		}
		g.recordChunk(r, &c, secrets)
		if c.reasoning != "" && keepText {
			reasoning.WriteString(c.reasoning)
		}
		if c.content != "" {
			r.Tokens++
			now := g.now()
			if r.FirstTokNs == 0 {
				r.FirstTokNs = now
			} else {
				r.ITLsUs = append(r.ITLsUs, (now-prevTokNs)/1000)
			}
			prevTokNs = now
			if keepText {
				content.WriteString(c.content)
			}
		}
		return false
	})
	r.Content, r.Reasoning = content.String(), reasoning.String()
	switch {
	case err != nil:
		// A read error, or a line over the bound, leaves the partial event untrusted.
		g.classifyStreamErr(r, err)
	case malformed || dropped > 0 || !sawDone:
		// An undecodable or oversized event, or a stream that ended before
		// [DONE]: malformed stream (§3).
		r.Outcome, r.ErrClass, r.DoneNs = OutcomeErrored, ErrMalformedStream, g.now()
	case r.StreamError != "" && g.cfg.Target.Hosted:
		// An in-band error object anywhere in a hosted stream.
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

// recordChunk keeps a chunk's usage, finish reason, reported model and
// any mismatch with the requested one, system fingerprint, in-band error
// and reasoning arrival on r.
func (g *gen) recordChunk(r *Request, c *chunk, secrets []string) {
	if !c.known {
		r.UnknownEvents++
	}
	if c.usage != nil {
		r.Usage = c.usage
		if n, ok := completionTokens(c.usage); ok {
			r.CompletionTokens, r.UsageSeen = n, true
		}
	}
	if c.finish != "" {
		r.FinishReason = c.finish
	}
	if c.model != "" && r.ReportedModel == "" {
		r.ReportedModel = c.model
	}
	if c.fingerprint != "" && r.SystemFingerprint == "" {
		r.SystemFingerprint = c.fingerprint
	}
	if c.model != "" && c.model != g.model &&
		len(r.ModelMismatch) < modelMismatchLimit && !slices.Contains(r.ModelMismatch, c.model) {
		r.ModelMismatch = append(r.ModelMismatch, c.model)
	}
	if c.err != nil && r.StreamError == "" {
		var b bytes.Buffer
		json.Compact(&b, c.err) //nolint:errcheck
		r.StreamError = string(readErrorBody(&b, secrets))
	}
	if c.reasoning != "" {
		r.ReasoningChunks++
		if r.FirstReasoningNs == 0 {
			r.FirstReasoningNs = g.now()
		}
	}
}

// chunk is one stream event read through a target's response shape.
type chunk struct {
	content, reasoning string
	finish, model      string
	fingerprint        string
	usage, err         json.RawMessage
	// known: the event carried choices, a usage object or an error.
	known bool
}

// parseChunk decodes one chat-completion chunk. ok is false when the
// payload is not a JSON object or null, or when any choice, delta, or
// content or reasoning value has the wrong type. Field names match
// exactly.
func parseChunk(payload []byte, sh config.ResponseShape) (c chunk, ok bool) {
	var root map[string]json.RawMessage
	if json.Unmarshal(payload, &root) != nil {
		return c, false
	}
	if raw, has := root["choices"]; has {
		c.known = true
		var choices []map[string]json.RawMessage
		if json.Unmarshal(raw, &choices) != nil {
			return c, false
		}
		for i, choice := range choices {
			var delta map[string]json.RawMessage
			if json.Unmarshal(orNull(choice["delta"]), &delta) != nil {
				return c, false
			}
			var content, reasoning string
			if !stringField(delta[sh.Content], &content) {
				return c, false
			}
			for _, k := range sh.Reasoning {
				var s string
				if !stringField(delta[k], &s) {
					return c, false
				}
				reasoning += s
			}
			if i == 0 {
				c.content, c.reasoning = content, reasoning
				json.Unmarshal(orNull(choice["finish_reason"]), &c.finish) //nolint:errcheck
			}
		}
	}
	json.Unmarshal(orNull(lookup(root, sh.Model)), &c.model)           //nolint:errcheck
	json.Unmarshal(orNull(root["system_fingerprint"]), &c.fingerprint) //nolint:errcheck
	for _, p := range sh.Usage {
		if v := lookup(root, p); v != nil {
			c.usage, c.known = v, true
		}
	}
	for _, p := range sh.Error {
		if v := lookup(root, p); v != nil && c.err == nil {
			c.err, c.known = v, true
		}
	}
	return c, true
}

// lookup returns the non-null value at path, or nil.
func lookup(root map[string]json.RawMessage, path string) json.RawMessage {
	segs := strings.Split(path, ".")
	v := root[segs[0]]
	for _, seg := range segs[1:] {
		if v == nil {
			return nil
		}
		if i, err := strconv.Atoi(seg); err == nil {
			var a []json.RawMessage
			if json.Unmarshal(v, &a) != nil || i < 0 || i >= len(a) {
				return nil
			}
			v = a[i]
			continue
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(v, &m) != nil {
			return nil
		}
		v = m[seg]
	}
	if v == nil || string(v) == "null" {
		return nil
	}
	return v
}

// stringField decodes a string or null into s; it is false for any other
// type.
func stringField(raw json.RawMessage, s *string) bool {
	return json.Unmarshal(orNull(raw), s) == nil
}

func orNull(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return json.RawMessage("null")
	}
	return raw
}

// ContentDelta decodes one chat-completion chunk and returns the first
// choice's delta content; ok is false for a payload that does not decode.
func ContentDelta(payload []byte) (content string, ok bool) {
	c, ok := parseChunk(payload, (&config.Target{}).Shape())
	return c.content, ok
}

// usageTokens returns the completion token count of a chunk's top-level
// usage object, when the chunk carries one.
func usageTokens(payload []byte) (n int, ok bool) {
	c, ok := parseChunk(payload, (&config.Target{}).Shape())
	if !ok || c.usage == nil {
		return 0, false
	}
	return completionTokens(c.usage)
}

// completionTokens reads completion_tokens from a usage object.
func completionTokens(usage json.RawMessage) (n int, ok bool) {
	var u struct {
		CompletionTokens int `json:"completion_tokens"`
	}
	if json.Unmarshal(usage, &u) != nil {
		return 0, false
	}
	return u.CompletionTokens, true
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

// Distinct mismatched model ids kept per request.
const modelMismatchLimit = 4

// Bytes of a non-200 body kept, and how long after the status the read
// may run.
const (
	errorBodyLimit = 4096
	errorBodyWait  = time.Second
)

func (g *gen) classifyTransportErr(r *Request, err error) {
	r.DoneNs = g.now()
	r.ErrText = redact.ErrorText(err, config.PinnedClientTimeoutS*time.Second)
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
	r.ErrText = redact.ErrorText(err, config.PinnedClientTimeoutS*time.Second)
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

// secrets are the credential values a recorded header or body must not
// carry: the bearer token and the base URL's userinfo and query values,
// longest first, so a shorter secret never splits a longer one's match.
func (g *gen) secrets() []string {
	s := redact.Secrets(g.cfg.Target.BaseURL)
	if g.apiKey != "" {
		s = append(s, g.apiKey)
	}
	sort.Slice(s, func(i, j int) bool { return len(s[i]) > len(s[j]) })
	return s
}

// allowedHeaders copies the headers the allowlist names, dropping any
// value that contains a secret.
func allowedHeaders(h http.Header, allow []string, secrets []string) map[string][]string {
	var out map[string][]string
	for name, vs := range h {
		if !config.HeaderAllowed(allow, name) {
			continue
		}
		for _, v := range vs {
			if containsAny(v, secrets) {
				continue
			}
			if out == nil {
				out = map[string][]string{}
			}
			out[name] = append(out[name], v)
		}
	}
	return out
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if sub != "" && strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// readErrorBody reads up to errorBodyLimit bytes, plus enough to see
// whole any secret that starts inside them, masks each secret with
// asterisks of the same length, and keeps the first errorBodyLimit bytes.
// secrets must be ordered longest first. A read cut short by the request
// context keeps what arrived.
func readErrorBody(body io.Reader, secrets []string) []byte {
	extra := 0
	if len(secrets) > 0 {
		extra = len(secrets[0])
	}
	b, _ := io.ReadAll(io.LimitReader(body, int64(errorBodyLimit+extra)))
	for _, s := range secrets {
		if s != "" {
			b = bytes.ReplaceAll(b, []byte(s), bytes.Repeat([]byte("*"), len(s)))
		}
	}
	if len(b) > errorBodyLimit {
		b = b[:errorBodyLimit]
	}
	return b
}
