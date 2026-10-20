package mock

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/percentes/percentes/internal/config"
)

// UsageMode selects when a shaped stream sends its usage-only chunk. On
// a Messages shape usage is unrequested: UsageOnRequest and UsageAlways
// place it in message_start and message_delta, UsageStart and UsageDelta
// in one of them, UsageNever in neither.
type UsageMode string

const (
	// UsageOnRequest sends it when the request sets
	// stream_options.include_usage.
	UsageOnRequest UsageMode = ""
	UsageAlways    UsageMode = "always"
	UsageNever     UsageMode = "never"
	// UsageStart and UsageDelta apply to a Messages shape only.
	UsageStart UsageMode = "start"
	UsageDelta UsageMode = "delta"
)

// unknownKey is the chunk and delta key a shaped stream adds when
// Shape.UnknownKeys is set.
const unknownKey = "x_mock_unknown"

// answerPieceRunes is the rune length of each content chunk of a routed
// answer; the last chunk may be shorter.
const answerPieceRunes = 4

// Shape is the hosted-provider stream form of a server built with
// NewShaped. Reasoning chunks come first, then the routed answer as
// content chunks, then the finish chunk, the usage-only chunk and
// [DONE]. The request's max_completion_tokens (or max_tokens when
// absent) caps reasoning and content chunks together; a capped stream
// ends with finish_reason "length". ignore_eos is not honoured.
type Shape struct {
	// ReasoningKey is the delta key that carries reasoning text.
	ReasoningKey    string
	ReasoningTokens int

	// Routes maps the last message's content to the answer text. Content
	// not in Routes is answered with Answer; an empty answer sends no
	// content chunk.
	Routes map[string]string
	Answer string

	// FinishReason is the reason on the finish chunk, "stop" when empty.
	// OmitFinish sends no finish chunk.
	FinishReason string
	OmitFinish   bool

	Usage UsageMode

	// Error, when non-nil, is sent as {"error": Error} after
	// ErrorAfterContent content chunks, in place of the finish and usage
	// chunks, and is followed by [DONE].
	Error             map[string]any
	ErrorAfterContent int

	// UnknownKeys adds x_mock_unknown to every chunk and every delta.
	UnknownKeys bool

	// Messages serves the Anthropic Messages stream at /v1/messages and
	// /anthropic/v1/messages in place of /v1/chat/completions: message_start,
	// reasoning as a thinking block, the answer as a text block, then
	// message_delta and message_stop. The ceiling is max_tokens.
	// FinishReason is the stop_reason, "end_turn" when empty and
	// "max_tokens" when capped; OmitFinish sends stop_reason null. An
	// Error replaces message_delta. A request without anthropic-version
	// config.MessagesVersion is refused with 400.
	Messages bool

	// Cut ends the body cleanly without the terminator.
	Cut bool

	// Key, when set, must arrive in the header AuthHeader names (a
	// target.auth_header value; bearer when empty) and in no other
	// credential header, or the request is refused with 401.
	Key        string
	AuthHeader string

	// Model, when set, is the model id every response reports in place
	// of the request's.
	Model string
}

// NewShaped builds a server that answers in the form sh describes. Fault
// modes, pacing and slow reload behave as for New.
func NewShaped(cfg config.Mock, sh Shape) *Server {
	s := New(cfg)
	s.shape = &sh
	return s
}

func (sh *Shape) validate() error {
	switch {
	case sh.ReasoningTokens < 0:
		return errors.New("reasoning tokens must be >= 0")
	case sh.Messages && sh.ReasoningKey != "":
		return errors.New("a messages shape sends reasoning in a thinking block, not under a reasoning key")
	case sh.Messages && sh.UnknownKeys:
		return errors.New("unknown keys apply to the chat shape only")
	case !sh.Messages && (sh.Usage == UsageStart || sh.Usage == UsageDelta):
		return fmt.Errorf("usage mode %q applies to a messages shape only", sh.Usage)
	case sh.AuthHeader != "" && sh.Key == "":
		return errors.New("auth header set without a key")
	case sh.ReasoningTokens > 0 && sh.ReasoningKey == "" && !sh.Messages:
		return errors.New("reasoning tokens need a reasoning key")
	case sh.ReasoningKey == "content":
		return errors.New(`reasoning key must not be "content"`)
	case sh.OmitFinish && sh.FinishReason != "":
		return errors.New("finish reason set with the finish chunk omitted")
	case sh.ErrorAfterContent < 0:
		return errors.New("error after content must be >= 0")
	case sh.ErrorAfterContent > 0 && sh.Error == nil:
		return errors.New("error after content set without an error object")
	}
	switch sh.Usage {
	case UsageOnRequest, UsageAlways, UsageNever, UsageStart, UsageDelta:
	default:
		return fmt.Errorf("unknown usage mode %q", sh.Usage)
	}
	switch sh.AuthHeader {
	case "", config.AuthBearer, config.AuthXAPIKey, config.AuthAPIKey:
	default:
		return fmt.Errorf("unknown auth header %q", sh.AuthHeader)
	}
	return nil
}

// credentialHeaders are the headers a key can travel in.
var credentialHeaders = []string{"Authorization", "X-Api-Key", "Api-Key"}

// keyArrived reports whether h carries sh.Key in exactly the one header
// sh.AuthHeader names and in no other credential header; true when no
// key is set.
func (sh *Shape) keyArrived(h http.Header) bool {
	if sh == nil || sh.Key == "" {
		return true
	}
	name, value := (&config.Target{AuthHeader: sh.AuthHeader}).KeyHeader(sh.Key)
	for _, k := range credentialHeaders {
		v := h.Values(k)
		if k == name && (len(v) != 1 || v[0] != value) || k != name && len(v) != 0 {
			return false
		}
	}
	return true
}

// piece is one streamed delta: its key and text.
type piece struct {
	key, text string
}

// streamPlan is what one request streams: the pieces, then either an
// in-band error or the finish and usage chunks.
type streamPlan struct {
	pieces []piece
	finish string // empty: no finish chunk, or stop_reason null
	usage  bool
	err    map[string]any
}

// plan returns the stream for req. A nil shape gives the Phase 0
// stream: limit content tokens and finish_reason "length".
func (s *Server) plan(req *chatRequest, limit int) streamPlan {
	sh := s.shape
	if sh == nil {
		p := streamPlan{finish: "length", usage: req.StreamOptions.IncludeUsage}
		for i := 1; i <= limit; i++ {
			p.pieces = append(p.pieces, piece{"content", fmt.Sprintf("t%d ", i)})
		}
		return p
	}

	reasoningKey, capped, done := sh.ReasoningKey, "length", "stop"
	if sh.Messages {
		reasoningKey, capped, done = thinkingKey, "max_tokens", "end_turn"
	}
	var pieces []piece
	for i := 1; i <= sh.ReasoningTokens; i++ {
		pieces = append(pieces, piece{reasoningKey, fmt.Sprintf("r%d ", i)})
	}
	answer, ok := sh.Routes[req.Messages[len(req.Messages)-1].Content]
	if !ok {
		answer = sh.Answer
	}
	content := splitRunes(answer, answerPieceRunes)
	if sh.Error != nil {
		content = content[:min(len(content), sh.ErrorAfterContent)]
	}
	for _, c := range content {
		pieces = append(pieces, piece{"content", c})
	}

	p := streamPlan{pieces: pieces}
	switch {
	case len(pieces) > limit:
		p.pieces = pieces[:limit]
		p.finish = capped
	case sh.Error != nil:
		p.err = sh.Error
		return p
	case !sh.OmitFinish:
		p.finish = sh.FinishReason
		if p.finish == "" {
			p.finish = done
		}
	}
	switch sh.Usage {
	case UsageAlways:
		p.usage = true
	case UsageOnRequest:
		p.usage = req.StreamOptions.IncludeUsage
	}
	return p
}

func splitRunes(s string, n int) []string {
	var out []string
	r := []rune(s)
	for len(r) > 0 {
		k := min(n, len(r))
		out = append(out, string(r[:k]))
		r = r[k:]
	}
	return out
}
