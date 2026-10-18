package mock

import (
	"errors"
	"fmt"

	"github.com/percentes/percentes/internal/config"
)

// UsageMode selects when a shaped stream sends its usage-only chunk.
type UsageMode string

const (
	// UsageOnRequest sends it when the request sets
	// stream_options.include_usage.
	UsageOnRequest UsageMode = ""
	UsageAlways    UsageMode = "always"
	UsageNever     UsageMode = "never"
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
	case sh.ReasoningTokens > 0 && sh.ReasoningKey == "":
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
	case UsageOnRequest, UsageAlways, UsageNever:
	default:
		return fmt.Errorf("unknown usage mode %q", sh.Usage)
	}
	return nil
}

// piece is one streamed delta: its key and text.
type piece struct {
	key, text string
}

// streamPlan is what one request streams: the pieces, then either an
// in-band error or the finish and usage chunks.
type streamPlan struct {
	pieces []piece
	finish string // empty: no finish chunk
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

	var pieces []piece
	for i := 1; i <= sh.ReasoningTokens; i++ {
		pieces = append(pieces, piece{sh.ReasoningKey, fmt.Sprintf("r%d ", i)})
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
		p.finish = "length"
	case sh.Error != nil:
		p.err = sh.Error
		return p
	case !sh.OmitFinish:
		p.finish = sh.FinishReason
		if p.finish == "" {
			p.finish = "stop"
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
