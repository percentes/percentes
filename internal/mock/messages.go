package mock

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// thinkingKey is the piece key of a Messages shape's reasoning.
const thinkingKey = "thinking"

// mockSignature is the signature_delta that closes a thinking block.
const mockSignature = "mock"

// messagesWriter writes a Messages stream: message_start before the first
// event, one content block per run of pieces with the same key, then
// message_delta (or the error event) and message_stop.
type messagesWriter struct {
	w            http.ResponseWriter
	rc           *http.ResponseController
	id, model    string
	usage        UsageMode
	prompt       int
	cut          bool
	started      bool
	index        int    // index of the open block
	open         string // key of the open block; empty: none
	outputTokens int
}

func (m *messagesWriter) event(v map[string]any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(m.w, "event: %s\ndata: %s\n\n", v["type"], raw); err != nil {
		return err
	}
	return m.rc.Flush()
}

func (m *messagesWriter) start() error {
	if m.started {
		return nil
	}
	m.started = true
	msg := map[string]any{
		"id": m.id, "type": "message", "role": "assistant", "content": []any{}, "model": m.model,
		"stop_reason": nil, "stop_sequence": nil,
	}
	if m.usage != UsageNever && m.usage != UsageDelta {
		msg["usage"] = map[string]any{"input_tokens": m.prompt, "output_tokens": 0}
	}
	return m.event(map[string]any{"type": "message_start", "message": msg})
}

func (m *messagesWriter) closeBlock() error {
	if m.open == "" {
		return nil
	}
	if m.open == thinkingKey {
		if err := m.event(map[string]any{"type": "content_block_delta", "index": m.index,
			"delta": map[string]any{"type": "signature_delta", "signature": mockSignature}}); err != nil {
			return err
		}
	}
	m.open = ""
	err := m.event(map[string]any{"type": "content_block_stop", "index": m.index})
	m.index++
	return err
}

func (m *messagesWriter) piece(p piece) error {
	if err := m.start(); err != nil {
		return err
	}
	block, delta := "text", "text_delta"
	if p.key == thinkingKey {
		block, delta = "thinking", "thinking_delta"
	}
	if p.key != m.open {
		if err := m.closeBlock(); err != nil {
			return err
		}
		m.open = p.key
		if err := m.event(map[string]any{"type": "content_block_start", "index": m.index,
			"content_block": map[string]any{"type": block, block: ""}}); err != nil {
			return err
		}
	}
	m.outputTokens++
	return m.event(map[string]any{"type": "content_block_delta", "index": m.index,
		"delta": map[string]any{"type": delta, block: p.text}})
}

func (m *messagesWriter) end(p streamPlan) error {
	if err := m.start(); err != nil {
		return err
	}
	if err := m.closeBlock(); err != nil {
		return err
	}
	if p.err != nil {
		if err := m.event(map[string]any{"type": "error", "error": p.err}); err != nil {
			return err
		}
	} else {
		var stop any
		if p.finish != "" {
			stop = p.finish
		}
		ev := map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": stop, "stop_sequence": nil}}
		if m.usage != UsageNever && m.usage != UsageStart {
			ev["usage"] = map[string]any{"output_tokens": m.outputTokens}
		}
		if err := m.event(ev); err != nil {
			return err
		}
	}
	if m.cut {
		return nil
	}
	return m.event(map[string]any{"type": "message_stop"})
}

// messagesErrorTypes maps a status to its Messages error type; any other
// status is "api_error".
var messagesErrorTypes = map[int]string{
	http.StatusBadRequest:      "invalid_request_error",
	http.StatusUnauthorized:    "authentication_error",
	http.StatusTooManyRequests: "rate_limit_error",
	529:                        "overloaded_error",
}

// writeError writes a non-200 response in the shape's error form.
func (s *Server) writeError(w http.ResponseWriter, status int, msg string) {
	if !s.messages() {
		writeJSON(w, status, map[string]any{"error": msg})
		return
	}
	typ, ok := messagesErrorTypes[status]
	if !ok {
		typ = "api_error"
	}
	writeJSON(w, status, map[string]any{"type": "error", "error": map[string]any{"type": typ, "message": msg}})
}

func (s *Server) messages() bool { return s.shape != nil && s.shape.Messages }
