package mock

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// mevent is one Messages stream event: its SSE event name and decoded data.
type mevent struct {
	name string
	data map[string]any
}

func (e mevent) typ() string { s, _ := e.data["type"].(string); return s }

func (e mevent) delta() map[string]any { d, _ := e.data["delta"].(map[string]any); return d }

func messagesBody(content string, maxTokens int) string {
	raw, _ := json.Marshal(map[string]any{
		"model":    "claude-sonnet-5-5",
		"system":   "Summarise.",
		"messages": []map[string]string{{"role": "user", "content": content}},
		"stream":   true, "max_tokens": maxTokens,
		"thinking": map[string]any{"type": "between_tools"}, "output_config": map[string]any{"effort": "low"},
	})
	return string(raw)
}

// postMessages posts body to base+path with hdr and returns the status, the
// decoded body of a non-200 response, and the stream's events ([DONE] skipped).
func postMessages(t *testing.T, base, path, body string, hdr map[string]string) (int, map[string]any, []mevent) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e) //nolint:errcheck
		return resp.StatusCode, e, nil
	}
	var events []mevent
	var name string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if v, ok := strings.CutPrefix(line, "event: "); ok {
			name = v
			continue
		}
		v, ok := strings.CutPrefix(line, "data: ")
		if !ok || v == "[DONE]" {
			continue
		}
		var d map[string]any
		if err := json.Unmarshal([]byte(v), &d); err != nil {
			t.Fatalf("event data %q: %v", v, err)
		}
		events = append(events, mevent{name, d})
		name = ""
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, nil, events
}

var versionHeader = map[string]string{"anthropic-version": config.MessagesVersion}

func streamMessages(t *testing.T, sh Shape, maxTokens int) []mevent {
	t.Helper()
	sh.Messages = true
	status, e, events := postMessages(t, startShaped(t, sh), "/v1/messages", messagesBody("x", maxTokens), versionHeader)
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, e)
	}
	for i, ev := range events {
		if ev.name == "" || ev.name != ev.typ() {
			t.Fatalf("event %d: name %q, data type %q", i, ev.name, ev.typ())
		}
	}
	return events
}

func types(events []mevent) string {
	var out []string
	for _, e := range events {
		t := e.typ()
		if d := e.delta(); t == "content_block_delta" {
			t, _ = d["type"].(string)
		}
		out = append(out, t)
	}
	return strings.Join(out, " ")
}

// messageDelta returns the message_delta event data, or nil.
func messageDelta(events []mevent) map[string]any {
	for _, e := range events {
		if e.typ() == "message_delta" {
			return e.data
		}
	}
	return nil
}

func TestMessagesShapedStream(t *testing.T) {
	sh := Shape{Messages: true, ReasoningTokens: 2, Answer: "The customer was billed."}
	for _, path := range []string{"/v1/messages", "/anthropic/v1/messages"} {
		status, e, events := postMessages(t, startShaped(t, sh), path, messagesBody("x", 512), versionHeader)
		if status != http.StatusOK {
			t.Fatalf("%s: status %d: %v", path, status, e)
		}
		want := "message_start content_block_start thinking_delta thinking_delta signature_delta content_block_stop " +
			"content_block_start text_delta text_delta text_delta text_delta text_delta text_delta content_block_stop message_delta message_stop"
		if got := types(events); got != want {
			t.Fatalf("%s: events\n%s\nwant\n%s", path, got, want)
		}
		msg, _ := events[0].data["message"].(map[string]any)
		if msg["model"] != "claude-sonnet-5-5" || msg["id"] != "msg_mock_1" {
			t.Fatalf("%s: message_start %v", path, msg)
		}
		var thinking, text string
		for _, ev := range events {
			if d := ev.delta(); d != nil {
				s1, _ := d["thinking"].(string)
				s2, _ := d["text"].(string)
				thinking, text = thinking+s1, text+s2
			}
		}
		if thinking != "r1 r2 " || text != "The customer was billed." {
			t.Fatalf("%s: thinking %q text %q", path, thinking, text)
		}
		if b, _ := events[1].data["content_block"].(map[string]any); b["type"] != "thinking" || events[1].data["index"] != float64(0) {
			t.Fatalf("%s: first block %v", path, events[1].data)
		}
		if b, _ := events[6].data["content_block"].(map[string]any); b["type"] != "text" || events[6].data["index"] != float64(1) {
			t.Fatalf("%s: second block %v", path, events[6].data)
		}
		md := messageDelta(events)
		if md["delta"].(map[string]any)["stop_reason"] != "end_turn" || md["usage"].(map[string]any)["output_tokens"] != float64(8) {
			t.Fatalf("%s: message_delta %v", path, md)
		}
	}
	if code, _ := postJSON(t, startShaped(t, sh)+"/v1/chat/completions", messagesBody("x", 512)); code != http.StatusNotFound {
		t.Fatalf("chat path on a messages shape: %d", code)
	}
}

// Cut, empty, ceiling, in-band error and no finish reason.
func TestMessagesShapedModes(t *testing.T) {
	overloaded := map[string]any{"type": "overloaded_error", "message": "Overloaded"}
	for _, tc := range []struct {
		name      string
		sh        Shape
		maxTokens int
		want      string
		stop      any
	}{
		{"cut", Shape{Answer: "abcd", Cut: true}, 512,
			"message_start content_block_start text_delta content_block_stop message_delta", "end_turn"},
		{"empty", Shape{}, 512, "message_start message_delta message_stop", "end_turn"},
		{"reasoning only at the ceiling", Shape{ReasoningTokens: 5, Answer: "abcd"}, 3,
			"message_start content_block_start thinking_delta thinking_delta thinking_delta signature_delta content_block_stop message_delta message_stop", "max_tokens"},
		{"error after content", Shape{Answer: "abcdefgh", Error: overloaded, ErrorAfterContent: 1}, 512,
			"message_start content_block_start text_delta content_block_stop error message_stop", nil},
		{"error before content, cut", Shape{Answer: "abcd", Error: overloaded, Cut: true}, 512,
			"message_start error", nil},
		{"no finish reason", Shape{Answer: "abcd", OmitFinish: true}, 512,
			"message_start content_block_start text_delta content_block_stop message_delta message_stop", nil},
		{"finish reason set", Shape{Answer: "abcd", FinishReason: "refusal"}, 512,
			"message_start content_block_start text_delta content_block_stop message_delta message_stop", "refusal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := streamMessages(t, tc.sh, tc.maxTokens)
			if got := types(events); got != tc.want {
				t.Fatalf("events\n%s\nwant\n%s", got, tc.want)
			}
			md := messageDelta(events)
			if md == nil {
				for _, e := range events {
					if e.typ() == "error" && e.data["error"].(map[string]any)["type"] != "overloaded_error" {
						t.Fatalf("error event %v", e.data)
					}
				}
				return
			}
			d := md["delta"].(map[string]any)
			if stop, ok := d["stop_reason"]; !ok || stop != tc.stop {
				t.Fatalf("stop_reason %v (present %v), want %v", stop, ok, tc.stop)
			}
		})
	}
}

func TestMessagesShapedUsagePlacement(t *testing.T) {
	for _, tc := range []struct {
		mode         UsageMode
		start, delta bool
	}{
		{UsageOnRequest, true, true},
		{UsageAlways, true, true},
		{UsageStart, true, false},
		{UsageDelta, false, true},
		{UsageNever, false, false},
	} {
		events := streamMessages(t, Shape{Answer: "abcdefgh", ReasoningTokens: 1, Usage: tc.mode}, 512)
		msg, _ := events[0].data["message"].(map[string]any)
		su, hasStart := msg["usage"].(map[string]any)
		du, hasDelta := messageDelta(events)["usage"].(map[string]any)
		if hasStart != tc.start || hasDelta != tc.delta {
			t.Fatalf("%q: usage in message_start %v, in message_delta %v", tc.mode, hasStart, hasDelta)
		}
		if hasStart && (su["input_tokens"] != float64(1) || su["output_tokens"] != float64(0)) {
			t.Fatalf("%q: message_start usage %v", tc.mode, su)
		}
		if hasDelta && du["output_tokens"] != float64(3) {
			t.Fatalf("%q: message_delta usage %v", tc.mode, du)
		}
	}
}

// The key must arrive in the one header the shape names; anthropic-version
// is required on a Messages shape; refusals use the shape's error form.
func TestShapedCredentialHeader(t *testing.T) {
	const key = "sk-mock-key"
	for _, tc := range []struct {
		name     string
		sh       Shape
		hdr      map[string]string
		status   int
		errType  string // Messages error type; empty for the chat form
		errorMsg bool
	}{
		{"x-api-key", Shape{Messages: true, Key: key, AuthHeader: config.AuthXAPIKey},
			map[string]string{"X-Api-Key": key}, http.StatusOK, "", false},
		{"bearer sent, x-api-key named", Shape{Messages: true, Key: key, AuthHeader: config.AuthXAPIKey},
			map[string]string{"Authorization": "Bearer " + key}, http.StatusUnauthorized, "authentication_error", false},
		{"both headers", Shape{Messages: true, Key: key, AuthHeader: config.AuthXAPIKey},
			map[string]string{"X-Api-Key": key, "Authorization": "Bearer " + key}, http.StatusUnauthorized, "authentication_error", false},
		{"wrong key", Shape{Messages: true, Key: key},
			map[string]string{"Authorization": "Bearer other"}, http.StatusUnauthorized, "authentication_error", false},
		{"bearer default", Shape{Messages: true, Key: key},
			map[string]string{"Authorization": "Bearer " + key}, http.StatusOK, "", false},
		{"no key header", Shape{Messages: true, Key: key}, map[string]string{}, http.StatusUnauthorized, "authentication_error", false},
		{"no anthropic-version", Shape{Messages: true}, map[string]string{"anthropic-version": ""}, http.StatusBadRequest, "invalid_request_error", false},
		{"other anthropic-version", Shape{Messages: true}, map[string]string{"anthropic-version": "2023-01-01"}, http.StatusBadRequest, "invalid_request_error", false},
		{"chat api-key", Shape{Answer: "a", Key: key, AuthHeader: config.AuthAPIKey},
			map[string]string{"Api-Key": key}, http.StatusOK, "", false},
		{"chat api-key sent as bearer", Shape{Answer: "a", Key: key, AuthHeader: config.AuthAPIKey},
			map[string]string{"Authorization": "Bearer " + key}, http.StatusUnauthorized, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, body := "/v1/chat/completions", messagesBody("x", 512)
			hdr := map[string]string{}
			if tc.sh.Messages {
				path, hdr["anthropic-version"] = "/v1/messages", config.MessagesVersion
			}
			for k, v := range tc.hdr {
				hdr[k] = v
			}
			status, e, events := postMessages(t, startShaped(t, tc.sh), path, body, hdr)
			if status != tc.status {
				t.Fatalf("status %d, want %d: %v", status, tc.status, e)
			}
			switch {
			case status == http.StatusOK:
				if len(events) == 0 {
					t.Fatal("no events")
				}
			case tc.errType != "":
				inner, _ := e["error"].(map[string]any)
				if e["type"] != "error" || inner["type"] != tc.errType || inner["message"] == "" {
					t.Fatalf("error body %v", e)
				}
			case tc.errorMsg:
				if _, ok := e["error"].(string); !ok {
					t.Fatalf("chat error body %v", e)
				}
			}
		})
	}
}

// An injected throttle on a Messages shape answers in the Messages error
// form.
func TestMessagesShapedInjectedFault(t *testing.T) {
	base := startShaped(t, Shape{Messages: true, Answer: "a"})
	if _, code := armFault(t, base, adminArmRequest{Mode: config.MockFaultThrottle, DurationS: 60}); code != http.StatusCreated {
		t.Fatalf("arm: %d", code)
	}
	status, e, _ := postMessages(t, base, "/v1/messages", messagesBody("x", 512), versionHeader)
	inner, _ := e["error"].(map[string]any)
	if status != http.StatusTooManyRequests || e["type"] != "error" || inner["type"] != "rate_limit_error" {
		t.Fatalf("status %d body %v", status, e)
	}
}

func TestShapedChatCut(t *testing.T) {
	base := startShaped(t, Shape{Answer: `{"route":"billing"}`, Usage: UsageAlways, Cut: true})
	frames, done := readFrames(t, base, pilotBody("x", 4096))
	if done || len(frames) == 0 {
		t.Fatalf("done=%v frames=%d", done, len(frames))
	}
	if _, ok := frames[len(frames)-1]["usage"]; !ok {
		t.Fatalf("last frame %v, want usage", frames[len(frames)-1])
	}
}
