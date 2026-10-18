package mock

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// frame is one SSE data line of a shaped stream, decoded.
type frame map[string]any

func (f frame) delta() map[string]any {
	choices, _ := f["choices"].([]any)
	if len(choices) != 1 {
		return nil
	}
	d, _ := choices[0].(map[string]any)["delta"].(map[string]any)
	return d
}

func (f frame) finish() (string, bool) {
	choices, _ := f["choices"].([]any)
	if len(choices) != 1 {
		return "", false
	}
	r, ok := choices[0].(map[string]any)["finish_reason"].(string)
	return r, ok
}

func startShaped(t *testing.T, sh Shape) string {
	t.Helper()
	cfg := baseMockCfg()
	cfg.TTFT = fixed(1)
	cfg.ITL = fixed(1)
	s := NewShaped(cfg, sh)
	if err := s.Start(); err != nil {
		t.Fatalf("start shaped mock: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return "http://" + s.Addr()
}

// readFrames posts body and returns the decoded data frames and whether
// [DONE] arrived.
func readFrames(t *testing.T, base string, body map[string]any) ([]frame, bool) {
	t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var frames []frame
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		if line == "[DONE]" {
			return frames, true
		}
		var f frame
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Fatalf("frame %q: %v", line, err)
		}
		frames = append(frames, f)
	}
	return frames, false
}

func pilotBody(ticket string, ceiling int) map[string]any {
	return map[string]any{
		"model": "openai/gpt-oss-120b",
		"messages": []map[string]string{
			{"role": "system", "content": "route the ticket"},
			{"role": "user", "content": ticket},
		},
		"stream":                true,
		"max_completion_tokens": ceiling,
		"reasoning_effort":      "low",
	}
}

// split returns the reasoning and content text in stream order and
// whether any content arrived before the last reasoning delta.
func split(frames []frame, reasoningKey string) (reasoning, content string, interleaved bool) {
	for _, f := range frames {
		d := f.delta()
		if r, ok := d[reasoningKey].(string); ok {
			if content != "" {
				interleaved = true
			}
			reasoning += r
		}
		if c, ok := d["content"].(string); ok {
			content += c
		}
	}
	return
}

func TestShapedReasoningThenRoutedAnswer(t *testing.T) {
	base := startShaped(t, Shape{
		ReasoningKey:    "reasoning",
		ReasoningTokens: 3,
		Routes:          map[string]string{"refund please": `{"route":"billing"}`},
		Answer:          `{"route":"technical"}`,
	})
	frames, done := readFrames(t, base, pilotBody("refund please", 4096))
	reasoning, content, interleaved := split(frames, "reasoning")
	if !done || reasoning != "r1 r2 r3 " || content != `{"route":"billing"}` || interleaved {
		t.Fatalf("done=%v reasoning=%q content=%q interleaved=%v", done, reasoning, content, interleaved)
	}
	last := frames[len(frames)-1]
	if r, ok := last.finish(); !ok || r != "stop" {
		t.Fatalf("last frame %v, want finish_reason stop", last)
	}
	for _, f := range frames {
		if _, ok := f["usage"]; ok {
			t.Fatalf("usage frame without include_usage: %v", f)
		}
	}

	frames, _ = readFrames(t, base, pilotBody("app crashes", 4096))
	if _, content, _ := split(frames, "reasoning"); content != `{"route":"technical"}` {
		t.Fatalf("unrouted ticket answered %q", content)
	}
}

// The ceiling counts reasoning and content together; a reasoning-only
// stream that hits it ends with finish_reason length.
func TestShapedCeilingCoversReasoning(t *testing.T) {
	base := startShaped(t, Shape{
		ReasoningKey:    "reasoning_content",
		ReasoningTokens: 5,
		Answer:          `{"route":"account"}`,
		Usage:           UsageAlways,
	})
	body := pilotBody("x", 0)
	delete(body, "max_completion_tokens")
	body["max_tokens"] = 3
	frames, done := readFrames(t, base, body)
	reasoning, content, _ := split(frames, "reasoning_content")
	if !done || reasoning != "r1 r2 r3 " || content != "" {
		t.Fatalf("done=%v reasoning=%q content=%q", done, reasoning, content)
	}
	if r, ok := frames[len(frames)-2].finish(); !ok || r != "length" {
		t.Fatalf("finish frame %v, want length", frames[len(frames)-2])
	}
	usage, _ := frames[len(frames)-1]["usage"].(map[string]any)
	if usage == nil || usage["completion_tokens"] != float64(3) || len(frames[len(frames)-1]["choices"].([]any)) != 0 {
		t.Fatalf("usage-only frame %v", frames[len(frames)-1])
	}
}

// An in-band error replaces the finish and usage frames and is followed
// by [DONE].
func TestShapedInBandErrorThenDone(t *testing.T) {
	errObj := map[string]any{"message": "upstream failed", "type": "server_error"}
	for _, after := range []int{0, 2} {
		base := startShaped(t, Shape{
			Answer:            `{"route":"escalation"}`,
			Usage:             UsageAlways,
			Error:             errObj,
			ErrorAfterContent: after,
		})
		frames, done := readFrames(t, base, pilotBody("x", 4096))
		_, content, _ := split(frames, "reasoning")
		if want := []string{"", `{"ro`, `{"route"`}[after]; !done || content != want {
			t.Fatalf("after=%d: done=%v content=%q, want %q", after, done, content, want)
		}
		last := frames[len(frames)-1]
		if e, _ := last["error"].(map[string]any); e == nil || e["message"] != "upstream failed" {
			t.Fatalf("after=%d: last frame %v, want the error object", after, last)
		}
		for _, f := range frames {
			if _, ok := f.finish(); ok {
				t.Fatalf("after=%d: finish frame alongside the error: %v", after, f)
			}
			if _, ok := f["usage"]; ok {
				t.Fatalf("after=%d: usage frame alongside the error: %v", after, f)
			}
		}
	}
}

func TestShapedOmitFinishWithUnknownKeys(t *testing.T) {
	base := startShaped(t, Shape{
		ReasoningKey:    "reasoning",
		ReasoningTokens: 1,
		Answer:          `{"route":"billing"}`,
		OmitFinish:      true,
		Usage:           UsageAlways,
		UnknownKeys:     true,
	})
	frames, done := readFrames(t, base, pilotBody("x", 4096))
	if !done || len(frames) == 0 {
		t.Fatalf("done=%v frames=%d", done, len(frames))
	}
	for i, f := range frames {
		if f[unknownKey] != "mock" {
			t.Fatalf("frame %d lacks %s: %v", i, unknownKey, f)
		}
		if _, ok := f.finish(); ok {
			t.Fatalf("frame %d carries a finish reason: %v", i, f)
		}
		if d := f.delta(); d != nil && d[unknownKey] != "mock" {
			t.Fatalf("delta %d lacks %s: %v", i, unknownKey, d)
		}
	}
	if _, ok := frames[len(frames)-1]["usage"]; !ok {
		t.Fatalf("last frame %v, want usage", frames[len(frames)-1])
	}
}

func TestShapedUsageNeverIgnoresTheRequest(t *testing.T) {
	base := startShaped(t, Shape{Answer: "a", Usage: UsageNever})
	body := pilotBody("x", 4096)
	body["stream_options"] = map[string]any{"include_usage": true}
	frames, done := readFrames(t, base, body)
	if !done {
		t.Fatal("no [DONE]")
	}
	for _, f := range frames {
		if _, ok := f["usage"]; ok {
			t.Fatalf("usage frame under UsageNever: %v", f)
		}
	}
}

func TestShapeRefusedAtStart(t *testing.T) {
	for name, sh := range map[string]Shape{
		"reasoning without key": {ReasoningTokens: 1},
		"reasoning as content":  {ReasoningKey: "content", ReasoningTokens: 1},
		"negative reasoning":    {ReasoningKey: "reasoning", ReasoningTokens: -1},
		"finish and omit":       {FinishReason: "stop", OmitFinish: true},
		"error offset alone":    {ErrorAfterContent: 1},
		"unknown usage mode":    {Usage: "sometimes"},
	} {
		s := NewShaped(baseMockCfg(), sh)
		if err := s.Start(); err == nil {
			s.Close()
			t.Errorf("%s: started", name)
		}
	}
}
