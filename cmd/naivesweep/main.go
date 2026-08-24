// Command naivesweep is a deliberately naive closed-loop sweep of an
// OpenAI-compatible endpoint. It flags any response that is HTTP 200 yet
// carries no usable content and gives no stop reason for it.
//
// Standalone reconnaissance. This is not instrument code and its numbers do
// not publish as a characterization: it applies none of the client-validity
// gates SPEC.md requires, so it cannot show whether the client was the
// bottleneck.
//
//	SWEEP_ENDPOINT=https://<host>/v1/chat/completions \
//	SWEEP_API_KEY=<key> SWEEP_MODEL=<model-id> go run ./cmd/naivesweep
//
// SWEEP_TOTAL and SWEEP_CONCURRENCY override the 300/6 defaults.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Headers a sweep reports, so a refusal can be read against the quota it spent.
var rateHeaders = []string{
	"retry-after",
	"x-ratelimit-limit-requests", "x-ratelimit-remaining-requests", "x-ratelimit-reset-requests",
	"x-ratelimit-limit-tokens", "x-ratelimit-remaining-tokens", "x-ratelimit-reset-tokens",
}

type outcome struct {
	idx          int
	status       int
	contentChunk int // SSE chunks carrying content, not tokens: providers batch
	reasonChunk  int
	finishReason string
	sawData      bool
	badChunk     int
	rawTail      string
	headers      string
	err          string
	ttftMs       float64
	e2eMs        float64
}

func atoiOr(name string, def int) int {
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		fmt.Fprintf(os.Stderr, "%s=%q is not a positive integer\n", name, s)
		os.Exit(1)
	}
	return n
}

// clip cuts s to at most n bytes without splitting a rune.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func main() {
	endpoint, key, model := os.Getenv("SWEEP_ENDPOINT"), os.Getenv("SWEEP_API_KEY"), os.Getenv("SWEEP_MODEL")
	for _, m := range []struct{ v, msg string }{
		{endpoint, "SWEEP_ENDPOINT unset: the full chat-completions URL of an OpenAI-compatible endpoint"},
		{key, "SWEEP_API_KEY unset"},
		{model, "SWEEP_MODEL unset: set it to a model id the endpoint serves"},
	} {
		if m.v == "" {
			fmt.Fprintln(os.Stderr, m.msg)
			os.Exit(1)
		}
	}
	total := atoiOr("SWEEP_TOTAL", 300)
	conc := atoiOr("SWEEP_CONCURRENCY", 6)
	maxTok := 128

	var completed, flagged, errored int64
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var exhibits []outcome

	record := func(o outcome) {
		mu.Lock()
		exhibits = append(exhibits, o)
		mu.Unlock()
	}

	client := &http.Client{Timeout: 45 * time.Second}
	start := time.Now()

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			o := outcome{idx: idx}
			body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"Explain in detail how TCP congestion control works. Request %d."}],"stream":true,"max_tokens":%d}`, model, idx, maxTok)
			req, err := http.NewRequest("POST", endpoint, bytes.NewReader([]byte(body)))
			if err != nil {
				atomic.AddInt64(&errored, 1)
				o.err = "build request: " + err.Error()
				record(o)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+key)

			t0 := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt64(&errored, 1)
				o.err = "transport: " + err.Error()
				record(o)
				return
			}
			defer resp.Body.Close()
			o.status = resp.StatusCode

			var hdr []string
			for _, h := range rateHeaders {
				if v := resp.Header.Get(h); v != "" {
					hdr = append(hdr, h+": "+v)
				}
			}
			o.headers = strings.Join(hdr, "  ")

			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				atomic.AddInt64(&errored, 1)
				o.err = "non-200"
				o.rawTail = clip(strings.TrimSpace(string(b)), 300)
				record(o)
				return
			}

			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 1<<20), 1<<20)
			var lastLines, head []string
			var firstTok time.Time
			for sc.Scan() {
				line := strings.TrimSpace(sc.Text())
				if len(head) < 3 && line != "" {
					head = append(head, line)
				}
				payload, ok := strings.CutPrefix(line, "data:")
				if !ok {
					continue
				}
				o.sawData = true
				payload = strings.TrimPrefix(payload, " ")
				if payload == "[DONE]" {
					break
				}
				lastLines = append(lastLines, payload)
				if len(lastLines) > 3 {
					lastLines = lastLines[1:]
				}
				var chunk struct {
					Error   json.RawMessage `json:"error"`
					Choices []struct {
						Delta struct {
							Content          string `json:"content"`
							Reasoning        string `json:"reasoning"`
							ReasoningContent string `json:"reasoning_content"`
						} `json:"delta"`
						FinishReason *string `json:"finish_reason"`
					} `json:"choices"`
				}
				if json.Unmarshal([]byte(payload), &chunk) != nil {
					o.badChunk++
					continue
				}
				if len(chunk.Error) > 0 && string(chunk.Error) != "null" {
					o.err = "in-band error: " + clip(string(chunk.Error), 200)
				}
				if len(chunk.Choices) == 0 {
					continue
				}
				if c := chunk.Choices[0].Delta.Content; c != "" {
					if o.contentChunk == 0 {
						firstTok = time.Now()
					}
					o.contentChunk++
				}
				if d := chunk.Choices[0].Delta; d.Reasoning != "" || d.ReasoningContent != "" {
					o.reasonChunk++
				}
				if fr := chunk.Choices[0].FinishReason; fr != nil {
					o.finishReason = *fr
				}
			}
			io.Copy(io.Discard, resp.Body) //nolint:errcheck // drained so the connection is reusable
			o.e2eMs = float64(time.Since(t0).Microseconds()) / 1000
			if !firstTok.IsZero() {
				o.ttftMs = float64(firstTok.Sub(t0).Microseconds()) / 1000
			}
			o.rawTail = clip(strings.Join(lastLines, " | "), 300)

			// A scanner stops on a read error exactly as it stops at EOF, so an
			// unchecked Err turns a reset, a client timeout, or an over-long
			// line into a zero-content success.
			if err := sc.Err(); err != nil {
				atomic.AddInt64(&errored, 1)
				switch {
				case errors.Is(err, bufio.ErrTooLong):
					o.err = "stream: line exceeded 1MB buffer"
				default:
					o.err = "stream: " + err.Error()
				}
				record(o)
				return
			}
			if o.contentChunk == 0 && o.badChunk > 0 {
				atomic.AddInt64(&errored, 1)
				o.err = fmt.Sprintf("stream: %d unparseable data lines", o.badChunk)
				record(o)
				return
			}
			if !o.sawData {
				atomic.AddInt64(&errored, 1)
				o.err = "no SSE data lines; endpoint did not stream"
				o.rawTail = clip(strings.Join(head, " | "), 300)
				record(o)
				return
			}
			if o.err != "" { // in-band error chunk
				atomic.AddInt64(&errored, 1)
				record(o)
				return
			}

			atomic.AddInt64(&completed, 1)

			// The defect is a response that carried no content and named no reason
			// for stopping. Any stated finish_reason is the endpoint accounting
			// for itself, including "length" from a reasoning model that spent
			// the budget thinking. Truncation is not scored: chunk counts are
			// not token counts, because providers batch.
			if o.finishReason == "" && o.contentChunk == 0 {
				atomic.AddInt64(&flagged, 1)
				record(o)
			}
		}(i)
	}
	wg.Wait()

	fmt.Printf("swept %d requests in %.1fs (concurrency %d, max_tokens %d)\n", total, time.Since(start).Seconds(), conc, maxTok)
	fmt.Printf("  completed=%d  errored=%d  FLAGGED=%d\n", completed, errored, flagged)
	if completed+errored != int64(total) {
		fmt.Printf("  WARNING: completed+errored != %d; the sweep lost requests\n", total)
	}
	if len(exhibits) == 0 {
		fmt.Println("  no errors and no silent-drop candidates in this sweep")
		return
	}
	fmt.Println("\n=== errors and candidates ===")
	for _, e := range exhibits {
		fmt.Printf("  req %d: status=%d contentChunks=%d reasonChunks=%d badChunks=%d finish_reason=%q e2e=%.0fms",
			e.idx, e.status, e.contentChunk, e.reasonChunk, e.badChunk, e.finishReason, e.e2eMs)
		if e.ttftMs > 0 {
			fmt.Printf(" ttft=%.0fms", e.ttftMs)
		}
		if e.err != "" {
			fmt.Printf(" err=%s", e.err)
		}
		fmt.Println()
		if e.headers != "" {
			fmt.Printf("     headers: %s\n", e.headers)
		}
		if e.rawTail != "" {
			fmt.Printf("     tail: %s\n", e.rawTail)
		}
	}
}
