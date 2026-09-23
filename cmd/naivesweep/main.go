// Command naivesweep is a deliberately naive closed-loop sweep of an
// OpenAI-compatible endpoint. It flags a response whose Hypertext Transfer
// Protocol (HTTP) status is 200, that reached the [DONE] terminator and that
// carries no content, no refusal and no stop reason. A read error, an
// unparseable data line, an in-band error chunk, a body with no data lines and
// a stream that ended before [DONE] are reported as errors before that test is
// reached.
//
// It reads OpenAI chat-completion chunks and looks only at the first choice.
// Reasoning deltas are counted separately and do not satisfy the content
// check, so a response carrying only reasoning is flagged when it also names
// no refusal and no stop reason. An event the decoder does not recognise is
// counted and reaches the report whatever the verdict; a response that carried
// nothing else is an error.
//
// A completed response that carried no content is counted as noContent,
// whether or not it is flagged, because a stop reason keeps such a response
// out of the flag and out of the report.
//
// Literal occurrences of the configured key, and of any query value or
// password in the endpoint, are replaced by [REDACTED] in the report. The
// endpoint's path and query never reach a message, though a transport error
// may name the host it dialled. A transport or read error is printed in full
// only when its Go type cannot carry response bytes; any other error is named
// by its type.
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/percentes/percentes/internal/redact"
	"github.com/percentes/percentes/internal/sse"
)

// Bytes read after parsing stops, before the body is closed. 4 KiB.
const drainLimit = 4 << 10

// Ceiling on one scanned line and on the data fields assembled into one event. 1 MiB.
const eventLimit = 1 << 20

// Bytes of a non-200 body that are read. 4 KiB.
const bodyLimit = 4 << 10

// Bytes kept of a printed field, and of a printed tail.
const fieldLimit, tailLimit = 200, 300

// Events kept for the printed tail, and for the head shown when nothing
// streamed.
const tailEvents = 3

// Separator between retained events in the printed tail.
const tailSep = " | "

// Bytes kept of each retained event, so all of them and their separators
// fill the tail rather than the first one filling it alone.
const eventTailLimit = (tailLimit - (tailEvents-1)*len(tailSep)) / tailEvents

const clientTimeout = 45 * time.Second

// Response headers an exhibit shows.
var rateHeaders = []string{
	"retry-after",
	"x-ratelimit-limit-requests", "x-ratelimit-remaining-requests", "x-ratelimit-reset-requests",
	"x-ratelimit-limit-tokens", "x-ratelimit-remaining-tokens", "x-ratelimit-reset-tokens",
}

type outcome struct {
	idx          int
	status       int
	contentChunk int // A chunk may carry several tokens.
	reasonChunk  int
	refusalChunk int
	finishReason string
	sawData      bool
	badChunk     int
	unknownChunk int
	rawTail      string
	headers      string
	err          string
	ttftMs       float64
	e2eMs        float64
}

func envInt(name string, def int) (int, error) {
	s := os.Getenv(name)
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s is not a positive integer", name)
	}
	return n, nil
}

// clip cuts s to at most n bytes without splitting a rune.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return strings.Clone(s[:n])
}

var tailEscape = strings.NewReplacer("\n", `\n`, "\r", `\r`)

func tailOf(rd redactor, s string, n int) string {
	return clip(tailEscape.Replace(rd.redact(s)), n)
}

// redactor replaces literal secrets; one re-encoded or split across events does not match.
type redactor struct {
	r       *strings.Replacer
	longest int
}

func newRedactor(secrets ...string) redactor {
	var rd redactor
	var pairs []string
	for _, s := range secrets {
		if s == "" {
			continue
		}
		pairs = append(pairs, s, "[REDACTED]")
		if len(s) > rd.longest {
			rd.longest = len(s)
		}
	}
	if len(pairs) > 0 {
		rd.r = strings.NewReplacer(pairs...)
	}
	return rd
}

func (rd redactor) redact(s string) string {
	if rd.r == nil {
		return s
	}
	return rd.r.Replace(s)
}

type config struct {
	endpoint string
	model    string
	key      string
	maxTok   int
	timeout  time.Duration
	rd       redactor
}

func main() {
	endpoint, key, model := os.Getenv("SWEEP_ENDPOINT"), os.Getenv("SWEEP_API_KEY"), os.Getenv("SWEEP_MODEL")
	for _, m := range []struct{ v, msg string }{
		{endpoint, "SWEEP_ENDPOINT unset: set it to the full chat-completions address of an OpenAI-compatible endpoint"},
		{key, "SWEEP_API_KEY unset"},
		{model, "SWEEP_MODEL unset: set it to a model id the endpoint serves"},
	} {
		if m.v == "" {
			fmt.Fprintln(os.Stderr, m.msg)
			os.Exit(1)
		}
	}
	rd := newRedactor(append([]string{key}, redact.Secrets(endpoint)...)...)
	if err := checkEndpoint(endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "SWEEP_ENDPOINT: %v\n", rd.redact(err.Error()))
		os.Exit(1)
	}
	total, err := envInt("SWEEP_TOTAL", 300)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	conc, err := envInt("SWEEP_CONCURRENCY", 6)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg := config{endpoint: endpoint, model: model, key: key, maxTok: 128, timeout: clientTimeout, rd: rd}

	var completed, flagged, errored, noContent int64
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var exhibits []outcome

	record := func(o outcome) {
		mu.Lock()
		exhibits = append(exhibits, o)
		mu.Unlock()
	}

	start := time.Now()

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			o, ok, flag := sweepOne(http.DefaultTransport, cfg, idx)
			switch {
			case !ok:
				atomic.AddInt64(&errored, 1)
			case flag:
				atomic.AddInt64(&completed, 1)
				atomic.AddInt64(&flagged, 1)
			default:
				atomic.AddInt64(&completed, 1)
			}
			if noContentOutcome(o, ok) {
				atomic.AddInt64(&noContent, 1)
			}
			if exhibited(o, ok, flag) {
				record(o)
			}
		}(i)
	}
	wg.Wait()

	fmt.Printf("swept %d requests in %.1fs (concurrency %d, max_tokens %d)\n", total, time.Since(start).Seconds(), conc, cfg.maxTok)
	fmt.Println(summaryLine(completed, errored, flagged, noContent))
	fmt.Print(trailer(total, completed, errored, noContent, exhibits))
}

// noContentOutcome reports whether a completed request delivered no text.
func noContentOutcome(o outcome, ok bool) bool {
	return ok && o.contentChunk == 0
}

// trailer renders everything the report prints after the summary. The clean
// bill is withheld while any completed request delivered no text.
func trailer(total int, completed, errored, noContent int64, exhibits []outcome) string {
	var b strings.Builder
	if completed+errored != int64(total) {
		fmt.Fprintf(&b, "  WARNING: completed+errored != %d; the sweep lost requests\n", total)
	}
	if len(exhibits) == 0 {
		if noContent == 0 {
			b.WriteString("  no errors and no silent-drop candidates in this sweep\n")
		}
		return b.String()
	}
	b.WriteString("\n=== errors and candidates ===\n")
	for _, e := range exhibits {
		b.WriteString(exhibitLine(e))
	}
	return b.String()
}

// A counted unknown event is exhibited even when the request completed.
func exhibited(o outcome, ok, flag bool) bool {
	return !ok || flag || o.unknownChunk > 0 || o.badChunk > 0
}

func exhibitLine(e outcome) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  req %d: status=%d contentChunks=%d reasonChunks=%d refusals=%d badChunks=%d unknownChunks=%d finish_reason=%q e2e=%.0fms",
		e.idx, e.status, e.contentChunk, e.reasonChunk, e.refusalChunk, e.badChunk, e.unknownChunk, e.finishReason, e.e2eMs)
	if e.ttftMs > 0 {
		fmt.Fprintf(&b, " ttft=%.0fms", e.ttftMs)
	}
	if e.err != "" {
		fmt.Fprintf(&b, " err=%s", e.err)
	}
	b.WriteString("\n")
	if e.headers != "" {
		fmt.Fprintf(&b, "     headers: %s\n", e.headers)
	}
	if e.rawTail != "" {
		fmt.Fprintf(&b, "     tail: %s\n", e.rawTail)
	}
	return b.String()
}

// A flagged response carried no content, so flagged counts within noContent.
func summaryLine(completed, errored, flagged, noContent int64) string {
	return fmt.Sprintf("  completed=%d (noContent=%d)  errored=%d  FLAGGED=%d (within completed)", completed, noContent, errored, flagged)
}

// checkEndpoint accepts an absolute http or https URL; no error names the URL.
func checkEndpoint(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return errors.New("not a URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme %q is not http or https", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("no host")
	}
	return nil
}

// readHead reads at most limit bytes; a body cut there also loses the cut-1 bytes before it.
func readHead(r io.Reader, limit, cut int) string {
	b, _ := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if len(b) <= limit {
		return string(b)
	}
	keep := limit - cut + 1
	if keep < 0 {
		keep = 0
	}
	return string(b[:keep])
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type request struct {
	Model     string    `json:"model"`
	Messages  []message `json:"messages"`
	Stream    bool      `json:"stream"`
	MaxTokens int       `json:"max_tokens"`
}

func sweepOne(rt http.RoundTripper, cfg config, idx int) (outcome, bool, bool) {
	o := outcome{idx: idx}
	body, _ := json.Marshal(request{
		Model:     cfg.model,
		Messages:  []message{{Role: "user", Content: fmt.Sprintf("Explain in detail how TCP congestion control works. Request %d.", idx)}},
		Stream:    true,
		MaxTokens: cfg.maxTok,
	})
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.endpoint, bytes.NewReader(body))
	if err != nil {
		o.err = "build request: " + redact.ErrorText(err, cfg.timeout)
		return o, false, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.key)

	t0 := time.Now()
	resp, err := rt.RoundTrip(req)
	if err != nil {
		o.err = "transport: " + cfg.rd.redact(redact.ErrorText(err, cfg.timeout))
		return o, false, false
	}
	defer resp.Body.Close()
	o.status = resp.StatusCode

	var hdr []string
	for _, h := range rateHeaders {
		if v := resp.Header.Get(h); v != "" {
			hdr = append(hdr, h+": "+clip(cfg.rd.redact(v), fieldLimit))
		}
	}
	o.headers = strings.Join(hdr, "  ")

	if resp.StatusCode != http.StatusOK {
		o.err = "non-200"
		o.rawTail = tailOf(cfg.rd, strings.TrimSpace(readHead(resp.Body, bodyLimit, cfg.rd.longest)), tailLimit)
		return o, false, false
	}

	sawDone, firstTok, head, lastLines, scanErr := parseStream(resp.Body, &o, cfg.rd)
	o.e2eMs = float64(time.Since(t0).Microseconds()) / 1000
	io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit)) //nolint:errcheck
	if !firstTok.IsZero() {
		o.ttftMs = float64(firstTok.Sub(t0).Microseconds()) / 1000
	}
	o.rawTail = tailOf(cfg.rd, strings.Join(lastLines, tailSep), tailLimit)

	ok, flag := verdict(&o, sawDone, head, scanErr, cfg)
	return o, ok, flag
}

func verdict(o *outcome, sawDone bool, head []string, scanErr error, cfg config) (bool, bool) {
	switch {
	case scanErr != nil:
		msg := "stream: " + cfg.rd.redact(redact.ErrorText(scanErr, cfg.timeout))
		if errors.Is(scanErr, bufio.ErrTooLong) {
			msg = "stream: line exceeded 1MB buffer"
		}
		if o.err != "" {
			msg = o.err + "; " + msg
		}
		o.err = msg
		return false, false
	case o.err != "": // in-band error chunk
		return false, false
	case o.badChunk > 0:
		o.err = fmt.Sprintf("stream: %d unparseable data lines", o.badChunk)
		return false, false
	case !o.sawData:
		o.err = "no Server-Sent Events data lines; endpoint did not stream"
		o.rawTail = tailOf(cfg.rd, strings.Join(head, tailSep), tailLimit)
		return false, false
	case !sawDone:
		o.err = "stream ended before [DONE]"
		return false, false
	case o.unknownChunk > 0 && o.contentChunk == 0 && o.refusalChunk == 0 && o.finishReason == "":
		o.err = fmt.Sprintf("stream: %d events in an unsupported shape", o.unknownChunk)
		return false, false
	}

	return true, o.finishReason == "" && o.contentChunk == 0 && o.refusalChunk == 0
}

// parseStream reads one Server-Sent Events response into o. An event's data
// fields are joined with newlines and a leading byte-order mark is allowed.
func parseStream(r io.Reader, o *outcome, rd redactor) (sawDone bool, firstTok time.Time, head, lastLines []string, err error) {
	// A JavaScript Object Notation (JSON) key set to null is present; an absent key is not.
	decode := func(payload []byte) bool {
		var chunk struct {
			Object  string          `json:"object"`
			ID      json.RawMessage `json:"id"`
			Created json.RawMessage `json:"created"`
			Model   json.RawMessage `json:"model"`
			Error   json.RawMessage `json:"error"`
			Usage   json.RawMessage `json:"usage"`
			Choices json.RawMessage `json:"choices"`
		}
		if json.Unmarshal(payload, &chunk) != nil {
			return false
		}
		present := func(f json.RawMessage) bool { return len(f) > 0 }
		set := func(f json.RawMessage) bool { return len(f) > 0 && string(f) != "null" }

		if set(chunk.Error) {
			o.err = "in-band error: " + clip(rd.redact(string(chunk.Error)), fieldLimit)
		}
		var choices []struct {
			Delta struct {
				Content          string `json:"content"`
				Refusal          string `json:"refusal"`
				Reasoning        string `json:"reasoning"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		}
		if set(chunk.Choices) && json.Unmarshal(chunk.Choices, &choices) != nil {
			return false
		}
		recognised := chunk.Object == "chat.completion.chunk" ||
			present(chunk.Error) || present(chunk.Usage) || present(chunk.Choices) ||
			present(chunk.ID) || present(chunk.Created) || present(chunk.Model)
		if !recognised {
			o.unknownChunk++
			return true
		}
		if len(choices) == 0 {
			return true
		}
		ch := choices[0]
		if ch.Delta.Content != "" {
			if o.contentChunk == 0 {
				firstTok = time.Now()
			}
			o.contentChunk++
		}
		if ch.Delta.Refusal != "" {
			o.refusalChunk++
		}
		if ch.Delta.Reasoning != "" || ch.Delta.ReasoningContent != "" {
			o.reasonChunk++
		}
		if ch.FinishReason != nil {
			o.finishReason = clip(rd.redact(*ch.FinishReason), fieldLimit)
		}
		return true
	}

	handle := func(payload []byte) bool {
		if string(payload) == "[DONE]" {
			return true
		}
		lastLines = append(lastLines, tailOf(rd, string(payload), eventTailLimit))
		if len(lastLines) > tailEvents {
			lastLines = lastLines[1:]
		}
		if decode(payload) {
			return false
		}
		o.badChunk++
		// A joined payload that does not decode may still hold whole objects.
		if lines := bytes.Split(payload, []byte("\n")); len(lines) > 1 {
			for _, line := range lines {
				decode(line)
			}
		}
		return false
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 4096), eventLimit)
	sc.Split(sse.SplitLines)
	var event []byte
	dropped, firstLine := false, true
	for sc.Scan() {
		line := sc.Bytes()
		if firstLine {
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
			firstLine = false
		}
		if len(line) == 0 {
			payload := bytes.TrimSuffix(event, []byte("\n"))
			done := !dropped && len(payload) > 0 && handle(payload)
			event, dropped = event[:0], false
			if done {
				sawDone = true
				break
			}
			continue
		}
		if len(head) < tailEvents {
			head = append(head, tailOf(rd, string(line), eventTailLimit))
		}
		v, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue
		}
		o.sawData = true
		if dropped {
			continue
		}
		v = bytes.TrimPrefix(v, []byte(" "))
		if len(event)+len(v)+1 > eventLimit {
			o.badChunk++
			event, dropped = event[:0], true
			continue
		}
		event = append(append(event, v...), '\n')
	}
	// A final event without a trailing blank line is still dispatched.
	if payload := bytes.TrimSuffix(event, []byte("\n")); !sawDone && !dropped && len(payload) > 0 && handle(payload) {
		sawDone = true
	}
	return sawDone, firstTok, head, lastLines, sc.Err()
}
