package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
	"unsafe"

	"github.com/percentes/percentes/internal/redact"
)

const goodEvent = `data: {"choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"

const echoKey = "review-key-not-secret"

const urlSecret = "AIzaSyURLSECRET123"

func fixtureConfig(endpoint, key string) config {
	return config{endpoint: endpoint, model: "fixture-model", key: key, maxTok: 8, timeout: clientTimeout, rd: newRedactor(key)}
}

func TestParseStreamFramings(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"plain", goodEvent},
		// The Server-Sent Events grammar permits a leading byte-order mark.
		{"byte-order mark", "\xef\xbb\xbf" + goodEvent},
		// Carriage return alone, and carriage return then line feed, are also terminators.
		{"carriage returns only", strings.ReplaceAll(goodEvent, "\n", "\r")},
		{"crlf", strings.ReplaceAll(goodEvent, "\n", "\r\n")},
		// An event may carry several data fields, joined before decoding.
		{"data split across lines", "data: {\"choices\":[\ndata: {\"delta\":{\"content\":\"Hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"},
		// A line holding one space names a field and does not end the event.
		{"space-named field inside an event", "data: {\"choices\":[\n \ndata: {\"delta\":{\"content\":\"Hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"},
		{"trailing space on a data line", strings.Replace(goodEvent, "}]}\n", "}]} \n", 1)},
		// A final event without a trailing blank line is still dispatched.
		{"no trailing blank line", strings.TrimRight(goodEvent, "\n")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var o outcome
			sawDone, _, head, _, err := parseStream(strings.NewReader(c.body), &o, redactor{})
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			ok, flag := verdict(&o, sawDone, head, err, config{})
			if o.contentChunk != 1 {
				t.Errorf("contentChunk %d, want 1", o.contentChunk)
			}
			if o.finishReason != "stop" {
				t.Errorf("finishReason %q, want \"stop\"", o.finishReason)
			}
			if o.badChunk != 0 {
				t.Errorf("badChunk %d, want 0", o.badChunk)
			}
			if !ok {
				t.Errorf("completed=false (err %q)", o.err)
			}
			if flag {
				t.Error("a response carrying content and a stop reason must not be flagged")
			}
			if !sawDone {
				t.Error("[DONE] not seen")
			}
		})
	}
}

func TestVoidResponseIsFlagged(t *testing.T) {
	var o outcome
	sawDone, _, head, _, err := parseStream(strings.NewReader("data: [DONE]\n\n"), &o, redactor{})
	if err != nil {
		t.Fatal(err)
	}
	if !o.sawData {
		t.Error("sawData false on a stream carrying a data line")
	}
	if _, flag := verdict(&o, sawDone, head, err, config{}); !flag {
		t.Error("a 200 that delivered nothing and named no reason must be flagged")
	}
}

func TestReasoningWithoutFinishReasonIsFlagged(t *testing.T) {
	const reasoning = `data: {"choices":[{"delta":{"reasoning":"weighing it up"},"finish_reason":null}]}` + "\n\n"
	cases := []struct {
		name     string
		body     string
		wantFlag bool
	}{
		{"reasoning and no finish reason", reasoning + "data: [DONE]\n\n", true},
		{"reasoning then a finish reason", reasoning + `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var o outcome
			sawDone, _, head, _, err := parseStream(strings.NewReader(c.body), &o, redactor{})
			if err != nil {
				t.Fatal(err)
			}
			if o.reasonChunk != 1 {
				t.Errorf("reasonChunk %d, want 1", o.reasonChunk)
			}
			ok, flag := verdict(&o, sawDone, head, err, config{})
			if !ok {
				t.Errorf("completed=false (err %q)", o.err)
			}
			if flag != c.wantFlag {
				t.Errorf("flagged=%v, want %v", flag, c.wantFlag)
			}
		})
	}
}

// carriesFragment reports whether s holds any eight-byte run of secret.
func carriesFragment(s, secret string) bool {
	for i := 0; i+8 <= len(secret); i++ {
		if strings.Contains(s, secret[i:i+8]) {
			return true
		}
	}
	return false
}

func TestExhibitRedactsConfiguredKey(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		header     string
		body       string
		wantMarker bool
	}{
		{"non-200 body", http.StatusUnauthorized, "", `{"error":"Invalid API key: ` + echoKey + `"}`, true},
		{"in-band error", http.StatusOK, "", "data: {\"error\":{\"message\":\"Invalid API key: " + echoKey + "\"}}\n\ndata: [DONE]\n\n", true},
		{"quota header", http.StatusOK, echoKey, goodEvent, true},
		{"streamed content", http.StatusOK, "", "data: {\"choices\":[{\"delta\":{\"content\":\"" + echoKey + "\"}}]}\n\n", true},
		{"non-streaming body", http.StatusOK, "", `{"note":"Invalid API key: ` + echoKey + `"}` + "\n", true},
		{"finish reason", http.StatusOK, "", "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":\"" + echoKey + "\"}]}\n\ndata: [DONE]\n\n", true},
		{"key cut by the body limit", http.StatusUnauthorized, "", strings.Repeat(" ", bodyLimit-len(echoKey)+1) + echoKey, false},
		{"key straddling the body limit", http.StatusUnauthorized, "", strings.Repeat(" ", bodyLimit-5) + echoKey, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if c.header != "" {
					w.Header().Set("retry-after", c.header)
				}
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, c.body)
			}))
			defer srv.Close()

			o, _, _ := sweepOne(srv.Client().Transport, fixtureConfig(srv.URL, echoKey), 0)

			line := exhibitLine(o)
			if carriesFragment(line, echoKey) {
				t.Errorf("exhibit carries the configured key or a fragment of it: %q", line)
			}
			if strings.Contains(line, "[REDACTED]") != c.wantMarker {
				t.Errorf("redaction marker present=%v, want %v: %q", !c.wantMarker, c.wantMarker, line)
			}
		})
	}
}

func TestNumericSettingErrorsOmitTheValue(t *testing.T) {
	for _, c := range []struct {
		value   string
		wantErr bool
		want    int
	}{
		{"", false, 300},
		{"12", false, 12},
		{"0", true, 0},
		{"-3", true, 0},
		{echoKey, true, 0},
	} {
		t.Run(c.value, func(t *testing.T) {
			t.Setenv("SWEEP_TOTAL", c.value)
			n, err := envInt("SWEEP_TOTAL", 300)
			if (err != nil) != c.wantErr {
				t.Fatalf("err %v, wantErr %v", err, c.wantErr)
			}
			if n != c.want {
				t.Errorf("value %d, want %d", n, c.want)
			}
			if err != nil && strings.Contains(err.Error(), c.value) {
				t.Errorf("error quotes the setting's value: %q", err)
			}
		})
	}
}

func TestMalformedEventIsAnError(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantOK  bool
		wantBad int
	}{
		{"malformed event alone", "data: not-json\n\ndata: [DONE]\n\n", false, 1},
		{"malformed event after content", strings.TrimSuffix(goodEvent, "data: [DONE]\n\n") + "data: not-json\n\ndata: [DONE]\n\n", false, 1},
		{"clean stream", goodEvent, true, 0},
		// An event whose only data field is empty is not dispatched.
		{"empty data field", "data:\n\n" + goodEvent, true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var o outcome
			sawDone, _, head, _, scanErr := parseStream(strings.NewReader(c.body), &o, redactor{})
			if o.badChunk != c.wantBad {
				t.Fatalf("badChunk %d, want %d", o.badChunk, c.wantBad)
			}
			ok, flag := verdict(&o, sawDone, head, scanErr, config{})
			if ok != c.wantOK {
				t.Errorf("completed=%v, want %v (err %q)", ok, c.wantOK, o.err)
			}
			if flag {
				t.Error("a stream carrying a decode failure must not be counted as a silent drop")
			}
			if !c.wantOK && o.err == "" {
				t.Error("an errored outcome carries no message, so no exhibit would report it")
			}
		})
	}
}

func TestInBandErrorOutranksAnUnparseableLine(t *testing.T) {
	const body = `data: {"error":{"message":"rate limit exceeded"}}` + "\n\ndata: junk\n\ndata: [DONE]\n\n"
	var o outcome
	sawDone, _, head, _, scanErr := parseStream(strings.NewReader(body), &o, redactor{})
	ok, _ := verdict(&o, sawDone, head, scanErr, config{})
	if ok {
		t.Error("a stream carrying an in-band error counted as a completed sweep")
	}
	if !strings.Contains(o.err, "rate limit exceeded") {
		t.Errorf("err %q drops the provider's own message", o.err)
	}
	if o.badChunk != 1 {
		t.Errorf("badChunk %d, want 1", o.badChunk)
	}
	if line := exhibitLine(o); !strings.Contains(line, "badChunks=1") {
		t.Errorf("exhibit does not report the unparseable line: %q", line)
	}
}

func TestProviderErrorSurvivesMalformedChoices(t *testing.T) {
	content := strings.TrimSuffix(goodEvent, "data: [DONE]\n\n")
	body := `data: {"error":{"message":"quota exhausted"},"choices":"bad"}` + "\n\n" +
		strings.Repeat(content, 4) + "data: [DONE]\n\n"
	var o outcome
	sawDone, _, head, _, scanErr := parseStream(strings.NewReader(body), &o, redactor{})
	ok, _ := verdict(&o, sawDone, head, scanErr, config{})
	if ok {
		t.Error("a stream carrying a provider error counted as a completed sweep")
	}
	if !strings.Contains(o.err, "quota exhausted") {
		t.Errorf("err %q drops the provider's own message", o.err)
	}
	if o.badChunk != 1 {
		t.Errorf("badChunk %d, want 1", o.badChunk)
	}
}

func TestMultiLinePayloadKeepsWhatItParsed(t *testing.T) {
	const body = `data: {"choices":[{"delta":{"content":"Hi"},"finish_reason":"stop"}]}` + "\n" +
		"data: trailing junk\n\ndata: [DONE]\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	o, ok, _ := sweepOne(srv.Client().Transport, fixtureConfig(srv.URL, ""), 0)
	if ok {
		t.Error("an event that did not decode counted as a completed sweep")
	}
	if o.badChunk != 1 {
		t.Errorf("badChunk %d, want 1", o.badChunk)
	}
	if o.contentChunk != 1 {
		t.Errorf("contentChunk %d, want 1: the event held one whole chunk", o.contentChunk)
	}
	if o.finishReason != "stop" {
		t.Errorf("finishReason %q, want \"stop\"", o.finishReason)
	}
	if strings.Contains(o.rawTail, "\n") {
		t.Errorf("tail spills across report lines: %q", o.rawTail)
	}
	if line := exhibitLine(o); strings.Count(line, "\n") != 2 {
		t.Errorf("exhibit runs to %d lines, want 2: %q", strings.Count(line, "\n"), line)
	}
}

func TestUnreadEventReachesTheReport(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"unsupported shape then content", `data: {"output_text":"Hello","stop_reason":"end_turn"}` + "\n\n" + goodEvent},
		{"unparseable line then content", "data: not-json\n\n" + goodEvent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var o outcome
			sawDone, _, head, _, scanErr := parseStream(strings.NewReader(c.body), &o, redactor{})
			ok, flag := verdict(&o, sawDone, head, scanErr, config{})
			if o.unknownChunk+o.badChunk == 0 {
				t.Fatal("the fixture did not reach the decoder's unread path")
			}
			if !exhibited(o, ok, flag) {
				t.Errorf("no exhibit for a request carrying %d unknown and %d bad events", o.unknownChunk, o.badChunk)
			}
		})
	}
}

func TestOversizedEventIsDropped(t *testing.T) {
	// No blank line separates the data lines, so this is one event.
	var b strings.Builder
	for i := 0; i < 200; i++ {
		b.WriteString("data: ")
		b.WriteString(strings.Repeat("x", 64<<10))
		b.WriteString("\n")
	}
	b.WriteString("\ndata: [DONE]\n\n")

	var o outcome
	sawDone, _, _, lastLines, err := parseStream(strings.NewReader(b.String()), &o, redactor{})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !sawDone {
		t.Error("[DONE] not seen after the oversized event")
	}
	if o.badChunk < 1 {
		t.Errorf("badChunk %d, want at least 1", o.badChunk)
	}
	if len(lastLines) != 0 {
		t.Errorf("a dropped event reached the tail: %d entries", len(lastLines))
	}
}

func TestEventCapIsExact(t *testing.T) {
	const open, closeFmt = `{"choices":[`, `{"delta":{"content":"%s"},"finish_reason":"stop"}]}`
	fixed := len(open) + 1 + len(closeFmt) - 2 + 1
	for _, c := range []struct {
		name        string
		over        int
		wantContent int
		wantBad     int
	}{
		{"at the cap", 0, 1, 0},
		{"one byte over", 1, 0, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			pad := strings.Repeat("x", eventLimit-fixed+c.over)
			body := "data: " + open + "\ndata: " + strings.Replace(closeFmt, "%s", pad, 1) + "\n\ndata: [DONE]\n\n"
			var o outcome
			sawDone, _, _, _, err := parseStream(strings.NewReader(body), &o, redactor{})
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if !sawDone {
				t.Error("[DONE] not seen")
			}
			if o.contentChunk != c.wantContent || o.badChunk != c.wantBad {
				t.Errorf("contentChunk %d badChunk %d, want %d and %d", o.contentChunk, o.badChunk, c.wantContent, c.wantBad)
			}
		})
	}
}

func TestPaddedDataLinesAreNotRetained(t *testing.T) {
	pr, pw := io.Pipe()
	parsed := make(chan struct{})
	go func() {
		var o outcome
		_, _, _, _, _ = parseStream(pr, &o, redactor{})
		close(parsed)
	}()
	line := "data: x" + strings.Repeat(" ", 256<<10) + "\n"
	if _, err := io.WriteString(pw, line); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < 64; i++ {
		if _, err := io.WriteString(pw, line); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	_ = pw.Close()
	<-parsed
	if grew := int64(after.HeapAlloc) - int64(before.HeapAlloc); grew > 8<<20 {
		t.Errorf("parser retained %d bytes across 64 padded lines, want under 8 MiB", grew)
	}
}

func TestClipCopiesWhatItKeeps(t *testing.T) {
	s := strings.Repeat("a", 1<<20)
	c := clip(s, 10)
	if len(c) != 10 {
		t.Fatalf("len %d, want 10", len(c))
	}
	if unsafe.StringData(c) == unsafe.StringData(s) {
		t.Error("clip returned a view of the original allocation")
	}
	if whole := clip(s[:10], 10); unsafe.StringData(whole) != unsafe.StringData(s) {
		t.Error("clip copied a string it did not cut")
	}
}

func TestTailShowsEveryRetainedEvent(t *testing.T) {
	marks := make([]string, 0, tailEvents+1)
	for i := 0; i <= tailEvents; i++ {
		marks = append(marks, fmt.Sprintf("MARK%02d", i))
	}
	var b strings.Builder
	for _, mark := range marks {
		b.WriteString(`data: {"note":"` + mark + strings.Repeat("x", tailLimit) + `"}` + "\n\n")
	}
	b.WriteString("data: [DONE]\n\n")

	var o outcome
	_, _, _, lastLines, err := parseStream(strings.NewReader(b.String()), &o, redactor{})
	if err != nil {
		t.Fatal(err)
	}
	if len(lastLines) != tailEvents {
		t.Fatalf("retained %d events, want %d", len(lastLines), tailEvents)
	}
	tail := tailOf(redactor{}, strings.Join(lastLines, tailSep), tailLimit)
	if len(tail) > tailLimit {
		t.Errorf("tail is %d bytes, over the %d limit", len(tail), tailLimit)
	}
	// Every retained event reaches the report; the evicted one does not.
	for _, want := range marks[1:] {
		if !strings.Contains(tail, want) {
			t.Errorf("tail drops %s, so only part of the retained events is shown: %q", want, tail)
		}
	}
	if strings.Contains(tail, marks[0]) {
		t.Errorf("tail carries an evicted event: %q", tail)
	}
}

func TestPrintedFieldsAreBounded(t *testing.T) {
	long := strings.Repeat("y", 300<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("retry-after", long)
		_, _ = io.WriteString(w, `data: {"output_text":"?"}`+"\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":\""+long+"\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()

	o, _, _ := sweepOne(srv.Client().Transport, fixtureConfig(srv.URL, ""), 0)
	if n := len(exhibitLine(o)); n > 2000 {
		t.Errorf("exhibit runs to %d bytes", n)
	}
}

func TestCheckEndpoint(t *testing.T) {
	for _, c := range []struct {
		in      string
		wantErr bool
	}{
		{"https://host/v1/chat/completions", false},
		{"http://127.0.0.1:8080/v1/chat/completions", false},
		{"host/v1/chat/completions", true},
		{"ftp://host/v1", true},
		{"https://", true},
		{"://host/v1", true},
		{"https://host/v1\x7f", true},
	} {
		t.Run(c.in, func(t *testing.T) {
			if err := checkEndpoint(c.in); (err != nil) != c.wantErr {
				t.Errorf("checkEndpoint(%q) = %v, wantErr %v", c.in, err, c.wantErr)
			}
		})
	}
}

func TestEndpointCredentialsStayOutOfMessages(t *testing.T) {
	for _, in := range []string{
		"http://127.0.0.1:1/v1/chat/completions?key=" + urlSecret,
		"https://user:" + urlSecret + "@127.0.0.1:1/v1/chat/completions",
		"ftp://host/v1?key=" + urlSecret,
		"://host/v1?key=" + urlSecret,
		"https://host/v1\x7f?key=" + urlSecret,
	} {
		t.Run(in, func(t *testing.T) {
			if err := checkEndpoint(in); err != nil {
				if strings.Contains(err.Error(), urlSecret) {
					t.Fatalf("checkEndpoint error carries the credential: %q", err)
				}
				return
			}
			cfg := fixtureConfig(in, "")
			o, ok, _ := sweepOne(http.DefaultTransport, cfg, 0)
			if ok {
				t.Fatal("a refused connection counted as a completed sweep")
			}
			if line := exhibitLine(o); strings.Contains(line, urlSecret) {
				t.Errorf("exhibit carries the credential: %q", line)
			}
		})
	}

	t.Run("query value echoed in a body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, "bad query: "+r.URL.RawQuery)
		}))
		defer srv.Close()
		endpoint := srv.URL + "/v1?key=" + urlSecret
		cfg := fixtureConfig(endpoint, "")
		cfg.rd = newRedactor(redact.Secrets(endpoint)...)
		o, _, _ := sweepOne(srv.Client().Transport, cfg, 0)
		line := exhibitLine(o)
		if strings.Contains(line, urlSecret) || !strings.Contains(line, "[REDACTED]") {
			t.Errorf("exhibit carries the query credential: %q", line)
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransportErrorTextIsAllowlisted(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		want    string
		exclude string
	}{
		{"network operation", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, "dial tcp: connection refused", ""},
		{"deadline", context.DeadlineExceeded, "timed out after 45s", ""},
		{"free text", errors.New("Location: /%zz?key=" + urlSecret), "*errors.errorString, text withheld", urlSecret},
		{"free text wrapped", &net.DNSError{Err: "no such host", Name: "host"}, "*net.DNSError, text withheld", "host"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rt := roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, c.err })
			o, ok, _ := sweepOne(rt, fixtureConfig("http://127.0.0.1:1/v1", ""), 0)
			if ok {
				t.Error("a failed request counted as a completed sweep")
			}
			if o.err != "transport: "+c.want {
				t.Errorf("err %q, want %q", o.err, "transport: "+c.want)
			}
			if c.exclude != "" && strings.Contains(o.err, c.exclude) {
				t.Errorf("err %q carries withheld text", o.err)
			}
		})
	}
}

func TestReadErrorIsReportedBesideTheProviderError(t *testing.T) {
	const inband = `data: {"error":{"message":"rate limit exceeded"}}` + "\n\n"
	cases := []struct {
		name    string
		err     error
		want    string
		exclude string
	}{
		{"network operation", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}, "read tcp: connection reset by peer", ""},
		{"free text", errors.New("Location: /%zz?key=" + urlSecret), "*errors.errorString, text withheld", urlSecret},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var o outcome
			r := io.MultiReader(strings.NewReader(inband), iotest.ErrReader(c.err))
			sawDone, _, head, _, scanErr := parseStream(r, &o, redactor{})
			if scanErr == nil {
				t.Fatal("the reader's failure did not reach the scanner")
			}
			ok, _ := verdict(&o, sawDone, head, scanErr, config{})
			if ok {
				t.Error("a failed read counted as a completed sweep")
			}
			want := "in-band error: " + `{"message":"rate limit exceeded"}` + "; stream: " + c.want
			if o.err != want {
				t.Errorf("err %q, want %q", o.err, want)
			}
			if c.exclude != "" && strings.Contains(o.err, c.exclude) {
				t.Errorf("err %q carries withheld text", o.err)
			}
		})
	}
}

func TestTimeoutIsReported(t *testing.T) {
	cases := []struct {
		name    string
		headers bool
		want    string
	}{
		{"before headers", false, "transport: timed out after 200ms"},
		{"during the body", true, "stream: timed out after 200ms"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if c.headers {
					_, _ = io.WriteString(w, "data: {\"choices\":[")
					w.(http.Flusher).Flush()
				}
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}))
			defer srv.Close()

			cfg := fixtureConfig(srv.URL, "")
			cfg.timeout = 200 * time.Millisecond
			o, ok, _ := sweepOne(srv.Client().Transport, cfg, 0)
			if ok {
				t.Error("a timed-out request counted as a completed sweep")
			}
			if o.err != c.want {
				t.Errorf("err %q, want %q", o.err, c.want)
			}
		})
	}
}

func TestRedirectIsReportedAsStatus(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, loc := range []string{"/second", "/%zz?key=" + urlSecret} {
			t.Run(http.StatusText(code)+" to "+loc, func(t *testing.T) {
				var hits int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					atomic.AddInt32(&hits, 1)
					if r.URL.Path == "/second" {
						_, _ = io.WriteString(w, goodEvent)
						return
					}
					w.Header().Set("Location", loc)
					w.WriteHeader(code)
				}))
				defer srv.Close()

				o, ok, _ := sweepOne(srv.Client().Transport, fixtureConfig(srv.URL, ""), 0)
				if n := atomic.LoadInt32(&hits); n != 1 {
					t.Errorf("endpoint received %d requests, want 1", n)
				}
				if ok {
					t.Error("a redirect counted as a completed sweep")
				}
				if o.status != code || o.err != "non-200" {
					t.Errorf("status=%d err=%q, want %d and non-200", o.status, o.err, code)
				}
				if line := exhibitLine(o); strings.Contains(line, urlSecret) {
					t.Errorf("exhibit carries the redirect target: %q", line)
				}
			})
		}
	}
}

func TestRequestBodyIsJSON(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, goodEvent)
	}))
	defer srv.Close()

	cfg := fixtureConfig(srv.URL, "")
	cfg.model = "model\x01id\"quoted"
	if _, ok, _ := sweepOne(srv.Client().Transport, cfg, 7); !ok {
		t.Fatal("the fixture did not complete")
	}
	if !json.Valid(got) {
		t.Fatalf("request body is not JSON: %q", got)
	}
	var req request
	if err := json.Unmarshal(got, &req); err != nil {
		t.Fatal(err)
	}
	if req.Model != cfg.model || !req.Stream || req.MaxTokens != cfg.maxTok || len(req.Messages) != 1 {
		t.Errorf("request %+v does not carry the configuration", req)
	}
}

func TestUnsupportedShapesAreNotSilentDrops(t *testing.T) {
	const done = "data: [DONE]\n\n"
	cases := []struct {
		name        string
		body        string
		wantOK      bool
		wantFlag    bool
		wantUnknown int
		wantRefusal int
	}{
		{"unsupported schema", `data: {"output_text":"Hello","stop_reason":"end_turn"}` + "\n\n" + done, false, false, 1, 0},
		{"refusal without a finish reason", `data: {"choices":[{"delta":{"refusal":"I cannot help with that"},"finish_reason":null}]}` + "\n\n" + done, true, false, 0, 1},
		{"role event then content", `data: {"choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}` + "\n\n" + goodEvent, true, false, 0, 0},
		{"usage event then content", `data: {"usage":{"total_tokens":5}}` + "\n\n" + goodEvent, true, false, 0, 0},
		{"envelope without choices then content", `data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"fixture-model"}` + "\n\n" + goodEvent, true, false, 0, 0},
		{"recognised empty output", `data: {"choices":[]}` + "\n\n" + done, true, true, 0, 0},
		// Shapes that carry nothing to count.
		{"null error then content", `data: {"error":null}` + "\n\n" + goodEvent, true, false, 0, 0},
		{"null choices then content", `data: {"choices":null}` + "\n\n" + goodEvent, true, false, 0, 0},
		{"bare envelope then content", `data: {"id":"c","created":1,"model":"m"}` + "\n\n" + goodEvent, true, false, 0, 0},
		{"keep-alive without object then content", `data: {"created":1}` + "\n\n" + goodEvent, true, false, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var o outcome
			sawDone, _, head, _, scanErr := parseStream(strings.NewReader(c.body), &o, redactor{})
			ok, flag := verdict(&o, sawDone, head, scanErr, config{})
			if ok != c.wantOK {
				t.Errorf("completed=%v, want %v (err %q)", ok, c.wantOK, o.err)
			}
			if flag != c.wantFlag {
				t.Errorf("flagged=%v, want %v", flag, c.wantFlag)
			}
			if o.unknownChunk != c.wantUnknown {
				t.Errorf("unknownChunk %d, want %d", o.unknownChunk, c.wantUnknown)
			}
			if o.refusalChunk != c.wantRefusal {
				t.Errorf("refusalChunk %d, want %d", o.refusalChunk, c.wantRefusal)
			}
			if c.wantUnknown > 0 && !strings.Contains(o.err, "unsupported shape") {
				t.Errorf("err %q does not name the unsupported shape", o.err)
			}
		})
	}
}

func TestEndToEndTimeStopsAtTheTerminator(t *testing.T) {
	const held = 400 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, goodEvent)
		w.(http.Flusher).Flush()
		time.Sleep(held)
	}))
	defer srv.Close()

	o, ok, _ := sweepOne(srv.Client().Transport, fixtureConfig(srv.URL, ""), 0)
	if !ok {
		t.Fatalf("stream did not complete: %q", o.err)
	}
	if o.e2eMs >= float64(held/time.Millisecond)/2 {
		t.Errorf("e2e %.0fms includes the interval the body was held open after [DONE]", o.e2eMs)
	}
}

func TestSummaryLineNamesFlaggedAsASubset(t *testing.T) {
	got := summaryLine(3, 1, 2, 2)
	for _, want := range []string{"completed=3", "errored=1", "FLAGGED=2", "within completed", "noContent=2"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q does not contain %q", got, want)
		}
	}
}

func TestNoContentIsCountedAndSuppressesTheCleanBill(t *testing.T) {
	const reasoning = `data: {"choices":[{"delta":{"reasoning":"thinking"},"finish_reason":null}]}` + "\n\n"
	const stop = `data: {"choices":[{"delta":{},"finish_reason":"length"}]}` + "\n\ndata: [DONE]\n\n"
	cases := []struct {
		name          string
		body          string
		wantOK        bool
		wantFlag      bool
		wantNoContent bool
	}{
		{"reasoning spent the whole budget", reasoning + stop, true, false, true},
		{"content arrived", goodEvent, true, false, false},
		{"nothing at all", "data: [DONE]\n\n", true, true, true},
		{"errored", "data: not-json\n\ndata: [DONE]\n\n", false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, c.body)
			}))
			defer srv.Close()

			o, ok, flag := sweepOne(srv.Client().Transport, fixtureConfig(srv.URL, ""), 0)
			if ok != c.wantOK || flag != c.wantFlag {
				t.Fatalf("completed=%v flagged=%v, want %v and %v (err %q)", ok, flag, c.wantOK, c.wantFlag, o.err)
			}
			if got := noContentOutcome(o, ok); got != c.wantNoContent {
				t.Errorf("noContentOutcome %v, want %v (contentChunk %d)", got, c.wantNoContent, o.contentChunk)
			}
		})
	}
}

func TestTrailerWithholdsTheCleanBill(t *testing.T) {
	const bill = "no errors and no silent-drop candidates in this sweep"
	exhibit := []outcome{{idx: 0, status: 200}}
	cases := []struct {
		name      string
		total     int
		completed int64
		errored   int64
		noContent int64
		exhibits  []outcome
		wantBill  bool
		wantWarn  bool
		wantBlock bool
	}{
		{"everything delivered", 2, 2, 0, 0, nil, true, false, false},
		{"one completed with nothing", 2, 2, 0, 1, nil, false, false, false},
		{"every completed with nothing", 2, 2, 0, 2, nil, false, false, false},
		{"an exhibit and nothing missing", 2, 1, 1, 0, exhibit, false, false, true},
		{"an exhibit and something missing", 2, 1, 1, 1, exhibit, false, false, true},
		{"requests lost", 3, 1, 1, 0, nil, true, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := trailer(c.total, c.completed, c.errored, c.noContent, c.exhibits)
			if strings.Contains(got, bill) != c.wantBill {
				t.Errorf("clean bill present=%v, want %v: %q", !c.wantBill, c.wantBill, got)
			}
			if strings.Contains(got, "WARNING") != c.wantWarn {
				t.Errorf("warning present=%v, want %v: %q", !c.wantWarn, c.wantWarn, got)
			}
			if strings.Contains(got, "=== errors and candidates ===") != c.wantBlock {
				t.Errorf("exhibit block present=%v, want %v: %q", !c.wantBlock, c.wantBlock, got)
			}
		})
	}
}
