package sse

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

const goodEvent = `data: {"choices":[{"delta":{"content":"Hello"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"

func collect(t *testing.T, body string, limit int) (payloads []string, sawData bool, dropped int) {
	t.Helper()
	sawData, dropped, err := Events(strings.NewReader(body), limit, func(p []byte) bool {
		payloads = append(payloads, string(p))
		return string(p) == "[DONE]"
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return payloads, sawData, dropped
}

func TestFramings(t *testing.T) {
	want := []string{`{"choices":[{"delta":{"content":"Hello"},"finish_reason":"stop"}]}`, "[DONE]"}
	cases := []struct {
		name string
		body string
	}{
		{"plain", goodEvent},
		// The grammar permits a leading byte-order mark.
		{"byte-order mark", "\xef\xbb\xbf" + goodEvent},
		// Carriage return alone, and carriage return then line feed, also end a line.
		{"carriage returns only", strings.ReplaceAll(goodEvent, "\n", "\r")},
		{"crlf", strings.ReplaceAll(goodEvent, "\n", "\r\n")},
		// One space after the colon is removed; none is also valid.
		{"no space after the colon", strings.ReplaceAll(goodEvent, "data: ", "data:")},
		// A final event without a trailing blank line is still delivered.
		{"no trailing blank line", strings.TrimRight(goodEvent, "\n")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, sawData, dropped := collect(t, c.body, 1<<20)
			if !sawData || dropped != 0 {
				t.Errorf("sawData=%v dropped=%d", sawData, dropped)
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Errorf("payloads %q, want %q", got, want)
			}
		})
	}
}

func TestDataFieldsJoinWithNewlines(t *testing.T) {
	// One event may carry several data fields; a line holding one space
	// names a field and does not end the event.
	body := "data: {\"a\":\n \ndata: 1}\n\ndata: [DONE]\n\n"
	got, _, _ := collect(t, body, 1<<20)
	if len(got) != 2 || got[0] != "{\"a\":\n1}" {
		t.Errorf("payloads %q", got)
	}
}

func TestEmptyDataFieldIsNotDelivered(t *testing.T) {
	got, sawData, _ := collect(t, "data:\n\n"+goodEvent, 1<<20)
	if !sawData || len(got) != 2 {
		t.Errorf("sawData=%v payloads %q", sawData, got)
	}
}

func TestOversizedEventIsDroppedAndCounted(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 8; i++ {
		b.WriteString("data: " + strings.Repeat("x", 256<<10) + "\n")
	}
	b.WriteString("\ndata: [DONE]\n\n")
	got, sawData, dropped := collect(t, b.String(), 1<<20)
	if !sawData || dropped != 1 {
		t.Errorf("sawData=%v dropped=%d", sawData, dropped)
	}
	if len(got) != 1 || got[0] != "[DONE]" {
		t.Errorf("a dropped event reached the caller: %q", got)
	}
}

func TestSplitLinesTerminators(t *testing.T) {
	for _, c := range []struct{ name, in string }{
		{"lf", "a\nb\nc"}, {"crlf", "a\r\nb\r\nc"}, {"cr", "a\rb\rc"}, {"mixed", "a\r\nb\rc"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			sc := bufio.NewScanner(strings.NewReader(c.in))
			sc.Split(SplitLines)
			for sc.Scan() {
				got = append(got, sc.Text())
			}
			if strings.Join(got, ",") != "a,b,c" {
				t.Fatalf("got %q", strings.Join(got, ","))
			}
		})
	}
}

func TestReadErrorIsReturned(t *testing.T) {
	_, _, err := Events(errReader{}, 1<<20, func([]byte) bool { return false })
	if err == nil {
		t.Fatal("a failing reader must surface its error")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errBoom }

var errBoom = &readErr{}

type readErr struct{}

func (*readErr) Error() string { return "boom" }

func TestOversizedLineEndsTheScan(t *testing.T) {
	body := "data: " + strings.Repeat("x", 2<<20) + "\n\ndata: [DONE]\n\n"
	var got []string
	sawData, dropped, err := Events(strings.NewReader(body), 1<<20, func(p []byte) bool {
		got = append(got, string(p))
		return false
	})
	if !errors.Is(err, bufio.ErrTooLong) || dropped != 1 || sawData {
		t.Errorf("err=%v dropped=%d sawData=%v", err, dropped, sawData)
	}
	if len(got) != 0 {
		t.Errorf("an oversized line reached the caller: %d payloads", len(got))
	}
}

// cutReader returns body, then err.
type cutReader struct {
	body string
	err  error
}

func (c *cutReader) Read(p []byte) (int, error) {
	if c.body == "" {
		return 0, c.err
	}
	n := copy(p, c.body)
	c.body = c.body[n:]
	return n, nil
}

// A pending event is delivered at the stream's end, and is cut only where
// a read error ended the stream; a whole event before the error is not.
func TestPendingEventIsCutOnlyByReadError(t *testing.T) {
	for _, c := range []struct {
		name, body string
		err        error
		want       []string
		cut        bool
	}{
		{"read error mid-event", "data: {\"a\":1}\n\ndata: {\"partial\":", errBoom, []string{`{"a":1}`, `{"partial":`}, true},
		{"read error after a whole event", "data: {\"a\":1}\n\n", errBoom, []string{`{"a":1}`}, false},
		{"clean end mid-event", "data: {\"a\":1}\n\ndata: [DONE]", io.EOF, []string{`{"a":1}`, "[DONE]"}, false},
	} {
		var got []string
		_, _, cut, err := EventsCut(&cutReader{c.body, c.err}, 1<<20, func(p []byte) bool {
			got = append(got, string(p))
			return false
		})
		if strings.Join(got, "|") != strings.Join(c.want, "|") || cut != c.cut || (err != nil) != (c.err != io.EOF) {
			t.Errorf("%s: payloads %q, cut %t, err %v; want %q, cut %t", c.name, got, cut, err, c.want, c.cut)
		}
	}
}
