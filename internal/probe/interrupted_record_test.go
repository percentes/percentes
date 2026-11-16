package probe

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/redact"
)

// messagesStart is the first event of a Messages stream.
var messagesStart = messagesEvent(`{"type":"message_start","message":{"model":"claude-sonnet-5-5","usage":{"input_tokens":12}}}`)

// messagesError is a Messages stream's in-band error event carrying inner.
func messagesError(inner string) string {
	return "event: error\ndata: {\"type\":\"error\",\"error\":" + inner + "}\n\n"
}

// checkRoute is one adapter's route for the interrupted-check tests: its
// configuration at a server's URL, a passing answer, its stream's first
// event and an in-band error event its error documentation gives 529.
type checkRoute struct {
	adapter         string
	config          func(t *testing.T, url string) *config.Config
	pass            http.HandlerFunc
	start, error529 string
}

// checkRoutes are a Messages route with anthropic's error documentation
// and a chat-completions route documenting overloaded_error as 529.
var checkRoutes = []checkRoute{
	{config.AdapterMessages, func(t *testing.T, url string) *config.Config {
		cfg := messagesConfig(releaseConfig(t, url+"/v1/messages"), 512)
		anthropicDocs(cfg, true)
		return cfg
	}, messagesPass, messagesStart, messagesError(`{"type":"overloaded_error","message":"Overloaded"}`)},
	{config.AdapterChatCompletions, func(t *testing.T, url string) *config.Config {
		cfg := releaseConfig(t, url+"/v1/chat/completions")
		cfg.Target.ErrorDocs = []config.ErrorDoc{overloaded}
		return cfg
	}, sseHandler(answerFrame, stopFrame, doneFrame), answerFrame, "data: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"},
}

// midEvent is the start of an event's data line, cut before its JSON
// closes.
const midEvent = "data: {\"partial\":"

// hangUp closes the request's connection with no response.
func hangUp(w http.ResponseWriter, _ *http.Request) {
	if c, _, err := w.(http.Hijacker).Hijack(); err == nil {
		c.Close()
	}
}

// resetConn resets the request's connection with no response.
func resetConn(w http.ResponseWriter, _ *http.Request) {
	if c, _, err := w.(http.Hijacker).Hijack(); err == nil {
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetLinger(0) //nolint:errcheck
		}
		c.Close()
	}
}

// cutAfter answers 200, sends pre, then closes the connection.
func cutAfter(pre string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, pre) //nolint:errcheck
		w.(http.Flusher).Flush()
		hangUp(w, req)
	}
}

// abortAt is r's route whose nth request, 1-based, ends ctx with an
// operator abort: after answering it with fault, 150 ms later, in the
// spacing wait; or, where fault is nil, while the request is open:
// before headers where pre is "", else 100 ms after sending 200 and pre.
func abortAt(t *testing.T, r checkRoute, n int32, cancel context.CancelCauseFunc, fault http.HandlerFunc, pre string) *config.Config {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		io.Copy(io.Discard, req.Body) //nolint:errcheck
		if hits.Add(1) != n {
			r.pass(w, req)
			return
		}
		if fault != nil {
			fault(w, req)
			go func() { time.Sleep(150 * time.Millisecond); cancel(loadgen.ErrOperatorAbort) }()
			return
		}
		if pre != "" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, pre) //nolint:errcheck
			w.(http.Flusher).Flush()
			time.Sleep(100 * time.Millisecond)
		}
		cancel(loadgen.ErrOperatorAbort)
		select {
		case <-req.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)
	return r.config(t, srv.URL)
}

// verifyRelease is rec's Verify under cfg.
func verifyRelease(rec *ReleaseRecord, cfg *config.Config) error {
	return rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens)
}

// lastError is the error the last request of rec ended on: its stream's
// read error where it has one, else its exchange's.
func lastError(rec *ReleaseRecord) string {
	q := &rec.Requests[len(rec.Requests)-1]
	if q.Shape != nil && q.Shape.ReadError != "" {
		return q.Shape.ReadError
	}
	return q.Exchange.Error
}

// On each adapter, the last request of a check interrupted by an abort
// refuses it where the provider ended it first: a refusing status, an
// in-band error, a hang-up or reset with no response and a stream cut at
// an event boundary or mid-event, each before an abort in the spacing
// wait, and an in-band error whose data line arrived whole before an
// abort mid-stream. A whole undecodable event before the cut keeps the
// cut from refusing. A last request the abort cut, before its headers,
// mid-stream or mid-event, decides nothing. Each record verifies.
func TestInterruptedCheckLeavesOutOnlyTheRequestTheAbortCut(t *testing.T) {
	for _, r := range checkRoutes {
		for _, c := range []struct {
			name, pre string
			fault     http.HandlerFunc
			outcome   string
			refusedBy string
			lastErr   string
		}{
			{"in-band 529, then an abort in the wait", "", sseHandler(r.start, r.error529), CheckRefused, "HTTP 529 in-band", ""},
			{"status 529, then an abort in the wait", "", statusBody(529, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`), CheckRefused, "HTTP 529", ""},
			{"a hang-up, then an abort in the wait", "", hangUp, CheckRefused, "no response", "connection closed"},
			{"a reset, then an abort in the wait", "", resetConn, CheckRefused, "no response", ""},
			{"a cut stream, then an abort in the wait", "", cutAfter(r.start), CheckRefused, RefusedStreamCut, "connection closed"},
			{"a stream cut mid-event, then an abort in the wait", "", cutAfter(r.start + midEvent), CheckRefused, RefusedStreamCut, "connection closed"},
			{"a whole undecodable event and a cut, then an abort in the wait", "", cutAfter(r.start + "data: {bad}\n\n"), CheckStopped, "", "connection closed"},
			{"an abort mid-stream after an in-band 529 event", r.start + r.error529, nil, CheckRefused, "HTTP 529 in-band", redact.Cancelled},
			{"an abort after an in-band 529 line with no blank line", r.start + strings.TrimSuffix(r.error529, "\n"), nil, CheckRefused, "HTTP 529 in-band", redact.Cancelled},
			{"an abort mid-stream", r.start, nil, CheckStopped, "", redact.Cancelled},
			{"an abort mid-event", r.start + midEvent, nil, CheckStopped, "", redact.Cancelled},
			{"an abort before headers", "", nil, CheckStopped, "", redact.Cancelled},
		} {
			name := r.adapter + ": " + c.name
			ctx, cancel := context.WithCancelCause(context.Background())
			cfg := abortAt(t, r, 3, cancel, c.fault, c.pre)
			rec, err := RunRelease(ctx, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap, LowestStepRPS: 4})
			cancel(nil)
			if err != nil {
				t.Fatal(err)
			}
			rec = asWritten(t, rec)
			if len(rec.Requests) != 3 || rec.Sent() != 3 || !strings.HasSuffix(rec.Interrupted, ": operator abort") || rec.Outcome() != c.outcome || rec.RefusedBy() != c.refusedBy {
				t.Errorf("%s: %d requests, %d sent, interrupted %q, outcome %s, refused by %q; want %s, %q", name, len(rec.Requests), rec.Sent(), rec.Interrupted, rec.Outcome(), rec.RefusedBy(), c.outcome, c.refusedBy)
			}
			if got := lastError(rec); c.lastErr != "" && got != c.lastErr || c.lastErr == "" && got == redact.Cancelled {
				t.Errorf("%s: last request's error %q, want %q", name, got, c.lastErr)
			}
			if q := rec.Requests[2]; c.pre != "" && (q.Exchange.Status != http.StatusOK || q.Shape == nil) {
				t.Errorf("%s: the abort did not land mid-stream: status %d, shape %t", name, q.Exchange.Status, q.Shape != nil)
			}
			if err := verifyRelease(rec, cfg); err != nil {
				t.Errorf("%s: Verify: %v", name, err)
			}
		}
	}
}

// Every interrupted record RunRelease writes verifies, on each adapter:
// an abort before the first request, and an abort at each request
// before its headers or mid-stream; so does InterruptedRecord's. Verify
// refuses an interrupted reason RunRelease does not write: one at
// another count of requests sent or another total, one with no cause
// after the count, or another reason on a record with requests. It does
// not check which cause the reason names.
func TestVerifyRefusesInterruptedReasonNotWritten(t *testing.T) {
	noSpacing(t)
	for _, r := range checkRoutes {
		var cutAt3 *ReleaseRecord
		var cutCfg *config.Config
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(loadgen.ErrOperatorAbort)
		cfg := abortAt(t, r, 99, func(error) {}, nil, "")
		rec := asWritten(t, releaseProbeCtx(t, ctx, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}))
		if want := "the check's context ended after 0 of 10 requests: operator abort"; rec.Interrupted != want || rec.Sent() != 0 || rec.Outcome() != CheckStopped {
			t.Fatalf("%s: an abort before the first request: interrupted %q, %d sent, outcome %s", r.adapter, rec.Interrupted, rec.Sent(), rec.Outcome())
		}
		if err := verifyRelease(rec, cfg); err != nil {
			t.Fatalf("%s: an abort before the first request: Verify: %v", r.adapter, err)
		}
		for n := int32(1); n <= CeilingRequests; n++ {
			for _, pre := range []string{"", r.start} {
				ctx, cancel := context.WithCancelCause(context.Background())
				cfg := abortAt(t, r, n, cancel, nil, pre)
				rec := asWritten(t, releaseProbeCtx(t, ctx, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}))
				cancel(nil)
				name := fmt.Sprintf("%s: an abort at request %d, mid-stream %t", r.adapter, n, pre != "")
				want := fmt.Sprintf("the check's context ended after %d of 10 requests: operator abort", n)
				if len(rec.Requests) != int(n) || rec.Sent() != int(n) || rec.Interrupted != want || rec.Outcome() != CheckStopped || lastError(rec) != redact.Cancelled {
					t.Errorf("%s: %d requests, %d sent, interrupted %q, outcome %s, last error %q", name, len(rec.Requests), rec.Sent(), rec.Interrupted, rec.Outcome(), lastError(rec))
				}
				if q := rec.Requests[n-1]; pre != "" && (q.Exchange.Status != http.StatusOK || q.Shape == nil) {
					t.Errorf("%s: the abort did not land mid-stream: status %d, shape %t", name, q.Exchange.Status, q.Shape != nil)
				}
				if err := verifyRelease(rec, cfg); err != nil {
					t.Errorf("%s: Verify: %v", name, err)
				}
				if n == 3 && pre != "" {
					cutAt3, cutCfg = rec, cfg
				}
			}
		}
		recovered, err := InterruptedRecord(ReleaseInput{Config: cutCfg, ByteCap: byteCap}, time.Now(), "the process ended")
		if err != nil {
			t.Fatal(err)
		}
		if err := verifyRelease(asWritten(t, recovered), cutCfg); err != nil {
			t.Fatalf("%s: InterruptedRecord: Verify: %v", r.adapter, err)
		}
		for _, reason := range []string{
			"the check's context ended after 7 of 10 requests: operator abort",
			"the check's context ended after 3 of 9 requests: operator abort",
			"the check's context ended after 03 of 10 requests: operator abort",
			"the check's context ended after 3 of 10 requests",
			"the check's context ended after 3 of 10 requests: ",
			"the check's context ended after 3 of 10 requests, then the provider refused it",
			"the check's context ended after 3 of 10 requestsXYZ",
			"the process ended",
		} {
			r2 := *cutAt3
			r2.Interrupted = reason
			if err := verifyRelease(&r2, cutCfg); err == nil || !strings.Contains(err.Error(), "the record says") {
				t.Errorf("%s: %q on 3 requests sent: Verify: %v", r.adapter, reason, err)
			}
		}
	}
}

// On each adapter, a whole check whose tenth request the provider ended
// with no response or a stream cut at an event boundary or mid-event is
// refused by it, and stays refused
// with an interrupted reason added, which Verify refuses. A provider
// hang-up at the tenth request racing an abort writes a record that
// verifies, interrupted only where its last error is the cancellation.
func TestInterruptedReasonOnWholeRefusedCheck(t *testing.T) {
	noSpacing(t)
	for _, r := range checkRoutes {
		for _, c := range []struct {
			name      string
			fault     http.HandlerFunc
			refusedBy string
		}{
			{"a hang-up", hangUp, "no response"},
			{"a cut stream", cutAfter(r.start), RefusedStreamCut},
			{"a stream cut mid-event", cutAfter(r.start + midEvent), RefusedStreamCut},
		} {
			name := r.adapter + ": " + c.name + " at request 10"
			cfg := r.config(t, faultAt(t, 10, r.pass, c.fault))
			rec := asWritten(t, releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}))
			if rec.Interrupted != "" || rec.Outcome() != CheckRefused || rec.RefusedBy() != c.refusedBy || verifyRelease(rec, cfg) != nil {
				t.Errorf("%s: interrupted %q, outcome %s, refused by %q, Verify %v", name, rec.Interrupted, rec.Outcome(), rec.RefusedBy(), verifyRelease(rec, cfg))
			}
			tampered := *rec
			tampered.Interrupted = "the check's context ended after 10 of 10 requests: operator abort"
			if tampered.Outcome() != CheckRefused || tampered.RefusedBy() != c.refusedBy {
				t.Errorf("%s, interrupted added: outcome %s, refused by %q", name, tampered.Outcome(), tampered.RefusedBy())
			}
			if err := verifyRelease(&tampered, cfg); err == nil || err.Error() != "the record is interrupted after all 10 requests and the interruption did not cut its last one" {
				t.Errorf("%s, interrupted added: Verify %v", name, err)
			}
		}
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithCancelCause(context.Background())
			cfg := r.config(t, faultAt(t, 10, r.pass, func(w http.ResponseWriter, req *http.Request) {
				hangUp(w, req)
				cancel(loadgen.ErrOperatorAbort)
			}))
			rec := asWritten(t, releaseProbeCtx(t, ctx, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}))
			cancel(nil)
			if len(rec.Requests) != CeilingRequests || (rec.Interrupted != "") != (lastError(rec) == redact.Cancelled) || verifyRelease(rec, cfg) != nil {
				t.Errorf("%s: a hang-up racing an abort at request 10: %d requests, interrupted %q, last error %q, Verify %v",
					r.adapter, len(rec.Requests), rec.Interrupted, lastError(rec), verifyRelease(rec, cfg))
			}
		}
	}
}

// releaseProbeCtx is releaseProbe under ctx.
func releaseProbeCtx(t *testing.T, ctx context.Context, in ReleaseInput) *ReleaseRecord {
	t.Helper()
	if in.LowestStepRPS == 0 {
		in.LowestStepRPS = testStepRPS
	}
	rec, err := RunRelease(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}
