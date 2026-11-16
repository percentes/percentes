package probe

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// overloaded is the error documentation a route records for the
// overloaded_error in-band type, HTTP 529.
var overloaded = config.ErrorDoc{Type: "overloaded_error", Status: 529, Meaning: "the API is temporarily overloaded", Source: "https://docs.example.test/errors", Retrieved: "2026-11-03"}

// streamFault answers the third request with frames, then aborts the
// connection where abort is set, and every other request with a passing
// stream.
func streamFault(t *testing.T, abort bool, frames ...string) string {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) != 3 {
			sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
			return
		}
		sseHandler(frames...)(w, r)
		if abort {
			panic(http.ErrAbortHandler)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/v1/chat/completions"
}

// A request answered 200 whose stream a transport error cut before its
// terminator, that ended before its terminator with no read error, or
// whose stream carries an in-band error the route's error documentation
// maps to 429 or a 5xx, refuses the check, each with its own label; an
// undocumented in-band type fails it, with or without a terminator. A
// Messages stream with an overloaded_error event and then a close
// refuses it too. Verify re-derives the in-band status.
func TestStreamFaultsRefuseOrFailCheck(t *testing.T) {
	noSpacing(t)
	inBand := "data: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
	other := "data: {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad\"}}\n\n"
	for _, c := range []struct {
		name, url, by, fail string
	}{
		{"cut", streamFault(t, true, answerFrame), RefusedStreamCut, "malformed stream"},
		{"clean end with no terminator", streamFault(t, false, answerFrame), RefusedEndedEarly, "no terminator"},
		{"documented in-band error", streamFault(t, false, answerFrame, inBand, doneFrame), "HTTP 529 in-band", "in-band error"},
		{"undocumented in-band error", streamFault(t, false, answerFrame, other, doneFrame), "", "in-band error"},
		{"undocumented in-band error, then a close", streamFault(t, false, answerFrame, other), "", "no terminator"},
	} {
		cfg := releaseConfig(t, c.url)
		cfg.Target.ErrorDocs = []config.ErrorDoc{overloaded}
		rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
		want := CheckRan
		if c.by != "" {
			want = CheckRefused
		}
		if rec.RefusedBy() != c.by || rec.Outcome() != want || rec.Requests[2].Fail != c.fail || rec.Interrupted != "" {
			t.Errorf("%s: refused by %q, outcome %s, request 2 fail %q; want %q, %s, %q", c.name, rec.RefusedBy(), rec.Outcome(), rec.Requests[2].Fail, c.by, want, c.fail)
		}
		if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
			t.Errorf("%s: Verify: %v", c.name, err)
		}
		if c.by == "HTTP 529 in-band" {
			rec.Requests[2].InBandStatus = 503
			if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err == nil {
				t.Errorf("%s: Verify of an edited in-band status: nil", c.name)
			}
		}
	}

	messages := []string{
		"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-5-5\",\"usage\":{\"input_tokens\":12}}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"The\"}}\n\n",
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
	}
	srv := httptest.NewServer(sseHandler(messages...))
	defer srv.Close()
	cfg := messagesConfig(releaseConfig(t, srv.URL+"/v1/messages"), 512)
	cfg.Target.ErrorDocs = []config.ErrorDoc{overloaded}
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if rec.RefusedBy() != "HTTP 529 in-band" || rec.Outcome() != CheckRefused || rec.Requests[0].Fail != "no terminator" || rec.Requests[0].InBandStatus != 529 {
		t.Fatalf("Messages overloaded_error, then a close: refused by %q, outcome %s, request 0 %+v", rec.RefusedBy(), rec.Outcome(), rec.Requests[0])
	}
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 529, BodyContains: "overloaded_error", Meaning: "m", Source: "s", Retrieved: "2026-11-03"}}
	if s := InBandRefusal(&cfg.Target, rec.Requests[0].Shape); s != 0 {
		t.Fatalf("an in-band error matching a billing refusal: %d", s)
	}
}

// A Shape R host group refused twice at 16 on a cut stream is excluded as
// inconclusive with the stream-cut label; a Shape S host that left its
// input bound unreported names it in the reason; reasons print numbers
// as the pages do.
func TestChooseCeilingExclusionReasons(t *testing.T) {
	sent := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	cutAt := func(seq int) *ReleaseRecord {
		r := checkedR("anthropic-R", 16, 9)
		r.LedgerSeq = seq
		r.Requests = []CheckRequest{{Exchange: Exchange{Status: http.StatusOK, DispatchUTC: sent, ConnAcquiredNs: 1}, Shape: &Shape{ReadError: "connection reset by peer"}}}
		return r
	}
	got, err := ChooseCeiling(&config.Shape{Name: config.ShapeR}, []string{"anthropic-R"}, []*ReleaseRecord{cutAt(4), cutAt(6)})
	if err != nil || got.Reason != "anthropic-R: ceiling check inconclusive, stream cut at 16" || !got.Excluded {
		t.Fatalf("Shape R cut twice: %+v, %v", got, err)
	}
	unreported := checked("baseten", 512, 10)
	unreported.InputBound = InputCheck{BoundTokens: 1200, Reported: 8, MaxPromptTokens: 1100}
	got, err = ChooseCeiling(&config.Shape{Name: config.ShapeS}, []string{"baseten"}, []*ReleaseRecord{unreported, checked("baseten", 1024, 10)})
	if want := "baseten failed at 512; baseten: the input bound unreported at 512: 8 of 10 requests reported; every host passed at 1,024"; err != nil || got.Reason != want {
		t.Fatalf("unreported, then a pass: %+v, %v; want %q", got, err, want)
	}
	again := checked("baseten", 1024, 10)
	again.InputBound = InputCheck{BoundTokens: 1200, Reported: 9, MaxPromptTokens: 1100}
	got, err = ChooseCeiling(&config.Shape{Name: config.ShapeS}, []string{"baseten"}, []*ReleaseRecord{unreported, again})
	if want := "baseten failed at 1,024; baseten: the input bound unreported at 1,024: 9 of 10 requests reported"; err != nil || !got.Excluded || got.Reason != want {
		t.Fatalf("unreported twice: %+v, %v; want %q", got, err, want)
	}
	for n, want := range map[int]string{16: "16", 1024: "1,024", 2800: "2,800", 1234567: "1,234,567", -1200: "-1,200"} {
		if got := Number(n); got != want {
			t.Errorf("Number(%d) %q, want %q", n, got, want)
		}
	}
}
