package probe

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// faultAt answers the nth request, 1-based, with fault and every other
// request with pass.
func faultAt(t *testing.T, n int32, pass, fault http.HandlerFunc) string {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) //nolint:errcheck
		if hits.Add(1) == n {
			fault(w, r)
			return
		}
		pass(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// asWritten is rec as Encode writes it and a reader decodes it.
func asWritten(t *testing.T, rec *ReleaseRecord) *ReleaseRecord {
	t.Helper()
	b, err := Encode(rec)
	if err != nil {
		t.Fatal(err)
	}
	var out ReleaseRecord
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

// statusBody answers a request with status and body.
func statusBody(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		io.WriteString(w, body) //nolint:errcheck
	}
}

// messagesEvent is one Messages stream event carrying data.
func messagesEvent(data string) string { return "event: x\ndata: " + data + "\n\n" }

// messagesPass is a Messages stream that passes the ceiling check.
var messagesPass = sseHandler(
	messagesEvent(`{"type":"message_start","message":{"model":"claude-sonnet-5-5","usage":{"input_tokens":12,"output_tokens":1}}}`),
	messagesEvent(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Billed twice."}}`),
	messagesEvent(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}`),
	messagesEvent(`{"type":"message_stop"}`))

// anthropicDocs sets cfg's error documentation and billing refusals to
// anthropic's, with the status-only 402 entry where with402 is set.
func anthropicDocs(cfg *config.Config, with402 bool) {
	const errorsPage, limitsPage = "https://platform.claude.com/docs/en/api/errors", "https://platform.claude.com/docs/en/api/rate-limits"
	doc := func(typ string, status int) config.ErrorDoc {
		return config.ErrorDoc{Type: typ, Status: status, Meaning: typ, Source: errorsPage, Retrieved: "2026-11-03"}
	}
	t := &cfg.Target
	t.ErrorDocs = []config.ErrorDoc{doc("invalid_request_error", 400), doc("authentication_error", 401), doc("billing_error", 402),
		doc("rate_limit_error", 429), doc("api_error", 500), doc("overloaded_error", 529)}
	t.BillingRefusals = []config.BillingRefusal{
		{Status: 429, BodyContains: "enforced_spend_limit_reached", Meaning: "the monthly spend cap is reached", Source: limitsPage, Retrieved: "2026-11-01"},
		{Status: 400, BodyContains: "You have reached your specified", Meaning: "a spend limit the operator set is reached", Source: limitsPage, Retrieved: "2026-11-01"},
	}
	if with402 {
		t.BillingRefusals = append(t.BillingRefusals, config.BillingRefusal{Status: 402, Meaning: "billing_error: an issue with the billing or payment information", Source: errorsPage, Retrieved: "2026-11-03"})
	}
}

// On a Messages route with anthropic's error documentation and a
// status-only 402 billing refusal, an HTTP 402 and an in-band
// billing_error each stop the check as the operator's billing setting,
// which ceiling-check prints as BILLING, and the record verifies; with no
// 402 entry the check runs and fails.
func TestAnthropic402StopsCheckAsBilling(t *testing.T) {
	noSpacing(t)
	const billingError = `{"type":"error","error":{"type":"billing_error","message":"There's an issue with your billing or payment information."}}`
	for _, c := range []struct {
		name, reason string
		n            int32
		fault        http.HandlerFunc
	}{
		{"HTTP 402", "HTTP 402, the operator's billing setting, a documented billing refusal: billing_error", 2, statusBody(http.StatusPaymentRequired, billingError)},
		{"HTTP 402 with no body", "HTTP 402, the operator's billing setting, a documented billing refusal: billing_error", 1, statusBody(http.StatusPaymentRequired, "")},
		{"in-band billing_error", "HTTP 402 in-band, the operator's billing setting, a documented billing refusal: billing_error", 3, sseHandler(
			messagesEvent(`{"type":"message_start","message":{"model":"claude-sonnet-5-5","usage":{"input_tokens":12}}}`),
			"event: error\ndata: "+billingError+"\n\n")},
	} {
		for _, with402 := range []bool{true, false} {
			cfg := messagesConfig(releaseConfig(t, faultAt(t, c.n, messagesPass, c.fault)+"/v1/messages"), 512)
			anthropicDocs(cfg, with402)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("%s: configuration: %v", c.name, err)
			}
			rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
			if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
				t.Fatalf("%s, 402 entry %t: Verify: %v", c.name, with402, err)
			}
			if !with402 {
				if rec.Outcome() != CheckRan || rec.Start.Blocked || rec.Ceiling.Pass {
					t.Errorf("%s with no 402 entry: outcome %s, start %+v, pass %t", c.name, rec.Outcome(), rec.Start, rec.Ceiling.Pass)
				}
				continue
			}
			if rec.Outcome() != CheckStopped || !rec.Start.Blocked || !strings.Contains(rec.Start.Reason, BillingSetting) ||
				!strings.HasPrefix(rec.Start.Reason, c.reason) || len(rec.Requests) != int(c.n) {
				t.Errorf("%s: outcome %s, start %+v, %d requests", c.name, rec.Outcome(), rec.Start, len(rec.Requests))
			}
		}
	}
}

// Verify refuses a stand-in for a request's body or in-band error on a
// route with no confidential limit, so a refused check cannot be
// rewritten as a billing stop; the same record from a route with a
// confidential limit verifies.
func TestVerifyRefusesStandInOnRouteThatWithholdsNothing(t *testing.T) {
	noSpacing(t)
	limited := "data: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}\n\n"
	cfg := releaseConfig(t, streamFault(t, false, answerFrame, limited, doneFrame))
	cfg.Target.ErrorDocs = []config.ErrorDoc{rateLimited}
	cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
	verify := func(r *ReleaseRecord) error {
		return r.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens)
	}
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if rec.Outcome() != CheckRefused || verify(rec) != nil {
		t.Fatalf("in-band rate limit: outcome %s, Verify %v", rec.Outcome(), verify(rec))
	}
	inBand := *rec
	inBand.Requests = append([]CheckRequest(nil), rec.Requests[:3]...)
	s := *inBand.Requests[2].Shape
	s.ErrorHMACSHA256 = "00"
	inBand.Requests[2].Shape = &s
	inBand.Start = Start{Blocked: true, Reason: inBandBillingReason(429, &spendCap)}
	inBand.Ceiling = ceilingCheck(inBand.Requests, inBand.Ceiling.Ceiling, LengthValue(cfg.Target.Adapter))
	inBand.InputBound = inputCheck(inBand.Requests, cfg.Release.Shape.InputBoundTokens)
	inBand.OutputBound = outputCheck(inBand.Requests, inBand.Ceiling.Ceiling)
	if err := verify(&inBand); err == nil || !strings.Contains(err.Error(), "request 2 carries a stand-in") {
		t.Errorf("in-band error with a stand-in on an open route: Verify %v", err)
	}

	cfg = releaseConfig(t, faultAt(t, 3, sseHandler(answerFrame, stopFrame, doneFrame), statusBody(http.StatusTooManyRequests, `{"error":{"type":"rate_limit_error"}}`))+"/v1/chat/completions")
	cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
	rec = releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if rec.Outcome() != CheckRefused || rec.RefusedBy() != "HTTP 429" {
		t.Fatalf("HTTP 429: outcome %s, refused by %q", rec.Outcome(), rec.RefusedBy())
	}
	body := *rec
	body.Requests = append([]CheckRequest(nil), rec.Requests[:3]...)
	body.Requests[2].Exchange.Body = &Body{Retained: true, HMACSHA256: "00"}
	body.Start = Start{Blocked: true, Reason: billingReason(429, &spendCap)}
	body.Ceiling = ceilingCheck(body.Requests, body.Ceiling.Ceiling, LengthValue(cfg.Target.Adapter))
	body.InputBound = inputCheck(body.Requests, cfg.Release.Shape.InputBoundTokens)
	body.OutputBound = outputCheck(body.Requests, body.Ceiling.Ceiling)
	if err := verify(&body); err == nil || !strings.Contains(err.Error(), "request 2 carries a stand-in") {
		t.Errorf("withheld body on an open route: Verify %v", err)
	}
	if withheldBilling(&cfg.Target, &body.Requests[2], body.Start.Reason) {
		t.Error("withheldBilling matched on a route with no confidential limit")
	}

	billing := "data: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"cap\",\"details\":{\"error_code\":\"enforced_spend_limit_reached\"}}}\n\n"
	cfg = releaseConfig(t, streamFault(t, false, answerFrame, billing, doneFrame))
	cfg.Target.ErrorDocs = []config.ErrorDoc{rateLimited}
	cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
	rec = releaseProbe(t, confidential(t, cfg))
	if rec.Requests[2].Shape.ErrorHMACSHA256 == "" || rec.Outcome() != CheckStopped || verify(rec) != nil {
		t.Errorf("confidential route: stand-in %q, outcome %s, Verify %v", rec.Requests[2].Shape.ErrorHMACSHA256, rec.Outcome(), verify(rec))
	}
}

// Verify re-derives a blocked record's start reason from its last
// request: a billing stop rewritten as a setup defect or as any other
// text is refused, with the body in the record and withheld.
func TestVerifyRederivesBlockedReason(t *testing.T) {
	noSpacing(t)
	const capBody = `{"error":{"type":"rate_limit_error","message":"cap","details":{"error_code":"enforced_spend_limit_reached"}}}`
	for _, withheld := range []bool{false, true} {
		cfg := releaseConfig(t, faultAt(t, 3, sseHandler(answerFrame, stopFrame, doneFrame), statusBody(http.StatusTooManyRequests, capBody))+"/v1/chat/completions")
		cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
		in := ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}
		if withheld {
			in = confidential(t, cfg)
		}
		rec := asWritten(t, releaseProbe(t, in))
		verify := func(r *ReleaseRecord) error {
			return r.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens)
		}
		if rec.Start.Reason != billingReason(429, &spendCap) || verify(rec) != nil {
			t.Fatalf("withheld %t: start %+v, Verify %v", withheld, rec.Start, verify(rec))
		}
		for _, reason := range []string{Gate(&cfg.Target, 400).Reason, "anything at all"} {
			r := *rec
			r.Start.Reason = reason
			var d *DocsError
			if err := verify(&r); err == nil || !errors.As(err, &d) {
				t.Errorf("withheld %t, reason %q: Verify %v", withheld, reason, err)
			}
		}
	}
}

// Verify marks as a DocsError the refusals the route's response
// documentation decides, and no other.
func TestVerifyMarksDocsErrors(t *testing.T) {
	noSpacing(t)
	inBand := "data: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
	cfg := releaseConfig(t, streamFault(t, false, answerFrame, inBand, doneFrame))
	cfg.Target.ErrorDocs = []config.ErrorDoc{overloaded}
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	verify := func(r *ReleaseRecord) error {
		return r.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens)
	}
	if err := verify(rec); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	docs := func(err error) bool {
		var d *DocsError
		return errors.As(err, &d)
	}
	cfg.Target.ErrorDocs[0].Status = 503
	if err := verify(rec); err == nil || !docs(err) {
		t.Errorf("error_docs edited: %v", err)
	}
	cfg.Target.ErrorDocs[0].Status = 529
	cut := *rec
	cut.Requests = rec.Requests[:9]
	if err := verify(&cut); err == nil || docs(err) {
		t.Errorf("a request left out: %v", err)
	}
	silent := *rec
	silent.Requests = append([]CheckRequest(nil), rec.Requests...)
	silent.Requests[4].Exchange.Status, silent.Requests[4].Exchange.ConnAcquiredNs = 0, 0
	if err := verify(&silent); err == nil || docs(err) {
		t.Errorf("a passed request's status cleared: %v", err)
	}
}
