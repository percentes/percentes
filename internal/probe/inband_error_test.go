package probe

import (
	"net/http/httptest"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// rateLimited is the error documentation of the rate_limit_error type,
// HTTP 429.
var rateLimited = config.ErrorDoc{Type: "rate_limit_error", Status: 429, Meaning: "a rate limit", Source: "https://docs.example.test/errors", Retrieved: "2026-11-03"}

// spendCap is a billing refusal at 429 marked by its body.
var spendCap = config.BillingRefusal{Status: 429, BodyContains: "enforced_spend_limit_reached", Meaning: "the spend cap is reached", Source: "https://docs.example.test/limits", Retrieved: "2026-11-01"}

// confidential marks cfg's first limit confidential and returns a release
// key for it.
func confidential(t *testing.T, cfg *config.Config) ReleaseInput {
	l := &cfg.Target.Limits[0]
	l.Marking, l.SHA256, l.HMACSHA256 = config.MarkingConfidential, "", l.SHA256
	return ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap, Key: newKey(t)}
}

// On a route with a confidential limit the in-band error's stand-in keeps
// its type, so a documented refusing type still refuses the check, on a
// chat-completions stream and on a Messages stream.
func TestInBandErrorTypeKeptWhenWithheld(t *testing.T) {
	noSpacing(t)
	inBand := "data: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
	cfg := releaseConfig(t, streamFault(t, false, answerFrame, inBand, doneFrame))
	cfg.Target.ErrorDocs = []config.ErrorDoc{overloaded}
	rec := releaseProbe(t, confidential(t, cfg))
	if s := rec.Requests[2].Shape; s.Error != `{"type":"overloaded_error"}` || rec.RefusedBy() != "HTTP 529 in-band" {
		t.Fatalf("chat completions: error %q, refused by %q", s.Error, rec.RefusedBy())
	}
	if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
		t.Fatalf("chat completions: Verify: %v", err)
	}

	srv := httptest.NewServer(sseHandler(
		"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-5-5\",\"usage\":{\"input_tokens\":12}}}\n\n",
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
	))
	defer srv.Close()
	cfg = messagesConfig(releaseConfig(t, srv.URL+"/v1/messages"), 512)
	cfg.Target.ErrorDocs = []config.ErrorDoc{overloaded}
	rec = releaseProbe(t, confidential(t, cfg))
	if s := rec.Requests[0].Shape; s.Error != `{"type":"overloaded_error"}` || rec.RefusedBy() != "HTTP 529 in-band" {
		t.Fatalf("Messages: error %q, refused by %q", s.Error, rec.RefusedBy())
	}
}

// An in-band error that matches a documented billing refusal at its
// type's documented status stops the check as a billing refusal, its
// record blocked with the billing setting named, and Verify re-derives
// the block, also where the route withholds the error's text.
func TestInBandBillingRefusalStopsCheck(t *testing.T) {
	noSpacing(t)
	billing := "data: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"cap\",\"details\":{\"error_code\":\"enforced_spend_limit_reached\"}}}\n\n"
	limited := "data: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}\n\n"
	for _, withheld := range []bool{false, true} {
		cfg := releaseConfig(t, streamFault(t, false, answerFrame, billing, doneFrame))
		cfg.Target.ErrorDocs = []config.ErrorDoc{rateLimited}
		cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
		in := ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}
		if withheld {
			in = confidential(t, cfg)
		}
		rec := releaseProbe(t, in)
		if !rec.Start.Blocked || rec.Outcome() != CheckStopped || len(rec.Requests) != 3 ||
			rec.Start.Reason != "HTTP 429 in-band, the operator's billing setting, a documented billing refusal: the spend cap is reached (https://docs.example.test/limits, retrieved 2026-11-01)" {
			t.Fatalf("withheld %t: start %+v, outcome %s, %d requests", withheld, rec.Start, rec.Outcome(), len(rec.Requests))
		}
		if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
			t.Fatalf("withheld %t: Verify: %v", withheld, err)
		}
		cfg.Target.BillingRefusals = nil
		if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err == nil {
			t.Fatalf("withheld %t: Verify with no billing refusal: nil", withheld)
		}
	}

	cfg := releaseConfig(t, streamFault(t, false, answerFrame, limited, doneFrame))
	cfg.Target.ErrorDocs = []config.ErrorDoc{rateLimited}
	cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if rec.Start.Blocked || rec.RefusedBy() != "HTTP 429 in-band" {
		t.Fatalf("a rate_limit_error with no billing marker: start %+v, refused by %q", rec.Start, rec.RefusedBy())
	}
}
