package main

import (
	"net/http"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
)

// On a route that withholds a probe's in-band error, the probe's billing
// stop is read from the retained error text, which the record's stand-in
// of the error's type alone does not give.
func TestProbeBillingFromRetainedInBandError(t *testing.T) {
	const text = `{"type":"rate_limit_error","message":"cap","details":{"error_code":"enforced_spend_limit_reached"}}`
	refusal := config.BillingRefusal{Status: 429, BodyContains: "enforced_spend_limit_reached", Meaning: "the spend cap is reached", Source: "https://docs.example.test/limits", Retrieved: "2026-11-01"}
	tg := &config.Target{ErrorDocs: []config.ErrorDoc{{Type: "rate_limit_error", Status: 429, Meaning: "a rate limit", Source: "https://docs.example.test/errors", Retrieved: "2026-11-03"}},
		BillingRefusals: []config.BillingRefusal{refusal}}
	pr := &probe.Record{Request: probe.Exchange{Status: http.StatusOK}, Shape: &probe.Shape{Error: config.RetainedError(text)}}
	if s := probeBilling(tg, pr); s != nil {
		t.Fatalf("the stand-in alone: %v", s)
	}
	pr.Request.Retained = &probe.Retained{StreamError: text}
	want := "route stop: HTTP 429 in-band, the operator's billing setting, a documented billing refusal: the spend cap is reached (https://docs.example.test/limits, retrieved 2026-11-01)"
	if s := probeBilling(tg, pr); s == nil || s.Error() != want {
		t.Fatalf("the retained error: %v; want %q", s, want)
	}
}
