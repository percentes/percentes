package probe

import (
	"errors"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// On a route with a confidential limit, the written record of a check an
// in-band billing error stopped keeps only the error's type, and Verify
// accepts its billing reason from target.error_docs and the billing
// refusal at that type's status.
func TestVerifyAcceptsWithheldInBandBilling(t *testing.T) {
	noSpacing(t)
	billing := "data: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"cap\",\"details\":{\"error_code\":\"enforced_spend_limit_reached\"}}}\n\n"
	cfg := releaseConfig(t, streamFault(t, false, answerFrame, billing, doneFrame))
	cfg.Target.ErrorDocs = []config.ErrorDoc{rateLimited}
	cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
	rec := asWritten(t, releaseProbe(t, confidential(t, cfg)))
	if s := rec.Requests[2].Shape; !rec.Start.Blocked || s.Error != `{"type":"rate_limit_error"}` || s.ErrorHMACSHA256 == "" {
		t.Fatalf("start %+v, error %q, HMAC %q", rec.Start, s.Error, s.ErrorHMACSHA256)
	}
	if requestGate(&cfg.Target, &rec.Requests[2].Exchange, rec.Requests[2].Shape).Blocked {
		t.Fatal("the stand-in matches the billing refusal by itself")
	}
	if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// Verify refuses a record whose in-band billing error blocks the start
// that is not blocked, one both blocked and interrupted, and, with no
// target at hand, a recorded in-band status on a request with no in-band
// error.
func TestVerifyRefusesInconsistentInBandRecords(t *testing.T) {
	noSpacing(t)
	billing := "data: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"cap\",\"details\":{\"error_code\":\"enforced_spend_limit_reached\"}}}\n\n"
	cfg := releaseConfig(t, streamFault(t, false, answerFrame, billing, doneFrame))
	cfg.Target.ErrorDocs = []config.ErrorDoc{rateLimited}
	cfg.Target.BillingRefusals = []config.BillingRefusal{spendCap}
	blocked := asWritten(t, releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}))
	verify := func(r *ReleaseRecord, t *config.Target) error {
		return r.Verify(t, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens)
	}
	if err := verify(blocked, &cfg.Target); err != nil || !blocked.Start.Blocked {
		t.Fatalf("start %+v, Verify %v", blocked.Start, err)
	}
	unblocked := *blocked
	unblocked.Start = Start{}
	var d *DocsError
	if err := verify(&unblocked, &cfg.Target); err == nil || !errors.As(err, &d) || err.Error() != "request 2's in-band error blocks the start; the record is not blocked" {
		t.Errorf("not blocked: %v", err)
	}
	interrupted := *blocked
	interrupted.Interrupted = "the check's context ended after 3 of 10 requests: operator abort"
	if err := verify(&interrupted, &cfg.Target); err == nil || err.Error() != "the record is both interrupted and blocked" {
		t.Errorf("interrupted and blocked: %v", err)
	}

	srv := streamFault(t, false, answerFrame, stopFrame, doneFrame)
	ran := asWritten(t, releaseProbe(t, ReleaseInput{Config: releaseConfig(t, srv), Texts: texts(0), ByteCap: byteCap}))
	if err := verify(ran, nil); err != nil {
		t.Fatalf("a check that ran: %v", err)
	}
	ran.Requests[4].InBandStatus = 529
	if err := verify(ran, nil); err == nil || !strings.Contains(err.Error(), "request 4: in-band status 529 recorded on a request with no in-band error") {
		t.Errorf("an in-band status with no in-band error: %v", err)
	}
}
