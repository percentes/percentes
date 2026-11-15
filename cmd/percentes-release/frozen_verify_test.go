package main

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/probe"
)

// frozenVerify names the frozen configuration fields on a Verify error
// those fields decide, and leaves any other Verify error as it is.
func TestFrozenVerifyNamesFieldsOnlyWhereTheyDecide(t *testing.T) {
	const frozen = "are frozen from the release's first ceiling check until the schedule, and an edit to one since then fails each record whose results it changes"
	cfg := releaseConfig(t, "http://127.0.0.1:1/openai/v1/chat/completions")
	verify := func(rec *probe.ReleaseRecord) error {
		return rec.Verify(&cfg.Target, probe.LengthValue(rec.Adapter), cfg.Release.Shape.InputBoundTokens)
	}
	rec := passedCeiling(cfg, 2000, "")
	if err := verify(rec); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	short := *rec
	short.Requests = rec.Requests[:9]
	if err := frozenVerify(verify(&short)); err == nil || strings.Contains(err.Error(), frozen) {
		t.Errorf("a request left out: %v", err)
	}
	inBand := *rec
	inBand.Requests = append([]probe.CheckRequest(nil), rec.Requests...)
	inBand.Requests[4].InBandStatus = 529
	if err := frozenVerify(verify(&inBand)); err == nil || !strings.Contains(err.Error(), "in-band status 529 recorded") || !strings.Contains(err.Error(), frozen) {
		t.Errorf("an in-band status the documentation does not give: %v", err)
	}
}
