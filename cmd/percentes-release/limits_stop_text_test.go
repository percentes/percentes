package main

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
)

// A binding limit that counts output at generation is named over its
// window plus the pinned hard timeout, in the limits stop and in the
// schedule print; an unflagged one over its window alone.
func TestBindingLimitSpanText(t *testing.T) {
	for _, flagged := range []bool{true, false} {
		tpm := config.Limit{Dimension: config.LimitTPM, Value: 250000, Marking: config.MarkingPublic, OutputAtGeneration: flagged}
		limits := []config.Limit{tpm}
		got := bindingText(limits, &admission.Demand{Dimension: config.LimitTPM, Limit: 0, WindowS: 60}, []evidence.RefreshedLimit{{Dimension: config.LimitTPM, Value: 100013}})
		want := "tokens_per_minute recorded 250000, read 100013: step rate above the rate it admits over 60 s"
		if flagged {
			want += " plus the 90 s hard timeout"
		}
		if got != want {
			t.Errorf("flagged %t: stop text %q, want %q", flagged, got, want)
		}
		cfg := &config.Config{}
		cfg.Target.Name, cfg.Target.Limits = "azure-S", limits
		rc := schedule.RouteConfig{Route: "azure-S", BindingRPS: 0.972, BindingLimit: config.LimitTPM, BindingWindowS: 60}
		name := bindingName(rc, []*config.Config{cfg})
		want = "tokens_per_minute over 60 s"
		if flagged {
			want += " plus the 90 s hard timeout"
		}
		if name != want {
			t.Errorf("flagged %t: schedule name %q, want %q", flagged, name, want)
		}
	}
}

// A saturated count prints as at least math.MaxInt in the breach stop.
func TestBreachStopSaturatedCountText(t *testing.T) {
	sh := &config.Shape{InputBoundTokens: 1200, CeilingTokens: 512}
	reqs := []loadgen.Request{{Usage: []byte(`{"prompt_tokens":1000,"completion_tokens":100,"completion_tokens_details":{"reasoning_tokens":99999999999999999999}}`)}}
	stop := pricingBreach(sh, reqs, nil, nil)
	if stop == nil || !strings.Contains(stop.reason, "the largest reported were 1000 prompt and at least 9223372036854775807 output tokens") {
		t.Errorf("breach stop %+v", stop)
	}
	big := math.MaxInt
	if stop := pricingBreach(sh, nil, &big, nil); stop == nil || !strings.Contains(stop.reason, "at least 9223372036854775807 prompt and 0 output tokens") {
		t.Errorf("probe breach stop %+v", stop)
	}
}

// run's binding-mismatch refusal names a limit that counts output at
// generation over its window plus the hard timeout, as the schedule
// print does.
func TestBindingMismatchRefusalSpanText(t *testing.T) {
	enoughOpenFiles(t)
	f := newFixture(t, releaseConfig(t, "https://api.example.test/openai/v1/chat/completions"), "+0.005")
	tpm := slices.IndexFunc(f.in.cfg.Target.Limits, func(l config.Limit) bool { return l.Dimension == config.LimitTPM })
	f.in.cfg.Target.Limits[tpm].OutputAtGeneration = true
	f.in.schedulePath = writeFile(t, t.TempDir(), "schedule.json", rebound(t, f, func(c *schedule.RouteConfig) {
		c.BindingLimit, c.BindingWindowS = config.LimitTPM, 60
	}))
	want := "the schedule froze 1.755 rps, tokens_per_minute over 60 s plus the 90 s hard timeout"
	if _, err := newRoute(f.in); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want %q", err, want)
	}
}

// The write allowances a cell's ladder runs under are the least of those
// the recorded limits and the limits read at the cell's start give.
func TestLimitsStopAllowances(t *testing.T) {
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	entries, _, err := readLedger(r.ledgerPath, r.ledger.Bound())
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := admission.WindowAllowances(r.rule.cfg, r.verdict)
	if err != nil {
		t.Fatal(err)
	}
	if len(recorded) != 1 || recorded[60] <= 0 || recorded[60] > 1000 {
		t.Fatalf("recorded allowances %v, want one 60 s window of at most 1000 requests", recorded)
	}
	read := func(tpm float64) []evidence.RefreshedLimit {
		return []evidence.RefreshedLimit{{Dimension: "requests_per_minute", Value: 1000}, {Dimension: "tokens_per_minute", Value: tpm}}
	}
	stop, got, err := r.limitsStop(read(500000), entries)
	if err != nil || stop != nil || !reflect.DeepEqual(got, recorded) {
		t.Fatalf("raised limit: stop %v, allowances %v, %v; want the recorded %v", stop, got, err, recorded)
	}
	stop, got, err = r.limitsStop(read(240000), entries)
	if err != nil || stop != nil || got[60] >= recorded[60] {
		t.Fatalf("lowered limit: stop %v, allowances %v, %v; want below the recorded %v", stop, got, err, recorded)
	}
}
