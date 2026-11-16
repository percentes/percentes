package main

import (
	"math"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

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
