package admission

import (
	"math"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// generation is l marked as counting output at generation.
func generation(l config.Limit) config.Limit {
	l.OutputAtGeneration = true
	return l
}

// A limit that counts output at generation divides each window's
// allowance by the window, 0.1 s and the 90 s hard timeout. Marked on the
// release's output-counting limits, it slows azure-S alone: 146 requests
// in 150.1 s at 512 and 112 at 1,024. Every other route keeps its binding
// rate, the azure-R and Baseten token limits and the Gemini spend window
// included.
func TestSolveOutputAtGeneration(t *testing.T) {
	flagged := map[string]func(*testing.T) *config.Config{
		"anthropic-R16": func(t *testing.T) *config.Config {
			return shapeConfig(t, "anthropic-R", config.ShapeR, 16, fallback(false), "430",
				windows(lim(config.LimitRPM, 5000), 1, 60), lim(config.LimitInputTPM, 5000000), generation(lim(config.LimitOutputTPM, 1000000)),
				lim(config.LimitSpendMonth, 430), lim(config.LimitReloadThreshold, 30))
		},
		"azure-S512":  azureAt("azure-S", config.ShapeS, 512, "280"),
		"azure-S1024": azureAt("azure-S", config.ShapeS, 1024, "280"),
		"azure-R16":   azureAt("azure-R", config.ShapeR, 16, "120"),
		"azure-R256":  azureAt("azure-R", config.ShapeR, 256, "120"),
		"gemini-S512": func(t *testing.T) *config.Config {
			return releaseConfig(t, "gemini-S", 512, "132.03",
				lim(config.LimitRPM, 1000), counted(lim(config.LimitTPM, 2000000), config.CountingInput, 0), generation(spendWindow(10, 600)),
				lim(config.LimitSpendMonth, 200.52), lim(config.LimitReloadThreshold, 9.40), zoned(lim(config.LimitRPD, 10000), "America/Los_Angeles"))
		},
		"baseten-S512": func(t *testing.T) *config.Config {
			return releaseConfig(t, "baseten-S", 512, "25", lim(config.LimitRPM, 15), generation(lim(config.LimitTPM, 100000)))
		},
	}
	plain := map[string]func(*testing.T) *config.Config{
		"anthropic-R16": anthropicR(16, false), "azure-S512": azure("azure-S", config.ShapeS, 512, "280"), "azure-S1024": azure("azure-S", config.ShapeS, 1024, "280"),
		"azure-R16": azure("azure-R", config.ShapeR, 16, "120"), "azure-R256": azure("azure-R", config.ShapeR, 256, "120"),
		"gemini-S512": gemini(512), "baseten-S512": basetenCfg(512),
	}
	prices := map[string]Price{"gemini-S512": geminiPrice, "baseten-S512": basetenPrice}
	want := map[string]struct {
		rps     float64
		rate    string
		edition string
	}{"azure-S512": {0.972, "0.972685", "85.39712"}, "azure-S1024": {0.746, "0.746169", "110.62528"}}
	for name, cfgAt := range flagged {
		cfg, p := cfgAt(t), anthropicPrice
		if q, ok := prices[name]; ok {
			p = q
		}
		var attempts []Attempt
		if strings.HasPrefix(name, "gemini") {
			attempts = busy(3)
		}
		s, v := solved(t, cfg, p, attempts, nil)
		base, _ := solved(t, plain[name](t), p, attempts, nil)
		w, slowed := want[name]
		switch {
		case !slowed && (s.BindingRPS != base.BindingRPS || rateKey(s.Binding) != rateKey(base.Binding)):
			t.Errorf("%s: binding %s at %g, unflagged %s at %g", name, rateKey(s.Binding), s.BindingRPS, rateKey(base.Binding), base.BindingRPS)
		case slowed && (s.BindingRPS != w.rps || rateKey(s.Binding) != config.LimitTPM+" 60 s" || at6(s.Binding.RPS) != w.rate || v.EditionUSD != w.edition):
			t.Errorf("%s: binding %s at %g (%s), edition US$%s; want %s at %g (%s), US$%s", name, rateKey(s.Binding), s.BindingRPS, at6(s.Binding.RPS), v.EditionUSD,
				config.LimitTPM+" 60 s", w.rps, w.rate, w.edition)
		}
		if err := CheckPublished(cfg, v); err != nil {
			t.Errorf("%s: the rule's own verdict: %v", name, err)
		}
		if !slowed {
			continue
		}
		top := v.Steps[len(v.Steps)-1]
		if d := demandOf(t, &top, config.LimitTPM); !d.Pass || at6(d.RateRPS) != w.rate {
			t.Errorf("%s: top step demand %+v", name, d)
		}
		above := math.Round(w.rps*1000+1) / 1000
		if over := at(t, cfg, p, above, nil, nil).Steps[4]; over.Admitted || over.Binding == nil || over.Binding.Dimension != config.LimitTPM {
			t.Errorf("%s at %g: top step admitted %t, binding %+v", name, above, over.Admitted, over.Binding)
		}
	}
}

// azureAt is azure's configuration with its token limit counting output
// at generation.
func azureAt(name, shape string, ceiling int, routeUSD string) func(*testing.T) *config.Config {
	return func(t *testing.T) *config.Config {
		var fb *bool
		if shape == config.ShapeR {
			fb = fallback(true)
		}
		return shapeConfig(t, name, shape, ceiling, fb, routeUSD,
			windows(lim(config.LimitRPM, 250), 1, 10, 60), generation(lim(config.LimitTPM, 250000)), lim(config.LimitSpendMonth, 400))
	}
}

// A confidential limit that counts output at generation is tested over
// the same span: at azure-S's 2.429 rps the top step fails it, and
// CheckPublished, at the pinned hard timeout, agrees with the published
// pass.
func TestCheckPublishedOutputAtGeneration(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	cfg := shapeConfig(t, "azure-S", config.ShapeS, 512, nil, "280",
		windows(lim(config.LimitRPM, 250), 1, 10, 60), confidential(generation(lim(config.LimitTPM, 250000)), sha), lim(config.LimitSpendMonth, 400))
	v := at(t, cfg, anthropicPrice, 2.429, nil, nil)
	top := v.Steps[4]
	if d := demandOf(t, &top, config.LimitTPM); d.Pass || top.Admitted {
		t.Fatalf("top step at 2.429 rps: admitted %t, token demand %+v", top.Admitted, d)
	}
	if err := CheckPublished(cfg, v); err != nil {
		t.Errorf("the rule's own verdict: %v", err)
	}
	for i := range top.Demands {
		if d := &v.Steps[4].Demands[i]; d.Dimension == config.LimitTPM {
			d.Pass = true
		}
	}
	if err := CheckPublished(cfg, v); err == nil || !strings.Contains(err.Error(), "pass false against the recorded value, published true") {
		t.Errorf("a pass over 60.1 s alone: %v", err)
	}
}
