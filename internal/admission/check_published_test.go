package admission

import (
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// alteredVerdicts are verdicts for cfg altered so that CheckPublished refuses them.
func alteredVerdicts(t *testing.T, cfg *config.Config) map[string]*Verdict {
	out := map[string]*Verdict{}
	base := at(t, cfg, anthropicPrice, 0.972, nil, nil)
	if err := CheckPublished(cfg, base); err != nil {
		t.Fatalf("own verdict: %v", err)
	}
	// The solution omits the tokens-per-minute (TPM) rates and binds on the next least.
	x := at(t, cfg, anthropicPrice, 0.972, nil, nil)
	var kept []Rate
	for _, r := range x.Solution.Rates {
		if r.Dimension != config.LimitTPM {
			kept = append(kept, r)
		}
	}
	least := 0
	for i := range kept {
		if kept[i].RPS < kept[least].RPS {
			least = i
		}
	}
	x.Solution.Rates, x.Solution.Binding = kept, kept[least]
	x.Solution.BindingRPS = float64(int64(kept[least].RPS*1000)) / 1000
	out["solution omits the TPM rates"] = x
	// The same solution, and the verdict's steps run at its rate with every step admitted.
	x2 := at(t, cfg, anthropicPrice, x.Solution.BindingRPS, nil, nil)
	x2.Solution = x.Solution
	for k := range x2.Steps {
		x2.Steps[k].Admitted, x2.Steps[k].Binding = true, nil
	}
	out["steps at the omitted solution's rate, all admitted"] = x2
	// Run at 1.944 requests per second, twice the rate, every step admitted.
	y := at(t, cfg, anthropicPrice, 1.944, nil, nil)
	for k := range y.Steps {
		y.Steps[k].Admitted, y.Steps[k].Binding = true, nil
	}
	out["all admitted at twice the rate"] = y
	// The base rate, a passing step marked not admitted (wrong exclusion label).
	y2 := at(t, cfg, anthropicPrice, 0.972, nil, nil)
	y2.Steps[2].Admitted = false
	y2.Steps[2].Binding = &y2.Steps[2].Demands[0]
	out["a passing step not admitted"] = y2
	// TPM demands removed at twice the rate.
	z := at(t, cfg, anthropicPrice, 1.944, nil, nil)
	for k := range z.Steps {
		var ds []Demand
		for _, d := range z.Steps[k].Demands {
			if d.Dimension != config.LimitTPM {
				ds = append(ds, d)
			}
		}
		z.Steps[k].Demands, z.Steps[k].Admitted, z.Steps[k].Binding = ds, true, nil
	}
	out["TPM demands dropped at twice the rate"] = z
	return out
}

func TestCheckPublishedRefusesAlteredVerdicts(t *testing.T) {
	cfg := azureAt("azure-S", config.ShapeS, 512, "280")(t)
	for name, v := range alteredVerdicts(t, cfg) {
		if err := CheckPublished(cfg, v); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
