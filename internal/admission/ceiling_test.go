package admission

import (
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

const releaseRef = "../../configs/release.reference.yaml"

func lim(dim string, value float64) config.Limit {
	l := config.Limit{Dimension: dim, Value: value, Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64)}
	if dim == config.LimitTPM || dim == config.LimitTPD {
		l.Counting = config.CountingInputOutput
	}
	return l
}

func counted(l config.Limit, counting string, burndown float64) config.Limit {
	l.Counting, l.OutputBurndown = counting, burndown
	return l
}

func spendWindow(usd, windowS float64) config.Limit {
	l := lim(config.LimitSpendWindow, usd)
	l.WindowS = windowS
	return l
}

func price(in, out string) Price {
	return Price{InputUSDPerMTok: in, OutputUSDPerMTok: out, Source: "https://example.test/pricing", Retrieved: "2026-10-07"}
}

// releaseConfig is the release reference as route name at ceiling with
// the given limits and route cap. The reference's other host, baseten,
// becomes groq when name is baseten.
func releaseConfig(t *testing.T, name string, ceiling int, routeUSD string, limits ...config.Limit) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	if name == cfg.Release.OtherHosts[0].Route {
		cfg.Release.OtherHosts[0].Route = cfg.Target.Name
	}
	cfg.Target.Name = name
	cfg.Release.Shape.CeilingTokens = ceiling
	cfg.Target.Body = strings.Replace(cfg.Target.Body, `"max_completion_tokens": 512`, `"max_completion_tokens": `+strconv.Itoa(ceiling), 1)
	cfg.Release.Caps.RouteUSD = routeUSD
	cfg.Target.Limits = limits
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func rule(t *testing.T, cfg *config.Config, p Price) *Verdict {
	t.Helper()
	v, err := CeilingRule(cfg, p, ExpectedPlan(cfg))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func stepAt(t *testing.T, v *Verdict, rps float64) *Step {
	t.Helper()
	for i := range v.Steps {
		if v.Steps[i].RPS == rps {
			return &v.Steps[i]
		}
	}
	t.Fatalf("no step %g", rps)
	return nil
}

func demandOf(t *testing.T, s *Step, dim string) Demand {
	t.Helper()
	for _, d := range s.Demands {
		if d.Dimension == dim {
			return d
		}
	}
	t.Fatalf("step %g: no %s demand", s.RPS, dim)
	return Demand{}
}

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }

// The expected plan is the pinned ladder's counts: 15 warm-up requests,
// 3,720 measured per full cell, 14 cells, six on one day, 100 probe
// requests.
func TestExpectedPlan(t *testing.T) {
	cfg, err := config.LoadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	p := ExpectedPlan(cfg)
	if p.WarmupRequests != 15 || !slices.Equal(p.StepRequests, []int{120, 240, 480, 960, 1920}) || p.Cells != 14 || p.DayCells != 6 || p.ProbeRequests != 100 {
		t.Fatalf("plan %+v", p)
	}
}

// The six release 1 routes at the limits recorded on 6 and 7 October
// give the admitted ladders and binding limits of the design's table.
func TestCeilingRuleReleaseRoutes(t *testing.T) {
	type route struct {
		name, routeUSD string
		p              Price
		limits         []config.Limit
		top            [2]float64 // at 512 and 1024
		binding        [2]string  // the first excluded step's binding dimension
	}
	burn := func(dim string, v float64) config.Limit {
		return counted(lim(dim, v), config.CountingOutputBurndown, 15)
	}
	routes := []route{
		{"groq", "10", price("0.15", "0.60"),
			[]config.Limit{lim(config.LimitRPM, 1000), lim(config.LimitTPM, 250000), lim(config.LimitRPD, 500000)},
			[2]float64{1, 0.5}, [2]string{config.LimitTPM, config.LimitTPM}},
		{"baseten", "10", price("0.10", "0.50"),
			[]config.Limit{lim(config.LimitRPM, 120), lim(config.LimitTPM, 500000)},
			[2]float64{1, 1}, [2]string{config.LimitRPM, config.LimitRPM}},
		{"anthropic", "400", price("2", "10"),
			[]config.Limit{lim(config.LimitRPM, 1000), lim(config.LimitInputTPM, 2000000), lim(config.LimitOutputTPM, 400000),
				lim(config.LimitSpendMonth, 500), lim(config.LimitReloadThreshold, 30)},
			[2]float64{4, 2}, [2]string{"", config.LimitOutputTPM}},
		{"google", "80", price("0.75", "3.75"),
			[]config.Limit{spendWindow(10, 600), lim(config.LimitSpendMonth, 250)},
			[2]float64{2, 1}, [2]string{config.LimitSpendWindow, config.LimitSpendWindow}},
		{"azure", "400", price("2", "10"),
			[]config.Limit{lim(config.LimitRPM, 1000), lim(config.LimitTPM, 1000000)},
			[2]float64{4, 2}, [2]string{"", config.LimitTPM}},
		{"bedrock", "400", price("2", "10"),
			[]config.Limit{burn(config.LimitTPM, 6000000), burn(config.LimitTPD, 700000000)},
			[2]float64{4, 2}, [2]string{"", config.LimitTPM}},
	}
	for _, r := range routes {
		for i, ceiling := range []int{512, 1024} {
			v := rule(t, releaseConfig(t, r.name, ceiling, r.routeUSD, r.limits...), r.p)
			if v.TopRPS != r.top[i] {
				t.Errorf("%s at %d: top step %g, want %g", r.name, ceiling, v.TopRPS, r.top[i])
			}
			want := []float64{}
			for _, s := range config.PinnedReleaseStepsRPS {
				if s <= r.top[i] {
					want = append(want, s)
				}
			}
			if !slices.Equal(v.Admitted(), want) {
				t.Errorf("%s at %d: admitted %v", r.name, ceiling, v.Admitted())
			}
			for _, s := range v.Steps {
				if !s.Admitted && s.Binding == nil {
					t.Errorf("%s at %d: step %g excluded with no binding limit", r.name, ceiling, s.RPS)
				}
				if s.Admitted && s.Binding != nil {
					t.Errorf("%s at %d: step %g admitted with binding %+v", r.name, ceiling, s.RPS, s.Binding)
				}
			}
			if r.binding[i] != "" {
				first := v.Steps[len(want)]
				if first.Binding.Dimension != r.binding[i] {
					t.Errorf("%s at %d: step %g bound by %s, want %s", r.name, ceiling, first.RPS, first.Binding.Dimension, r.binding[i])
				}
			}
		}
	}

	// Figures printed in the design's load-profile section.
	groq := rule(t, releaseConfig(t, "groq", 512, "10", routes[0].limits...), routes[0].p)
	if d := demandOf(t, stepAt(t, groq, 1), config.LimitTPM); d.Demand != 102720 || !near(d.Fraction, 0.41088) || !d.Half || !d.Pass {
		t.Errorf("groq 1 rps TPM: %+v", d)
	}
	if d := demandOf(t, stepAt(t, groq, 2), config.LimitTPM); d.Demand != 205440 || d.Pass {
		t.Errorf("groq 2 rps TPM: %+v", d)
	}
	if groq.PerRequestUSD != "0.0004872" || groq.ProbeUSD != "0.0007944" {
		t.Errorf("groq prices %s, %s", groq.PerRequestUSD, groq.ProbeUSD)
	}
	baseten := rule(t, releaseConfig(t, "baseten", 512, "10", routes[1].limits...), routes[1].p)
	if d := demandOf(t, stepAt(t, baseten, 1), config.LimitRPM); d.Demand != 60 || d.Fraction != 0.5 || !d.Pass {
		t.Errorf("baseten 1 rps RPM, exactly half, must pass: %+v", d)
	}
	anthropic := rule(t, releaseConfig(t, "anthropic", 512, "400", routes[2].limits...), routes[2].p)
	top := stepAt(t, anthropic, 4)
	for dim, f := range map[string]float64{config.LimitOutputTPM: 0.3072, config.LimitInputTPM: 0.144, config.LimitRPM: 0.24} {
		if d := demandOf(t, top, dim); !near(d.Fraction, f) {
			t.Errorf("anthropic 4 rps %s fraction %g, want %g", dim, d.Fraction, f)
		}
	}
	// 3,735 requests at US$0.00752; 52,290 of them plus 100 probe
	// requests at US$0.01264.
	if anthropic.CellUSD != "28.0872" || anthropic.EditionUSD != "394.4848" {
		t.Errorf("anthropic cell US$%s, edition US$%s", anthropic.CellUSD, anthropic.EditionUSD)
	}
	if d := demandOf(t, top, config.LimitReloadThreshold); d.Half || !d.Pass || d.Demand != 28.0872 {
		t.Errorf("anthropic reload threshold: %+v", d)
	}
	if d := demandOf(t, top, RouteCapDimension); d.Limit != -1 || d.Half || !d.Pass || d.Value != 400 {
		t.Errorf("anthropic route cap: %+v", d)
	}
	google := rule(t, releaseConfig(t, "google", 512, "80", routes[3].limits...), routes[3].p)
	if d := demandOf(t, stepAt(t, google, 2), config.LimitSpendWindow); !near(d.Demand, 3.384) || !near(d.Fraction, 0.3384) || !d.Pass {
		t.Errorf("google 2 rps spend window: %+v", d)
	}
	if d := demandOf(t, stepAt(t, google, 4), config.LimitSpendWindow); !near(d.Demand, 6.768) || d.Pass {
		t.Errorf("google 4 rps spend window: %+v", d)
	}
	if google.EditionUSD != "72.1302" {
		t.Errorf("google edition US$%s", google.EditionUSD)
	}
	azure := rule(t, releaseConfig(t, "azure", 512, "400", routes[4].limits...), routes[4].p)
	if d := demandOf(t, stepAt(t, azure, 4), config.LimitTPM); d.Demand != 410880 || !d.Pass {
		t.Errorf("azure 4 rps TPM: %+v", d)
	}
	bedrock := rule(t, releaseConfig(t, "bedrock", 512, "400", routes[5].limits...), routes[5].p)
	if d := demandOf(t, stepAt(t, bedrock, 4), config.LimitTPM); d.Demand != 2131200 || !d.Pass {
		t.Errorf("bedrock 4 rps TPM: %+v", d)
	}
	if d := demandOf(t, stepAt(t, bedrock, 4), config.LimitTPD); d.Demand != 199000800 || !near(d.Fraction, 199000800.0/7e8) {
		t.Errorf("bedrock six full cells a day: %+v", d)
	}
	bedrock1024 := rule(t, releaseConfig(t, "bedrock", 1024, "400", routes[5].limits...), routes[5].p)
	if d := demandOf(t, stepAt(t, bedrock1024, 2), config.LimitTPD); d.Demand != 180338400 {
		t.Errorf("bedrock six cells to 2 rps at 1024: %+v", d)
	}
	if d := demandOf(t, stepAt(t, bedrock1024, 4), config.LimitTPM); d.Demand != 3974400 || d.Pass {
		t.Errorf("bedrock 1024 4 rps TPM: %+v", d)
	}
}

// Each dimension can bind, and a token limit counts a request under its
// counting.
func TestCeilingRuleDimensions(t *testing.T) {
	groq := price("0.15", "0.60")
	anthropic := price("2", "10")
	for _, tc := range []struct {
		name     string
		p        Price
		routeUSD string
		limits   []config.Limit
		top      float64
		binding  string
	}{
		{"concurrency: 1 rps holds 90 against 50", groq, "10", []config.Limit{lim(config.LimitConcurrency, 100)}, 0.5, config.LimitConcurrency},
		{"per-second rate", groq, "10", []config.Limit{lim(config.LimitRPS, 1)}, 0.5, config.LimitRPS},
		{"daily requests: six cells to 1 rps are 5,130", groq, "10", []config.Limit{lim(config.LimitRPD, 6000)}, 0.5, config.LimitRPD},
		{"daily tokens", groq, "10", []config.Limit{lim(config.LimitTPD, 2*6*375*1712)}, 0.5, config.LimitTPD},
		{"input tokens per minute", anthropic, "400", []config.Limit{lim(config.LimitInputTPM, 300000)}, 2, config.LimitInputTPM},
		{"one cell under the reload threshold", anthropic, "400", []config.Limit{lim(config.LimitReloadThreshold, 25)}, 2, config.LimitReloadThreshold},
		{"monthly cap covers the edition", anthropic, "400", []config.Limit{lim(config.LimitSpendMonth, 300)}, 2, config.LimitSpendMonth},
		{"route ledger cap", groq, "5", []config.Limit{lim(config.LimitRPM, 1000)}, 0.5, RouteCapDimension},
		{"unverified baseten admits nothing", price("0.10", "0.50"), "10", []config.Limit{lim(config.LimitRPM, 15)}, 0, config.LimitRPM},
		{"first failed limit in order binds", anthropic, "400", []config.Limit{lim(config.LimitRPM, 100), lim(config.LimitConcurrency, 100)}, 0.5, config.LimitRPM},
	} {
		v := rule(t, releaseConfig(t, "route", 512, tc.routeUSD, tc.limits...), tc.p)
		if v.TopRPS != tc.top {
			t.Errorf("%s: top step %g, want %g", tc.name, v.TopRPS, tc.top)
			continue
		}
		first := v.Steps[len(v.Admitted())]
		if first.Binding == nil || first.Binding.Dimension != tc.binding {
			t.Errorf("%s: binding %+v, want %s", tc.name, first.Binding, tc.binding)
		}
		for _, s := range v.Steps[len(v.Admitted()):] {
			if s.Admitted || s.Binding == nil {
				t.Errorf("%s: step %g above the top step: admitted %t, binding %v", tc.name, s.RPS, s.Admitted, s.Binding)
			}
		}
		if tc.top == 0 && (v.CellUSD != "0" || v.EditionUSD != "0" || len(v.Admitted()) != 0) {
			t.Errorf("%s: %+v", tc.name, v)
		}
	}

	for _, c := range []struct {
		l    config.Limit
		want float64
	}{
		{lim(config.LimitTPM, 1e9), 60 * 1712},
		{counted(lim(config.LimitTPM, 1e9), config.CountingInput, 0), 60 * 1200},
		{counted(lim(config.LimitTPM, 1e9), config.CountingOutputBurndown, 15), 60 * (1200 + 15*512)},
		{lim(config.LimitOutputTPM, 1e9), 60 * 512},
	} {
		v := rule(t, releaseConfig(t, "route", 512, "10", c.l), groq)
		if d := demandOf(t, stepAt(t, v, 1), c.l.Dimension); d.Demand != c.want {
			t.Errorf("%s counting %q: 1 rps demand %g, want %g", c.l.Dimension, c.l.Counting, d.Demand, c.want)
		}
	}
}

func unpublished(dims ...string) []config.UnpublishedLimit {
	var out []config.UnpublishedLimit
	for _, d := range dims {
		out = append(out, config.UnpublishedLimit{Dimension: d, Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64), Ruled: "2026-10-08", Ruling: "rulings.md"})
	}
	return out
}

// An unpublished limit caps the route at 0.5 rps as a cover test, after
// every recorded limit and before the route cap.
func TestCeilingRuleUnpublished(t *testing.T) {
	groq := price("0.15", "0.60")
	limits := []config.Limit{lim(config.LimitRPM, 1000), lim(config.LimitTPM, 250000)}
	cfg := releaseConfig(t, "groq", 512, "10", limits...)
	if v := rule(t, cfg, groq); v.TopRPS != 1 {
		t.Fatalf("without a ruling: top step %g, want 1", v.TopRPS)
	}
	cfg.Target.UnpublishedLimits = unpublished("acceleration", "concurrent_requests")
	v := rule(t, cfg, groq)
	if v.TopRPS != 0.5 || !slices.Equal(v.Admitted(), []float64{0.25, 0.5}) {
		t.Fatalf("top step %g, admitted %v", v.TopRPS, v.Admitted())
	}
	for _, s := range v.Steps {
		var dims []string
		for _, d := range s.Demands {
			dims = append(dims, d.Dimension)
		}
		if want := []string{config.LimitRPM, config.LimitTPM, "unpublished:acceleration", "unpublished:concurrent_requests", RouteCapDimension}; !slices.Equal(dims, want) {
			t.Errorf("step %g demands %q, want %q", s.RPS, dims, want)
		}
		for k, dim := range []string{"unpublished:acceleration", "unpublished:concurrent_requests"} {
			d := demandOf(t, &s, dim)
			want := Demand{Dimension: dim, Limit: k, Unpublished: true, Value: 0.5, Demand: s.RPS, Fraction: s.RPS / 0.5, Pass: s.RPS <= 0.5}
			if d != want {
				t.Errorf("step %g: %+v, want %+v", s.RPS, d, want)
			}
		}
		if d := demandOf(t, &s, config.LimitRPM); d.Unpublished || d.Limit != 0 {
			t.Errorf("step %g recorded limit: %+v", s.RPS, d)
		}
	}
	if b := stepAt(t, v, 1).Binding; b == nil || b.Dimension != "unpublished:acceleration" || !b.Unpublished || b.Limit != 0 {
		t.Errorf("1 rps binding %+v", b)
	}
	b, err := json.Marshal(demandOf(t, stepAt(t, v, 1), "unpublished:acceleration"))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"dimension":"unpublished:acceleration","limit":0,"unpublished":true,"value":0.5,"demand":1,"fraction":2,"half":false,"pass":false}`; string(b) != want {
		t.Errorf("JSON %s, want %s", b, want)
	}
	if b, _ := json.Marshal(demandOf(t, stepAt(t, v, 1), config.LimitRPM)); strings.Contains(string(b), "unpublished") {
		t.Errorf("recorded limit JSON %s", b)
	}

	// A recorded limit failing at the same step binds first; the ruling
	// binds before the route cap.
	cfg = releaseConfig(t, "route", 512, "10", lim(config.LimitRPM, 15))
	cfg.Target.UnpublishedLimits = unpublished("acceleration")
	if v := rule(t, cfg, groq); v.TopRPS != 0 || v.Steps[0].Binding.Dimension != config.LimitRPM {
		t.Errorf("recorded limit first: top %g, binding %+v", v.TopRPS, v.Steps[0].Binding)
	}
	cfg = releaseConfig(t, "route", 512, "5", lim(config.LimitRPM, 1000))
	if v := rule(t, cfg, groq); v.Steps[2].Binding.Dimension != RouteCapDimension {
		t.Fatalf("route cap binds 1 rps without a ruling: %+v", v.Steps[2].Binding)
	}
	cfg.Target.UnpublishedLimits = unpublished("acceleration")
	if v := rule(t, cfg, groq); v.TopRPS != 0.5 || v.Steps[2].Binding.Dimension != "unpublished:acceleration" {
		t.Errorf("ruling before the route cap: top %g, binding %+v", v.TopRPS, v.Steps[2].Binding)
	}
}

// The rule refuses a configuration outside the release profile or that
// does not validate, a plan off the ladder, and an unreadable price.
func TestCeilingRuleRefuses(t *testing.T) {
	cfg := releaseConfig(t, "groq", 512, "10", lim(config.LimitRPM, 1000))
	good := price("0.15", "0.60")
	plan := ExpectedPlan(cfg)
	if _, err := CeilingRule(cfg, good, plan); err != nil {
		t.Fatal(err)
	}
	pilot, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	invalid := releaseConfig(t, "groq", 512, "10", lim(config.LimitRPM, 1000))
	invalid.Target.Limits[0].Value = 0
	short := plan
	short.StepRequests = plan.StepRequests[:4]
	noCells := plan
	noCells.Cells = 0
	crowdedDay := plan
	crowdedDay.DayCells = plan.Cells + 1
	negative := plan
	negative.StepRequests = []int{120, 240, -1, 960, 1920}
	for name, err := range map[string]error{
		"pilot config":   func() error { _, err := CeilingRule(pilot, good, plan); return err }(),
		"invalid config": func() error { _, err := CeilingRule(invalid, good, plan); return err }(),
		"four steps":     func() error { _, err := CeilingRule(cfg, good, short); return err }(),
		"no cells":       func() error { _, err := CeilingRule(cfg, good, noCells); return err }(),
		"crowded day":    func() error { _, err := CeilingRule(cfg, good, crowdedDay); return err }(),
		"negative step":  func() error { _, err := CeilingRule(cfg, good, negative); return err }(),
		"price":          func() error { _, err := CeilingRule(cfg, price("0.15", "x"), plan); return err }(),
	} {
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
