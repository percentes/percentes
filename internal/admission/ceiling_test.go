package admission

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

const releaseRef = "../../configs/release.reference.yaml"

func lim(dim string, value float64) config.Limit {
	l := config.Limit{Dimension: dim, Value: value, Marking: config.MarkingPublic, Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64)}
	if dim == config.LimitTPM || dim == config.LimitTPD {
		l.Counting = config.CountingInputOutput
	}
	if dim == config.LimitRPD || dim == config.LimitTPD {
		l.ResetZone = "UTC"
	}
	return l
}

func counted(l config.Limit, counting string, burndown float64) config.Limit {
	l.Counting, l.OutputBurndown = counting, burndown
	return l
}

func windows(l config.Limit, ws ...float64) config.Limit {
	l.EvaluationWindowsS = ws
	return l
}

func zoned(l config.Limit, zone string) config.Limit {
	l.ResetZone = zone
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
// the given limits and route cap, its rate-limit headers retained where a
// limit is confidential. The reference's other host, baseten, becomes
// groq when name is baseten.
func releaseConfig(t *testing.T, name string, ceiling int, routeUSD string, limits ...config.Limit) *config.Config {
	t.Helper()
	return shapeConfig(t, name, config.ShapeS, ceiling, nil, routeUSD, limits...)
}

// shapeConfig is releaseConfig for a route of shape at ceiling, its
// declared fallback as given.
func shapeConfig(t *testing.T, name, shape string, ceiling int, fallback *bool, routeUSD string, limits ...config.Limit) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	if name == cfg.Release.OtherHosts[0].Route {
		cfg.Release.OtherHosts[0].Route = cfg.Target.Name
	}
	pin, _ := config.LookupShape(shape)
	cfg.Target.Name = name
	cfg.Release.Shape.Name, cfg.Release.Shape.InputBoundTokens, cfg.Release.Shape.Fallback = shape, pin.InputBoundTokens, fallback
	cfg.Release.Shape.CeilingTokens = ceiling
	cfg.Target.Body = strings.Replace(cfg.Target.Body, `"max_completion_tokens": 512`, `"max_completion_tokens": `+strconv.Itoa(ceiling), 1)
	cfg.Release.Caps.RouteUSD = routeUSD
	cfg.Target.Limits = limits
	if cfg.Target.Confidential() {
		cfg.Target.RetainedHeaders = []string{"x-ratelimit-*"}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// solved is cfg's solution and its verdict at the solved binding rate,
// at the count rule's counts.
func solved(t *testing.T, cfg *config.Config, p Price, attempts []Attempt, checks []time.Time) (*Solution, *Verdict) {
	t.Helper()
	plan := ExpectedPlan(cfg, 1)
	plan.Attempts, plan.ChecksUTC = attempts, checks
	s, err := Solve(cfg, p, plan)
	if err != nil {
		t.Fatal(err)
	}
	return s, at(t, cfg, p, s.BindingRPS, attempts, checks)
}

// at is cfg's verdict at bindingRPS, at the count rule's counts.
func at(t *testing.T, cfg *config.Config, p Price, bindingRPS float64, attempts []Attempt, checks []time.Time) *Verdict {
	t.Helper()
	plan := ExpectedPlan(cfg, bindingRPS)
	plan.Attempts, plan.ChecksUTC = attempts, checks
	v, err := CeilingRule(cfg, p, plan, bindingRPS)
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

// at6 is x to six decimal places.
func at6(x float64) string { return strconv.FormatFloat(x, 'f', 6, 64) }

// The expected plan holds the count rule's counts: floor(r·T) per
// segment at the ladder's frozen step rates, 14 cells and 100 probe
// requests, here at 75.454 rps.
func TestExpectedPlan(t *testing.T) {
	cfg, err := config.LoadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		rps    float64
		warmup int
		steps  []int
	}{
		{1, 15, []int{60, 120, 180, 216, 240}},
		{75.454, 1131, []int{4527, 9054, 13581, 16297, 18108}},
		{3.007, 45, []int{180, 360, 541, 649, 721}},
	} {
		p := ExpectedPlan(cfg, c.rps)
		if p.WarmupRequests != c.warmup || !slices.Equal(p.StepRequests, c.steps) || p.Cells != 14 || p.ProbeRequests != 100 || p.Attempts != nil || p.ChecksUTC != nil {
			t.Errorf("at %g rps: plan %+v", c.rps, p)
		}
	}
}

// day is the date of 2026 month and day at hour:minute UTC.
func day(month time.Month, d, hour, minute int) time.Time {
	return time.Date(2026, month, d, hour, minute, 0, 0, time.UTC)
}

// busy is k attempts on the Pacific reset day of Tue 3 Nov 2026 (08:00
// UTC to 08:00 UTC), first attempts before re-runs, and one first
// attempt on each of two other days.
func busy(k int) []Attempt {
	out := []Attempt{{Start: day(11, 1, 12, 0)}, {Start: day(11, 6, 12, 0)}}
	for i := 0; i < k; i++ {
		out = append(out, Attempt{Start: day(11, 3, 9+3*i, 0), Rerun: i >= 5})
	}
	return out
}

// branch is one route and ceiling with its figures: the rate at each
// limit, cap and rule to six places, the binding limit and window, the
// frozen binding rate and ladder, the cell's requests and the worst cases
// of the cell and the edition.
type branch struct {
	name    string
	cfg     func(t *testing.T) *config.Config
	p       Price
	k       int
	rates   map[string]string
	binding string
	rb      float64
	ladder  []float64
	n       int
	cell    string
	edition string
}

func fallback(b bool) *bool { return &b }

func anthropicR(ceiling int, declared bool) func(*testing.T) *config.Config {
	return func(t *testing.T) *config.Config {
		return shapeConfig(t, "anthropic-R", config.ShapeR, ceiling, fallback(declared), "430",
			windows(lim(config.LimitRPM, 5000), 1, 60), lim(config.LimitInputTPM, 5000000), lim(config.LimitOutputTPM, 1000000),
			lim(config.LimitSpendMonth, 430), lim(config.LimitReloadThreshold, 30))
	}
}

func azure(name, shape string, ceiling int, routeUSD string) func(*testing.T) *config.Config {
	return func(t *testing.T) *config.Config {
		var fb *bool
		if shape == config.ShapeR {
			fb = fallback(true)
		}
		return shapeConfig(t, name, shape, ceiling, fb, routeUSD,
			windows(lim(config.LimitRPM, 250), 1, 10, 60), lim(config.LimitTPM, 250000), lim(config.LimitSpendMonth, 400))
	}
}

func gemini(ceiling int) func(*testing.T) *config.Config {
	return func(t *testing.T) *config.Config {
		return releaseConfig(t, "gemini-S", ceiling, "132.03",
			lim(config.LimitRPM, 1000), counted(lim(config.LimitTPM, 2000000), config.CountingInput, 0), spendWindow(10, 600),
			lim(config.LimitSpendMonth, 200.52), lim(config.LimitReloadThreshold, 9.40), zoned(lim(config.LimitRPD, 10000), "America/Los_Angeles"))
	}
}

func basetenCfg(ceiling int) func(*testing.T) *config.Config {
	return func(t *testing.T) *config.Config {
		return releaseConfig(t, "baseten-S", ceiling, "25", lim(config.LimitRPM, 15), lim(config.LimitTPM, 100000))
	}
}

var (
	anthropicPrice = price("2", "10")
	geminiPrice    = price("0.75", "3.75")
	basetenPrice   = price("0.10", "0.50")
)

// rateKey names a rate by its dimension and evaluation window.
func rateKey(x Rate) string {
	if x.WindowS > 0 {
		return fmt.Sprintf("%s %g s", x.Dimension, x.WindowS)
	}
	return x.Dimension
}

// The solver reproduces the release's binding rates for every route and
// branch: anthropic-R at 16, and at 256, a branch the release excludes;
// azure-S and azure-R at both ceilings; Gemini at 512 and 1,024 for k
// from 3 to 7; Baseten at both ceilings.
func TestSolveReleaseBranches(t *testing.T) {
	rpm := config.LimitRPM
	gem512 := func(k int, rpd, rb string, ladder []float64, n int, cell, edition string) branch {
		return branch{fmt.Sprintf("gemini-S512 k=%d", k), gemini(512), geminiPrice, k,
			map[string]string{rpm + " 60 s": "16.638935", config.LimitTPM + " 60 s": "27.720466", config.LimitSpendWindow + " 600 s": "5.909015",
				RouteCapDimension: "4.015743", config.LimitReloadThreshold: "4.011231", config.LimitRPD: rpd},
			config.LimitRPD, mustFloat(rb), ladder, n, cell, edition}
	}
	gem1024 := func(k int, rpd, binding, rb string, ladder []float64, n int, cell, edition string) branch {
		return branch{fmt.Sprintf("gemini-S1024 k=%d", k), gemini(1024), geminiPrice, k,
			map[string]string{rpm + " 60 s": "16.638935", config.LimitTPM + " 60 s": "27.720466", config.LimitSpendWindow + " 600 s": "3.514414",
				RouteCapDimension: "2.385631", config.LimitReloadThreshold: "2.386429", config.LimitRPD: rpd},
			binding, mustFloat(rb), ladder, n, cell, edition}
	}
	gemTop := []float64{0.596, 1.192, 1.788, 2.146, 2.385}
	for _, b := range []branch{
		{"anthropic-R16", anthropicR(16, false), anthropicPrice, 0,
			map[string]string{rpm + " 1 s": "75.454545", rpm + " 60 s": "83.194676", config.LimitInputTPM + " 60 s": "649.950083",
				config.LimitOutputTPM + " 60 s": "1039.933444", config.LimitSpendMonth: "88.839079", config.LimitReloadThreshold: "86.781450", RouteCapDimension: "88.839079"},
			rpm + " 1 s", 75.454, []float64{18.863, 37.727, 56.590, 67.908, 75.454}, 62698, "26.082368", "365.194752"},
		{"anthropic-R256", anthropicR(256, true), anthropicPrice, 0,
			map[string]string{rpm + " 1 s": "75.454545", config.LimitOutputTPM + " 60 s": "64.991681", config.LimitSpendMonth: "13.116629", config.LimitReloadThreshold: "12.819987"},
			config.LimitReloadThreshold, 12.819, []float64{3.204, 6.409, 9.614, 11.537, 12.819}, 10649, "29.987584", "420.107776"},
		{"azure-S512", azure("azure-S", config.ShapeS, 512, "280"), anthropicPrice, 0,
			map[string]string{rpm + " 1 s": "3.636364", rpm + " 10 s": "4.059406", rpm + " 60 s": "4.159734", config.LimitTPM + " 60 s": "2.429285", RouteCapDimension: "3.191855"},
			config.LimitTPM + " 60 s", 2.429, []float64{0.607, 1.214, 1.821, 2.186, 2.429}, 2015, "15.1528", "212.8912"},
		{"azure-S1024", azure("azure-S", config.ShapeS, 1024, "280"), anthropicPrice, 0,
			map[string]string{rpm + " 1 s": "3.636364", config.LimitTPM + " 60 s": "1.863561", RouteCapDimension: "1.895470"},
			config.LimitTPM + " 60 s", 1.863, []float64{0.465, 0.931, 1.397, 1.676, 1.863}, 1545, "19.5288", "274.6672"},
		{"azure-R16", azure("azure-R", config.ShapeR, 16, "120"), anthropicPrice, 0,
			map[string]string{rpm + " 1 s": "3.636364", rpm + " 10 s": "4.059406", rpm + " 60 s": "4.159734", config.LimitTPM + " 60 s": "28.885191", RouteCapDimension: "24.786104"},
			rpm + " 1 s", 3.636, []float64{0.909, 1.818, 2.727, 3.272, 3.636}, 3019, "1.255904", "17.624256"},
		{"azure-R256", azure("azure-R", config.ShapeR, 256, "120"), anthropicPrice, 0,
			map[string]string{rpm + " 1 s": "3.636364", config.LimitTPM + " 60 s": "10.831947", RouteCapDimension: "3.654258"},
			rpm + " 1 s", 3.636, []float64{0.909, 1.818, 2.727, 3.272, 3.636}, 3019, "8.501504", "119.302656"},
		gem512(3, "4.010028", "4.010", []float64{1.002, 2.005, 3.007, 3.609, 4.010}, 3330, "9.3906", "131.7504"),
		gem512(4, "3.007220", "3.007", []float64{0.751, 1.503, 2.255, 2.706, 3.007}, 2496, "7.03872", "98.82408"),
		gem512(5, "2.405535", "2.405", []float64{0.601, 1.202, 1.803, 2.164, 2.405}, 1996, "5.62872", "79.08408"),
		gem512(6, "2.004412", "2.004", []float64{0.501, 1.002, 1.503, 1.803, 2.004}, 1662, "4.68684", "65.89776"),
		gem512(7, "1.717896", "1.717", []float64{0.429, 0.858, 1.287, 1.545, 1.717}, 1422, "4.01004", "56.42256"),
		gem1024(3, "4.010028", RouteCapDimension, "2.385", gemTop, 1980, "9.3852", "131.8668"),
		gem1024(4, "3.007220", RouteCapDimension, "2.385", gemTop, 1980, "9.3852", "131.8668"),
		gem1024(5, "2.405535", RouteCapDimension, "2.385", gemTop, 1980, "9.3852", "131.8668"),
		gem1024(6, "2.004412", config.LimitRPD, "2.004", []float64{0.501, 1.002, 1.503, 1.803, 2.004}, 1662, "7.87788", "110.76432"),
		gem1024(7, "1.717896", config.LimitRPD, "1.717", []float64{0.429, 0.858, 1.287, 1.545, 1.717}, 1422, "6.74028", "94.83792"),
		{"baseten-S512", basetenCfg(512), basetenPrice, 0,
			map[string]string{rpm + " 60 s": "0.249584", config.LimitTPM + " 60 s": "0.965058", RouteCapDimension: "5.706495"},
			rpm + " 60 s", 0.249, []float64{0.062, 0.124, 0.186, 0.224, 0.249}, 202, "0.075952", "1.100928"},
		{"baseten-S1024", basetenCfg(1024), basetenPrice, 0,
			map[string]string{rpm + " 60 s": "0.249584", config.LimitTPM + " 60 s": "0.732113", RouteCapDimension: "3.391522"},
			rpm + " 60 s", 0.249, []float64{0.062, 0.124, 0.186, 0.224, 0.249}, 202, "0.127664", "1.850496"},
	} {
		cfg := b.cfg(t)
		var attempts []Attempt
		if b.k > 0 {
			attempts = busy(b.k)
		}
		s, v := solved(t, cfg, b.p, attempts, nil)
		got := map[string]string{}
		for _, x := range s.Rates {
			got[rateKey(x)] = at6(x.RPS)
		}
		for key, want := range b.rates {
			if got[key] != want {
				t.Errorf("%s: rate at %s %s, want %s (all %v)", b.name, key, got[key], want, got)
			}
		}
		if rateKey(s.Binding) != b.binding || s.BindingRPS != b.rb {
			t.Errorf("%s: binding %s at %g, want %s at %g", b.name, rateKey(s.Binding), s.BindingRPS, b.binding, b.rb)
		}
		if b.k > 0 && (s.Binding.Dimension == config.LimitRPD) != (s.Binding.Attempts == b.k) {
			t.Errorf("%s: binding %+v reserves %d attempts, want %d", b.name, s.Binding, s.Binding.Attempts, b.k)
		}
		var rates []float64
		for _, st := range v.Steps {
			rates = append(rates, st.RPS)
			if !st.Admitted || st.Binding != nil {
				t.Errorf("%s: step %g at its own binding rate: admitted %t, binding %+v", b.name, st.RPS, st.Admitted, st.Binding)
			}
		}
		n := v.Plan.WarmupRequests
		for _, c := range v.Plan.StepRequests {
			n += c
		}
		if !slices.Equal(rates, b.ladder) || n != b.n || v.CellUSD != b.cell || v.EditionUSD != b.edition || v.TopRPS != b.rb || v.BindingRPS != b.rb {
			t.Errorf("%s: ladder %v, %d requests a cell, cell US$%s, edition US$%s, top %g; want %v, %d, US$%s, US$%s", b.name, rates, n, v.CellUSD, v.EditionUSD, v.TopRPS, b.ladder, b.n, b.cell, b.edition)
		}
		if err := CheckPublished(cfg, v); err != nil {
			t.Errorf("%s: the rule's own verdict: %v", b.name, err)
		}
		// A rate above a window rate fails that window's test at the top step.
		if s.Binding.WindowS > 0 {
			above := math.Round(b.rb*1000+1) / 1000
			over := at(t, cfg, b.p, above, attempts, nil)
			if top := over.Steps[4]; top.Admitted || rateKey(Rate{Dimension: top.Binding.Dimension, WindowS: top.Binding.WindowS}) != b.binding {
				t.Errorf("%s at %g: top step admitted %t, binding %+v", b.name, above, top.Admitted, top.Binding)
			}
		}
	}
}

func mustFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}
	return f
}

// anthropic-R declares no fallback, so its configuration cannot run at
// 256 tokens, and a route that may declares its 256 branch.
func TestShapeRFallbackDeclared(t *testing.T) {
	cfg := anthropicR(16, false)(t)
	cfg.Release.Shape.CeilingTokens = 256
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "release.shape.ceiling_tokens: must be one of [16]") {
		t.Errorf("anthropic-R at 256 with no fallback declared: %v", err)
	}
	if got := ReleaseProbeTokens(anthropicR(16, false)(t)); got != (Tokens{Input: 128, Output: 16}) {
		t.Errorf("anthropic-R check tokens %+v", got)
	}
	if got := ReleaseProbeTokens(azure("azure-R", config.ShapeR, 16, "120")(t)); got != (Tokens{Input: 128, Output: 256}) {
		t.Errorf("azure-R check tokens %+v", got)
	}
}

// The daily test reserves each Pacific reset day: an attempt counts on
// every day its interval, from 15 minutes before its start to 43.5
// minutes after it, touches; at most two re-runs count on one day; the
// 25-hour day of Sun 1 Nov 2026 runs to 08:00 UTC; ceiling-check requests
// count on the day they were sent. Gemini at 512 with three attempts and
// the 40 checks on the freeze day's reset day is bound at 3.993 rps.
func TestDailyResetDays(t *testing.T) {
	cfg := gemini(512)(t)
	r, err := newRule(cfg, geminiPrice, Plan{StepRequests: make([]int, 5), Cells: 14, Attempts: []Attempt{
		{Start: day(11, 3, 8, 10)},              // 23:55 PST Mon 2 Nov to 08:53:30 UTC: Mon 2 and Tue 3 Nov
		{Start: day(11, 2, 7, 30)},              // 23:15 PST Sun 1 Nov to 00:13:30 PST Mon 2 Nov: both days
		{Start: day(11, 1, 7, 20)},              // 00:05 to 01:03:30 PDT Sun 1 Nov: that day only
		{Start: day(11, 4, 12, 0), Rerun: true}, // three re-runs on Wed 4 Nov
		{Start: day(11, 4, 16, 0), Rerun: true},
		{Start: day(11, 4, 20, 0), Rerun: true},
	}, ChecksUTC: []time.Time{day(11, 4, 10, 0)}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range r.resetDays(&cfg.Target.Limits[5]) {
		got = append(got, fmt.Sprintf("%s %d+%d+%d k=%d", d.date, d.first, d.reruns, d.checks, d.attempts()))
	}
	want := []string{"2026-11-01 2+0+0 k=2", "2026-11-02 2+0+0 k=2", "2026-11-03 1+0+0 k=1", "2026-11-04 0+3+1 k=2"}
	if !slices.Equal(got, want) {
		t.Errorf("reset days %q, want %q", got, want)
	}

	freeze := make([]time.Time, 40)
	for i := range freeze {
		freeze[i] = day(10, 30, 10, i)
	}
	attempts := []Attempt{{Start: day(10, 31, 0, 0)}, {Start: day(10, 31, 4, 0)}, {Start: day(10, 31, 0, 40), Rerun: true}, {Start: day(11, 2, 12, 0)}}
	s, v := solved(t, cfg, geminiPrice, attempts, freeze)
	if s.Binding.Dimension != config.LimitRPD || s.Binding.ResetDay != "2026-10-30" || s.Binding.Attempts != 3 || at6(s.Binding.RPS) != "3.993983" || s.BindingRPS != 3.993 {
		t.Errorf("freeze-day binding %+v at %g", s.Binding, s.BindingRPS)
	}
	top := demandOf(t, &v.Steps[4], config.LimitRPD)
	n := v.Plan.WarmupRequests
	for _, c := range v.Plan.StepRequests {
		n += c
	}
	if top.Demand != float64(3*(n+1)+40) || top.Demand > 10000 || !top.Pass || top.ResetDay != "2026-10-30" || top.Attempts != 3 {
		t.Errorf("freeze-day demand %+v for %d requests a cell", top, n)
	}

	// Gemini at 512 with three attempts on its busiest day reserves
	// 3 × 3,331 = 9,993 requests.
	_, v = solved(t, cfg, geminiPrice, busy(3), nil)
	if d := demandOf(t, &v.Steps[4], config.LimitRPD); d.Demand != 9993 || d.ResetDay != "2026-11-03" || d.Attempts != 3 || !d.Pass {
		t.Errorf("busiest day %+v", d)
	}
	// A daily limit is refused without the schedule's attempts.
	if _, err := Solve(cfg, geminiPrice, Plan{StepRequests: make([]int, 5), Cells: 14}); err == nil || !strings.Contains(err.Error(), "requests_per_day is tested over the frozen schedule's cell attempts") {
		t.Errorf("no attempts: %v", err)
	}
}

// Each dimension can bind at its own rate, and a token limit counts a
// request under its counting.
func TestSolveDimensions(t *testing.T) {
	groq := price("0.15", "0.60")
	const c512, cGroq = 0.00752, 0.0004872
	for _, tc := range []struct {
		name     string
		p        Price
		routeUSD string
		limits   []config.Limit
		binding  string
		rps      float64
	}{
		{"concurrency: 100 over the 90 s timeout", groq, "10", []config.Limit{lim(config.LimitConcurrency, 100)}, config.LimitConcurrency, 100.0 / 90},
		{"per-second rate", groq, "10", []config.Limit{lim(config.LimitRPS, 1)}, config.LimitRPS, 1 / 1.1},
		{"input tokens per minute", anthropicPrice, "400", []config.Limit{lim(config.LimitInputTPM, 120000)}, config.LimitInputTPM, 100 / 60.1},
		{"output tokens per minute", anthropicPrice, "400", []config.Limit{lim(config.LimitOutputTPM, 51200)}, config.LimitOutputTPM, 100 / 60.1},
		{"one cell under the reload threshold", anthropicPrice, "400", []config.Limit{lim(config.LimitReloadThreshold, 4)}, config.LimitReloadThreshold, 4 / (831 * c512)},
		{"monthly cap covers the edition", anthropicPrice, "400", []config.Limit{lim(config.LimitSpendMonth, 50)}, config.LimitSpendMonth, (50/c512 - 100) / (14 * 831)},
		{"route ledger cap", groq, "2", []config.Limit{lim(config.LimitRPM, 1000)}, RouteCapDimension, (2/cGroq - 100) / (14 * 831)},
		{"spend window", groq, "10", []config.Limit{spendWindow(0.1, 600)}, config.LimitSpendWindow, 205 / 600.1},
		{"first least in order binds", anthropicPrice, "400", []config.Limit{windows(lim(config.LimitRPM, 60), 1, 60), lim(config.LimitRPS, 1)}, config.LimitRPM, 1 / 1.1},
	} {
		s, v := solved(t, releaseConfig(t, "route", 512, tc.routeUSD, tc.limits...), tc.p, nil, nil)
		if s.Binding.Dimension != tc.binding || !near(s.Binding.RPS, tc.rps) || s.BindingRPS != math.Floor(tc.rps*1000)/1000 {
			t.Errorf("%s: binding %+v at %g, want %s at %g", tc.name, s.Binding, s.BindingRPS, tc.binding, tc.rps)
		}
		if len(v.Admitted()) != 5 {
			t.Errorf("%s: admitted %v at its binding rate", tc.name, v.Admitted())
		}
	}

	for _, c := range []struct {
		l         config.Limit
		allowance int64
		demand    float64
	}{
		{lim(config.LimitTPM, 1e9), 584112, 60 * 1712},
		{counted(lim(config.LimitTPM, 1e9), config.CountingInput, 0), 833333, 60 * 1200},
		{counted(lim(config.LimitTPM, 1e9), config.CountingOutputBurndown, 15), 112612, 60 * (1200 + 15*512)},
		{lim(config.LimitOutputTPM, 1e9), 1953125, 60 * 512},
	} {
		v := at(t, releaseConfig(t, "route", 512, "10", c.l), groq, 1, nil, nil)
		if d := demandOf(t, stepAt(t, v, 1), c.l.Dimension); d.Demand != c.demand || d.Allowance != c.allowance || d.WindowS != 60 {
			t.Errorf("%s counting %q: 1 rps demand %g, allowance %d, want %g and %d", c.l.Dimension, c.l.Counting, d.Demand, d.Allowance, c.demand, c.allowance)
		}
	}

	// Daily tokens: the busiest day's attempts at 1,712 tokens a request.
	cfg := releaseConfig(t, "route", 512, "10", lim(config.LimitTPD, 3*1712*1000))
	s, v := solved(t, cfg, groq, []Attempt{{Start: day(11, 3, 0, 0)}, {Start: day(11, 3, 4, 0)}, {Start: day(11, 3, 8, 0)}}, nil)
	want := (3000.0 - 3) / (3 * 831)
	if s.Binding.Dimension != config.LimitTPD || !near(s.Binding.RPS, want) || s.Binding.Attempts != 3 || s.Binding.ResetDay != "2026-11-03" {
		t.Errorf("daily tokens binding %+v, want %g", s.Binding, want)
	}
	if d := demandOf(t, &v.Steps[4], config.LimitTPD); !d.Pass || d.Demand > 3*1712*1000 {
		t.Errorf("daily tokens at the binding rate %+v", d)
	}
}

func unpublished(dims ...string) []config.UnpublishedLimit {
	var out []config.UnpublishedLimit
	for _, d := range dims {
		out = append(out, config.UnpublishedLimit{Dimension: d, Source: "limits.png", Retrieved: "2026-10-07", SHA256: strings.Repeat("0", 64), Ruled: "2026-10-08", Ruling: "rulings.md"})
	}
	return out
}

// An unpublished limit admits 0.5 rps as a cover test, after every
// recorded limit and before the route cap.
func TestCeilingRuleUnpublished(t *testing.T) {
	groq := price("0.15", "0.60")
	cfg := releaseConfig(t, "groq", 512, "10", lim(config.LimitRPM, 1000), lim(config.LimitTPM, 250000))
	cfg.Target.UnpublishedLimits = unpublished("acceleration", "concurrent_requests")
	s, err := Solve(cfg, groq, ExpectedPlan(cfg, 1))
	if err != nil {
		t.Fatal(err)
	}
	if s.Binding != (Rate{Dimension: "unpublished:acceleration", Limit: 0, Unpublished: true, RPS: 0.5}) || s.BindingRPS != 0.5 {
		t.Fatalf("binding %+v at %g", s.Binding, s.BindingRPS)
	}
	v := at(t, cfg, groq, 1, nil, nil)
	for _, st := range v.Steps {
		var dims []string
		for _, d := range st.Demands {
			dims = append(dims, d.Dimension)
		}
		if want := []string{config.LimitRPM, config.LimitTPM, "unpublished:acceleration", "unpublished:concurrent_requests", RouteCapDimension}; !slices.Equal(dims, want) {
			t.Errorf("step %g demands %q, want %q", st.RPS, dims, want)
		}
		for k, dim := range []string{"unpublished:acceleration", "unpublished:concurrent_requests"} {
			d := demandOf(t, &st, dim)
			want := Demand{Dimension: dim, Limit: k, Unpublished: true, Value: 0.5, Demand: st.RPS, Fraction: st.RPS / 0.5, Pass: st.RPS <= 0.5}
			if d != want {
				t.Errorf("step %g: %+v, want %+v", st.RPS, d, want)
			}
		}
	}
	if !slices.Equal(v.Admitted(), []float64{0.25, 0.5}) {
		t.Errorf("admitted at 1 rps: %v", v.Admitted())
	}
	if b := stepAt(t, v, 0.75).Binding; b == nil || b.Dimension != "unpublished:acceleration" || !b.Unpublished || b.Limit != 0 {
		t.Errorf("0.75 rps binding %+v", b)
	}
	b, err := json.Marshal(demandOf(t, stepAt(t, v, 1), "unpublished:acceleration"))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"dimension":"unpublished:acceleration","limit":0,"unpublished":true,"value":0.5,"demand":1,"fraction":2,"pass":false}`; string(b) != want {
		t.Errorf("JSON %s, want %s", b, want)
	}
	b, _ = json.Marshal(demandOf(t, stepAt(t, v, 1), config.LimitRPM))
	if want := `{"dimension":"requests_per_minute","limit":0,"value":1000,"window_s":60,"allowance":1000,"rate_rps":16.638935108153078,"demand":60,"fraction":0.06,"pass":true}`; string(b) != want {
		t.Errorf("recorded limit JSON %s, want %s", b, want)
	}
}

// The rule refuses a configuration outside the release profile or that
// does not validate, a plan off the ladder, an unreadable price and a
// binding rate that is not positive or not rounded down to 0.001.
func TestCeilingRuleRefuses(t *testing.T) {
	cfg := releaseConfig(t, "groq", 512, "10", lim(config.LimitRPM, 1000))
	good := price("0.15", "0.60")
	plan := ExpectedPlan(cfg, 1)
	if _, err := CeilingRule(cfg, good, plan, 1); err != nil {
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
	negative := plan
	negative.StepRequests = []int{120, 240, -1, 960, 1920}
	for name, err := range map[string]error{
		"pilot config":    func() error { _, err := CeilingRule(pilot, good, plan, 1); return err }(),
		"invalid config":  func() error { _, err := CeilingRule(invalid, good, plan, 1); return err }(),
		"four steps":      func() error { _, err := CeilingRule(cfg, good, short, 1); return err }(),
		"no cells":        func() error { _, err := CeilingRule(cfg, good, noCells, 1); return err }(),
		"negative step":   func() error { _, err := CeilingRule(cfg, good, negative, 1); return err }(),
		"price":           func() error { _, err := CeilingRule(cfg, price("0.15", "x"), plan, 1); return err }(),
		"unrounded rate":  func() error { _, err := CeilingRule(cfg, good, plan, 75.4545); return err }(),
		"zero rate":       func() error { _, err := CeilingRule(cfg, good, plan, 0); return err }(),
		"solve: no cells": func() error { _, err := Solve(cfg, good, noCells); return err }(),
	} {
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// confidential is l marked confidential, its retained snapshot's
// HMAC-SHA256 under the release key recorded as sha.
func confidential(l config.Limit, sha string) config.Limit {
	l.Marking, l.Source, l.SHA256, l.HMACSHA256 = config.MarkingConfidential, "console-limits.png", "", sha
	return l
}

// A confidential limit sets no rate: it is tested at the binding rate.
// Its demand carries the limit's dimension, source and keyed hash and
// the test's result, and no value, fraction, allowance, rate or SHA-256;
// a step it fails names it the same way, and public limits keep their
// values.
func TestCeilingRuleConfidential(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	groq := price("0.15", "0.60")
	cfg := releaseConfig(t, "groq", 512, "10", lim(config.LimitRPM, 1000),
		confidential(lim(config.LimitTPM, 80001), sha), confidential(lim(config.LimitSpendMonth, 2501), sha))
	s, err := Solve(cfg, groq, ExpectedPlan(cfg, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range s.Rates {
		if x.Limit > 0 {
			t.Errorf("a confidential limit sets a rate: %+v", x)
		}
	}
	// 0.9 rps sends 54.09 requests in 60.1 s, above the 46 that 80,001
	// tokens a minute admit at 1,712 a request.
	v := at(t, cfg, groq, 1, nil, nil)
	if !slices.Equal(v.Admitted(), []float64{0.25, 0.5, 0.75}) {
		t.Fatalf("admitted %v", v.Admitted())
	}
	for _, st := range v.Steps {
		if d := demandOf(t, &st, config.LimitRPM); d.Confidential || d.Value != 1000 || d.Source != "" || d.Allowance != 1000 {
			t.Errorf("step %g: public demand %+v", st.RPS, d)
		}
		for _, dim := range []string{config.LimitTPM, config.LimitSpendMonth} {
			d := demandOf(t, &st, dim)
			if !d.Confidential || d.Value != 0 || d.Fraction != 0 || d.Allowance != 0 || d.RateRPS != 0 || d.Source != "console-limits.png" || d.HMACSHA256 != sha || d.Demand == 0 {
				t.Errorf("step %g: confidential demand %+v", st.RPS, d)
			}
		}
		if tpm := demandOf(t, &st, config.LimitTPM); tpm.Pass != (st.RPS <= 0.75) || tpm.WindowS != 60 {
			t.Errorf("step %g: tokens per minute pass %t, window %g", st.RPS, tpm.Pass, tpm.WindowS)
		}
		if month := demandOf(t, &st, config.LimitSpendMonth); !month.Pass || month.WindowS != 0 {
			t.Errorf("step %g: monthly cap pass %t, window %g", st.RPS, month.Pass, month.WindowS)
		}
	}
	if b := stepAt(t, v, 0.9).Binding; b == nil || b.Dimension != config.LimitTPM || !b.Confidential || b.Source != "console-limits.png" || b.HMACSHA256 != sha || b.Value != 0 {
		t.Errorf("binding limit at 0.9 rps %+v", b)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"80001", "2501", "8.0001e+0", "2.501e+0", `"allowance":46`} {
		if strings.Contains(string(raw), s) {
			t.Errorf("the verdict as published carries %s", s)
		}
	}
	var back struct {
		Steps []struct {
			Demands []map[string]any `json:"demands"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	for k, s := range back.Steps {
		for _, d := range s.Demands {
			_, value := d["value"]
			_, fraction := d["fraction"]
			_, plain := d["sha256"]
			if d["confidential"] == true && (value || fraction || plain || d["hmac_sha256"] != sha) || d["confidential"] != true && (!value || !fraction) {
				t.Errorf("step %d: demand as published %v", k+1, d)
			}
		}
	}
	if err := CheckPublished(cfg, v); err != nil {
		t.Errorf("the rule's own verdict: %v", err)
	}
}

// CheckPublished refuses a verdict that disagrees with the recorded
// limits: a confidential value, fraction or allowance published, a pass
// the recorded value does not give, another source or hash, a marking
// that differs, a public value or allowance that differs, another
// dimension, window or test, and a limit index outside target.limits, in
// a demand, a binding limit or the solution.
func TestCheckPublished(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	groq := price("0.15", "0.60")
	cfg := releaseConfig(t, "groq", 512, "10", windows(lim(config.LimitRPM, 1000), 1, 60), confidential(lim(config.LimitTPM, 80001), sha))
	tpm := func(v *Verdict) *Demand { return &stepAt(t, v, 0.75).Demands[2] }
	rpm := func(v *Verdict) *Demand { return &stepAt(t, v, 0.75).Demands[1] }
	for _, tc := range []struct {
		name   string
		mutate func(v *Verdict)
		want   string
	}{
		{"value", func(v *Verdict) { tpm(v).Value = 80001 }, "step 3, tokens_per_minute: a confidential limit published with its value or fraction"},
		{"fraction", func(v *Verdict) { tpm(v).Fraction = 0.64 }, "step 3, tokens_per_minute: a confidential limit published with its value or fraction"},
		{"allowance", func(v *Verdict) { tpm(v).Allowance = 46 }, "step 3, tokens_per_minute: a confidential limit published with its value or fraction"},
		{"pass", func(v *Verdict) { tpm(v).Pass = false }, "step 3, tokens_per_minute: pass true against the recorded value, published false"},
		{"source", func(v *Verdict) { tpm(v).Source = "other.png" }, `step 3, tokens_per_minute: source "other.png"`},
		{"hash", func(v *Verdict) { tpm(v).HMACSHA256 = strings.Repeat("0", 64) }, "step 3, tokens_per_minute: source"},
		{"public hash", func(v *Verdict) { rpm(v).HMACSHA256 = sha }, "step 3, requests_per_minute: value 1000"},
		{"unmarked", func(v *Verdict) { tpm(v).Confidential = false }, "step 3, tokens_per_minute: confidential false, target.limits[1] is marked confidential"},
		{"public value", func(v *Verdict) { rpm(v).Value = 999 }, "step 3, requests_per_minute: value 999"},
		{"public allowance", func(v *Verdict) { rpm(v).Allowance = 1001 }, "step 3, requests_per_minute: allowance 1001 in 60 s, target.limits[0] admits 1000"},
		{"marked public", func(v *Verdict) { rpm(v).Confidential = true }, "step 3, requests_per_minute: confidential true, target.limits[0] is marked public"},
		{"dimension", func(v *Verdict) { tpm(v).Dimension = config.LimitRPD }, "step 3, requests_per_day: target.limits[1] is tokens_per_minute with window tests over [60] s"},
		{"window", func(v *Verdict) { rpm(v).WindowS = 10 }, "step 3, requests_per_minute: target.limits[0] is requests_per_minute with window tests over [1 60] s"},
		{"cover test", func(v *Verdict) { tpm(v).WindowS = 0 }, "step 3, tokens_per_minute: target.limits[1] is tokens_per_minute with window tests over [60] s"},
		{"index", func(v *Verdict) { tpm(v).Limit = 2 }, "step 3, tokens_per_minute: limit 2 is not in target.limits"},
		{"binding", func(v *Verdict) {
			b := *stepAt(t, v, 0.9).Binding
			b.Pass = true
			stepAt(t, v, 0.9).Binding = &b
		}, "step 4, tokens_per_minute: pass false against the recorded value, published true"},
	} {
		v := at(t, cfg, groq, 1, nil, nil)
		tc.mutate(v)
		if err := CheckPublished(cfg, v); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got %v", tc.name, tc.want, err)
		}
	}
}
