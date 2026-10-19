package admission

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"

	"github.com/percentes/percentes/internal/config"
)

// The edition a release route's caps must cover (SPEC-HOSTED §4 grid and
// HA3 point 9): twelve cells and at most two re-runs, at most six cells
// on one UTC day (one per slot), and the probe allowance in requests.
const (
	ReleaseCells          = 12
	ReleaseReruns         = 2
	ReleaseDayCells       = 6
	ReleaseProbeAllowance = 100
)

// Plan is the traffic the ceiling rule tests a route's ladder with: one
// cell's warm-up requests and measured requests per step, the cells in
// the edition with re-runs, the most cells on one UTC day, and the probe
// requests. With exact seeded counts, a cell's counts are the largest of
// any cell's.
type Plan struct {
	WarmupRequests int   `json:"warmup_requests"`
	StepRequests   []int `json:"step_requests"`
	Cells          int   `json:"cells"`
	DayCells       int   `json:"day_cells"`
	ProbeRequests  int   `json:"probe_requests"`
}

// ExpectedPlan is the plan at the expected request counts: each step's
// rate times its window, the warm-up at the first step's rate. cfg must
// have passed Validate under the release profile.
func ExpectedPlan(cfg *config.Config) Plan {
	l := cfg.Release.Ladder
	p := Plan{
		WarmupRequests: int(math.Round(l.StepsRPS[0] * l.WarmupS)),
		Cells:          ReleaseCells + ReleaseReruns,
		DayCells:       ReleaseDayCells,
		ProbeRequests:  ReleaseProbeAllowance,
	}
	for _, r := range l.StepsRPS {
		p.StepRequests = append(p.StepRequests, int(math.Round(r*l.StepS)))
	}
	return p
}

// RouteCapDimension names the configured release.caps.route_usd in a
// Demand.
const RouteCapDimension = "route_cap_usd"

// UnpublishedPrefix opens the Dimension of a Demand against a
// target.unpublished_limits entry, before the entry's dimension.
const UnpublishedPrefix = "unpublished:"

// Demand is a step's nominal demand against one limit. A half test admits
// a demand of at most half the limit; a cover test, of at most the limit.
type Demand struct {
	Dimension string `json:"dimension"`
	// Limit indexes target.limits, or target.unpublished_limits where
	// Unpublished is set; -1 is release.caps.route_usd.
	Limit       int  `json:"limit"`
	Unpublished bool `json:"unpublished,omitempty"`
	// Confidential is set against a limit marked
	// config.MarkingConfidential: Value and Fraction are absent, and
	// Source and SHA256 are the limit's source and the SHA-256 of its
	// retained snapshot.
	Confidential bool    `json:"confidential,omitempty"`
	Source       string  `json:"source,omitempty"`
	SHA256       string  `json:"sha256,omitempty"`
	Value        float64 `json:"value,omitempty"`
	Demand       float64 `json:"demand"`
	Fraction     float64 `json:"fraction,omitempty"`
	Half         bool    `json:"half"`
	Pass         bool    `json:"pass"`
}

// Step is one ladder step's verdict. An excluded step names its binding
// limit: its first failed test in target.limits order, then
// target.unpublished_limits order, then the route cap.
type Step struct {
	RPS      float64  `json:"rps"`
	Admitted bool     `json:"admitted"`
	Binding  *Demand  `json:"binding,omitempty"`
	Demands  []Demand `json:"demands"`
}

// Verdict is the ceiling rule applied to one route at its ceiling.
// CellUSD and EditionUSD are the worst cases of the costliest cell and
// of the edition at the top admitted step.
type Verdict struct {
	Route         string  `json:"route"`
	InputTokens   int     `json:"input_tokens"`
	CeilingTokens int     `json:"ceiling_tokens"`
	PerRequestUSD string  `json:"per_request_usd"`
	ProbeUSD      string  `json:"probe_request_usd"`
	TopRPS        float64 `json:"top_rps"`
	CellUSD       string  `json:"cell_usd"`
	EditionUSD    string  `json:"edition_usd"`
	Plan          Plan    `json:"plan"`
	Steps         []Step  `json:"steps"`
}

// Admitted lists the admitted steps' rates in ladder order.
func (v *Verdict) Admitted() []float64 {
	var out []float64
	for _, s := range v.Steps {
		if s.Admitted {
			out = append(out, s.RPS)
		}
	}
	return out
}

// ReleaseTokens are the token bounds of a release route's cell requests:
// the shape's input bound and ceiling.
func ReleaseTokens(cfg *config.Config) Tokens {
	return Tokens{Input: cfg.Release.Shape.InputBoundTokens, Output: cfg.Release.Shape.CeilingTokens}
}

// ReleaseProbeTokens are the token bounds a release route's ceiling check
// is admitted at: the shape's input bound and the highest pinned ceiling.
func ReleaseProbeTokens(cfg *config.Config) Tokens {
	return Tokens{Input: cfg.Release.Shape.InputBoundTokens, Output: slices.Max(config.PinnedReleaseCeilings)}
}

// Covers reports whether the ledger prices cfg's route's requests at
// least as high as v does: its highest per-request price at ReleaseTokens
// at least v's PerRequestUSD, and at ReleaseProbeTokens at least v's
// ProbeUSD.
func (s *State) Covers(cfg *config.Config, v *Verdict) error {
	for _, c := range []struct {
		tok  Tokens
		want string
	}{{ReleaseTokens(cfg), v.PerRequestUSD}, {ReleaseProbeTokens(cfg), v.ProbeUSD}} {
		want, err := decimal(c.want)
		if err != nil {
			return fmt.Errorf("admission: ceiling rule price: %w", err)
		}
		have := s.price(cfg.Target.Name, c.tok)
		if have == nil {
			return fmt.Errorf("admission: no price recorded for %s at %d input and %d output tokens", cfg.Target.Name, c.tok.Input, c.tok.Output)
		}
		if have.Cmp(want) < 0 {
			return fmt.Errorf("admission: the ledger admits %s at US$%s a request at %d input and %d output tokens, under the ceiling rule's US$%s",
				cfg.Target.Name, decimalString(have), c.tok.Input, c.tok.Output, c.want)
		}
	}
	return nil
}

// CeilingRule applies SPEC-HOSTED HA3 point 6 to a release route at its
// configured ceiling. A request is taken at the shape's input bound and
// ceiling and priced at p; a probe request at the highest pinned
// ceiling. Rates, tokens and concurrency test at half of each limit,
// counted under the limit's counting; a daily limit takes the day's
// cells up to the step, a spend window the step's rate over the window.
// The monthly spend cap and the route's ledger cap must cover the
// edition up to the step plus the probe requests, and the auto-reload
// threshold the costliest cell. Each unpublished limit caps the step's
// rate at config.PinnedReleaseUnpublishedCapRPS. A step is admitted when
// it passes every test; every demand rises with the step, so the
// admitted steps are the ladder up to the top step. A demand against a
// confidential limit carries the test's result without the value or the
// fraction. cfg must validate.
func CeilingRule(cfg *config.Config, p Price, plan Plan) (*Verdict, error) {
	if cfg.Profile != config.ProfileRelease || cfg.Release == nil {
		return nil, errors.New("admission: ceiling rule: not a release configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("admission: ceiling rule: %w", err)
	}
	rel := cfg.Release
	steps := rel.Ladder.StepsRPS
	switch {
	case len(plan.StepRequests) != len(steps):
		return nil, fmt.Errorf("admission: ceiling rule: plan has %d steps, the ladder %d", len(plan.StepRequests), len(steps))
	case plan.WarmupRequests < 0 || plan.DayCells < 1 || plan.DayCells > plan.Cells || plan.ProbeRequests < 0 || slices.Min(plan.StepRequests) < 0:
		return nil, fmt.Errorf("admission: ceiling rule: plan %+v", plan)
	}
	in, ceiling := rel.Shape.InputBoundTokens, rel.Shape.CeilingTokens
	p.InputTokens, p.OutputTokens = in, ceiling
	c, err := checkPrice(cfg.Target.Name, p)
	if err != nil {
		return nil, fmt.Errorf("admission: ceiling rule: price: %w", err)
	}
	p.OutputTokens = ReleaseProbeTokens(cfg).Output
	cProbe, _ := checkPrice(cfg.Target.Name, p)
	routeCap, err := decimal(rel.Caps.RouteUSD)
	if err != nil {
		return nil, fmt.Errorf("admission: ceiling rule: release.caps.route_usd: %w", err)
	}

	v := &Verdict{Route: cfg.Target.Name, InputTokens: in, CeilingTokens: ceiling, PerRequestUSD: decimalString(c),
		ProbeUSD: decimalString(cProbe), Plan: plan, CellUSD: "0", EditionUSD: "0"}
	cell := plan.WarmupRequests
	for k, rate := range steps {
		cell += plan.StepRequests[k]
		r := rat(rate)
		cellUSD := mul(c, ratInt(cell))
		editionUSD := mul(c, ratInt(plan.Cells*cell))
		editionUSD.Add(editionUSD, mul(cProbe, ratInt(plan.ProbeRequests)))
		s := Step{RPS: rate, Admitted: true}
		for i := range cfg.Target.Limits {
			l := &cfg.Target.Limits[i]
			d, half := limitDemand(l, r, cell, plan.DayCells, in, ceiling, c, cfg.Client.HTTPTimeoutS, cellUSD, editionUSD)
			dm := demand(l.Dimension, i, rat(l.Value), d, half)
			if l.Marking == config.MarkingConfidential {
				dm.Value, dm.Fraction = 0, 0
				dm.Confidential, dm.Source, dm.SHA256 = true, l.Source, l.SHA256
			}
			s.Demands = append(s.Demands, dm)
		}
		for i, u := range cfg.Target.UnpublishedLimits {
			d := demand(UnpublishedPrefix+u.Dimension, i, rat(config.PinnedReleaseUnpublishedCapRPS), r, false)
			d.Unpublished = true
			s.Demands = append(s.Demands, d)
		}
		s.Demands = append(s.Demands, demand(RouteCapDimension, -1, routeCap, editionUSD, false))
		for i := range s.Demands {
			if d := &s.Demands[i]; !d.Pass && s.Binding == nil {
				s.Admitted, s.Binding = false, d
			}
		}
		if s.Admitted {
			v.TopRPS, v.CellUSD, v.EditionUSD = rate, decimalString(cellUSD), decimalString(editionUSD)
		}
		v.Steps = append(v.Steps, s)
	}
	return v, nil
}

// CheckPublished reports every way v, a ceiling-rule verdict as
// published, disagrees with cfg's recorded limits. Each demand against
// target.limits, binding limits included, names the limit's dimension
// and its test; one against a confidential limit carries the limit's
// source and hash, no value and no fraction, and a pass that its demand
// against the value cfg records gives; one against a public limit
// carries the value cfg records.
func CheckPublished(cfg *config.Config, v *Verdict) error {
	var problems []string
	check := func(k int, d *Demand) {
		if d.Unpublished || d.Limit == -1 && d.Dimension == RouteCapDimension {
			return
		}
		fail := func(format string, a ...any) {
			problems = append(problems, fmt.Sprintf("step %d, %s: ", k+1, d.Dimension)+fmt.Sprintf(format, a...))
		}
		if d.Limit < 0 || d.Limit >= len(cfg.Target.Limits) {
			fail("limit %d is not in target.limits", d.Limit)
			return
		}
		l := &cfg.Target.Limits[d.Limit]
		cover := l.Dimension == config.LimitSpendMonth || l.Dimension == config.LimitReloadThreshold
		test := "half"
		if cover {
			test = "cover"
		}
		switch confidential := l.Marking == config.MarkingConfidential; {
		case d.Dimension != l.Dimension || d.Half == cover:
			fail("target.limits[%d] is %s with a %s test", d.Limit, l.Dimension, test)
		case d.Confidential != confidential:
			fail("confidential %t, target.limits[%d] is marked %s", d.Confidential, d.Limit, l.Marking)
		case confidential && (d.Value != 0 || d.Fraction != 0):
			fail("a confidential limit published with its value or fraction")
		case confidential && (d.Source != l.Source || d.SHA256 != l.SHA256):
			fail("source %q and sha256 %s, target.limits[%d] has %q and %s", d.Source, d.SHA256, d.Limit, l.Source, l.SHA256)
		case !confidential && (d.Value != l.Value || d.Source != "" || d.SHA256 != ""):
			fail("value %g with source %q, target.limits[%d] records %g and is public", d.Value, d.Source, d.Limit, l.Value)
		case confidential:
			bound := rat(l.Value)
			if d.Half {
				bound.Quo(bound, big.NewRat(2, 1))
			}
			if pass := rat(d.Demand).Cmp(bound) <= 0; pass != d.Pass {
				fail("pass %t against the recorded value, published %t", pass, d.Pass)
			}
		}
	}
	for k := range v.Steps {
		s := &v.Steps[k]
		for i := range s.Demands {
			check(k, &s.Demands[i])
		}
		if s.Binding != nil {
			check(k, s.Binding)
		}
	}
	if len(problems) > 0 {
		return errors.New("admission: published verdict: " + strings.Join(problems, "; "))
	}
	return nil
}

// limitDemand is a step's nominal demand against l at rate r, with cell
// requests in one cell up to the step, and whether it is a half test.
func limitDemand(l *config.Limit, r *big.Rat, cell, dayCells, in, ceiling int, c *big.Rat, hardTimeoutS int, cellUSD, editionUSD *big.Rat) (*big.Rat, bool) {
	perMinute := mul(r, ratInt(60))
	day := ratInt(dayCells * cell)
	switch l.Dimension {
	case config.LimitRPM:
		return perMinute, true
	case config.LimitRPS:
		return r, true
	case config.LimitTPM:
		return mul(perMinute, tokens(l, in, ceiling)), true
	case config.LimitInputTPM:
		return mul(perMinute, ratInt(in)), true
	case config.LimitOutputTPM:
		return mul(perMinute, ratInt(ceiling)), true
	case config.LimitConcurrency:
		return mul(r, ratInt(hardTimeoutS)), true
	case config.LimitRPD:
		return day, true
	case config.LimitTPD:
		return mul(day, tokens(l, in, ceiling)), true
	case config.LimitSpendWindow:
		return mul(mul(r, rat(l.WindowS)), c), true
	case config.LimitSpendMonth:
		return editionUSD, false
	case config.LimitReloadThreshold:
		return cellUSD, false
	}
	panic("admission: unknown limit dimension " + l.Dimension)
}

// tokens is one request's count under l's counting.
func tokens(l *config.Limit, in, ceiling int) *big.Rat {
	switch l.Counting {
	case config.CountingInput:
		return ratInt(in)
	case config.CountingOutputBurndown:
		return new(big.Rat).Add(ratInt(in), mul(ratInt(ceiling), rat(l.OutputBurndown)))
	}
	return ratInt(in + ceiling)
}

func demand(dim string, index int, value, d *big.Rat, half bool) Demand {
	bound := new(big.Rat).Set(value)
	if half {
		bound.Quo(bound, big.NewRat(2, 1))
	}
	fv, _ := value.Float64()
	fd, _ := d.Float64()
	ff, _ := new(big.Rat).Quo(d, value).Float64()
	return Demand{Dimension: dim, Limit: index, Value: fv, Demand: fd, Fraction: ff, Half: half, Pass: d.Cmp(bound) <= 0}
}

func rat(f float64) *big.Rat { return new(big.Rat).SetFloat64(f) }

func ratInt(n int) *big.Rat { return new(big.Rat).SetInt64(int64(n)) }

func mul(a, b *big.Rat) *big.Rat { return new(big.Rat).Mul(a, b) }
