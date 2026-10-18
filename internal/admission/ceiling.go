package admission

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"

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

// Demand is a step's nominal demand against one limit. A half test admits
// a demand of at most half the limit; a cover test, of at most the limit.
type Demand struct {
	Dimension string `json:"dimension"`
	// Limit indexes target.limits; -1 is release.caps.route_usd.
	Limit    int     `json:"limit"`
	Value    float64 `json:"value"`
	Demand   float64 `json:"demand"`
	Fraction float64 `json:"fraction"`
	Half     bool    `json:"half"`
	Pass     bool    `json:"pass"`
}

// Step is one ladder step's verdict. An excluded step names its binding
// limit: its first failed test in target.limits order, then the route
// cap.
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

// CeilingRule applies SPEC-HOSTED HA3 point 6 to a release route at its
// configured ceiling. A request is taken at the shape's input bound and
// ceiling and priced at p; a probe request at the highest pinned
// ceiling. Rates, tokens and concurrency test at half of each limit,
// counted under the limit's counting; a daily limit takes the day's
// cells up to the step, a spend window the step's rate over the window.
// The monthly spend cap and the route's ledger cap must cover the
// edition up to the step plus the probe requests, and the auto-reload
// threshold the costliest cell. A step is admitted when it passes every
// test; every demand rises with the step, so the admitted steps are the
// ladder up to the top step. cfg must validate.
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
	p.OutputTokens = slices.Max(config.PinnedReleaseCeilings)
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
			s.Demands = append(s.Demands, demand(l.Dimension, i, rat(l.Value), d, half))
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
