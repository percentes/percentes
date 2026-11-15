package admission

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// The edition a release route's caps must cover (SPEC-HOSTED §4 grid,
// HA3 point 9 and HA4): twelve cells and at most two re-runs, and the
// probe allowance in requests.
const (
	ReleaseCells          = 12
	ReleaseReruns         = 2
	ReleaseProbeAllowance = 100
)

// ReleaseStartTolerance is how long before or after its scheduled start
// a cell attempt may start (SPEC-HOSTED §4). A daily limit takes an
// attempt over its interval, from the tolerance before its scheduled
// start to the tolerance and one cell after it.
const ReleaseStartTolerance = 15 * time.Minute

// windowSlackS is added to an evaluation window's length in the rate at
// a limit (SPEC-HOSTED HA4).
var windowSlackS = big.NewRat(1, 10)

// Plan is the traffic the ceiling rule tests a route with: one cell's
// warm-up requests and measured requests per step, the cells in the
// edition with re-runs and the probe requests. Attempts are the route's
// cell attempts in the frozen schedule and ChecksUTC the send times of
// the ceiling-check requests on the route's account; a daily limit takes
// both over each reset day. With exact counts, a cell's counts are the
// largest of any cell's.
type Plan struct {
	WarmupRequests int         `json:"warmup_requests"`
	StepRequests   []int       `json:"step_requests"`
	Cells          int         `json:"cells"`
	ProbeRequests  int         `json:"probe_requests"`
	Attempts       []Attempt   `json:"attempts,omitempty"`
	ChecksUTC      []time.Time `json:"checks_utc,omitempty"`

	// DayCells stays until every caller reads Attempts.
	DayCells int `json:"-"`
}

// Attempt is one scheduled cell attempt: its scheduled start, and
// whether it is a cell's re-run date.
type Attempt struct {
	Start time.Time `json:"start"`
	Rerun bool      `json:"rerun,omitempty"`
}

// ExpectedPlan is the plan at the count rule's counts for the ladder at
// bindingRPS (config.Ladder.StepRates): floor(r·T) requests in a segment
// of T seconds at rate r, the warm-up at the first step's rate. It holds
// no attempts and no checks. cfg must have passed Validate under the
// release profile.
func ExpectedPlan(cfg *config.Config, bindingRPS float64) Plan {
	l := cfg.Release.Ladder
	rates := l.StepRates(bindingRPS)
	p := Plan{
		WarmupRequests: segmentRequests(l.WarmupS, rates[0]),
		Cells:          ReleaseCells + ReleaseReruns,
		ProbeRequests:  ReleaseProbeAllowance,
	}
	for _, r := range rates {
		p.StepRequests = append(p.StepRequests, segmentRequests(l.StepS, r))
	}
	return p
}

// segmentRequests is the count rule's requests in a segment, as the
// deterministic arrival process counts them.
func segmentRequests(seconds, rps float64) int {
	return int(math.Floor(float64(int64(seconds*1e9)) / 1e9 * rps))
}

// RouteCapDimension names the configured release.caps.route_usd in a
// Demand or a Rate.
const RouteCapDimension = "route_cap_usd"

// UnpublishedPrefix opens the Dimension of a Demand or a Rate against a
// target.unpublished_limits entry, before the entry's dimension.
const UnpublishedPrefix = "unpublished:"

// Rate is the rate one limit, cap or ruling admits a route at under the
// binding-rate rule (SPEC-HOSTED HA4).
type Rate struct {
	Dimension string `json:"dimension"`
	// Limit indexes target.limits, or target.unpublished_limits where
	// Unpublished is set; -1 is release.caps.route_usd.
	Limit       int  `json:"limit"`
	Unpublished bool `json:"unpublished,omitempty"`
	// WindowS is a rate, token or spend-window limit's evaluation window,
	// and Allowance the requests the limit admits in it, its share of the
	// limit at one request's nominal demand rounded down.
	WindowS   float64 `json:"window_s,omitempty"`
	Allowance int64   `json:"allowance,omitempty"`
	// ResetDay is a daily limit's reset day that sets its rate, the date
	// in the limit's reset zone, and Attempts the attempts it reserves.
	ResetDay string  `json:"reset_day,omitempty"`
	Attempts int     `json:"attempts,omitempty"`
	RPS      float64 `json:"rps"`
}

// Solution is the binding-rate rule applied to a route: the rate at each
// public limit in each evaluation window it records, and at each other
// limit, ruling and the route cap, in target.limits order, then
// target.unpublished_limits order, then the route cap; the binding
// limit, the first of the least of them; and the binding rate, its rate
// rounded down to 0.001 requests per second. A confidential limit sets no
// rate.
type Solution struct {
	Rates      []Rate  `json:"rates"`
	Binding    Rate    `json:"binding"`
	BindingRPS float64 `json:"binding_rps"`
}

// Demand is a step's nominal demand against one limit and its test. A
// window test, against a rate, token or spend-window limit in one
// evaluation window, admits a step rate at most the rate at the limit in
// that window; a cover test admits a demand of at most the limit.
type Demand struct {
	Dimension string `json:"dimension"`
	// Limit indexes target.limits, or target.unpublished_limits where
	// Unpublished is set; -1 is release.caps.route_usd.
	Limit       int  `json:"limit"`
	Unpublished bool `json:"unpublished,omitempty"`
	// Confidential is set against a limit marked
	// config.MarkingConfidential: Value, Fraction, Allowance and RateRPS
	// are absent, and Source and HMACSHA256 are the limit's source and
	// the HMAC-SHA256 of its retained snapshot under the release key.
	Confidential bool    `json:"confidential,omitempty"`
	Source       string  `json:"source,omitempty"`
	HMACSHA256   string  `json:"hmac_sha256,omitempty"`
	Value        float64 `json:"value,omitempty"`
	// WindowS is a window test's evaluation window, Allowance the
	// requests the limit admits in it and RateRPS the rate at the limit,
	// Allowance over the window plus 0.1 s, and plus the hard timeout for a
	// limit that counts output at generation; all absent for a cover test.
	WindowS   float64 `json:"window_s,omitempty"`
	Allowance int64   `json:"allowance,omitempty"`
	RateRPS   float64 `json:"rate_rps,omitempty"`
	// Demand is in the limit's unit over the window its value is stated
	// over; for a daily limit, on the reset day ResetDay with the most,
	// which reserves Attempts attempts.
	Demand   float64 `json:"demand"`
	Fraction float64 `json:"fraction,omitempty"`
	ResetDay string  `json:"reset_day,omitempty"`
	Attempts int     `json:"attempts,omitempty"`
	Pass     bool    `json:"pass"`

	// Half stays until every caller reads the window tests.
	Half bool `json:"-"`
}

// Step is one ladder step's verdict. A step that fails a test names its
// binding limit: its first failed test in target.limits order, then
// target.unpublished_limits order, then the route cap.
type Step struct {
	RPS      float64  `json:"rps"`
	Admitted bool     `json:"admitted"`
	Binding  *Demand  `json:"binding,omitempty"`
	Demands  []Demand `json:"demands"`
}

// Verdict is the ceiling rule applied to one route at its ceiling: the
// step rates at BindingRPS tested against every limit, and the rule's
// own Solution under the configuration. CellUSD and EditionUSD are the
// worst cases of the costliest cell and of the edition at the top
// admitted step.
type Verdict struct {
	Route         string   `json:"route"`
	InputTokens   int      `json:"input_tokens"`
	CeilingTokens int      `json:"ceiling_tokens"`
	PerRequestUSD string   `json:"per_request_usd"`
	BindingRPS    float64  `json:"binding_rps"`
	Solution      Solution `json:"solution"`
	TopRPS        float64  `json:"top_rps"`
	CellUSD       string   `json:"cell_usd"`
	EditionUSD    string   `json:"edition_usd"`
	Plan          Plan     `json:"plan"`
	Steps         []Step   `json:"steps"`
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

// ReleaseTokens are the token bounds of a release route's cell and probe
// requests: the shape's input bound and the ceiling the route's check
// chose.
func ReleaseTokens(cfg *config.Config) Tokens {
	return Tokens{Input: cfg.Release.Shape.InputBoundTokens, Output: cfg.Release.Shape.CeilingTokens}
}

// ReleaseProbeTokens are the token bounds a release route's ceiling check
// is admitted at: the shape's input bound and the highest ceiling the
// shape permits the route (config.Shape.Ceilings).
func ReleaseProbeTokens(cfg *config.Config) Tokens {
	return Tokens{Input: cfg.Release.Shape.InputBoundTokens, Output: slices.Max(cfg.Release.Shape.Ceilings())}
}

// Covers reports whether the ledger prices cfg's route's requests at
// ReleaseTokens at least as high as v's PerRequestUSD.
func (s *State) Covers(cfg *config.Config, v *Verdict) error {
	tok := ReleaseTokens(cfg)
	want, err := decimal(v.PerRequestUSD)
	if err != nil {
		return fmt.Errorf("admission: ceiling rule price: %w", err)
	}
	have := s.price(cfg.Target.Name, tok)
	if have == nil {
		return fmt.Errorf("admission: no price recorded for %s at %d input and %d output tokens", cfg.Target.Name, tok.Input, tok.Output)
	}
	if have.Cmp(want) < 0 {
		return fmt.Errorf("admission: the ledger admits %s at US$%s a request at %d input and %d output tokens, under the ceiling rule's US$%s",
			cfg.Target.Name, decimalString(have), tok.Input, tok.Output, v.PerRequestUSD)
	}
	return nil
}

// rule is a release route's inputs to the binding-rate rule: a cell or
// probe request at the shape's input bound and the configured ceiling,
// priced at c; a ceiling-check request at the highest ceiling the shape
// permits; nominal, a cell's requests per unit rate. Under noDaily the
// daily limits set no rate.
type rule struct {
	cfg              *config.Config
	plan             Plan
	in, ceiling      int
	checkCeiling     int
	c                *big.Rat
	routeCap         *big.Rat
	nominal          *big.Rat
	cellLength       time.Duration
	hardTimeoutS     int
	unpublishedLimit *big.Rat
	noDaily          bool
}

func newRule(cfg *config.Config, p Price, plan Plan) (*rule, error) {
	if cfg.Profile != config.ProfileRelease || cfg.Release == nil {
		return nil, errors.New("admission: ceiling rule: not a release configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("admission: ceiling rule: %w", err)
	}
	p.InputTokens, p.OutputTokens = cfg.Release.Shape.InputBoundTokens, cfg.Release.Shape.CeilingTokens
	c, err := checkPrice(cfg.Target.Name, p)
	if err != nil {
		return nil, fmt.Errorf("admission: ceiling rule: price: %w", err)
	}
	return ruleAt(cfg, c, plan)
}

// ruleAt is newRule, without validating cfg, with a request priced at c.
func ruleAt(cfg *config.Config, c *big.Rat, plan Plan) (*rule, error) {
	if cfg.Profile != config.ProfileRelease || cfg.Release == nil {
		return nil, errors.New("admission: ceiling rule: not a release configuration")
	}
	rel := cfg.Release
	l := rel.Ladder
	switch {
	case len(plan.StepRequests) != len(l.StepFractions):
		return nil, fmt.Errorf("admission: ceiling rule: plan has %d steps, the ladder %d", len(plan.StepRequests), len(l.StepFractions))
	case plan.WarmupRequests < 0 || plan.Cells < 1 || plan.ProbeRequests < 0 || slices.Min(plan.StepRequests) < 0:
		return nil, fmt.Errorf("admission: ceiling rule: plan of %d warm-up, %v step, %d cells and %d probe requests", plan.WarmupRequests, plan.StepRequests, plan.Cells, plan.ProbeRequests)
	}
	r := &rule{cfg: cfg, plan: plan, in: rel.Shape.InputBoundTokens, ceiling: rel.Shape.CeilingTokens,
		checkCeiling: slices.Max(rel.Shape.Ceilings()), hardTimeoutS: cfg.Client.HTTPTimeoutS,
		unpublishedLimit: dec(config.PinnedReleaseUnpublishedCapRPS), c: c}
	var err error
	if r.routeCap, err = decimal(rel.Caps.RouteUSD); err != nil {
		return nil, fmt.Errorf("admission: ceiling rule: release.caps.route_usd: %w", err)
	}
	sum := new(big.Rat)
	for _, f := range l.StepFractions {
		sum.Add(sum, dec(f))
	}
	r.nominal = new(big.Rat).Add(mul(dec(l.WarmupS), dec(l.StepFractions[0])), mul(dec(l.StepS), sum))
	r.cellLength = cellLength(l)
	return r, nil
}

// cellLength is one cell of ladder l: its warm-up, and every step with
// its drain.
func cellLength(l config.Ladder) time.Duration {
	return time.Duration((l.WarmupS + float64(len(l.StepFractions))*(l.StepS+l.DrainS)) * float64(time.Second))
}

// Reservation is one reset day of a daily limit as the ceiling rule
// reserves it: the date in the limit's reset zone, the attempts it
// reserves, its ceiling-check requests, and the reservation in the
// limit's unit, each attempt a cell at the plan's counts with one probe
// request and each check at the highest permitted ceiling.
type Reservation struct {
	Date     string  `json:"date"`
	Attempts int     `json:"attempts"`
	Checks   int     `json:"checks"`
	Reserved float64 `json:"reserved"`
}

// Reservations are the reset days of target.limits[i], a daily limit of
// cfg, under plan, in date order (resetDays). cfg must validate.
func Reservations(cfg *config.Config, plan Plan, i int) ([]Reservation, error) {
	if cfg.Release == nil || i < 0 || i >= len(cfg.Target.Limits) {
		return nil, fmt.Errorf("admission: reservations: no release limit %d", i)
	}
	l := &cfg.Target.Limits[i]
	if l.Dimension != config.LimitRPD && l.Dimension != config.LimitTPD {
		return nil, fmt.Errorf("admission: reservations: target.limits[%d] is %s, not a daily limit", i, l.Dimension)
	}
	rel := cfg.Release
	r := &rule{cfg: cfg, plan: plan, in: rel.Shape.InputBoundTokens, ceiling: rel.Shape.CeilingTokens,
		checkCeiling: slices.Max(rel.Shape.Ceilings()), cellLength: cellLength(rel.Ladder)}
	cell := plan.WarmupRequests
	for _, n := range plan.StepRequests {
		cell += n
	}
	d, dCheck := r.perRequest(l, r.ceiling), r.perRequest(l, r.checkCeiling)
	var out []Reservation
	for _, day := range r.resetDays(l) {
		n := mul(ratInt(day.attempts()*(cell+1)), d)
		n.Add(n, mul(ratInt(day.checks), dCheck))
		f, _ := n.Float64()
		out = append(out, Reservation{Date: day.date, Attempts: day.attempts(), Checks: day.checks, Reserved: f})
	}
	return out, nil
}

// needAttempts refuses a plan with no attempts where a daily limit takes
// the rule.
func (r *rule) needAttempts() error {
	if r.noDaily || len(r.plan.Attempts) > 0 {
		return nil
	}
	for _, lim := range r.cfg.Target.Limits {
		if lim.Dimension == config.LimitRPD || lim.Dimension == config.LimitTPD {
			return fmt.Errorf("admission: ceiling rule: %s is tested over the frozen schedule's cell attempts, and the plan holds none", lim.Dimension)
		}
	}
	return nil
}

// CheckRPS is the rate a release route's ceiling check is paced from,
// before the schedule freezes the route's binding rate: the binding rate
// of Solve at the configured ceiling over every limit but the daily ones,
// whose reset-day test counts the check's requests. cfg must validate.
func CheckRPS(cfg *config.Config, p Price) (float64, error) {
	r, err := newRule(cfg, p, ExpectedPlan(cfg, 1))
	if err != nil {
		return 0, err
	}
	r.noDaily = true
	return r.solve().BindingRPS, nil
}

// Solve applies the binding-rate rule of SPEC-HOSTED HA4 to a release
// route at its configured ceiling, the ceiling its check chose. The rate
// at a rate, token or spend-window limit in each evaluation window W is
// the requests the limit admits in W, its share of the limit at one
// request's nominal demand rounded down, over W plus 0.1 s, and plus the
// hard timeout for a limit that counts output at generation. A request is
// taken at the shape's input bound and ceiling, under each token limit's
// counting, and priced at p. A concurrency limit admits its value over
// the hard timeout, and each unpublished limit
// config.PinnedReleaseUnpublishedCapRPS. The monthly spend cap and the
// route cap admit the rate whose edition, plan.Cells cells of a cell's
// nominal requests and the probe requests, they cover; the auto-reload
// threshold the rate whose cell it covers. A daily limit admits, on each
// reset day in its zone that holds k attempts, the rate at which k
// cells, each with one probe request, and the day's ceiling-check
// requests fit within it (resetDays). It reads the plan's cells, probe
// requests, attempts and checks. cfg must validate.
func Solve(cfg *config.Config, p Price, plan Plan) (*Solution, error) {
	r, err := newRule(cfg, p, plan)
	if err != nil {
		return nil, err
	}
	if err := r.needAttempts(); err != nil {
		return nil, err
	}
	return r.solve(), nil
}

type exactRate struct {
	Rate
	rps *big.Rat
}

func (r *rule) solve() *Solution {
	var rates []exactRate
	add := func(x Rate, rps *big.Rat) {
		if rps.Sign() < 0 {
			rps = new(big.Rat)
		}
		x.RPS, _ = rps.Float64()
		rates = append(rates, exactRate{x, rps})
	}
	for i := range r.cfg.Target.Limits {
		l := &r.cfg.Target.Limits[i]
		if l.Marking == config.MarkingConfidential {
			continue
		}
		value := dec(l.Value)
		x := Rate{Dimension: l.Dimension, Limit: i}
		switch l.Dimension {
		case config.LimitRPM, config.LimitRPS, config.LimitTPM, config.LimitInputTPM, config.LimitOutputTPM, config.LimitSpendWindow:
			for _, w := range l.WindowsS() {
				a := r.allowance(l, value, w)
				x.WindowS, x.Allowance = w, a
				add(x, new(big.Rat).Quo(ratInt(int(a)), r.span(l, w)))
			}
		case config.LimitConcurrency:
			add(x, new(big.Rat).Quo(value, ratInt(r.hardTimeoutS)))
		case config.LimitRPD, config.LimitTPD:
			if r.noDaily {
				continue
			}
			rps, day := r.dailyRate(l, value)
			x.ResetDay, x.Attempts = day.date, day.attempts()
			add(x, rps)
		case config.LimitSpendMonth:
			add(x, r.capRate(value))
		case config.LimitReloadThreshold:
			add(x, new(big.Rat).Quo(value, mul(r.nominal, r.c)))
		}
	}
	for i, u := range r.cfg.Target.UnpublishedLimits {
		add(Rate{Dimension: UnpublishedPrefix + u.Dimension, Limit: i, Unpublished: true}, r.unpublishedLimit)
	}
	add(Rate{Dimension: RouteCapDimension, Limit: -1}, r.capRate(r.routeCap))

	s := &Solution{}
	least := 0
	for i, x := range rates {
		s.Rates = append(s.Rates, x.Rate)
		if x.rps.Cmp(rates[least].rps) < 0 {
			least = i
		}
	}
	s.Binding = rates[least].Rate
	s.BindingRPS = float64(floorInt(mul(rates[least].rps, ratInt(1000)))) / 1000
	return s
}

// perRequest is one cell request's nominal demand under l's counting, at
// the given ceiling: requests, tokens or US dollars.
func (r *rule) perRequest(l *config.Limit, ceiling int) *big.Rat {
	switch l.Dimension {
	case config.LimitTPM, config.LimitTPD:
		return tokens(l, r.in, ceiling)
	case config.LimitInputTPM:
		return ratInt(r.in)
	case config.LimitOutputTPM:
		return ratInt(ceiling)
	case config.LimitSpendWindow:
		return r.c
	}
	return ratInt(1)
}

// span is the interval whose writes can count in an evaluation window of
// w seconds of l: w plus 0.1 s, and the hard timeout as well where l
// counts output at generation.
func (r *rule) span(l *config.Limit, w float64) *big.Rat {
	s := new(big.Rat).Add(dec(w), windowSlackS)
	if l.OutputAtGeneration {
		s.Add(s, ratInt(r.hardTimeoutS))
	}
	return s
}

// allowance is the requests l, of the given value, admits in a window
// of w seconds: its share of the value stated over l's window, at one
// request's nominal demand, rounded down.
func (r *rule) allowance(l *config.Limit, value *big.Rat, w float64) int64 {
	share := new(big.Rat).Quo(mul(value, dec(w)), dec(l.StatedWindowS()))
	return floorInt(new(big.Rat).Quo(share, r.perRequest(l, r.ceiling)))
}

// capRate is the rate whose edition at nominal counts cap covers.
func (r *rule) capRate(cap *big.Rat) *big.Rat {
	requests := new(big.Rat).Sub(new(big.Rat).Quo(cap, r.c), ratInt(r.plan.ProbeRequests))
	return requests.Quo(requests, mul(ratInt(r.plan.Cells), r.nominal))
}

// resetDay is one reset day of a daily limit: its first attempts, its
// re-run dates and its ceiling-check requests.
type resetDay struct {
	date                  string
	first, reruns, checks int
}

// attempts is the attempts the day reserves: every first attempt and at
// most ReleaseReruns re-runs.
func (d resetDay) attempts() int { return d.first + min(d.reruns, ReleaseReruns) }

// resetDays are l's reset days, the calendar dates of its reset zone, in
// date order, that the plan's attempt intervals touch or a ceiling-check
// request falls on.
func (r *rule) resetDays(l *config.Limit) []resetDay {
	loc, err := time.LoadLocation(l.ResetZone)
	if err != nil {
		panic("admission: reset zone of a validated limit: " + err.Error())
	}
	days := map[string]*resetDay{}
	day := func(t time.Time) *resetDay {
		key := t.In(loc).Format(time.DateOnly)
		if days[key] == nil {
			days[key] = &resetDay{date: key}
		}
		return days[key]
	}
	for _, a := range r.plan.Attempts {
		from, to := a.Start.Add(-ReleaseStartTolerance), a.Start.Add(ReleaseStartTolerance+r.cellLength)
		for t := from; ; {
			if a.Rerun {
				day(t).reruns++
			} else {
				day(t).first++
			}
			y, m, d := t.In(loc).Date()
			if t = time.Date(y, m, d+1, 0, 0, 0, 0, loc); t.After(to) {
				break
			}
		}
	}
	for _, t := range r.plan.ChecksUTC {
		day(t).checks++
	}
	out := make([]resetDay, 0, len(days))
	for _, d := range days {
		out = append(out, *d)
	}
	slices.SortFunc(out, func(a, b resetDay) int { return strings.Compare(a.date, b.date) })
	return out
}

// dailyRate is the least rate over l's reset days holding an attempt at
// which the day's attempts, each a cell at nominal counts and one probe
// request, and its ceiling-check requests at the highest permitted
// ceiling fit within value, with the day that sets it.
func (r *rule) dailyRate(l *config.Limit, value *big.Rat) (*big.Rat, resetDay) {
	d, dCheck := r.perRequest(l, r.ceiling), r.perRequest(l, r.checkCeiling)
	var least *big.Rat
	var at resetDay
	for _, day := range r.resetDays(l) {
		k := day.attempts()
		if k == 0 {
			continue
		}
		room := new(big.Rat).Sub(value, mul(ratInt(day.checks), dCheck))
		room.Quo(room, d).Sub(room, ratInt(k))
		rps := room.Quo(room, mul(ratInt(k), r.nominal))
		if least == nil || rps.Cmp(least) < 0 {
			least, at = rps, day
		}
	}
	return least, at
}

// CeilingRule tests a release route's ladder at bindingRPS, the frozen
// binding rate, against every limit (SPEC-HOSTED HA4): each step's rate
// is its fraction of bindingRPS rounded down to 0.001 requests per
// second, and its cell holds the plan's warm-up and step requests up to
// the step. A rate, token or spend-window limit takes a window test in
// each of its evaluation windows; a concurrency limit, each unpublished
// limit, the caps, the auto-reload threshold and a daily limit, a cover
// test. The monthly spend cap and the route cap cover the edition up to
// the step, plan.Cells cells and the probe requests at the configured
// ceiling; the auto-reload threshold the cell; a daily limit, on each
// reset day, its attempts, each the cell and one probe request, and the
// day's ceiling-check requests at the highest permitted ceiling. A step
// is admitted when it passes every test. A demand against a confidential
// limit carries the test's result without the value, the fraction, the
// allowance or the rate. The verdict carries the rule's own Solution
// under cfg. cfg must validate.
func CeilingRule(cfg *config.Config, p Price, plan Plan, bindingRPS float64) (*Verdict, error) {
	r, err := newRule(cfg, p, plan)
	if err != nil {
		return nil, err
	}
	if err := r.needAttempts(); err != nil {
		return nil, err
	}
	if !(bindingRPS > 0) || config.RoundDownRPS(bindingRPS) != bindingRPS {
		return nil, fmt.Errorf("admission: ceiling rule: binding rate %g: want a positive rate rounded down to 0.001 requests per second", bindingRPS)
	}
	return r.verdict(bindingRPS), nil
}

// verdict is CeilingRule's verdict under r at bindingRPS.
func (r *rule) verdict(bindingRPS float64) *Verdict {
	cfg, plan := r.cfg, r.plan
	v := &Verdict{Route: cfg.Target.Name, InputTokens: r.in, CeilingTokens: r.ceiling, PerRequestUSD: decimalString(r.c),
		BindingRPS: bindingRPS, Solution: *r.solve(), Plan: plan, CellUSD: "0", EditionUSD: "0"}
	cell := plan.WarmupRequests
	for k, rate := range cfg.Release.Ladder.StepRates(bindingRPS) {
		cell += plan.StepRequests[k]
		cellUSD := mul(r.c, ratInt(cell))
		editionUSD := mul(r.c, ratInt(plan.Cells*cell+plan.ProbeRequests))
		s := Step{RPS: rate, Admitted: true}
		for i := range cfg.Target.Limits {
			for _, d := range r.demands(i, dec(rate), cell, cellUSD, editionUSD) {
				if l := &cfg.Target.Limits[i]; l.Marking == config.MarkingConfidential {
					d.Value, d.Fraction, d.Allowance, d.RateRPS = 0, 0, 0, 0
					d.Confidential, d.Source, d.HMACSHA256 = true, l.Source, l.HMACSHA256
				}
				s.Demands = append(s.Demands, d)
			}
		}
		for i, u := range cfg.Target.UnpublishedLimits {
			d := cover(UnpublishedPrefix+u.Dimension, i, r.unpublishedLimit, dec(rate))
			d.Unpublished = true
			s.Demands = append(s.Demands, d)
		}
		s.Demands = append(s.Demands, cover(RouteCapDimension, -1, r.routeCap, editionUSD))
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
	return v
}

// demands are the step at rate s's demands against target.limits[i],
// with cell requests in one cell up to the step: one per evaluation
// window of a window test, else one.
func (r *rule) demands(i int, s *big.Rat, cell int, cellUSD, editionUSD *big.Rat) []Demand {
	l := &r.cfg.Target.Limits[i]
	value := dec(l.Value)
	switch l.Dimension {
	case config.LimitRPM, config.LimitRPS, config.LimitTPM, config.LimitInputTPM, config.LimitOutputTPM, config.LimitSpendWindow:
		demand := mul(mul(s, dec(l.StatedWindowS())), r.perRequest(l, r.ceiling))
		var out []Demand
		for _, w := range l.WindowsS() {
			d := cover(l.Dimension, i, value, demand)
			d.WindowS, d.Allowance = w, r.allowance(l, value, w)
			span := r.span(l, w)
			d.RateRPS, _ = new(big.Rat).Quo(ratInt(int(d.Allowance)), span).Float64()
			d.Pass = mul(s, span).Cmp(ratInt(int(d.Allowance))) <= 0
			out = append(out, d)
		}
		return out
	case config.LimitConcurrency:
		return []Demand{cover(l.Dimension, i, value, mul(s, ratInt(r.hardTimeoutS)))}
	case config.LimitRPD, config.LimitTPD:
		d, dCheck := r.perRequest(l, r.ceiling), r.perRequest(l, r.checkCeiling)
		var most *big.Rat
		var at resetDay
		for _, day := range r.resetDays(l) {
			n := mul(ratInt(day.attempts()*(cell+1)), d)
			n.Add(n, mul(ratInt(day.checks), dCheck))
			if most == nil || n.Cmp(most) > 0 {
				most, at = n, day
			}
		}
		dm := cover(l.Dimension, i, value, most)
		dm.ResetDay, dm.Attempts = at.date, at.attempts()
		return []Demand{dm}
	case config.LimitSpendMonth:
		return []Demand{cover(l.Dimension, i, value, editionUSD)}
	case config.LimitReloadThreshold:
		return []Demand{cover(l.Dimension, i, value, cellUSD)}
	}
	panic("admission: unknown limit dimension " + l.Dimension)
}

// CheckPublished reports every way v, a ceiling-rule verdict as
// published, disagrees with cfg's recorded limits. Each demand against
// target.limits, binding limits included, names the limit's dimension
// and takes its test: a window test in one of the limit's evaluation
// windows, at the allowance its recorded value gives, or a cover test.
// One against a confidential limit carries the limit's source and keyed
// hash, no value, fraction, allowance or rate, and a pass that the value
// cfg records gives; one against a public limit carries the value cfg
// records. A window test's pass, and a public one's rate, are those the
// allowance gives over the limit's span (rule.span) at the pinned hard
// timeout. Each rate of the solution against target.limits names a
// public limit of its dimension, in one of its evaluation windows, and
// a window rate's allowance and rate are those of the limit over its
// span. The solution's binding limit is among its rates and none is
// lower; where it is a public window rate, the binding rate is that
// rate rounded down to 0.001 requests per second. The ceiling rule then
// rebuilds the verdict under cfg, at the pinned release ladder and hard
// timeout and at v's plan, per-request price and binding rate, and each
// of the token bounds, the solution, the steps, the top rate and the
// cell and edition costs must equal the rule's. Those three inputs are
// taken from v: the rule must accept the plan, and the binding rate must
// be positive and rounded down to 0.001 requests per second.
func CheckPublished(cfg *config.Config, v *Verdict) error {
	var problems []string
	price, err := decimal(v.PerRequestUSD)
	if err != nil {
		return fmt.Errorf("admission: published verdict: per_request_usd: %w", err)
	}
	// Spans use the pinned release hard timeout, whatever a cell's configured one.
	r := &rule{cfg: cfg, in: v.InputTokens, ceiling: v.CeilingTokens, c: price, hardTimeoutS: config.PinnedReleaseHardTimeoutS}
	limit := func(fail func(string, ...any), index int, dim string, windowS float64) *config.Limit {
		if index < 0 || index >= len(cfg.Target.Limits) {
			fail("limit %d is not in target.limits", index)
			return nil
		}
		l := &cfg.Target.Limits[index]
		test := "a cover test"
		if ws := l.WindowsS(); len(ws) > 0 {
			test = fmt.Sprintf("window tests over %v s", ws)
		}
		if ws := l.WindowsS(); dim != l.Dimension || (len(ws) > 0) != (windowS > 0) || windowS > 0 && !slices.Contains(ws, windowS) {
			fail("target.limits[%d] is %s with %s", index, l.Dimension, test)
			return nil
		}
		return l
	}
	check := func(k int, s *Step, d *Demand) {
		if d.Unpublished || d.Limit == -1 && d.Dimension == RouteCapDimension {
			return
		}
		fail := func(format string, a ...any) {
			problems = append(problems, fmt.Sprintf("step %d, %s: ", k+1, d.Dimension)+fmt.Sprintf(format, a...))
		}
		l := limit(fail, d.Limit, d.Dimension, d.WindowS)
		if l == nil {
			return
		}
		var allowance int64
		if d.WindowS > 0 {
			allowance = r.allowance(l, dec(l.Value), d.WindowS)
		}
		switch confidential := l.Marking == config.MarkingConfidential; {
		case d.Confidential != confidential:
			fail("confidential %t, target.limits[%d] is marked %s", d.Confidential, d.Limit, l.Marking)
		case confidential && (d.Value != 0 || d.Fraction != 0 || d.Allowance != 0 || d.RateRPS != 0):
			fail("a confidential limit published with its value or fraction")
		case confidential && (d.Source != l.Source || d.HMACSHA256 != l.HMACSHA256):
			fail("source %q and hmac_sha256 %s, target.limits[%d] has %q and %s", d.Source, d.HMACSHA256, d.Limit, l.Source, l.HMACSHA256)
		case !confidential && (d.Value != l.Value || d.Source != "" || d.HMACSHA256 != ""):
			fail("value %g with source %q, target.limits[%d] records %g and is public", d.Value, d.Source, d.Limit, l.Value)
		case !confidential && d.Allowance != allowance:
			fail("allowance %d in %g s, target.limits[%d] admits %d", d.Allowance, d.WindowS, d.Limit, allowance)
		case confidential || d.WindowS > 0:
			pass := rat(d.Demand).Cmp(rat(l.Value)) <= 0
			if d.WindowS > 0 {
				pass = mul(dec(s.RPS), r.span(l, d.WindowS)).Cmp(ratInt(int(allowance))) <= 0
			}
			if pass != d.Pass {
				fail("pass %t against the recorded value, published %t", pass, d.Pass)
			}
			if rate, _ := new(big.Rat).Quo(ratInt(int(allowance)), r.span(l, d.WindowS)).Float64(); !confidential && rate != d.RateRPS {
				fail("rate %g rps in %g s, target.limits[%d] admits %g", d.RateRPS, d.WindowS, d.Limit, rate)
			}
		}
	}
	for k := range v.Steps {
		s := &v.Steps[k]
		for i := range s.Demands {
			check(k, s, &s.Demands[i])
		}
		if s.Binding != nil {
			check(k, s, s.Binding)
		}
	}
	if v.Solution.Binding.Dimension == "" {
		problems = append(problems, "solution: no binding limit")
	}
	rates := append(slices.Clone(v.Solution.Rates), v.Solution.Binding)
	for i, x := range rates {
		if x.Dimension == "" {
			continue
		}
		if x.Unpublished || x.Limit == -1 && x.Dimension == RouteCapDimension {
			continue
		}
		fail := func(format string, a ...any) {
			problems = append(problems, fmt.Sprintf("solution, %s: ", x.Dimension)+fmt.Sprintf(format, a...))
		}
		l := limit(fail, x.Limit, x.Dimension, x.WindowS)
		switch {
		case l == nil:
		case l.Marking == config.MarkingConfidential:
			fail("target.limits[%d] is confidential and sets no rate", x.Limit)
		case x.WindowS > 0:
			allowance := r.allowance(l, dec(l.Value), x.WindowS)
			exact := new(big.Rat).Quo(ratInt(int(allowance)), r.span(l, x.WindowS))
			rate, _ := exact.Float64()
			if x.Allowance != allowance || x.RPS != rate {
				fail("allowance %d and rate %g rps in %g s, target.limits[%d] admits %d at %g", x.Allowance, x.RPS, x.WindowS, x.Limit, allowance, rate)
			}
			if i == len(rates)-1 {
				if want := float64(floorInt(mul(exact, ratInt(1000)))) / 1000; v.Solution.BindingRPS != want {
					fail("binding rate %g rps, the binding limit admits %g", v.Solution.BindingRPS, want)
				}
			}
		}
	}
	if b := v.Solution.Binding; b.Dimension != "" {
		switch {
		case !slices.Contains(v.Solution.Rates, b):
			problems = append(problems, "solution: the binding limit is not among its rates")
		case v.Solution.BindingRPS > b.RPS:
			problems = append(problems, fmt.Sprintf("solution: binding rate %g rps above the binding limit's %g", v.Solution.BindingRPS, b.RPS))
		}
		for _, x := range v.Solution.Rates {
			if x.RPS < b.RPS {
				problems = append(problems, fmt.Sprintf("solution: %s at %g rps is below the binding limit's %g", x.Dimension, x.RPS, b.RPS))
				break
			}
		}
	}
	problems = append(problems, recomputed(cfg, price, v)...)
	if len(problems) > 0 {
		return errors.New("admission: published verdict: " + strings.Join(problems, "; "))
	}
	return nil
}

// recomputed lists every part of v that differs from the ceiling rule's
// verdict under cfg at v's plan, price and binding rate.
func recomputed(cfg *config.Config, price *big.Rat, v *Verdict) []string {
	if cfg.Release == nil {
		return []string{"not a release configuration"}
	}
	// The rule reads the pinned ladder and hard timeout, whatever a cell's configured ones.
	c, rel := *cfg, *cfg.Release
	rel.Ladder = config.Ladder{StepFractions: slices.Clone(config.PinnedReleaseStepFractions),
		WarmupS: config.PinnedReleaseWarmupS, StepS: config.PinnedReleaseStepS, DrainS: config.PinnedReleaseDrainS}
	c.Release = &rel
	r, err := ruleAt(&c, price, v.Plan)
	if err == nil {
		err = r.needAttempts()
	}
	if err != nil {
		return []string{err.Error()}
	}
	r.hardTimeoutS = config.PinnedReleaseHardTimeoutS
	if !(v.BindingRPS > 0) || config.RoundDownRPS(v.BindingRPS) != v.BindingRPS {
		return []string{fmt.Sprintf("binding rate %g: want a positive rate rounded down to 0.001 requests per second", v.BindingRPS)}
	}
	w := r.verdict(v.BindingRPS)
	var out []string
	if v.InputTokens != w.InputTokens || v.CeilingTokens != w.CeilingTokens {
		out = append(out, fmt.Sprintf("tokens %d and %d, the configuration's %d and %d", v.InputTokens, v.CeilingTokens, w.InputTokens, w.CeilingTokens))
	}
	if !reflect.DeepEqual(v.Solution, w.Solution) {
		out = append(out, fmt.Sprintf("solution %+v, the rule gives %+v", v.Solution, w.Solution))
	}
	if len(v.Steps) != len(w.Steps) {
		out = append(out, fmt.Sprintf("%d steps, the ladder %d", len(v.Steps), len(w.Steps)))
	} else {
		for k := range w.Steps {
			if !reflect.DeepEqual(v.Steps[k], w.Steps[k]) {
				out = append(out, fmt.Sprintf("step %d is not the rule's at %g rps", k+1, w.Steps[k].RPS))
			}
		}
	}
	if v.TopRPS != w.TopRPS || v.CellUSD != w.CellUSD || v.EditionUSD != w.EditionUSD {
		out = append(out, fmt.Sprintf("top %g rps, cell US$%s, edition US$%s; the rule gives %g, US$%s, US$%s", v.TopRPS, v.CellUSD, v.EditionUSD, w.TopRPS, w.CellUSD, w.EditionUSD))
	}
	return out
}

// WindowAllowances are the requests cfg's rate, token and spend-window
// limits admit in each of their evaluation windows, by its length in
// seconds, the least over the limits sharing a window: each limit's
// share at one request's nominal demand under v's token bounds and
// per-request price, rounded down. A limit that counts output at
// generation also gives its allowance for each window w under the key
// w plus client.http_timeout_s. A confidential limit counts at the value
// cfg holds.
func WindowAllowances(cfg *config.Config, v *Verdict) (map[float64]int64, error) {
	price, err := decimal(v.PerRequestUSD)
	if err != nil {
		return nil, fmt.Errorf("admission: window allowances: per_request_usd: %w", err)
	}
	r := &rule{cfg: cfg, in: v.InputTokens, ceiling: v.CeilingTokens, c: price}
	out := map[float64]int64{}
	for i := range cfg.Target.Limits {
		l := &cfg.Target.Limits[i]
		switch l.Dimension {
		case config.LimitRPM, config.LimitRPS, config.LimitTPM, config.LimitInputTPM, config.LimitOutputTPM, config.LimitSpendWindow:
			for _, w := range l.WindowsS() {
				a := r.allowance(l, dec(l.Value), w)
				keys := []float64{w}
				if l.OutputAtGeneration {
					keys = append(keys, w+float64(cfg.Client.HTTPTimeoutS))
				}
				for _, k := range keys {
					if have, ok := out[k]; !ok || a < have {
						out[k] = a
					}
				}
			}
		}
	}
	return out, nil
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

// cover is a cover test of demand d against value.
func cover(dim string, index int, value, d *big.Rat) Demand {
	fv, _ := value.Float64()
	fd, _ := d.Float64()
	ff, _ := new(big.Rat).Quo(d, value).Float64()
	return Demand{Dimension: dim, Limit: index, Value: fv, Demand: fd, Fraction: ff, Pass: d.Cmp(value) <= 0}
}

// dec is f at its shortest decimal form, exactly.
func dec(f float64) *big.Rat {
	r, ok := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 64))
	if !ok {
		panic("admission: not a finite number: " + strconv.FormatFloat(f, 'g', -1, 64))
	}
	return r
}

// floorInt is r rounded down to an integer; 0 for a negative r.
func floorInt(r *big.Rat) int64 {
	if r.Sign() <= 0 {
		return 0
	}
	return new(big.Int).Quo(r.Num(), r.Denom()).Int64()
}

func rat(f float64) *big.Rat { return new(big.Rat).SetFloat64(f) }

func ratInt(n int) *big.Rat { return new(big.Rat).SetInt64(int64(n)) }

func mul(a, b *big.Rat) *big.Rat { return new(big.Rat).Mul(a, b) }
