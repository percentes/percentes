// Package calibrate measures the single-replica capacity lambda_max by the
// §10 ramp and freezes lambda_r from it. The ramp runs against a Runner;
// LoadRunner drives the load generator against one replica.
package calibrate

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/serverstats"
)

// Spec is one step's parameters: the rate held, the settle period,
// discarded, the measured period, and the arrival-schedule seed.
type Spec struct {
	RateRPS  float64 `json:"rate_rps"`
	SettleS  float64 `json:"settle_s"`
	MeasureS float64 `json:"measure_s"`
	Seed     int64   `json:"seed"`
}

// Step is one Spec as run. The §10 criteria apply to the measured
// portion, bounded by the two wall times.
type Step struct {
	Spec
	StartedWall       time.Time `json:"started_wall"`
	MeasuredStartWall time.Time `json:"measured_start_wall"`
	MeasuredEndWall   time.Time `json:"measured_end_wall"`
	// Stats is the §3 collection over the measured window.
	Stats   *collect.Stats `json:"stats,omitempty"`
	Goodput float64        `json:"goodput"`
	// QueueMean is the waiting-queue gauge's mean over the measured
	// window, from QueueSamples of the QueueExpected at the recorded
	// cadence. QueueSeries is every sample the step took, settle included.
	QueueMean      float64              `json:"queue_mean"`
	QueueSamples   int                  `json:"queue_samples"`
	QueueExpected  int                  `json:"queue_expected"`
	QueueIntervalS float64              `json:"queue_interval_s"`
	QueueSeries    []serverstats.Sample `json:"queue_series,omitempty"`
	ScrapeErrors   int                  `json:"scrape_errors"`
	Gates          loadgen.GateReport   `json:"gates"`
	// Pass is the §10 verdict, absent until Judge decides the step. The
	// §5 reference and a step that ended in an error carry no verdict.
	Pass    *bool    `json:"pass,omitempty"`
	Reasons []string `json:"reasons,omitempty"`
	// Error is the execution error that ended the step, when one did. An
	// errored step is not judged.
	Error string `json:"error,omitempty"`
}

// Runner runs one Spec and returns the step's numbers. Judge decides the
// step.
type Runner interface {
	RunStep(ctx context.Context, sp Spec) (Step, error)
}

// Judge applies the §10 pass criteria: goodput at least the pinned
// minimum, waiting-queue mean at most the pinned maximum, client-validity
// gate clean. An unobserved gauge fails, as it does for G7.
func Judge(s *Step) {
	s.Reasons = nil
	if s.Goodput < config.PinnedCalibrationGoodputMin {
		s.Reasons = append(s.Reasons, fmt.Sprintf("goodput %.4f below %.2f", s.Goodput, config.PinnedCalibrationGoodputMin))
	}
	switch {
	case s.QueueSamples == 0:
		s.Reasons = append(s.Reasons, "waiting-queue gauge unobserved over the measured window")
	case s.QueueMean > config.PinnedQueueGaugeMax:
		s.Reasons = append(s.Reasons, fmt.Sprintf("waiting-queue mean %.3f above %.1f", s.QueueMean, config.PinnedQueueGaugeMax))
	}
	if !s.Gates.Pass {
		s.Reasons = append(s.Reasons, "client-validity gate failed (§2)")
	}
	pass := len(s.Reasons) == 0
	s.Pass = &pass
}

// Passed reports a decided pass. An undecided step is not one.
func (s *Step) Passed() bool { return s.Pass != nil && *s.Pass }

// coverageOK reports whether samples reach the pinned fraction of the
// expected count (§10).
func coverageOK(samples, expected int) bool {
	return float64(samples) >= config.PinnedQueueCoverageMin*float64(expected)
}

// Ramp is one coarse-then-fine pass. LambdaMax is the highest passing
// rate; Valid is false when the first step failed or the ceiling was
// reached without a failing step, and Reason names which.
type Ramp struct {
	Steps     []Step  `json:"steps"`
	LambdaMax float64 `json:"lambda_max"`
	Valid     bool    `json:"valid"`
	Reason    string  `json:"reason,omitempty"`
}

// Options bound a ramp beyond the pinned procedure. MaxRateRPS, when
// positive, caps the rate sent: a coarse candidate at or above it runs at
// the ceiling, and a ramp that reaches the ceiling without a failing step
// ends invalid. Seed is the schedule seed of the ramp's first step; each
// step adds one. Zero settle or measure selects the pins. OnStep receives
// the ramp so far after every step; Progress receives the procedure so
// far after every step.
type Options struct {
	MaxRateRPS float64
	SettleS    float64
	MeasureS   float64
	Seed       int64
	OnStep     func(Ramp)
	Progress   func(*Result)
}

func (o Options) settle() float64 {
	if o.SettleS > 0 {
		return o.SettleS
	}
	return config.PinnedCalibrationSettleS
}

func (o Options) measure() float64 {
	if o.MeasureS > 0 {
		return o.MeasureS
	}
	return config.PinnedCalibrationMeasureS
}

func (o Options) notify(rp Ramp) {
	if o.OnStep != nil {
		o.OnStep(rp)
	}
}

// maxRampSteps bounds a ramp; the seed ranges of consecutive ramps are
// this far apart.
const maxRampSteps = 1000

// RunRamp runs the coarse ramp from the start rate, doubling until a step
// fails, then the fine ramp from the last passing rate in steps of the
// pinned fraction of that rate until a step fails. The partial ramp is
// returned with any error; an errored step is kept with its error.
func RunRamp(ctx context.Context, r Runner, o Options) (Ramp, error) {
	var ramp Ramp
	step := func(rate float64) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, fmt.Errorf("calibrate: interrupted before the %.4g rps step: %w", rate, err)
		}
		if len(ramp.Steps) >= maxRampSteps {
			return false, fmt.Errorf("calibrate: ramp exceeded %d steps", maxRampSteps)
		}
		sp := Spec{RateRPS: rate, SettleS: o.settle(), MeasureS: o.measure(), Seed: o.Seed + int64(len(ramp.Steps))}
		s, err := r.RunStep(ctx, sp)
		if err == nil && s.QueueExpected > 0 && !coverageOK(s.QueueSamples, s.QueueExpected) {
			err = fmt.Errorf("calibrate: waiting-queue gauge under-observed: %d of %d expected samples in the measured window, %d scrape errors (§10)", s.QueueSamples, s.QueueExpected, s.ScrapeErrors)
		}
		if err != nil {
			s.Spec, s.Error = sp, err.Error()
			ramp.Steps = append(ramp.Steps, s)
			o.notify(ramp)
			return false, err
		}
		Judge(&s)
		ramp.Steps = append(ramp.Steps, s)
		o.notify(ramp)
		return s.Passed(), nil
	}

	last := 0.0
	for rate := config.PinnedCalibrationStartRPS; ; rate *= 2 {
		capped := false
		if o.MaxRateRPS > 0 && rate >= o.MaxRateRPS {
			rate, capped = o.MaxRateRPS, true
		}
		pass, err := step(rate)
		if err != nil {
			return ramp, err
		}
		if !pass {
			if last == 0 {
				ramp.Reason = fmt.Sprintf("the first step, %.4g rps, failed", rate)
				return ramp, nil
			}
			break
		}
		last = rate
		if capped {
			ramp.Reason = fmt.Sprintf("ceiling %.4g rps reached without a failing step", o.MaxRateRPS)
			return ramp, nil
		}
	}

	base, inc := last, config.PinnedCalibrationFineStepFrac*last
	for k := 1; ; k++ {
		rate := base + float64(k)*inc
		if o.MaxRateRPS > 0 && rate > o.MaxRateRPS {
			ramp.Reason = fmt.Sprintf("fine ramp reached the ceiling %.4g rps without a failing step", o.MaxRateRPS)
			return ramp, nil
		}
		pass, err := step(rate)
		if err != nil {
			return ramp, err
		}
		if !pass {
			break
		}
		last = rate
	}
	ramp.LambdaMax, ramp.Valid = last, true
	return ramp, nil
}

// Result is the whole procedure: two ramps, a third when they disagree,
// the decided lambda_max, lambda_r, the experiment's offered load, and
// the §5 reference run.
type Result struct {
	Ramps     []Ramp  `json:"ramps"`
	LambdaMax float64 `json:"lambda_max"`
	LambdaR   float64 `json:"lambda_r"`
	// LoadRateRPS is replicas x lambda_r, the value load.rate_rps takes
	// in the experiment configuration (§10).
	LoadRateRPS float64 `json:"load_rate_rps"`
	Decision    string  `json:"decision"`
	Valid       bool    `json:"valid"`
	Reason      string  `json:"reason,omitempty"`
	Reference   *Step   `json:"reference,omitempty"`
}

// disagree reports whether two lambda_max values differ by more than the
// pinned fraction of the larger (§10).
func disagree(a, b float64) bool {
	return math.Abs(a-b) > config.PinnedCalibrationAgreementFrac*math.Max(a, b)
}

// Calibrate runs the procedure twice; when the two lambda_max values
// disagree, a third ramp decides by median; when they agree, lambda_max
// is the lower of the two (§10). Each ramp's seeds start
// maxRampSteps apart. Every ramp and step so far is kept when a step
// errors.
func Calibrate(ctx context.Context, r Runner, o Options) (*Result, error) {
	res := &Result{}
	ramp := func(i int) (bool, error) {
		ro := o
		ro.Seed = o.Seed + int64(i)*maxRampSteps
		if o.Progress != nil {
			ro.OnStep = func(rp Ramp) {
				snap := *res
				snap.Ramps = append(append([]Ramp(nil), res.Ramps...), rp)
				o.Progress(&snap)
			}
		}
		rp, err := RunRamp(ctx, r, ro)
		res.Ramps = append(res.Ramps, rp)
		if err != nil {
			return false, err
		}
		if !rp.Valid {
			res.Reason = fmt.Sprintf("ramp %d invalid: %s", i+1, rp.Reason)
		}
		return rp.Valid, nil
	}
	for i := 0; i < 2; i++ {
		if ok, err := ramp(i); err != nil || !ok {
			return res, err
		}
	}
	a, b := res.Ramps[0].LambdaMax, res.Ramps[1].LambdaMax
	if disagree(a, b) {
		if ok, err := ramp(2); err != nil || !ok {
			return res, err
		}
		vals := []float64{a, b, res.Ramps[2].LambdaMax}
		sort.Float64s(vals)
		res.LambdaMax = vals[1]
		res.Decision = fmt.Sprintf("ramps 1 and 2 differed by more than %.0f%% of the larger; ramp 3 decided by median (§10)", config.PinnedCalibrationAgreementFrac*100)
	} else {
		res.LambdaMax = math.Min(a, b)
		res.Decision = fmt.Sprintf("ramps 1 and 2 agreed within %.0f%% of the larger; lambda_max is the lower (§10)", config.PinnedCalibrationAgreementFrac*100)
	}
	res.LambdaR = config.PinnedLambdaRFrac * res.LambdaMax
	res.LoadRateRPS = float64(config.PinnedExperimentReplicas) * res.LambdaR
	res.Valid = true
	return res, nil
}

// Reference runs the §5 independent reference: one no-fault step at the
// post-fault survivor load, 2 lambda_r, over the §1 warm-up and baseline
// durations. It is published and appears in no gate (§5), so it is not
// judged.
func Reference(ctx context.Context, r Runner, res *Result, seed int64) error {
	s, err := r.RunStep(ctx, Spec{RateRPS: res.LoadRateRPS, SettleS: config.PinnedWarmupS, MeasureS: config.PinnedBaselineS, Seed: seed})
	if err != nil {
		return err
	}
	res.Reference = &s
	return nil
}

// CheckConfig refuses a configuration §10 does not calibrate from: the
// experiment profile, Poisson arrivals, and a self-hosted target.
func CheckConfig(cfg *config.Config) error {
	if cfg.Profile != config.ProfileExperiment {
		return fmt.Errorf("calibrate: profile must be %q (§10: the experiment's exact pins), got %q", config.ProfileExperiment, cfg.Profile)
	}
	if cfg.Load.ArrivalProcess != "poisson" {
		return fmt.Errorf("calibrate: load.arrival_process must be \"poisson\" (§10), got %q", cfg.Load.ArrivalProcess)
	}
	if cfg.Target.Hosted {
		return fmt.Errorf("calibrate: target.hosted must be false: §10 calibrates one self-hosted replica by direct address")
	}
	return nil
}
