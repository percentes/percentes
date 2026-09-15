package calibrate

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/serverstats"
)

// capRunner passes a step iff its rate is at or under the capacity for
// the current ramp; a new ramp begins whenever the start rate recurs.
// failAt, when positive, makes that call (1-based) return an error;
// transient lists calls that fail whatever the rate; underAt makes that
// call report a short queue series.
type capRunner struct {
	caps      []float64
	failAt    int
	transient map[int]bool
	underAt   int
	ramp      int
	specs     []Spec
}

func (c *capRunner) RunStep(_ context.Context, sp Spec) (Step, error) {
	if sp.RateRPS == config.PinnedCalibrationStartRPS && len(c.specs) > 0 {
		c.ramp++
	}
	c.specs = append(c.specs, sp)
	call := len(c.specs)
	if c.failAt > 0 && call == c.failAt {
		return Step{Spec: sp}, errors.New("step aborted")
	}
	capacity := c.caps[min(c.ramp, len(c.caps)-1)]
	s := Step{Spec: sp, QueueSamples: 120, QueueExpected: 120, QueueIntervalS: 1, Gates: loadgen.GateReport{Pass: true}}
	if call == c.underAt {
		s.QueueSamples = 100
	}
	if sp.RateRPS <= capacity+1e-9 && !c.transient[call] {
		s.Goodput, s.QueueMean = 1, 0
	} else {
		s.Goodput, s.QueueMean = 0.9, 5
	}
	return s, nil
}

func (c *capRunner) rates() []float64 {
	out := make([]float64, len(c.specs))
	for i, sp := range c.specs {
		out[i] = sp.RateRPS
	}
	return out
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func sameRates(got, want []float64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if !near(got[i], want[i]) {
			return false
		}
	}
	return true
}

func TestRampCoarseThenFine(t *testing.T) {
	r := &capRunner{caps: []float64{12}}
	ramp, err := RunRamp(context.Background(), r, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Coarse 2, 4, 8, 16(fail); fine from 8 in steps of 0.8: 8.8 ... 12.0
	// pass, 12.8 fails.
	want := []float64{2, 4, 8, 16, 8.8, 9.6, 10.4, 11.2, 12, 12.8}
	if !sameRates(r.rates(), want) {
		t.Fatalf("rates run %v, want %v", r.rates(), want)
	}
	if !ramp.Valid || !near(ramp.LambdaMax, 12) || ramp.Reason != "" {
		t.Fatalf("lambda_max %v valid=%v reason=%q, want 12 valid", ramp.LambdaMax, ramp.Valid, ramp.Reason)
	}
	if ramp.Steps[3].Pass || !ramp.Steps[8].Pass || ramp.Steps[9].Pass {
		t.Fatal("pass flags do not follow the capacity")
	}
	for i, s := range ramp.Steps {
		if s.SettleS != config.PinnedCalibrationSettleS || s.MeasureS != config.PinnedCalibrationMeasureS {
			t.Fatalf("step timings %v/%v are not the pins", s.SettleS, s.MeasureS)
		}
		if s.Seed != int64(i) {
			t.Fatalf("step %d seed %d, want %d", i, s.Seed, i)
		}
	}
}

func TestRampFineFirstStepFailingKeepsTheCoarseRate(t *testing.T) {
	r := &capRunner{caps: []float64{8}}
	ramp, err := RunRamp(context.Background(), r, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{2, 4, 8, 16, 8.8}; !sameRates(r.rates(), want) {
		t.Fatalf("rates run %v, want %v", r.rates(), want)
	}
	if !ramp.Valid || !near(ramp.LambdaMax, 8) {
		t.Fatalf("lambda_max %v valid=%v, want 8 valid", ramp.LambdaMax, ramp.Valid)
	}
}

func TestRampInvalidWhenFirstStepFails(t *testing.T) {
	ramp, err := RunRamp(context.Background(), &capRunner{caps: []float64{1}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if ramp.Valid || ramp.LambdaMax != 0 || len(ramp.Steps) != 1 || ramp.Reason == "" {
		t.Fatalf("a failing first step must invalidate the ramp with no lambda_max: %+v", ramp)
	}
}

// A coarse step that fails after the fine ramp passed a lower rate is
// the ordinary path: the ramp retries the failed rate as a fine step and
// carries on.
func TestRampFineStepRevisitsTheFailedCoarseRate(t *testing.T) {
	// 16 fails once (call 4); the fine ramp from 8 reaches 16 at k=10 and
	// passes it, then 16.8 fails at capacity 16.
	r := &capRunner{caps: []float64{16}, transient: map[int]bool{4: true}}
	ramp, err := RunRamp(context.Background(), r, Options{})
	if err != nil {
		t.Fatal(err)
	}
	rates := r.rates()
	if !near(rates[3], 16) || !near(rates[13], 16) || !near(rates[14], 16.8) || len(rates) != 15 {
		t.Fatalf("rates run %v", rates)
	}
	if !ramp.Valid || !near(ramp.LambdaMax, 16) {
		t.Fatalf("lambda_max %v valid=%v, want 16 valid", ramp.LambdaMax, ramp.Valid)
	}
}

func TestRampCeilingCapsTheCoarseStep(t *testing.T) {
	// Capacity 38 under a 40 rps ceiling: the coarse candidate 64 runs at
	// 40 and fails, the fine ramp from 32 passes 35.2 and fails 38.4.
	r := &capRunner{caps: []float64{38}}
	ramp, err := RunRamp(context.Background(), r, Options{MaxRateRPS: 40})
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{2, 4, 8, 16, 32, 40, 35.2, 38.4}; !sameRates(r.rates(), want) {
		t.Fatalf("rates run %v, want %v", r.rates(), want)
	}
	if !ramp.Valid || !near(ramp.LambdaMax, 35.2) {
		t.Fatalf("lambda_max %v valid=%v, want 35.2 valid", ramp.LambdaMax, ramp.Valid)
	}

	// A ceiling step that passes has not measured the replica.
	r = &capRunner{caps: []float64{math.Inf(1)}}
	ramp, err = RunRamp(context.Background(), r, Options{MaxRateRPS: 40})
	if err != nil {
		t.Fatal(err)
	}
	if ramp.Valid || ramp.Reason == "" || !near(r.rates()[len(r.rates())-1], 40) {
		t.Fatalf("passing the ceiling must be invalid with the ceiling as the last rate: %+v rates %v", ramp, r.rates())
	}

	// A coarse candidate above the ceiling runs at it; passing there is
	// invalid too.
	r = &capRunner{caps: []float64{38}}
	ramp, err = RunRamp(context.Background(), r, Options{MaxRateRPS: 36})
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{2, 4, 8, 16, 32, 36}; !sameRates(r.rates(), want) {
		t.Fatalf("rates run %v, want %v", r.rates(), want)
	}
	if ramp.Valid || ramp.Reason == "" {
		t.Fatalf("a ceiling step that passes must be invalid: %+v", ramp)
	}

	// A ceiling at the start rate is one step: the dry run.
	r = &capRunner{caps: []float64{38}}
	ramp, err = RunRamp(context.Background(), r, Options{MaxRateRPS: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !sameRates(r.rates(), []float64{2}) || ramp.Valid || !strings.Contains(ramp.Reason, "ceiling 2 rps") {
		t.Fatalf("a ceiling at the start rate must run one step and end invalid: %+v rates %v", ramp, r.rates())
	}
}

// A fine ramp the ceiling cuts short has no failing step to bound
// lambda_max, so the ramp is invalid.
func TestRampFineRampReachingTheCeilingIsInvalid(t *testing.T) {
	// 16 fails once (call 4), so the fine ramp from 8 climbs by 0.8 toward
	// unbounded capacity; 20.8 is the last rate under a ceiling of 21.
	r := &capRunner{caps: []float64{math.Inf(1)}, transient: map[int]bool{4: true}}
	ramp, err := RunRamp(context.Background(), r, Options{MaxRateRPS: 21})
	if err != nil {
		t.Fatal(err)
	}
	rates := r.rates()
	if last := rates[len(rates)-1]; !near(last, 20.8) || len(rates) != 20 {
		t.Fatalf("rates run %v", rates)
	}
	if ramp.Valid || ramp.LambdaMax != 0 || !strings.Contains(ramp.Reason, "ceiling 21 rps") {
		t.Fatalf("fine ramp cut by the ceiling must be invalid with no lambda_max: %+v", ramp)
	}
	for _, s := range ramp.Steps[4:] {
		if !s.Pass {
			t.Fatalf("every fine step passed by construction: %+v", s)
		}
	}
}

func TestRampUnderObservedStepIsAnError(t *testing.T) {
	r := &capRunner{caps: []float64{12}, underAt: 3}
	ramp, err := RunRamp(context.Background(), r, Options{})
	if err == nil || !strings.Contains(err.Error(), "under-observed") {
		t.Fatalf("100 of 120 expected samples must stop the ramp: err %v", err)
	}
	if len(ramp.Steps) != 3 || ramp.Steps[2].Error == "" || ramp.Steps[2].Pass || ramp.Valid {
		t.Fatalf("the short step is kept, errored and unjudged: %+v", ramp)
	}
}

func TestRampStopsAtTheStepBound(t *testing.T) {
	ramp, err := RunRamp(context.Background(), &capRunner{caps: []float64{math.Inf(1)}}, Options{})
	if err == nil || !strings.Contains(err.Error(), "exceeded") || len(ramp.Steps) != maxRampSteps || ramp.Valid {
		t.Fatalf("an unbounded ramp must stop at %d steps: err %v, %d steps", maxRampSteps, err, len(ramp.Steps))
	}
}

func TestRampInterruptedBeforeAStep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ramp, err := RunRamp(ctx, &capRunner{caps: []float64{12}}, Options{})
	if err == nil || len(ramp.Steps) != 0 {
		t.Fatalf("a cancelled context must stop before the first step: err %v, %d steps", err, len(ramp.Steps))
	}
}

func TestRampReportsAfterEveryStep(t *testing.T) {
	calls, last := 0, 0
	ramp, err := RunRamp(context.Background(), &capRunner{caps: []float64{12}}, Options{OnStep: func(rp Ramp) {
		calls++
		last = len(rp.Steps)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if calls != len(ramp.Steps) || last != len(ramp.Steps) {
		t.Fatalf("%d reports for %d steps, last saw %d", calls, len(ramp.Steps), last)
	}
}

func TestJudgeCriteria(t *testing.T) {
	ok := loadgen.GateReport{Pass: true}
	cases := []struct {
		name string
		s    Step
		pass bool
	}{
		{"clean", Step{Goodput: 0.995, QueueMean: 1.0, QueueSamples: 10, Gates: ok}, true},
		{"goodput at the floor", Step{Goodput: 0.99, QueueMean: 0, QueueSamples: 10, Gates: ok}, true},
		{"goodput under floor", Step{Goodput: 0.989, QueueMean: 0, QueueSamples: 10, Gates: ok}, false},
		{"queue over maximum", Step{Goodput: 1, QueueMean: 1.01, QueueSamples: 10, Gates: ok}, false},
		{"queue unobserved", Step{Goodput: 1, QueueMean: 0, QueueSamples: 0, Gates: ok}, false},
		{"client gate failed", Step{Goodput: 1, QueueMean: 0, QueueSamples: 10}, false},
	}
	for _, c := range cases {
		Judge(&c.s)
		if c.s.Pass != c.pass {
			t.Fatalf("%s: pass=%v reasons=%v", c.name, c.s.Pass, c.s.Reasons)
		}
	}
}

func TestCoverageBoundary(t *testing.T) {
	for _, c := range []struct {
		samples, expected int
		want              bool
	}{{108, 120, true}, {107, 120, false}, {120, 120, true}, {0, 0, true}, {45, 50, true}, {44, 50, false}} {
		if got := coverageOK(c.samples, c.expected); got != c.want {
			t.Fatalf("coverageOK(%d, %d) = %v, want %v", c.samples, c.expected, got, c.want)
		}
	}
}

func TestDisagreeBoundary(t *testing.T) {
	for _, c := range []struct {
		a, b float64
		want bool
	}{{9, 10, false}, {18, 20, false}, {8.99, 10, true}, {10, 9, false}, {12, 12, false}, {12, 20, true}} {
		if got := disagree(c.a, c.b); got != c.want {
			t.Fatalf("disagree(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestCalibrateAgreeingRampsTakeTheLower(t *testing.T) {
	// Capacity 12 gives 12.0; capacity 12.8 gives 12.8 (13.6 fails). The
	// two agree (0.8 is within 1.28) and the lower stands, in either order.
	for _, caps := range [][]float64{{12, 12.8}, {12.8, 12}} {
		r := &capRunner{caps: caps}
		res, err := Calibrate(context.Background(), r, Options{Seed: 7})
		if err != nil {
			t.Fatal(err)
		}
		if !res.Valid || len(res.Ramps) != 2 || !near(res.LambdaMax, 12) {
			t.Fatalf("caps %v: got %+v", caps, res)
		}
		if !near(res.LambdaR, config.PinnedLambdaRFrac*12) || !near(res.LoadRateRPS, config.PinnedExperimentReplicas*config.PinnedLambdaRFrac*12) {
			t.Fatalf("lambda_r %v load %v", res.LambdaR, res.LoadRateRPS)
		}
		if res.Ramps[1].Steps[0].Seed != 7+maxRampSteps {
			t.Fatalf("ramp 2 seeds must start %d past ramp 1's: %d", maxRampSteps, res.Ramps[1].Steps[0].Seed)
		}
	}
}

func TestCalibrateMedianOverThreeOrders(t *testing.T) {
	// Capacity 20 lands at 19.2 (32 fails, fine from 16 by 1.6: 17.6 and
	// 19.2 pass, 20.8 fails); capacity 30 at 28.8; capacity 13 at 12.8.
	for _, c := range []struct {
		caps []float64
		want float64
	}{
		{[]float64{12, 20, 30}, 19.2}, // ramp 2 is the median
		{[]float64{20, 12, 30}, 19.2}, // ramp 1 is the median
		{[]float64{12, 20, 13}, 12.8}, // ramp 3 is the median
	} {
		res, err := Calibrate(context.Background(), &capRunner{caps: c.caps}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Ramps) != 3 || !res.Valid || !near(res.LambdaMax, c.want) {
			t.Fatalf("caps %v: lambda_max %v over %d ramps (%v %v %v), want %v", c.caps, res.LambdaMax, len(res.Ramps),
				res.Ramps[0].LambdaMax, res.Ramps[1].LambdaMax, res.Ramps[len(res.Ramps)-1].LambdaMax, c.want)
		}
	}
}

func TestCalibrateInvalidRampStopsTheProcedure(t *testing.T) {
	res, err := Calibrate(context.Background(), &capRunner{caps: []float64{12, 1}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid || res.LambdaMax != 0 || res.Reason == "" || len(res.Ramps) != 2 {
		t.Fatalf("an invalid ramp must invalidate the calibration: %+v", res)
	}
}

func TestCalibrateKeepsThePartialTraceOnError(t *testing.T) {
	// Ramp 1 takes ten steps; the third step of ramp 2 errors and is kept.
	r := &capRunner{caps: []float64{12}, failAt: 13}
	res, err := Calibrate(context.Background(), r, Options{})
	if err == nil {
		t.Fatal("the step error must surface")
	}
	if res == nil || len(res.Ramps) != 2 || len(res.Ramps[0].Steps) != 10 || len(res.Ramps[1].Steps) != 3 || res.Valid {
		t.Fatalf("partial trace not kept: %+v", res)
	}
	if s := res.Ramps[1].Steps[2]; s.Error == "" || s.Pass || !near(s.RateRPS, 8) {
		t.Fatalf("the errored step is kept with its error and rate: %+v", s)
	}
}

func TestCalibrateProgressSnapshotsTheProcedure(t *testing.T) {
	var snaps []*Result
	res, err := Calibrate(context.Background(), &capRunner{caps: []float64{12}}, Options{Progress: func(p *Result) { snaps = append(snaps, p) }})
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 20 {
		t.Fatalf("%d snapshots for 20 steps", len(snaps))
	}
	if s := snaps[9]; len(s.Ramps) != 1 || len(s.Ramps[0].Steps) != 10 || s.Valid {
		t.Fatalf("snapshot after ramp 1's last step: %+v", s)
	}
	if s := snaps[10]; len(s.Ramps) != 2 || len(s.Ramps[1].Steps) != 1 || len(s.Ramps[0].Steps) != 10 {
		t.Fatalf("snapshot after ramp 2's first step: %+v", s)
	}
	if !res.Valid || len(res.Ramps) != 2 {
		t.Fatalf("result %+v", res)
	}
}

func TestReferenceRunsAtTheSurvivorLoadUnjudged(t *testing.T) {
	r := &capRunner{caps: []float64{12}}
	res, err := Calibrate(context.Background(), r, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := Reference(context.Background(), r, res, 9000); err != nil {
		t.Fatal(err)
	}
	s := res.Reference
	if s == nil || !near(s.RateRPS, 2*res.LambdaR) || s.SettleS != config.PinnedWarmupS || s.MeasureS != config.PinnedBaselineS || s.Seed != 9000 {
		t.Fatalf("reference step %+v", s)
	}
	if s.Pass || s.Reasons != nil {
		t.Fatalf("the reference is judged by no gate: %+v", s)
	}
}

func TestCheckConfigRefusesWhatSection10DoesNotCalibrate(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/experiment.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckConfig(cfg); err != nil {
		t.Fatalf("the experiment reference must pass: %v", err)
	}
	hosted := *cfg
	hosted.Target.Hosted = true
	if err := CheckConfig(&hosted); err == nil {
		t.Fatal("a hosted target must be refused")
	}
	det := *cfg
	det.Load.ArrivalProcess = "deterministic"
	if err := CheckConfig(&det); err == nil {
		t.Fatal("deterministic arrivals must be refused")
	}
	ac := *cfg
	ac.Profile = config.ProfileAC
	if err := CheckConfig(&ac); err == nil {
		t.Fatal("the ac profile must be refused")
	}
}

func TestOutputCarriesEveryStepAndTheSeries(t *testing.T) {
	res, err := Calibrate(context.Background(), &capRunner{caps: []float64{12}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range res.Ramps[0].Steps {
		res.Ramps[0].Steps[i].QueueSeries = []serverstats.Sample{{Replica: "r0", Value: float64(i)}}
	}
	res.Ramps[1].Steps[1].Error = "step aborted"
	raw, err := json.Marshal(&Output{Calibration: res, MaxRateRPS: 40, SkipReference: true})
	if err != nil {
		t.Fatal(err)
	}
	var back Output
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Calibration.Ramps[0].Steps) != len(res.Ramps[0].Steps) || back.MaxRateRPS != 40 || !back.SkipReference {
		t.Fatal("steps or options lost in the record")
	}
	for i, s := range back.Calibration.Ramps[0].Steps {
		if len(s.QueueSeries) != 1 || s.QueueSeries[0].Value != float64(i) || s.RateRPS != res.Ramps[0].Steps[i].RateRPS {
			t.Fatalf("step %d lost its series or rate: %+v", i, s)
		}
	}
	human := Human(&back)
	for _, s := range res.Ramps[0].Steps {
		if !strings.Contains(human, strings.TrimSpace(strings.Split(stepRow(&s), "\n")[0])) {
			t.Fatalf("human trace lacks the row for %.4g rps", s.RateRPS)
		}
	}
	for _, want := range []string{"load.rate_rps for the 2-replica experiment: 15.6 rps", "rate ceiling 40 rps", "reference step skipped", "ERROR: step aborted"} {
		if !strings.Contains(human, want) {
			t.Fatalf("human trace lacks %q:\n%s", want, human)
		}
	}
}
