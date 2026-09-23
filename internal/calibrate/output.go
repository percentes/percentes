package calibrate

import (
	"fmt"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/redact"
)

// Output is the published calibration record: the full trace (§10), the
// decided rates, the configuration the steps derived from, the command
// options that bound the procedure, the instrument commit, and the error
// that ended the procedure early, if one did.
type Output struct {
	InstrumentCommit string         `json:"instrument_commit"`
	TargetURL        string         `json:"target_url"`
	MetricsURL       string         `json:"metrics_url"`
	QueueGauge       string         `json:"queue_gauge"`
	MaxRateRPS       float64        `json:"max_rate_rps,omitempty"`
	SkipReference    bool           `json:"skip_reference,omitempty"`
	StartedWall      time.Time      `json:"started_wall"`
	FinishedWall     time.Time      `json:"finished_wall"`
	Config           *config.Config `json:"config"`
	Calibration      *Result        `json:"calibration"`
	Error            string         `json:"error,omitempty"`
}

// Redact strips credentials from the endpoints the trace records.
func (o *Output) Redact() {
	o.TargetURL = redact.URL(o.TargetURL)
	o.MetricsURL = redact.URL(o.MetricsURL)
	if o.Config != nil {
		o.Config = o.Config.Redacted()
	}
}

const stepHeader = "  %10s %6s %9s %9s %9s %9s %8s %7s %6s  %s\n"

// Human renders the trace as text: one row per step, then the decision,
// then the reference run.
func Human(o *Output) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Percentes calibration (SPEC §10), instrument %s\n", o.InstrumentCommit)
	fmt.Fprintf(&b, "target %s, gauge %s at %s\n", o.TargetURL, o.QueueGauge, o.MetricsURL)
	fmt.Fprintf(&b, "started %s, finished %s\n", o.StartedWall.UTC().Format(time.RFC3339), o.FinishedWall.UTC().Format(time.RFC3339))
	if o.MaxRateRPS > 0 {
		fmt.Fprintf(&b, "rate ceiling %.4g rps (--max-rate); a ramp that reaches it without a failing step is invalid\n", o.MaxRateRPS)
	}
	if o.SkipReference {
		b.WriteString("reference step skipped (--skip-reference)\n")
	}
	if o.Error != "" {
		fmt.Fprintf(&b, "execution error, trace partial: %s\n", o.Error)
	}
	b.WriteString("\n")
	c := o.Calibration
	if c == nil {
		return b.String()
	}
	for i, r := range c.Ramps {
		fmt.Fprintf(&b, "ramp %d\n", i+1)
		fmt.Fprintf(&b, stepHeader, "rate_rps", "seed", "scheduled", "completed", "errored", "censored", "goodput", "queue", "gate", "verdict")
		for j := range r.Steps {
			b.WriteString(stepRow(&r.Steps[j]))
		}
		switch {
		case r.Valid && r.Reason != "":
			fmt.Fprintf(&b, "  lambda_max %.4g rps (%s)\n\n", r.LambdaMax, r.Reason)
		case r.Valid:
			fmt.Fprintf(&b, "  lambda_max %.4g rps\n\n", r.LambdaMax)
		case r.Reason != "":
			fmt.Fprintf(&b, "  invalid: %s\n\n", r.Reason)
		default:
			b.WriteString("  incomplete\n\n")
		}
	}
	if c.Valid {
		fmt.Fprintf(&b, "%s\nlambda_max %.4g rps, lambda_r %.4g rps (%.2f x lambda_max)\n", c.Decision, c.LambdaMax, c.LambdaR, config.PinnedLambdaRFrac)
		fmt.Fprintf(&b, "load.rate_rps for the %d-replica experiment: %.4g rps (%d x lambda_r)\n", config.PinnedExperimentReplicas, c.LoadRateRPS, config.PinnedExperimentReplicas)
	} else if c.Reason != "" {
		fmt.Fprintf(&b, "calibration invalid: %s\nno lambda_max recorded\n", c.Reason)
	}
	if s := c.Reference; s != nil {
		fmt.Fprintf(&b, "\nindependent reference (§5) at %d x lambda_r, %.4g s warm-up, %.4g s measured, judged by no gate\n", config.PinnedExperimentReplicas, s.SettleS, s.MeasureS)
		fmt.Fprintf(&b, "  %10s %6s %9s %9s %9s %9s %8s %13s %7s\n", "rate_rps", "seed", "scheduled", "completed", "errored", "censored", "goodput", "censored_rate", "queue")
		b.WriteString(referenceRow(s))
	}
	return b.String()
}

func counts(s *Step) (sched, comp, errd, cens int, censRate float64) {
	if s.Stats != nil {
		return s.Stats.Scheduled, s.Stats.Completed, s.Stats.Errored, s.Stats.Censored, s.Stats.CensoredRate
	}
	return 0, 0, 0, 0, 0
}

func queueCell(s *Step) string {
	if s.QueueSamples > 0 {
		return fmt.Sprintf("%.3f", s.QueueMean)
	}
	return "n/a"
}

func stepRow(s *Step) string {
	sched, comp, errd, cens, _ := counts(s)
	gate := "pass"
	if !s.Gates.Pass {
		gate = "FAIL"
	}
	verdict := "pass"
	switch {
	case s.Error != "":
		verdict = "ERROR: " + s.Error
	case !s.Passed():
		verdict = "FAIL: " + strings.Join(s.Reasons, "; ")
	}
	return fmt.Sprintf("  %10.4g %6d %9d %9d %9d %9d %8.4f %7s %6s  %s\n", s.RateRPS, s.Seed, sched, comp, errd, cens, s.Goodput, queueCell(s), gate, verdict)
}

func referenceRow(s *Step) string {
	sched, comp, errd, cens, censRate := counts(s)
	return fmt.Sprintf("  %10.4g %6d %9d %9d %9d %9d %8.4f %13.4f %7s\n", s.RateRPS, s.Seed, sched, comp, errd, cens, s.Goodput, censRate, queueCell(s))
}
