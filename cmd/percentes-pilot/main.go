// percentes-pilot runs the SPEC-HOSTED HA2 descriptive pilot from one
// pilot-profile config: the pre-run probe, the development gate and the
// paired sessions, each request counted in the admission ledger before it
// is sent. Probe and development traffic is sent at the session rate or
// slower. The bearer tokens come from the env files named with --env,
// never from the inherited environment, and are never printed.
//
//	percentes-pilot ledger create|price|hosting|show --ledger FILE ...
//	percentes-pilot probe --config FILE --env FILE --ledger FILE --out DIR
//	percentes-pilot dev --config FILE --env FILE --ledger FILE --out DIR
//	percentes-pilot session --config FILE --env FILE --ledger FILE --out DIR
//	    --schedule FILE --session LABEL --frames DIR --clock-sync CMD
//	    --status-cmd CMD --operator NAME=FILE --operator NAME=FILE
//	percentes-pilot check-cpu
//
// Exit codes: 0 = done, with every window client-valid, every probe
// unblocked and every endpoint through the development gate; 2 = a
// window was client-invalid or did not start, a probe was blocked, the
// second window started more than five minutes after the first drained,
// or an endpoint failed the gate; 1 = execution error or refusal.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/score"
)

// Exit codes.
const (
	exitOK      = 0
	exitError   = 1
	exitAdverse = 2
)

// scoringCutoff is the scoring deadline from intended dispatch, inclusive
// (SPEC-HOSTED HA2 Terms).
const scoringCutoff = 10 * time.Second

// Finish reasons the scorer names: the ordinary complete answer and the
// length limit. Any other reason is instrument-incompatible.
var (
	acceptedFinish = []string{"stop"}
	lengthFinish   = []string{"length"}
)

const usage = `usage: percentes-pilot <command> [flags]

commands:
  ledger create|price|hosting|show   the admission ledger
  probe                              the pre-run probe of each endpoint
  dev                                the development gate
  session                            one paired session
  check-cpu                          whether this build measures host CPU

Run percentes-pilot <command> -h for the command's flags.
`

func main() {
	os.Exit(dispatch(os.Args[1:]))
}

func dispatch(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return exitError
	}
	ctx, stop := operatorContext()
	defer stop()
	var (
		code int
		err  error
	)
	switch args[0] {
	case "ledger":
		code, err = ledgerCmd(args[1:])
	case "probe":
		code, err = probeCmd(ctx, args[1:])
	case "dev":
		code, err = devCmd(ctx, args[1:])
	case "session":
		code, err = sessionCmd(ctx, args[1:])
	case "check-cpu":
		code, err = checkCPUCmd(args[1:])
	default:
		fmt.Fprint(os.Stderr, usage)
		return exitError
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "percentes-pilot: %v\n", err)
		return exitError
	}
	return code
}

// operatorContext is cancelled with loadgen.ErrOperatorAbort on SIGINT or
// SIGTERM, so a window it stops records an operator abort.
func operatorContext() (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case <-sig:
			cancel(loadgen.ErrOperatorAbort)
		case <-done:
		}
	}()
	return ctx, func() {
		signal.Stop(sig)
		close(done)
		cancel(nil)
	}
}

// listFlag collects a repeated flag's values.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(s string) error {
	*l = append(*l, s)
	return nil
}

// loadPilot loads a config and refuses one outside the pilot profile.
func loadPilot(path string) (*config.Config, error) {
	if path == "" {
		return nil, errors.New("--config is required")
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		return nil, err
	}
	if cfg.Profile != config.ProfilePilot {
		return nil, fmt.Errorf("%s: profile %q, want %q", path, cfg.Profile, config.ProfilePilot)
	}
	return cfg, nil
}

// windowConfig is cfg with t as the run's target, the form loadgen, the
// evidence writers and the gates read.
func windowConfig(cfg *config.Config, t *config.Target) *config.Config {
	w := *cfg
	w.Target = *t
	return &w
}

// selectTargets returns the targets named, in config order, or every
// target when names is empty.
func selectTargets(cfg *config.Config, names []string) ([]*config.Target, error) {
	var out []*config.Target
	seen := map[string]bool{}
	for i := range cfg.Pilot.Targets {
		t := &cfg.Pilot.Targets[i]
		if len(names) == 0 || contains(names, t.Name) {
			out = append(out, t)
			seen[t.Name] = true
		}
	}
	for _, n := range names {
		if !seen[n] {
			return nil, fmt.Errorf("--target %q is not a pilot target", n)
		}
	}
	return out, nil
}

func contains(list []string, s string) bool {
	for _, e := range list {
		if e == s {
			return true
		}
	}
	return false
}

// fixture is the pilot's ticket fixture.
func fixture() (*pilot.Fixture, error) {
	return pilot.Generate(pilot.Seed)
}

// newScorer scores answers against the pilot's labels.
func newScorer() (*score.Scorer, error) {
	c, err := score.NewContract(pilot.Labels)
	if err != nil {
		return nil, err
	}
	return score.NewScorer(c, score.Rule{Cutoff: scoringCutoff, Accepted: acceptedFinish, Length: lengthFinish})
}

// waitUntil sleeps until t, or returns ctx's cause when it ends first.
func waitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// interval is the session's spacing between dispatches under cfg.
func interval(cfg *config.Config) time.Duration {
	return time.Duration(float64(time.Second) / cfg.Load.RateRPS)
}

// writeJSON writes v, indented, to path; the file must not exist.
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// replaceJSON writes v, indented, to path, replacing any file there.
func replaceJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// checkCPUCmd samples host CPU through the loadgen monitor and prints
// whether a session's client-validity gate could measure it.
func checkCPUCmd(args []string) (int, error) {
	if len(args) > 0 {
		return exitError, fmt.Errorf("check-cpu takes no arguments")
	}
	ok := loadgen.CPUMeasured(config.PinnedCPUWindowS)
	fmt.Printf("cpu_measured: %t\n", ok)
	if !ok {
		return exitAdverse, nil
	}
	return exitOK, nil
}
