package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/score"
)

// Development gate: at least devMinCorrect correct, complete answers of
// the development cases, and at least devMinPerLabel in each label.
const (
	devMinCorrect  = 22
	devMinPerLabel = 5
)

// devAttempts is the first attempt and its one recorded revision.
const devAttempts = 2

// Files in a development attempt's directory.
const (
	devConfigFile = "config.yaml"
	devRecordFile = "dev.json"
)

// devRecord is one development attempt.
type devRecord struct {
	SchemaVersion int    `json:"schema_version"`
	Attempt       int    `json:"attempt"`
	Revision      string `json:"revision,omitempty"`
	ConfigSHA256  string `json:"config_sha256"`
	// Rule states the gate; Correct counts success and late_correct, the
	// correct, complete answers.
	Rule      string        `json:"rule"`
	StartUTC  time.Time     `json:"start_utc"`
	EndUTC    time.Time     `json:"end_utc"`
	Endpoints []devEndpoint `json:"endpoints"`
}

// devEndpoint is one endpoint's development run and its gate result.
type devEndpoint struct {
	Endpoint string              `json:"endpoint"`
	Sent     int                 `json:"sent"`
	Correct  int                 `json:"correct"`
	ByCutoff int                 `json:"correct_by_cutoff"`
	PerLabel map[string]int      `json:"correct_per_label"`
	Classes  map[score.Class]int `json:"classes"`
	Pass     bool                `json:"pass"`
	Error    string              `json:"error,omitempty"`
}

// devCmd sends the development cases to each endpoint at the session
// rate and applies the gate. The first attempt is dev-1 under --out; one
// revision, of the system text and the generation settings only, runs as
// dev-2 with its reason. A started attempt counts whether or not it
// finished.
func devCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "pilot config (required)")
	var envFiles listFlag
	fs.Var(&envFiles, "env", "env file holding a target's api_key_env (repeatable, required)")
	ledgerPath := fs.String("ledger", "", "admission ledger file (required)")
	out := fs.String("out", "", "development directory; attempts are dev-1 and dev-2 under it (required)")
	revision := fs.String("revision", "", "the reason for the one revision after dev-1 (required for dev-2)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *out == "" || *ledgerPath == "" {
		return exitError, errors.New("dev: --out and --ledger are required")
	}
	cfg, err := loadPilot(*cfgPath)
	if err != nil {
		return exitError, err
	}
	attempt, err := nextDevAttempt(*out, cfg, *revision)
	if err != nil {
		return exitError, err
	}
	targets, err := selectTargets(cfg, nil)
	if err != nil {
		return exitError, err
	}
	if err := loadKeys(envFiles, targets); err != nil {
		return exitError, err
	}
	ledger, err := admission.Open(*ledgerPath)
	if err != nil {
		return exitError, err
	}
	fx, err := fixture()
	if err != nil {
		return exitError, err
	}
	scorer, err := newScorer()
	if err != nil {
		return exitError, err
	}

	dir := filepath.Join(*out, fmt.Sprintf("dev-%d", attempt))
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return exitError, err
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		return exitError, err
	}
	if err := os.WriteFile(filepath.Join(dir, devConfigFile), cfg.Raw, 0o644); err != nil {
		return exitError, err
	}
	sum := sha256.Sum256(cfg.Raw)
	rec := &devRecord{
		SchemaVersion: 1,
		Attempt:       attempt,
		Revision:      *revision,
		ConfigSHA256:  hex.EncodeToString(sum[:]),
		Rule:          fmt.Sprintf("at least %d of %d correct, complete answers, and at least %d of %d in each label", devMinCorrect, len(fx.Development), devMinPerLabel, pilot.DevelopmentPerLabel),
		StartUTC:      time.Now().UTC(),
	}
	code := exitOK
	var runErr error
	for _, t := range targets {
		e, err := devRun(ctx, ledger, cfg, t, fx.Development, scorer, filepath.Join(dir, t.Name))
		if err != nil {
			e.Error = err.Error()
			runErr = err
		}
		rec.Endpoints = append(rec.Endpoints, *e)
		fmt.Printf("%s: %d of %d correct (%d by the cutoff), per label %v: %s\n", t.Name, e.Correct, len(fx.Development), e.ByCutoff, e.PerLabel, passWord(e.Pass))
		if !e.Pass {
			code = exitAdverse
		}
		if err != nil {
			break
		}
	}
	rec.EndUTC = time.Now().UTC()
	if err := writeJSON(filepath.Join(dir, devRecordFile), rec); err != nil {
		return exitError, errors.Join(runErr, err)
	}
	if runErr != nil {
		return exitError, runErr
	}
	return code, nil
}

func passWord(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

// nextDevAttempt returns the attempt number the next development run
// takes under out. The second needs a reason and a config that differs
// from the first's only in the system text and the targets' generation
// settings; there is no third.
func nextDevAttempt(out string, cfg *config.Config, reason string) (int, error) {
	attempt := 1
	for ; attempt <= devAttempts; attempt++ {
		if _, err := os.Stat(filepath.Join(out, fmt.Sprintf("dev-%d", attempt))); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return 0, err
		}
	}
	switch {
	case attempt > devAttempts:
		return 0, fmt.Errorf("dev: %s holds dev-1 and its one revision, dev-2; no further development run is allowed", out)
	case attempt == 1 && reason != "":
		return 0, errors.New("dev: --revision applies to the second attempt; dev-1 does not exist yet")
	case attempt == 2 && reason == "":
		return 0, errors.New("dev: dev-1 exists; the one revision needs its reason in --revision")
	case attempt == 2:
		raw, err := os.ReadFile(filepath.Join(out, "dev-1", devConfigFile))
		if err != nil {
			return 0, fmt.Errorf("dev: the first attempt's config: %w", err)
		}
		prev, err := config.Parse(raw)
		if err != nil {
			return 0, fmt.Errorf("dev: the first attempt's config: %w", err)
		}
		if err := checkRevision(prev, cfg); err != nil {
			return 0, err
		}
	}
	return attempt, nil
}

// checkRevision refuses a revision that changes nothing, or anything
// beyond pilot.body (whose system text is the only free part) and the
// targets' params and not_applied lists.
func checkRevision(prev, next *config.Config) error {
	if string(prev.Raw) == string(next.Raw) {
		return errors.New("dev: the revision's config is identical to dev-1's")
	}
	strip := func(c *config.Config) config.Config {
		s := *c
		s.Raw = nil
		if c.Pilot != nil {
			p := *c.Pilot
			p.Body = ""
			p.Targets = append([]config.Target(nil), p.Targets...)
			for i := range p.Targets {
				p.Targets[i].Params, p.Targets[i].NotApplied = nil, nil
			}
			s.Pilot = &p
		}
		return s
	}
	if a, b := strip(prev), strip(next); !reflect.DeepEqual(a, b) {
		return errors.New("dev: the revision may change only the system text and the targets' generation settings (params, not_applied)")
	}
	return nil
}

// devRun admits the development cases for t, sends them in fixture order
// at the session rate through the session's request path, scores them
// and settles the admission with the requests sent.
func devRun(ctx context.Context, ledger *admission.Ledger, cfg *config.Config, t *config.Target, cases []pilot.Ticket, scorer *score.Scorer, dir string) (e *devEndpoint, err error) {
	e = &devEndpoint{Endpoint: t.Name, PerLabel: map[string]int{}}
	wc := windowConfig(cfg, t)
	// Half an interval past the last dispatch keeps the floor in the
	// schedule's count clear of rounding.
	wc.Run.Phases = config.Phases{BaselineS: (float64(len(cases)) + 0.5) / wc.Load.RateRPS}
	if n := len(loadgen.BuildSchedule(wc)); n != len(cases) {
		return e, fmt.Errorf("dev: %d scheduled requests for %d cases", n, len(cases))
	}
	texts := make([]string, len(cases))
	labels := make([]string, len(cases))
	for i, c := range cases {
		texts[i], labels[i] = c.Text, c.Label
	}
	a, err := ledger.Admit(t.EndpointURL, admission.PurposeDevelopment, len(cases))
	if err != nil {
		return e, err
	}
	var res *loadgen.Result
	defer func() {
		_, sent := admission.RunSent(res)
		e.Sent = sent
		if serr := ledger.Settle(a, admission.Counts{Development: sent}); serr != nil {
			err = errors.Join(err, serr)
		}
	}()
	res, err = loadgen.Run(ctx, wc, &loadgen.Hooks{Cases: texts})
	if res == nil {
		return e, err
	}
	opt := evidence.Options{Scoring: &evidence.Scoring{Scorer: scorer, Expected: labels}}
	if _, werr := evidence.WriteRun(dir, wc, res, opt); werr != nil {
		return e, errors.Join(err, werr)
	}
	devGate(e, evidence.Records(wc, res, opt))
	return e, err
}

// devGate fills e's counts and gate result from scored records.
func devGate(e *devEndpoint, recs []evidence.Record) {
	e.Classes = map[score.Class]int{}
	e.Correct, e.ByCutoff = 0, 0
	for _, l := range pilot.Labels {
		e.PerLabel[l] = 0
	}
	for i := range recs {
		s := recs[i].Score
		if s == nil {
			continue
		}
		e.Classes[s.Class]++
		if s.Class == score.Success || s.Class == score.LateCorrect {
			e.Correct++
			e.PerLabel[s.Expected]++
		}
		if s.Class == score.Success {
			e.ByCutoff++
		}
	}
	e.Pass = e.Correct >= devMinCorrect
	for _, l := range pilot.Labels {
		if e.PerLabel[l] < devMinPerLabel {
			e.Pass = false
		}
	}
}
