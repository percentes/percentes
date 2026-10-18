package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
)

// probeCmd probes each selected target once, before the development gate
// and the sessions, with the first development case as the user text.
// The optional wrong-model request is recorded whatever its status and
// never blocks.
func probeCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "pilot config (required)")
	var envFiles, names listFlag
	fs.Var(&envFiles, "env", "env file holding a target's api_key_env (repeatable, required)")
	ledgerPath := fs.String("ledger", "", "admission ledger file (required)")
	out := fs.String("out", "", "output directory; each probe writes probe-<UTC time>/<target>/"+probe.File+" (required)")
	fs.Var(&names, "target", "target to probe (repeatable; default every target)")
	wrongModel := fs.String("wrong-model", "", "model id sent in a second request, the session interval after the first")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *out == "" || *ledgerPath == "" {
		return exitError, errors.New("probe: --out and --ledger are required")
	}
	cfg, err := loadPilot(*cfgPath)
	if err != nil {
		return exitError, err
	}
	targets, err := selectTargets(cfg, names)
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
	dir := filepath.Join(*out, "probe-"+time.Now().UTC().Format("20060102T150405Z"))
	code := exitOK
	for _, t := range targets {
		rec, err := probeTarget(ctx, ledger, cfg.Pilot, t, fx.Development[0].Text, *wrongModel, filepath.Join(dir, t.Name))
		if err != nil {
			return exitError, err
		}
		fmt.Printf("%s: status %d over %s; %d findings; written %s\n", t.Name, rec.Request.Status, rec.Request.Proto, len(rec.Findings), filepath.Join(dir, t.Name, probe.File))
		if rec.Start.Blocked {
			fmt.Printf("%s: BLOCKED: %s\n", t.Name, rec.Start.Reason)
			code = exitAdverse
		}
	}
	return code, nil
}

// probeTarget admits, sends and settles one probe of t and writes its
// record to dir.
func probeTarget(ctx context.Context, ledger *admission.Ledger, p *config.Pilot, t *config.Target, caseText, wrongModel, dir string) (*probe.Record, error) {
	n := 1
	if wrongModel != "" {
		n = 2
	}
	a, err := ledger.Admit(t.EndpointURL, admission.PurposeProbe, n, admission.PilotTokens)
	if err != nil {
		return nil, err
	}
	rec, err := probe.Run(ctx, probe.Input{Pilot: p, Target: t, CaseText: caseText, WrongModel: wrongModel})
	if serr := ledger.Settle(a, admission.Counts{Probe: admission.ProbeSent(rec)}); serr != nil {
		return nil, errors.Join(err, serr)
	}
	if err != nil {
		return nil, err
	}
	if _, err := probe.Write(dir, rec); err != nil {
		return nil, err
	}
	return rec, nil
}
