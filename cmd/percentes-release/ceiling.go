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
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/probe"
)

// ceilingCheckCmd sends the route's ceiling check (SPEC-HOSTED HA3 point
// 7) at its configured ceiling: probe.CeilingRequests requests of Shape S
// text, admitted in the release ledger before the first is sent and
// settled with the requests sent. It writes the record run reads with
// --ceiling-probe, also when the check fails or is blocked, and what the
// record withholds under --frames. A record reporting a model id on which
// run refuses the route (probe.ModelFault) is refused here too.
func ceilingCheckCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("ceiling-check", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "the route's release configuration at the ceiling checked (required)")
	envFile := fs.String("env", "", "env file holding the target's api_key_env (required)")
	ledgerPath := fs.String("ledger", "", "the release admission ledger, holding the route's price (required)")
	out := fs.String("out", "", "output directory; the check writes ceiling-<UTC time>/<route>-<ceiling>/"+probe.File+" under it (required)")
	byteCap := fs.Int("byte-cap", 0, "Shape S ticket byte cap (required)")
	snapshots := fs.String("snapshots", "", "directory of the retained limit copies the configuration names by file name")
	framesRoot := fs.String("frames", "", "raw-frame store directory outside the source tree; the check writes ceiling-<UTC time>/<route>-<ceiling>/"+probe.RetainedFile+
		" under it (required when the configuration sets target.retained_headers or target.probe.limits_retained)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *cfgPath == "" || *ledgerPath == "" || *out == "" {
		return exitError, errors.New("ceiling-check: --config, --ledger and --out are required")
	}
	if *byteCap <= 0 {
		return exitError, fmt.Errorf("ceiling-check: --byte-cap %d: the Shape S ticket byte cap is required", *byteCap)
	}
	cfgs, err := loadReleases([]string{*cfgPath})
	if err != nil {
		return exitError, err
	}
	cfg := cfgs[0]
	name, ceiling := cfg.Target.Name, cfg.Release.Shape.CeilingTokens
	if *framesRoot == "" && probe.Retains(&cfg.Target) {
		return exitError, errors.New("ceiling-check: --frames is required: the configuration withholds probe values")
	}
	if *framesRoot != "" {
		if err := evidence.CheckFramesRoot(*framesRoot); err != nil {
			return exitError, err
		}
	}
	shape, err := pilot.GenerateShapeS(pilot.ShapeSSeed, *byteCap)
	if err != nil {
		return exitError, err
	}
	ledger, err := admission.Open(*ledgerPath)
	if err != nil {
		return exitError, err
	}
	st, err := ledger.State()
	if err != nil {
		return exitError, err
	}
	if err := st.CheckRelease(cfg); err != nil {
		return exitError, err
	}
	if err := loadKey(*envFile, &cfg.Target); err != nil {
		return exitError, err
	}

	rec, err := sendCeiling(ctx, ledger, cfg, shape, *snapshots, *byteCap)
	if err != nil {
		return exitError, err
	}
	rel := filepath.Join("ceiling-"+time.Now().UTC().Format("20060102T150405Z"), fmt.Sprintf("%s-%d", name, ceiling))
	path, err := probe.Write(filepath.Join(*out, rel), rec)
	if err != nil {
		return exitError, err
	}
	if *framesRoot != "" {
		held, err := probe.WriteRetained(filepath.Join(*framesRoot, rel), rec)
		if err != nil {
			return exitError, err
		}
		if held != "" {
			fmt.Printf("%s: withheld values written %s\n", name, held)
		}
	}

	c, ib := rec.Ceiling, rec.InputBound
	fmt.Printf("%s: ceiling %d: %d of %d sent passed; input bound %d tokens: %d reported, largest %d, %d above; %d findings; written %s\n",
		name, c.Ceiling, c.Passed, c.Sent, ib.BoundTokens, ib.Reported, ib.MaxPromptTokens, ib.Exceeded, len(rec.Findings), path)
	refusal := modelRefusal(&cfg.Target, rec)
	if refusal != "" {
		fmt.Printf("%s: REFUSED: run refuses the route (SPEC-HOSTED §2): %s\n", name, refusal)
	}
	switch {
	case rec.Start.Blocked:
		fmt.Printf("%s: BLOCKED: %s\n", name, rec.Start.Reason)
	case !c.Pass:
		fmt.Printf("%s: FAILED the ceiling check at %d\n", name, c.Ceiling)
	case !ib.Pass:
		fmt.Printf("%s: FAILED the input bound of %d tokens\n", name, ib.BoundTokens)
	case refusal == "":
		return exitOK, nil
	}
	return exitAdverse, nil
}

// sendCeiling admits cfg's ceiling check in ledger, sends it with
// shape's texts cut at byteCap, and settles the requests sent. The
// ceiling is the texts' request-reference seed, so the checks at the two
// ceilings send different texts.
func sendCeiling(ctx context.Context, ledger *admission.Ledger, cfg *config.Config, shape *pilot.ShapeS, snapshots string, byteCap int) (*probe.ReleaseRecord, error) {
	texts := make([]string, probe.CeilingRequests)
	for i := range texts {
		texts[i] = shape.UserText(int64(cfg.Release.Shape.CeilingTokens), i)
	}
	a, err := ledger.Admit(cfg.Target.Name, admission.PurposeProbe, probe.CeilingRequests, admission.ReleaseProbeTokens(cfg))
	if err != nil {
		return nil, err
	}
	rec, err := probe.RunRelease(ctx, probe.ReleaseInput{Config: cfg, Texts: texts, SnapshotDir: snapshots, ByteCap: byteCap})
	sent := 0
	if rec != nil {
		sent = rec.Sent()
	}
	if serr := ledger.Settle(a, admission.Counts{Probe: sent}); serr != nil {
		return nil, errors.Join(err, serr)
	}
	return rec, err
}

// modelRefusal is the fault in the model ids rec reports on which run
// refuses t's route (probe.ModelFault), or "". A record reporting no id
// is left to run, which reads it beside the route's other records.
func modelRefusal(t *config.Target, rec *probe.ReleaseRecord) string {
	if len(rec.ReportedModels()) == 0 {
		return ""
	}
	return probe.ModelFault(t, []*probe.ReleaseRecord{rec})
}
