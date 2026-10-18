package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/score"
	"github.com/percentes/percentes/internal/validity"
)

// measuredWindow names the run's one collected window, from warm-up end
// to run end: the phase name hosted-G6 reads, and the config's
// run.phases.baseline_s.
const measuredWindow = "baseline"

// windowDeps are what every window of a session shares.
type windowDeps struct {
	ledger     *admission.Ledger
	ledgerPath string
	framesRoot string
	clock      evidence.ClockCommand
	status     []string
	build      *debug.BuildInfo
	configName string
	scorer     *score.Scorer
}

// window is one endpoint's window of a session.
type window struct {
	// cfg is the window config: the pilot config with the endpoint as its
	// target.
	cfg     *config.Config
	dir     string
	purpose string
	op      *evidence.Operator
	// cases and expected are the user text and frozen label by request
	// index; warm-up indexes carry no label.
	cases    []string
	expected []string
}

// windowRecord is one window as the session record states it.
type windowRecord struct {
	Endpoint string `json:"endpoint"`
	Dir      string `json:"dir"`
	Purpose  string `json:"purpose"`
	// Blocked says why the window sent no scheduled request: an admission
	// refusal, a probe status that blocks the start, or a stop before the
	// first dispatch.
	Blocked     string     `json:"blocked,omitempty"`
	ProbeStatus int        `json:"probe_status,omitempty"`
	EpochUTC    *time.Time `json:"epoch_utc,omitempty"`
	DrainedUTC  *time.Time `json:"drained_utc,omitempty"`
	ClientValid bool       `json:"client_valid"`
	// InvalidReasons are the independent evidence that makes the window
	// client-invalid (SPEC-HOSTED HA2 §5): a failed G2 gate, a tripwire, a
	// safety stop, a protocol other than HTTP/1.1 or a flagged clock.
	InvalidReasons []string              `json:"invalid_reasons,omitempty"`
	Interrupted    *loadgen.Interruption `json:"interrupted,omitempty"`
	ReportedModels []string              `json:"reported_models,omitempty"`
	// AuthStatuses counts 401 and 403 responses inside the window: each
	// is a status_other result, and the window stays in the record.
	AuthStatuses int              `json:"auth_statuses,omitempty"`
	Sent         admission.Counts `json:"sent"`
}

// run admits the window, records the endpoint status and clock offset,
// probes the endpoint and, unless the probe blocks the start, runs the
// schedule one session interval after the probe request, then writes the
// window's evidence and settles the admission with the requests sent. A
// window that does not start is returned with Blocked set; an error is an
// execution error.
func (d *windowDeps) run(ctx context.Context, w window) (rec *windowRecord, err error) {
	t := &w.cfg.Target
	rec = &windowRecord{Endpoint: t.Name, Dir: filepath.Base(w.dir), Purpose: w.purpose}
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return rec, err
	}
	a, err := d.ledger.Admit(t.EndpointURL, w.purpose, admission.WindowRequests, admission.PilotTokens)
	var refused *admission.Refusal
	if errors.As(err, &refused) {
		rec.Blocked = refused.Error()
		return rec, nil
	}
	if err != nil {
		return rec, err
	}
	var (
		pr  *probe.Record
		res *loadgen.Result
	)
	defer func() {
		rec.Sent = admission.WindowSent(pr, res)
		if serr := d.ledger.Settle(a, rec.Sent); serr != nil {
			err = errors.Join(err, serr)
		}
	}()

	ledgerRef, err := evidence.ReadLedger(d.ledgerPath)
	if err != nil {
		return rec, err
	}
	statusStart, err := evidence.WriteStatus(w.dir, evidence.StatusStartFile, evidence.ReadStatus(ctx, d.status))
	if err != nil {
		return rec, err
	}
	clock := evidence.ReadClock(ctx, d.clock)
	clockPath, err := evidence.WriteClock(w.dir, clock)
	if err != nil {
		return rec, err
	}
	if pr, err = probe.Run(ctx, probe.Input{Pilot: w.cfg.Pilot, Target: t, CaseText: w.cases[0]}); err != nil {
		return rec, err
	}
	probePath, err := probe.Write(w.dir, pr)
	if err != nil {
		return rec, err
	}
	rec.ProbeStatus = pr.Request.Status
	if pr.Start.Blocked {
		rec.Blocked = pr.Start.Reason
		return rec, nil
	}
	if werr := waitUntil(ctx, pr.Request.DispatchUTC.Add(interval(w.cfg))); werr != nil {
		rec.Blocked = "stopped before the first dispatch: " + werr.Error()
		return rec, nil
	}

	var (
		canary    *loadgen.Canary
		canaryErr error
	)
	res, runErr := loadgen.Run(ctx, w.cfg, &loadgen.Hooks{Cases: w.cases, OnEpoch: func(e time.Time) {
		canary, canaryErr = loadgen.StartCanary(ctx, e)
	}})
	var streams []loadgen.CanaryStream
	if canary != nil {
		streams = canary.Stop()
	}
	if res == nil {
		return rec, runErr
	}
	drained := time.Now().UTC()
	epoch := res.EpochWall.UTC()
	rec.EpochUTC, rec.DrainedUTC = &epoch, &drained
	rec.Interrupted, rec.ReportedModels = res.Interrupted, res.ReportedModels
	for i := range res.Requests {
		if s := res.Requests[i].Status; s == http.StatusUnauthorized || s == http.StatusForbidden {
			rec.AuthStatuses++
		}
	}

	statusEnd, serr := evidence.WriteStatus(w.dir, evidence.StatusEndFile, evidence.ReadStatus(ctx, d.status))
	if serr != nil {
		statusEnd = ""
	}
	// A frame store that fails to write leaves the attempts and manifest
	// to be written; the failure is returned after them.
	framesPath, ferr := evidence.WriteRunFrames(d.framesRoot, res)
	if ferr == nil {
		ferr = w.op.WriteCopies(filepath.Dir(framesPath))
	}

	reasons := invalidReasons(res, clock, runErr)
	var (
		gates  *validity.Report
		replay *run.Record
	)
	if runErr == nil {
		art, err := artifacts(w.cfg, res, streams, canaryErr)
		if err != nil {
			return rec, errors.Join(ferr, err)
		}
		g := validity.Evaluate(art, validity.Observations{})
		gates, replay = &g, art.Record()
	}
	rec.ClientValid, rec.InvalidReasons = len(reasons) == 0, reasons

	attempts, err := evidence.WriteRun(w.dir, w.cfg, res, evidence.Options{
		Scoring:     &evidence.Scoring{Scorer: d.scorer, Expected: w.expected},
		RunFailures: reasons,
	})
	if err != nil {
		return rec, errors.Join(ferr, err)
	}
	m, err := evidence.NewManifest(evidence.ManifestInput{
		Config:          w.cfg,
		ConfigName:      d.configName,
		Target:          t,
		Result:          res,
		Build:           d.build,
		Operator:        w.op,
		Ledger:          ledgerRef,
		Clock:           clock,
		ClockPath:       clockPath,
		StatusStartPath: statusStart,
		StatusEndPath:   statusEnd,
		ArtifactPaths:   []string{attempts, probePath},
		FramesRoot:      d.framesRoot,
		FramesPath:      framesPath,
		Gates:           gates,
		RunValid:        rec.ClientValid,
		InvalidReasons:  reasons,
		Replay:          replay,
	})
	if err != nil {
		return rec, errors.Join(ferr, err)
	}
	if _, err := evidence.WriteManifest(w.dir, m); err != nil {
		return rec, errors.Join(ferr, err)
	}
	return rec, ferr
}

// invalidReasons lists what makes a window client-invalid. Tripwires and
// G2 invalidate a window and never stop it.
func invalidReasons(res *loadgen.Result, clock *evidence.ClockRecord, runErr error) []string {
	var r []string
	if runErr != nil {
		r = append(r, runErr.Error())
	}
	if !res.Gates.Pass {
		reason := "client-validity gate failed (§2)"
		if !res.Gates.CPUMeasured {
			reason += ": host CPU unmeasured on this platform build"
		}
		r = append(r, reason)
	}
	if tw := res.Tripwires; tw != nil && tw.Fired {
		r = append(r, fmt.Sprintf("tripwire fired (SPEC-HOSTED HA2 §3): %d dispatches against %d scheduled, at most %d in flight against a limit of %d",
			tw.Dispatches, tw.Scheduled, tw.MaxInFlight, tw.InFlightLimit))
	}
	if clock.Flag != "" {
		r = append(r, clock.Flag)
	}
	return r
}

// artifacts collects res's measured window and attaches the canary
// streams to it.
func artifacts(cfg *config.Config, res *loadgen.Result, streams []loadgen.CanaryStream, canaryErr error) (*run.Artifacts, error) {
	st, err := collect.Collect(cfg, res.Requests, collect.Window{Name: measuredWindow, StartNs: res.WarmupEndNs, EndNs: res.BaselineEndNs})
	if err != nil {
		return nil, err
	}
	art := &run.Artifacts{Config: cfg, Loadgen: res, Windows: map[string]*collect.Stats{measuredWindow: st}, RunValid: true}
	art.AttachObservations(run.Observed{Canary: streams, CanaryErr: canaryErr})
	return art, nil
}
