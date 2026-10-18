package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// cellConfig is the route's configuration for one cell attempt: the
// attempt's seed, and the ladder cut at the top admitted step. The cut
// ladder's schedule is a prefix of the whole ladder's.
func cellConfig(cfg *config.Config, seed int64, steps int) *config.Config {
	c := *cfg
	c.Run.Seed = seed
	rel := *cfg.Release
	rel.Ladder.StepsRPS = slices.Clone(cfg.Release.Ladder.StepsRPS[:steps])
	c.Release = &rel
	return &c
}

// runCell runs one cell attempt (SPEC-HOSTED HA3 point 16) and returns its
// record, also written to the cell's directory. A clock offset above the
// pinned bound or a ledger refusal stops the route before any request; a
// probe status that blocks the start ends the cell before the ladder; a
// documented billing refusal on the probe or inside the ladder stops the
// route; an EMFILE error ends the ladder and the cell is client-invalid.
// Every finished cell keeps its evidence with a labelled manifest and a
// report pair (retain). An error is an execution error.
func (r *route) runCell(ctx context.Context, run schedule.Run) (rec *cellRecord, err error) {
	dir := r.cellDir(run)
	rec = r.newRecord(run)
	rec.Outcome = outcomeRunning
	if err := r.writeNew(run, rec); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, cellFile)
	finish := func(outcome string, reasons ...string) {
		rec.Outcome = outcome
		rec.Reasons = append(rec.Reasons, reasons...)
	}
	defer func() {
		if err != nil {
			rec.Outcome, rec.Error = outcomeInterrupted, err.Error()
		}
		end := r.now().UTC()
		rec.EndUTC = &end
		if werr := replaceJSON(path, rec); werr != nil {
			err = errors.Join(err, werr)
		}
	}()

	cfg := cellConfig(r.cfg, run.Seed, r.steps)
	t := &cfg.Target
	ev := &cellFiles{dir: dir, run: run, cfg: cfg}
	if ev.live, err = startLiveSeries(filepath.Join(dir, dispatchFile), run); err != nil {
		return rec, err
	}
	defer ev.live.stop()
	ev.clock = evidence.ReadClock(ctx, r.clock)
	if ev.clockPath, err = evidence.WriteClock(dir, ev.clock); err != nil {
		return rec, err
	}
	if ev.statusStart, err = evidence.WriteStatus(dir, evidence.StatusStartFile, evidence.ReadStatus(ctx, r.status)); err != nil {
		return rec, err
	}
	if ev.balanceStart, err = evidence.WriteStatus(dir, evidence.BalanceStartFile, evidence.ReadBalance(ctx, r.balance)); err != nil {
		return rec, err
	}
	if c := ev.clock; c.AbsOffsetS != nil && *c.AbsOffsetS > config.PinnedClockOffsetMaxS {
		finish(outcomeStopped, (&routeStop{c.Flag}).Error())
		return rec, r.retain(ctx, ev, rec, nil)
	}

	offsets := loadgen.BuildSchedule(cfg)
	cases := make([]string, len(offsets))
	for i := range cases {
		cases[i] = r.shape.UserText(run.Seed, i)
	}
	a, err := r.ledger.Admit(t.Name, admission.PurposeCell, 1+len(offsets))
	var refused *admission.Refusal
	if errors.As(err, &refused) {
		finish(outcomeStopped, (&routeStop{refused.Error()}).Error())
		return rec, r.retain(ctx, ev, rec, nil)
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
		if serr := r.ledger.Settle(a, rec.Sent); serr != nil {
			err = errors.Join(err, serr)
		}
	}()

	if ev.ledger, err = evidence.ReadLedger(r.ledgerPath); err != nil {
		return rec, err
	}
	if pr, err = probe.Run(ctx, probe.Input{Target: t, CaseText: r.shape.UserText(run.Seed, len(offsets)), Timeout: time.Duration(cfg.Client.HTTPTimeoutS) * time.Second}); err != nil {
		return rec, err
	}
	probePath, err := probe.Write(dir, pr)
	if err != nil {
		return rec, err
	}
	ev.probePath = probePath
	rec.ProbeStatus = pr.Request.Status
	var probeBody []byte
	if b := pr.Request.Body; b != nil && b.Text != nil {
		probeBody = []byte(*b.Text)
	}
	if stop := billingRefusal(t, pr.Request.Status, probeBody); stop != nil {
		finish(outcomeStopped, stop.Error())
		return rec, r.retain(ctx, ev, rec, nil)
	}
	if pr.Start.Blocked {
		finish(outcomeBlocked, pr.Start.Reason)
		return rec, r.retain(ctx, ev, rec, nil)
	}
	interval := time.Duration(float64(time.Second) / cfg.Release.Ladder.StepsRPS[0])
	if werr := waitUntil(ctx, pr.Request.DispatchUTC.Add(interval)); werr != nil {
		finish(outcomeStopped, "stopped before the first dispatch: "+werr.Error())
		return rec, r.retain(ctx, ev, rec, nil)
	}

	cellCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	var (
		canary    *loadgen.Canary
		canaryErr error
	)
	res, runErr := loadgen.Run(cellCtx, cfg, &loadgen.Hooks{
		Cases: cases,
		OnEpoch: func(e time.Time) {
			ev.live.setEpoch(e)
			canary, canaryErr = loadgen.StartCanary(cellCtx, e)
		},
		OnDispatch: ev.live.dispatched,
		OnDone: func(q *loadgen.Request) {
			if s := billingRefusal(t, q.Status, q.ErrorBody); s != nil {
				stop(s)
			} else if isEMFILE(q) {
				stop(errEMFILE)
			}
		},
	})
	var streams []loadgen.CanaryStream
	if canary != nil {
		streams = canary.Stop()
	}
	if res == nil {
		return rec, runErr
	}
	epoch := res.EpochWall.UTC()
	rec.EpochUTC = &epoch
	rec.Ladder, rec.Tripwires, rec.Interrupted = res.Ladder, res.Tripwires, res.Interrupted
	if res.Ladder != nil {
		for _, st := range res.Ladder.Steps {
			if st.Attempted {
				rec.Canary = append(rec.Canary, loadgen.SummarizeCanary(streams, st.StartNs, st.EndNs))
			}
		}
	}

	outcome, reasons := cellOutcome(res, runErr, context.Cause(cellCtx), ev.clock, canaryErr)
	finish(outcome, reasons...)
	return rec, r.retain(ctx, ev, rec, res)
}

// cellFiles are what a cell has recorded before its evidence is
// retained: the running dispatch series, the clock record, the start
// status and balance, the ledger position and the probe record where the
// cell reached them. A missed or interrupted cell that labelCell labels
// has no running series: own is its series as last written, lastUTC the
// latest time its records show it running, and dispatched whether it
// sent a ladder request.
type cellFiles struct {
	dir                    string
	run                    schedule.Run
	cfg                    *config.Config
	live                   *liveSeries
	clock                  *evidence.ClockRecord
	clockPath, statusStart string
	balanceStart           string
	ledger                 *evidence.LedgerRef
	probePath              string

	own        *dispatchSeries
	lastUTC    time.Time
	dispatched bool
}

// retain writes a finished cell's evidence: the final dispatch series,
// the end status and balance, the frame store and operator copies, the
// attempts file, the manifest carrying the cell's label, the host's
// aggregate dispatch series and the inter-chunk gap histograms, and the
// report pair. A nil res is a cell with no ladder result: its attempts
// file and frame store are empty, and its epoch is the cell's start. A
// missed cell has no end records and no concurrent runs. A frame store
// that fails to write leaves the rest to be written; the failure is
// returned after them.
func (r *route) retain(ctx context.Context, ev *cellFiles, rec *cellRecord, res *loadgen.Result) error {
	ladder := res != nil
	if !ladder {
		res = &loadgen.Result{EpochWall: rec.StartUTC}
	}
	dispatchPath := filepath.Join(ev.dir, dispatchFile)
	own, from := ev.own, rec.StartUTC
	if ev.live != nil {
		ev.live.stop()
		own, from = newDispatchSeries(ev.run, res), ev.live.start
		if err := replaceJSON(dispatchPath, own); err != nil {
			return err
		}
	}
	var err error
	if ev.ledger == nil {
		if ev.ledger, err = evidence.ReadLedger(r.ledgerPath); err != nil {
			return err
		}
	}
	var statusEnd, balanceEnd string
	if rec.Outcome != outcomeMissed {
		var serr, berr error
		if statusEnd, serr = evidence.WriteStatus(ev.dir, evidence.StatusEndFile, evidence.ReadStatus(ctx, r.status)); serr != nil {
			statusEnd = ""
		}
		if balanceEnd, berr = evidence.WriteStatus(ev.dir, evidence.BalanceEndFile, evidence.ReadBalance(ctx, r.balance)); berr != nil {
			balanceEnd = ""
		}
	}
	framesPath, ferr := evidence.WriteRunFrames(r.framesRoot, res)
	if ferr == nil {
		ferr = r.op.WriteCopies(filepath.Dir(framesPath))
	}
	invalid := slices.Clone(rec.Reasons)
	if rec.Outcome == outcomeCounted {
		invalid = nil
	}
	attempts, err := evidence.WriteRun(ev.dir, ev.cfg, res, evidence.Options{RunFailures: invalid})
	if err != nil {
		return errors.Join(ferr, err)
	}
	artifacts := []string{attempts}
	if ev.probePath != "" {
		artifacts = append(artifacts, ev.probePath)
	}
	artifacts = append(artifacts, dispatchPath)
	end, to := time.Now(), r.now().UTC()
	if ev.live == nil {
		end, to = ev.lastUTC, ev.lastUTC
	}
	if rec.Outcome != outcomeMissed {
		if rec.Concurrent, err = r.concurrent(rec.StartUTC, to); err != nil {
			return errors.Join(ferr, err)
		}
	}
	cell, err := r.cellEvidence(ev.dir, ev.run, rec)
	if err != nil {
		return errors.Join(ferr, err)
	}
	if cell.Dispatch, err = r.hostDispatch(from, end, own, cell.Concurrent); err != nil {
		return errors.Join(ferr, err)
	}
	cell.Outcome, cell.Reasons, cell.LadderStarted = rec.Outcome, rec.Reasons, ladder || ev.dispatched
	cell.Gaps = evidence.StepGaps(res)
	m, err := evidence.NewManifest(evidence.ManifestInput{
		Config:           ev.cfg,
		ConfigName:       r.configName,
		Target:           &ev.cfg.Target,
		Result:           res,
		Build:            r.build,
		Operator:         r.op,
		Ledger:           ev.ledger,
		Clock:            ev.clock,
		ClockPath:        ev.clockPath,
		StatusStartPath:  ev.statusStart,
		StatusEndPath:    statusEnd,
		ArtifactPaths:    artifacts,
		BalanceStartPath: ev.balanceStart,
		BalanceEndPath:   balanceEnd,
		FramesRoot:       r.framesRoot,
		FramesPath:       framesPath,
		RunValid:         rec.Outcome == outcomeCounted,
		InvalidReasons:   invalid,
		Release:          cell,
	})
	if err != nil {
		return errors.Join(ferr, err)
	}
	if _, _, _, err := report.WriteHostedPair(ev.dir, ev.cfg.Raw, m); err != nil {
		return errors.Join(ferr, err)
	}
	return ferr
}

// labelCell writes the labelled manifest and report pair of a missed or
// interrupted cell (SPEC-HOSTED HA3 point 16) from what its directory
// holds: the clock record, start status and balance, probe record and
// dispatch series the cell wrote, and each start record it did not write
// read now. The empty attempts file is written first, so a label that
// fails part-way is never written again and the cell keeps its record.
func (r *route) labelCell(ctx context.Context, rec *cellRecord) error {
	run := rec.Run
	dir := r.cellDir(run)
	cfg := cellConfig(r.cfg, run.Seed, r.steps)
	if _, err := evidence.WriteRun(dir, cfg, &loadgen.Result{}, evidence.Options{}); err != nil {
		return err
	}
	if rec.Error != "" {
		rec.Reasons = append(rec.Reasons, "execution error: "+rec.Error)
	}
	ev := &cellFiles{dir: dir, run: run, cfg: cfg, clockPath: filepath.Join(dir, evidence.ClockFile)}
	switch b, err := os.ReadFile(ev.clockPath); {
	case err == nil:
		ev.clock = &evidence.ClockRecord{}
		if err := json.Unmarshal(b, ev.clock); err != nil {
			return fmt.Errorf("%s: %w", ev.clockPath, err)
		}
	case errors.Is(err, os.ErrNotExist):
		ev.clock = evidence.ReadClock(ctx, r.clock)
		if ev.clockPath, err = evidence.WriteClock(dir, ev.clock); err != nil {
			return err
		}
	default:
		return err
	}
	for _, s := range []struct {
		path *string
		name string
		read func(context.Context, []string) *evidence.StatusRecord
		args []string
	}{
		{&ev.statusStart, evidence.StatusStartFile, evidence.ReadStatus, r.status},
		{&ev.balanceStart, evidence.BalanceStartFile, evidence.ReadBalance, r.balance},
	} {
		*s.path = filepath.Join(dir, s.name)
		if _, err := os.Stat(*s.path); errors.Is(err, os.ErrNotExist) {
			if *s.path, err = evidence.WriteStatus(dir, s.name, s.read(ctx, s.args)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	if p := filepath.Join(dir, probe.File); fileExists(p) {
		ev.probePath = p
	}

	path := filepath.Join(dir, dispatchFile)
	switch b, err := os.ReadFile(path); {
	case err == nil:
		ev.own = &dispatchSeries{}
		if err := json.Unmarshal(b, ev.own); err != nil {
			return fmt.Errorf("dispatch: %s: %w", path, err)
		}
		if ev.own.Run != run {
			return fmt.Errorf("dispatch: %s is the series of %s %s attempt %d", path, ev.own.Run.Route, ev.own.Run.Label, ev.own.Run.Attempt)
		}
	case errors.Is(err, os.ErrNotExist):
		ev.own = newDispatchSeries(run, &loadgen.Result{EpochWall: rec.StartUTC})
		if err := replaceJSON(path, ev.own); err != nil {
			return err
		}
	default:
		return err
	}
	ev.lastUTC = rec.StartUTC
	later := func(t time.Time) {
		if t.After(ev.lastUTC) {
			ev.lastUTC = t
		}
	}
	if rec.EndUTC != nil {
		later(*rec.EndUTC)
	}
	if ev.own.ThroughUTC != nil {
		later(*ev.own.ThroughUTC)
	}
	for k, n := range ev.own.Counts {
		if n > 0 {
			ev.dispatched = true
			later(ev.own.StartUTC.Add(time.Duration(k) * time.Second))
		}
	}
	ev.dispatched = ev.dispatched || rec.EpochUTC != nil
	if err := r.retain(ctx, ev, rec, nil); err != nil {
		return err
	}
	return replaceJSON(filepath.Join(dir, cellFile), rec)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// cellOutcome classifies a run that reached the ladder. cause is the
// cell context's cause, nil when it was never cancelled. A route stop or
// any other cancellation stops the route; an EMFILE error, a failed
// client-validity gate, a tripwire, a protocol other than HTTP/1.1, a
// flagged clock or a canary that did not start makes the cell
// client-invalid; otherwise the cell counts, with the reason the backoff
// rule ended its ladder, if it did.
func cellOutcome(res *loadgen.Result, runErr, cause error, clock *evidence.ClockRecord, canaryErr error) (string, []string) {
	var (
		stopped []string
		invalid []string
	)
	var rs *routeStop
	switch {
	case cause == nil:
		if runErr != nil {
			invalid = append(invalid, runErr.Error())
		}
	case errors.As(cause, &rs):
		stopped = append(stopped, rs.Error())
	case errors.Is(cause, errEMFILE):
		invalid = append(invalid, errEMFILE.Error())
	default:
		stopped = append(stopped, "stopped: "+cause.Error())
	}
	if !slices.Contains(invalid, errEMFILE.Error()) {
		for i := range res.Requests {
			if isEMFILE(&res.Requests[i]) {
				invalid = append(invalid, errEMFILE.Error())
				break
			}
		}
	}
	// A stopped run leaves requests unsent, which fails the whole run's
	// gate; its steps' own verdicts still stand.
	gates := []*loadgen.GateReport{&res.Gates}
	if res.Interrupted != nil && res.Ladder != nil {
		gates = nil
		for i := range res.Ladder.Steps {
			if g := res.Ladder.Steps[i].Gates; g != nil {
				gates = append(gates, g)
			}
		}
	}
	for _, g := range gates {
		if !g.Pass {
			reason := "client-validity gate failed (§2)"
			if !g.CPUMeasured {
				reason += ": host CPU unmeasured on this platform build"
			}
			invalid = append(invalid, reason)
			break
		}
	}
	if tw := res.Tripwires; tw != nil && tw.Fired {
		invalid = append(invalid, fmt.Sprintf("tripwire fired (SPEC-HOSTED HA3 point 16): %d dispatches against %d scheduled, at most %d in flight against limits %v",
			tw.Dispatches, tw.Scheduled, tw.MaxInFlight, tw.InFlightLimits))
	}
	if clock.Flag != "" {
		invalid = append(invalid, clock.Flag)
	}
	if canaryErr != nil {
		invalid = append(invalid, "loopback canary did not start: "+canaryErr.Error())
	}
	var ended []string
	if l := res.Ladder; l != nil && l.End != "" {
		ended = append(ended, fmt.Sprintf("ladder ended at step %d: %s", l.EndStep, l.End))
	}
	switch {
	case len(stopped) > 0:
		return outcomeStopped, append(append(stopped, invalid...), ended...)
	case len(invalid) > 0:
		return outcomeInvalid, append(invalid, ended...)
	}
	return outcomeCounted, ended
}

// cellEvidence is the manifest's cell record, with the ceiling check's
// probe records copied into the cell's directory.
func (r *route) cellEvidence(dir string, run schedule.Run, rec *cellRecord) (*evidence.Cell, error) {
	c := &evidence.Cell{
		Run:               run,
		OffsetMinutes:     r.offsetMinutes,
		Concurrent:        rec.Concurrent,
		Admission:         r.verdict,
		CeilingTokens:     r.cfg.Release.Shape.CeilingTokens,
		Ceiling:           r.ceiling,
		Probes:            []evidence.FileRef{},
		AccountCreated:    r.accountCreated,
		PreWindowRequests: r.preWindow,
	}
	if c.Concurrent == nil {
		c.Concurrent = []schedule.Run{}
	}
	names := make([]string, 0, len(r.ceilingProbes))
	for n := range r.ceilingProbes {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		p := filepath.Join(dir, n)
		if err := os.WriteFile(p, r.ceilingProbes[n], 0o644); err != nil {
			return nil, err
		}
		ref, err := evidence.HashFile(p, n)
		if err != nil {
			return nil, err
		}
		c.Probes = append(c.Probes, ref)
	}
	return c, nil
}

// concurrent lists the other routes' cells under the release output
// directory whose time between start and end, or now for one still
// running, overlaps [from, to).
func (r *route) concurrent(from, to time.Time) ([]schedule.Run, error) {
	paths, err := filepath.Glob(filepath.Join(r.out, "*", "*", cellFile))
	if err != nil {
		return nil, err
	}
	var out []schedule.Run
	for _, p := range paths {
		if filepath.Dir(filepath.Dir(p)) == filepath.Clean(r.dir) {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var c cellRecord
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if c.Outcome == outcomeMissed {
			continue
		}
		end := to
		if c.EndUTC != nil {
			end = *c.EndUTC
		}
		if c.StartUTC.Before(to) && end.After(from) {
			out = append(out, c.Run)
		}
	}
	slices.SortFunc(out, func(a, b schedule.Run) int { return a.Start.Compare(b.Start) })
	return out, nil
}
