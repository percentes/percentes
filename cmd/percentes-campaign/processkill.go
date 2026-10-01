package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/campaign"
	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/validity"
	"github.com/percentes/percentes/internal/vllmlog"
)

// processKillPlan is how each process-kill run reaches its container.
type processKillPlan struct {
	container    string
	probeURL     string
	outDir       string
	profile      config.Profile
	readyTimeout time.Duration
	poll         time.Duration
}

// Window before the fire that the event and log reads start from.
const captureLead = 5 * time.Second

// Upper bound on one health-and-inference readiness attempt.
const readyAttempt = 5 * time.Second

// dryKillTInject is the dry kill's fire time after arming; tests shorten it.
var dryKillTInject = 5 * time.Second

// probeTarget is what the readiness and calibration probes request, as
// the run's own probes do.
func probeTarget(c *config.Config) detect.ProbeTarget {
	t := detect.ProbeTarget{Model: c.Target.ModelName, Hosted: c.Target.Hosted}
	if c.Target.APIKeyEnv != "" {
		t.APIKey = os.Getenv(c.Target.APIKeyEnv)
	}
	return t
}

// patternsFor is the boundary line set the profile's server prints.
func patternsFor(profile config.Profile) []vllmlog.Pattern {
	if profile == config.ProfileAC {
		return vllmlog.Mock
	}
	return vllmlog.VLLM0290
}

// waitReady polls, up to readyTimeout, until the container runs, /health
// answers 200 and one streamed inference is served, and returns the
// container's state. An inspection that finds no such container ends the
// wait at once; other inspection errors are retried until the deadline.
func waitReady(ctx context.Context, ops orchestrator.ContainerOps, p processKillPlan, target detect.ProbeTarget) (orchestrator.ContainerState, error) {
	deadline := time.Now().Add(p.readyTimeout)
	attempt := readyAttempt
	if p.readyTimeout < attempt {
		attempt = p.readyTimeout
	}
	var why string
	for {
		st, err := ops.Inspect(ctx, p.container)
		switch {
		case err != nil && (ctx.Err() != nil || strings.Contains(strings.ToLower(err.Error()), "no such object")):
			return orchestrator.ContainerState{}, err
		case err != nil:
			why = err.Error()
		case !st.Running:
			why = "container not running"
		default:
			actx, cancel := context.WithTimeout(ctx, attempt)
			_, err = detect.CalibrateHealth(actx, p.probeURL, p.poll, target)
			cancel()
			if err == nil {
				return st, nil
			}
			why = err.Error()
		}
		if time.Now().After(deadline) {
			return orchestrator.ContainerState{}, fmt.Errorf("container %s not ready within %s: %s", p.container, p.readyTimeout, why)
		}
		select {
		case <-ctx.Done():
			return orchestrator.ContainerState{}, ctx.Err()
		case <-time.After(p.poll):
		}
	}
}

// processKillRunner wraps inner so every run waits for the container,
// records it before and after, arms its own kill and attaches the
// restart record, the server log, the in-flight split at the fire and the
// outage window.
func processKillRunner(ops orchestrator.ContainerOps, p processKillPlan, inner campaign.Runner) campaign.Runner {
	n := 0
	var prevAfter *orchestrator.ContainerState
	return func(ctx context.Context, c *config.Config, o run.Options) (*run.Artifacts, error) {
		n++
		if err := os.MkdirAll(p.outDir, 0o755); err != nil {
			return nil, err
		}
		if _, err := waitReady(ctx, ops, p, probeTarget(c)); err != nil {
			return nil, fmt.Errorf("process_kill: %w", err)
		}
		before, err := ops.Inspect(ctx, p.container)
		if err != nil {
			return nil, fmt.Errorf("process_kill: inspect before the run: %w", err)
		}
		reasons := pinReasons(c, p.container, before)
		if prevAfter != nil && before.RestartCount != prevAfter.RestartCount {
			reasons = append(reasons, fmt.Sprintf("container restarted %d times between runs", before.RestartCount-prevAfter.RestartCount))
		}
		prevAfter = nil
		fpBefore := filepath.Join(p.outDir, fmt.Sprintf("run-%d-fingerprint-before.txt", n))
		if err := writeFingerprint(ctx, ops, p, fpBefore); err != nil {
			reasons = append(reasons, err.Error())
		}
		off, err := ops.ClockOffset(ctx)
		if err != nil {
			return nil, fmt.Errorf("process_kill: clock offset before the run: %w", err)
		}
		inj := orchestrator.NewProcessKillInjector(ops, p.container, before, off)
		o.Injector = inj
		art, err := inner(ctx, c, o)
		if err != nil {
			return nil, err
		}

		rec := &run.ContainerRestart{Before: before, OffsetAtArm: off, Kill: inj.Kill(), FingerprintBefore: fpBefore}
		fire := fireOf(art)
		after, err := ops.Inspect(ctx, p.container)
		if err != nil {
			reasons = append(reasons, "docker inspect after the run: "+err.Error())
		} else {
			rec.After = after
			prevAfter = &after
			if d := after.RestartCount - before.RestartCount; d != 1 {
				reasons = append(reasons, fmt.Sprintf("restart count advanced by %d, expected 1", d))
			}
		}
		rec.FingerprintAfter = filepath.Join(p.outDir, fmt.Sprintf("run-%d-fingerprint-after.txt", n))
		if err := writeFingerprint(ctx, ops, p, rec.FingerprintAfter); err != nil {
			reasons = append(reasons, err.Error())
		}
		offAfter := off
		if oa, err := ops.ClockOffset(ctx); err != nil {
			reasons = append(reasons, "clock offset after the run: "+err.Error())
		} else {
			rec.OffsetAfter, offAfter = oa, oa
		}
		if rec.Kill != nil {
			rec.FireUncertaintyNs = orchestrator.FireUncertaintyOf(*rec.Kill, off, offAfter).Nanoseconds()
			rec.IndeterminateZoneNs = rec.FireUncertaintyNs + max(off.BoundNs, offAfter.BoundNs)
		}
		reasons = append(reasons, capture(ctx, ops, p, rec, fire, off, offAfter, fmt.Sprintf("run-%d-server.log", n))...)

		// Boundaries use the arm offset, as the fire did.
		if art.Decomposition != nil {
			offset := time.Duration(off.OffsetNs)
			vllmlog.Apply(art.Decomposition, rec.Boundaries, fire, offset)
			setContainerStart(art.Decomposition, rec.After, fire, offset)
		}
		if art.Loadgen != nil {
			k, det := collect.SplitAtFire(art.Loadgen.Requests, art.ActualFireNs, rec.IndeterminateZoneNs)
			art.InFlight.IndeterminateAtFire, art.InFlight.Determinate = k, &det
			if w, ok := outageWindow(art); ok {
				st, err := collect.Collect(c, art.Loadgen.Requests, w)
				if err != nil {
					reasons = append(reasons, "collect outage: "+err.Error())
				} else {
					if art.Windows == nil {
						art.Windows = map[string]*collect.Stats{}
					}
					art.Windows[w.Name] = st
				}
			}
		}
		art.Container = rec
		if len(reasons) > 0 {
			art.RunValid = false
			art.InvalidReasons = append(art.InvalidReasons, reasons...)
		}
		return art, nil
	}
}

// pinReasons compares the container and its inspected restart policy with
// the §6 container pins.
func pinReasons(c *config.Config, container string, st orchestrator.ContainerState) []string {
	pins := c.Pins.Container
	if pins == nil {
		return nil
	}
	var out []string
	if st.RestartPolicy != pins.RestartPolicy {
		out = append(out, fmt.Sprintf("restart policy %q, pinned %q (§6)", st.RestartPolicy, pins.RestartPolicy))
	}
	if container != pins.Name {
		out = append(out, fmt.Sprintf("container %q, pinned %q (§6)", container, pins.Name))
	}
	return out
}

// outageWindow is [fire, replica_ready) in run time; false when
// replica_ready was not measured.
func outageWindow(art *run.Artifacts) (collect.Window, bool) {
	if art.Decomposition == nil {
		return collect.Window{}, false
	}
	for _, s := range art.Decomposition.Segments {
		if s.Name != "replica_ready" {
			continue
		}
		if d := s.DurationS(); d != nil {
			return collect.Window{Name: "outage", StartNs: art.ActualFireNs, EndNs: art.ActualFireNs + int64(*d*1e9)}, true
		}
	}
	return collect.Window{}, false
}

// fireOf is the run's observed fire on the client clock, or the planned
// one when the injector recorded none.
func fireOf(art *run.Artifacts) time.Time {
	if o := art.Orchestration; o != nil {
		if o.ObservedFire != nil {
			return *o.ObservedFire
		}
		return o.PlannedFireAt
	}
	if art.Loadgen != nil {
		return art.Loadgen.EpochWall.Add(time.Duration(art.ActualFireNs))
	}
	return time.Time{}
}

// writeFingerprint writes the host fingerprint to path. Under the
// experiment profile a failed read is returned as an invalidating reason;
// under the ac profile its text goes in the file.
func writeFingerprint(ctx context.Context, ops orchestrator.ContainerOps, p processKillPlan, path string) error {
	fp, err := ops.Fingerprint(ctx, p.container)
	if err != nil {
		text := err.Error()
		if p.profile == config.ProfileExperiment {
			_ = os.WriteFile(path, []byte("fingerprint failed: "+text+"\n"), 0o644)
			return fmt.Errorf("fingerprint %s: %s", filepath.Base(path), text)
		}
		fp = "fingerprint failed: " + text + "\n"
	}
	return os.WriteFile(path, []byte(fp), 0o644)
}

// capture reads the runtime's events and the server log from captureLead
// before the fire, writes the log to logName and parses its boundaries
// into rec. fireOff converts the fire to host time; nowOff converts the
// end of the event read. Failed reads are returned as reasons.
func capture(ctx context.Context, ops orchestrator.ContainerOps, p processKillPlan, rec *run.ContainerRestart, fire time.Time, fireOff, nowOff orchestrator.ClockOffset, logName string) []string {
	var reasons []string
	fireHost := fire.Add(time.Duration(fireOff.OffsetNs))
	since := fireHost.Add(-captureLead)
	events, err := ops.Events(ctx, p.container, since, time.Now().Add(time.Duration(nowOff.OffsetNs)))
	if err != nil {
		reasons = append(reasons, "docker events: "+err.Error())
	}
	rec.Events = events
	rec.DieToStartS = dieToStart(events)
	logs, err := ops.Logs(ctx, p.container, since)
	if err != nil {
		reasons = append(reasons, "docker logs: "+err.Error())
	}
	rec.LogPath = filepath.Join(p.outDir, logName)
	rec.LogBytes = len(logs)
	if werr := os.WriteFile(rec.LogPath, logs, 0o644); werr != nil {
		reasons = append(reasons, "write "+logName+": "+werr.Error())
	}
	rec.Boundaries = vllmlog.Parse(logs, patternsFor(p.profile), fireHost, fireHost.UTC().Year())
	return reasons
}

// dieToStart is the first die event to the first start after it, in
// seconds.
func dieToStart(events []orchestrator.ContainerEvent) *float64 {
	for i, e := range events {
		if e.Action != "die" {
			continue
		}
		for _, s := range events[i+1:] {
			if s.Action == "start" {
				d := s.At.Sub(e.At).Seconds()
				return &d
			}
		}
		return nil
	}
	return nil
}

// setContainerStart measures container_start from the fire to the
// runtime's recorded start, converted to the client clock.
func setContainerStart(d *detect.Decomposition, after orchestrator.ContainerState, fire time.Time, offset time.Duration) {
	if after.StartedAt.IsZero() {
		return
	}
	start := after.StartedAt.Add(-offset)
	if start.Before(fire) {
		d.SetNote("container_start", "the container's recorded start precedes the fire: no restart after the kill")
		return
	}
	d.SetMeasured("container_start", fire, start)
	clearNote(d, "container_start")
}

// clearNote drops the absence note of a measured row.
func clearNote(d *detect.Decomposition, name string) {
	for i := range d.Segments {
		if d.Segments[i].Name == name && d.Segments[i].Measured {
			d.Segments[i].Note = ""
		}
	}
}

// dryKillRecord is dry-kill.json. The bracket entries are null where the
// kill path printed no parseable remote stamps; the *_after_fire_s figures
// are the calibration's, measured from the fire.
type dryKillRecord struct {
	Container                string                        `json:"container"`
	Before                   orchestrator.ContainerState   `json:"before"`
	After                    orchestrator.ContainerState   `json:"after"`
	OffsetBefore             orchestrator.ClockOffset      `json:"offset_before"`
	OffsetAfter              orchestrator.ClockOffset      `json:"offset_after"`
	SignalZero               []orchestrator.KillRecord     `json:"signal_zero"`
	SignalZeroRoundTripMs    []float64                     `json:"signal_zero_round_trip_ms"`
	SignalZeroBracketMs      []*float64                    `json:"signal_zero_bracket_ms"`
	RemoteStampsNote         string                        `json:"remote_stamps_note,omitempty"`
	Orchestration            *orchestrator.Timestamps      `json:"orchestration,omitempty"`
	Kill                     *orchestrator.KillRecord      `json:"kill,omitempty"`
	FireErrorMs              *float64                      `json:"fire_error_ms,omitempty"`
	FireUncertaintyNs        int64                         `json:"fire_uncertainty_ns"`
	RestartCountAdvanced     int                           `json:"restart_count_advanced"`
	Events                   []orchestrator.ContainerEvent `json:"events,omitempty"`
	DieToStartS              *float64                      `json:"die_to_start_s,omitempty"`
	CalibrationStartedAt     time.Time                     `json:"health_calibration_started_at"`
	CalibrationLagS          *float64                      `json:"health_calibration_lag_after_fire_s,omitempty"`
	HealthCalibration        *detect.HealthCalibration     `json:"health_calibration,omitempty"`
	HealthReadyAfterFireS    *float64                      `json:"health_ready_after_fire_s,omitempty"`
	InferenceReadyAfterFireS *float64                      `json:"inference_ready_after_fire_s,omitempty"`
	Boundaries               vllmlog.Boundaries            `json:"boundaries"`
	LogPath                  string                        `json:"log_path"`
	LogBytes                 int                           `json:"log_bytes"`
	Errors                   []string                      `json:"errors,omitempty"`
}

// dryKill waits for the container, times five signal-0 brackets on the
// fire path, kills the container once with no load, calibrates /health
// against first inference after the fire, and writes dry-kill.json and
// dry-kill-server.log. It returns the record's path once written, also on
// a failure after the first inspection.
func dryKill(ctx context.Context, ops orchestrator.ContainerOps, p processKillPlan, c *config.Config) (string, error) {
	if err := os.MkdirAll(p.outDir, 0o755); err != nil {
		return "", err
	}
	target := probeTarget(c)
	if _, err := waitReady(ctx, ops, p, target); err != nil {
		return "", fmt.Errorf("dry kill: %w", err)
	}
	before, err := ops.Inspect(ctx, p.container)
	if err != nil {
		return "", fmt.Errorf("dry kill: inspect: %w", err)
	}
	rec := dryKillRecord{Container: p.container, Before: before}
	rec.Errors = pinReasons(c, p.container, before)
	write := func() (string, error) {
		raw, err := json.MarshalIndent(rec, "", "  ")
		if err != nil {
			return "", err
		}
		path := filepath.Join(p.outDir, "dry-kill.json")
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			return "", err
		}
		if len(rec.Errors) > 0 {
			return path, errors.New("dry kill: " + strings.Join(rec.Errors, "; "))
		}
		return path, nil
	}
	off, err := ops.ClockOffset(ctx)
	if err != nil {
		rec.Errors = append(rec.Errors, "clock offset: "+err.Error())
		return write()
	}
	rec.OffsetBefore = off
	for i := 0; i < 5; i++ {
		k, err := ops.KillInit(ctx, before.Pid, before.ID, 0)
		if err != nil {
			rec.Errors = append(rec.Errors, fmt.Sprintf("signal 0 bracket %d: %s", i+1, err))
			return write()
		}
		rec.SignalZero = append(rec.SignalZero, k)
		rec.SignalZeroRoundTripMs = append(rec.SignalZeroRoundTripMs, float64(k.ReturnedAt.Sub(k.SentAt).Microseconds())/1000)
		if k.RemoteBeforeNs == 0 || k.RemoteAfterNs == 0 {
			rec.SignalZeroBracketMs = append(rec.SignalZeroBracketMs, nil)
			rec.RemoteStampsNote = "the kill path printed no parseable nanosecond stamps: no remote bracket, and the fire is the local round-trip midpoint"
			continue
		}
		ms := float64(k.RemoteAfterNs-k.RemoteBeforeNs) / 1e6
		rec.SignalZeroBracketMs = append(rec.SignalZeroBracketMs, &ms)
	}

	inj := orchestrator.NewProcessKillInjector(ops, p.container, before, off)
	ts, err := orchestrator.Execute(ctx, inj, time.Now(), dryKillTInject, orchestrator.ContainerOpTimeout.Seconds())
	rec.Orchestration, rec.Kill = ts, inj.Kill()
	if err != nil {
		rec.Errors = append(rec.Errors, err.Error())
		return write()
	}
	if ms, err := ts.FireErrorMs(); err == nil {
		rec.FireErrorMs = &ms
	}
	fire := *ts.ObservedFire

	cctx, cancel := context.WithTimeout(ctx, p.readyTimeout)
	rec.CalibrationStartedAt = time.Now()
	cal, calErr := detect.CalibrateHealth(cctx, p.probeURL, 500*time.Millisecond, target)
	cancel()
	rec.HealthCalibration = &cal
	lag := rec.CalibrationStartedAt.Sub(fire).Seconds()
	rec.CalibrationLagS = &lag
	if cal.HealthOK {
		v := cal.HealthReadyAfterS + lag
		rec.HealthReadyAfterFireS = &v
	}
	if cal.InferenceOK {
		v := cal.InferenceReadyAfterS + lag
		rec.InferenceReadyAfterFireS = &v
	}
	if calErr != nil {
		rec.Errors = append(rec.Errors, "health calibration: "+calErr.Error())
	}

	if after, err := ops.Inspect(ctx, p.container); err != nil {
		rec.Errors = append(rec.Errors, "docker inspect after the kill: "+err.Error())
	} else {
		rec.After = after
		rec.RestartCountAdvanced = after.RestartCount - before.RestartCount
		if rec.RestartCountAdvanced != 1 {
			rec.Errors = append(rec.Errors, fmt.Sprintf("restart count advanced by %d, expected 1", rec.RestartCountAdvanced))
		}
	}
	offAfter := off
	if oa, err := ops.ClockOffset(ctx); err != nil {
		rec.Errors = append(rec.Errors, "clock offset after the kill: "+err.Error())
	} else {
		rec.OffsetAfter, offAfter = oa, oa
	}
	if rec.Kill != nil {
		rec.FireUncertaintyNs = orchestrator.FireUncertaintyOf(*rec.Kill, off, offAfter).Nanoseconds()
	}
	var restart run.ContainerRestart
	rec.Errors = append(rec.Errors, capture(ctx, ops, p, &restart, fire, off, offAfter, "dry-kill-server.log")...)
	rec.Events, rec.DieToStartS, rec.Boundaries, rec.LogPath, rec.LogBytes = restart.Events, restart.DieToStartS, restart.Boundaries, restart.LogPath, restart.LogBytes
	return write()
}

// writeRun writes run-N.json and run-N.txt for one run.
func writeRun(outDir string, n int, art *run.Artifacts, gate *validity.Report) error {
	raw, text, err := report.Generate(art, gate)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, fmt.Sprintf("run-%d.json", n)), raw, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, fmt.Sprintf("run-%d.txt", n)), []byte(text), 0o644)
}
