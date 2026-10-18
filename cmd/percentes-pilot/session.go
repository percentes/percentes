package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/pilot"
)

// Session timing (SPEC-HOSTED §4 start tolerance; HA2 §4): a session
// starts at its slot or up to startTolerance after it, and its second
// window starts within maxGap of the first draining.
const (
	startTolerance = 15 * time.Minute
	maxGap         = 5 * time.Minute
)

// sessionFile is the session record's name in the session's directory.
const sessionFile = "session.json"

// operatorRecords are the operator record kinds a session window
// requires beyond evidence's required kinds: the HA2 §6 and §9 hashes.
var operatorRecords = []string{evidence.RecordCases, evidence.RecordLabels, evidence.RecordRubric, evidence.RecordScorer, evidence.RecordPrompt, evidence.RecordSchedule}

// sessionRecord is one session as run.
type sessionRecord struct {
	SchemaVersion  int       `json:"schema_version"`
	Label          string    `json:"label"`
	DayType        string    `json:"day_type"`
	Date           string    `json:"date"`
	SlotUTC        time.Time `json:"slot_utc"`
	Replaces       string    `json:"replaces,omitempty"`
	Order          []string  `json:"order"`
	Permutation    []int     `json:"permutation"`
	ScheduleSHA256 string    `json:"schedule_sha256"`
	ConfigSHA256   string    `json:"config_sha256"`
	StartUTC       time.Time `json:"start_utc"`
	EndUTC         time.Time `json:"end_utc,omitempty"`
	// GapS is the time from the first window draining to the second
	// window's epoch, present when both ran; GapExceeded marks a gap over
	// five minutes.
	GapS        *float64       `json:"gap_s,omitempty"`
	GapExceeded bool           `json:"gap_exceeded,omitempty"`
	Windows     []windowRecord `json:"windows"`
	// Error is an execution error that ended the session.
	Error string `json:"error,omitempty"`
}

// sessionInput is a session command after its flags are read.
type sessionInput struct {
	cfg        *config.Config
	configName string
	schedule   *pilot.Schedule
	scheduleSH string
	label      string
	replaces   string
	out        string
	local      bool
	operators  map[string]*evidence.Operator
	deps       *windowDeps
	now        func() time.Time
}

// sessionCmd runs one session: each endpoint's window in the frozen
// order, the second starting as soon as the first drains. A replacement
// runs on its reserve date in the replaced session's order and replaces
// both windows; the replaced session's directory stays.
func sessionCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("session", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "pilot config (required)")
	var envFiles, opFlags listFlag
	fs.Var(&envFiles, "env", "env file holding a target's api_key_env (repeatable, required)")
	ledgerPath := fs.String("ledger", "", "admission ledger file (required)")
	out := fs.String("out", "", "pilot output directory; the session writes <label>/ under it (required)")
	schedulePath := fs.String("schedule", "", "schedule file: the JSON of the frozen schedule, or of its input alone (required)")
	label := fs.String("session", "", "session or reserve label to run (required)")
	replaces := fs.String("replaces", "", "the session a reserve replaces (required with a reserve label)")
	framesRoot := fs.String("frames", "", "raw-frame store directory outside the source tree (required)")
	clockSync := fs.String("clock-sync", "", "host clock-sync command run before each window, split on spaces with no shell (required)")
	statusCmd := fs.String("status-cmd", "", "endpoint-status command run at each window's start and end, split on spaces with no shell (required)")
	fs.Var(&opFlags, "operator", "NAME=FILE: the operator file of the target NAME (one per target, required)")
	local := fs.Bool("local", false, "run at once, outside the slot; refused unless every endpoint is a loopback address")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for name, v := range map[string]string{"--ledger": *ledgerPath, "--out": *out, "--schedule": *schedulePath, "--session": *label, "--frames": *framesRoot} {
		if v == "" {
			return exitError, fmt.Errorf("session: %s is required", name)
		}
	}
	cfg, err := loadPilot(*cfgPath)
	if err != nil {
		return exitError, err
	}
	targets, err := selectTargets(cfg, nil)
	if err != nil {
		return exitError, err
	}
	sched, sum, err := readSchedule(*schedulePath, cfg)
	if err != nil {
		return exitError, err
	}
	build, _ := debug.ReadBuildInfo()
	ops, err := loadOperators(opFlags, targets, build)
	if err != nil {
		return exitError, err
	}
	if err := loadKeys(envFiles, targets); err != nil {
		return exitError, err
	}
	deps, err := newWindowDeps(*ledgerPath, *framesRoot, *clockSync, *statusCmd, filepath.Base(*cfgPath), build)
	if err != nil {
		return exitError, err
	}
	return runSession(ctx, &sessionInput{
		cfg: cfg, configName: filepath.Base(*cfgPath), schedule: sched, scheduleSH: sum,
		label: *label, replaces: *replaces, out: *out, local: *local,
		operators: ops, deps: deps, now: time.Now,
	})
}

// newWindowDeps checks the session's evidence settings and opens the
// ledger.
func newWindowDeps(ledgerPath, framesRoot, clockSync, statusCmd, configName string, build *debug.BuildInfo) (*windowDeps, error) {
	if err := evidence.CheckFramesRoot(framesRoot); err != nil {
		return nil, err
	}
	clock, err := evidence.ParseClockCommand(clockSync)
	if err != nil {
		return nil, err
	}
	status, err := evidence.ParseStatusCommand(statusCmd)
	if err != nil {
		return nil, err
	}
	ledger, err := admission.Open(ledgerPath)
	if err != nil {
		return nil, err
	}
	scorer, err := newScorer()
	if err != nil {
		return nil, err
	}
	return &windowDeps{ledger: ledger, ledgerPath: ledgerPath, framesRoot: framesRoot, clock: clock, status: status, build: build, configName: configName, scorer: scorer}, nil
}

// readSchedule reads a schedule file and derives the schedule again from
// its input. A file that also lists sessions or reserves must match the
// derivation; its endpoints must be the config's targets in order.
func readSchedule(path string, cfg *config.Config) (*pilot.Schedule, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("schedule: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var f pilot.Schedule
	if err := dec.Decode(&f); err != nil {
		return nil, "", fmt.Errorf("schedule %s: %w", path, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, "", fmt.Errorf("schedule %s: data after the JSON object", path)
	}
	if want := [2]string{cfg.Pilot.Targets[0].Name, cfg.Pilot.Targets[1].Name}; f.Input.Endpoints != want {
		return nil, "", fmt.Errorf("schedule %s: endpoints %q, the config's targets are %q", path, f.Input.Endpoints, want)
	}
	s, err := pilot.NewSchedule(f.Input)
	if err != nil {
		return nil, "", err
	}
	if f.Sessions != nil || f.Reserves != nil || f.Counter != 0 {
		got, _ := json.Marshal(f)
		want, _ := json.Marshal(s)
		if !bytes.Equal(got, want) {
			return nil, "", fmt.Errorf("schedule %s: its sessions or reserves differ from the schedule its input derives", path)
		}
	}
	sum := sha256.Sum256(raw)
	return s, hex.EncodeToString(sum[:]), nil
}

// loadOperators reads one operator file per target from NAME=FILE flags,
// requires the pilot's record kinds in each, and checks the build's
// source against it.
func loadOperators(flags []string, targets []*config.Target, build *debug.BuildInfo) (map[string]*evidence.Operator, error) {
	ops := map[string]*evidence.Operator{}
	for _, f := range flags {
		name, path, ok := strings.Cut(f, "=")
		if !ok || name == "" || path == "" {
			return nil, fmt.Errorf("--operator %q: want NAME=FILE", f)
		}
		if _, dup := ops[name]; dup {
			return nil, fmt.Errorf("--operator: %s named twice", name)
		}
		op, err := evidence.LoadOperator(path)
		if err != nil {
			return nil, err
		}
		for _, kind := range operatorRecords {
			if !hasRecord(op, kind) {
				return nil, fmt.Errorf("operator file %s: no %s record", path, kind)
			}
		}
		if err := evidence.CheckSource(build, op); err != nil {
			return nil, err
		}
		ops[name] = op
	}
	for _, t := range targets {
		if ops[t.Name] == nil {
			return nil, fmt.Errorf("--operator: no operator file for target %s", t.Name)
		}
	}
	if len(ops) != len(targets) {
		return nil, errors.New("--operator: a name that is not a pilot target")
	}
	return ops, nil
}

func hasRecord(op *evidence.Operator, kind string) bool {
	for i := range op.Records {
		if op.Records[i].Kind == kind {
			return true
		}
	}
	return false
}

// pick returns the session or reserve labelled label, and whether it is a
// reserve.
func pick(s *pilot.Schedule, label string) (*pilot.Session, bool, error) {
	for i := range s.Sessions {
		if s.Sessions[i].Label == label {
			return &s.Sessions[i], false, nil
		}
	}
	for i := range s.Reserves {
		if s.Reserves[i].Label == label {
			return &s.Reserves[i], true, nil
		}
	}
	return nil, false, fmt.Errorf("session %q is not in the schedule", label)
}

// replacement checks that reserve may replace the session labelled
// replaces under out, and returns that session. The replaced session is
// of the reserve's day type, and either never ran or kept a window that
// is client-invalid or did not start. A pilot has at most one
// replacement.
func replacement(s *pilot.Schedule, reserve *pilot.Session, replaces, out string) (*pilot.Session, error) {
	if replaces == "" {
		return nil, fmt.Errorf("session %s is a reserve: --replaces names the session it replaces", reserve.Label)
	}
	old, isReserve, err := pick(s, replaces)
	if err != nil {
		return nil, err
	}
	if isReserve || old.DayType != reserve.DayType {
		return nil, fmt.Errorf("reserve %s replaces only a %s session; %s is not one", reserve.Label, reserve.DayType, replaces)
	}
	for _, r := range s.Reserves {
		if _, err := os.Stat(filepath.Join(out, r.Label)); err == nil {
			return nil, fmt.Errorf("reserve %s has run under %s; a pilot has at most one replacement", r.Label, out)
		}
	}
	raw, err := os.ReadFile(filepath.Join(out, replaces, sessionFile))
	if errors.Is(err, os.ErrNotExist) {
		return old, nil
	}
	if err != nil {
		return nil, err
	}
	var prev sessionRecord
	if err := json.Unmarshal(raw, &prev); err != nil {
		return nil, fmt.Errorf("%s: %w", replaces, err)
	}
	valid := 0
	for _, w := range prev.Windows {
		if w.Blocked == "" && w.ClientValid {
			valid++
		}
	}
	if valid == len(old.Order) {
		return nil, fmt.Errorf("session %s has two client-valid windows; an adverse result is never re-run", replaces)
	}
	return old, nil
}

// loopbackOnly reports whether every target's endpoint is a loopback
// address.
func loopbackOnly(cfg *config.Config) bool {
	for _, t := range cfg.Pilot.Targets {
		u, err := url.Parse(t.EndpointURL)
		if err != nil {
			return false
		}
		host := u.Hostname()
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return false
		}
	}
	return true
}

// sessionCases are a session window's user texts and frozen labels by
// request index: each warm-up request carries the permutation's first
// measured case (SPEC-HOSTED HA2 §3), and the measured requests carry the
// measured cases in permutation order. Warm-up indexes carry no label.
func sessionCases(cfg *config.Config, fx *pilot.Fixture, perm []int) (cases, expected []string, err error) {
	warmupEndNs := int64(cfg.Run.Phases.WarmupS * 1e9)
	schedule := loadgen.BuildSchedule(cfg)
	cases, expected = make([]string, len(schedule)), make([]string, len(schedule))
	m := 0
	for i, off := range schedule {
		if off < warmupEndNs {
			cases[i] = fx.Measured[perm[0]].Text
			continue
		}
		if m >= len(perm) {
			return nil, nil, fmt.Errorf("%d measured requests scheduled for %d measured cases", m+1, len(perm))
		}
		c := fx.Measured[perm[m]]
		cases[i], expected[i] = c.Text, c.Label
		m++
	}
	return cases, expected, nil
}

// runSession runs in's session and writes its record.
func runSession(ctx context.Context, in *sessionInput) (int, error) {
	cfg := in.cfg
	sess, isReserve, err := pick(in.schedule, in.label)
	if err != nil {
		return exitError, err
	}
	order, purpose := sess.Order, admission.PurposeWindow
	switch {
	case isReserve:
		old, err := replacement(in.schedule, sess, in.replaces, in.out)
		if err != nil {
			return exitError, err
		}
		order, purpose = old.Order, admission.PurposeReplacement
	case in.replaces != "":
		return exitError, fmt.Errorf("session %s is not a reserve; --replaces applies to reserves", sess.Label)
	}
	dir := filepath.Join(in.out, sess.Label)
	if _, err := os.Stat(dir); err == nil {
		return exitError, fmt.Errorf("session %s has run under %s; a session is never re-run", sess.Label, in.out)
	}
	if in.local {
		if !loopbackOnly(cfg) {
			return exitError, errors.New("session: --local needs every endpoint on a loopback address")
		}
	} else if late := in.now().Sub(sess.Start); late > startTolerance {
		return exitError, fmt.Errorf("session %s: %s after its slot at %s, beyond the %s start tolerance", sess.Label, late.Round(time.Second), sess.Start.Format(time.RFC3339), startTolerance)
	}

	fx, err := fixture()
	if err != nil {
		return exitError, err
	}
	targets := map[string]*config.Target{}
	for i := range cfg.Pilot.Targets {
		targets[cfg.Pilot.Targets[i].Name] = &cfg.Pilot.Targets[i]
	}
	windows := make([]window, len(order))
	for i, name := range order {
		wc := windowConfig(cfg, targets[name])
		cases, expected, err := sessionCases(wc, fx, sess.Permutation)
		if err != nil {
			return exitError, err
		}
		windows[i] = window{cfg: wc, dir: filepath.Join(dir, name), purpose: purpose, op: in.operators[name], cases: cases, expected: expected}
	}

	if !in.local {
		if err := waitUntil(ctx, sess.Start); err != nil {
			return exitError, err
		}
	}
	sum := sha256.Sum256(cfg.Raw)
	rec := &sessionRecord{
		SchemaVersion:  1,
		Label:          sess.Label,
		DayType:        sess.DayType,
		Date:           sess.Date,
		SlotUTC:        sess.Start,
		Order:          order,
		Permutation:    sess.Permutation,
		ScheduleSHA256: in.scheduleSH,
		ConfigSHA256:   hex.EncodeToString(sum[:]),
		StartUTC:       in.now().UTC(),
		Windows:        []windowRecord{},
	}
	if isReserve {
		rec.Replaces = in.replaces
	}
	if err := os.MkdirAll(in.out, 0o755); err != nil {
		return exitError, err
	}
	if err := writeJSON(filepath.Join(dir, sessionFile), rec); err != nil {
		return exitError, err
	}

	var runErr error
	for i, w := range windows {
		if i > 0 && ctx.Err() != nil {
			rec.Windows = append(rec.Windows, windowRecord{Endpoint: w.cfg.Target.Name, Dir: filepath.Base(w.dir), Purpose: w.purpose, Blocked: "not started: " + context.Cause(ctx).Error()})
			continue
		}
		wr, err := in.deps.run(ctx, w)
		rec.Windows = append(rec.Windows, *wr)
		if err != nil {
			runErr = err
			rec.Error = err.Error()
			break
		}
	}
	if len(rec.Windows) == 2 {
		a, b := rec.Windows[0], rec.Windows[1]
		if a.DrainedUTC != nil && b.EpochUTC != nil {
			gap := b.EpochUTC.Sub(*a.DrainedUTC).Seconds()
			rec.GapS, rec.GapExceeded = &gap, b.EpochUTC.Sub(*a.DrainedUTC) > maxGap
		}
	}
	rec.EndUTC = in.now().UTC()
	if err := replaceJSON(filepath.Join(dir, sessionFile), rec); err != nil {
		return exitError, errors.Join(runErr, err)
	}
	if runErr != nil {
		return exitError, runErr
	}
	code := exitOK
	for _, w := range rec.Windows {
		fmt.Printf("%s %s: %s\n", rec.Label, w.Endpoint, windowSummary(w))
		if w.Blocked != "" || !w.ClientValid {
			code = exitAdverse
		}
	}
	if len(rec.Windows) != len(order) || rec.GapExceeded {
		code = exitAdverse
	}
	if rec.GapExceeded {
		fmt.Printf("%s: the second window started %.0f s after the first drained, beyond %s\n", rec.Label, *rec.GapS, maxGap)
	}
	return code, nil
}

func windowSummary(w windowRecord) string {
	switch {
	case w.Blocked != "":
		return "did not start: " + w.Blocked
	case !w.ClientValid:
		return fmt.Sprintf("client-invalid: %s", strings.Join(w.InvalidReasons, "; "))
	}
	return fmt.Sprintf("client-valid; %d warm-up and %d measured requests sent", w.Sent.Warmup, w.Sent.Measured)
}
