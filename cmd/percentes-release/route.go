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
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/redact"
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/schedule"
)

// cellFile is a cell's run record in its directory.
const cellFile = "cell.json"

// Cell outcomes. A cell counts when its ladder ran to the top admitted
// step's drain or the backoff rule ended it, with no response reporting a
// model other than the expected one; every other finished outcome takes a
// re-run within the route's bound.
const (
	outcomeRunning     = "running"
	outcomeCounted     = evidence.CellCounted
	outcomeInvalid     = evidence.CellInvalid
	outcomeMismatch    = evidence.CellModelMismatch
	outcomeBlocked     = evidence.CellBlocked
	outcomeStopped     = evidence.CellStopped
	outcomeMissed      = "missed"
	outcomeInterrupted = "interrupted"
)

// cellRecord is one cell attempt as run.
type cellRecord struct {
	SchemaVersion int          `json:"schema_version"`
	Run           schedule.Run `json:"run"`
	OffsetMinutes int          `json:"offset_minutes"`
	// ScheduleSHA256 is the SHA-256 of the schedule file the cell ran
	// under.
	ScheduleSHA256 string `json:"schedule_sha256"`
	Outcome        string `json:"outcome"`
	// Reasons say why the cell does not count, or why the ladder ended
	// early.
	Reasons []string `json:"reasons,omitempty"`
	// StartUTC is when the process began the cell; EpochUTC the ladder's
	// epoch.
	StartUTC       time.Time  `json:"start_utc"`
	EpochUTC       *time.Time `json:"epoch_utc,omitempty"`
	EndUTC         *time.Time `json:"end_utc,omitempty"`
	AdmittedRPS    []float64  `json:"admitted_rps"`
	OpenFilesLimit uint64     `json:"open_files_limit"`
	ProbeStatus    int        `json:"probe_status,omitempty"`
	// AuthStatuses counts the ladder's 401 and 403 responses by status.
	AuthStatuses map[int]int `json:"auth_statuses,omitempty"`
	// Ladder, Tripwires and Interrupted are loadgen's records of the run.
	Ladder      *loadgen.LadderRecord   `json:"ladder,omitempty"`
	Tripwires   *loadgen.Tripwires      `json:"tripwires,omitempty"`
	Interrupted *loadgen.Interruption   `json:"interrupted,omitempty"`
	Canary      []loadgen.CanarySummary `json:"canary,omitempty"`
	// Concurrent lists the other routes' cells under the same --out that
	// ran while this one did.
	Concurrent []schedule.Run   `json:"concurrent_runs,omitempty"`
	Sent       admission.Counts `json:"sent"`
	Error      string           `json:"error,omitempty"`
}

func (c *cellRecord) rerunnable() bool {
	return c.Outcome != outcomeCounted && c.Outcome != outcomeRunning
}

// route is one route's process: its configuration, schedule and the
// records every cell shares.
type route struct {
	cfg        *config.Config
	configName string
	sched      *schedule.Schedule
	// scheduleSHA256 is the SHA-256 of the schedule file's bytes.
	scheduleSHA256 string
	// runs are the route's attempt 1 runs in start order.
	runs          []schedule.Run
	offsetMinutes int
	// out is the release output directory; dir the route's under it.
	out, dir string

	verdict        *admission.Verdict
	steps          int
	ceiling        probe.CeilingChoice
	ceilingProbes  map[string][]byte
	accountCreated string
	preWindow      int
	shape          *pilot.ShapeS
	openFiles      uint64
	// rule is the ceiling rule's input behind verdict other than the
	// price, which limitsStop applies again at each cell's start.
	rule ceilingInput

	ledger     *admission.Ledger
	ledgerPath string
	// key is the release key the schedule froze the configuration under.
	key        *releasekey.Key
	framesRoot string
	clock      evidence.ClockCommand
	status     []string
	balance    []string
	refresh    []string
	// opPath is the operator file each cell reads again; op is the file
	// as read when the route started.
	opPath string
	op     *evidence.Operator
	// withheld are the confidential limits the refresh command read, in
	// this process and in the retained records of earlier cells.
	withheld []evidence.RefreshedLimit
	build    *debug.BuildInfo

	now  func() time.Time
	wait func(ctx context.Context, t time.Time) error
	// cell runs one cell attempt; runCell unless a test replaces it.
	cell func(ctx context.Context, run schedule.Run) (*cellRecord, error)
	// label labels a missed or interrupted cell; labelCell unless a test
	// replaces it.
	label func(ctx context.Context, rec *cellRecord) error
	// close writes the window-close record; windowClose unless a test
	// replaces it.
	close func(ctx context.Context, st *routeState) error
}

// runCmd reads the route's inputs, refuses to start on any defect, and
// runs its cells.
func runCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "the route's release configuration (required)")
	envFile := fs.String("env", "", "env file holding the target's api_key_env (required)")
	ledgerPath := fs.String("ledger", "", "the release admission ledger the schedule froze, holding the route's price (required)")
	keyPath := fs.String("release-key", "", "the release key file the schedule froze the configuration under (required)")
	schedulePath := fs.String("schedule", "", "the committed schedule file (required)")
	out := fs.String("out", "", "release output directory; the route writes <route>/<cell>-a<attempt>/ under it (required)")
	framesRoot := fs.String("frames", "", "raw-frame store directory outside the source tree (required)")
	clockSync := fs.String("clock-sync", "", "host clock-sync command run before each cell, split on spaces with no shell (required)")
	statusCmd := fs.String("status-cmd", "", "endpoint-status command run at each cell's start and end, split on spaces with no shell (required)")
	balanceCmd := fs.String("balance-cmd", "", "account balance or quota command run at each cell's start and end, split on spaces with no shell, printing {\"amount_usd\": \"<decimal>\"} or {\"unavailable\": \"<reason>\"} (required)")
	refreshCmd := fs.String("refresh-cmd", "", "operator-records refresh command run at each cell's start and once after the last cell, split on spaces with no shell, rewriting the files of the operator file's limits and eligibility records and printing {\"limits\": [...]}, each target.limits entry's current value or why it is unavailable (required)")
	opPath := fs.String("operator", "", "the route's operator file, read again at each cell's start (required)")
	var hostFiles, probeFiles listFlag
	fs.Var(&hostFiles, "host-config", "the release configuration of a route in release.other_hosts, as the schedule froze it: one per route (repeatable, required for each)")
	fs.Var(&probeFiles, "ceiling-probe", "a ceiling check record of a host of the route's model: every host's at 512, and at 1024 when one failed at 512 (repeatable, required)")
	byteCap := fs.Int("byte-cap", 0, "Shape S ticket byte cap (required)")
	accountCreated := fs.String("account-created", "", "the account's creation date, 2006-01-02 (required)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for name, v := range map[string]string{"--config": *cfgPath, "--ledger": *ledgerPath, "--schedule": *schedulePath, "--out": *out,
		"--frames": *framesRoot, "--operator": *opPath, "--account-created": *accountCreated} {
		if v == "" {
			return exitError, fmt.Errorf("run: %s is required", name)
		}
	}
	cfg, err := config.LoadFile(*cfgPath)
	if err != nil {
		return exitError, err
	}
	var hostCfgs []*config.Config
	for _, p := range hostFiles {
		c, err := config.LoadFile(p)
		if err != nil {
			return exitError, err
		}
		hostCfgs = append(hostCfgs, c)
	}
	key, err := readReleaseKey(*keyPath)
	if err != nil {
		return exitError, err
	}
	build, _ := debug.ReadBuildInfo()
	r, err := newRoute(routeInput{
		cfg: cfg, hostCfgs: hostCfgs, configName: filepath.Base(*cfgPath), schedulePath: *schedulePath, out: *out, ledgerPath: *ledgerPath, key: key,
		framesRoot: *framesRoot, clockSync: *clockSync, statusCmd: *statusCmd, balanceCmd: *balanceCmd, refreshCmd: *refreshCmd, opPath: *opPath, probeFiles: probeFiles,
		byteCap: *byteCap, accountCreated: *accountCreated, build: build,
	})
	if err != nil {
		return exitError, err
	}
	if err := loadKey(*envFile, &cfg.Target); err != nil {
		return exitError, err
	}
	fmt.Printf("%s: offset %d min; admitted %v rps at ceiling %d; open-file limit %d\n", cfg.Target.Name, r.offsetMinutes, r.verdict.Admitted(), r.verdict.CeilingTokens, r.openFiles)
	return r.loop(ctx)
}

// ceilingInput is the ceiling rule's input other than the price: the
// configuration as the route started, before a test or the rehearsal
// scales its ladder, and the plan at the schedule's exact counts.
type ceilingInput struct {
	cfg  *config.Config
	plan admission.Plan
}

// routeInput is a run command after its flags are read.
type routeInput struct {
	cfg                                       *config.Config
	hostCfgs                                  []*config.Config
	configName, schedulePath, out, ledgerPath string
	key                                       *releasekey.Key
	framesRoot, clockSync, statusCmd, opPath  string
	balanceCmd, refreshCmd                    string
	probeFiles                                []string
	byteCap                                   int
	accountCreated                            string
	build                                     *debug.BuildInfo
}

// newRoute checks the route's inputs and fixes what every cell shares:
// the ceiling rule's verdict at the schedule's exact counts and the
// route's newest price up to the ledger position the schedule froze, the
// ceiling choice, the Shape S set and the account record. It refuses an
// output directory holding a cell of the route written under another
// schedule file.
func newRoute(in routeInput) (*route, error) {
	cfg := in.cfg
	if cfg.Profile != config.ProfileRelease || cfg.Release == nil {
		return nil, fmt.Errorf("run: profile %q, want %q", cfg.Profile, config.ProfileRelease)
	}
	name := cfg.Target.Name
	raw, err := os.ReadFile(in.schedulePath)
	if err != nil {
		return nil, fmt.Errorf("schedule: %w", err)
	}
	sched, err := schedule.Load(raw)
	if err != nil {
		return nil, err
	}
	runs, err := sched.Runs(name)
	if err != nil {
		return nil, err
	}
	if err := checkFrozen(cfg, sched, in.key); err != nil {
		return nil, err
	}
	hosts, err := hostTargets(cfg, in.hostCfgs, sched, in.key)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	r := &route{cfg: cfg, configName: in.configName, sched: sched, scheduleSHA256: hex.EncodeToString(sum[:]), runs: runs, out: in.out, dir: filepath.Join(in.out, name),
		ledgerPath: in.ledgerPath, key: in.key, framesRoot: in.framesRoot, opPath: in.opPath, build: in.build, now: time.Now, wait: waitUntil}
	r.cell, r.label, r.close = r.runCell, r.labelCell, r.windowClose
	if err := r.sameSchedule(); err != nil {
		return nil, err
	}
	for _, o := range sched.Offsets {
		if o.Route == name {
			r.offsetMinutes = o.Minutes
		}
	}

	if r.op, err = evidence.LoadOperator(in.opPath); err != nil {
		return nil, err
	}
	if cfg.Target.Confidential() {
		read, err := r.retainedReads()
		if err != nil {
			return nil, err
		}
		r.withhold(read)
	}
	if err := r.op.CheckConfidential(&cfg.Target); err != nil {
		return nil, err
	}
	if err := evidence.CheckSource(in.build, r.op); err != nil {
		return nil, err
	}
	if r.openFiles, err = checkOpenFiles(); err != nil {
		return nil, err
	}
	if err := evidence.CheckFramesRoot(in.framesRoot); err != nil {
		return nil, err
	}
	if r.clock, err = evidence.ParseClockCommand(in.clockSync); err != nil {
		return nil, err
	}
	if r.status, err = evidence.ParseStatusCommand(in.statusCmd); err != nil {
		return nil, err
	}
	if r.balance, err = evidence.ParseBalanceCommand(in.balanceCmd); err != nil {
		return nil, err
	}
	if r.refresh, err = evidence.ParseRefreshCommand(in.refreshCmd); err != nil {
		return nil, err
	}

	if r.ledger, err = admission.Open(in.ledgerPath); err != nil {
		return nil, err
	}
	st, err := r.ledger.State()
	if err != nil {
		return nil, err
	}
	if err := checkLedger(st, sched); err != nil {
		return nil, err
	}
	r.ledger.Bind(sched.Input.LedgerSHA256)
	if err := st.CheckRelease(cfg); err != nil {
		return nil, err
	}
	entries, read, err := readLedger(in.ledgerPath, sched.Input.LedgerSHA256)
	if err != nil {
		return nil, err
	}
	frozen, err := frozenEntries(entries, read.HeadSHA256, sched.Input)
	if err != nil {
		return nil, err
	}
	price, err := routePrice(frozen, name)
	if err != nil {
		return nil, err
	}
	plan, err := exactPlan(cfg, sched)
	if err != nil {
		return nil, err
	}
	if r.verdict, err = admission.CeilingRule(cfg, price, plan); err != nil {
		return nil, err
	}
	rc, rel := *cfg, *cfg.Release
	rc.Release = &rel
	r.rule = ceilingInput{cfg: &rc, plan: plan}
	if st, err = r.ledger.State(); err != nil {
		return nil, err
	}
	if err := st.Covers(cfg, r.verdict); err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}
	if r.steps = len(r.verdict.Admitted()); r.steps == 0 {
		return nil, fmt.Errorf("run: the ceiling rule admits no step of route %s; binding limit %s", name, r.verdict.Steps[0].Binding.Dimension)
	}
	first, _ := time.Parse(schedule.DateLayout, sched.Input.First)
	r.preWindow = sentBefore(entries, name, first)

	if in.byteCap <= 0 {
		return nil, fmt.Errorf("run: --byte-cap %d: the Shape S ticket byte cap is required", in.byteCap)
	}
	if r.ceiling, r.ceilingProbes, err = readCeilingProbes(cfg, hosts, in.probeFiles, in.byteCap, sched.Input.LedgerSHA256); err != nil {
		return nil, err
	}
	if r.shape, err = pilot.GenerateShapeS(pilot.ShapeSSeed, in.byteCap); err != nil {
		return nil, err
	}
	if _, err := time.Parse(time.DateOnly, in.accountCreated); err != nil {
		return nil, fmt.Errorf("run: --account-created %q is not a date (2006-01-02)", in.accountCreated)
	}
	r.accountCreated = in.accountCreated
	return r, nil
}

// exactPlan is the ceiling rule's plan at the schedule's seeded counts:
// each step's count and the warm-up's are the largest over every cell
// attempt's ladder.
func exactPlan(cfg *config.Config, sched *schedule.Schedule) (admission.Plan, error) {
	p := admission.ExpectedPlan(cfg)
	p.WarmupRequests = 0
	p.StepRequests = make([]int, len(p.StepRequests))
	for _, c := range sched.Cells {
		for _, a := range c.Attempts {
			sc := *cfg
			sc.Run.Seed = a.Seed
			l := loadgen.BuildLadder(&sc)
			if l == nil || len(l.Steps) != len(p.StepRequests) {
				return admission.Plan{}, errors.New("run: the configuration builds no release ladder")
			}
			p.WarmupRequests = max(p.WarmupRequests, l.Steps[0].First)
			for k, st := range l.Steps {
				p.StepRequests[k] = max(p.StepRequests[k], st.End-st.First)
			}
		}
	}
	return p, nil
}

// hostTargets are the targets of hostCfgs by route: one configuration of
// each route in cfg's release.other_hosts and of no other, each frozen by
// the schedule (checkFrozen).
func hostTargets(cfg *config.Config, hostCfgs []*config.Config, sched *schedule.Schedule, key *releasekey.Key) (map[string]*config.Target, error) {
	out := map[string]*config.Target{}
	for _, h := range hostCfgs {
		name := h.Target.Name
		switch {
		case h.Profile != config.ProfileRelease || h.Release == nil:
			return nil, fmt.Errorf("run: --host-config of route %s: profile %q, want %q", name, h.Profile, config.ProfileRelease)
		case !slices.ContainsFunc(cfg.Release.OtherHosts, func(m config.ModelHost) bool { return m.Route == name }):
			return nil, fmt.Errorf("run: --host-config of route %s, which release.other_hosts does not list", name)
		case out[name] != nil:
			return nil, fmt.Errorf("run: --host-config of route %s given twice", name)
		}
		if err := checkFrozen(h, sched, key); err != nil {
			return nil, err
		}
		out[name] = &h.Target
	}
	for _, m := range cfg.Release.OtherHosts {
		if out[m.Route] == nil {
			return nil, fmt.Errorf("run: --host-config is required for route %s in release.other_hosts", m.Route)
		}
	}
	return out, nil
}

// readCeilingProbes reads the ceiling check's records of the route's
// model, whose hosts are the target and release.other_hosts, and requires
// their choice to run the model at the configured ceiling. Every record
// holds no field probe.ReleaseRecord does not define and is byte for byte
// the probe.Encode encoding of the record it decodes to. Every host's
// record is judged by the host's target, this route's own or the one in
// configs: it carries the host's model and byteCap, was admitted in the
// ledger whose open-line SHA-256 is ledger, carries the target's expected
// model id, adapter and endpoint and the hash of its body at the record's
// ceiling (config.ReleaseBodyAt), none of the values the target withholds
// (probe.Exposed), and the results its requests give at the pinned input
// bound through the target's start gate (probe.ReleaseRecord.Verify).
// This route's records together carry no reported model id that keeps the
// provider-model out (probe.ModelFault).
// Another host's records also carry none of the values their snapshots'
// confidential markings withhold (probe.ExposedByMarking), and another
// host whose records carry a model fault is left out of the choice
// (probe.ChooseCeiling).
func readCeilingProbes(cfg *config.Config, configs map[string]*config.Target, files []string, byteCap int, ledger string) (probe.CeilingChoice, map[string][]byte, error) {
	var none probe.CeilingChoice
	if len(files) == 0 {
		return none, nil, errors.New("run: --ceiling-probe is required: the ceiling check's probe records")
	}
	t := &cfg.Target
	hosts, models := []string{t.Name}, map[string]string{t.Name: t.ModelName}
	for _, h := range cfg.Release.OtherHosts {
		hosts, models[h.Route] = append(hosts, h.Route), h.Model
	}
	targets := map[string]*config.Target{}
	for route, ht := range configs {
		targets[route] = ht
	}
	targets[t.Name] = t
	bodies := map[string]map[int]string{}
	for route, ht := range targets {
		bodies[route] = map[int]string{}
		for _, c := range config.PinnedReleaseCeilings {
			b, err := config.ReleaseBodyAt(ht.Body, ht.Adapter, c)
			if err != nil {
				return none, nil, fmt.Errorf("run: route %s: target.body at %d: %w", route, c, err)
			}
			sum := sha256.Sum256([]byte(b))
			bodies[route][c] = hex.EncodeToString(sum[:])
		}
	}
	var recs, own []*probe.ReleaseRecord
	copies := map[string][]byte{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return none, nil, fmt.Errorf("ceiling probe: %w", err)
		}
		var rec probe.ReleaseRecord
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil {
			return none, nil, fmt.Errorf("ceiling probe %s: %w", f, err)
		}
		if canon, err := probe.Encode(&rec); err != nil || !bytes.Equal(b, canon) {
			return none, nil, fmt.Errorf("ceiling probe %s: not the record as ceiling-check writes it", f)
		}
		var fault string
		model, host := models[rec.Route]
		ht, body := targets[rec.Route], bodies[rec.Route][rec.Ceiling.Ceiling]
		switch {
		case !host:
		case rec.RequestedModel != model:
			fault = fmt.Sprintf("model %q, want %q", rec.RequestedModel, model)
		case rec.ByteCap != byteCap:
			fault = fmt.Sprintf("byte cap %d, want --byte-cap %d", rec.ByteCap, byteCap)
		case rec.LedgerSHA256 != ledger:
			fault = fmt.Sprintf("ledger open line sha256 %q; the schedule froze %s", rec.LedgerSHA256, ledger)
		case ht == nil:
			fault = "no configuration of the route"
		case rec.Expected() != ht.ExpectedModel():
			fault = fmt.Sprintf("expected model %q, want %q", rec.Expected(), ht.ExpectedModel())
		case rec.Adapter != ht.Adapter:
			fault = fmt.Sprintf("adapter %q, want %q", rec.Adapter, ht.Adapter)
		case rec.EndpointURL != redact.URL(ht.EndpointURL):
			fault = fmt.Sprintf("endpoint %s, want %s", rec.EndpointURL, redact.URL(ht.EndpointURL))
		case rec.BodySHA256 != body:
			fault = fmt.Sprintf("body sha256 %s, want %s (target.body at ceiling %d)", rec.BodySHA256, body, rec.Ceiling.Ceiling)
		default:
			exposed := probe.Exposed(ht, &rec)
			if ht != t {
				for _, e := range probe.ExposedByMarking(&rec) {
					if !slices.Contains(exposed, e) {
						exposed = append(exposed, e)
					}
				}
			}
			if len(exposed) > 0 {
				fault = "carries values the route withholds: " + strings.Join(exposed, ", ")
			}
		}
		if fault == "" && host {
			if err := rec.Verify(ht, probe.LengthValue(rec.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
				fault = err.Error()
			}
		}
		if fault != "" {
			return none, nil, fmt.Errorf("ceiling probe %s: route %s at %d: %s", f, rec.Route, rec.Ceiling.Ceiling, fault)
		}
		recs = append(recs, &rec)
		if rec.Route == t.Name {
			own = append(own, &rec)
		}
		copies[fmt.Sprintf("ceiling-probe-%s-%d.json", rec.Route, rec.Ceiling.Ceiling)] = b
	}
	if fault := probe.ModelFault(t, own); fault != "" {
		return none, nil, fmt.Errorf("run: route %s is not characterized (SPEC-HOSTED §2): %s", t.Name, fault)
	}
	choice, err := probe.ChooseCeiling(hosts, recs)
	if err != nil {
		return none, nil, err
	}
	switch {
	case choice.Excluded:
		return none, nil, fmt.Errorf("run: the ceiling rule excludes the model: %s", choice.Reason)
	case choice.Ceiling != cfg.Release.Shape.CeilingTokens:
		return none, nil, fmt.Errorf("run: the ceiling check chose %d (%s); the configuration runs at %d", choice.Ceiling, choice.Reason, cfg.Release.Shape.CeilingTokens)
	}
	return choice, copies, nil
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

// cellDir is a run's directory under the route's.
func (r *route) cellDir(run schedule.Run) string { return runDir(r.dir, run) }

// runDir is a run's directory under its route's directory.
func runDir(routeDir string, run schedule.Run) string {
	return filepath.Join(routeDir, fmt.Sprintf("%s-a%d", run.Label, run.Attempt))
}

// routeState is the route's position read from its cell records: every
// run it takes in start order, the attempt 1 runs and the re-runs taken,
// and the first of them with no record; and, in start order, the missed
// and interrupted cells whose directory holds no manifest and no
// attempts file.
type routeState struct {
	runs       []schedule.Run
	records    map[string]*cellRecord
	next       *schedule.Run
	unlabelled []*cellRecord
}

func runKey(r schedule.Run) string { return fmt.Sprintf("%s/%d", r.Label, r.Attempt) }

// withhold keeps the confidential limits in read, as the refresh command
// printed them, for the operator file as read when the route started and
// every later read of it (cellOperator).
func (r *route) withhold(read []evidence.RefreshedLimit) {
	for _, l := range read {
		if l.Confidential {
			r.withheld = append(r.withheld, l)
		}
	}
	r.op.Withhold(read)
}

// retainedReads are the limits read by the refresh records as run that
// the retained store keeps beside the frames of the route's cells with a
// manifest.
func (r *route) retainedReads() ([]evidence.RefreshedLimit, error) {
	paths, err := filepath.Glob(filepath.Join(r.dir, "*", evidence.ManifestFile))
	if err != nil {
		return nil, err
	}
	var out []evidence.RefreshedLimit
	for _, p := range paths {
		m, err := evidence.ReadManifest(p)
		if err != nil {
			return nil, err
		}
		if m.Frames == nil {
			continue
		}
		rec, err := evidence.ReadRetainedRefresh(filepath.Join(r.framesRoot, filepath.Dir(filepath.FromSlash(m.Frames.Name))))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if read, err := evidence.ReadRefreshedLimits(rec, &r.cfg.Target); err == nil {
			out = append(out, read...)
		}
	}
	return out, nil
}

// state reads the route's cell records and refuses one written under
// another schedule file. A record left running by an ended process
// becomes interrupted. A re-run is taken, in order of the
// attempt 1 runs, for each attempt 1 that does not count while the route
// has re-runs left and the window holds the cell's attempt 2.
func (r *route) state() (*routeState, error) {
	paths, err := filepath.Glob(filepath.Join(r.dir, "*", cellFile))
	if err != nil {
		return nil, err
	}
	st := &routeState{records: map[string]*cellRecord{}}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var rec cellRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if rec.Run.Route != r.cfg.Target.Name {
			return nil, fmt.Errorf("%s: route %q, this process runs %q", p, rec.Run.Route, r.cfg.Target.Name)
		}
		if err := r.scheduleOf(p, &rec); err != nil {
			return nil, err
		}
		if rec.Outcome == outcomeRunning {
			rec.Outcome, rec.Reasons = outcomeInterrupted, append(rec.Reasons, "the process ended during the cell")
			if err := replaceJSON(p, &rec); err != nil {
				return nil, err
			}
		}
		st.records[runKey(rec.Run)] = &rec
		dir := filepath.Dir(p)
		if (rec.Outcome == outcomeMissed || rec.Outcome == outcomeInterrupted) &&
			!fileExists(filepath.Join(dir, evidence.ManifestFile)) && !fileExists(filepath.Join(dir, evidence.AttemptsFile)) {
			st.unlabelled = append(st.unlabelled, &rec)
		}
	}
	slices.SortFunc(st.unlabelled, func(a, b *cellRecord) int { return a.Run.Start.Compare(b.Run.Start) })
	st.runs = slices.Clone(r.runs)
	taken := 0
	for _, a := range r.runs {
		rec := st.records[runKey(a)]
		if rec == nil || !rec.rerunnable() || !r.hasAttempt2(a.Label) {
			continue
		}
		rr, err := r.sched.Rerun(a.Route, a.Label, taken)
		if err != nil {
			continue
		}
		taken++
		st.runs = append(st.runs, rr)
	}
	slices.SortFunc(st.runs, func(a, b schedule.Run) int { return a.Start.Compare(b.Start) })
	for i := range st.runs {
		if st.records[runKey(st.runs[i])] == nil {
			st.next = &st.runs[i]
			break
		}
	}
	return st, nil
}

// sameSchedule refuses the route's cell records written under a schedule
// file other than the route's.
func (r *route) sameSchedule() error {
	paths, err := filepath.Glob(filepath.Join(r.dir, "*", cellFile))
	if err != nil {
		return err
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var rec cellRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		if err := r.scheduleOf(p, &rec); err != nil {
			return err
		}
	}
	return nil
}

// scheduleOf refuses rec, read from path, unless it was written under
// the route's schedule file.
func (r *route) scheduleOf(path string, rec *cellRecord) error {
	if rec.ScheduleSHA256 != r.scheduleSHA256 {
		return fmt.Errorf("run: %s: the cell ran under schedule sha256 %q; --schedule has sha256 %s", path, rec.ScheduleSHA256, r.scheduleSHA256)
	}
	return nil
}

func (r *route) hasAttempt2(label string) bool {
	for _, c := range r.sched.Cells {
		if c.Label == label {
			return len(c.Attempts) > 1
		}
	}
	return false
}

// loop runs the route's cells in start order until none is left, the
// route stops, or ctx ends. A run whose start passed more than the start
// tolerance ago is recorded as missed. Each missed or interrupted cell is
// labelled before the next run. After the last cell the route's
// window-close record is written.
func (r *route) loop(ctx context.Context) (int, error) {
	for {
		st, err := r.state()
		if err != nil {
			return exitError, err
		}
		for _, rec := range st.unlabelled {
			if err := r.label(ctx, rec); err != nil {
				return exitError, fmt.Errorf("%s %s attempt %d: label: %w", rec.Run.Route, rec.Run.Label, rec.Run.Attempt, err)
			}
			fmt.Printf("%s %s attempt %d: %s, labelled\n", rec.Run.Route, rec.Run.Label, rec.Run.Attempt, rec.Outcome)
		}
		if st.next == nil {
			if err := r.close(ctx, st); err != nil {
				return exitError, err
			}
			return r.finish(st), nil
		}
		next := *st.next
		if late := r.now().Sub(next.Start); late > schedule.StartTolerance {
			rec := r.newRecord(next)
			rec.Outcome = outcomeMissed
			rec.Reasons = []string{fmt.Sprintf("not started: %s after its start at %s, beyond the %s start tolerance", late.Round(time.Second), next.Start.Format(time.RFC3339), schedule.StartTolerance)}
			if err := r.writeNew(next, rec); err != nil {
				return exitError, err
			}
			fmt.Printf("%s %s attempt %d: missed\n", next.Route, next.Label, next.Attempt)
			continue
		}
		if err := r.wait(ctx, next.Start); err != nil {
			fmt.Printf("%s: stopped before %s attempt %d: %v\n", next.Route, next.Label, next.Attempt, err)
			return exitStopped, nil
		}
		rec, err := r.cell(ctx, next)
		if err != nil {
			return exitError, err
		}
		fmt.Printf("%s %s attempt %d: %s\n", next.Route, next.Label, next.Attempt, cellSummary(rec))
		if rec.Outcome == outcomeStopped {
			return exitStopped, nil
		}
	}
}

// finish prints each cell's final attempt and returns exitOK when every
// cell counts.
func (r *route) finish(st *routeState) int {
	code := exitOK
	for _, a := range r.runs {
		rec := st.records[runKey(a)]
		if rec != nil && rec.Outcome != outcomeCounted {
			if rr := st.records[runKey(schedule.Run{Label: a.Label, Attempt: 2})]; rr != nil {
				rec = rr
			}
		}
		if rec == nil || rec.Outcome != outcomeCounted {
			code = exitAdverse
		}
		if rec != nil {
			fmt.Printf("%s %s: attempt %d %s\n", a.Route, a.Label, rec.Run.Attempt, cellSummary(rec))
		}
	}
	return code
}

func cellSummary(rec *cellRecord) string {
	s := rec.Outcome
	if len(rec.Reasons) > 0 {
		s += ": " + fmt.Sprint(rec.Reasons)
	}
	return s
}

// newRecord is a run's record before the cell begins.
func (r *route) newRecord(run schedule.Run) *cellRecord {
	return &cellRecord{SchemaVersion: 1, Run: run, OffsetMinutes: r.offsetMinutes, ScheduleSHA256: r.scheduleSHA256, StartUTC: r.now().UTC(),
		AdmittedRPS: r.verdict.Admitted(), OpenFilesLimit: r.openFiles}
}

// writeNew writes a run's first record; a run already recorded is never
// run again.
func (r *route) writeNew(run schedule.Run, rec *cellRecord) error {
	path := filepath.Join(r.cellDir(run), cellFile)
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s exists: a cell attempt is never run twice", path)
	}
	return replaceJSON(path, rec)
}
