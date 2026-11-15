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
	// BillingRefusals counts the probe's and the ladder's responses that
	// the target documents as a billing refusal (billingRefusal).
	BillingRefusals int `json:"billing_refusals,omitempty"`
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
	// Decided is a stopped cell's label, written to its running record
	// before its evidence.
	Decided *decidedLabel `json:"decided,omitempty"`
}

// decidedLabel is a cell's outcome, reasons and billing refusals.
type decidedLabel struct {
	Outcome         string   `json:"outcome"`
	Reasons         []string `json:"reasons,omitempty"`
	BillingRefusals int      `json:"billing_refusals,omitempty"`
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
	// ackDir holds the operator's acknowledgement files (route.holds).
	ackDir string

	verdict        *admission.Verdict
	steps          int
	ceiling        probe.CeilingChoice
	ceilingProbes  map[string][]byte
	accountCreated string
	preWindow      int
	shape          pilot.Shape
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
	// due is when runCell takes a run to be due, its scheduled start
	// where nil; the rehearsal and tests run cells at other times.
	due func(run schedule.Run) time.Time
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
	ackDir := fs.String("ack-dir", "", "directory outside --out, read-only to the route's user, of the operator's acknowledgement files: a stopped cell holds the route until resume-<route>-<cell>-a<attempt> exists in it, an operator abort until resume-<route>-abort-<time> does, and a stopped cell whose record failed to write until resume-<route>-stop-<cell>-a<attempt> does (required)")
	framesRoot := fs.String("frames", "", "raw-frame store directory outside the source tree (required)")
	clockSync := fs.String("clock-sync", "", "host clock-sync command run before each cell, split on spaces with no shell (required)")
	statusCmd := fs.String("status-cmd", "", "endpoint-status command run at each cell's start and end, split on spaces with no shell (required)")
	balanceCmd := fs.String("balance-cmd", "", "account balance or quota command run at each cell's start and end, split on spaces with no shell, printing {\"amount_usd\": \"<decimal>\"} or {\"unavailable\": \"<reason>\"} (required)")
	refreshCmd := fs.String("refresh-cmd", "", "operator-records refresh command run at each cell's start and once after the last cell, split on spaces with no shell, rewriting the files of the operator file's limits and eligibility records and printing {\"limits\": [...]}, each target.limits entry's current value or why it is unavailable (required)")
	opPath := fs.String("operator", "", "the route's operator file, read again at each cell's start (required)")
	var hostFiles, probeFiles listFlag
	fs.Var(&hostFiles, "host-config", "the release configuration of a route in release.other_hosts, as the schedule froze it: one per route (repeatable, required for each)")
	fs.Var(&probeFiles, "ceiling-probe", "a ceiling check record of a host of the route's host group: every host's at the shape's ceiling (512 or 16), and at its fallback (1024 or 256) when one failed there; for Shape S also each record of another route that spent a byte cap (repeatable, required)")
	byteCap := fs.Int("byte-cap", 0, "Shape S ticket byte cap (required for a Shape S route; absent for Shape R)")
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
	if *ackDir == "" {
		return exitError, errors.New("run: --ack-dir is required")
	}
	if err := checkAckDir(*ackDir, *out); err != nil {
		return exitError, err
	}
	build, _ := debug.ReadBuildInfo()
	r, err := newRoute(routeInput{
		cfg: cfg, hostCfgs: hostCfgs, configName: filepath.Base(*cfgPath), schedulePath: *schedulePath, out: *out, ackDir: *ackDir, ledgerPath: *ledgerPath, key: key,
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
// scales its ladder, the plan at the schedule's exact counts and the
// binding rate the schedule froze, which the steps are fractions of.
type ceilingInput struct {
	cfg        *config.Config
	plan       admission.Plan
	bindingRPS float64
}

// routeInput is a run command after its flags are read.
type routeInput struct {
	cfg                                       *config.Config
	hostCfgs                                  []*config.Config
	configName, schedulePath, out, ledgerPath string
	ackDir                                    string
	key                                       *releasekey.Key
	framesRoot, clockSync, statusCmd, opPath  string
	balanceCmd, refreshCmd                    string
	probeFiles                                []string
	byteCap                                   int
	accountCreated                            string
	build                                     *debug.BuildInfo
}

// newRoute checks the route's inputs and fixes what every cell shares:
// the binding rate the schedule froze, which the binding-rate rule must
// give again at the route's newest price up to the ledger position the
// schedule froze (bindingSolution); the ceiling rule's verdict at that
// rate and price and the schedule's exact counts, which must admit every
// step; the ceiling choice, the request set and the account record. It
// refuses an output directory holding a cell of the route written under
// another schedule file.
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
	r := &route{cfg: cfg, configName: in.configName, sched: sched, scheduleSHA256: hex.EncodeToString(sum[:]), runs: runs, out: in.out, dir: filepath.Join(in.out, name), ackDir: in.ackDir,
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
		read, err := retainedReads(r.dir, r.framesRoot, &cfg.Target)
		if err != nil {
			return nil, err
		}
		r.withhold(read)
	}
	if err := r.op.CheckConfidential(&cfg.Target, cfg.Release); err != nil {
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
	fc, _ := sched.Config(name)
	if fc.BindingLimit == "" {
		return nil, fmt.Errorf("run: the schedule freezes no binding rate of route %s", name)
	}
	sol, err := bindingSolution(cfg, sched, frozen)
	if err != nil {
		return nil, err
	}
	if sol.BindingRPS != fc.BindingRPS || sol.Binding.Dimension != fc.BindingLimit || sol.Binding.WindowS != fc.BindingWindowS {
		got := schedule.RouteConfig{Route: name, BindingLimit: sol.Binding.Dimension, BindingWindowS: sol.Binding.WindowS}
		cfgs := []*config.Config{cfg}
		return nil, fmt.Errorf("run: route %s: the binding-rate rule gives %g rps, binding limit %s; the schedule froze %g rps, %s",
			name, sol.BindingRPS, bindingName(got, cfgs), fc.BindingRPS, bindingName(fc, cfgs))
	}
	cfg.Release.BindingRPS = fc.BindingRPS
	plan, err := exactPlan(cfg, sched)
	if err != nil {
		return nil, err
	}
	plan.ChecksUTC = checkTimes(frozen, sched, name)
	if r.verdict, err = admission.CeilingRule(cfg, price, plan, fc.BindingRPS); err != nil {
		return nil, err
	}
	rc, rel := *cfg, *cfg.Release
	rc.Release = &rel
	r.rule = ceilingInput{cfg: &rc, plan: plan, bindingRPS: fc.BindingRPS}
	if st, err = r.ledger.State(); err != nil {
		return nil, err
	}
	if err := st.Covers(cfg, r.verdict); err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}
	for k, s := range r.verdict.Steps {
		if !s.Admitted {
			return nil, fmt.Errorf("run: route %s: the ceiling rule at the frozen binding rate %g rps refuses step %d (%g rps) at the schedule's counts; binding limit %s",
				name, fc.BindingRPS, k+1, s.RPS, s.Binding.Dimension)
		}
	}
	r.steps = len(r.verdict.Steps)
	first, _ := time.Parse(schedule.DateLayout, sched.Input.First)
	r.preWindow = sentBefore(entries, name, first)

	if r.shape, err = routeShape("run", cfg, in.byteCap); err != nil {
		return nil, err
	}
	if r.ceiling, r.ceilingProbes, _, err = ceilingChoice(cfg, hosts, in.probeFiles, in.byteCap, sched.Input.LedgerSHA256, frozen); err != nil {
		return nil, err
	}
	if err := runsAt(cfg, r.ceiling); err != nil {
		return nil, err
	}
	group := cfg.Release.AccountGroup
	if err := lateChecks(entries, len(frozen), group, groupRoutes(sched, name, group)); err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}
	if _, err := time.Parse(time.DateOnly, in.accountCreated); err != nil {
		return nil, fmt.Errorf("run: --account-created %q is not a date (2006-01-02)", in.accountCreated)
	}
	r.accountCreated = in.accountCreated
	return r, nil
}

// exactPlan is the ceiling rule's plan at the schedule's seeded counts of
// cfg's ladder at release.BindingRPS, each step's count and the warm-up's
// the largest over every cell attempt's ladder, with the route's
// scheduled attempts (scheduledAttempts).
func exactPlan(cfg *config.Config, sched *schedule.Schedule) (admission.Plan, error) {
	p := admission.ExpectedPlan(cfg, cfg.Release.BindingRPS)
	p.WarmupRequests = 0
	p.StepRequests = make([]int, len(p.StepRequests))
	for _, c := range sched.Cells {
		for _, a := range c.Attempts {
			sc := *cfg
			sc.Run.Seed = a.Seed
			l := loadgen.BuildLadder(&sc)
			if l == nil || len(l.Steps) != len(p.StepRequests) {
				return admission.Plan{}, errors.New("run: the configuration builds no release ladder at its binding rate")
			}
			p.WarmupRequests = max(p.WarmupRequests, l.Steps[0].First)
			for k, st := range l.Steps {
				p.StepRequests[k] = max(p.StepRequests[k], st.End-st.First)
			}
		}
	}
	p.Attempts = scheduledAttempts(sched, cfg.Target.Name)
	return p, nil
}

// scheduledAttempts are route's cell attempts in sched, each at its slot
// start plus the route's offset.
func scheduledAttempts(sched *schedule.Schedule, route string) []admission.Attempt {
	var offset time.Duration
	for _, o := range sched.Offsets {
		if o.Route == route {
			offset = time.Duration(o.Minutes) * time.Minute
		}
	}
	var out []admission.Attempt
	for _, c := range sched.Cells {
		for _, a := range c.Attempts {
			out = append(out, admission.Attempt{Start: a.SlotStart.Add(offset), Rerun: a.Attempt > 1})
		}
	}
	return out
}

// checkTimes are the send times the ledger entries record for the
// ceiling-check requests on route's account group in sched, each probe
// admission's reserved requests at its time: the group's routes, or the
// route alone where it names none.
func checkTimes(entries []admission.Entry, sched *schedule.Schedule, route string) []time.Time {
	group := []string{route}
	if own, _ := sched.Config(route); own.AccountGroup != "" {
		group = nil
		for _, c := range sched.Input.Configs {
			if c.AccountGroup == own.AccountGroup {
				group = append(group, c.Route)
			}
		}
	}
	var out []time.Time
	for _, e := range entries {
		if e.Kind == admission.KindAdmit && e.Purpose == admission.PurposeProbe && slices.Contains(group, e.Route) {
			for i := 0; i < e.Reserve; i++ {
				out = append(out, e.Time)
			}
		}
	}
	return out
}

// bindingSolution is the binding-rate rule (admission.Solve) applied to
// cfg's route at its newest price in entries, over its attempts in sched
// and the ceiling-check requests entries record on its account group
// (checkTimes).
func bindingSolution(cfg *config.Config, sched *schedule.Schedule, entries []admission.Entry) (*admission.Solution, error) {
	price, err := routePrice(entries, cfg.Target.Name)
	if err != nil {
		return nil, err
	}
	// Solve reads no step counts.
	plan := admission.ExpectedPlan(cfg, 1)
	plan.Attempts = scheduledAttempts(sched, cfg.Target.Name)
	plan.ChecksUTC = checkTimes(entries, sched, cfg.Target.Name)
	return admission.Solve(cfg, price, plan)
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
// host group, whose hosts are the target and release.other_hosts, all of
// the route's request shape, and requires their choice to run the group
// at the configured ceiling. Every record holds no field
// probe.ReleaseRecord does not define and is byte for byte the
// probe.Encode encoding of the record it decodes to. Every host's record
// is judged by the host's target, this route's own or the one in configs:
// it carries the host's model, the route's shape and byteCap or a byte
// cap spent above it (ceilingChoiceFor), was admitted in the ledger whose
// open-line SHA-256 is ledger, carries the target's expected model id,
// adapter and endpoint and the hash of its body at the record's ceiling,
// one the shape permits the route (config.ReleaseBodyAt), none of the
// values the target withholds (probe.Exposed), and the results its
// requests give at the shape's input bound through the target's start
// gate (probe.ReleaseRecord.Verify). This route's records together carry
// no reported model id that keeps the provider-model out
// (probe.ModelFault). Another host's records also carry none of the
// values their snapshots' confidential markings withhold
// (probe.ExposedByMarking), and another host whose records carry a model
// fault is left out of the choice (probe.ChooseCeiling). A record at a
// ceiling the choice does not need is kept with the others.
func readCeilingProbes(cfg *config.Config, configs map[string]*config.Target, files []string, byteCap int, ledger string) (probe.CeilingChoice, map[string][]byte, error) {
	choice, copies, _, err := ceilingChoice(cfg, configs, files, byteCap, ledger, nil)
	if err != nil {
		return probe.CeilingChoice{}, nil, err
	}
	if err := runsAt(cfg, choice); err != nil {
		return probe.CeilingChoice{}, nil, err
	}
	return choice, copies, nil
}

// runsAt refuses a choice that excludes cfg's host group or does not run
// it at cfg's ceiling.
func runsAt(cfg *config.Config, choice probe.CeilingChoice) error {
	switch {
	case choice.Excluded:
		return fmt.Errorf("run: the ceiling check excludes the host group: %s", choice.Reason)
	case choice.Ceiling != cfg.Release.Shape.CeilingTokens:
		return fmt.Errorf("run: the ceiling check chose %d (%s); the configuration runs at %d", choice.Ceiling, choice.Reason, cfg.Release.Shape.CeilingTokens)
	}
	return nil
}

// ceilingChoice is ceilingChoiceFor as run reads the records.
func ceilingChoice(cfg *config.Config, configs map[string]*config.Target, files []string, byteCap int, ledger string, entries []admission.Entry) (probe.CeilingChoice, map[string][]byte, []*probe.ReleaseRecord, error) {
	return ceilingChoiceFor(cfg, configs, files, byteCap, ledger, entries, false)
}

// ceilingChoiceFor is readCeilingProbes up to the ceiling check's choice,
// which may exclude the host group, with the host group's records read.
// A Shape S host group also reads the records among files of other
// routes that spend a byte cap (probe.Spent), each sent on ledger and
// re-derived from its requests; it refuses byteCap above a spent cap,
// and a host's record at a byte cap other than byteCap unless it was
// stopped or the cap is spent and above byteCap, where the record
// decides nothing (probe.ChooseCeilingAt). A host's record is verified
// under its route's configuration (frozenVerify). A host's repeat of a
// refused check must start at least probe.RepeatQuiet of the host's
// target after the refused check's end. Where entries, the ledger lines
// the schedule froze, are given, the records must hold one record of each
// ceiling-check admission there of a host (checkAdmitted). Where
// excluding is set, a model fault in this route's own records
// (probe.ModelFault) excludes the host group as not characterized;
// otherwise it is refused.
func ceilingChoiceFor(cfg *config.Config, configs map[string]*config.Target, files []string, byteCap int, ledger string, entries []admission.Entry, excluding bool) (probe.CeilingChoice, map[string][]byte, []*probe.ReleaseRecord, error) {
	var none probe.CeilingChoice
	if len(files) == 0 {
		return none, nil, nil, errors.New("run: --ceiling-probe is required: the ceiling check's probe records")
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
		for _, c := range cfg.Release.Shape.Ceilings() {
			b, err := config.ReleaseBodyAt(ht.Body, ht.Adapter, c)
			if err != nil {
				return none, nil, nil, fmt.Errorf("run: route %s: target.body at %d: %w", route, c, err)
			}
			sum := sha256.Sum256([]byte(b))
			bodies[route][c] = hex.EncodeToString(sum[:])
		}
	}
	shapeS := cfg.Release.Shape.Name == config.ShapeS
	var recs, own, others []*probe.ReleaseRecord
	paths := map[*probe.ReleaseRecord]string{}
	copies := map[string][]byte{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return none, nil, nil, fmt.Errorf("ceiling probe: %w", err)
		}
		var rec probe.ReleaseRecord
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil {
			return none, nil, nil, fmt.Errorf("ceiling probe %s: %w", f, err)
		}
		if canon, err := probe.Encode(&rec); err != nil || !bytes.Equal(b, canon) {
			return none, nil, nil, fmt.Errorf("ceiling probe %s: not the record as ceiling-check writes it", f)
		}
		var fault string
		model, host := models[rec.Route]
		ht, body := targets[rec.Route], bodies[rec.Route][rec.Ceiling.Ceiling]
		switch {
		case !host && (!shapeS || len(probe.Spent([]*probe.ReleaseRecord{&rec})) == 0):
			fault = fmt.Sprintf("route %s is not a host of the host group (%s), and its record spends no byte cap", rec.Route, strings.Join(hosts, ", "))
		case !host && rec.LedgerSHA256 != ledger:
			fault = fmt.Sprintf("ledger open line sha256 %q; the schedule froze %s", rec.LedgerSHA256, ledger)
		case !host:
			if err := rec.Verify(nil, probe.LengthValue(rec.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
				fault = err.Error()
			}
		case rec.RequestedModel != model:
			fault = fmt.Sprintf("model %q, want %q", rec.RequestedModel, model)
		case rec.Shape != cfg.Release.Shape.Name:
			fault = fmt.Sprintf("Shape %q, want Shape %s", rec.Shape, cfg.Release.Shape.Name)
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
				fault = frozenVerify(err).Error()
			}
		}
		if fault != "" {
			return none, nil, nil, fmt.Errorf("ceiling probe %s: route %s at %d: %s", f, rec.Route, rec.Ceiling.Ceiling, fault)
		}
		paths[&rec] = f
		switch {
		case !host:
			others = append(others, &rec)
		case rec.Route == t.Name:
			own = append(own, &rec)
			fallthrough
		default:
			recs = append(recs, &rec)
		}
		name := probeCopyName(&rec)
		if copies[name] != nil {
			return none, nil, nil, fmt.Errorf("ceiling probe %s: route %s at %d: a record of ledger line %d given twice", f, rec.Route, rec.Ceiling.Ceiling, rec.LedgerSeq)
		}
		copies[name] = b
	}
	spent := probe.Spent(append(slices.Clone(recs), others...))
	var superseded []int
	if shapeS {
		var caps []int
		for c := range spent {
			caps = append(caps, c)
		}
		slices.Sort(caps)
		for _, c := range caps {
			if byteCap > c {
				return none, nil, nil, fmt.Errorf("run: --byte-cap %d is above byte cap %d, which route %s's check at ledger line %d spent", byteCap, c, spent[c].Route, spent[c].LedgerSeq)
			}
			if c > byteCap {
				superseded = append(superseded, c)
			}
		}
	}
	var live []*probe.ReleaseRecord
	for _, rec := range recs {
		if rec.ByteCap != byteCap && !slices.Contains(superseded, rec.ByteCap) && rec.Outcome() != probe.CheckStopped {
			return none, nil, nil, fmt.Errorf("ceiling probe %s: route %s at %d: byte cap %d, want --byte-cap %d", paths[rec], rec.Route, rec.Ceiling.Ceiling, rec.ByteCap, byteCap)
		}
		if !slices.Contains(superseded, rec.ByteCap) {
			live = append(live, rec)
		}
	}
	modelFault := probe.ModelFault(t, own)
	if modelFault != "" && !excluding {
		return none, nil, nil, fmt.Errorf("run: route %s is not characterized (SPEC-HOSTED §2): %s", t.Name, modelFault)
	}
	if err := repeatQuiet(live, targets); err != nil {
		return none, nil, nil, err
	}
	if entries != nil {
		if err := checkAdmitted(entries, hosts, append(slices.Clone(recs), others...)); err != nil {
			return none, nil, nil, fmt.Errorf("run: %w", err)
		}
	}
	if modelFault != "" {
		return probe.CeilingChoice{Excluded: true, Reason: "not characterized (SPEC-HOSTED §2): " + modelFault}, copies, recs, nil
	}
	choice, err := probe.ChooseCeilingAt(&cfg.Release.Shape, hosts, recs, superseded)
	if err != nil {
		return none, nil, nil, err
	}
	return choice, copies, recs, nil
}

// repeatQuiet refuses a host's repeat of a refused check, the later of
// two records at one ceiling that were not stopped, that started less
// than probe.RepeatQuiet of the host's target in targets after the
// refused check ended.
func repeatQuiet(recs []*probe.ReleaseRecord, targets map[string]*config.Target) error {
	type key struct {
		route   string
		ceiling int
	}
	at := map[key][]*probe.ReleaseRecord{}
	for _, r := range recs {
		if r.Outcome() != probe.CheckStopped {
			k := key{r.Route, r.Ceiling.Ceiling}
			at[k] = append(at[k], r)
		}
	}
	for k, rs := range at {
		if len(rs) != 2 || targets[k.route] == nil {
			continue
		}
		a, b := rs[0], rs[1]
		if b.LedgerSeq < a.LedgerSeq {
			a, b = b, a
		}
		if q := probe.RepeatQuiet(targets[k.route]); a.Outcome() == probe.CheckRefused && b.StartUTC.Before(a.EndUTC.Add(q)) {
			return fmt.Errorf("run: route %s at %d: the check at ledger line %d repeats the one refused at line %d %s after its end; the repeat waits %s",
				k.route, k.ceiling, b.LedgerSeq, a.LedgerSeq, b.StartUTC.Sub(a.EndUTC), q)
		}
	}
	return nil
}

// waitUntil sleeps until the wall clock reaches t, or returns ctx's cause
// when ctx has ended or ends first. A t with no monotonic reading is
// reached on the wall clock, which timers do not follow.
func waitUntil(ctx context.Context, t time.Time) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	for d := time.Until(t); d > 0; d = time.Until(t) {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		}
	}
	return nil
}

// cellDir is a run's directory under the route's.
func (r *route) cellDir(run schedule.Run) string { return runDir(r.dir, run) }

// runDir is a run's directory under its route's directory.
func runDir(routeDir string, run schedule.Run) string {
	return filepath.Join(routeDir, fmt.Sprintf("%s-a%d", run.Label, run.Attempt))
}

// routeState is the route's position read from its cell records: every
// run it takes in start order, the attempt 1 runs and the re-runs taken,
// and the first of them with no record; in start order, the missed and
// interrupted cells whose directory holds no manifest and no attempts
// file; and the cells left running by an ended process that state labels
// stopped.
type routeState struct {
	runs       []schedule.Run
	records    map[string]*cellRecord
	next       *schedule.Run
	unlabelled []*cellRecord
	recovered  []*cellRecord
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

// retainedReads are the limits of t read by the refresh records as run
// that the retained store under framesRoot keeps beside the frames of
// the cells with a manifest under dir, the route's directory.
func retainedReads(dir, framesRoot string, t *config.Target) ([]evidence.RefreshedLimit, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*", evidence.ManifestFile))
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
		rec, err := evidence.ReadRetainedRefresh(filepath.Join(framesRoot, filepath.Dir(filepath.FromSlash(m.Frames.Name))))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if read, err := evidence.ReadRefreshedLimits(rec, t); err == nil {
			out = append(out, read...)
		}
	}
	return out, nil
}

// state reads the route's cell records and refuses one written under
// another schedule file. A record left running by an ended process takes
// the label of a manifest beside it (labelledBy); with no manifest, a
// record whose cell left a stop marker takes the marker's label, probe
// status and requests sent where the marker carries them, one
// that carries its decided label takes that, and one whose probe record
// is a documented billing refusal (probeBilling) is stopped, each with an
// execution error, and any other becomes interrupted, with the probe's
// status and count from its probe record (probeSent); each of these that
// ends stopped goes to recovered. Each takes an end: the stop marker's
// time, and otherwise the latest time its records carry (recordsEnd). A
// stopped record is never labelled after the fact: it holds the route
// until its acknowledgement file exists (holds), and pages publishes it
// from its run record. A re-run is taken, in order of the
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
		dir := filepath.Dir(p)
		if rec.Outcome == outcomeRunning {
			sm, err := r.stopMarker(rec.Run)
			if err != nil {
				return nil, err
			}
			switch d := rec.Decided; {
			case labelledBy(dir, &rec):
			case sm != nil:
				rec.Outcome, rec.Reasons, rec.BillingRefusals = outcomeStopped, slices.Clone(sm.Reasons), sm.BillingRefusals
				rec.Error = "the cell's record failed to write; its label is its stop marker's"
				if sm.Sent != nil {
					rec.ProbeStatus, rec.Sent = sm.ProbeStatus, *sm.Sent
				} else {
					probeSent(dir, &rec)
				}
				end := sm.AtUTC.UTC()
				rec.EndUTC = &end
			case d != nil:
				rec.Outcome, rec.Reasons, rec.BillingRefusals = d.Outcome, slices.Clone(d.Reasons), d.BillingRefusals
				rec.Error = "the process ended before the cell's evidence was written"
				probeSent(dir, &rec)
			default:
				pb, err := r.probeRecordBilling(dir)
				if err != nil {
					return nil, err
				}
				if pb != nil {
					rec.Outcome, rec.Reasons, rec.BillingRefusals = outcomeStopped, []string{pb.Error()}, 1
					rec.Error = "the cell's record was not rewritten after its probe; its label is its probe record's"
					probeSent(dir, &rec)
					break
				}
				rec.Outcome, rec.Reasons = outcomeInterrupted, append(rec.Reasons, "the process ended during the cell")
				probeSent(dir, &rec)
			}
			if rec.EndUTC == nil {
				end, err := recordsEnd(dir, &rec)
				if err != nil {
					return nil, err
				}
				rec.EndUTC = &end
			}
			rec.Decided = nil
			if rec.Outcome == outcomeStopped {
				st.recovered = append(st.recovered, &rec)
			}
			if err := replaceJSON(p, &rec); err != nil {
				return nil, err
			}
		}
		st.records[runKey(rec.Run)] = &rec
		if rec.Outcome != outcomeStopped && rec.unlabelled() && !fileExists(filepath.Join(dir, evidence.ManifestFile)) && !fileExists(filepath.Join(dir, evidence.AttemptsFile)) {
			st.unlabelled = append(st.unlabelled, &rec)
		}
	}
	slices.SortFunc(st.unlabelled, func(a, b *cellRecord) int { return a.Run.Start.Compare(b.Run.Start) })
	slices.SortFunc(st.recovered, func(a, b *cellRecord) int { return a.Run.Start.Compare(b.Run.Start) })
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

// labelledBy gives rec, a record left running, the outcome, reasons,
// billing refusals and epoch of the manifest in dir, read unverified, and
// its modification time as the end, with the probe's status and count
// from the cell's probe record (probeSent), and reports whether a
// manifest labelled the cell.
func labelledBy(dir string, rec *cellRecord) bool {
	path := filepath.Join(dir, evidence.ManifestFile)
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	m, err := evidence.ReadManifest(path)
	if err != nil || m.Release == nil || m.Release.Outcome == "" || m.Release.Outcome == outcomeRunning {
		return false
	}
	rec.Outcome, rec.Reasons, rec.BillingRefusals = m.Release.Outcome, slices.Clone(m.Release.Reasons), m.Release.BillingRefusals
	if m.Release.LadderStarted {
		epoch := m.StartUTC.UTC()
		rec.EpochUTC = &epoch
	}
	end := fi.ModTime().UTC()
	rec.EndUTC = &end
	probeSent(dir, rec)
	return true
}

// probeSent gives rec the status and request count of the probe record
// in dir, where one decodes.
func probeSent(dir string, rec *cellRecord) {
	b, err := os.ReadFile(filepath.Join(dir, probe.File))
	if err != nil {
		return
	}
	var pr probe.Record
	if json.Unmarshal(b, &pr) != nil {
		return
	}
	rec.ProbeStatus, rec.Sent.Probe = pr.Request.Status, admission.ProbeSent(&pr)
}

// probeBilling is the route stop for pr where t documents its response
// as a billing refusal (billingRefusal), and nil otherwise.
func probeBilling(t *config.Target, pr *probe.Record) *routeStop {
	// A target with a confidential limit withholds the probe's body and
	// in-band error.
	b := pr.Request.Body
	if h := pr.Request.Retained; h != nil && h.Body != nil {
		b = h.Body
	}
	var body []byte
	if b != nil && b.Text != nil {
		body = []byte(*b.Text)
	}
	inBand := ""
	if pr.Shape != nil {
		inBand = pr.Shape.Error
	}
	if h := pr.Request.Retained; h != nil && h.StreamError != "" {
		inBand = h.StreamError
	}
	return billingRefusal(t, pr.Request.Status, body, inBand)
}

// probeRecordBilling is the probeBilling stop of the probe record in
// dir, nil where none is written or it does not decode.
func (r *route) probeRecordBilling(dir string) (*routeStop, error) {
	b, err := os.ReadFile(filepath.Join(dir, probe.File))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var pr probe.Record
	if json.Unmarshal(b, &pr) != nil {
		return nil, nil
	}
	return probeBilling(&r.cfg.Target, &pr), nil
}

// unlabelled reports whether the record is of a cell that may have no
// manifest: a missed or interrupted cell, which a manifest labels after
// the fact, or a stopped one whose evidence failed to write, which keeps
// its run record alone.
func (c *cellRecord) unlabelled() bool {
	return c.Outcome == outcomeMissed || c.Outcome == outcomeInterrupted || c.Outcome == outcomeStopped && c.Error != ""
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
// tolerance ago is recorded as missed, and a cell whose ladder would start
// later than that is missed too (runCell). On every start, before it
// labels, records a missed cell or sends, it exits with the stop code
// while a stopped cell or an operator abort or stop marker has no
// acknowledgement file (route.holds); a cell that stopped the route stops
// it even when its evidence then failed to write. An operator abort
// writes its marker at the signal, naming the cell in progress or the
// run the loop waits for (abortHold), or when that cell or the wait ends,
// and stops the route; one that ends the loop with no run left stops it
// before the window close, with no marker. SIGTERM writes no marker and
// ends the loop with exitTerminated before the next label, wait, cell or
// window close, or once the cell in progress ends unless it stopped the
// route. A wait that ends for another cause is an execution error. Each
// cell state finds unlabelled is labelled before the next run, a label
// under way running its commands to their own timeouts after ctx ends,
// as a cell's end status and balance commands do. After the last cell
// the route's window-close record is written. It refuses to start while
// another process holds the route's lock on the ledger
// (admission.Ledger.LockRoute).
func (r *route) loop(ctx context.Context) (int, error) {
	owner, err := r.ledger.LockRoute(r.cfg.Target.Name)
	if err != nil {
		return exitError, err
	}
	defer owner.Close()
	hold := &abortHold{r: r}
	defer hold.watch(ctx)()
	for {
		st, err := r.state()
		if err != nil {
			return exitError, err
		}
		hold.at(st.next, false)
		for _, rec := range st.recovered {
			fmt.Printf("%s %s attempt %d: %s, recorded before the process ended\n", rec.Run.Route, rec.Run.Label, rec.Run.Attempt, cellSummary(rec))
		}
		holds, err := r.holds(st)
		if err != nil {
			return exitError, err
		}
		if len(holds) > 0 {
			for _, h := range holds {
				fmt.Println(h)
			}
			return exitStopped, nil
		}
		for _, rec := range st.unlabelled {
			if terminated(ctx) {
				return r.endTerminated(fmt.Sprintf("before labelling %s attempt %d", rec.Run.Label, rec.Run.Attempt)), nil
			}
			if err := r.label(context.WithoutCancel(ctx), rec); err != nil {
				return exitError, fmt.Errorf("%s %s attempt %d: label: %w", rec.Run.Route, rec.Run.Label, rec.Run.Attempt, err)
			}
			fmt.Printf("%s %s attempt %d: %s, labelled\n", rec.Run.Route, rec.Run.Label, rec.Run.Attempt, rec.Outcome)
		}
		if st.next == nil {
			if terminated(ctx) {
				return r.endTerminated("before the window close"), nil
			}
			if ctx.Err() != nil {
				fmt.Printf("%s: stopped before the window close: %v\n", r.cfg.Target.Name, context.Cause(ctx))
				return exitStopped, nil
			}
			if err := r.close(ctx, st); err != nil {
				if terminated(ctx) {
					fmt.Fprintf(os.Stderr, "%s: window close: %v\n", r.cfg.Target.Name, err)
					return r.endTerminated("during the window close"), nil
				}
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
			if terminated(ctx) {
				return r.endTerminated(fmt.Sprintf("before %s attempt %d", next.Label, next.Attempt)), nil
			}
			if !errors.Is(err, loadgen.ErrOperatorAbort) {
				return exitError, fmt.Errorf("%s: wait for %s attempt %d: %w", next.Route, next.Label, next.Attempt, err)
			}
			fmt.Printf("%s: stopped before %s attempt %d: %v\n", next.Route, next.Label, next.Attempt, err)
			hold.settle(ctx)
			if id, werr := hold.write(next, false, err); werr != nil {
				fmt.Fprintf(os.Stderr, "%s: abort marker not written: %v\n", next.Route, werr)
			} else {
				fmt.Printf("%s: held until %s exists\n", next.Route, r.ackFile(id))
			}
			return exitStopped, nil
		}
		hold.at(&next, true)
		rec, err := r.cell(ctx, next)
		if code, done, err := r.cellEnded(ctx, hold, next, rec, err); done {
			return code, err
		}
	}
}

// cellEnded prints the outcome of next's cell, rec, which ended with
// err, and returns loop's exit code, with done false where the loop goes
// on. From here until the loop names the next run, an abort names none.
// An operator abort during the cell stops the route, held by its marker,
// which is written now where the signal's write failed; SIGTERM ends the
// loop with exitTerminated, or exitStopped where the cell stopped.
func (r *route) cellEnded(ctx context.Context, hold *abortHold, next schedule.Run, rec *cellRecord, err error) (int, bool, error) {
	cause := context.Cause(ctx)
	aborted := errors.Is(cause, loadgen.ErrOperatorAbort)
	hold.at(nil, false)
	if errors.Is(cause, errTerminated) && rec != nil {
		fmt.Printf("%s %s attempt %d: %s\n", next.Route, next.Label, next.Attempt, cellSummary(rec))
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s attempt %d: execution error: %v\n", next.Route, next.Label, next.Attempt, err)
		}
		if rec.Outcome == outcomeStopped {
			return exitStopped, true, nil
		}
		return r.endTerminated(fmt.Sprintf("after %s attempt %d", next.Label, next.Attempt)), true, nil
	}
	if aborted {
		if rec != nil {
			fmt.Printf("%s %s attempt %d: %s\n", next.Route, next.Label, next.Attempt, cellSummary(rec))
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s attempt %d: execution error: %v\n", next.Route, next.Label, next.Attempt, err)
		}
		hold.settle(ctx)
		if id, werr := hold.write(next, true, cause); werr != nil {
			fmt.Fprintf(os.Stderr, "%s: abort marker not written: %v\n", next.Route, werr)
		} else {
			fmt.Printf("%s: held until %s exists\n", next.Route, r.ackFile(id))
		}
		return exitStopped, true, nil
	}
	if err != nil && rec != nil && rec.Outcome == outcomeStopped {
		fmt.Fprintf(os.Stderr, "%s %s attempt %d: %s; execution error: %v\n", next.Route, next.Label, next.Attempt, cellSummary(rec), err)
		return exitStopped, true, nil
	}
	if err != nil {
		return exitError, true, err
	}
	fmt.Printf("%s %s attempt %d: %s\n", next.Route, next.Label, next.Attempt, cellSummary(rec))
	if rec.Outcome == outcomeStopped {
		return exitStopped, true, nil
	}
	return 0, false, nil
}

// endTerminated prints that the process ends on SIGTERM where, with no
// hold, and returns exitTerminated.
func (r *route) endTerminated(where string) int {
	fmt.Printf("%s: ended on SIGTERM %s; no hold, the next start goes on\n", r.cfg.Target.Name, where)
	return exitTerminated
}

// finish prints each cell's final attempt and returns exitOK when every
// cell counts, and exitWindowAdverse when one does not.
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
			code = exitWindowAdverse
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
