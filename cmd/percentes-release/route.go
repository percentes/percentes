package main

import (
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
	"github.com/percentes/percentes/internal/schedule"
)

// cellFile is a cell's run record in its directory.
const cellFile = "cell.json"

// Cell outcomes. A cell counts when its ladder ran to the top admitted
// step's drain or the backoff rule ended it; every other finished outcome
// takes a re-run within the route's bound.
const (
	outcomeRunning     = "running"
	outcomeCounted     = evidence.CellCounted
	outcomeInvalid     = evidence.CellInvalid
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
	Outcome       string       `json:"outcome"`
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
	// rule is the ceiling rule's input behind verdict, which limitsStop
	// applies again at each cell's start.
	rule ceilingInput

	ledger     *admission.Ledger
	ledgerPath string
	framesRoot string
	clock      evidence.ClockCommand
	status     []string
	balance    []string
	refresh    []string
	// opPath is the operator file each cell reads again; op is the file
	// as read when the route started.
	opPath string
	op     *evidence.Operator
	build  *debug.BuildInfo

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
	ledgerPath := fs.String("ledger", "", "the release admission ledger, holding the route's price (required)")
	schedulePath := fs.String("schedule", "", "the committed schedule file (required)")
	out := fs.String("out", "", "release output directory; the route writes <route>/<cell>-a<attempt>/ under it (required)")
	framesRoot := fs.String("frames", "", "raw-frame store directory outside the source tree (required)")
	clockSync := fs.String("clock-sync", "", "host clock-sync command run before each cell, split on spaces with no shell (required)")
	statusCmd := fs.String("status-cmd", "", "endpoint-status command run at each cell's start and end, split on spaces with no shell (required)")
	balanceCmd := fs.String("balance-cmd", "", "account balance or quota command run at each cell's start and end, split on spaces with no shell, printing {\"amount_usd\": \"<decimal>\"} or {\"unavailable\": \"<reason>\"} (required)")
	refreshCmd := fs.String("refresh-cmd", "", "operator-records refresh command run at each cell's start and once after the last cell, split on spaces with no shell, rewriting the files of the operator file's limits and eligibility records and printing {\"limits\": [...]}, each target.limits entry's current value or why it is unavailable (required)")
	opPath := fs.String("operator", "", "the route's operator file, read again at each cell's start (required)")
	var probeFiles listFlag
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
	build, _ := debug.ReadBuildInfo()
	r, err := newRoute(routeInput{
		cfg: cfg, configName: filepath.Base(*cfgPath), schedulePath: *schedulePath, out: *out, ledgerPath: *ledgerPath,
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

// ceilingInput is the ceiling rule's input: the configuration as the
// route started, before a test or the rehearsal scales its ladder, the
// route's price and the plan at the schedule's exact counts.
type ceilingInput struct {
	cfg   *config.Config
	price admission.Price
	plan  admission.Plan
}

// routeInput is a run command after its flags are read.
type routeInput struct {
	cfg                                       *config.Config
	configName, schedulePath, out, ledgerPath string
	framesRoot, clockSync, statusCmd, opPath  string
	balanceCmd, refreshCmd                    string
	probeFiles                                []string
	byteCap                                   int
	accountCreated                            string
	build                                     *debug.BuildInfo
}

// newRoute checks the route's inputs and fixes what every cell shares:
// the ceiling rule's verdict at the schedule's exact counts, the ceiling
// choice, the Shape S set and the account record.
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
	r := &route{cfg: cfg, configName: in.configName, sched: sched, runs: runs, out: in.out, dir: filepath.Join(in.out, name),
		ledgerPath: in.ledgerPath, framesRoot: in.framesRoot, opPath: in.opPath, build: in.build, now: time.Now, wait: waitUntil}
	r.cell, r.label, r.close = r.runCell, r.labelCell, r.windowClose
	for _, o := range sched.Offsets {
		if o.Route == name {
			r.offsetMinutes = o.Minutes
		}
	}

	if r.op, err = evidence.LoadOperator(in.opPath); err != nil {
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
	if err := st.CheckRelease(cfg); err != nil {
		return nil, err
	}
	entries, err := ledgerEntries(in.ledgerPath)
	if err != nil {
		return nil, err
	}
	price, err := routePrice(entries, name)
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
	r.rule = ceilingInput{cfg: &rc, price: price, plan: plan}
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
	if r.ceiling, r.ceilingProbes, err = readCeilingProbes(cfg, in.probeFiles, in.byteCap); err != nil {
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

// readCeilingProbes reads the ceiling check's records of the route's
// model, whose hosts are the target and release.other_hosts, and requires
// their choice to run the model at the configured ceiling. Every host's
// record carries the host's model and byteCap; this route's also carry
// its adapter and endpoint and none of the values the route withholds
// (probe.Exposed), and the one at the configured ceiling its body's
// hash.
func readCeilingProbes(cfg *config.Config, files []string, byteCap int) (probe.CeilingChoice, map[string][]byte, error) {
	var none probe.CeilingChoice
	if len(files) == 0 {
		return none, nil, errors.New("run: --ceiling-probe is required: the ceiling check's probe records")
	}
	t := &cfg.Target
	hosts, models := []string{t.Name}, map[string]string{t.Name: t.ModelName}
	for _, h := range cfg.Release.OtherHosts {
		hosts, models[h.Route] = append(hosts, h.Route), h.Model
	}
	sum := sha256.Sum256([]byte(t.Body))
	body := hex.EncodeToString(sum[:])
	var recs []*probe.ReleaseRecord
	copies := map[string][]byte{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return none, nil, fmt.Errorf("ceiling probe: %w", err)
		}
		var rec probe.ReleaseRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return none, nil, fmt.Errorf("ceiling probe %s: %w", f, err)
		}
		var fault string
		model, host := models[rec.Route]
		switch {
		case !host:
		case rec.RequestedModel != model:
			fault = fmt.Sprintf("model %q, want %q", rec.RequestedModel, model)
		case rec.ByteCap != byteCap:
			fault = fmt.Sprintf("byte cap %d, want --byte-cap %d", rec.ByteCap, byteCap)
		case rec.Route != t.Name:
		case rec.Adapter != t.Adapter:
			fault = fmt.Sprintf("adapter %q, want %q", rec.Adapter, t.Adapter)
		case rec.EndpointURL != redact.URL(t.EndpointURL):
			fault = fmt.Sprintf("endpoint %s, want %s", rec.EndpointURL, redact.URL(t.EndpointURL))
		case rec.Ceiling.Ceiling == cfg.Release.Shape.CeilingTokens && rec.BodySHA256 != body:
			fault = fmt.Sprintf("body sha256 %s, want target.body's %s", rec.BodySHA256, body)
		default:
			if exposed := probe.Exposed(t, &rec); len(exposed) > 0 {
				fault = "carries values the route withholds: " + strings.Join(exposed, ", ")
			}
		}
		if fault != "" {
			return none, nil, fmt.Errorf("ceiling probe %s: route %s at %d: %s", f, rec.Route, rec.Ceiling.Ceiling, fault)
		}
		recs = append(recs, &rec)
		copies[fmt.Sprintf("ceiling-probe-%s-%d.json", rec.Route, rec.Ceiling.Ceiling)] = b
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

// state reads the route's cell records. A record left running by an
// ended process becomes interrupted. A re-run is taken, in order of the
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
	return &cellRecord{SchemaVersion: 1, Run: run, OffsetMinutes: r.offsetMinutes, StartUTC: r.now().UTC(),
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
