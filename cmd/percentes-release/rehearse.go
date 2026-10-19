package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/percentes/percentes/configs"
	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
	"gopkg.in/yaml.v3"
)

// rehearsalFile is the rehearsal's summary at the --out root.
const rehearsalFile = "rehearsal.json"

// rehearsalURL stands in for every documentation URL of a mock route.
const rehearsalURL = "https://mock.invalid/percentes-rehearsal"

// rehearsalNote is the file every record of the mock routes' operator
// file names.
const rehearsalNote = "rehearsal.txt"

// mockForm is one of the release's request forms: a route name, its
// adapter, credential header, URL path and literal body, and whether its
// stream carries reasoning, under reasoningKey on a chat target.
type mockForm struct {
	name, adapter, auth, path, body string
	reasons                         bool
	reasoningKey                    string
}

// chatBody is a chat-completions body (SPEC-HOSTED HA3 point 3) with the
// ceiling under ceilingKey.
func chatBody(ceilingKey, extra string) string {
	return `{"model": "mock-model", "messages": [{"role": "system", "content": "System text."}, {"role": "user", "content": "{{case}}"}],` +
		` "stream": true, "stream_options": {"include_usage": true}, "` + ceilingKey + `": 512, "reasoning_effort": "low"` + extra + `}`
}

// messagesBody is a Messages body (SPEC-HOSTED HA3 point 3).
const messagesBody = `{"model": "mock-model", "system": "System text.", "messages": [{"role": "user", "content": "{{case}}"}],` +
	` "stream": true, "max_tokens": 512, "thinking": {"type": "between_tools"}, "output_config": {"effort": "low"}}`

// releaseForms are the release's six request forms: chat completions
// with either ceiling key and the key as a bearer token or in api-key,
// and Messages at the direct and the Bedrock path with the key in
// x-api-key or as a bearer token.
var releaseForms = []mockForm{
	{"anthropic", config.AdapterMessages, config.AuthXAPIKey, "/v1/messages", messagesBody, true, ""},
	{"azure", config.AdapterChatCompletions, config.AuthAPIKey, "/v1/chat/completions", chatBody("max_completion_tokens", ""), false, ""},
	{"baseten", config.AdapterChatCompletions, config.AuthBearer, "/v1/chat/completions", chatBody("max_tokens", ""), true, "reasoning_content"},
	{"bedrock", config.AdapterMessages, config.AuthBearer, "/anthropic/v1/messages", messagesBody, false, ""},
	{"google", config.AdapterChatCompletions, config.AuthBearer, "/v1/chat/completions", chatBody("max_tokens", ""), false, ""},
	{"groq", config.AdapterChatCompletions, config.AuthBearer, "/v1/chat/completions", chatBody("max_completion_tokens", `, "reasoning_format": "parsed"`), true, "reasoning"},
}

// startMock serves form f on a loopback port: reasoning tokens where the
// form reasons, then answer, paced by ttft and itl, with key accepted in
// the form's credential header alone.
func (f *mockForm) startMock(seed int64, ttft, itl config.LatencyDist, reasoning int, answer, key string) (*mock.Server, error) {
	sh := mock.Shape{Messages: f.adapter == config.AdapterMessages, ReasoningKey: f.reasoningKey, Answer: answer, Key: key, AuthHeader: f.auth}
	if f.reasons {
		sh.ReasoningTokens = reasoning
	}
	s := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: seed, TTFT: ttft, ITL: itl}, sh)
	if err := s.Start(); err != nil {
		return nil, err
	}
	return s, nil
}

// routeConfig is the release reference ref turned into form f's route at
// endpoint, its key in keyEnv, the only host of its model, with limits
// that admit the whole ladder at the 512 ceiling, parsed from bytes that
// hold the route as edited, since a cell's report pair is rendered from
// them.
func (f *mockForm) routeConfig(ref []byte, endpoint, keyEnv string) (*config.Config, error) {
	cfg, err := config.Parse(ref)
	if err != nil {
		return nil, err
	}
	tg := &cfg.Target
	tg.Name, tg.Adapter, tg.AuthHeader, tg.Body, tg.ModelName = f.name, f.adapter, f.auth, f.body, "mock-model"
	tg.EndpointURL, tg.APIKeyEnv = endpoint, keyEnv
	if f.reasoningKey != "" {
		tg.ResponseShape = &config.ResponseShape{Reasoning: []string{f.reasoningKey}}
	}
	for i := range tg.Limits {
		if tg.Limits[i].Dimension == config.LimitTPM {
			tg.Limits[i].Value = 1_000_000
		}
	}
	tg.Naming = &config.Naming{Provider: "Mock " + f.name, Lines: []string{"A mock route served on loopback."}}
	cfg.Release.Caps.RouteUSD = "40"
	cfg.Release.OtherHosts = nil
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	// Marshal writes the empty blocks the release profile refuses.
	delete(doc, "slo")
	delete(doc, "recovery_detector")
	if b, err = yaml.Marshal(doc); err != nil {
		return nil, err
	}
	return config.Parse(b)
}

// mockBalanceCmd prints a balance of cfg's route cap, which covers every
// cell the ledger admits.
func mockBalanceCmd(cfg *config.Config) string {
	return `echo {"amount_usd":"` + cfg.Release.Caps.RouteUSD + `"}`
}

// writeMockRefresh writes path, a mock route's refresh command: it
// touches the file its argument names and prints each of cfg's recorded
// limits at its recorded value.
func writeMockRefresh(path string, cfg *config.Config) error {
	limits := make([]string, len(cfg.Target.Limits))
	for i, l := range cfg.Target.Limits {
		limits[i] = `{"dimension":"` + l.Dimension + `","value":` + strconv.FormatFloat(l.Value, 'f', -1, 64) + `}`
	}
	script := "#!/bin/sh\ntouch \"$1\" && echo '{\"limits\":[" + strings.Join(limits, ",") + "]}'\n"
	return os.WriteFile(path, []byte(script), 0o755)
}

// mockFinished is the count of finished requests by outcome label that
// the mock at addr reports on its metrics endpoint.
func mockFinished(addr string) (map[string]int, error) {
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Get("http://" + addr + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	for _, line := range strings.Split(string(b), "\n") {
		rest, ok := strings.CutPrefix(line, `percentes_mock_requests_total{outcome="`)
		if !ok {
			continue
		}
		label, value, _ := strings.Cut(rest, `"} `)
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("mock metrics line %q: %w", line, err)
		}
		out[label] = int(v)
	}
	return out, nil
}

// The rehearsal's mock streams at full length: the first event after 0.3
// to 1.5 s, then reasoning and answer events 10 to 40 ms apart.
var (
	rehearsalTTFT = config.LatencyDist{Distribution: "uniform", MinMs: 300, MaxMs: 1500}
	rehearsalITL  = config.LatencyDist{Distribution: "uniform", MinMs: 10, MaxMs: 40}
)

// rehearsalReasoning is the reasoning tokens of a form that reasons;
// rehearsalAnswer is streamed in 4-rune content events.
const rehearsalReasoning = 200

var rehearsalAnswer = strings.Repeat("The customer reports a duplicate charge and asks for a refund. ", 8)

// rehearsalSettings are what a rehearsal runs at: the ladder, the
// client-validity CPU window, one minute of a route's offset, and the
// mocks' streams.
type rehearsalSettings struct {
	ladder     config.Ladder
	cpuWindowS int
	minute     time.Duration
	ttft, itl  config.LatencyDist
	reasoning  int
	answer     string
}

// fullLength reports whether s is the pinned ladder at real offsets.
func (s *rehearsalSettings) fullLength() bool {
	l := s.ladder
	return slices.Equal(l.StepsRPS, config.PinnedReleaseStepsRPS) && l.WarmupS == config.PinnedReleaseWarmupS && l.StepS == config.PinnedReleaseStepS &&
		l.DrainS == config.PinnedReleaseDrainS && s.cpuWindowS == config.PinnedCPUWindowS && s.minute == time.Minute
}

// settingsAt is the pinned ladder and the full-length mock streams with
// every duration but the CPU window divided by scale; tests replace it.
var settingsAt = func(scale int) rehearsalSettings {
	k := float64(scale)
	div := func(d config.LatencyDist) config.LatencyDist {
		d.MinMs, d.MaxMs = d.MinMs/k, d.MaxMs/k
		return d
	}
	return rehearsalSettings{
		ladder: config.Ladder{StepsRPS: slices.Clone(config.PinnedReleaseStepsRPS), WarmupS: config.PinnedReleaseWarmupS / k,
			StepS: config.PinnedReleaseStepS / k, DrainS: config.PinnedReleaseDrainS / k},
		cpuWindowS: config.PinnedCPUWindowS,
		minute:     time.Minute / time.Duration(scale),
		ttft:       div(rehearsalTTFT),
		itl:        div(rehearsalITL),
		reasoning:  rehearsalReasoning,
		answer:     rehearsalAnswer,
	}
}

// rehearsal is the summary file: what ran, each route's cell, and
// whether the rehearsal is green. Green needs every cell counted with
// G2 passing at each of the five steps, no tripwire, the completeness
// check passed, and each mock's answers equal to the requests its cell
// and ceiling check sent, all 200.
type rehearsal struct {
	SchemaVersion int  `json:"schema_version"`
	Green         bool `json:"green"`
	// FullLength is true for the pinned ladder at real offsets, the only
	// form whose summary is the acceptance run's evidence file.
	FullLength bool     `json:"full_length"`
	Scale      int      `json:"scale"`
	Reasons    []string `json:"reasons,omitempty"`

	Ladder          config.Ladder      `json:"ladder"`
	CPUWindowS      int                `json:"cpu_window_s"`
	OffsetMinuteS   float64            `json:"offset_minute_s"`
	MockTTFT        config.LatencyDist `json:"mock_ttft"`
	MockITL         config.LatencyDist `json:"mock_itl"`
	MockReasoning   int                `json:"mock_reasoning_tokens"`
	MockAnswerRunes int                `json:"mock_answer_runes"`
	// CeilingProbes says how each route's ceiling check record was made.
	CeilingProbes string `json:"ceiling_probes"`

	MasterSeed string          `json:"master_seed"`
	Cell       string          `json:"cell"`
	Source     evidence.Source `json:"source"`
	Host       rehearsalHost   `json:"host"`
	ClockSync  []string        `json:"clock_sync"`
	StartUTC   time.Time       `json:"start_utc"`
	EndUTC     time.Time       `json:"end_utc"`

	Routes   []rehearsalRoute `json:"routes"`
	Dispatch *aggregateSeries `json:"dispatch,omitempty"`
}

type rehearsalHost struct {
	Name           string `json:"name"`
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	CPUs           int    `json:"cpus"`
	OpenFilesLimit uint64 `json:"open_files_limit"`
}

// rehearsalRoute is one route's cell. Dir is the cell's directory under
// --out; Completeness is "pass" or the completeness check's refusal.
type rehearsalRoute struct {
	Route         string             `json:"route"`
	Adapter       string             `json:"adapter"`
	AuthHeader    string             `json:"auth_header"`
	Path          string             `json:"path"`
	OffsetMinutes int                `json:"offset_minutes"`
	Run           schedule.Run       `json:"run"`
	Dir           string             `json:"dir,omitempty"`
	StartUTC      *time.Time         `json:"start_utc,omitempty"`
	EndUTC        *time.Time         `json:"end_utc,omitempty"`
	Outcome       string             `json:"outcome,omitempty"`
	Reasons       []string           `json:"reasons,omitempty"`
	Error         string             `json:"error,omitempty"`
	LadderEnd     string             `json:"ladder_end,omitempty"`
	Steps         []rehearsalStep    `json:"steps,omitempty"`
	Tripwires     *loadgen.Tripwires `json:"tripwires,omitempty"`
	Sent          admission.Counts   `json:"sent"`
	CeilingCheck  probe.CeilingCheck `json:"ceiling_check"`
	Concurrent    int                `json:"concurrent_runs"`
	MockFinished  map[string]int     `json:"mock_finished,omitempty"`
	Completeness  string             `json:"completeness,omitempty"`
	Manifest      *evidence.FileRef  `json:"manifest,omitempty"`
	Green         bool               `json:"green"`
	Problems      []string           `json:"problems,omitempty"`
}

// rehearsalStep is one ladder step with its G2 verdict.
type rehearsalStep struct {
	RateRPS   float64             `json:"rate_rps"`
	Scheduled int                 `json:"scheduled"`
	Attempted bool                `json:"attempted"`
	G2        *loadgen.GateReport `json:"g2,omitempty"`
}

// rehearsalInput is a rehearse command after its flags are read.
type rehearsalInput struct {
	out, frames, clockSync, masterSeed, sourceDiff string
	scale, byteCap                                 int
	build                                          *debug.BuildInfo
}

// rehearseCmd reads the rehearsal's flags and runs it.
func rehearseCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("rehearse", flag.ContinueOnError)
	out := fs.String("out", "", "new or empty output directory for the inputs, the cells' evidence and "+rehearsalFile+" (required)")
	frames := fs.String("frames", "", "raw-frame store directory outside the source tree (required)")
	clockSync := fs.String("clock-sync", "", "host clock-sync command run before each cell, as for run (required)")
	byteCap := fs.Int("byte-cap", 0, "Shape S ticket byte cap (required)")
	scale := fs.Int("scale", 1, "divide the ladder's durations, the offsets and the mocks' stream times by this; 1 is full length")
	seed := fs.String("master-seed", "percentes-rehearsal", "master seed of the rehearsal's schedule, which draws the offsets")
	diff := fs.String("source-diff", "", "the working tree's diff against the build's revision, required for a build from a modified tree")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	for name, v := range map[string]string{"--out": *out, "--frames": *frames, "--clock-sync": *clockSync} {
		if v == "" {
			return exitError, fmt.Errorf("rehearse: %s is required", name)
		}
	}
	build, _ := debug.ReadBuildInfo()
	return rehearse(ctx, rehearsalInput{out: *out, frames: *frames, clockSync: *clockSync, masterSeed: *seed, sourceDiff: *diff,
		scale: *scale, byteCap: *byteCap, build: build})
}

// rehearse runs one cell of each release form against its own mock in
// this process, concurrently, each starting at its pinned offset, and
// writes the summary. Its schedule's window opens on the current UTC
// date; every route runs the schedule's first cell. A flag refused
// before --out is created leaves nothing written.
func rehearse(ctx context.Context, in rehearsalInput) (int, error) {
	if in.scale < 1 {
		return exitError, fmt.Errorf("rehearse: --scale %d: want 1 or more", in.scale)
	}
	set := settingsAt(in.scale)
	if set.ladder.StepS < float64(set.cpuWindowS) {
		return exitError, fmt.Errorf("rehearse: --scale %d: a %g s step is shorter than the %d s CPU window", in.scale, set.ladder.StepS, set.cpuWindowS)
	}
	if in.byteCap <= 0 {
		return exitError, fmt.Errorf("rehearse: --byte-cap %d: the Shape S ticket byte cap is required", in.byteCap)
	}
	if in.masterSeed == "" {
		return exitError, errors.New("rehearse: --master-seed is empty")
	}
	if err := evidence.CheckFramesRoot(in.frames); err != nil {
		return exitError, err
	}
	if _, err := evidence.ParseClockCommand(in.clockSync); err != nil {
		return exitError, err
	}
	if strings.ContainsFunc(in.out, unicode.IsSpace) {
		return exitError, fmt.Errorf("rehearse: --out %q holds white space, which the mock routes' refresh command, split on spaces, cannot carry", in.out)
	}
	if _, err := checkOpenFiles(); err != nil {
		return exitError, err
	}
	switch src := evidence.SourceOf(in.build); {
	case src.Revision == "":
		return exitError, errors.New("rehearse: the binary carries no version-control revision (build it inside a git checkout)")
	case src.Modified && in.sourceDiff == "":
		return exitError, fmt.Errorf("rehearse: the binary was built from a modified tree at %s; --source-diff is required", src.Revision)
	}
	if err := newDir(in.out); err != nil {
		return exitError, err
	}
	inputs := filepath.Join(in.out, "inputs")
	if err := os.MkdirAll(inputs, 0o755); err != nil {
		return exitError, err
	}
	now := time.Now().UTC()
	today := now.Format(time.DateOnly)
	opPath, err := writeRehearsalOperator(inputs, today, in.sourceDiff)
	if err != nil {
		return exitError, err
	}
	note := filepath.Join(inputs, rehearsalNote)

	n := len(releaseForms)
	mocks := make([]*mock.Server, 0, n)
	var envs []string
	defer func() {
		for _, s := range mocks {
			s.Close()
		}
		for _, env := range envs {
			os.Unsetenv(env)
		}
	}()
	cfgs := make([]*config.Config, n)
	names := make([]string, n)
	for i := range releaseForms {
		f := &releaseForms[i]
		key, err := randomKey()
		if err != nil {
			return exitError, err
		}
		s, err := f.startMock(int64(i+1), set.ttft, set.itl, set.reasoning, set.answer, key)
		if err != nil {
			return exitError, err
		}
		mocks = append(mocks, s)
		env := "PERCENTES_REHEARSAL_KEY_" + strings.ToUpper(f.name)
		envs = append(envs, env)
		if err := os.Setenv(env, key); err != nil {
			return exitError, err
		}
		if cfgs[i], err = f.routeConfig(configs.ReleaseReference, "http://"+s.Addr()+f.path, env); err != nil {
			return exitError, fmt.Errorf("rehearse: %s: %w", f.name, err)
		}
		names[i] = f.name
	}

	ledgerPath := filepath.Join(inputs, "ledger.jsonl")
	caps, err := admission.ReleaseCaps(cfgs...)
	if err != nil {
		return exitError, err
	}
	l, err := admission.Create(ledgerPath, caps)
	if err != nil {
		return exitError, err
	}
	st, err := l.State()
	if err != nil {
		return exitError, err
	}
	l.Bind(st.OpenSHA256)
	// The rehearsal's release key is never written.
	key, err := releasekey.New()
	if err != nil {
		return exitError, err
	}
	frozen := make([]schedule.RouteConfig, n)
	for i, cfg := range cfgs {
		if frozen[i], err = frozenConfig(cfg, key); err != nil {
			return exitError, err
		}
	}
	sched, err := schedule.New(schedule.Input{MasterSeed: in.masterSeed, First: today, Last: now.AddDate(0, 0, 9).Format(time.DateOnly), Routes: names, Configs: frozen,
		LedgerSHA256: st.OpenSHA256})
	if err != nil {
		return exitError, err
	}
	sb, err := sched.JSON()
	if err != nil {
		return exitError, err
	}
	schedulePath := filepath.Join(inputs, "schedule.json")
	if err := os.WriteFile(schedulePath, sb, 0o644); err != nil {
		return exitError, err
	}

	routes := make([]*route, n)
	runs := make([]schedule.Run, n)
	label := sched.Cells[0].Label
	for _, cfg := range cfgs {
		if _, err := recordPrice(l, cfg, "0.15", "0.60", rehearsalURL, today); err != nil {
			return exitError, err
		}
	}
	shape, err := pilot.GenerateShapeS(pilot.ShapeSSeed, in.byteCap)
	if err != nil {
		return exitError, err
	}
	checks := make([]*probe.ReleaseRecord, n)
	checkErrs := make([]error, n)
	var cwg sync.WaitGroup
	for i, cfg := range cfgs {
		cwg.Add(1)
		go func(i int, cfg *config.Config) {
			defer cwg.Done()
			checks[i], checkErrs[i] = sendCeiling(ctx, l, cfg, shape, "", in.byteCap, key)
		}(i, cfg)
	}
	cwg.Wait()
	if err := errors.Join(checkErrs...); err != nil {
		return exitError, fmt.Errorf("rehearse: ceiling check: %w", err)
	}
	for i, cfg := range cfgs {
		name := cfg.Target.Name
		cp, err := json.Marshal(checks[i])
		if err != nil {
			return exitError, err
		}
		probePath := filepath.Join(inputs, "ceiling-"+name+".json")
		if err := os.WriteFile(probePath, cp, 0o644); err != nil {
			return exitError, err
		}
		refreshPath := filepath.Join(inputs, "refresh-"+name)
		if err := writeMockRefresh(refreshPath, cfg); err != nil {
			return exitError, err
		}
		r, err := newRoute(routeInput{
			cfg: cfg, configName: name + ".yaml", schedulePath: schedulePath, out: in.out, ledgerPath: ledgerPath, key: key,
			framesRoot: in.frames, clockSync: in.clockSync, statusCmd: "true", balanceCmd: mockBalanceCmd(cfg), refreshCmd: refreshPath + " " + note, opPath: opPath,
			probeFiles: []string{probePath}, byteCap: in.byteCap, accountCreated: today, build: in.build,
		})
		if err != nil {
			return exitError, err
		}
		if got := r.verdict.Admitted(); !slices.Equal(got, config.PinnedReleaseStepsRPS) {
			return exitError, fmt.Errorf("rehearse: %s: the ceiling rule admits %v, not the whole ladder", name, got)
		}
		// The ceiling rule has read the pinned ladder; the cells run at the
		// rehearsal's.
		r.cfg.Release.Ladder = set.ladder
		r.cfg.ClientValidity.CPUWindowS = set.cpuWindowS
		k := slices.IndexFunc(r.runs, func(run schedule.Run) bool { return run.Label == label })
		if k < 0 {
			return exitError, fmt.Errorf("rehearse: %s: no run of cell %s", name, label)
		}
		routes[i], runs[i] = r, r.runs[k]
	}

	start := time.Now().Add(time.Second)
	cellS := set.ladder.WarmupS + float64(len(set.ladder.StepsRPS))*(set.ladder.StepS+set.ladder.DrainS)
	last := 0
	for _, r := range routes {
		last = max(last, r.offsetMinutes)
		fmt.Printf("rehearse: %s (%s, %s) at offset %d min, from %s UTC\n", r.cfg.Target.Name, r.cfg.Target.Adapter, r.cfg.Target.AuthHeader,
			r.offsetMinutes, start.Add(time.Duration(r.offsetMinutes)*set.minute).UTC().Format(time.TimeOnly))
	}
	fmt.Printf("rehearse: cell %s, %g s each; the last ends about %s UTC\n", label, cellS,
		start.Add(time.Duration(last)*set.minute+time.Duration(cellS*float64(time.Second))).UTC().Format(time.TimeOnly))

	recs := make([]*cellRecord, n)
	errs := make([]error, n)
	notStarted := make([]error, n)
	var wg sync.WaitGroup
	for i, r := range routes {
		wg.Add(1)
		go func(i int, r *route) {
			defer wg.Done()
			if notStarted[i] = waitUntil(ctx, start.Add(time.Duration(r.offsetMinutes)*set.minute)); notStarted[i] != nil {
				return
			}
			recs[i], errs[i] = r.runCell(ctx, runs[i])
			if errs[i] == nil {
				fmt.Printf("rehearse: %s %s: %s\n", r.cfg.Target.Name, label, cellSummary(recs[i]))
			}
		}(i, r)
	}
	wg.Wait()

	sum := &rehearsal{
		SchemaVersion: 1, FullLength: set.fullLength(), Scale: in.scale,
		Ladder: set.ladder, CPUWindowS: set.cpuWindowS, OffsetMinuteS: set.minute.Seconds(),
		MockTTFT: set.ttft, MockITL: set.itl, MockReasoning: set.reasoning, MockAnswerRunes: len([]rune(set.answer)),
		CeilingProbes: "each route's ceiling check, sent to its mock at the configured ceiling before the cells",
		MasterSeed:    in.masterSeed, Cell: label, Source: evidence.SourceOf(in.build),
		Host:      rehearsalHost{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), OpenFilesLimit: routes[0].openFiles},
		ClockSync: routes[0].clock.Args, StartUTC: now,
	}
	sum.Host.Name, _ = os.Hostname()
	sum.Green = true
	for i, r := range routes {
		rr := r.rehearsed(&releaseForms[i], runs[i], recs[i], checks[i], mocks[i].Addr(), in.frames)
		switch {
		case notStarted[i] != nil:
			rr.Problems = append(rr.Problems, "not started: "+notStarted[i].Error())
		case errs[i] != nil:
			rr.Error = errs[i].Error()
			rr.Problems = append(rr.Problems, "execution error: "+rr.Error)
		}
		rr.Green = len(rr.Problems) == 0
		for _, p := range rr.Problems {
			sum.Reasons = append(sum.Reasons, r.cfg.Target.Name+": "+p)
		}
		sum.Green = sum.Green && rr.Green
		sum.Routes = append(sum.Routes, rr)
	}
	if sum.Dispatch, err = aggregate(in.out); err != nil {
		sum.Green = false
		sum.Reasons = append(sum.Reasons, "aggregate dispatch series: "+err.Error())
	}
	sum.EndUTC = time.Now().UTC()
	path := filepath.Join(in.out, rehearsalFile)
	if err := replaceJSON(path, sum); err != nil {
		return exitError, err
	}

	verdict := "green"
	if !sum.Green {
		verdict = "not green"
	}
	if !sum.FullLength {
		verdict += ", shortened (scale " + strconv.Itoa(in.scale) + ")"
	}
	for _, reason := range sum.Reasons {
		fmt.Printf("rehearse: %s\n", reason)
	}
	fmt.Printf("rehearse: %s; written %s\n", verdict, path)
	if err := errors.Join(errs...); err != nil {
		return exitError, err
	}
	if !sum.Green {
		return exitAdverse, nil
	}
	return exitOK, nil
}

// rehearsed is the summary of route r's ceiling check and rehearsal
// cell, with the problems that keep it from green. rec is nil for a cell
// never run.
func (r *route) rehearsed(f *mockForm, run schedule.Run, rec *cellRecord, check *probe.ReleaseRecord, mockAddr, framesRoot string) rehearsalRoute {
	rr := rehearsalRoute{Route: f.name, Adapter: f.adapter, AuthHeader: f.auth, Path: f.path, OffsetMinutes: r.offsetMinutes, Run: run, CeilingCheck: check.Ceiling}
	if rec == nil {
		return rr
	}
	problem := func(format string, a ...any) { rr.Problems = append(rr.Problems, fmt.Sprintf(format, a...)) }
	dir := r.cellDir(run)
	if rel, err := filepath.Rel(r.out, dir); err == nil {
		rr.Dir = filepath.ToSlash(rel)
	}
	rr.StartUTC, rr.EndUTC = &rec.StartUTC, rec.EndUTC
	rr.Outcome, rr.Reasons = rec.Outcome, rec.Reasons
	rr.Tripwires, rr.Sent, rr.Concurrent = rec.Tripwires, rec.Sent, len(rec.Concurrent)
	if rec.Outcome != outcomeCounted {
		problem("outcome %s: %s", rec.Outcome, strings.Join(rec.Reasons, "; "))
	}
	if l := rec.Ladder; l != nil {
		rr.LadderEnd = l.End
		if l.End != "" {
			problem("ladder ended at step %d: %s", l.EndStep, l.End)
		}
		for k, st := range l.Steps {
			rr.Steps = append(rr.Steps, rehearsalStep{RateRPS: st.RateRPS, Scheduled: st.Scheduled, Attempted: st.Attempted, G2: st.Gates})
			switch {
			case !st.Attempted:
				problem("step %d not attempted", k+1)
			case st.Gates == nil || !st.Gates.Pass:
				problem("step %d: G2 did not pass", k+1)
			}
		}
	}
	if len(rr.Steps) != len(config.PinnedReleaseStepsRPS) {
		problem("%d ladder steps, want %d", len(rr.Steps), len(config.PinnedReleaseStepsRPS))
	}
	if tw := rec.Tripwires; tw == nil || tw.Fired {
		problem("tripwires %+v", tw)
	}
	sent := rec.Sent.Probe + rec.Sent.Warmup + rec.Sent.Measured
	if c, err := mockFinished(mockAddr); err != nil {
		problem("mock metrics: %v", err)
	} else if rr.MockFinished = c; len(c) != 1 || c["200"] != sent+check.Sent() {
		problem("the mock finished %v; the ceiling check sent %d and the cell %d", c, check.Sent(), sent)
	}
	rr.Completeness = "pass"
	if _, err := evidence.CheckRelease(evidence.Release{Dir: dir, Config: r.cfg.Raw, FramesRoot: framesRoot, Pair: [2]string{report.JSONFile, report.TextFile}}); err != nil {
		rr.Completeness = err.Error()
		problem("completeness check: %v", err)
	}
	if ref, err := evidence.HashFile(filepath.Join(dir, evidence.ManifestFile), evidence.ManifestFile); err != nil {
		problem("manifest: %v", err)
	} else {
		rr.Manifest = &ref
	}
	return rr
}

// newDir creates dir, or accepts it when it exists and is empty.
func newDir(dir string) error {
	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return os.MkdirAll(dir, 0o755)
	case err != nil:
		return err
	case len(entries) > 0:
		return fmt.Errorf("rehearse: --out %s is not empty", dir)
	}
	return nil
}

// writeRehearsalOperator writes the mock routes' operator file into dir,
// every required record naming one note file, with the source diff
// copied beside it when one is given, and returns the file's path.
func writeRehearsalOperator(dir, today, sourceDiff string) (string, error) {
	if err := os.WriteFile(filepath.Join(dir, rehearsalNote), []byte("Percentes rehearsal: mock routes served on loopback by the runner's own process. This file stands in for every provider record.\n"), 0o644); err != nil {
		return "", err
	}
	type record struct {
		Kind      string `json:"kind"`
		Path      string `json:"path"`
		URL       string `json:"url,omitempty"`
		Retrieved string `json:"retrieved"`
	}
	var recs []record
	for _, k := range []string{evidence.RecordLimits, evidence.RecordPrices, evidence.RecordQuota, evidence.RecordTerms, evidence.RecordStatusMapping,
		evidence.RecordEligibility, evidence.RecordDedicatedAccount, evidence.RecordAffiliation} {
		recs = append(recs, record{Kind: k, Path: rehearsalNote, URL: rehearsalURL, Retrieved: today})
	}
	if sourceDiff != "" {
		b, err := os.ReadFile(sourceDiff)
		if err != nil {
			return "", fmt.Errorf("rehearse: --source-diff: %w", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "source.diff"), b, 0o644); err != nil {
			return "", err
		}
		recs = append(recs, record{Kind: evidence.RecordSourceDiff, Path: "source.diff", Retrieved: today})
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "unnamed host"
	}
	b, err := json.MarshalIndent(map[string]any{"provider": "Mock routes", "account_tier": "mock", "client_host": host, "records": recs}, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "operator.json")
	return path, os.WriteFile(path, append(b, '\n'), 0o644)
}

// randomKey is a fresh key for a mock route.
func randomKey() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "rehearsal-" + hex.EncodeToString(b), nil
}
