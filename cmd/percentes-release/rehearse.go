package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
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

// rehearsalHost is the address every mock serves on and its certificate
// names.
const rehearsalHost = "127.0.0.1"

// checksDir is the check-only step's directory under --out.
const checksDir = "checks"

// mockForm is one provider's request form, served by one mock: its name,
// adapter, credential header, URL path and ceiling key, and the key a
// chat stream carries reasoning under, empty for none.
type mockForm struct {
	name, adapter, auth, path, ceilingKey string
	reasoningKey                          string
}

// body is the form's literal request body at ceiling (SPEC-HOSTED HA3
// point 3): a chat-completions body, or a Messages body.
func (f *mockForm) body(ceiling int) string {
	if f.adapter == config.AdapterMessages {
		return fmt.Sprintf(`{"model": "mock-model", "system": "System text.", "messages": [{"role": "user", "content": "{{case}}"}],`+
			` "stream": true, "max_tokens": %d, "thinking": {"type": "between_tools"}, "output_config": {"effort": "low"}}`, ceiling)
	}
	return fmt.Sprintf(`{"model": "mock-model", "messages": [{"role": "system", "content": "System text."}, {"role": "user", "content": "{{case}}"}],`+
		` "stream": true, "stream_options": {"include_usage": true}, "%s": %d, "reasoning_effort": "low"}`, f.ceilingKey, ceiling)
}

// rehearsalForms are the release's four request forms: Messages with the
// key in x-api-key, and chat completions with either ceiling key and the
// key in api-key or as a bearer token.
var rehearsalForms = []mockForm{
	{"anthropic", config.AdapterMessages, config.AuthXAPIKey, "/v1/messages", "max_tokens", ""},
	{"azure", config.AdapterChatCompletions, config.AuthAPIKey, "/v1/chat/completions", "max_completion_tokens", "reasoning_content"},
	{"baseten", config.AdapterChatCompletions, config.AuthBearer, "/v1/chat/completions", "max_tokens", "reasoning_content"},
	{"gemini", config.AdapterChatCompletions, config.AuthBearer, "/v1/chat/completions", "max_tokens", ""},
}

func formOf(name string) *mockForm {
	for i := range rehearsalForms {
		if rehearsalForms[i].name == name {
			return &rehearsalForms[i]
		}
	}
	panic("rehearse: no request form " + name)
}

// mockRoute is one release route as the rehearsal runs it: its name, the
// form of the mock that serves it, its request shape and declared
// fallback, account group, route cap and list prices in US dollars per
// million tokens in and out, and the limits its configuration records.
// Under them the binding-rate rule gives the release route's binding
// rate at the ceiling the route's check chooses, ceiling: rps over
// binding in its window windowS, where rps is set; where it is 0 the
// schedule's draw decides.
type mockRoute struct {
	name, form, shape string
	fallback          *bool
	group, routeUSD   string
	priceIn, priceOut string
	limits            func() []config.Limit
	ceiling           int
	binding           string
	windowS, rps      float64
}

func declares(b bool) *bool { return &b }

// rehearsalRoutes are the release's five routes: anthropic, azure and
// azure-R on one deployment, gemini and baseten, with the release's
// limits, caps and prices.
var rehearsalRoutes = []mockRoute{
	{name: "anthropic", form: "anthropic", shape: config.ShapeR, fallback: declares(false), group: "anthropic", routeUSD: "860", priceIn: "2", priceOut: "10",
		limits: func() []config.Limit {
			return []config.Limit{
				{Dimension: config.LimitRPM, Value: 10_000, EvaluationWindowsS: []float64{1, 60}},
				{Dimension: config.LimitInputTPM, Value: 5_000_000},
				{Dimension: config.LimitOutputTPM, Value: 1_000_000, OutputAtGeneration: true},
				{Dimension: config.LimitSpendMonth, Value: 860, OperatorSet: true},
				{Dimension: config.LimitReloadThreshold, Value: 60, OperatorSet: true},
			}
		}, ceiling: 16, binding: config.LimitRPM, windowS: 1, rps: 150.909},
	{name: "azure", form: "azure", shape: config.ShapeS, group: "azure", routeUSD: "280", priceIn: "2", priceOut: "10", limits: azureLimits,
		ceiling: 512, binding: config.LimitTPM, windowS: 60, rps: 0.972},
	{name: "azure-R", form: "azure", shape: config.ShapeR, fallback: declares(true), group: "azure", routeUSD: "120", priceIn: "2", priceOut: "10", limits: azureLimits,
		ceiling: 256, binding: config.LimitRPM, windowS: 1, rps: 3.636},
	{name: "gemini", form: "gemini", shape: config.ShapeS, group: "gemini", routeUSD: "132.03", priceIn: "0.75", priceOut: "3.75",
		limits: func() []config.Limit {
			return []config.Limit{
				{Dimension: config.LimitSpendWindow, Value: 10, WindowS: 600, OutputAtGeneration: true},
				{Dimension: config.LimitRPM, Value: 1000},
				{Dimension: config.LimitInputTPM, Value: 2_000_000},
				{Dimension: config.LimitRPD, Value: 10_000, ResetZone: "America/Los_Angeles"},
				{Dimension: config.LimitSpendMonth, Value: 200.52, OperatorSet: true},
				{Dimension: config.LimitReloadThreshold, Value: 10.69, OperatorSet: true},
			}
		}, ceiling: 512},
	{name: "baseten", form: "baseten", shape: config.ShapeS, group: "baseten", routeUSD: "25", priceIn: "0.10", priceOut: "0.50",
		limits: func() []config.Limit {
			return []config.Limit{
				{Dimension: config.LimitRPM, Value: 15},
				{Dimension: config.LimitTPM, Value: 100_000, Counting: config.CountingInputOutput, OutputAtGeneration: true},
				{Dimension: config.LimitSpendMonth, Value: 25, OperatorSet: true},
			}
		}, ceiling: 512, binding: config.LimitRPM, windowS: 60, rps: 0.249},
}

func azureLimits() []config.Limit {
	return []config.Limit{
		{Dimension: config.LimitRPM, Value: 250, EvaluationWindowsS: []float64{1, 10, 60}},
		{Dimension: config.LimitTPM, Value: 250_000, Counting: config.CountingInputOutput, OutputAtGeneration: true},
		{Dimension: config.LimitSpendMonth, Value: 400, OperatorSet: true},
	}
}

// rehearsalRPMs are the anthropic route's requests_per_minute values a
// rehearsal accepts, each with the binding rate it solves to at the
// 16-token ceiling.
var rehearsalRPMs = map[int]float64{5_000: 75.454, 10_000: 150.909}

// rehearsalRoutesAt is a copy of rehearsalRoutes with the anthropic
// route's requests_per_minute limit at rpm.
func rehearsalRoutesAt(rpm int) ([]mockRoute, error) {
	rps, ok := rehearsalRPMs[rpm]
	if !ok {
		return nil, fmt.Errorf("rehearse: --anthropic-rpm %d: want 5000 or 10000", rpm)
	}
	routes := slices.Clone(rehearsalRoutes)
	for i := range routes {
		if routes[i].name != "anthropic" {
			continue
		}
		base := routes[i].limits
		routes[i].limits = func() []config.Limit {
			limits := base()
			for j := range limits {
				if limits[j].Dimension == config.LimitRPM {
					limits[j].Value = float64(rpm)
				}
			}
			return limits
		}
		routes[i].rps = rps
	}
	return routes, nil
}

func (h *rehearsalRun) routeOf(name string) *mockRoute {
	for i := range h.specs {
		if h.specs[i].name == name {
			return &h.specs[i]
		}
	}
	panic("rehearse: no route " + name)
}

// mockStream is what one mock streams: reasoning tokens, where its form
// carries reasoning, then the answer.
type mockStream struct {
	reasoning int
	answer    string
}

// startMock serves form f on a loopback port, over TLS under cert where
// it is set, paced by ttft and itl, with key accepted in the form's
// credential header alone.
func (f *mockForm) startMock(seed int64, ttft, itl config.LatencyDist, st mockStream, key string, cert *tls.Certificate) (*mock.Server, error) {
	sh := mock.Shape{Messages: f.adapter == config.AdapterMessages, ReasoningKey: f.reasoningKey, Answer: st.answer, Key: key, AuthHeader: f.auth}
	if f.reasoningKey != "" || sh.Messages {
		sh.ReasoningTokens = st.reasoning
	}
	s := mock.NewShaped(config.Mock{ListenAddr: rehearsalHost + ":0", Seed: seed, TTFT: ttft, ITL: itl}, sh)
	start := s.Start
	if cert != nil {
		start = func() error { return s.StartTLS(*cert) }
	}
	if err := start(); err != nil {
		return nil, err
	}
	return s, nil
}

// routeConfig is the release reference ref turned into route sp at
// ceiling, served by form f at endpoint, its key in keyEnv, the only host
// of its model and request shape, its limits retrieved on today, parsed
// from bytes that hold the route as edited, since a cell's report pair is
// rendered from them.
func routeConfig(ref []byte, sp *mockRoute, f *mockForm, ceiling int, endpoint, keyEnv, today string) (*config.Config, error) {
	cfg, err := config.Parse(ref)
	if err != nil {
		return nil, err
	}
	pin, ok := config.LookupShape(sp.shape)
	if !ok {
		return nil, fmt.Errorf("no request shape %q", sp.shape)
	}
	tg := &cfg.Target
	tg.Name, tg.Adapter, tg.AuthHeader, tg.Body, tg.ModelName = sp.name, f.adapter, f.auth, f.body(ceiling), "mock-model"
	tg.EndpointURL, tg.APIKeyEnv = endpoint, keyEnv
	tg.ResponseShape = nil
	if f.reasoningKey != "" {
		tg.ResponseShape = &config.ResponseShape{Reasoning: []string{f.reasoningKey}}
	}
	tg.Limits = sp.limits()
	for i := range tg.Limits {
		l := &tg.Limits[i]
		l.Marking, l.Source, l.Retrieved, l.SHA256 = config.MarkingPublic, rehearsalURL, today, strings.Repeat("0", 64)
	}
	tg.Naming = &config.Naming{Provider: "Mock " + sp.name, Lines: []string{"A mock route served on loopback."}}
	s := &cfg.Release.Shape
	s.Name, s.InputBoundTokens, s.CeilingTokens, s.Fallback = sp.shape, pin.InputBoundTokens, ceiling, sp.fallback
	cfg.Release.Caps.RouteUSD = sp.routeUSD
	cfg.Release.AccountGroup = sp.group
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
// the mock at base, its scheme and address, reports on its metrics
// endpoint.
func mockFinished(client *http.Client, base string) (map[string]int, error) {
	resp, err := client.Get(base + "/metrics")
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

// finishedSince is after less before, by outcome label, without zeros.
func finishedSince(before, after map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

// armFault arms one fault of mode on the mock at base through its admin
// endpoint, to fire at at and last d.
func armFault(client *http.Client, base, mode string, at time.Time, d time.Duration) (mock.FaultRecord, error) {
	body, err := json.Marshal(map[string]any{"mode": mode, "delay_s": max(0, time.Until(at).Seconds()), "duration_s": d.Seconds()})
	if err != nil {
		return mock.FaultRecord{}, err
	}
	resp, err := client.Post(base+"/admin/faults", "application/json", bytes.NewReader(body))
	if err != nil {
		return mock.FaultRecord{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return mock.FaultRecord{}, err
	}
	if resp.StatusCode != http.StatusCreated {
		return mock.FaultRecord{}, fmt.Errorf("mock admin: %s: %s", resp.Status, b)
	}
	var rec mock.FaultRecord
	return rec, json.Unmarshal(b, &rec)
}

// mockFaults are the fault records of the mock at base.
func mockFaults(client *http.Client, base string) ([]mock.FaultRecord, error) {
	resp, err := client.Get(base + "/admin/faults")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var snap struct {
		Faults []mock.FaultRecord `json:"faults"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return nil, err
	}
	return snap.Faults, nil
}

// The rehearsal's mock streams at full length: the first event after 0.3
// to 1.5 s, then reasoning and answer events 10 to 40 ms apart.
var (
	rehearsalTTFT = config.LatencyDist{Distribution: "uniform", MinMs: 300, MaxMs: 1500}
	rehearsalITL  = config.LatencyDist{Distribution: "uniform", MinMs: 10, MaxMs: 40}
)

// rehearsalReasoning is the reasoning tokens of a mock that reasons, more
// than Shape R's 16-token ceiling holds; rehearsalAnswer is streamed in
// 4-rune content events.
const rehearsalReasoning = 200

var rehearsalAnswer = strings.Repeat("The customer reports a duplicate charge and asks for a refund. ", 8)

// checkAnswer is 520 content events: more than Shape S's 512-token
// ceiling holds and fewer than its 1,024-token fallback.
var checkAnswer = strings.Repeat("The customer asks for a refund. ", 65)

// rehearsalOverload is the multiple of the anthropic route's top rate
// step 3 runs one step at.
const rehearsalOverload = 1.5

// rehearsalSettings are what a rehearsal runs at: the ladder, the
// binding rate its steps are fractions of where set, each route's frozen
// one where 0, the client-validity CPU window, the hard timeout, the
// multiple of its paced rate a ceiling check is sent at, one minute of a
// route's offset, the mocks' streams and, in a test alone, an edit of each
// route once it is built.
type rehearsalSettings struct {
	ladder     config.Ladder
	bindingRPS float64
	cpuWindowS int
	timeoutS   int
	checkPace  float64
	minute     time.Duration
	ttft, itl  config.LatencyDist
	reasoning  int
	answer     string
	editRoute  func(*route)
}

// fullLength reports whether s is the pinned ladder at each route's
// frozen binding rate, hard timeout and check pacing, and real offsets,
// with no route edited.
func (s *rehearsalSettings) fullLength() bool {
	l := s.ladder
	return slices.Equal(l.StepFractions, config.PinnedReleaseStepFractions) && l.WarmupS == config.PinnedReleaseWarmupS && l.StepS == config.PinnedReleaseStepS &&
		l.DrainS == config.PinnedReleaseDrainS && s.bindingRPS == 0 && s.cpuWindowS == config.PinnedCPUWindowS && s.timeoutS == config.PinnedReleaseHardTimeoutS &&
		s.checkPace == 1 && s.minute == time.Minute && s.editRoute == nil
}

// settingsAt is the pinned ladder, the ceiling checks' pacing and the
// full-length mock streams with every duration but the CPU window and the
// hard timeout divided by scale; tests replace it.
var settingsAt = func(scale int) rehearsalSettings {
	k := float64(scale)
	div := func(d config.LatencyDist) config.LatencyDist {
		d.MinMs, d.MaxMs = d.MinMs/k, d.MaxMs/k
		return d
	}
	return rehearsalSettings{
		ladder: config.Ladder{StepFractions: slices.Clone(config.PinnedReleaseStepFractions), WarmupS: config.PinnedReleaseWarmupS / k,
			StepS: config.PinnedReleaseStepS / k, DrainS: config.PinnedReleaseDrainS / k},
		cpuWindowS: config.PinnedCPUWindowS,
		timeoutS:   config.PinnedReleaseHardTimeoutS,
		checkPace:  k,
		minute:     time.Minute / time.Duration(scale),
		ttft:       div(rehearsalTTFT),
		itl:        div(rehearsalITL),
		reasoning:  rehearsalReasoning,
		answer:     rehearsalAnswer,
	}
}

// rehearsal is the summary file: what ran in each step and whether the
// rehearsal is green, which needs every step's problems empty and a
// window's projected disk within the free disk. Routes
// are step 1's cells, the five release routes at their offsets, and
// Mocks what each mock served in it; Attribution is step 2's cell under
// a mock rate limit, Overload step 3's one step above the top rate,
// FailureMode step 4's cell with the throttle and silent-hang faults
// armed in its top step, and Checks step 5's ceiling checks with no cell.
type rehearsal struct {
	SchemaVersion int  `json:"schema_version"`
	Green         bool `json:"green"`
	// FullLength is true for the pinned ladder at real offsets, the only
	// form whose summary is the acceptance run's evidence file.
	FullLength bool     `json:"full_length"`
	Scale      int      `json:"scale"`
	Reasons    []string `json:"reasons,omitempty"`
	// AnthropicRPM is the anthropic route's requests_per_minute limit
	// rehearsed.
	AnthropicRPM int `json:"anthropic_rpm"`

	Ladder          config.Ladder      `json:"ladder"`
	CPUWindowS      int                `json:"cpu_window_s"`
	HardTimeoutS    int                `json:"hard_timeout_s"`
	CheckPace       float64            `json:"check_pace"`
	OffsetMinuteS   float64            `json:"offset_minute_s"`
	MockTTFT        config.LatencyDist `json:"mock_ttft"`
	MockITL         config.LatencyDist `json:"mock_itl"`
	MockReasoning   int                `json:"mock_reasoning_tokens"`
	MockAnswerRunes int                `json:"mock_answer_runes"`
	// TLS says how the mocks serve and the client verifies them.
	TLS string `json:"tls"`
	// CeilingProbes says how each route's ceiling check record was made.
	CeilingProbes string `json:"ceiling_probes"`

	MasterSeed string          `json:"master_seed"`
	Cell       string          `json:"cell"`
	Source     evidence.Source `json:"source"`
	Host       rehearsalHostID `json:"host"`
	ClockSync  []string        `json:"clock_sync"`
	StartUTC   time.Time       `json:"start_utc"`
	EndUTC     time.Time       `json:"end_utc"`

	Routes      []rehearsalRoute  `json:"routes"`
	Mocks       []rehearsalMock   `json:"mocks"`
	Dispatch    *aggregateSeries  `json:"dispatch,omitempty"`
	Attribution *rehearsalRoute   `json:"attribution,omitempty"`
	Overload    *rehearsalRoute   `json:"overload,omitempty"`
	FailureMode *rehearsalRoute   `json:"failure_mode,omitempty"`
	Checks      []rehearsalChecks `json:"check_only,omitempty"`
}

type rehearsalHostID struct {
	Name           string `json:"name"`
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	CPUs           int    `json:"cpus"`
	OpenFilesLimit uint64 `json:"open_files_limit"`
}

// rehearsalRoute is one route's cell in one step. Dir is the cell's
// directory under --out; Completeness is "pass" or the completeness
// check's refusal, and Replay "pass" or why the cell's report pair was
// not rendered again byte for byte from inputs/<route>.yaml.
// CeilingChecks and Ceiling are the route's ceiling checks and the
// ceiling rule's choice from them, and Binding its frozen binding rate
// and limit. Finished is what the route's mock finished during a step 2
// to 4 cell, by outcome, and Status429 and Censored the cell's records
// with status 429 and censored by the hard timeout.
type rehearsalRoute struct {
	Step           int                  `json:"step"`
	Route          string               `json:"route"`
	Adapter        string               `json:"adapter"`
	AuthHeader     string               `json:"auth_header"`
	Path           string               `json:"path"`
	Shape          string               `json:"shape"`
	AccountGroup   string               `json:"account_group"`
	OffsetMinutes  int                  `json:"offset_minutes"`
	Run            schedule.Run         `json:"run"`
	CeilingChecks  []probe.CeilingCheck `json:"ceiling_checks"`
	Ceiling        probe.CeilingChoice  `json:"ceiling_choice"`
	BindingRPS     float64              `json:"binding_rps"`
	BindingLimit   string               `json:"binding_limit"`
	BindingWindowS float64              `json:"binding_window_s,omitempty"`
	Dir            string               `json:"dir,omitempty"`
	StartUTC       *time.Time           `json:"start_utc,omitempty"`
	EndUTC         *time.Time           `json:"end_utc,omitempty"`
	Outcome        string               `json:"outcome,omitempty"`
	Reasons        []string             `json:"reasons,omitempty"`
	Error          string               `json:"error,omitempty"`
	LadderEnd      string               `json:"ladder_end,omitempty"`
	Steps          []rehearsalStep      `json:"steps,omitempty"`
	Tripwires      *loadgen.Tripwires   `json:"tripwires,omitempty"`
	Sent           admission.Counts     `json:"sent"`
	Concurrent     int                  `json:"concurrent_runs"`
	Completeness   string               `json:"completeness,omitempty"`
	Replay         string               `json:"replay,omitempty"`
	Manifest       *evidence.FileRef    `json:"manifest,omitempty"`
	Finished       map[string]int       `json:"mock_finished,omitempty"`
	Status429      int                  `json:"status_429,omitempty"`
	Censored       int                  `json:"censored,omitempty"`
	RateLimit      *rehearsalRateLimit  `json:"mock_rate_limit,omitempty"`
	Labels         *rehearsalLabels     `json:"labels,omitempty"`
	Faults         []mock.FaultRecord   `json:"faults,omitempty"`
	Resources      *rehearsalResources  `json:"resources,omitempty"`
	Green          bool                 `json:"green"`
	Problems       []string             `json:"problems,omitempty"`
}

// rehearsalStep is one ladder step with its G2 verdict.
type rehearsalStep struct {
	RateRPS   float64             `json:"rate_rps"`
	Scheduled int                 `json:"scheduled"`
	Attempted bool                `json:"attempted"`
	G2        *loadgen.GateReport `json:"g2,omitempty"`
}

// rehearsalMock is what one mock finished in step 1, by outcome, beside
// the requests its routes' ceiling checks and cells sent.
type rehearsalMock struct {
	Form     string         `json:"form"`
	Routes   []string       `json:"routes"`
	Sent     int            `json:"sent"`
	Finished map[string]int `json:"finished"`
}

// rehearsalRateLimit is the mock's rate limit in step 2: Requests
// admitted in WindowS seconds, one below the requests the route's binding
// limit admits in its window.
type rehearsalRateLimit struct {
	Requests int     `json:"requests"`
	WindowS  float64 `json:"window_s"`
}

// rehearsalLabels are step 2's status_429 labels as the route's page
// prints them, and the label of one refusal once the account group's
// history loses the write time of its own request.
type rehearsalLabels struct {
	Page         report.LabelCounts `json:"page"`
	Refusals     int                `json:"refusals"`
	Unwritten    int64              `json:"unwritten_index"`
	UnwrittenAs  string             `json:"unwritten_label"`
	UnwrittenWhy []string           `json:"unwritten_unresolved,omitempty"`
}

// rehearsalChecks is one route's ceiling checks in step 5 and the
// ceiling rule's choice from them, with the record files under --out.
type rehearsalChecks struct {
	Route    string               `json:"route"`
	Shape    string               `json:"shape"`
	Checks   []probe.CeilingCheck `json:"ceiling_checks"`
	Choice   probe.CeilingChoice  `json:"ceiling_choice"`
	Files    []string             `json:"files"`
	Finished map[string]int       `json:"mock_finished,omitempty"`
	Green    bool                 `json:"green"`
	Problems []string             `json:"problems,omitempty"`
}

// rehearsalInput is a rehearse command after its flags are read.
type rehearsalInput struct {
	out, frames, clockSync, masterSeed, sourceDiff string
	scale, byteCap, anthropicRPM                   int
	build                                          *debug.BuildInfo
}

// rehearseCmd reads the rehearsal's flags and runs it.
func rehearseCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("rehearse", flag.ContinueOnError)
	out := fs.String("out", "", "new or empty output directory for the inputs, the cells' evidence and "+rehearsalFile+" (required)")
	frames := fs.String("frames", "", "raw-frame store directory outside the source tree (required)")
	clockSync := fs.String("clock-sync", "", "host clock-sync command run before each cell, as for run (required)")
	byteCap := fs.Int("byte-cap", 0, "Shape S ticket byte cap (required)")
	rpm := fs.Int("anthropic-rpm", 10_000, "the anthropic route's requests_per_minute limit, 5000 or 10000")
	scale := fs.Int("scale", 1, "divide the ladder's durations, the offsets, the mocks' stream times and the ceiling checks' spacing by this; 1 is full length")
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
		scale: *scale, byteCap: *byteCap, anthropicRPM: *rpm, build: build})
}

// rehearsalRun is one rehearsal's shared state: its routes, its input
// and settings, the inputs directory, the date, the operator file, the
// mocks' TLS certificate and the root pool that verifies it, a client
// trusting it, the release key, step 1's mocks and their streams by
// form and their keys' variables, the routes' configurations, ceiling
// checks and choices, the ledger, the schedule and the ledger lines it
// froze, and the routes.
type rehearsalRun struct {
	specs  []mockRoute
	in     rehearsalInput
	set    rehearsalSettings
	inputs string
	today  string
	opPath string
	cert   tls.Certificate
	roots  *x509.CertPool
	client *http.Client
	key    *releasekey.Key

	servers []*mock.Server
	mocks   map[string]*mock.Server
	streams map[string]mockStream
	envs    []string

	cfgs    []*config.Config
	checks  []*routeCheck
	ledger  string
	sched   *schedule.Schedule
	frozen  []admission.Entry
	routes  []*route
	byteCap []int
}

// close stops every mock and clears the keys' variables.
func (h *rehearsalRun) close() {
	for _, s := range h.servers {
		s.Close()
	}
	for _, env := range h.envs {
		os.Unsetenv(env)
	}
	if tr, ok := h.client.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
}

// rehearse runs the five steps of the rehearsal against mocks served
// over TLS in this process and writes the summary. Step 1 runs one cell
// of each release route against its mock, concurrently, each starting at
// its pinned offset; steps 2 to 4 run one cell each after it, and step 5
// ceiling checks alone. Its schedule's window runs schedule.MaxWindowDays
// from the current UTC date, which on any day of the week holds a
// Saturday and a Sunday before each day type's last date. A flag refused
// before --out is created leaves nothing written.
func rehearse(ctx context.Context, in rehearsalInput) (int, error) {
	if in.scale < 1 {
		return exitError, fmt.Errorf("rehearse: --scale %d: want 1 or more", in.scale)
	}
	specs, err := rehearsalRoutesAt(in.anthropicRPM)
	if err != nil {
		return exitError, err
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
	h := &rehearsalRun{specs: specs, in: in, set: set, inputs: filepath.Join(in.out, "inputs"), mocks: map[string]*mock.Server{}}
	if err := os.MkdirAll(h.inputs, 0o755); err != nil {
		return exitError, err
	}
	now := time.Now().UTC()
	h.today = now.Format(time.DateOnly)
	if h.opPath, err = writeRehearsalOperator(h.inputs, h.today, in.sourceDiff); err != nil {
		return exitError, err
	}
	if h.cert, h.roots, err = mock.SelfSignedCert(rehearsalHost); err != nil {
		return exitError, err
	}
	h.client = &http.Client{Transport: loadgen.NewTransport(1, h.roots), Timeout: 10 * time.Second}
	defer h.close()
	// The rehearsal's release key is never written.
	if h.key, err = releasekey.New(); err != nil {
		return exitError, err
	}
	if err := h.setUp(ctx); err != nil {
		return exitError, err
	}

	sum := &rehearsal{
		SchemaVersion: 2, FullLength: set.fullLength(), Scale: in.scale, AnthropicRPM: in.anthropicRPM,
		Ladder: set.ladder, CPUWindowS: set.cpuWindowS, HardTimeoutS: set.timeoutS, CheckPace: set.checkPace, OffsetMinuteS: set.minute.Seconds(),
		MockTTFT: set.ttft, MockITL: set.itl, MockReasoning: set.reasoning, MockAnswerRunes: len([]rune(set.answer)),
		TLS:           "every mock serves HTTP/1.1 over TLS under a self-signed certificate for " + rehearsalHost + ", verified against the client's root pool",
		CeilingProbes: "each route's ceiling check, sent to its mock at the shape's first ceiling and at its fallback where that failed, before the schedule",
		MasterSeed:    in.masterSeed, Cell: h.sched.Cells[0].Label, Source: evidence.SourceOf(in.build),
		Host:      rehearsalHostID{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), OpenFilesLimit: h.routes[0].openFiles},
		ClockSync: h.routes[0].clock.Args, StartUTC: now,
	}
	sum.Host.Name, _ = os.Hostname()
	var errs []error
	note := func(step int, name string, problems []string, err error) {
		if err != nil {
			errs = append(errs, err)
			problems = append(problems, "execution error: "+err.Error())
		}
		for _, p := range problems {
			sum.Reasons = append(sum.Reasons, fmt.Sprintf("step %d: %s: %s", step, name, p))
		}
	}

	routes, mocks, err := h.full(ctx)
	sum.Routes, sum.Mocks = routes, mocks
	for _, rr := range routes {
		note(1, rr.Route, rr.Problems, nil)
	}
	if err != nil {
		note(1, "cells", nil, err)
	}
	for _, m := range mocks {
		if !maps1(m.Finished, "200", m.Sent) {
			note(1, "mock "+m.Form, []string{fmt.Sprintf("finished %v; its routes' ceiling checks and cells sent %d", m.Finished, m.Sent)}, nil)
		}
	}
	if sum.Dispatch, err = aggregate(in.out); err != nil {
		note(1, "aggregate dispatch series", []string{err.Error()}, nil)
	}

	step := func(n int, run func(context.Context) (*rehearsalRoute, error)) *rehearsalRoute {
		rr, err := run(ctx)
		name := "cell"
		if rr != nil {
			name = rr.Route
			rr.Green = len(rr.Problems) == 0 && err == nil
			note(n, name, rr.Problems, err)
		} else {
			note(n, name, nil, err)
		}
		return rr
	}
	sum.Attribution = step(2, h.attribution)
	sum.Overload = step(3, h.overload)
	sum.FailureMode = step(4, h.failureMode)

	checks, err := h.checkOnly(ctx)
	sum.Checks = checks
	for _, c := range checks {
		note(5, c.Route, c.Problems, nil)
	}
	if err != nil {
		note(5, "ceiling checks", nil, err)
	}
	sum.Green = len(sum.Reasons) == 0
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

// maps1 reports whether m holds key at n and nothing else.
func maps1(m map[string]int, key string, n int) bool {
	return len(m) == 1 && m[key] == n
}

// startMocks starts a mock of each form streams names, seeded from seed
// on, its key set in the form's variable, and returns their addresses by
// form.
func (h *rehearsalRun) startMocks(streams map[string]mockStream, seed int64) (map[string]string, error) {
	addrs := map[string]string{}
	for i := range rehearsalForms {
		f := &rehearsalForms[i]
		st, ok := streams[f.name]
		if !ok {
			continue
		}
		key, err := randomKey()
		if err != nil {
			return nil, err
		}
		s, err := f.startMock(seed+int64(i), h.set.ttft, h.set.itl, st, key, &h.cert)
		if err != nil {
			return nil, err
		}
		h.servers = append(h.servers, s)
		env := keyEnv(f)
		h.envs = append(h.envs, env)
		if err := os.Setenv(env, key); err != nil {
			return nil, err
		}
		addrs[f.name] = s.Addr()
		if seed == 1 {
			h.mocks[f.name] = s
		}
	}
	return addrs, nil
}

// keyEnv is the variable holding the key of form f's mock.
func keyEnv(f *mockForm) string {
	return "PERCENTES_REHEARSAL_KEY_" + strings.ToUpper(f.name)
}

// routeCheck is one route's ceiling check records, at the shape's first
// ceiling and at its fallback where that failed, with their files and the
// ceiling rule's choice from them; cfg is the route at the ceiling
// chosen, or at its first ceiling where the rule excludes it.
type routeCheck struct {
	cfg    *config.Config
	recs   []*probe.ReleaseRecord
	paths  []string
	choice probe.CeilingChoice
}

// checkRoute sends route sp's ceiling check, admitted in l, through its
// mock at addr, at its shape's first ceiling and, where that fails and
// the shape permits the route a fallback, at the fallback, paced from
// the prices in entries, writes each record into dir, and applies the
// ceiling rule to them.
func (h *rehearsalRun) checkRoute(ctx context.Context, l *admission.Ledger, entries []admission.Entry, sp *mockRoute, addr, dir string) (*routeCheck, error) {
	f := formOf(sp.form)
	pin, ok := config.LookupShape(sp.shape)
	if !ok {
		return nil, fmt.Errorf("rehearse: %s: no request shape %q", sp.name, sp.shape)
	}
	at := map[int]*config.Config{}
	rc := &routeCheck{}
	ceilings := []int{pin.Ceiling}
	for i := 0; i < len(ceilings); i++ {
		cfg, err := routeConfig(configs.ReleaseReference, sp, f, ceilings[i], "https://"+addr+f.path, keyEnv(f), h.today)
		if err != nil {
			return nil, fmt.Errorf("rehearse: %s: %w", sp.name, err)
		}
		if i == 0 {
			rc.cfg, ceilings = cfg, cfg.Release.Shape.Ceilings()
		}
		at[ceilings[i]] = cfg
		byteCap := 0
		if sp.shape == config.ShapeS {
			byteCap = h.in.byteCap
		}
		shape, err := routeShape("rehearse", cfg, byteCap)
		if err != nil {
			return nil, err
		}
		stepRPS, err := checkStepRPS(cfg, entries)
		if err != nil {
			return nil, err
		}
		rec, err := sendCeiling(ctx, l, cfg, shape, "", byteCap, h.key, stepRPS*h.set.checkPace, h.roots, nil)
		if err != nil {
			return nil, fmt.Errorf("rehearse: %s: ceiling check at %d: %w", sp.name, ceilings[i], err)
		}
		if rec.Start.Blocked {
			return nil, fmt.Errorf("rehearse: %s: ceiling check at %d blocked: %s", sp.name, ceilings[i], rec.Start.Reason)
		}
		b, err := probe.Encode(rec)
		if err != nil {
			return nil, err
		}
		p := filepath.Join(dir, fmt.Sprintf("ceiling-%s-%d.json", sp.name, ceilings[i]))
		if err := os.WriteFile(p, b, 0o644); err != nil {
			return nil, err
		}
		rc.recs, rc.paths = append(rc.recs, rec), append(rc.paths, p)
		if rec.Ceiling.Pass && rec.InputBound.Pass && rec.OutputBound.Pass {
			break
		}
	}
	choice, err := probe.ChooseCeiling(&rc.cfg.Release.Shape, []string{sp.name}, rc.recs)
	if err != nil {
		return nil, fmt.Errorf("rehearse: %s: %w", sp.name, err)
	}
	rc.choice = choice
	if !choice.Excluded {
		rc.cfg = at[choice.Ceiling]
	}
	return rc, nil
}

// setUp starts step 1's mocks, records each route's prices in a new
// ledger, sends every route's ceiling check, the checks of one account
// group one after another, freezes the schedule on the configurations at
// the ceilings chosen, writes each to inputs/<route>.yaml, owner-only,
// for replay, and starts each route as run would, the cells to run at
// the rehearsal's settings. The anthropic and gemini mocks stream no
// reasoning, and their routes pass at their first ceiling; the azure and
// baseten mocks stream the settings' reasoning tokens before the answer,
// and azure-R fails at 16 and takes its 256 fallback.
func (h *rehearsalRun) setUp(ctx context.Context) error {
	h.streams = stepOneStreams(h.set)
	addrs, err := h.startMocks(h.streams, 1)
	if err != nil {
		return err
	}
	n := len(h.specs)
	firsts := make([]*config.Config, n)
	for i := range h.specs {
		sp := &h.specs[i]
		f := formOf(sp.form)
		pin, _ := config.LookupShape(sp.shape)
		if firsts[i], err = routeConfig(configs.ReleaseReference, sp, f, pin.Ceiling, "https://"+addrs[f.name]+f.path, keyEnv(f), h.today); err != nil {
			return fmt.Errorf("rehearse: %s: %w", sp.name, err)
		}
	}
	h.ledger = filepath.Join(h.inputs, "ledger.jsonl")
	caps, err := admission.ReleaseCaps(firsts...)
	if err != nil {
		return err
	}
	l, err := admission.Create(h.ledger, caps)
	if err != nil {
		return err
	}
	for i, cfg := range firsts {
		if _, err := recordPrice(l, cfg, h.specs[i].priceIn, h.specs[i].priceOut, rehearsalURL, h.today); err != nil {
			return err
		}
	}
	st, err := l.State()
	if err != nil {
		return err
	}
	l.Bind(st.OpenSHA256)
	entries, err := ledgerEntries(h.ledger, st.OpenSHA256)
	if err != nil {
		return err
	}
	h.checks = make([]*routeCheck, n)
	errs := make([]error, n)
	groups := map[string][]int{}
	var order []string
	for i := range h.specs {
		g := h.specs[i].group
		if groups[g] == nil {
			order = append(order, g)
		}
		groups[g] = append(groups[g], i)
	}
	var wg sync.WaitGroup
	for _, g := range order {
		wg.Add(1)
		go func(idx []int) {
			defer wg.Done()
			for _, i := range idx {
				sp := &h.specs[i]
				h.checks[i], errs[i] = h.checkRoute(ctx, l, entries, sp, addrs[sp.form], h.inputs)
			}
		}(groups[g])
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	h.cfgs = make([]*config.Config, n)
	names := make([]string, n)
	frozen := make([]schedule.RouteConfig, n)
	for i, rc := range h.checks {
		if rc.choice.Excluded {
			return fmt.Errorf("rehearse: %s: the ceiling check excludes the route: %s", h.specs[i].name, rc.choice.Reason)
		}
		h.cfgs[i], names[i] = rc.cfg, rc.cfg.Target.Name
		if frozen[i], err = frozenConfig(rc.cfg, h.key); err != nil {
			return err
		}
	}
	if err := checkHostGroups(h.cfgs); err != nil {
		return err
	}
	if err := checkAccountGroups(h.cfgs); err != nil {
		return err
	}
	if st, err = l.State(); err != nil {
		return err
	}
	windowStart, err := time.Parse(time.DateOnly, h.today)
	if err != nil {
		return err
	}
	sched, err := schedule.New(schedule.Input{MasterSeed: h.in.masterSeed, First: h.today, Last: windowStart.AddDate(0, 0, schedule.MaxWindowDays-1).Format(time.DateOnly), Routes: names, Configs: frozen,
		LedgerSHA256: st.OpenSHA256, LedgerLines: st.Lines, LedgerHeadSHA256: st.HeadSHA256})
	if err != nil {
		return err
	}
	all, read, err := readLedger(h.ledger, st.OpenSHA256)
	if err != nil {
		return err
	}
	if h.frozen, err = frozenEntries(all, read.HeadSHA256, sched.Input); err != nil {
		return err
	}
	if h.sched, err = freezeBinding(sched, h.cfgs, h.frozen); err != nil {
		return err
	}
	sb, err := h.sched.JSON()
	if err != nil {
		return err
	}
	schedulePath := filepath.Join(h.inputs, "schedule.json")
	if err := os.WriteFile(schedulePath, sb, 0o644); err != nil {
		return err
	}

	note := filepath.Join(h.inputs, rehearsalNote)
	h.routes = make([]*route, n)
	h.byteCap = make([]int, n)
	for i, cfg := range h.cfgs {
		name := cfg.Target.Name
		if err := os.WriteFile(filepath.Join(h.inputs, name+".yaml"), cfg.Raw, 0o600); err != nil {
			return err
		}
		if cfg.Release.Shape.Name == config.ShapeS {
			h.byteCap[i] = h.in.byteCap
		}
		refreshPath := filepath.Join(h.inputs, "refresh-"+name)
		if err := writeMockRefresh(refreshPath, cfg); err != nil {
			return err
		}
		r, err := newRoute(routeInput{
			cfg: cfg, configName: name + ".yaml", schedulePath: schedulePath, out: h.in.out, ledgerPath: h.ledger, key: h.key,
			framesRoot: h.in.frames, clockSync: h.in.clockSync, statusCmd: "true", balanceCmd: mockBalanceCmd(cfg), refreshCmd: refreshPath + " " + note, opPath: h.opPath,
			probeFiles: h.checks[i].paths, byteCap: h.byteCap[i], accountCreated: h.today, build: h.in.build,
		})
		if err != nil {
			return err
		}
		fc, _ := h.sched.Config(name)
		if got, want := r.verdict.Admitted(), r.cfg.Release.Ladder.StepRates(fc.BindingRPS); !slices.Equal(got, want) {
			return fmt.Errorf("rehearse: %s: the ceiling rule admits %v, not the ladder %v of the binding rate %g rps", name, got, want, fc.BindingRPS)
		}
		r.roots = h.roots
		r.cfg.ClientValidity.CPUWindowS = h.set.cpuWindowS
		r.cfg.Client.HTTPTimeoutS = h.set.timeoutS
		if h.set.editRoute != nil {
			h.set.editRoute(r)
		}
		h.routes[i] = r
	}
	return nil
}

// ladderAt sets r's cells to run every step of ladder at bindingRPS
// where it is set, or at the binding rate the schedule froze.
func ladderAt(r *route, ladder config.Ladder, bindingRPS float64) {
	r.cfg.Release.Ladder, r.steps = ladder, len(ladder.StepFractions)
	r.cfg.Release.BindingRPS = r.rule.bindingRPS
	if bindingRPS > 0 {
		r.cfg.Release.BindingRPS = bindingRPS
	}
	r.overLimits = slices.Max(ladder.StepRates(r.cfg.Release.BindingRPS)) > r.verdict.TopRPS
}

// runAt runs r's attempt 1 of the schedule's k-th cell now.
func runAt(ctx context.Context, r *route, k int) (schedule.Run, *cellRecord, error) {
	label := r.sched.Cells[k].Label
	i := slices.IndexFunc(r.runs, func(run schedule.Run) bool { return run.Label == label })
	if i < 0 {
		return schedule.Run{}, nil, fmt.Errorf("rehearse: %s: no run of cell %s", r.cfg.Target.Name, label)
	}
	run := r.runs[i]
	due := time.Now()
	r.due = func(schedule.Run) time.Time { return due }
	rec, err := r.runCell(ctx, run)
	return run, rec, err
}

// full runs step 1: one cell of each route at its offset from a common
// start, the schedule's first cell, and what each mock served.
func (h *rehearsalRun) full(ctx context.Context) ([]rehearsalRoute, []rehearsalMock, error) {
	set := h.set
	label := h.sched.Cells[0].Label
	runs := make([]schedule.Run, len(h.routes))
	for i, r := range h.routes {
		ladderAt(r, set.ladder, set.bindingRPS)
		k := slices.IndexFunc(r.runs, func(run schedule.Run) bool { return run.Label == label })
		if k < 0 {
			return nil, nil, fmt.Errorf("rehearse: %s: no run of cell %s", r.cfg.Target.Name, label)
		}
		runs[i] = r.runs[k]
	}
	start := time.Now().Add(time.Second)
	cellS := set.ladder.WarmupS + float64(len(set.ladder.StepFractions))*(set.ladder.StepS+set.ladder.DrainS)
	last := 0
	for _, r := range h.routes {
		due := start.Add(time.Duration(r.offsetMinutes) * set.minute)
		r.due = func(schedule.Run) time.Time { return due }
		last = max(last, r.offsetMinutes)
		fmt.Printf("rehearse: step 1: %s (%s, %s, Shape %s at %d) at offset %d min, from %s UTC\n", r.cfg.Target.Name, r.cfg.Target.Adapter, r.cfg.Target.AuthHeader,
			r.cfg.Release.Shape.Name, r.cfg.Release.Shape.CeilingTokens, r.offsetMinutes, due.UTC().Format(time.TimeOnly))
	}
	fmt.Printf("rehearse: step 1: cell %s, %g s each; the last ends about %s UTC\n", label, cellS,
		start.Add(time.Duration(last)*set.minute+time.Duration(cellS*float64(time.Second))).UTC().Format(time.TimeOnly))

	n := len(h.routes)
	recs := make([]*cellRecord, n)
	errs := make([]error, n)
	notStarted := make([]error, n)
	var wg sync.WaitGroup
	for i, r := range h.routes {
		wg.Add(1)
		go func(i int, r *route) {
			defer wg.Done()
			if notStarted[i] = waitUntil(ctx, start.Add(time.Duration(r.offsetMinutes)*set.minute)); notStarted[i] != nil {
				return
			}
			recs[i], errs[i] = r.runCell(ctx, runs[i])
			if errs[i] == nil {
				fmt.Printf("rehearse: step 1: %s %s: %s\n", r.cfg.Target.Name, label, cellSummary(recs[i]))
			}
		}(i, r)
	}
	wg.Wait()

	out := make([]rehearsalRoute, n)
	for i := range h.routes {
		sp := &h.specs[i]
		rr := h.rehearsed(1, i, runs[i], recs[i])
		switch {
		case notStarted[i] != nil:
			rr.Problems = append(rr.Problems, "not started: "+notStarted[i].Error())
		case errs[i] != nil:
			rr.Error = errs[i].Error()
			rr.Problems = append(rr.Problems, "execution error: "+rr.Error)
		}
		if c := rr.Ceiling; c.Excluded || c.Ceiling != sp.ceiling {
			rr.Problems = append(rr.Problems, fmt.Sprintf("the ceiling rule chose %+v, want %d", c, sp.ceiling))
		}
		if sp.rps > 0 && (rr.BindingRPS != sp.rps || rr.BindingLimit != sp.binding || rr.BindingWindowS != sp.windowS) {
			rr.Problems = append(rr.Problems, fmt.Sprintf("binding rate %g rps, %s over %g s; the release route's is %g rps, %s over %g s",
				rr.BindingRPS, rr.BindingLimit, rr.BindingWindowS, sp.rps, sp.binding, sp.windowS))
		}
		rr.Green = len(rr.Problems) == 0
		out[i] = rr
	}

	var mocks []rehearsalMock
	for _, f := range rehearsalForms {
		s := h.mocks[f.name]
		if s == nil {
			continue
		}
		m := rehearsalMock{Form: f.name}
		for i := range h.specs {
			if h.specs[i].form != f.name {
				continue
			}
			m.Routes = append(m.Routes, h.specs[i].name)
			for _, rec := range h.checks[i].recs {
				m.Sent += rec.Sent()
			}
			if recs[i] != nil {
				m.Sent += recs[i].Sent.Probe + recs[i].Sent.Warmup + recs[i].Sent.Measured
			}
		}
		var err error
		if m.Finished, err = mockFinished(h.client, "https://"+s.Addr()); err != nil {
			m.Finished = map[string]int{"metrics unread: " + err.Error(): 0}
		}
		mocks = append(mocks, m)
	}
	return out, mocks, errors.Join(errs...)
}

// rehearsed is the summary of route i's cell in step, with the problems
// that keep it from green. rec is nil for a cell never run.
func (h *rehearsalRun) rehearsed(step, i int, run schedule.Run, rec *cellRecord) rehearsalRoute {
	r, sp := h.routes[i], &h.specs[i]
	f := formOf(sp.form)
	fc, _ := h.sched.Config(sp.name)
	rr := rehearsalRoute{Step: step, Route: sp.name, Adapter: f.adapter, AuthHeader: f.auth, Path: f.path, Shape: sp.shape, AccountGroup: sp.group,
		OffsetMinutes: r.offsetMinutes, Run: run, Ceiling: h.checks[i].choice, BindingRPS: fc.BindingRPS, BindingLimit: fc.BindingLimit, BindingWindowS: fc.BindingWindowS}
	for _, c := range h.checks[i].recs {
		rr.CeilingChecks = append(rr.CeilingChecks, c.Ceiling)
	}
	if rec == nil {
		rr.Problems = append(rr.Problems, "no cell record")
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
	if want := len(r.cfg.Release.Ladder.StepFractions); len(rr.Steps) != want {
		problem("%d ladder steps, want %d", len(rr.Steps), want)
	}
	if tw := rec.Tripwires; tw == nil || tw.Fired {
		problem("tripwires %+v", tw)
	}
	rr.Completeness = "pass"
	if _, err := evidence.CheckRelease(evidence.Release{Dir: dir, Config: r.cfg.Raw, FramesRoot: h.in.frames, Pair: [2]string{report.JSONFile, report.TextFile}, Key: r.key}); err != nil {
		rr.Completeness = err.Error()
		problem("completeness check: %v", err)
	}
	if ref, err := evidence.HashFile(filepath.Join(dir, evidence.ManifestFile), evidence.ManifestFile); err != nil {
		problem("manifest: %v", err)
	} else {
		rr.Manifest = &ref
	}
	rr.Replay = "pass"
	if err := replayCell(dir, filepath.Join(h.inputs, sp.name+".yaml")); err != nil {
		rr.Replay = err.Error()
		problem("replay: %v", err)
	}
	return rr
}

// replayCell renders the report pair of the cell in dir again from the
// configuration file at cfgPath, as percentes --replay does, and
// compares it with the pair the cell wrote.
func replayCell(dir, cfgPath string) error {
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	js, txt, err := report.FromRun(dir, raw)
	if err != nil {
		return err
	}
	for name, got := range map[string][]byte{report.JSONFile: js, report.TextFile: []byte(txt)} {
		want, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%s differs from the one rendered from %s", name, filepath.Base(cfgPath))
		}
	}
	return nil
}

// stepCell runs route name's attempt 1 of the schedule's k-th cell in
// step with its ladder set by ladder, between before and after, and
// returns its summary with what its mock finished in it and its records'
// status 429 and censored counts.
func (h *rehearsalRun) stepCell(ctx context.Context, step int, name string, k int, ladder func(r *route), before, after func(r *route, run schedule.Run)) (*rehearsalRoute, error) {
	i := slices.IndexFunc(h.specs, func(sp mockRoute) bool { return sp.name == name })
	r := h.routes[i]
	s := h.mocks[h.specs[i].form]
	ladder(r)
	defer ladderAt(r, h.set.ladder, h.set.bindingRPS)
	pre, err := mockFinished(h.client, "https://"+s.Addr())
	if err != nil {
		return nil, err
	}
	label := r.sched.Cells[k].Label
	fmt.Printf("rehearse: step %d: %s %s at %v rps\n", step, name, label, r.cfg.Release.Ladder.StepRates(r.cfg.Release.BindingRPS))
	var run schedule.Run
	if before != nil {
		before(r, run)
	}
	run, rec, err := runAt(ctx, r, k)
	if rec != nil {
		fmt.Printf("rehearse: step %d: %s %s: %s\n", step, name, label, cellSummary(rec))
	}
	rr := h.rehearsed(step, i, run, rec)
	if after != nil {
		after(r, run)
	}
	if err != nil {
		rr.Error = err.Error()
		return &rr, err
	}
	post, err := mockFinished(h.client, "https://"+s.Addr())
	if err != nil {
		return &rr, err
	}
	rr.Finished = finishedSince(pre, post)
	if recs, err := readCellRecords(r.cellDir(run)); err != nil {
		rr.Problems = append(rr.Problems, "attempts: "+err.Error())
	} else {
		for _, q := range recs {
			if q.Status == http.StatusTooManyRequests {
				rr.Status429++
			}
			if q.Outcome == loadgen.OutcomeCensored {
				rr.Censored++
			}
		}
	}
	return &rr, nil
}

// stepOneStreams are step 1's mocks' streams by form at set.
func stepOneStreams(set rehearsalSettings) map[string]mockStream {
	return map[string]mockStream{
		"anthropic": {0, set.answer}, "azure": {set.reasoning, set.answer}, "baseten": {set.reasoning, set.answer}, "gemini": {0, set.answer},
	}
}

// mockEvents is the reasoning and content events step 1's mock streams
// to route i per request at the ceiling its check chose.
func (h *rehearsalRun) mockEvents(i int) int {
	f := formOf(h.specs[i].form)
	st := h.streams[f.name]
	n := (len([]rune(st.answer)) + 3) / 4
	if f.reasoningKey != "" || f.adapter == config.AdapterMessages {
		n += st.reasoning
	}
	return min(n, h.checks[i].choice.Ceiling)
}

// sent is the requests a cell sent.
func (rr *rehearsalRoute) sent() int { return rr.Sent.Probe + rr.Sent.Warmup + rr.Sent.Measured }

// attribution runs step 2: one azure-R cell at the frozen ladder, with
// the azure mock refusing above one request fewer than the binding limit
// admits in its window. A counted cell must record a status_429, every
// one on the route's page labelled at or below the limit's share, and one
// refusal must read comparison unresolved once the account group's
// history loses its request's write time; pages hold counted cells only,
// so a cell that did not count is not labelled. Step 2 starts once the
// group's sends have left its longest limit window (groupQuiet).
func (h *rehearsalRun) attribution(ctx context.Context) (*rehearsalRoute, error) {
	const name = "azure-R"
	i := slices.IndexFunc(h.specs, func(sp mockRoute) bool { return sp.name == name })
	sol, err := bindingSolution(h.routes[i].rule.cfg, h.sched, h.frozen)
	if err != nil {
		return nil, err
	}
	lim := &rehearsalRateLimit{Requests: int(sol.Binding.Allowance) - 1, WindowS: sol.Binding.WindowS}
	if lim.Requests < 1 || lim.WindowS <= 0 {
		return nil, fmt.Errorf("rehearse: %s: binding limit %s admits %d requests in %g s; no mock limit below it", name, sol.Binding.Dimension, sol.Binding.Allowance, sol.Binding.WindowS)
	}
	if err := h.groupQuiet(ctx, h.routes[i].rule.cfg); err != nil {
		return nil, err
	}
	s := h.mocks[h.specs[i].form]
	ladder := h.set.ladder
	ladder.StepFractions = slices.Clone(config.PinnedReleaseStepFractions)
	rr, err := h.stepCell(ctx, 2, name, 1, func(r *route) { ladderAt(r, ladder, 0) },
		func(*route, schedule.Run) {
			s.SetRateLimit(lim.Requests, time.Duration(lim.WindowS*float64(time.Second)))
		},
		func(*route, schedule.Run) { s.SetRateLimit(0, 0) })
	if rr == nil {
		return nil, err
	}
	rr.RateLimit = lim
	if err != nil {
		return rr, err
	}
	got := rr.Finished
	if rr.Status429 == 0 && rr.Outcome == outcomeCounted {
		rr.Problems = append(rr.Problems, "the mock refused none of the cell's requests")
	}
	if len(got) > 2 || got["200"]+got["429"] != rr.sent() || got["429"] != rr.Status429 {
		rr.Problems = append(rr.Problems, fmt.Sprintf("the mock finished %v; the cell sent %d and recorded %d status_429", got, rr.sent(), rr.Status429))
	}
	if rr.Outcome != outcomeCounted {
		return rr, nil
	}
	labels, problems, err := h.labels(i, rr)
	rr.Labels = labels
	rr.Problems = append(rr.Problems, problems...)
	return rr, err
}

// groupQuiet waits until every send in the history of cfg's account
// group has left the longest window of the group's limits: the latest
// time any send can have been written or ended, plus that window.
func (h *rehearsalRun) groupQuiet(ctx context.Context, cfg *config.Config) error {
	cfgs := h.frozenConfigs()
	group := cfg.Release.AccountGroup
	hist, err := readHistory(h.in.out, group, groupMembers(cfgs, h.sched)[group], cfgs, h.frozenAll(), h.checkPaths())
	if err != nil {
		return err
	}
	var longest float64
	for _, c := range cfgs {
		if c.Release.AccountGroup != group {
			continue
		}
		for k := range c.Target.Limits {
			for _, w := range c.Target.Limits[k].WindowsS() {
				longest = max(longest, w)
			}
		}
	}
	var last time.Time
	for k := range hist.Sends {
		for _, t := range []time.Time{hist.Sends[k].LatestUTC, hist.Sends[k].EndUTC} {
			if t.After(last) {
				last = t
			}
		}
	}
	if last.IsZero() {
		return nil
	}
	return waitUntil(ctx, last.Add(time.Duration(longest*float64(time.Second))))
}

// labels reads step 2's cell's status_429 labels from the pages the
// release builds of every route's cells, then labels the cell's first
// refusal again from its account group's history with its own request's
// write time removed.
func (h *rehearsalRun) labels(i int, rr *rehearsalRoute) (*rehearsalLabels, []string, error) {
	r := h.routes[i]
	name := r.cfg.Target.Name
	cfgs := h.frozenConfigs()
	rp, err := buildPages(h.in.out, h.in.frames, h.sched, r.scheduleSHA256, h.ledger, h.checkPaths(), cfgs, h.key, "")
	if err != nil {
		return nil, nil, err
	}
	var page report.StepTables
	if err := json.Unmarshal(rp.files[path.Join(routesDir, name+".json")], &page); err != nil {
		return nil, nil, fmt.Errorf("rehearse: %s page: %w", name, err)
	}
	cell := cellLabel(rr.Run)
	refusals := func(st *report.StepTables) []*report.Refusal {
		var out []*report.Refusal
		for ci := range st.Cells {
			c := &st.Cells[ci]
			if c.Cell != cell && c.Cell != rr.Run.Label {
				continue
			}
			for _, seg := range append([]*report.SegmentRefusals{c.Warmup}, stepRefusals(c)...) {
				if seg == nil {
					continue
				}
				for k := range seg.Refusals {
					out = append(out, &seg.Refusals[k])
				}
			}
		}
		return out
	}
	lb := &rehearsalLabels{}
	var problems []string
	got := refusals(&page)
	lb.Refusals = len(got)
	for _, ref := range got {
		switch ref.Label {
		case report.LabelAtOrBelow:
			lb.Page.AtOrBelow++
		case report.LabelAbove:
			lb.Page.Above++
		case report.LabelUnresolved:
			lb.Page.Unresolved++
		}
	}
	if lb.Refusals != rr.Status429 || lb.Page.AtOrBelow != lb.Refusals {
		problems = append(problems, fmt.Sprintf("the page labels %d status_429 (%d at or below, %d above, %d unresolved); the cell recorded %d, each sent within the binding limit's allowance",
			lb.Refusals, lb.Page.AtOrBelow, lb.Page.Above, lb.Page.Unresolved, rr.Status429))
	}
	if len(got) == 0 || got[0].WroteUTC == nil {
		return lb, append(problems, "no refusal with a write time to remove"), nil
	}
	first := *got[0]
	lb.Unwritten = first.Index

	cfg := r.rule.cfg
	recs, err := readRouteCells(h.in.out, h.in.frames, cfg, h.key)
	if err != nil {
		return lb, problems, err
	}
	st, err := report.NewStepTables(cfg, recs.verdict, recs.cells)
	if err != nil {
		return lb, problems, err
	}
	hist, err := readHistory(h.in.out, cfg.Release.AccountGroup, groupMembers(cfgs, h.sched)[cfg.Release.AccountGroup], cfgs, h.frozenAll(), h.checkPaths())
	if err != nil {
		return lb, problems, err
	}
	removed := 0
	for k := range hist.Sends {
		if s := &hist.Sends[k]; s.Route == name && s.WroteUTC != nil && s.WroteUTC.Equal(*first.WroteUTC) {
			s.WroteUTC = nil
			removed++
		}
	}
	if removed != 1 {
		return lb, append(problems, fmt.Sprintf("%d sends of the history carry the first refusal's write time, want 1", removed)), nil
	}
	if err := st.Attribute(cfg, recs.verdict, hist); err != nil {
		return lb, problems, err
	}
	for _, ref := range refusals(st) {
		if ref.Index == first.Index {
			lb.UnwrittenAs, lb.UnwrittenWhy = ref.Label, ref.Unresolved
		}
	}
	if lb.UnwrittenAs != report.LabelUnresolved || !slices.ContainsFunc(lb.UnwrittenWhy, func(u string) bool { return strings.Contains(u, "no write time") }) {
		problems = append(problems, fmt.Sprintf("refusal %d, its write time removed from the history, reads %q (%q), want comparison unresolved for a send with no write time",
			first.Index, lb.UnwrittenAs, lb.UnwrittenWhy))
	}
	return lb, problems, nil
}

// stepRefusals are a cell table's steps' status_429 records.
func stepRefusals(c *report.CellTable) []*report.SegmentRefusals {
	out := make([]*report.SegmentRefusals, len(c.Steps))
	for k := range c.Steps {
		out[k] = c.Steps[k].Refusals
	}
	return out
}

// frozenConfigs are the routes' configurations as they started, before
// the rehearsal set their cells' ladders, as the pages read them.
func (h *rehearsalRun) frozenConfigs() []*config.Config {
	out := make([]*config.Config, len(h.routes))
	for i, r := range h.routes {
		out[i] = r.rule.cfg
	}
	return out
}

// checkPaths are step 1's ceiling check records.
func (h *rehearsalRun) checkPaths() []string {
	var out []string
	for _, rc := range h.checks {
		out = append(out, rc.paths...)
	}
	return out
}

// frozenAll is every line of the ledger, which the pages and the history
// read.
func (h *rehearsalRun) frozenAll() []admission.Entry {
	entries, _, err := readLedger(h.ledger, h.sched.Input.LedgerSHA256)
	if err != nil {
		return h.frozen
	}
	return entries
}

// overload runs step 3: one anthropic cell of a warm-up, one step at
// rehearsalOverload times the top rate and a drain, with the host's
// resources read throughout.
func (h *rehearsalRun) overload(ctx context.Context) (*rehearsalRoute, error) {
	l := h.set.ladder
	l.StepFractions = []float64{rehearsalOverload}
	var (
		sampler *hostSampler
		res     *rehearsalResources
	)
	rr, err := h.stepCell(ctx, 3, "anthropic", 1, func(r *route) { ladderAt(r, l, h.set.bindingRPS) },
		func(*route, schedule.Run) { sampler = sampleHost(h.in.frames) },
		func(r *route, run schedule.Run) { res = sampler.finish(r.cellDir(run), h.in.frames) })
	if rr == nil {
		return nil, err
	}
	rr.Resources = res
	if rr.Resources != nil {
		rr.Problems = append(rr.Problems, rr.Resources.problems()...)
	}
	if err == nil && !maps1(rr.Finished, "200", rr.sent()) {
		rr.Problems = append(rr.Problems, fmt.Sprintf("the mock finished %v; the cell sent %d", rr.Finished, rr.sent()))
	}
	return rr, err
}

// failureMode runs step 4: one anthropic cell at the rehearsal's
// ladder with the mock's throttle fault armed from one eighth to one
// quarter of its top step and its silent-hang fault from three eighths
// to the step's end, through the mock's admin endpoint, with the host's
// resources read throughout. The faults' firing and the throttled, hung
// and censored requests are required where the cell attempted its top
// step, and status 200 alone where it did not.
func (h *rehearsalRun) failureMode(ctx context.Context) (*rehearsalRoute, error) {
	const name = "anthropic"
	s := h.mocks[h.routeOf(name).form]
	l := h.set.ladder
	top := l.WarmupS + float64(len(l.StepFractions)-1)*(l.StepS+l.DrainS)
	sec := func(x float64) time.Duration { return time.Duration(x * float64(time.Second)) }
	var (
		armed   []mock.FaultRecord
		armErr  error
		arming  sync.WaitGroup
		sampler *hostSampler
		res     *rehearsalResources
	)
	rr, err := h.stepCell(ctx, 4, name, 2, func(r *route) {
		ladderAt(r, l, h.set.bindingRPS)
		r.onEpoch = func(epoch time.Time) {
			arming.Add(1)
			go func() {
				defer arming.Done()
				for _, f := range []struct {
					mode     string
					from, to float64
				}{{config.MockFaultThrottle, 1.0 / 8, 2.0 / 8}, {config.MockFaultSilentHang, 3.0 / 8, 1}} {
					rec, err := armFault(h.client, "https://"+s.Addr(), f.mode, epoch.Add(sec(top+f.from*l.StepS)), sec((f.to-f.from)*l.StepS))
					if err != nil {
						armErr = err
						return
					}
					armed = append(armed, rec)
				}
			}()
		}
	}, func(*route, schedule.Run) { sampler = sampleHost(h.in.frames) },
		func(r *route, run schedule.Run) {
			r.onEpoch = nil
			arming.Wait()
			res = sampler.finish(r.cellDir(run), h.in.frames)
		})
	if rr == nil {
		return nil, err
	}
	rr.Resources = res
	if rr.Resources != nil {
		rr.Problems = append(rr.Problems, rr.Resources.problems()...)
	}
	if armErr != nil {
		rr.Problems = append(rr.Problems, "arming the faults: "+armErr.Error())
	}
	if err != nil {
		return rr, err
	}
	all, ferr := mockFaults(h.client, "https://"+s.Addr())
	if ferr != nil {
		rr.Problems = append(rr.Problems, "fault records: "+ferr.Error())
	}
	reachedTop := len(rr.Steps) > 0 && rr.Steps[len(rr.Steps)-1].Attempted
	for _, a := range armed {
		for _, f := range all {
			if f.ID == a.ID {
				rr.Faults = append(rr.Faults, f)
				if reachedTop && (f.State != "expired" || f.FiredOffsetS == nil) {
					rr.Problems = append(rr.Problems, fmt.Sprintf("fault %d (%s) %s, want fired and expired", f.ID, f.Mode, f.State))
				}
			}
		}
	}
	if len(rr.Faults) != 2 {
		rr.Problems = append(rr.Problems, fmt.Sprintf("%d of the 2 faults armed and recorded", len(rr.Faults)))
	}
	got := rr.Finished
	if reachedTop && (got["429"] == 0 || got["hung"] == 0 || rr.Censored == 0) {
		rr.Problems = append(rr.Problems, fmt.Sprintf("the mock finished %v and the cell recorded %d censored; want throttled and hung requests, censored by the hard timeout", got, rr.Censored))
	}
	if !reachedTop && !maps1(got, "200", rr.sent()) {
		rr.Problems = append(rr.Problems, fmt.Sprintf("the mock finished %v below the top ladder step; the cell sent %d, want status 200 to each", got, rr.sent()))
	}
	if len(got) > 3 || got["200"]+got["429"]+got["hung"] != rr.sent() || got["429"] != rr.Status429 {
		rr.Problems = append(rr.Problems, fmt.Sprintf("the mock finished %v; the cell sent %d and recorded %d status_429", got, rr.sent(), rr.Status429))
	}
	return rr, nil
}

// checkOnly runs step 5 in its own directory and ledger: the anthropic
// and gemini routes' ceiling checks against mocks of their own, the
// anthropic mock reasoning past the 16-token ceiling, so that the route,
// which declares no fallback, is excluded, and the gemini mock answering
// past 512 tokens and within 1,024, so that the route takes its fallback.
func (h *rehearsalRun) checkOnly(ctx context.Context) ([]rehearsalChecks, error) {
	dir := filepath.Join(h.in.out, checksDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	addrs, err := h.startMocks(map[string]mockStream{"anthropic": {h.set.reasoning, h.set.answer}, "gemini": {0, checkAnswer}}, int64(len(rehearsalForms)+1))
	if err != nil {
		return nil, err
	}
	want := []struct {
		route    string
		excluded bool
		ceiling  int
	}{{"anthropic", true, 0}, {"gemini", false, 1024}}
	cfgs := make([]*config.Config, len(want))
	for i, w := range want {
		sp := h.routeOf(w.route)
		f := formOf(sp.form)
		pin, _ := config.LookupShape(sp.shape)
		if cfgs[i], err = routeConfig(configs.ReleaseReference, sp, f, pin.Ceiling, "https://"+addrs[f.name]+f.path, keyEnv(f), h.today); err != nil {
			return nil, err
		}
	}
	ledger := filepath.Join(dir, "ledger.jsonl")
	caps, err := admission.ReleaseCaps(cfgs...)
	if err != nil {
		return nil, err
	}
	l, err := admission.Create(ledger, caps)
	if err != nil {
		return nil, err
	}
	for i, cfg := range cfgs {
		sp := h.routeOf(want[i].route)
		if _, err := recordPrice(l, cfg, sp.priceIn, sp.priceOut, rehearsalURL, h.today); err != nil {
			return nil, err
		}
	}
	st, err := l.State()
	if err != nil {
		return nil, err
	}
	l.Bind(st.OpenSHA256)
	entries, err := ledgerEntries(ledger, st.OpenSHA256)
	if err != nil {
		return nil, err
	}
	pre := map[string]map[string]int{}
	for _, w := range want {
		if pre[w.route], err = mockFinished(h.client, "https://"+addrs[h.routeOf(w.route).form]); err != nil {
			return nil, err
		}
	}
	rcs := make([]*routeCheck, len(want))
	errs := make([]error, len(want))
	var wg sync.WaitGroup
	for i, w := range want {
		wg.Add(1)
		go func(i int, sp *mockRoute) {
			defer wg.Done()
			rcs[i], errs[i] = h.checkRoute(ctx, l, entries, sp, addrs[sp.form], dir)
		}(i, h.routeOf(w.route))
	}
	wg.Wait()
	out := make([]rehearsalChecks, len(want))
	for i, w := range want {
		sp := h.routeOf(w.route)
		c := rehearsalChecks{Route: sp.name, Shape: sp.shape}
		if errs[i] != nil {
			c.Problems = append(c.Problems, "execution error: "+errs[i].Error())
			out[i] = c
			continue
		}
		rc := rcs[i]
		c.Choice = rc.choice
		sent := 0
		for k, rec := range rc.recs {
			c.Checks = append(c.Checks, rec.Ceiling)
			rel, _ := filepath.Rel(h.in.out, rc.paths[k])
			c.Files = append(c.Files, filepath.ToSlash(rel))
			sent += rec.Sent()
		}
		switch ch := rc.choice; {
		case w.excluded && (!ch.Excluded || ch.Reason == ""):
			c.Problems = append(c.Problems, fmt.Sprintf("the ceiling rule chose %+v, want the route excluded with its reason", ch))
		case !w.excluded && (ch.Excluded || ch.Ceiling != w.ceiling):
			c.Problems = append(c.Problems, fmt.Sprintf("the ceiling rule chose %+v, want %d", ch, w.ceiling))
		}
		if post, err := mockFinished(h.client, "https://"+addrs[sp.form]); err != nil {
			c.Problems = append(c.Problems, "mock metrics: "+err.Error())
		} else if c.Finished = finishedSince(pre[w.route], post); !maps1(c.Finished, "200", sent) {
			c.Problems = append(c.Problems, fmt.Sprintf("the mock finished %v; the checks sent %d", c.Finished, sent))
		}
		c.Green = len(c.Problems) == 0
		out[i] = c
	}
	return out, errors.Join(errs...)
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
