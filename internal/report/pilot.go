package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/score"
)

// PilotPairs is the number of compatible, client-valid pairs one series
// segment needs for a complete pilot (SPEC-HOSTED HA2 §4).
const PilotPairs = 6

// CaveatPilot is the pilot report's caveat.
const CaveatPilot = "CAVEAT: two hosted endpoints measured from one client host, in consecutive windows on each session date. Every timing is client-observed from intended dispatch, and the model weights and serving stack behind each model identifier are not verifiable (SPEC-HOSTED §6). No service-level objective (SLO) is defined for hosted data, so latency is reported as distributions with no pass bar (SPEC-HOSTED §7)."

// PilotDemonstration is the label of a pilot whose pair Percentes chose.
const PilotDemonstration = "DEMONSTRATION: Percentes chose this pair to show the report and its method. A pair named by a reader replaces it."

// Fixed pilot disclosures.
const (
	PilotOneAuthor = "One author: Percentes has one author. No second person checked the instrument, the cases, the labels or this report."
	PilotFixture   = "Cases: synthetic support tickets from a deterministic generator, labelled by construction. Their realism is limited, and they are not validated as production quality."
	PilotRepeated  = "The same measured cases run in every session, in a per-session order, so the sessions are not independent task examples."
)

// noRoute is the confusion-matrix column of a measured attempt with no
// parsed label.
const noRoute = "none"

// PilotInput is what the pilot report is rendered from: the two endpoints
// in config order, the frozen labels in rubric order, the scoring cutoff,
// and every session with each endpoint's window.
type PilotInput struct {
	InstrumentCommit string
	ConfigSHA256     string
	// Demonstration marks a pair Percentes chose.
	Demonstration bool
	Labels        []string
	Cutoff        time.Duration
	Endpoints     [2]PilotEndpoint
	Roles         *evidence.Roles
	Sessions      []PilotSession
}

// PilotEndpoint is one target as configured.
type PilotEndpoint struct {
	Name        string
	Provider    string
	AccountTier string
	Model       string
	Params      *config.Params
}

// PilotSession is one dated session. First names the endpoint that ran
// first; Replaces names the session a replacement stands in for.
type PilotSession struct {
	Label    string
	Date     string
	First    string
	Replaces string
	// Windows is indexed as Endpoints; nil is a window that did not run.
	Windows [2]*PilotWindow
}

// PilotWindow is one endpoint's window: its client validity, any safety
// stop, the model identifiers the responses reported, and one scored
// record per scheduled request, warm-up included.
type PilotWindow struct {
	Valid          bool
	InvalidReasons []string
	Interrupted    *loadgen.Interruption
	ReportedModels []string
	Records        []evidence.Record
}

// PilotReport is the pilot report's JSON form.
type PilotReport struct {
	SchemaVersion     int                 `json:"schema_version"`
	InstrumentCommit  string              `json:"instrument_commit"`
	ConfigSHA256      string              `json:"config_sha256"`
	Demonstration     string              `json:"demonstration,omitempty"`
	Incomplete        string              `json:"incomplete,omitempty"`
	Caveat            string              `json:"caveat"`
	Disclosures       []string            `json:"disclosures"`
	PinsNotVerifiable []string            `json:"pins_not_verifiable"`
	CutoffS           float64             `json:"cutoff_s"`
	TimeoutS          int                 `json:"timeout_s"`
	Labels            []string            `json:"labels"`
	Endpoints         []PilotEndpointView `json:"endpoints"`
	Pairs             []PilotPair         `json:"pairs"`
	Segments          []PilotSegment      `json:"segments"`
	ValidPairs        int                 `json:"valid_pairs"`
}

// PilotEndpointView is an endpoint as the report states it.
type PilotEndpointView struct {
	Name        string         `json:"name"`
	Provider    string         `json:"provider"`
	AccountTier string         `json:"account_tier"`
	Model       string         `json:"requested_model"`
	Params      *config.Params `json:"params,omitempty"`
}

// PilotPair is one session's two windows. Difference is the first
// endpoint's headline numerator minus the second's, present for a valid
// pair only.
type PilotPair struct {
	Session    string             `json:"session"`
	Date       string             `json:"date"`
	First      string             `json:"first,omitempty"`
	Replaces   string             `json:"replaces,omitempty"`
	ReplacedBy string             `json:"replaced_by,omitempty"`
	Segment    int                `json:"segment"`
	Valid      bool               `json:"valid"`
	Reasons    []string           `json:"reasons,omitempty"`
	Difference *int               `json:"difference,omitempty"`
	Windows    []*PilotWindowView `json:"windows"`
}

// PilotSegment is a run of sessions with one reported model identity per
// endpoint; a change in either starts the next segment (HA2 §6).
type PilotSegment struct {
	Index          int      `json:"index"`
	FirstSession   string   `json:"first_session"`
	Sessions       int      `json:"sessions"`
	ValidPairs     int      `json:"valid_pairs"`
	MeanDiff       *float64 `json:"mean_difference,omitempty"`
	MinDiff        *int     `json:"min_difference,omitempty"`
	MaxDiff        *int     `json:"max_difference,omitempty"`
	Differences    []int    `json:"differences"`
	ReportedModels []string `json:"reported_models"`
}

// PilotWindowView is one window's rows over its measured requests. A
// window that did not run carries only its endpoint and Missing.
type PilotWindowView struct {
	Endpoint       string                `json:"endpoint"`
	Missing        bool                  `json:"missing,omitempty"`
	ClientValid    bool                  `json:"client_valid"`
	InvalidReasons []string              `json:"invalid_reasons,omitempty"`
	Interrupted    *loadgen.Interruption `json:"interrupted,omitempty"`
	ReportedModels []string              `json:"reported_models,omitempty"`
	// Correct is the headline numerator: complete correct answers by the
	// cutoff. Scheduled is every scheduled measured request.
	Correct          int            `json:"correct_by_cutoff"`
	Scheduled        int            `json:"scheduled"`
	CorrectByTimeout int            `json:"correct_by_timeout"`
	Classes          map[string]int `json:"task_classes,omitempty"`
	SchemaReasons    map[string]int `json:"schema_failure_reasons,omitempty"`
	Outcomes         map[string]int `json:"transport_outcomes,omitempty"`
	ErrorClasses     map[string]int `json:"error_classes,omitempty"`
	Statuses         map[string]int `json:"http_statuses,omitempty"`
	FirstContent     *PilotLatency  `json:"first_visible_content,omitempty"`
	Completed        *PilotLatency  `json:"completed_answer,omitempty"`
	Usage            *PilotUsage    `json:"provider_usage,omitempty"`
	ContentChunks    int            `json:"content_chunks"`
	ReasoningChunks  int            `json:"reasoning_chunks"`
	LabelCounts      map[string]int `json:"label_counts,omitempty"`
	// Confusion counts measured requests by frozen label, then by the
	// scored answer label, or "none" where no label was parsed.
	Confusion map[string]map[string]int `json:"confusion,omitempty"`
}

// PilotLatency is a client-observed latency from intended dispatch over a
// named cohort, with the excluded measured requests by transport outcome.
// The median is the lower middle value at an even count.
type PilotLatency struct {
	Cohort   string         `json:"cohort"`
	N        int            `json:"n"`
	MinNs    *int64         `json:"min_ns,omitempty"`
	MedianNs *int64         `json:"median_ns,omitempty"`
	MaxNs    *int64         `json:"max_ns,omitempty"`
	Excluded map[string]int `json:"excluded"`
}

// PilotUsage sums the usage objects the responses carried.
type PilotUsage struct {
	Responses        int   `json:"responses"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

// GeneratePilot renders the pilot report pair from in.
func GeneratePilot(in *PilotInput) ([]byte, string, error) {
	rep, err := newPilotReport(in)
	if err != nil {
		return nil, "", err
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, "", fmt.Errorf("report: marshal pilot: %w", err)
	}
	return raw, humanPilot(rep), nil
}

func newPilotReport(in *PilotInput) (*PilotReport, error) {
	if err := checkPilotInput(in); err != nil {
		return nil, err
	}
	rep := &PilotReport{
		SchemaVersion:     1,
		InstrumentCommit:  in.InstrumentCommit,
		ConfigSHA256:      in.ConfigSHA256,
		Caveat:            CaveatPilot,
		PinsNotVerifiable: HostedPinsNotVerifiable,
		CutoffS:           in.Cutoff.Seconds(),
		TimeoutS:          config.PinnedClientTimeoutS,
		Labels:            in.Labels,
		Pairs:             []PilotPair{},
		Segments:          []PilotSegment{},
	}
	if in.Demonstration {
		rep.Demonstration = PilotDemonstration
	}
	for _, e := range in.Endpoints {
		rep.Endpoints = append(rep.Endpoints, PilotEndpointView(e))
	}
	rep.Disclosures = pilotDisclosures(in)

	sessions := slices.Clone(in.Sessions)
	sort.SliceStable(sessions, func(i, j int) bool { return sessions[i].Date < sessions[j].Date })
	replacedBy := map[string]string{}
	for _, s := range sessions {
		if s.Replaces != "" {
			replacedBy[s.Replaces] = s.Label
		}
	}
	var identity [2]string
	for _, s := range sessions {
		p := PilotPair{Session: s.Label, Date: s.Date, First: s.First, Replaces: s.Replaces, ReplacedBy: replacedBy[s.Label]}
		var expected [2][]string
		changed := false
		for i, win := range s.Windows {
			name := in.Endpoints[i].Name
			if win == nil {
				p.Windows = append(p.Windows, &PilotWindowView{Endpoint: name, Missing: true})
				p.Reasons = append(p.Reasons, fmt.Sprintf("no %s window", name))
				continue
			}
			v, exp, err := pilotWindow(in, name, win)
			if err != nil {
				return nil, fmt.Errorf("report: session %s, %s: %w", s.Label, name, err)
			}
			p.Windows = append(p.Windows, v)
			expected[i] = exp
			if !win.Valid {
				p.Reasons = append(p.Reasons, fmt.Sprintf("%s window client-invalid: %s", name, strings.Join(win.InvalidReasons, "; ")))
			}
			if win.Interrupted != nil {
				p.Reasons = append(p.Reasons, fmt.Sprintf("%s window interrupted: %s", name, win.Interrupted.Cause))
			}
			if n := v.Classes[string(score.InstrumentIncompatible)]; n > 0 {
				p.Reasons = append(p.Reasons, fmt.Sprintf("%s: %d instrument-incompatible attempts", name, n))
			}
			if id := strings.Join(win.ReportedModels, ","); id != "" {
				if identity[i] != "" && identity[i] != id {
					changed = true
				}
			}
		}
		if s.Windows[0] != nil && s.Windows[1] != nil && !slices.Equal(expected[0], expected[1]) {
			p.Reasons = append(p.Reasons, "the two windows' measured requests carry different frozen labels")
		}
		if len(rep.Segments) == 0 || changed {
			rep.Segments = append(rep.Segments, PilotSegment{Index: len(rep.Segments) + 1, FirstSession: s.Label, Differences: []int{}})
		}
		for i, win := range s.Windows {
			if win != nil && len(win.ReportedModels) > 0 {
				identity[i] = strings.Join(win.ReportedModels, ",")
			}
		}
		seg := &rep.Segments[len(rep.Segments)-1]
		seg.Sessions++
		p.Segment = seg.Index
		p.Valid = len(p.Reasons) == 0
		if p.Valid {
			d := p.Windows[0].Correct - p.Windows[1].Correct
			p.Difference = &d
			seg.Differences = append(seg.Differences, d)
			seg.ValidPairs++
			rep.ValidPairs++
		}
		seg.ReportedModels = []string{identity[0], identity[1]}
		rep.Pairs = append(rep.Pairs, p)
	}
	best := 0
	for i := range rep.Segments {
		seg := &rep.Segments[i]
		best = max(best, seg.ValidPairs)
		if len(seg.Differences) == 0 {
			continue
		}
		sum := 0
		lo, hi := seg.Differences[0], seg.Differences[0]
		for _, d := range seg.Differences {
			sum += d
			lo, hi = min(lo, d), max(hi, d)
		}
		mean := float64(sum) / float64(len(seg.Differences))
		seg.MeanDiff, seg.MinDiff, seg.MaxDiff = &mean, &lo, &hi
	}
	if best < PilotPairs {
		counts := make([]string, len(rep.Segments))
		for i, seg := range rep.Segments {
			counts[i] = fmt.Sprintf("segment %d: %d", seg.Index, seg.ValidPairs)
		}
		rep.Incomplete = fmt.Sprintf("INCOMPLETE: %d compatible, client-valid pairs in all (%s); the design needs %d in one segment. These rows describe the valid pairs only and do not complete the design.",
			rep.ValidPairs, strings.Join(counts, ", "), PilotPairs)
	}
	return rep, nil
}

func checkPilotInput(in *PilotInput) error {
	if _, err := score.NewContract(in.Labels); err != nil {
		return fmt.Errorf("report: %w", err)
	}
	if in.Cutoff <= 0 {
		return fmt.Errorf("report: cutoff %v is not positive", in.Cutoff)
	}
	a, b := in.Endpoints[0].Name, in.Endpoints[1].Name
	if a == "" || b == "" || a == b {
		return fmt.Errorf("report: endpoint names %q and %q must be set and distinct", a, b)
	}
	if len(in.Sessions) == 0 {
		return errors.New("report: no sessions")
	}
	labels := map[string]bool{}
	for _, s := range in.Sessions {
		if s.Label == "" || labels[s.Label] {
			return fmt.Errorf("report: session label %q is empty or repeated", s.Label)
		}
		if _, err := time.Parse("2006-01-02", s.Date); err != nil {
			return fmt.Errorf("report: session %s: date %q", s.Label, s.Date)
		}
		if s.First != "" && s.First != a && s.First != b {
			return fmt.Errorf("report: session %s: first endpoint %q is not configured", s.Label, s.First)
		}
		labels[s.Label] = true
	}
	for _, s := range in.Sessions {
		if s.Replaces != "" && (s.Replaces == s.Label || !labels[s.Replaces]) {
			return fmt.Errorf("report: session %s replaces %q, which is not another session", s.Label, s.Replaces)
		}
	}
	return nil
}

func pilotDisclosures(in *PilotInput) []string {
	out := []string{PilotOneAuthor}
	if r := in.Roles; r != nil {
		out = append(out, fmt.Sprintf("Roles: task chosen by %s; cases and labels supplied by %s; rubric written by %s; interest of the person who named the pair: %s.", r.Task, r.CasesLabels, r.Rubric, r.PairInterest))
	}
	out = append(out, PilotFixture, PilotRepeated)
	var parts []string
	var values []int
	for _, e := range in.Endpoints {
		key, v := "no ceiling", 0
		if p := e.Params; p != nil {
			switch {
			case p.MaxCompletionTokens != 0:
				key, v = "max_completion_tokens", p.MaxCompletionTokens
			case p.MaxTokens != 0:
				key, v = "max_tokens", p.MaxTokens
			}
		}
		values = append(values, v)
		if v == 0 {
			parts = append(parts, fmt.Sprintf("%s %s", e.Name, key))
		} else {
			parts = append(parts, fmt.Sprintf("%s %s=%d", e.Name, key, v))
		}
	}
	if values[0] == values[1] && values[0] != 0 {
		out = append(out, fmt.Sprintf("Output ceiling, the same value on both endpoints: %s.", strings.Join(parts, "; ")))
	} else {
		out = append(out, fmt.Sprintf("Output ceiling DIFFERS between the endpoints: %s.", strings.Join(parts, "; ")))
	}
	if m := in.Endpoints[0].Model; m != "" && m == in.Endpoints[1].Model {
		out = append(out, fmt.Sprintf("Both endpoints were sent the model identifier %s. A shared identifier does not establish identical weights, numerical precision or backend versions.", m))
	}
	return out
}

// pilotWindow reduces win's measured requests and returns the frozen
// labels in request order.
func pilotWindow(in *PilotInput, name string, win *PilotWindow) (*PilotWindowView, []string, error) {
	v := &PilotWindowView{
		Endpoint: name, ClientValid: win.Valid, InvalidReasons: win.InvalidReasons, Interrupted: win.Interrupted, ReportedModels: win.ReportedModels,
		Classes: map[string]int{}, SchemaReasons: map[string]int{}, Outcomes: map[string]int{}, ErrorClasses: map[string]int{}, Statuses: map[string]int{},
		LabelCounts: map[string]int{}, Confusion: map[string]map[string]int{},
	}
	for _, c := range score.Classes {
		v.Classes[string(c)] = 0
	}
	for _, l := range in.Labels {
		v.LabelCounts[l] = 0
		row := map[string]int{noRoute: 0}
		for _, m := range in.Labels {
			row[m] = 0
		}
		v.Confusion[l] = row
	}
	measuredFrom := int64(config.PinnedPilotWarmupS) * int64(time.Second)
	var expected []string
	var first, done []int64
	firstExcl, doneExcl := map[string]int{}, map[string]int{}
	usage := &PilotUsage{}
	for i := range win.Records {
		r := &win.Records[i]
		if r.IntendedNs < measuredFrom {
			continue
		}
		s := r.Score
		switch {
		case s == nil:
			return nil, nil, fmt.Errorf("measured request %d has no score", r.Index)
		case s.Error != "":
			return nil, nil, fmt.Errorf("measured request %d: scorer error %q", r.Index, s.Error)
		case !slices.Contains(in.Labels, s.Expected):
			return nil, nil, fmt.Errorf("measured request %d: frozen label %q is not configured", r.Index, s.Expected)
		case s.Route != "" && !slices.Contains(in.Labels, s.Route):
			return nil, nil, fmt.Errorf("measured request %d: answer label %q is not configured", r.Index, s.Route)
		}
		if _, ok := v.Classes[string(s.Class)]; !ok {
			return nil, nil, fmt.Errorf("measured request %d: task class %q is unknown", r.Index, s.Class)
		}
		expected = append(expected, s.Expected)
		v.Scheduled++
		v.Classes[string(s.Class)]++
		switch s.Class {
		case score.Success:
			v.Correct++
			v.CorrectByTimeout++
		case score.LateCorrect:
			v.CorrectByTimeout++
		case score.SchemaFailure:
			v.SchemaReasons[string(s.Reason)]++
		}
		v.LabelCounts[s.Expected]++
		col := s.Route
		if col == "" {
			col = noRoute
		}
		v.Confusion[s.Expected][col]++

		outcome := string(r.Outcome)
		if outcome == "" {
			outcome = "unsent"
		}
		v.Outcomes[outcome]++
		if r.ErrClass != "" {
			v.ErrorClasses[r.ErrClass]++
		}
		if r.Status != 0 {
			v.Statuses[strconv.Itoa(r.Status)]++
		}
		if r.FirstTokNs > 0 {
			first = append(first, r.FirstTokNs-r.IntendedNs)
		} else {
			firstExcl[outcome]++
		}
		if r.Outcome == loadgen.OutcomeCompleted {
			done = append(done, r.DoneNs-r.IntendedNs)
		} else {
			doneExcl[outcome]++
		}
		if len(r.Usage) > 0 {
			var u struct {
				PromptTokens     *int64 `json:"prompt_tokens"`
				CompletionTokens *int64 `json:"completion_tokens"`
			}
			if json.Unmarshal(r.Usage, &u) == nil {
				usage.Responses++
				if u.PromptTokens != nil {
					usage.PromptTokens += *u.PromptTokens
				}
				if u.CompletionTokens != nil {
					usage.CompletionTokens += *u.CompletionTokens
				}
			}
		}
		v.ContentChunks += r.ContentEvents
		v.ReasoningChunks += r.ReasoningChunks
	}
	v.FirstContent = latency("measured requests with visible content", first, firstExcl)
	v.Completed = latency("completed measured requests", done, doneExcl)
	v.Usage = usage
	return v, expected, nil
}

func latency(cohort string, ns []int64, excluded map[string]int) *PilotLatency {
	l := &PilotLatency{Cohort: cohort, N: len(ns), Excluded: excluded}
	if len(ns) == 0 {
		return l
	}
	slices.Sort(ns)
	l.MinNs, l.MedianNs, l.MaxNs = &ns[0], &ns[(len(ns)-1)/2], &ns[len(ns)-1]
	return l
}

func humanPilot(r *PilotReport) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	a, c := r.Endpoints[0].Name, r.Endpoints[1].Name

	w("Percentes pilot report (SPEC-HOSTED HA2)")
	w("instrument commit: %s", r.InstrumentCommit)
	w("config sha256: %s", r.ConfigSHA256)
	w("")
	if r.Demonstration != "" {
		w("%s", r.Demonstration)
	}
	if r.Incomplete != "" {
		w("%s", r.Incomplete)
	}
	if r.Demonstration != "" || r.Incomplete != "" {
		w("")
	}
	w("%s", r.Caveat)
	w("")
	w("== Disclosures ==")
	for _, d := range r.Disclosures {
		w("- %s", d)
	}
	w("")
	w("== Environment pins (SPEC-HOSTED §6) ==")
	w("not applicable or not verifiable against a hosted endpoint: %s", strings.Join(r.PinsNotVerifiable, "; "))
	w("")
	w("== Endpoints ==")
	for _, e := range r.Endpoints {
		params := "none"
		if e.Params != nil {
			if pj, err := json.Marshal(e.Params); err == nil {
				params = string(pj)
			}
		}
		w("%s: provider %s, account tier %s, requested model %s, params %s", e.Name, e.Provider, e.AccountTier, e.Model, params)
	}
	w("frozen labels: %s", strings.Join(r.Labels, ", "))
	w("")

	w("== Paired table ==")
	w("headline: correct complete answers by the %gs cutoff from intended dispatch / all scheduled measured requests", r.CutoffS)
	w("%-16s %-10s %-10s %-10s %-10s %-11s %-8s %s", "session", "date", "first", a, c, "difference", "segment", "pair")
	for _, p := range r.Pairs {
		diff := "-"
		if p.Difference != nil {
			diff = strconv.Itoa(*p.Difference)
		}
		status := "valid"
		if !p.Valid {
			status = "excluded: " + strings.Join(p.Reasons, "; ")
		}
		if p.Replaces != "" {
			status += " (replaces " + p.Replaces + ")"
		}
		if p.ReplacedBy != "" {
			status += " (replaced by " + p.ReplacedBy + ")"
		}
		first := p.First
		if first == "" {
			first = "-"
		}
		w("%-16s %-10s %-10s %-10s %-10s %-11s %-8d %s", p.Session, p.Date, first, headlineCell(p.Windows[0]), headlineCell(p.Windows[1]), diff, p.Segment, status)
	}
	w("difference = %s numerator minus %s numerator", a, c)
	w("")
	w("== Across pairs ==")
	for _, s := range r.Segments {
		line := fmt.Sprintf("segment %d (from %s, %d sessions, reported models %s | %s): %d valid pairs", s.Index, s.FirstSession, s.Sessions, orDash(s.ReportedModels[0]), orDash(s.ReportedModels[1]), s.ValidPairs)
		if s.MeanDiff != nil {
			line += fmt.Sprintf("; differences %s; mean %.2f; range %d to %d", intsText(s.Differences), *s.MeanDiff, *s.MinDiff, *s.MaxDiff)
		}
		w("%s", line)
	}
	w("")

	for _, p := range r.Pairs {
		w("== Session %s (%s) ==", p.Session, p.Date)
		for _, v := range p.Windows {
			if v.Missing {
				w("%s: no window", v.Endpoint)
				continue
			}
			pilotWindowText(w, r, v)
		}
		w("")
	}
	w("%s", r.Caveat)
	return b.String()
}

func pilotWindowText(w func(string, ...any), r *PilotReport, v *PilotWindowView) {
	valid := "yes"
	if !v.ClientValid {
		valid = "no: " + strings.Join(v.InvalidReasons, "; ")
	}
	w("%s: client-valid %s", v.Endpoint, valid)
	if in := v.Interrupted; in != nil {
		w("  interrupted: %s; scheduled %d, dispatched %d, unsent %d", in.Cause, in.Scheduled, in.Dispatched, in.Unsent)
	}
	w("  reported models: %s", orDash(strings.Join(v.ReportedModels, ", ")))
	w("  headline: %d/%d correct complete answers by the %gs cutoff", v.Correct, v.Scheduled, r.CutoffS)
	w("  correct by the %ds timeout: %d/%d (late correct %d)", r.TimeoutS, v.CorrectByTimeout, v.Scheduled, v.Classes[string(score.LateCorrect)])
	classes := make([]string, 0, len(score.Classes))
	for _, cl := range score.Classes {
		classes = append(classes, fmt.Sprintf("%s=%d", cl, v.Classes[string(cl)]))
	}
	w("  task outcomes: %s", strings.Join(classes, " "))
	w("  schema failures by reason: %s", orDash(classText(v.SchemaReasons)))
	w("  transport outcomes: %s", classText(v.Outcomes))
	w("  error classes: %s", orDash(classText(v.ErrorClasses)))
	w("  HTTP statuses: %s", orDash(classText(v.Statuses)))
	if n := v.ErrorClasses[loadgen.ErrConnect]; v.Scheduled > 0 && n*100 > v.Scheduled {
		w("  connect fraction above 1 percent: %d/%d", n, v.Scheduled)
	}
	w("  time to first visible content, client-observed: %s", latencyText(v.FirstContent))
	w("  completed-answer latency, client-observed: %s", latencyText(v.Completed))
	w("  provider-reported usage: %d responses carried usage; prompt tokens %d; completion tokens %d", v.Usage.Responses, v.Usage.PromptTokens, v.Usage.CompletionTokens)
	w("  chunks: content chunks %d; reasoning chunks %d", v.ContentChunks, v.ReasoningChunks)
	counts := make([]string, 0, len(r.Labels))
	for _, l := range r.Labels {
		counts = append(counts, fmt.Sprintf("%s=%d", l, v.LabelCounts[l]))
	}
	w("  frozen label counts: %s", strings.Join(counts, " "))
	w("  answer label (columns) against frozen label (rows):")
	cols := append(slices.Clone(r.Labels), noRoute)
	head := fmt.Sprintf("    %-12s", "")
	for _, col := range cols {
		head += fmt.Sprintf(" %10s", col)
	}
	w("%s", head)
	for _, l := range r.Labels {
		line := fmt.Sprintf("    %-12s", l)
		for _, col := range cols {
			line += fmt.Sprintf(" %10d", v.Confusion[l][col])
		}
		w("%s", line)
	}
}

func headlineCell(v *PilotWindowView) string {
	if v.Missing {
		return "-"
	}
	return fmt.Sprintf("%d/%d", v.Correct, v.Scheduled)
}

func latencyText(l *PilotLatency) string {
	s := fmt.Sprintf("%s, n=%d", l.Cohort, l.N)
	if l.N > 0 {
		s += fmt.Sprintf(", min %.1f ms, median %.1f ms, max %.1f ms", nsMs(*l.MinNs), nsMs(*l.MedianNs), nsMs(*l.MaxNs))
	}
	return s + "; excluded: " + orDash(classText(l.Excluded))
}

func nsMs(ns int64) float64 { return float64(ns) / 1e6 }

func intsText(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}
	return strings.Join(parts, ", ")
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
