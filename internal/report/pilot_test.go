package report

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/score"
)

var updatePilot = flag.Bool("update-pilot", false, "rewrite the pilot report golden files")

var pilotLabels = []string{"billing", "technical", "account", "escalation"}

const pilotModel = "openai/gpt-oss-120b"

func pilotScoring(t *testing.T, expected []string) *evidence.Scoring {
	t.Helper()
	c, err := score.NewContract(pilotLabels)
	if err != nil {
		t.Fatal(err)
	}
	s, err := score.NewScorer(c, score.Rule{Cutoff: 10 * time.Second, Accepted: []string{"stop"}, Length: []string{"length"}, Refusal: []string{"content_filter"}})
	if err != nil {
		t.Fatal(err)
	}
	return &evidence.Scoring{Scorer: s, Expected: expected}
}

// records scores reqs with the pilot rule, as E1 writes them.
func records(t *testing.T, reqs []loadgen.Request, expected []string) []evidence.Record {
	t.Helper()
	cfg := &config.Config{Target: config.Target{Hosted: true}}
	return evidence.Records(cfg, &loadgen.Result{Requests: reqs}, evidence.Options{Scoring: pilotScoring(t, expected)})
}

func answer(label string) string { return `{"route":"` + label + `"}` }

// fixtureWindow is 126 requests 10 s apart: six warm-up, then 120 measured
// with balanced labels. Each request's behaviour is drawn from (session,
// endpoint, case) so the windows differ.
func fixtureWindow(t *testing.T, sess, ep int) *PilotWindow {
	t.Helper()
	const s = int64(time.Second)
	const ms = int64(time.Millisecond)
	var reqs []loadgen.Request
	var expected []string
	for i := 0; i < 126; i++ {
		j := max(i-6, 0)
		label := pilotLabels[(j+sess)%4]
		expected = append(expected, label)
		r := loadgen.Request{Index: int64(i), IntendedNs: int64(i) * 10 * s, ReportedModel: pilotModel}
		r.DispatchNs = r.IntendedNs + 200_000
		first := r.IntendedNs + 300*ms + int64(j%7)*20*ms + int64(ep)*50*ms
		ok := func(content string, done int64) {
			r.Outcome, r.FirstTokNs, r.DoneNs = loadgen.OutcomeCompleted, first, done
			r.Content, r.Tokens, r.FinishReason, r.Proto, r.Status = content, 5, "stop", "HTTP/1.1", 200
			r.Usage, r.UsageSeen, r.CompletionTokens = json.RawMessage(`{"prompt_tokens":350,"completion_tokens":60}`), true, 60
			r.ReasoningChunks = 40
		}
		h := (j*31 + sess*17 + ep*11) % 120
		switch {
		case i < 6:
			ok(answer(label), first+400*ms)
		case h == 0:
			ok(answer(pilotLabels[(j+sess+1)%4]), first+400*ms)
		case h == 1:
			ok(`{"route":"`+label+`","note":"x"}`, first+400*ms)
		case h == 2 && ep == 1:
			ok(answer(label), r.IntendedNs+12*s)
		case h == 3 && ep == 0:
			r.Outcome, r.ErrClass, r.Status, r.DoneNs, r.Proto = loadgen.OutcomeErrored, loadgen.ErrStatus429, 429, r.IntendedNs+90*ms, "HTTP/1.1"
		case (h == 4 || h == 5) && sess == 3 && ep == 1:
			r.Outcome, r.ErrClass, r.DoneNs, r.ErrText = loadgen.OutcomeErrored, loadgen.ErrConnect, r.IntendedNs+30*ms, "dial tcp: connection refused"
		case h == 6:
			r.Outcome, r.ErrClass, r.FinishReason, r.DoneNs, r.Status, r.Proto = loadgen.OutcomeErrored, loadgen.ErrEmptyStream, "length", r.IntendedNs+9*s, 200, "HTTP/1.1"
			r.ReasoningChunks = 4096
		case h == 7 && ep == 0:
			r.Outcome, r.FirstTokNs, r.Content, r.Tokens, r.Status, r.Proto = loadgen.OutcomeCensored, first, `{"rou`, 1, 200, "HTTP/1.1"
		default:
			ok(answer(label), first+400*ms+int64(j%5)*30*ms)
		}
		reqs = append(reqs, r)
	}
	return &PilotWindow{Valid: true, ReportedModels: []string{pilotModel}, Records: records(t, reqs, expected)}
}

func pilotFixture(t *testing.T) *PilotInput {
	t.Helper()
	sessions := []struct{ label, date, first, replaces string }{
		{"weekday-1", "2026-10-13", "groq", ""},
		{"weekend-1", "2026-10-17", "baseten", ""},
		{"weekend-2", "2026-10-18", "groq", ""},
		{"weekday-2", "2026-10-19", "baseten", ""},
		{"weekday-3", "2026-10-22", "groq", ""},
		{"weekend-3", "2026-10-24", "baseten", ""},
		{"reserve-weekend", "2026-10-31", "groq", "weekend-2"},
	}
	in := &PilotInput{
		InstrumentCommit: "0123456789abcdef0123456789abcdef01234567",
		ConfigSHA256:     strings.Repeat("ab", 32),
		Demonstration:    true,
		Labels:           pilotLabels,
		Cutoff:           10 * time.Second,
		Endpoints: [2]PilotEndpoint{
			{Name: "groq", Provider: "Groq", AccountTier: "Developer", Model: pilotModel, Params: &config.Params{MaxCompletionTokens: 4096, ReasoningEffort: "low", ReasoningFormat: "parsed", StreamOptions: &config.StreamOptions{IncludeUsage: true}}},
			{Name: "baseten", Provider: "Baseten", AccountTier: "Basic", Model: pilotModel, Params: &config.Params{MaxTokens: 4096, ReasoningEffort: "low", StreamOptions: &config.StreamOptions{IncludeUsage: true}}},
		},
		Roles: &evidence.Roles{Task: "Percentes", CasesLabels: "a deterministic generator", Rubric: "Percentes", PairInterest: "none; Percentes chose the pair"},
	}
	// Session order differs from date order on input; the report sorts.
	for i, s := range sessions {
		ps := PilotSession{Label: s.label, Date: s.date, First: s.first, Replaces: s.replaces}
		ps.Windows[0], ps.Windows[1] = fixtureWindow(t, i, 0), fixtureWindow(t, i, 1)
		in.Sessions = append(in.Sessions, ps)
	}
	in.Sessions[0], in.Sessions[1] = in.Sessions[1], in.Sessions[0]
	bad := in.Sessions[2].Windows[1]
	bad.Valid, bad.InvalidReasons = false, []string{"G2: send skew max 61 ms above 50 ms"}
	return in
}

func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *updatePilot {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("%s differs from the golden file (run with -update-pilot and review the diff)", name)
	}
}

// The pilot report pair over six valid pairs, a client-invalid window and
// its replacement matches the golden files byte for byte.
func TestPilotReportGolden(t *testing.T) {
	raw, text, err := GeneratePilot(pilotFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "pilot-report.json", raw)
	checkGolden(t, "pilot-report.txt", []byte(text))
}

// pilotBanned are words of a ranked or inferential claim (HA2: no winner,
// no grade, no interval claim, no 99th percentile).
var pilotBanned = []string{"winner", "rank", "interval", "p99", "99th", "percentile", "grade", "confidence"}

// No winner, rank, interval or 99th-percentile word appears anywhere in
// either file, in a complete, an incomplete or a split series.
func TestPilotReportNamesNoWinner(t *testing.T) {
	complete := pilotFixture(t)
	incomplete := pilotFixture(t)
	incomplete.Sessions = incomplete.Sessions[:len(incomplete.Sessions)-1]
	split := pilotFixture(t)
	for _, s := range split.Sessions[3:] {
		s.Windows[1].ReportedModels = []string{"openai/gpt-oss-120b-2"}
	}
	for name, in := range map[string]*PilotInput{"complete": complete, "incomplete": incomplete, "split": split} {
		raw, text, err := GeneratePilot(in)
		if err != nil {
			t.Fatal(err)
		}
		for _, out := range []string{string(raw), text} {
			lower := strings.ToLower(out)
			for _, word := range pilotBanned {
				if strings.Contains(lower, word) {
					t.Errorf("%s: %q appears in the report", name, word)
				}
			}
		}
	}
}

// The headline numerator is complete correct answers by the cutoff over
// every scheduled measured request: warm-up excluded, late correct,
// errored, censored and unsent requests in the denominator only.
func TestPilotHeadlineCounts(t *testing.T) {
	const s = int64(time.Second)
	var reqs []loadgen.Request
	var expected []string
	add := func(intended int64, label string, f func(*loadgen.Request)) {
		r := loadgen.Request{Index: int64(len(reqs)), IntendedNs: intended, DispatchNs: intended + 1, FirstTokNs: intended + s/2, DoneNs: intended + s,
			Outcome: loadgen.OutcomeCompleted, Content: answer(label), FinishReason: "stop", Status: 200, Tokens: 3}
		if f != nil {
			f(&r)
		}
		reqs = append(reqs, r)
		expected = append(expected, label)
	}
	add(0, "billing", nil)                                                                // warm-up
	add(50*s, "account", nil)                                                             // warm-up
	add(60*s, "billing", nil)                                                             // success at the measured window's first instant
	add(70*s, "technical", func(r *loadgen.Request) { r.DoneNs = r.IntendedNs + 10*s })   // success at the cutoff
	add(80*s, "account", func(r *loadgen.Request) { r.DoneNs = r.IntendedNs + 10*s + 1 }) // late correct
	add(90*s, "escalation", func(r *loadgen.Request) { r.Content = answer("billing") })   // wrong label
	add(100*s, "billing", func(r *loadgen.Request) {
		r.Outcome, r.ErrClass, r.Status, r.FirstTokNs, r.Content = loadgen.OutcomeErrored, loadgen.ErrStatus429, 429, 0, ""
	})
	add(110*s, "technical", func(r *loadgen.Request) {
		r.Outcome, r.DispatchNs, r.FirstTokNs, r.DoneNs, r.Content, r.Status = "", 0, 0, 0, "", 0
	})
	win := &PilotWindow{Valid: true, Records: records(t, reqs, expected)}
	v, exp, err := pilotWindow(&PilotInput{Labels: pilotLabels, Cutoff: 10 * time.Second}, "groq", win)
	if err != nil {
		t.Fatal(err)
	}
	if v.Correct != 2 || v.Scheduled != 6 || v.CorrectByTimeout != 3 {
		t.Errorf("correct %d, scheduled %d, by timeout %d; want 2, 6, 3", v.Correct, v.Scheduled, v.CorrectByTimeout)
	}
	if len(exp) != 6 || exp[0] != "billing" || exp[5] != "technical" {
		t.Errorf("frozen labels %q", exp)
	}
	if v.Confusion["escalation"]["billing"] != 1 || v.Confusion["billing"]["none"] != 1 || v.Confusion["technical"]["none"] != 1 || v.Confusion["account"]["account"] != 1 {
		t.Errorf("confusion %v", v.Confusion)
	}
	if v.LabelCounts["billing"] != 2 || v.LabelCounts["technical"] != 2 || v.LabelCounts["account"] != 1 || v.LabelCounts["escalation"] != 1 {
		t.Errorf("label counts %v", v.LabelCounts)
	}
	if v.Outcomes["completed"] != 4 || v.Outcomes["errored"] != 1 || v.Outcomes["unsent"] != 1 || v.Statuses["429"] != 1 || v.Statuses["200"] != 4 {
		t.Errorf("outcomes %v, statuses %v", v.Outcomes, v.Statuses)
	}
	if v.FirstContent.N != 4 || v.FirstContent.Excluded["errored"] != 1 || v.FirstContent.Excluded["unsent"] != 1 {
		t.Errorf("first content %+v", v.FirstContent)
	}
	if c := v.Completed; c.N != 4 || *c.MinNs != s || *c.MedianNs != s || *c.MaxNs != 10*s+1 {
		t.Errorf("completed %+v", c)
	}
}

// A pair is excluded, with its reasons and no difference, when a window is
// missing, client-invalid, interrupted or instrument-incompatible, or the
// windows carry different frozen labels; the incomplete label then names
// the pair count per segment.
func TestPilotPairExclusions(t *testing.T) {
	in := pilotFixture(t)
	in.Demonstration = false
	in.Sessions = in.Sessions[:6]
	byLabel := map[string]*PilotSession{}
	for i := range in.Sessions {
		byLabel[in.Sessions[i].Label] = &in.Sessions[i]
	}
	byLabel["weekday-1"].Windows[0] = nil
	byLabel["weekday-2"].Windows[0].Interrupted = &loadgen.Interruption{Cause: "ledger cap reached", Scheduled: 126, Dispatched: 80, Unsent: 46}
	w3 := byLabel["weekday-3"].Windows[1]
	w3.Records[10].Score.Class = score.InstrumentIncompatible
	w3.Records[10].Score.Route = ""
	w4 := byLabel["weekend-3"].Windows[0]
	w4.Records[20].Score.Expected = "billing"
	w4.Records[21].Score.Expected = "billing"
	rep, err := newPilotReport(in)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"weekday-1": "no groq window",
		"weekend-2": "baseten window client-invalid: G2: send skew max 61 ms above 50 ms",
		"weekday-2": "groq window interrupted: ledger cap reached",
		"weekday-3": "baseten: 1 instrument-incompatible attempts",
		"weekend-3": "the two windows' measured requests carry different frozen labels",
	}
	for _, p := range rep.Pairs {
		reason, excluded := want[p.Session]
		if p.Valid == excluded || (p.Difference != nil) == excluded {
			t.Errorf("%s: valid %v, difference %v", p.Session, p.Valid, p.Difference)
		}
		if excluded && (len(p.Reasons) != 1 || p.Reasons[0] != reason) {
			t.Errorf("%s: reasons %q, want %q", p.Session, p.Reasons, reason)
		}
	}
	if rep.ValidPairs != 1 || rep.Demonstration != "" {
		t.Errorf("valid pairs %d, demonstration %q", rep.ValidPairs, rep.Demonstration)
	}
	const inc = "INCOMPLETE: 1 compatible, client-valid pairs in all (segment 1: 1); the design needs 6 in one segment."
	if !strings.HasPrefix(rep.Incomplete, inc) {
		t.Errorf("incomplete %q", rep.Incomplete)
	}
	_, text, err := GeneratePilot(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, inc) || strings.Contains(text, "DEMONSTRATION") {
		t.Error("text: incomplete label missing or demonstration label present")
	}
	if !strings.Contains(text, "interrupted: ledger cap reached; scheduled 126, dispatched 80, unsent 46") || !strings.Contains(text, "groq: no window") {
		t.Error("text: interrupted counts or missing window not shown")
	}
}

// A change in an endpoint's reported model identity starts a new segment;
// mean and range are taken within each segment, and no segment of six
// valid pairs labels the pilot incomplete.
func TestPilotSeriesSplit(t *testing.T) {
	in := pilotFixture(t)
	rep0, err := newPilotReport(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep0.Segments) != 1 || rep0.Segments[0].ValidPairs != 6 || rep0.Incomplete != "" {
		t.Fatalf("unsplit: segments %+v, incomplete %q", rep0.Segments, rep0.Incomplete)
	}
	var diffs []int
	for _, p := range rep0.Pairs {
		if p.Difference != nil {
			diffs = append(diffs, *p.Difference)
		}
	}
	sum, lo, hi := 0, diffs[0], diffs[0]
	for _, d := range diffs {
		sum, lo, hi = sum+d, min(lo, d), max(hi, d)
	}
	if s := rep0.Segments[0]; *s.MeanDiff != float64(sum)/6 || *s.MinDiff != lo || *s.MaxDiff != hi {
		t.Errorf("mean %v min %v max %v; want %v %v %v", *s.MeanDiff, *s.MinDiff, *s.MaxDiff, float64(sum)/6, lo, hi)
	}

	for i := range in.Sessions {
		if in.Sessions[i].Date >= "2026-10-19" {
			in.Sessions[i].Windows[1].ReportedModels = []string{"openai/gpt-oss-120b-2"}
		}
	}
	rep, err := newPilotReport(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Segments) != 2 || rep.Segments[0].FirstSession != "weekday-1" || rep.Segments[1].FirstSession != "weekday-2" {
		t.Fatalf("segments %+v", rep.Segments)
	}
	if rep.Segments[0].ValidPairs != 2 || rep.Segments[1].ValidPairs != 4 || rep.Segments[0].Sessions != 3 {
		t.Errorf("segment pairs %d and %d, first segment sessions %d", rep.Segments[0].ValidPairs, rep.Segments[1].ValidPairs, rep.Segments[0].Sessions)
	}
	if !strings.Contains(rep.Incomplete, "(segment 1: 2, segment 2: 4)") {
		t.Errorf("incomplete %q", rep.Incomplete)
	}
}

// The output ceiling disclosure states one value when both endpoints
// carry it and names a difference otherwise; the shared-identifier caveat
// appears only for one shared identifier.
func TestPilotDisclosures(t *testing.T) {
	in := pilotFixture(t)
	d := pilotDisclosures(in)
	if !contains(d, "Output ceiling, the same value on both endpoints: groq max_completion_tokens=4096; baseten max_tokens=4096.") {
		t.Errorf("ceiling disclosure missing: %q", d)
	}
	if !contains(d, "Both endpoints were sent the model identifier openai/gpt-oss-120b. A shared identifier does not establish identical weights, numerical precision or backend versions.") {
		t.Errorf("shared-identifier caveat missing: %q", d)
	}
	if d[0] != PilotOneAuthor {
		t.Errorf("first disclosure %q", d[0])
	}
	in.Endpoints[1].Params = &config.Params{MaxTokens: 2048}
	in.Endpoints[1].Model = "other"
	d = pilotDisclosures(in)
	if !contains(d, "Output ceiling DIFFERS between the endpoints: groq max_completion_tokens=4096; baseten max_tokens=2048.") {
		t.Errorf("differing ceiling not named: %q", d)
	}
	for _, s := range d {
		if strings.Contains(s, "shared identifier") {
			t.Errorf("shared-identifier caveat with two identifiers: %q", s)
		}
	}
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

// A measured request with no score or a scorer error, an unknown label,
// and a malformed session list are refused.
func TestPilotRefusals(t *testing.T) {
	cases := map[string]func(in *PilotInput){
		"unscored":            func(in *PilotInput) { in.Sessions[0].Windows[0].Records[10].Score = nil },
		"scorer error":        func(in *PilotInput) { in.Sessions[0].Windows[0].Records[10].Score.Error = "score: x" },
		"unknown expected":    func(in *PilotInput) { in.Sessions[0].Windows[0].Records[10].Score.Expected = "sales" },
		"unknown route":       func(in *PilotInput) { in.Sessions[0].Windows[0].Records[10].Score.Route = "sales" },
		"unknown class":       func(in *PilotInput) { in.Sessions[0].Windows[0].Records[10].Score.Class = "maybe" },
		"repeated session":    func(in *PilotInput) { in.Sessions[1].Label = in.Sessions[0].Label },
		"bad date":            func(in *PilotInput) { in.Sessions[0].Date = "13 Oct" },
		"unknown first":       func(in *PilotInput) { in.Sessions[0].First = "gemini" },
		"replaces nothing":    func(in *PilotInput) { in.Sessions[6].Replaces = "weekend-9" },
		"same endpoints":      func(in *PilotInput) { in.Endpoints[1].Name = "groq" },
		"no labels":           func(in *PilotInput) { in.Labels = nil },
		"no cutoff":           func(in *PilotInput) { in.Cutoff = 0 },
		"no sessions":         func(in *PilotInput) { in.Sessions = nil },
		"replaces itself":     func(in *PilotInput) { in.Sessions[6].Replaces = in.Sessions[6].Label },
		"warm-up unscored ok": nil,
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := pilotFixture(t)
			if mutate == nil {
				in.Sessions[0].Windows[0].Records[2].Score = nil
				if _, _, err := GeneratePilot(in); err != nil {
					t.Fatalf("warm-up record without a score refused: %v", err)
				}
				return
			}
			mutate(in)
			if _, _, err := GeneratePilot(in); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}
