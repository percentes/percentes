package report

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

// secs is s seconds in nanoseconds.
func secs(s float64) int64 { return int64(s * 1e9) }

// sent is a completed record at step, dispatched and written at atS
// seconds from the epoch, with prompt tokens reported where prompt is
// not negative.
func sent(step int, atS float64, prompt int) evidence.Record {
	r := evidence.Record{Step: step, IntendedNs: secs(atS), DispatchNs: secs(atS), WroteRequestNs: secs(atS), FirstTokNs: secs(atS + 0.5),
		DoneNs: secs(atS + 1), Outcome: loadgen.OutcomeCompleted, InTime: true}
	if prompt >= 0 {
		r.Usage = json.RawMessage(fmt.Sprintf(`{"prompt_tokens":%d}`, prompt))
	}
	return r
}

// ruled is cfg validated, with the ceiling rule's verdict at a binding
// rate of 1 over plan, the expected plan where plan is nil.
func ruled(t *testing.T, cfg *config.Config, plan *admission.Plan) (*config.Config, *admission.Verdict) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p := admission.ExpectedPlan(cfg, 1)
	if plan != nil {
		p = *plan
	}
	v, err := admission.CeilingRule(cfg, admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.6", Source: "https://example.test/pricing", Retrieved: "2026-10-07"}, p, 1)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, v
}

// refused is a status_429 record at step, dispatched at atS seconds from
// the epoch and written 2 ms later where written is set.
func refused(step int, atS float64, written bool) evidence.Record {
	r := evidence.Record{Step: step, IntendedNs: secs(atS), DispatchNs: secs(atS), DoneNs: secs(atS + 0.01),
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429, Status: 429}
	if written {
		r.WroteRequestNs = secs(atS) + 2_000_000
	}
	return r
}

// timedCell is a counted cell of recs from epoch, indexed in order.
func timedCell(label string, epoch time.Time, recs ...evidence.Record) CellRecords {
	c := CellRecords{Cell: label, Outcome: evidence.CellCounted, EpochUTC: epoch}
	for i, r := range recs {
		r.Index, r.Shape, r.CeilingTokens = int64(i), "S", 512
		c.Records = append(c.Records, r)
	}
	return c
}

// A cell's records give one send each dispatched request, at the epoch
// plus its offsets, its write time where recorded and its reported prompt
// count; a probe or ceiling-check exchange gives one where it was
// dispatched. A send ends at its done time where it completed, returned
// an empty stream or got a status other than 200, and otherwise at the
// hard timeout after its dispatch, a reset or malformed stream's error
// time included.
func TestHistorySends(t *testing.T) {
	epoch := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	unsent := evidence.Record{Step: 1, IntendedNs: secs(3)}
	hung := evidence.Record{Step: 1, IntendedNs: secs(4), DispatchNs: secs(4), WroteRequestNs: secs(4.001), Outcome: loadgen.OutcomeCensored}
	reset := evidence.Record{Step: 1, IntendedNs: secs(5), DispatchNs: secs(5), WroteRequestNs: secs(5), DoneNs: secs(7), Status: 200,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset}
	refused := evidence.Record{Step: 1, IntendedNs: secs(6), DispatchNs: secs(6), WroteRequestNs: secs(6), DoneNs: secs(6.2), Status: 429,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429}
	empty := evidence.Record{Step: 1, IntendedNs: secs(8), DispatchNs: secs(8), WroteRequestNs: secs(8), DoneNs: secs(10), Status: 200,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrEmptyStream}
	malformed := evidence.Record{Step: 1, IntendedNs: secs(9), DispatchNs: secs(9), WroteRequestNs: secs(9), DoneNs: secs(11), Status: 200,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrMalformedStream}
	warm := sent(1, 1, 7)
	warm.Warmup = true
	recs := []evidence.Record{warm, sent(1, 2, -1), unsent, hung, reset, refused, empty, malformed}
	for i := range recs {
		recs[i].Index, recs[i].CeilingTokens = int64(i), 512
	}
	got := RecordSends("groq", "c", epoch, recs, 1200, 90)
	if len(got) != 7 {
		t.Fatalf("%d sends, want 7: %+v", len(got), got)
	}
	for i, want := range []time.Time{epoch.Add(2 * time.Second), epoch.Add(3 * time.Second), epoch.Add(94 * time.Second), epoch.Add(95 * time.Second), epoch.Add(6200 * time.Millisecond),
		epoch.Add(10 * time.Second), epoch.Add(99 * time.Second)} {
		if !got[i].EndUTC.Equal(want) {
			t.Errorf("send %d ends %s, want %s", i, got[i].EndUTC, want)
		}
	}
	if !got[3].LatestUTC.Equal(epoch.Add(7 * time.Second)) {
		t.Errorf("reset send latest %s, want its error time", got[3].LatestUTC)
	}
	if !got[6].LatestUTC.Equal(epoch.Add(11 * time.Second)) {
		t.Errorf("malformed send latest %s, want its error time", got[6].LatestUTC)
	}
	w := epoch.Add(time.Second)
	if s := got[0]; s.Kind != SendWarmup || !s.DispatchUTC.Equal(w) || !s.WroteUTC.Equal(w) || !s.LatestUTC.Equal(epoch.Add(2*time.Second)) ||
		s.PromptTokens == nil || *s.PromptTokens != 7 || s.InputBound != 1200 || s.Ceiling != 512 {
		t.Errorf("warm-up send %+v", s)
	}
	if s := got[1]; s.Kind != SendLadder || s.PromptTokens != nil || s.Index != 1 {
		t.Errorf("ladder send %+v", s)
	}
	if s := got[2]; !s.LatestUTC.Equal(epoch.Add(94*time.Second)) || !s.WroteUTC.Equal(epoch.Add(4001*time.Millisecond)) {
		t.Errorf("censored send %+v, latest at the hard timeout", s)
	}

	ex := &probe.Exchange{DispatchUTC: epoch, WroteRequestNs: 3_000_000, DoneNs: 1_000_000_000}
	prompt := 900
	s, ok := ExchangeSend("groq", "ceiling check", SendCheck, 4, ex, nil, &prompt, 1200, 1024, 90)
	if !ok || !s.WroteUTC.Equal(epoch.Add(3*time.Millisecond)) || !s.LatestUTC.Equal(epoch.Add(time.Second)) || *s.PromptTokens != 900 || s.Ceiling != 1024 {
		t.Errorf("check send %+v", s)
	}
	if !s.EndUTC.Equal(epoch.Add(90 * time.Second)) {
		t.Errorf("check send with no status ends %s, want the hard timeout", s.EndUTC)
	}
	ex.Status = 200
	for _, c := range []struct {
		sh  *probe.Shape
		end time.Time
	}{
		{&probe.Shape{Done: true}, epoch.Add(time.Second)},
		{&probe.Shape{Done: true, ReadError: "reset"}, epoch.Add(90 * time.Second)},
		{&probe.Shape{}, epoch.Add(90 * time.Second)},
		{nil, epoch.Add(90 * time.Second)},
	} {
		if s, _ := ExchangeSend("groq", "ceiling check", SendCheck, 4, ex, c.sh, &prompt, 1200, 1024, 90); !s.EndUTC.Equal(c.end) {
			t.Errorf("status 200, stream %+v: ends %s, want %s", c.sh, s.EndUTC, c.end)
		}
	}
	ex.Status = 429
	if s, _ := ExchangeSend("groq", "ceiling check", SendCheck, 4, ex, nil, &prompt, 1200, 1024, 90); !s.EndUTC.Equal(epoch.Add(time.Second)) {
		t.Errorf("status 429 ends %s, want its done time", s.EndUTC)
	}
	if s, ok := ExchangeSend("groq", "probe", SendProbe, 0, &probe.Exchange{DispatchUTC: epoch}, nil, nil, 1200, 512, 90); !ok || s.WroteUTC != nil || !s.LatestUTC.Equal(epoch.Add(90*time.Second)) {
		t.Errorf("unwritten probe send %+v", s)
	}
	if _, ok := ExchangeSend("groq", "probe", SendProbe, 1, nil, nil, nil, 1200, 512, 90); ok {
		t.Error("an absent wrong-model exchange gave a send")
	}
	if _, ok := ExchangeSend("groq", "probe", SendProbe, 1, &probe.Exchange{}, nil, nil, 1200, 512, 90); ok {
		t.Error("an undispatched exchange gave a send")
	}
}

// A Shape R route's page says that its timings describe no Shape S
// request; a Shape S page does not.
func TestShapeRHeading(t *testing.T) {
	cfg, v := stepsConfig(t)
	st, err := NewStepTables(cfg, v, []CellRecords{cellOf("weekday-00", outcome{1, LineInTime, 0, 10})})
	if err != nil {
		t.Fatal(err)
	}
	const line = "Shape R is a short request that probes request-count limits; its timings describe no Shape S request.\n"
	if text := st.Text(); !strings.Contains(text, "Request shape S; binding rate 1 requests per second (rps), binding limit ") || strings.Contains(text, line) {
		t.Errorf("Shape S page:\n%s", text)
	}
	st.Shape = config.ShapeR
	if text := st.Text(); !strings.Contains(text, "Request shape R; binding rate 1 requests per second (rps), binding limit ") || !strings.Contains(text, line) {
		t.Errorf("Shape R page:\n%s", text)
	}
}

// Each token limit's printed counting names what its window count adds
// up for a send: prompt tokens, the output ceiling, both, or prompt
// tokens plus the ceiling times the output burndown rate.
func TestTokenCounting(t *testing.T) {
	prompt := 100
	s := &Send{InputBound: 128, PromptTokens: &prompt, Ceiling: 16}
	ix := &historyIndex{}
	for _, c := range []struct {
		l     config.Limit
		units int64
		text  string
	}{
		{config.Limit{Dimension: config.LimitInputTPM}, 100, "as counted in reported prompt tokens"},
		{config.Limit{Dimension: config.LimitOutputTPM}, 16, "as counted at the output ceiling"},
		{config.Limit{Dimension: config.LimitTPM, Counting: config.CountingInput}, 100, "as counted in reported prompt tokens"},
		{config.Limit{Dimension: config.LimitTPM, Counting: config.CountingInputOutput}, 116, "as counted in reported prompt tokens plus the ceiling"},
		{config.Limit{Dimension: config.LimitTPD, Counting: config.CountingOutputBurndown, OutputBurndown: 2}, 132,
			"as counted in reported prompt tokens plus the ceiling times the output burndown rate of 2"},
		{config.Limit{Dimension: config.LimitRPM}, 1, ""},
	} {
		got, _ := ix.demand(&c.l, s)
		if got.Cmp(big.NewRat(c.units, 1)) != 0 || tokenCounting(&c.l) != c.text {
			t.Errorf("%s %s: demand %s, counting %q; want %d, %q", c.l.Dimension, c.l.Counting, got.RatString(), tokenCounting(&c.l), c.units, c.text)
		}
	}
}

// A limit that counts output at generation also counts, in each window,
// the sends written before it that may have ended in it: two written
// 61 s before a refusal and ending at it take a 60 s window of 16 tokens
// to 48 against a share of 20, which reads unresolved, while the same
// sends ended before the window leave it at or below. A window whose
// sends by write time exceed the share reads unresolved, never above.
func TestAttributionOutputAtGeneration(t *testing.T) {
	at := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	old := at.Add(-61 * time.Second)
	send := func(wrote, end time.Time) Send {
		return Send{Route: "azure", WroteUTC: &wrote, DispatchUTC: wrote, LatestUTC: end, EndUTC: end, Ceiling: 16}
	}
	cfg := &config.Config{}
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitOutputTPM, Value: 20, Marking: config.MarkingPublic, OutputAtGeneration: true}}
	for _, c := range []struct {
		name  string
		sends []Send
		label string
		count float64
		bound float64
	}{
		{"ended in the window", []Send{send(old, at), send(old, at), send(at, at)}, LabelUnresolved, 16, 48},
		{"ended before the window", []Send{send(old, old.Add(time.Second)), send(old, old.Add(time.Second)), send(at, at)}, LabelAtOrBelow, 16, 16},
		{"ended at the window's open start", []Send{send(old, at.Add(-60*time.Second)), send(at, at)}, LabelAtOrBelow, 16, 16},
		{"above by write time", []Send{send(at.Add(-time.Second), at), send(at, at)}, LabelUnresolved, 32, 32},
	} {
		ix, err := newHistoryIndex(&History{Sends: c.sends})
		if err != nil {
			t.Fatal(err)
		}
		r := &Refusal{WroteUTC: &at}
		if err := ix.attribute(r, cfg, make([]bool, len(cfg.Target.Limits)), 0); err != nil {
			t.Fatal(err)
		}
		wc := r.Windows[0]
		if r.Label != c.label || wc.Above || wc.Count != c.count || wc.Bound == nil || *wc.Bound != c.bound {
			t.Errorf("%s: label %s, window %+v, want %s with count %g and bound %g", c.name, r.Label, wc, c.label, c.count, c.bound)
		}
		if c.label == LabelUnresolved && !strings.Contains(strings.Join(r.Unresolved, "; "), "counts output at generation") {
			t.Errorf("%s: unresolved %q", c.name, r.Unresolved)
		}
	}

	cfg.Target.Limits[0].OutputAtGeneration = false
	ix, err := newHistoryIndex(&History{Sends: []Send{send(old, at), send(at.Add(-time.Second), at), send(at, at)}})
	if err != nil {
		t.Fatal(err)
	}
	r := &Refusal{WroteUTC: &at}
	if err := ix.attribute(r, cfg, make([]bool, len(cfg.Target.Limits)), 0); err != nil {
		t.Fatal(err)
	}
	if r.Label != LabelAbove || r.Windows[0].Bound != nil {
		t.Errorf("unflagged limit: label %s, window %+v", r.Label, r.Windows[0])
	}
}

// A send with no write time whose end reaches into the window of a limit
// that counts output at generation leaves that window unresolved, though
// it was written by a time before the window.
func TestAttributionOutputAtGenerationUnwritten(t *testing.T) {
	at := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	d := at.Add(-80 * time.Second)
	h := &History{Sends: []Send{{Route: "azure", DispatchUTC: d, LatestUTC: d.Add(time.Second), EndUTC: d.Add(90 * time.Second), Ceiling: 16}}}
	cfg := &config.Config{}
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitOutputTPM, Value: 20, Marking: config.MarkingPublic, OutputAtGeneration: true}}
	for _, flagged := range []bool{true, false} {
		cfg.Target.Limits[0].OutputAtGeneration = flagged
		ix, err := newHistoryIndex(h)
		if err != nil {
			t.Fatal(err)
		}
		r := &Refusal{WroteUTC: &at}
		if err := ix.attribute(r, cfg, make([]bool, len(cfg.Target.Limits)), 0); err != nil {
			t.Fatal(err)
		}
		if want := map[bool]string{true: LabelUnresolved, false: LabelAtOrBelow}[flagged]; r.Label != want {
			t.Errorf("flagged %t: label %s %q, want %s", flagged, r.Label, r.Unresolved, want)
		}
	}
}

// A dispatch the write limit refused sent nothing, and is no send.
func TestRecordSendsSkipsRefusedWrites(t *testing.T) {
	epoch := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	recs := []evidence.Record{
		{Index: 0, Step: 1, DispatchNs: 1e9, WroteRequestNs: 1e9 + 1},
		{Index: 1, Step: 1, DispatchNs: 2e9, WriteDenied: true},
		{Index: 2, Step: 1},
	}
	sends := RecordSends("azure-S", "cell", epoch, recs, 4000, 90)
	if len(sends) != 1 || sends[0].Index != 0 {
		t.Fatalf("sends %+v, want request 0 alone", sends)
	}
}

// A send's prompt count in exponent form is taken at its value.
func TestRecordSendsExponentPromptCount(t *testing.T) {
	epoch := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	recs := []evidence.Record{{Index: 1, DispatchNs: 1e9, WroteRequestNs: 1e9 + 1, Usage: json.RawMessage(`{"prompt_tokens":1.5e4,"completion_tokens":5.12e3}`)}}
	s := RecordSends("azure", "weekday-00", epoch, recs, 1200, 90)
	if len(s) != 1 || s[0].PromptTokens == nil || *s[0].PromptTokens != 15000 {
		t.Fatalf("sends %+v", s)
	}
}
