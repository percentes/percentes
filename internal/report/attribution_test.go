package report

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"
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

// refusalAt is the refusal of request index in st's only cell.
func refusalAt(t *testing.T, st *StepTables, index int64) *Refusal {
	t.Helper()
	for k := range st.Cells[0].Steps {
		if seg := st.Cells[0].Steps[k].Refusals; seg != nil {
			for i := range seg.Refusals {
				if seg.Refusals[i].Index == index {
					return &seg.Refusals[i]
				}
			}
		}
	}
	t.Fatalf("no refusal of request %d", index)
	return nil
}

// Each status_429 is labelled from the account group's sends by write
// time over (t - W, t] in every evaluation window: above where the
// recorded sends exceed the limit's share in a window, naming it;
// unresolved where the refused request or a send that may fall in a
// window has no write time, or a history gap overlaps a window; at or
// below otherwise. Token counts take reported prompt tokens plus the
// ceiling, or the input bound where usage is unreported, and say so.
func TestAttributeLabels(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg.Target.Limits[0].EvaluationWindowsS = []float64{1, 10, 60}
	cfg.Target.ErrorDocs = []config.ErrorDoc{{Type: "rate_limit_error", Status: 429, Meaning: "rate limit", Source: "https://example.test/errors", Retrieved: "2026-10-07"}}
	cfg, v := ruled(t, cfg, nil)
	epoch := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	var recs []evidence.Record
	// Sixteen sends in (99.9 s, 100.9 s], then the refusal at 100.9 s: 17
	// in 1 s against a share of 1000 / 60.
	for i := 0; i < 16; i++ {
		recs = append(recs, sent(1, 99.95+float64(i)*0.05, 100))
	}
	a := refused(1, 100.898, true)
	a.ErrorType, a.ErrorCode = "rate_limit_error", "rate_limited"
	a.Headers = map[string][]string{"x-ratelimit-remaining-requests": {"0"}, "content-type": {"application/json"}}
	recs = append(recs, a, refused(1, 130, true))
	// An unwritten send from 197 s to 199 s: inside C's 10 s window, not
	// its 1 s window, which opens after 199 s.
	unwritten := sent(2, 197, 100)
	unwritten.WroteRequestNs, unwritten.DoneNs = 0, secs(199)
	recs = append(recs, unwritten, refused(2, 199.998, true), refused(2, 260, false), refused(3, 400, true))
	cell := timedCell("weekday-12", epoch, recs...)
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	gapTo := epoch.Add(355 * time.Second)
	h := &History{Group: "groq", Routes: []string{"groq"}, Sends: RecordSends("groq", "weekday-12", epoch, cell.Records, 1200, 90),
		Gaps: []HistoryGap{{Route: "groq", FromUTC: epoch.Add(350 * time.Second), ToUTC: &gapTo, Reason: "a test gap"}}}
	if err := st.Attribute(cfg, v, h); err != nil {
		t.Fatal(err)
	}

	ra := refusalAt(t, st, 16)
	if ra.Label != LabelAbove || !slices.Equal(ra.AboveIn, []string{"requests_per_minute over 1 s"}) || ra.Unresolved != nil {
		t.Errorf("refusal A: %s above %q unresolved %q", ra.Label, ra.AboveIn, ra.Unresolved)
	}
	if len(ra.Windows) != 4 {
		t.Fatalf("refusal A: %d windows, want RPM over 1, 10 and 60 s and TPM over 60 s", len(ra.Windows))
	}
	if w := ra.Windows[0]; w.Requests != 17 || w.Count != 17 || !near(w.Share, 1000.0/60) || !w.Above {
		t.Errorf("refusal A, 1 s: %+v", w)
	}
	if w := ra.Windows[3]; w.Dimension != config.LimitTPM || w.Requests != 17 || w.Count != 16*612+1712 || w.AtBound != 1 || w.Share != 80000 || w.Above {
		t.Errorf("refusal A, tokens: %+v", w)
	}
	if ra.WriteDelayNs != 2_000_000 || ra.Documented == nil || ra.Documented.Meaning != "rate limit" || len(ra.Headers) != 1 {
		t.Errorf("refusal A: delay %d, documented %v, headers %v", ra.WriteDelayNs, ra.Documented, ra.Headers)
	}

	rb := refusalAt(t, st, 17)
	if rb.Label != LabelAtOrBelow || rb.AboveIn != nil || rb.Unresolved != nil {
		t.Errorf("refusal B: %s above %q unresolved %q", rb.Label, rb.AboveIn, rb.Unresolved)
	}
	if w := rb.Windows[2]; w.WindowS != 60 || w.Requests != 18 {
		t.Errorf("refusal B, 60 s: %+v", w)
	}
	if rb.Documented != nil {
		t.Errorf("refusal B: documented %v; a 429 with no recorded error type is unclassified", rb.Documented)
	}

	rc := refusalAt(t, st, 19)
	want := []string{"1 sends of route groq that may fall in the window have no write time"}
	if rc.Label != LabelUnresolved || !slices.Equal(rc.Unresolved, want) || rc.Windows[0].Unresolved != nil || rc.Windows[1].Unresolved == nil {
		t.Errorf("refusal C: %s %q, windows %+v", rc.Label, rc.Unresolved, rc.Windows)
	}
	if rd := refusalAt(t, st, 20); rd.Label != LabelUnresolved || !slices.Equal(rd.Unresolved, []string{"the refused request has no write time"}) || rd.Windows != nil {
		t.Errorf("refusal D: %s %q", rd.Label, rd.Unresolved)
	}
	re := refusalAt(t, st, 21)
	if re.Label != LabelUnresolved || len(re.Unresolved) != 1 || !strings.Contains(re.Unresolved[0], "the history may lack sends of route groq from 2026-11-03T12:05:50Z to 2026-11-03T12:05:55Z: a test gap") ||
		re.Windows[1].Unresolved != nil {
		t.Errorf("refusal E: %s %q", re.Label, re.Unresolved)
	}

	if l := st.Cells[0].Steps[0].Refusals.Labels; *l != (LabelCounts{Above: 1, AtOrBelow: 1}) {
		t.Errorf("step 1 labels %+v", *l)
	}
	if l := st.Cells[0].Steps[1].Refusals.Labels; *l != (LabelCounts{Unresolved: 2}) {
		t.Errorf("step 2 labels %+v", *l)
	}
	if a := st.Attribution; a == nil || a.Group != "groq" || a.Sends != len(cell.Records) || len(a.Gaps) != 1 || a.ResetDays != nil {
		t.Errorf("attribution %+v", a)
	}

	text := st.Text()
	for _, frag := range []string{
		"    status_429 by intended dispatch from the step's start, in 30 s bins: 1, 1, 0, 0, 0, 0, 0, 0\n",
		"  status_429 in the first 30 s of the step: 1 of 2\n",
		"    labels: 1 at or below the limit's share in every window; 1 above the limit's share in a window; 0 comparison unresolved; no cause inferred\n",
		"    request 16 at 0.948 s, written 2026-11-03T12:01:40.9Z, 2.0 ms after dispatch: refused with recorded sends above the limit's share in window requests_per_minute over 1 s; " +
			"requests_per_minute over 1 s: 17 requests against a share of 16.667 of a limit of 1000, as recorded; requests_per_minute over 10 s: 17 requests against a share of 166.667 of a limit of 1000, as recorded; " +
			"requests_per_minute over 60 s: 17 requests against a share of 1000 of a limit of 1000, as recorded; tokens_per_minute over 60 s: 17 requests, 11504 tokens as counted in reported prompt tokens plus the ceiling " +
			"(1 at the input bound, no usable prompt count reported) against a share of 80000 of a limit of 80000, as recorded; HTTP 429, error type \"rate_limit_error\", code \"rate_limited\", " +
			"documented as \"rate limit\" (https://example.test/errors, retrieved 2026-10-07); X-Ratelimit-Remaining-Requests: 0\n",
		"against a share of 80000 of a limit of 80000, as recorded; HTTP 429, an unclassified 429\n",
		"    request 20 at 63.000 s, no write time: comparison unresolved; the refused request has no write time; HTTP 429, an unclassified 429\n",
		"  dispatch to write: largest 2.0 ms over 18 written requests; 0 dispatched with no write time\n",
		"  dispatch to write: largest 2.0 ms over 1 written requests; 2 dispatched with no write time\n",
		"-- Account group groq: 22 recorded sends of routes groq, by write time, attribute each status_429 --\n",
		"The counts are this client's recorded sends; provider receipt times and the limiter's type and state are unknown, and the labels assume that the account carries no traffic from outside the instrument.\n",
		"  the history may lack sends of route groq from 2026-11-03T12:05:50Z to 2026-11-03T12:05:55Z: a test gap\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}

	for _, c := range []struct {
		name   string
		mutate func(*History, **admission.Verdict)
		want   string
	}{
		{"no history", func(h *History, _ **admission.Verdict) { *h = History{} }, `history of account group ""`},
		{"other group", func(h *History, _ **admission.Verdict) { h.Group = "azure" }, `history of account group "azure", route groq's is another`},
		{"route outside", func(h *History, _ **admission.Verdict) { h.Routes = []string{"baseten"} }, `route groq is not among the history's routes ["baseten"]`},
		{"no verdict", func(_ *History, v **admission.Verdict) { *v = nil }, "no ceiling-rule verdict"},
		{"price", func(h *History, _ **admission.Verdict) {
			h.Prices = []RoutePrice{{Route: "groq", InputTokens: 1200, OutputTokens: 512, PerRequestUSD: "x"}}
		}, `route groq price "x"`},
	} {
		h := &History{Group: "groq", Routes: []string{"groq"}}
		vv := v
		c.mutate(h, &vv)
		if err := st.Attribute(cfg, vv, h); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
		}
	}
	if err := st.Attribute(cfg, v, nil); err == nil || !strings.Contains(err.Error(), "no account-group history") {
		t.Errorf("nil history: %v", err)
	}
}

// A daily limit is counted over the provider's reset day in its zone,
// from midnight to the refusal, the 25-hour reset day of Sun 1 Nov 2026
// included; a refusal within the cell's clock-offset bound of a reset is
// unresolved. Each reset day's sends by write time print beside the
// reservation of its attempts and ceiling-check requests.
func TestAttributeResetDays(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg.Target.Limits = append(cfg.Target.Limits, config.Limit{Dimension: config.LimitRPD, Value: 10000, Marking: config.MarkingPublic,
		ResetZone: "America/Los_Angeles", Source: "rate-limits.png", Retrieved: "2026-10-24", SHA256: strings.Repeat("0", 64)})
	plan := admission.ExpectedPlan(cfg, 1)
	plan.Attempts = []admission.Attempt{{Start: time.Date(2026, 11, 1, 20, 0, 0, 0, time.UTC)}, {Start: time.Date(2026, 11, 2, 7, 30, 0, 0, time.UTC)}}
	for i := 0; i < 40; i++ {
		plan.ChecksUTC = append(plan.ChecksUTC, time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC))
	}
	cfg, v := ruled(t, cfg, &plan)
	epoch := time.Date(2026, 11, 2, 7, 59, 59, 0, time.UTC)
	cell := timedCell("weekday-04", epoch, refused(1, 0.498, true), sent(1, 1.5, 100))
	half := 0.25
	cell.ClockAbsOffsetS = &half
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	sends := RecordSends("groq", "weekday-04", epoch, cell.Records, 1200, 90)
	for _, at := range []time.Time{time.Date(2026, 11, 1, 7, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 6, 59, 59, 0, time.UTC)} {
		w := at
		sends = append(sends, Send{Route: "groq", Kind: SendProbe, DispatchUTC: at, WroteUTC: &w, LatestUTC: at, InputBound: 1200, Ceiling: 512})
	}
	h := &History{Group: "groq", Routes: []string{"groq"}, Sends: sends}
	if err := st.Attribute(cfg, v, h); err != nil {
		t.Fatal(err)
	}
	r := refusalAt(t, st, 0)
	w := r.Windows[len(r.Windows)-1]
	if r.Label != LabelAtOrBelow || w.ResetDay != "2026-11-01" || w.Requests != 2 || w.Share != 10000 || w.Unresolved != nil {
		t.Errorf("refusal 0.5 s before the reset, offset bound 0.25 s: %s, %+v", r.Label, w)
	}
	want := []ResetDaySent{
		{Dimension: config.LimitRPD, Zone: "America/Los_Angeles", Reservation: admission.Reservation{Date: "2026-10-30", Checks: 40, Reserved: 40}},
		{Dimension: config.LimitRPD, Zone: "America/Los_Angeles", Reservation: admission.Reservation{Date: "2026-11-01", Attempts: 2, Reserved: 2 * 832},
			SentRequests: 2, Sent: 2},
		{Dimension: config.LimitRPD, Zone: "America/Los_Angeles", Reservation: admission.Reservation{Date: "2026-11-02", Attempts: 1, Reserved: 832},
			SentRequests: 1, Sent: 1},
	}
	if got := st.Attribution.ResetDays; !slices.Equal(got, want) {
		t.Errorf("reset days %+v, want %+v", got, want)
	}
	text := st.Text()
	for _, frag := range []string{
		"; requests_per_day over the reset day 2026-11-01: 2 requests against a share of 10000 of a limit of 10000, as recorded;",
		"  reset day 2026-10-30 (America/Los_Angeles), requests_per_day: 40 reserved for 0 attempts and 40 ceiling-check requests; 0 requests sent by the account group\n",
		"  reset day 2026-11-01 (America/Los_Angeles), requests_per_day: 1664 reserved for 2 attempts and 0 ceiling-check requests; 2 requests sent by the account group\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}

	one := 1.0
	st.Cells[0].ClockAbsOffsetS = &one
	if err := st.Attribute(cfg, v, h); err != nil {
		t.Fatal(err)
	}
	if r := refusalAt(t, st, 0); r.Label != LabelUnresolved ||
		!slices.Equal(r.Unresolved, []string{"the refusal falls within the cell's clock-offset bound of 1 s of a reset of America/Los_Angeles"}) {
		t.Errorf("refusal 0.5 s before the reset, offset bound 1 s: %s %q", r.Label, r.Unresolved)
	}
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

// A route's cells stopped by a documented billing refusal print on a
// line of their own, as the operator's billing setting; their responses
// enter no step table, since a stopped cell is outside the frame.
func TestBillingStopsLine(t *testing.T) {
	cfg, v := stepsConfig(t)
	st, err := NewStepTables(cfg, v, []CellRecords{cellOf("weekday-00", outcome{1, LineInTime, 0, 10})})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(st.Text(), "Billing stops") {
		t.Errorf("a route with no billing stop prints the line:\n%s", st.Text())
	}
	st.BillingStops = []BillingStop{{Cell: "weekend-04-a1", Responses: 2, Reasons: []string{"route stop: HTTP 402, the operator's billing setting, a documented billing refusal: prepay credit depleted"}}}
	want := "-- Billing stops: the operator's billing setting, outside every provider-failure figure --\n" +
		"cell weekend-04-a1: 2 responses documented as a billing refusal; route stop: HTTP 402, the operator's billing setting, a documented billing refusal: prepay credit depleted\n"
	if text := st.Text(); !strings.Contains(text, want) {
		t.Errorf("text lacks %q:\n%s", want, text)
	}
	stopped := cellOf("weekend-04-a1", outcome{1, LineInTime, 0, 10})
	stopped.Outcome = evidence.CellStopped
	if _, err := NewStepTables(cfg, v, []CellRecords{stopped}); err == nil {
		t.Error("a stopped cell entered the step tables")
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

// A refusal compares against the limit values the refresh command read
// at its cell's start, the value printed beside each share; a limit read
// as unavailable keeps its recorded value, and a refresh record that does
// not match target.limits is refused.
func TestAttributeRefreshedLimits(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg.Target.Limits[0].EvaluationWindowsS = []float64{1, 60}
	cfg, v := ruled(t, cfg, nil)
	epoch := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	var records []evidence.Record
	for i := 0; i < 10; i++ {
		records = append(records, sent(1, 1+float64(i)*0.05, 20))
	}
	records = append(records, refused(1, 1.6, true))
	attribute := func(refreshed []evidence.RefreshedLimit) (*StepTables, error) {
		cell := timedCell("cell", epoch, records...)
		cell.Refreshed = refreshed
		st, err := NewStepTables(cfg, v, []CellRecords{cell})
		if err != nil {
			t.Fatal(err)
		}
		h := &History{Group: cfg.Release.AccountGroup, Routes: []string{cfg.Target.Name}, Sends: RecordSends(cfg.Target.Name, "cell", epoch, cell.Records, 1200, 90)}
		return st, st.Attribute(cfg, v, h)
	}
	read := func(rpm float64, unavailable string) []evidence.RefreshedLimit {
		out := make([]evidence.RefreshedLimit, len(cfg.Target.Limits))
		for i, l := range cfg.Target.Limits {
			out[i] = evidence.RefreshedLimit{Dimension: l.Dimension, Value: l.Value}
		}
		out[0].Value, out[0].Unavailable = rpm, unavailable
		return out
	}
	st, err := attribute(read(600, ""))
	if err != nil {
		t.Fatal(err)
	}
	r := refusalAt(t, st, 10)
	if w := r.Windows[0]; r.Label != LabelAbove || w.Share != 10 || w.Value != 600 || !w.ReadAtStart {
		t.Fatalf("11 writes in 1 s, 600 RPM read at the start (10/s), %g RPM recorded: label %s, window %+v", cfg.Target.Limits[0].Value, r.Label, w)
	}
	if text := st.Text(); !strings.Contains(text, "11 requests against a share of 10 of a limit of 600, as read at the cell's start;") {
		t.Errorf("text lacks the read limit:\n%s", text)
	}
	if st, err = attribute(read(0, "not published")); err != nil {
		t.Fatal(err)
	}
	if r := refusalAt(t, st, 10); r.Label != LabelAtOrBelow || r.Windows[0].Value != cfg.Target.Limits[0].Value || r.Windows[0].ReadAtStart {
		t.Errorf("unavailable read: label %s, window %+v", r.Label, r.Windows[0])
	}
	bad := read(600, "")
	bad[0].Dimension = config.LimitTPM
	if _, err := attribute(bad); err == nil || !strings.Contains(err.Error(), "cell cell: refresh limits[0]") {
		t.Errorf("mismatched refresh record: %v", err)
	}
	if _, err := attribute(bad[:1]); err == nil || !strings.Contains(err.Error(), "the refresh record holds 1 limits") {
		t.Errorf("short refresh record: %v", err)
	}
}

// A reset day counts the sends written in it: one dispatched before a
// reset and written after it counts on the later day, and one with no
// write time marks every day it could have been written in incomplete.
func TestResetDaysByWriteTime(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg.Target.Limits = append(cfg.Target.Limits, config.Limit{Dimension: config.LimitRPD, Value: 10000, Marking: config.MarkingPublic,
		ResetZone: "America/Los_Angeles", Source: "limits", Retrieved: "2026-10-24", SHA256: strings.Repeat("0", 64)})
	at := time.Date(2026, 11, 14, 8, 0, 0, 0, time.UTC)
	plan := admission.ExpectedPlan(cfg, 1)
	plan.Attempts = []admission.Attempt{{Start: at}}
	cfg, v := ruled(t, cfg, &plan)
	cell := timedCell("cell", at, refused(1, 300, true))
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	before, after := at.Add(-time.Second), at.Add(time.Second)
	h := &History{Group: cfg.Release.AccountGroup, Routes: []string{cfg.Target.Name}, Sends: []Send{
		{Route: cfg.Target.Name, DispatchUTC: before, WroteUTC: &after, LatestUTC: after},
		{Route: cfg.Target.Name, DispatchUTC: before, LatestUTC: after},
	}}
	if err = st.Attribute(cfg, v, h); err != nil {
		t.Fatal(err)
	}
	days := st.Attribution.ResetDays
	if len(days) != 2 || days[0].SentRequests != 0 || days[1].SentRequests != 1 || !days[0].Incomplete || !days[1].Incomplete {
		t.Fatalf("reset days %+v", days)
	}
}

// A negative reported prompt count is taken at the input bound, and a
// count too large to add to the ceiling in int is summed exactly, so
// neither can cancel the demand of other sends: 47 sends of 1200 prompt
// tokens and the 512 ceiling hold 80464 tokens against the 80000 share.
func TestMalformedUsageAtBound(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg, v := ruled(t, cfg, nil)
	epoch := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	for _, usage := range []string{`{"prompt_tokens":-10000}`, `{"prompt_tokens":9223372036854775807}`} {
		var records []evidence.Record
		for i := 0; i < 47; i++ {
			records = append(records, sent(1, 61+float64(i), 1200))
		}
		bad := sent(1, 109, -1)
		bad.Usage = json.RawMessage(usage)
		records = append(records, bad, refused(1, 110, true))
		cell := timedCell("weekday-12", epoch, records...)
		st, err := NewStepTables(cfg, v, []CellRecords{cell})
		if err != nil {
			t.Fatal(err)
		}
		h := &History{Group: "groq", Routes: []string{"groq"}, Sends: RecordSends("groq", "weekday-12", epoch, cell.Records, 1200, 90)}
		if negative := -1; strings.Contains(usage, "-") {
			if h.Sends[47].PromptTokens != nil {
				t.Fatalf("%s read as %d", usage, *h.Sends[47].PromptTokens)
			}
			h.Sends[47].PromptTokens = &negative
		}
		if err = st.Attribute(cfg, v, h); err != nil {
			t.Fatal(err)
		}
		r := refusalAt(t, st, int64(len(records)-1))
		if r.Label != LabelAbove {
			t.Errorf("%s: label %s, windows %+v", usage, r.Label, r.Windows)
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

// A reset day's token count notes its sends with no usable prompt count
// as a window count does, and one holding a saturated prompt count prints
// as at least a sum no greater than the exact one.
func TestResetDayCountNotes(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg.Target.Limits = append(cfg.Target.Limits, config.Limit{Dimension: config.LimitTPD, Value: 1e7, Marking: config.MarkingPublic, Counting: config.CountingInputOutput,
		ResetZone: "America/Los_Angeles", Source: "rate-limits.png", Retrieved: "2026-10-24", SHA256: strings.Repeat("0", 64)})
	plan := admission.ExpectedPlan(cfg, 1)
	plan.Attempts = []admission.Attempt{{Start: time.Date(2026, 11, 1, 20, 0, 0, 0, time.UTC)}}
	cfg, v := ruled(t, cfg, &plan)
	epoch := time.Date(2026, 11, 1, 20, 0, 0, 0, time.UTC)
	cell := timedCell("weekday-04", epoch, refused(1, 0.498, true), sent(1, 1.5, 100))
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 1, 19, 0, 0, 0, time.UTC)
	most, small := math.MaxInt, 1000
	send := func(prompt *int) Send {
		w := at
		return Send{Route: "groq", Kind: SendProbe, DispatchUTC: at, WroteUTC: &w, LatestUTC: at, PromptTokens: prompt, InputBound: 1200, Ceiling: 512}
	}
	day := func(sends ...Send) (ResetDaySent, string) {
		t.Helper()
		h := &History{Group: "groq", Routes: []string{"groq"}, Sends: sends}
		if err := st.Attribute(cfg, v, h); err != nil {
			t.Fatal(err)
		}
		for _, d := range st.Attribution.ResetDays {
			if d.Date == "2026-11-01" {
				for _, line := range strings.Split(st.Text(), "\n") {
					if strings.HasPrefix(line, "  reset day 2026-11-01 ") {
						return d, line
					}
				}
			}
		}
		t.Fatal("no reset day 2026-11-01")
		return ResetDaySent{}, ""
	}

	d, line := day(send(&small), send(nil))
	if d.AtBound != 1 || d.Saturated != 0 || !strings.Contains(line, " (1 at the input bound, no usable prompt count reported)") || strings.Contains(line, "at least") {
		t.Errorf("unsaturated day %+v:\n%s", d, line)
	}
	d, line = day(send(&most), send(&small), send(nil))
	if d.Saturated != 1 || d.AtBound != 1 || !strings.Contains(line, ", at least ") ||
		!strings.Contains(line, " (1 at the input bound, no usable prompt count reported) (1 at a prompt count of at least 9223372036854775807)") {
		t.Errorf("saturated day %+v:\n%s", d, line)
	}
	exact := new(big.Rat).SetInt64(math.MaxInt)
	exact.Add(exact, big.NewRat(1000+1200+3*512, 1))
	if got := new(big.Rat).SetFloat64(d.Sent); got.Cmp(exact) > 0 || d.Sent < float64(math.MaxInt)-4096 {
		t.Errorf("saturated day sent %s, exact %s", got.FloatString(0), exact.FloatString(0))
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
