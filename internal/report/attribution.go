package report

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

// Kinds of a Send.
const (
	SendLadder = "ladder"
	SendWarmup = "warmup"
	SendProbe  = "probe"
	SendCheck  = "ceiling_check"
)

// Send is one request in an account group's history. WroteUTC is when
// it was written in full, nil where its record has none; LatestUTC then
// bounds when it could have been written. EndUTC is when it ended: its
// done time where a status other than 200 or the stream's terminator
// arrived, else the hard timeout after its dispatch. PromptTokens is the
// provider-reported prompt count, nil where unreported.
type Send struct {
	Route        string     `json:"route"`
	Source       string     `json:"source"`
	Kind         string     `json:"kind"`
	Index        int64      `json:"index"`
	DispatchUTC  time.Time  `json:"dispatch_utc"`
	WroteUTC     *time.Time `json:"wrote_utc,omitempty"`
	LatestUTC    time.Time  `json:"latest_utc"`
	EndUTC       time.Time  `json:"end_utc"`
	PromptTokens *int       `json:"prompt_tokens,omitempty"`
	InputBound   int        `json:"input_bound_tokens"`
	Ceiling      int        `json:"ceiling_tokens"`
}

// HistoryGap is a period in which an account group's history may lack
// sends of Route; ToUTC nil leaves it open.
type HistoryGap struct {
	Route   string     `json:"route"`
	FromUTC time.Time  `json:"from_utc"`
	ToUTC   *time.Time `json:"to_utc,omitempty"`
	Reason  string     `json:"reason"`
}

// RoutePrice is a route's price per request at one pair of token bounds,
// in effect from RecordedUTC, when the ledger recorded it.
type RoutePrice struct {
	Route         string    `json:"route"`
	InputTokens   int       `json:"input_tokens"`
	OutputTokens  int       `json:"output_tokens"`
	PerRequestUSD string    `json:"per_request_usd"`
	RecordedUTC   time.Time `json:"recorded_utc"`
}

// History is one account group's recorded sends: every ladder, warm-up,
// probe and ceiling-check request of its routes, of counted and labelled
// cells, with the periods the records or the ledger leave incomplete.
// Unadmitted counts the sends no ledger admission of their route
// brackets.
type History struct {
	Group      string
	Routes     []string
	Sends      []Send
	Gaps       []HistoryGap
	Prices     []RoutePrice
	Unadmitted int
}

// RecordSends are the sends of a cell's attempts file: each dispatched
// record the write limit did not refuse, at epoch plus its offsets, of
// route's shape input bound. A record with no done time can have been
// written until the hard timeout after its dispatch. A send ends at its
// done time where it completed, returned an empty stream (status 200 and
// the terminator with no content) or got a status other than 200, and
// otherwise at the hard timeout after its dispatch.
func RecordSends(route, source string, epoch time.Time, recs []evidence.Record, inputBound, hardTimeoutS int) []Send {
	var out []Send
	at := func(ns int64) time.Time { return epoch.Add(time.Duration(ns)).UTC() }
	for i := range recs {
		r := &recs[i]
		if r.DispatchNs == 0 || r.WriteDenied {
			continue
		}
		s := Send{Route: route, Source: source, Kind: SendLadder, Index: r.Index, DispatchUTC: at(r.DispatchNs),
			InputBound: inputBound, Ceiling: r.CeilingTokens}
		if r.Warmup {
			s.Kind = SendWarmup
		}
		if r.WroteRequestNs > 0 {
			w := at(r.WroteRequestNs)
			s.WroteUTC = &w
		}
		s.LatestUTC = s.DispatchUTC.Add(time.Duration(hardTimeoutS) * time.Second)
		s.EndUTC = s.LatestUTC
		if r.DoneNs >= r.DispatchNs && r.DoneNs > 0 {
			s.LatestUTC = at(r.DoneNs)
			if r.Outcome == loadgen.OutcomeCompleted || r.Status != 0 && r.Status != http.StatusOK ||
				r.Status == http.StatusOK && r.ErrClass == loadgen.ErrEmptyStream {
				s.EndUTC = s.LatestUTC
			}
		}
		if n, ok := promptTokens(r.Usage); ok {
			s.PromptTokens = &n
		}
		out = append(out, s)
	}
	return out
}

// ExchangeSend is the send of a probe or ceiling-check exchange, whose
// stream read as sh where it streamed, ok false where it was never
// dispatched.
func ExchangeSend(route, source, kind string, index int64, ex *probe.Exchange, sh *probe.Shape, prompt *int, inputBound, ceiling, hardTimeoutS int) (Send, bool) {
	if ex == nil || ex.DispatchUTC.IsZero() {
		return Send{}, false
	}
	d := ex.DispatchUTC.UTC()
	s := Send{Route: route, Source: source, Kind: kind, Index: index, DispatchUTC: d, PromptTokens: prompt,
		InputBound: inputBound, Ceiling: ceiling, LatestUTC: d.Add(time.Duration(hardTimeoutS) * time.Second)}
	s.EndUTC = s.LatestUTC
	if ex.WroteRequestNs > 0 {
		w := d.Add(time.Duration(ex.WroteRequestNs))
		s.WroteUTC = &w
	}
	if ex.DoneNs > 0 {
		s.LatestUTC = d.Add(time.Duration(ex.DoneNs))
		if ex.Status != 0 && ex.Status != http.StatusOK || ex.Status == http.StatusOK && sh != nil && sh.Done && sh.ReadError == "" {
			s.EndUTC = s.LatestUTC
		}
	}
	return s, true
}

// Values of a Refusal's Label.
const (
	LabelAtOrBelow  = "at_or_below"
	LabelAbove      = "above"
	LabelUnresolved = "unresolved"
)

// labelText is a label as the page prints it, naming the windows above.
func labelText(label string, above []string) string {
	switch label {
	case LabelAtOrBelow:
		return "refused with recorded sends at or below the limit's share in every window"
	case LabelAbove:
		return "refused with recorded sends above the limit's share in window " + strings.Join(above, " and in window ")
	case LabelUnresolved:
		return "comparison unresolved"
	}
	return "not labelled: no account-group history read"
}

// WindowCount is the account group's recorded sends, by write time, in
// one evaluation window of one recorded public limit ending at a
// refusal's write time, or over the reset day up to it: Requests sends,
// Count of them in the limit's unit, against Share, the limit's share
// of the window. AtBound counts the sends with no usable prompt count,
// taken at their shape's input bound, and Saturated those whose prompt
// count is math.MaxInt, a count past it (loadgen.TokenCount), so that
// Count and Bound are lower bounds. Counting states how a token limit
// counts a send (tokenCounting). A spend window prices each send at its
// input bound and ceiling, at the price recorded by its write time. For a
// limit that counts output at generation, Earlier are the sends written
// before the window whose interval from write to end reaches into it,
// EarlierSaturated those of them at a prompt count of math.MaxInt, and
// Bound is Count with them added. Value is the limit's value the share
// is of: the value the refresh command read at the refused cell's start
// where ReadAtStart, else the recorded one.
type WindowCount struct {
	Dimension        string   `json:"dimension"`
	Limit            int      `json:"limit"`
	Value            float64  `json:"limit_value"`
	ReadAtStart      bool     `json:"limit_read_at_cell_start,omitempty"`
	WindowS          float64  `json:"window_s,omitempty"`
	ResetDay         string   `json:"reset_day,omitempty"`
	Requests         int      `json:"requests"`
	Count            float64  `json:"count"`
	Earlier          int      `json:"earlier_sends_ending_in_window,omitempty"`
	EarlierSaturated int      `json:"earlier_saturated_prompt_counts,omitempty"`
	Bound            *float64 `json:"bound_at_generation,omitempty"`
	Counting         string   `json:"counting,omitempty"`
	AtBound          int      `json:"at_input_bound,omitempty"`
	Saturated        int      `json:"saturated_prompt_counts,omitempty"`
	Share            float64  `json:"share"`
	Above            bool     `json:"above"`
	Unresolved       []string `json:"unresolved,omitempty"`

	count, bound, share *big.Rat
}

// amount is x in the window's unit as the page prints it.
func (w *WindowCount) amount(x float64) string {
	if w.Dimension == config.LimitSpendWindow {
		return fmt.Sprintf("US$%.6f", x)
	}
	return decimalText(x)
}

// exact is r in the window's unit as the page prints it, x where r is
// nil, and "at least" before it where lower; an amount of a limit
// counted in tokens carries its unit.
func (w *WindowCount) exact(r *big.Rat, x float64, lower bool) string {
	var s string
	switch {
	case w.Dimension == config.LimitSpendWindow && r != nil:
		s = "US$" + r.FloatString(6)
	case r != nil:
		s = strings.TrimSuffix(strings.TrimRight(r.FloatString(3), "0"), ".")
	default:
		s = w.amount(x)
	}
	if w.Counting != "" {
		s += " tokens"
	}
	if lower {
		s = "at least " + s
	}
	return s
}

// name is the window as a label names it.
func (w *WindowCount) name() string {
	if w.ResetDay != "" {
		return fmt.Sprintf("%s over the reset day %s", w.Dimension, w.ResetDay)
	}
	return fmt.Sprintf("%s over %g s", w.Dimension, w.WindowS)
}

// Refusal is one status_429: its request index, its step (0 for the
// warm-up), its intended dispatch from the segment's start, when it was
// written and its delay from dispatch to write, its status, error type,
// code and status string as received, the error documentation recorded
// for its type or code at status 429 (nil: an unclassified 429), and its
// recorded rate-limit headers. Label, AboveIn, Unresolved and Windows
// are its attribution (StepTables.Attribute).
type Refusal struct {
	Index        int64               `json:"index"`
	Step         int                 `json:"step"`
	OffsetS      float64             `json:"offset_s"`
	WroteUTC     *time.Time          `json:"wrote_utc,omitempty"`
	WriteDelayNs int64               `json:"dispatch_to_write_ns,omitempty"`
	Status       int                 `json:"status"`
	ErrorType    string              `json:"error_type,omitempty"`
	ErrorCode    string              `json:"error_code,omitempty"`
	ErrorStatus  string              `json:"error_status,omitempty"`
	Documented   *config.ErrorDoc    `json:"documented,omitempty"`
	Headers      map[string][]string `json:"rate_limit_headers,omitempty"`
	Label        string              `json:"label,omitempty"`
	AboveIn      []string            `json:"above_in,omitempty"`
	Unresolved   []string            `json:"unresolved,omitempty"`
	Windows      []WindowCount       `json:"windows,omitempty"`
}

// LabelCounts counts refusals per label.
type LabelCounts struct {
	AtOrBelow  int `json:"at_or_below"`
	Above      int `json:"above"`
	Unresolved int `json:"unresolved"`
}

func (c *LabelCounts) add(label string) {
	switch label {
	case LabelAtOrBelow:
		c.AtOrBelow++
	case LabelAbove:
		c.Above++
	case LabelUnresolved:
		c.Unresolved++
	}
}

// binS is the width of a refusal bin.
const binS = 30

// SegmentRefusals are the status_429 responses of one step, or the
// warm-up, of one cell: their count, their count per 30 s bin of
// intended dispatch from the segment's start, the first bin publishing
// on its own line, each refusal, and their labels once attributed.
type SegmentRefusals struct {
	Count    int          `json:"count"`
	Bins     []int        `json:"bins_30s"`
	Refusals []Refusal    `json:"refusals"`
	Labels   *LabelCounts `json:"labels,omitempty"`
}

// WriteDelay is a step's delay from dispatch to request written: the
// largest over its Written requests, and Unwritten, its dispatched
// requests with no write time.
type WriteDelay struct {
	Written   int   `json:"written"`
	MaxNs     int64 `json:"max_ns"`
	Unwritten int   `json:"unwritten"`
}

func (d *WriteDelay) add(o *WriteDelay) {
	d.Written += o.Written
	d.Unwritten += o.Unwritten
	d.MaxNs = max(d.MaxNs, o.MaxNs)
}

// rateLimitHeader reports a response header the page prints beside a
// refusal: one whose name carries "ratelimit", or Retry-After.
func rateLimitHeader(name string) bool {
	n := strings.ToLower(name)
	return strings.Contains(n, "ratelimit") || n == "retry-after"
}

// segment is the refusals and write delay of one segment's records,
// which recs holds in index order, lengthS long, of a cell whose epoch
// is epoch (zero: unknown). The segment starts at its first scheduled
// request's intended time (the count rule's phase zero).
func (t *tables) segment(recs []*evidence.Record, step int, lengthS float64, epoch time.Time) (*SegmentRefusals, *WriteDelay) {
	start := int64(-1)
	for _, r := range recs {
		if start < 0 || r.IntendedNs < start {
			start = r.IntendedNs
		}
	}
	bins := max(1, int((lengthS+binS-1)/binS))
	seg := &SegmentRefusals{Bins: make([]int, bins), Refusals: []Refusal{}}
	wd := &WriteDelay{}
	for _, r := range recs {
		if r.DispatchNs != 0 {
			if r.WroteRequestNs > 0 {
				wd.Written++
				wd.MaxNs = max(wd.MaxNs, r.WroteRequestNs-r.DispatchNs)
			} else {
				wd.Unwritten++
			}
		}
		if r.BillingRefusal || r.Outcome != loadgen.OutcomeErrored || r.ErrClass != loadgen.ErrStatus429 {
			continue
		}
		off := float64(r.IntendedNs-start) / 1e9
		seg.Count++
		seg.Bins[min(bins-1, max(0, int(off/binS)))]++
		ref := Refusal{Index: r.Index, Step: step, OffsetS: off, Status: r.Status, ErrorType: r.ErrorType, ErrorCode: r.ErrorCode,
			ErrorStatus: r.ErrorStatus, Documented: t.refusalDoc(r)}
		if r.WroteRequestNs > 0 && !epoch.IsZero() {
			w := epoch.Add(time.Duration(r.WroteRequestNs)).UTC()
			ref.WroteUTC, ref.WriteDelayNs = &w, r.WroteRequestNs-r.DispatchNs
		}
		for name, vs := range r.Headers {
			if rateLimitHeader(name) {
				if ref.Headers == nil {
					ref.Headers = map[string][]string{}
				}
				ref.Headers[http.CanonicalHeaderKey(name)] = vs
			}
		}
		seg.Refusals = append(seg.Refusals, ref)
	}
	return seg, wd
}

// refusalDoc is the error documentation recorded for a status_429's
// error code or type at status 429, nil where none is.
func (t *tables) refusalDoc(r *evidence.Record) *config.ErrorDoc {
	for _, k := range []string{r.ErrorCode, r.ErrorType} {
		if d := t.errorDocs[k]; k != "" && d != nil && d.Status == 429 {
			return d
		}
	}
	return nil
}

// Attribution is the account-group history a route page's refusals are
// attributed from: the group, its routes, the sends read, the periods the
// history may lack sends, the sends no ledger admission brackets, the
// recorded confidential limits, which no window compares, each daily
// limit's reset days with the requests the group sent beside the route's
// reservation, and the public limits that count output at generation,
// with the hard timeout that ends a send.
type Attribution struct {
	Group        string         `json:"account_group"`
	Routes       []string       `json:"routes"`
	Sends        int            `json:"sends"`
	Gaps         []HistoryGap   `json:"gaps,omitempty"`
	Unadmitted   int            `json:"unadmitted_sends,omitempty"`
	Confidential int            `json:"confidential_limits,omitempty"`
	ResetDays    []ResetDaySent `json:"reset_days,omitempty"`
	AtGeneration []string       `json:"counted_at_generation,omitempty"`
	HardTimeoutS int            `json:"hard_timeout_s,omitempty"`
}

// ResetDaySent is one reset day of a daily limit: the route's
// reservation and what the account group sent, by recorded write time, in
// requests and in the limit's unit. AtBound and Saturated count sends as
// a WindowCount's do; with a saturated send, Sent is a lower bound.
// Incomplete marks a day a history gap overlaps, or a send with no write
// time could have been written in.
type ResetDaySent struct {
	Dimension string `json:"dimension"`
	Zone      string `json:"reset_zone"`
	admission.Reservation
	SentRequests int     `json:"sent_requests"`
	Sent         float64 `json:"sent"`
	Counting     string  `json:"counting,omitempty"`
	AtBound      int     `json:"at_input_bound,omitempty"`
	Saturated    int     `json:"saturated_prompt_counts,omitempty"`
	Incomplete   bool    `json:"incomplete,omitempty"`
}

// historyIndex is a history sorted for window counts: written sends by
// write time, the sends with no write time, the gaps and the prices,
// each pair of bounds' revisions in ledger order. longest is the most
// any written send's end follows its write.
type historyIndex struct {
	h         *History
	written   []*Send
	times     []int64
	unwritten []*Send
	prices    map[priceKey][]datedPrice
	longest   time.Duration
}

type datedPrice struct {
	from time.Time
	usd  *big.Rat
}

type priceKey struct {
	route   string
	in, out int
}

func newHistoryIndex(h *History) (*historyIndex, error) {
	ix := &historyIndex{h: h, prices: map[priceKey][]datedPrice{}}
	for i := range h.Sends {
		s := &h.Sends[i]
		if s.WroteUTC != nil {
			ix.written = append(ix.written, s)
		} else {
			ix.unwritten = append(ix.unwritten, s)
		}
	}
	slices.SortStableFunc(ix.written, func(a, b *Send) int { return a.WroteUTC.Compare(*b.WroteUTC) })
	for _, s := range ix.written {
		ix.times = append(ix.times, s.WroteUTC.UnixNano())
		ix.longest = max(ix.longest, s.EndUTC.Sub(*s.WroteUTC))
	}
	for _, p := range h.Prices {
		r, ok := new(big.Rat).SetString(p.PerRequestUSD)
		if !ok {
			return nil, fmt.Errorf("report: history: route %s price %q", p.Route, p.PerRequestUSD)
		}
		k := priceKey{p.Route, p.InputTokens, p.OutputTokens}
		ix.prices[k] = append(ix.prices[k], datedPrice{p.RecordedUTC, r})
	}
	return ix, nil
}

// demand is one send's demand under limit l, in l's unit: a request, its
// tokens under l's counting at its reported prompt count or, atBound,
// its input bound, or its price, the latest recorded at or before its
// write time (its dispatch time where it has none); price nil where none
// is.
func (ix *historyIndex) demand(l *config.Limit, s *Send) (units *big.Rat, atBound bool) {
	in := s.InputBound
	atBound = s.PromptTokens == nil || *s.PromptTokens < 0
	if !atBound {
		in = *s.PromptTokens
	}
	switch l.Dimension {
	case config.LimitTPM, config.LimitTPD:
		switch l.Counting {
		case config.CountingInput:
			return big.NewRat(int64(in), 1), atBound
		case config.CountingOutputBurndown:
			out := new(big.Rat).Mul(big.NewRat(int64(s.Ceiling), 1), new(big.Rat).SetFloat64(l.OutputBurndown))
			return out.Add(out, big.NewRat(int64(in), 1)), atBound
		}
		return new(big.Rat).Add(big.NewRat(int64(in), 1), big.NewRat(int64(s.Ceiling), 1)), atBound
	case config.LimitInputTPM:
		return big.NewRat(int64(in), 1), atBound
	case config.LimitOutputTPM:
		return big.NewRat(int64(s.Ceiling), 1), false
	case config.LimitSpendWindow:
		at := s.DispatchUTC
		if s.WroteUTC != nil {
			at = *s.WroteUTC
		}
		prices := ix.prices[priceKey{s.Route, s.InputBound, s.Ceiling}]
		for i := len(prices) - 1; i >= 0; i-- {
			if !prices[i].from.After(at) {
				return prices[i].usd, false
			}
		}
		return nil, false
	}
	return big.NewRat(1, 1), false
}

// saturated reports whether limit l counts send s at a prompt count of
// math.MaxInt, a reported count past it.
func saturated(l *config.Limit, s *Send) bool {
	switch l.Dimension {
	case config.LimitTPM, config.LimitTPD, config.LimitInputTPM:
		return s.PromptTokens != nil && *s.PromptTokens == math.MaxInt
	}
	return false
}

// tokenCounting is how limit l's window counts a send's tokens, as the
// page prints it beside the count (demand); empty for a limit not
// counted in tokens.
func tokenCounting(l *config.Limit) string {
	switch l.Dimension {
	case config.LimitInputTPM:
		return "as counted in reported prompt tokens"
	case config.LimitOutputTPM:
		return "as counted at the output ceiling"
	case config.LimitTPM, config.LimitTPD:
		switch l.Counting {
		case config.CountingInput:
			return "as counted in reported prompt tokens"
		case config.CountingOutputBurndown:
			return fmt.Sprintf("as counted in reported prompt tokens plus the ceiling times the output burndown rate of %g", l.OutputBurndown)
		}
		return "as counted in reported prompt tokens plus the ceiling"
	}
	return ""
}

// window counts the written sends in (from, to], or [from, to] where
// closed, against limit l of share, with what may be missing.
func (ix *historyIndex) window(wc *WindowCount, l *config.Limit, share *big.Rat, from, to time.Time, closed bool) {
	lo, hi := from.UnixNano(), to.UnixNano()
	i := sortSearch(ix.times, lo, closed)
	j := sortSearch(ix.times, hi, false)
	sum, unpriced := new(big.Rat), 0
	for _, s := range ix.written[i:j] {
		d, atBound := ix.demand(l, s)
		wc.Requests++
		if atBound {
			wc.AtBound++
		}
		if saturated(l, s) {
			wc.Saturated++
		}
		if d == nil {
			unpriced++
			continue
		}
		sum.Add(sum, d)
	}
	wc.Count, _ = sum.Float64()
	wc.Share, _ = share.Float64()
	wc.count, wc.share = sum, share
	wc.Above = sum.Cmp(share) > 0
	if l.OutputAtGeneration {
		bound, earlierUnpriced := new(big.Rat).Set(sum), 0
		for _, s := range ix.written[sortSearch(ix.times, from.Add(-ix.longest).UnixNano(), true):i] {
			if !s.EndUTC.After(from) && !(closed && s.EndUTC.Equal(from)) {
				continue
			}
			d, _ := ix.demand(l, s)
			wc.Earlier++
			if saturated(l, s) {
				wc.EarlierSaturated++
			}
			if d == nil {
				earlierUnpriced++
				continue
			}
			bound.Add(bound, d)
		}
		b, _ := bound.Float64()
		wc.Bound, wc.bound = &b, bound
		if bound.Cmp(share) > 0 {
			wc.Above = false
			wc.Unresolved = append(wc.Unresolved, fmt.Sprintf("%s counts output at generation: with the %d sends written before the window that may have ended in it, the window may hold %s against a share of %s",
				wc.name(), wc.Earlier, wc.exact(bound, b, wc.Saturated+wc.EarlierSaturated > 0), wc.exact(share, wc.Share, false)))
		}
		if earlierUnpriced > 0 {
			wc.Unresolved = append(wc.Unresolved, fmt.Sprintf("%d sends written before the window that may have ended in it have no recorded price", earlierUnpriced))
		}
	}
	if unpriced > 0 {
		wc.Unresolved = append(wc.Unresolved, fmt.Sprintf("%d sends in the window have no recorded price", unpriced))
	}
	overlaps := func(a time.Time, b *time.Time) bool {
		return !a.After(to) && (b == nil || b.After(from) || closed && b.Equal(from))
	}
	missing := map[string]int{}
	var routes []string
	for _, s := range ix.unwritten {
		end := s.LatestUTC
		if l.OutputAtGeneration && s.EndUTC.After(end) {
			end = s.EndUTC
		}
		if overlaps(s.DispatchUTC, &end) {
			if missing[s.Route] == 0 {
				routes = append(routes, s.Route)
			}
			missing[s.Route]++
		}
	}
	for _, r := range routes {
		wc.Unresolved = append(wc.Unresolved, fmt.Sprintf("%d sends of route %s that may fall in the window have no write time", missing[r], r))
	}
	for _, g := range ix.h.Gaps {
		if overlaps(g.FromUTC, g.ToUTC) {
			wc.Unresolved = append(wc.Unresolved, fmt.Sprintf("the history may lack sends of route %s from %s: %s", g.Route, gapSpan(g), g.Reason))
		}
	}
}

// sortSearch is the first index of times after t, or at or after it
// where closed.
func sortSearch(times []int64, t int64, closed bool) int {
	i, _ := slices.BinarySearch(times, t)
	if closed {
		return i
	}
	for i < len(times) && times[i] <= t {
		i++
	}
	return i
}

func gapSpan(g HistoryGap) string {
	if g.ToUTC == nil {
		return g.FromUTC.Format(time.RFC3339Nano) + " on"
	}
	return g.FromUTC.Format(time.RFC3339Nano) + " to " + g.ToUTC.Format(time.RFC3339Nano)
}

// attribute labels ref from the account group's history against cfg's
// public limits, clockS being the cell's recorded clock-offset bound in
// seconds; read[i] marks a limit at the value read at the cell's start.
func (ix *historyIndex) attribute(ref *Refusal, cfg *config.Config, read []bool, clockS float64) error {
	ref.AboveIn, ref.Unresolved, ref.Windows = nil, nil, nil
	if ref.WroteUTC == nil {
		ref.Label, ref.Unresolved = LabelUnresolved, []string{"the refused request has no write time"}
		return nil
	}
	t := *ref.WroteUTC
	unresolved := false
	for i := range cfg.Target.Limits {
		l := &cfg.Target.Limits[i]
		if l.Marking != config.MarkingPublic {
			continue
		}
		value, ok := new(big.Rat).SetString(strconv.FormatFloat(l.Value, 'g', -1, 64))
		if !ok {
			return fmt.Errorf("report: target.limits[%d] value %g", i, l.Value)
		}
		switch l.Dimension {
		case config.LimitRPD, config.LimitTPD:
			loc, err := time.LoadLocation(l.ResetZone)
			if err != nil {
				return fmt.Errorf("report: target.limits[%d] reset zone: %w", i, err)
			}
			lt := t.In(loc)
			start := time.Date(lt.Year(), lt.Month(), lt.Day(), 0, 0, 0, 0, loc)
			next := time.Date(lt.Year(), lt.Month(), lt.Day()+1, 0, 0, 0, 0, loc)
			wc := WindowCount{Dimension: l.Dimension, Limit: i, Value: l.Value, ReadAtStart: read[i], ResetDay: start.Format(time.DateOnly), Counting: tokenCounting(l)}
			ix.window(&wc, l, value, start, t, true)
			bound := time.Duration(clockS * float64(time.Second))
			if t.Sub(start) <= bound || next.Sub(t) <= bound {
				wc.Unresolved = append(wc.Unresolved, fmt.Sprintf("the refusal falls within the cell's clock-offset bound of %g s of a reset of %s", clockS, l.ResetZone))
				wc.Above = false
			}
			ref.Windows = append(ref.Windows, wc)
		case config.LimitRPM, config.LimitRPS, config.LimitTPM, config.LimitInputTPM, config.LimitOutputTPM, config.LimitSpendWindow:
			for _, w := range l.WindowsS() {
				share := new(big.Rat).Mul(value, new(big.Rat).SetFloat64(w))
				share.Quo(share, new(big.Rat).SetFloat64(l.StatedWindowS()))
				wc := WindowCount{Dimension: l.Dimension, Limit: i, Value: l.Value, ReadAtStart: read[i], WindowS: w, Counting: tokenCounting(l)}
				ix.window(&wc, l, share, t.Add(-time.Duration(w*float64(time.Second))), t, false)
				ref.Windows = append(ref.Windows, wc)
			}
		}
	}
	for i := range ref.Windows {
		wc := &ref.Windows[i]
		switch {
		case wc.Above:
			ref.AboveIn = append(ref.AboveIn, wc.name())
		case len(wc.Unresolved) > 0:
			unresolved = true
			for _, u := range wc.Unresolved {
				if !slices.Contains(ref.Unresolved, u) {
					ref.Unresolved = append(ref.Unresolved, u)
				}
			}
		}
	}
	switch {
	case len(ref.AboveIn) > 0:
		ref.Label, ref.Unresolved = LabelAbove, nil
	case unresolved:
		ref.Label = LabelUnresolved
	default:
		ref.Label = LabelAtOrBelow
	}
	return nil
}

// Attribute labels every status_429 of the tables' cells, and of the
// cells outside the frame, from h, the history of cfg's account group,
// against cfg's public limits (SPEC-HOSTED HA4, §5 status_429), each at
// the value the refresh command read at the refused cell's start, a
// limit it read as unavailable at its recorded value (cellLimits); it
// counts the labels per step and per pooled step, and sets each daily
// limit's reset days under v's plan beside what the group sent. A label
// above names every window whose recorded sends exceed the limit's
// share; otherwise any window the history leaves incomplete labels the
// refusal unresolved. A window of a limit that counts output at
// generation never reads above: where its sends, with those written
// before it that may have ended in it, exceed the share, it reads
// unresolved.
func (t *StepTables) Attribute(cfg *config.Config, v *admission.Verdict, h *History) error {
	switch {
	case h == nil:
		return errors.New("report: attribution: no account-group history")
	case cfg.Release == nil || h.Group != cfg.Release.AccountGroup:
		return fmt.Errorf("report: attribution: history of account group %q, route %s's is another", h.Group, t.Route)
	case !slices.Contains(h.Routes, t.Route):
		return fmt.Errorf("report: attribution: route %s is not among the history's routes %q", t.Route, h.Routes)
	case v == nil:
		return errors.New("report: attribution: no ceiling-rule verdict")
	}
	ix, err := newHistoryIndex(h)
	if err != nil {
		return err
	}
	at := &Attribution{Group: h.Group, Routes: slices.Clone(h.Routes), Sends: len(h.Sends), Gaps: h.Gaps, Unadmitted: h.Unadmitted}
	for _, l := range cfg.Target.Limits {
		switch {
		case l.Marking == config.MarkingConfidential:
			at.Confidential++
		case l.OutputAtGeneration:
			at.AtGeneration = append(at.AtGeneration, l.Dimension)
			at.HardTimeoutS = cfg.Client.HTTPTimeoutS
		}
	}
	label := func(cell string, refreshed []evidence.RefreshedLimit, clock *float64, segs ...*SegmentRefusals) error {
		cellCfg, read, err := cellLimits(cfg, refreshed)
		if err != nil {
			return fmt.Errorf("report: attribution: cell %s: %w", cell, err)
		}
		clockS := float64(config.PinnedClockOffsetMaxS)
		if clock != nil {
			clockS = *clock
		}
		for _, seg := range segs {
			if seg == nil {
				continue
			}
			seg.Labels = &LabelCounts{}
			for i := range seg.Refusals {
				if err := ix.attribute(&seg.Refusals[i], cellCfg, read, clockS); err != nil {
					return err
				}
				seg.Labels.add(seg.Refusals[i].Label)
			}
		}
		return nil
	}
	for ci := range t.Cells {
		c := &t.Cells[ci]
		segs := []*SegmentRefusals{c.Warmup}
		for k := range c.Steps {
			segs = append(segs, c.Steps[k].Refusals)
		}
		if err := label(c.Cell, c.refreshed, c.ClockAbsOffsetS, segs...); err != nil {
			return err
		}
	}
	for ci := range t.Outside {
		c := &t.Outside[ci]
		segs := []*SegmentRefusals{c.Warmup}
		for _, s := range c.Steps {
			segs = append(segs, s.Refusals)
		}
		if err := label(c.Cell, c.refreshed, c.clockS, segs...); err != nil {
			return err
		}
	}
	poolLabels := func(steps []PooledStep, cells []string) {
		for k := range steps {
			if steps[k].Refusals == nil {
				continue
			}
			steps[k].Refusals.Labels = &LabelCounts{}
			for _, c := range t.Cells {
				if !slices.Contains(cells, c.Cell) || c.Steps[k].Refusals == nil {
					continue
				}
				for _, r := range c.Steps[k].Refusals.Refusals {
					steps[k].Refusals.Labels.add(r.Label)
				}
			}
		}
	}
	var all []string
	for _, c := range t.Cells {
		all = append(all, c.Cell)
	}
	poolLabels(t.Pooled, all)
	for i := range t.DriftParts {
		poolLabels(t.DriftParts[i].Pooled, t.DriftParts[i].Cells)
	}
	for i := range cfg.Target.Limits {
		l := &cfg.Target.Limits[i]
		if l.Marking != config.MarkingPublic || l.Dimension != config.LimitRPD && l.Dimension != config.LimitTPD {
			continue
		}
		days, err := admission.Reservations(cfg, v.Plan, i)
		if err != nil {
			return err
		}
		loc, err := time.LoadLocation(l.ResetZone)
		if err != nil {
			return fmt.Errorf("report: target.limits[%d] reset zone: %w", i, err)
		}
		for _, d := range days {
			start, err := time.ParseInLocation(time.DateOnly, d.Date, loc)
			if err != nil {
				return err
			}
			next := time.Date(start.Year(), start.Month(), start.Day()+1, 0, 0, 0, 0, loc)
			rd := ResetDaySent{Dimension: l.Dimension, Zone: l.ResetZone, Reservation: d, Counting: tokenCounting(l)}
			sum := new(big.Rat)
			for j := range h.Sends {
				s := &h.Sends[j]
				if s.WroteUTC == nil {
					if s.DispatchUTC.Before(next) && !s.LatestUTC.Before(start) {
						rd.Incomplete = true
					}
					continue
				}
				if s.WroteUTC.Before(start) || !s.WroteUTC.Before(next) {
					continue
				}
				u, atBound := ix.demand(l, s)
				rd.SentRequests++
				sum.Add(sum, u)
				if atBound {
					rd.AtBound++
				}
				if saturated(l, s) {
					rd.Saturated++
				}
			}
			rd.Sent, _ = sum.Float64()
			if rd.Saturated > 0 && new(big.Rat).SetFloat64(rd.Sent).Cmp(sum) > 0 {
				rd.Sent = math.Nextafter(rd.Sent, math.Inf(-1))
			}
			for _, g := range h.Gaps {
				if g.FromUTC.Before(next) && (g.ToUTC == nil || g.ToUTC.After(start)) {
					rd.Incomplete = true
				}
			}
			at.ResetDays = append(at.ResetDays, rd)
		}
	}
	t.Attribution = at
	return nil
}

// cellLimits is cfg with each public limit at the value refreshed, a
// cell's refresh record, read at its start, and read marking those
// limits; a limit read as unavailable, or withheld as confidential,
// keeps its recorded value.
func cellLimits(cfg *config.Config, refreshed []evidence.RefreshedLimit) (*config.Config, []bool, error) {
	read := make([]bool, len(cfg.Target.Limits))
	if refreshed == nil {
		return cfg, read, nil
	}
	if len(refreshed) != len(cfg.Target.Limits) {
		return nil, nil, fmt.Errorf("the refresh record holds %d limits, target.limits %d", len(refreshed), len(cfg.Target.Limits))
	}
	c := *cfg
	c.Target.Limits = slices.Clone(cfg.Target.Limits)
	for i, r := range refreshed {
		l := &c.Target.Limits[i]
		switch {
		case r.Dimension != l.Dimension:
			return nil, nil, fmt.Errorf("refresh limits[%d] is %q, target.limits[%d] %q", i, r.Dimension, i, l.Dimension)
		case l.Marking != config.MarkingPublic || r.Confidential || r.Unavailable != "":
		case !(r.Value > 0):
			return nil, nil, fmt.Errorf("refresh limits[%d] %s holds no value and no unavailable reason", i, r.Dimension)
		default:
			l.Value, read[i] = r.Value, true
		}
	}
	return &c, read, nil
}

// refusalsText writes a segment's status_429 context: the 30 s bins, the
// first bin on its own line, each refusal with its label, window counts,
// error fields as received and rate-limit headers.
func refusalsText(w func(string, ...any), seg *SegmentRefusals, segment string) {
	if seg == nil || seg.Count == 0 {
		return
	}
	bins := make([]string, len(seg.Bins))
	for i, n := range seg.Bins {
		bins[i] = strconv.Itoa(n)
	}
	w("    status_429 by intended dispatch from the %s's start, in 30 s bins: %s", segment, strings.Join(bins, ", "))
	w("  status_429 in the first 30 s of the %s: %d of %d", segment, seg.Bins[0], seg.Count)
	if l := seg.Labels; l != nil {
		w("    labels: %d at or below the limit's share in every window; %d above the limit's share in a window; %d comparison unresolved; no cause inferred",
			l.AtOrBelow, l.Above, l.Unresolved)
	}
	for _, r := range seg.Refusals {
		w("%s", refusalLine(&r))
	}
}

// refusalLine is one refusal as the page prints it.
func refusalLine(r *Refusal) string {
	var b strings.Builder
	fmt.Fprintf(&b, "    request %d at %.3f s", r.Index, r.OffsetS)
	if r.WroteUTC != nil {
		fmt.Fprintf(&b, ", written %s, %s after dispatch", r.WroteUTC.Format(time.RFC3339Nano), msText(r.WriteDelayNs))
	} else {
		b.WriteString(", no write time")
	}
	fmt.Fprintf(&b, ": %s", labelText(r.Label, r.AboveIn))
	for _, u := range r.Unresolved {
		fmt.Fprintf(&b, "; %s", u)
	}
	for _, wc := range r.Windows {
		fmt.Fprintf(&b, "; %s: %d requests", wc.name(), wc.Requests)
		share := strings.TrimSuffix(wc.exact(wc.share, wc.Share, false), " tokens")
		switch wc.Dimension {
		case config.LimitTPM, config.LimitTPD, config.LimitInputTPM, config.LimitOutputTPM:
			fmt.Fprintf(&b, ", %s %s%s", wc.exact(wc.count, wc.Count, wc.Saturated > 0), wc.Counting, countNotes(wc.AtBound, wc.Saturated))
		case config.LimitSpendWindow:
			fmt.Fprintf(&b, ", %s at each request's input bound and ceiling", wc.exact(wc.count, wc.Count, false))
		}
		if wc.Bound != nil {
			fmt.Fprintf(&b, ", %s with the %d sends written before it that may have ended in it", wc.exact(wc.bound, *wc.Bound, wc.Saturated+wc.EarlierSaturated > 0), wc.Earlier)
		}
		value := "as recorded"
		if wc.ReadAtStart {
			value = "as read at the cell's start"
		}
		fmt.Fprintf(&b, " against a share of %s of a limit of %s, %s", share, wc.amount(wc.Value), value)
	}
	fmt.Fprintf(&b, "; HTTP %d", r.Status)
	for _, f := range [][2]string{{"error type", r.ErrorType}, {"code", r.ErrorCode}, {"status", r.ErrorStatus}} {
		if f[1] != "" {
			fmt.Fprintf(&b, ", %s %q", f[0], f[1])
		}
	}
	if d := r.Documented; d != nil {
		fmt.Fprintf(&b, ", documented as %q (%s, retrieved %s)", d.Meaning, d.Source, d.Retrieved)
	} else {
		b.WriteString(", an unclassified 429")
	}
	names := make([]string, 0, len(r.Headers))
	for n := range r.Headers {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		fmt.Fprintf(&b, "; %s: %s", n, strings.Join(r.Headers[n], ", "))
	}
	return b.String()
}

// countNotes names a token count's sends with no usable prompt count and
// those at a prompt count of math.MaxInt.
func countNotes(atBound, saturated int) string {
	var s string
	if atBound > 0 {
		s += fmt.Sprintf(" (%d at the input bound, no usable prompt count reported)", atBound)
	}
	if saturated > 0 {
		s += fmt.Sprintf(" (%d at a prompt count of at least %d)", saturated, math.MaxInt)
	}
	return s
}

// decimalText is x to at most three decimal places, trailing zeros cut.
func decimalText(x float64) string {
	s := strconv.FormatFloat(x, 'f', 3, 64)
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// writeDelayText writes a step's dispatch-to-write line.
func writeDelayText(w func(string, ...any), d *WriteDelay) {
	if d == nil {
		return
	}
	if d.Written == 0 {
		w("  dispatch to write: no request written; %d dispatched with no write time", d.Unwritten)
		return
	}
	w("  dispatch to write: largest %s over %d written requests; %d dispatched with no write time", msText(d.MaxNs), d.Written, d.Unwritten)
}

// attributionText writes the account-group history the refusals are
// attributed from and each reset day's sends beside its reservation.
func attributionText(w func(string, ...any), a *Attribution) {
	if a == nil {
		w("-- Account-group history: not read; no status_429 is labelled --")
		w("")
		return
	}
	w("-- Account group %s: %d recorded sends of routes %s, by write time, attribute each status_429 --", a.Group, a.Sends, strings.Join(a.Routes, ", "))
	w("The counts are this client's recorded sends; provider receipt times and the limiter's type and state are unknown, and the labels assume that the account carries no traffic from outside the instrument.")
	w("Each window's share is of the limit's value the refresh command read at the refused cell's start, or of its recorded value where the command read none.")
	for _, g := range a.Gaps {
		w("  the history may lack sends of route %s from %s: %s", g.Route, gapSpan(g), g.Reason)
	}
	if a.Unadmitted > 0 {
		w("  %d recorded sends fall in no ledger admission of their route", a.Unadmitted)
	}
	if a.Confidential > 0 {
		w("  %d confidential recorded limits take no window count", a.Confidential)
	}
	if len(a.AtGeneration) > 0 {
		verb := "count"
		if len(a.AtGeneration) == 1 {
			verb = "counts"
		}
		w("  %s %s output at generation: each window also counts the sends written before it whose interval from write to end reaches into it. A ladder or warm-up send ends at its done time where it got a status other than 200, completed or returned an empty stream; a probe or ceiling-check send ends at its done time where it got a status other than 200 or its stream's terminator arrived with no read error. Any other send ends %d s after its dispatch, at the hard timeout. The provider is taken to stop counting a request when it ends.",
			strings.Join(a.AtGeneration, " and "), verb, a.HardTimeoutS)
	}
	for _, d := range a.ResetDays {
		line := fmt.Sprintf("  reset day %s (%s), %s: %s reserved for %d attempts and %d ceiling-check requests; %d requests sent by the account group",
			d.Date, d.Zone, d.Dimension, decimalText(d.Reserved), d.Attempts, d.Checks, d.SentRequests)
		if d.Dimension == config.LimitTPD {
			sent := decimalText(d.Sent)
			if d.Saturated > 0 {
				sent = "at least " + sent
			}
			line += fmt.Sprintf(", %s tokens %s%s", sent, d.Counting, countNotes(d.AtBound, d.Saturated))
		}
		if d.Incomplete {
			line += "; the history may lack sends of this day"
		}
		w("%s", line)
	}
	w("")
}

// pooledRefusalsText writes a pooled step's status_429 bins and labels.
func pooledRefusalsText(w func(string, ...any), r *PooledRefusals) {
	if r == nil || r.Count == 0 {
		return
	}
	bins := make([]string, len(r.Bins))
	for i, n := range r.Bins {
		bins[i] = strconv.Itoa(n)
	}
	w("    status_429 by intended dispatch from the step's start, in 30 s bins over the pooled cells: %s", strings.Join(bins, ", "))
	w("  status_429 in the first 30 s of the step over the pooled cells: %d of %d", r.Bins[0], r.Count)
	if l := r.Labels; l != nil {
		w("    labels: %d at or below the limit's share in every window; %d above the limit's share in a window; %d comparison unresolved; no cause inferred",
			l.AtOrBelow, l.Above, l.Unresolved)
	}
}
