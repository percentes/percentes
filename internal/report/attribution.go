package report

import (
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
)

// HistoryGap is a period in which an account group's history may lack
// sends of Route; ToUTC nil leaves it open.
type HistoryGap struct {
	Route   string     `json:"route"`
	FromUTC time.Time  `json:"from_utc"`
	ToUTC   *time.Time `json:"to_utc,omitempty"`
	Reason  string     `json:"reason"`
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
