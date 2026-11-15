package report

import (
	"time"

	"github.com/percentes/percentes/internal/admission"
)

// HistoryGap is a period in which an account group's history may lack
// sends of Route; ToUTC nil leaves it open.
type HistoryGap struct {
	Route   string     `json:"route"`
	FromUTC time.Time  `json:"from_utc"`
	ToUTC   *time.Time `json:"to_utc,omitempty"`
	Reason  string     `json:"reason"`
}

// LabelCounts counts refusals per label.
type LabelCounts struct {
	AtOrBelow  int `json:"at_or_below"`
	Above      int `json:"above"`
	Unresolved int `json:"unresolved"`
}

// WriteDelay is a step's delay from dispatch to request written: the
// largest over its Written requests, and Unwritten, its dispatched
// requests with no write time.
type WriteDelay struct {
	Written   int   `json:"written"`
	MaxNs     int64 `json:"max_ns"`
	Unwritten int   `json:"unwritten"`
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
