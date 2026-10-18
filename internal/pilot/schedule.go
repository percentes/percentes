package pilot

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"time"
)

// Day types (SPEC-HOSTED §4: the UTC calendar day of the slot start).
const (
	Weekday = "weekday"
	Weekend = "weekend"
)

// Schedule shape (SPEC-HOSTED HA2 §4).
const (
	SessionsPerDayType = 3
	SlotHourUTC        = 20
	DateLayout         = "2006-01-02"
)

// maxCounter bounds the re-derivation counter.
const maxCounter = 10000

// ScheduleInput is what the schedule is derived from. Dates are UTC
// calendar dates in DateLayout.
type ScheduleInput struct {
	MasterSeed string `json:"master_seed"`
	// Sessions fall strictly after Freeze; reserves on or before Last.
	Freeze  string   `json:"freeze"`
	Last    string   `json:"last"`
	Exclude []string `json:"exclude,omitempty"`
	// Endpoints are the two target names, in config order.
	Endpoints [2]string `json:"endpoints"`
}

// Session is one dated slot. Order is empty on a reserve: a replacement
// takes the replaced session's order.
type Session struct {
	Label   string    `json:"label"`
	DayType string    `json:"day_type"`
	Date    string    `json:"date"`
	Start   time.Time `json:"start"`
	Order   []string  `json:"order,omitempty"`
	// Permutation[i] is the 0-based line, in the measured file, of the
	// case sent as the session's i-th measured request.
	Permutation []int `json:"permutation"`
}

// Schedule is the derived schedule. Counter is the value appended to the
// master seed by the re-derivation rule; 0 means the bare seed.
type Schedule struct {
	Input    ScheduleInput `json:"input"`
	Counter  int           `json:"counter"`
	Sessions []Session     `json:"sessions"`
	Reserves []Session     `json:"reserves"`
}

// sessionLabels are the cell labels, in the fixed order the endpoint
// order is drawn over.
var sessionLabels = func() []struct{ label, dayType string } {
	var out []struct{ label, dayType string }
	for _, dt := range []string{Weekday, Weekend} {
		for i := 1; i <= SessionsPerDayType; i++ {
			out = append(out, struct{ label, dayType string }{fmt.Sprintf("%s-%d", dt, i), dt})
		}
	}
	return out
}()

// draw is the SHA-256 of key read as a big-endian unsigned integer,
// reduced modulo n (SPEC-HOSTED §4).
func draw(key string, n int) int {
	sum := sha256.Sum256([]byte(key))
	var m big.Int
	return int(m.Mod(new(big.Int).SetBytes(sum[:]), big.NewInt(int64(n))).Int64())
}

// permutation shuffles 0..n-1, drawing the swap for position i from
// seed + label + "/case/" + i.
func permutation(seed, label string, n int) []int {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	for i := n - 1; i > 0; i-- {
		j := draw(fmt.Sprintf("%s%s/case/%d", seed, label, i), i+1)
		p[i], p[j] = p[j], p[i]
	}
	return p
}

func dayType(d time.Time) string {
	if wd := d.Weekday(); wd == time.Saturday || wd == time.Sunday {
		return Weekend
	}
	return Weekday
}

func parseDate(field, s string) (time.Time, error) {
	d, err := time.Parse(DateLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("pilot: schedule %s %q: want %s", field, s, DateLayout)
	}
	return d, nil
}

// eligible returns, per day type, the dates after freeze up to last that
// are not excluded, in calendar order.
func eligible(in ScheduleInput) (map[string][]time.Time, error) {
	var errs []error
	freeze, err := parseDate("freeze", in.Freeze)
	errs = append(errs, err)
	last, err := parseDate("last", in.Last)
	errs = append(errs, err)
	excluded := map[time.Time]bool{}
	for _, s := range in.Exclude {
		d, err := parseDate("exclude", s)
		errs = append(errs, err)
		excluded[d] = true
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if !last.After(freeze) {
		return nil, fmt.Errorf("pilot: schedule last %s is not after freeze %s", in.Last, in.Freeze)
	}
	out := map[string][]time.Time{}
	for d := freeze.AddDate(0, 0, 1); !d.After(last); d = d.AddDate(0, 0, 1) {
		if !excluded[d] {
			out[dayType(d)] = append(out[dayType(d)], d)
		}
	}
	return out, nil
}

// slot returns the session or reserve for label on date d, its case
// permutation drawn from seed.
func slot(seed, label string, d time.Time) Session {
	return Session{
		Label:       label,
		DayType:     dayType(d),
		Date:        d.Format(DateLayout),
		Start:       d.Add(SlotHourUTC * time.Hour),
		Permutation: permutation(seed, label, MeasuredPerLabel*len(Labels)),
	}
}

// derive draws each session's date from the dates of its day type
// strictly before that type's reserve. ok is false when two sessions of
// one day type share a date.
func derive(seed string, pools map[string][]time.Time) (sessions []Session, ok bool) {
	taken := map[time.Time]bool{}
	for _, c := range sessionLabels {
		pool := pools[c.dayType]
		d := pool[draw(seed+c.label, len(pool))]
		if taken[d] {
			return nil, false
		}
		taken[d] = true
		sessions = append(sessions, slot(seed, c.label, d))
	}
	return sessions, true
}

// NewSchedule derives the pilot schedule from in by the SPEC-HOSTED §4
// procedure. Each day type's reserve is its last eligible date on or
// before Last; its sessions are drawn from its eligible dates strictly
// before the reserve, each by SHA-256 of the seed and the session label
// reduced modulo the pool size. Where two sessions of a day type draw
// the same date, an incrementing counter is appended to the master seed
// and the whole schedule re-derived. The endpoint order is a shuffle of
// three of each order over the labels in fixed order, and each session
// and reserve has its own case permutation.
func NewSchedule(in ScheduleInput) (*Schedule, error) {
	switch {
	case in.MasterSeed == "":
		return nil, errors.New("pilot: schedule: empty master seed")
	case in.Endpoints[0] == "" || in.Endpoints[1] == "":
		return nil, errors.New("pilot: schedule: empty endpoint name")
	case in.Endpoints[0] == in.Endpoints[1]:
		return nil, fmt.Errorf("pilot: schedule: endpoint %q named twice", in.Endpoints[0])
	}
	byType, err := eligible(in)
	if err != nil {
		return nil, err
	}
	reserves := map[string]time.Time{}
	pools := map[string][]time.Time{}
	for _, dt := range []string{Weekday, Weekend} {
		ds := byType[dt]
		if len(ds) < SessionsPerDayType+1 {
			return nil, fmt.Errorf("pilot: schedule: %d eligible %s dates after %s up to %s, want %d", len(ds), dt, in.Freeze, in.Last, SessionsPerDayType+1)
		}
		reserves[dt] = ds[len(ds)-1]
		pools[dt] = ds[:len(ds)-1]
	}

	for counter := 0; counter <= maxCounter; counter++ {
		seed := in.MasterSeed
		if counter > 0 {
			seed += strconv.Itoa(counter)
		}
		sessions, ok := derive(seed, pools)
		if !ok {
			continue
		}
		orders := make([]int, len(sessions))
		for i := range orders {
			orders[i] = i % 2
		}
		for i := len(orders) - 1; i > 0; i-- {
			j := draw(fmt.Sprintf("%sorder/%d", seed, i), i+1)
			orders[i], orders[j] = orders[j], orders[i]
		}
		for i := range sessions {
			a, b := in.Endpoints[orders[i]], in.Endpoints[1-orders[i]]
			sessions[i].Order = []string{a, b}
		}
		slices.SortFunc(sessions, func(a, b Session) int { return a.Start.Compare(b.Start) })
		in.Exclude = slices.Clone(in.Exclude)
		return &Schedule{
			Input:    in,
			Counter:  counter,
			Sessions: sessions,
			Reserves: []Session{
				slot(seed, "reserve-"+Weekday, reserves[Weekday]),
				slot(seed, "reserve-"+Weekend, reserves[Weekend]),
			},
		}, nil
	}
	return nil, fmt.Errorf("pilot: schedule: no distinct dates within counter %d", maxCounter)
}
