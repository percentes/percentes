// Package schedule derives a release's temporal frame (SPEC-HOSTED §4
// grid as amended by HA3 point 9) from one master seed: twelve cells per
// route over the committed window, each route at its own offset inside
// the slot, with attempt seeds for the first run and the one re-run.
package schedule

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// Day types (SPEC-HOSTED §4: the UTC calendar day of the slot start).
const (
	Weekday = "weekday"
	Weekend = "weekend"
)

// The frame (SPEC-HOSTED §4 and HA3 point 9).
const (
	DateLayout = "2006-01-02"
	// MaxWindowDays bounds the committed window, first and last date
	// included.
	MaxWindowDays = 14
	// OffsetStep spaces the routes' offsets inside a slot.
	OffsetStep = 10 * time.Minute
	// StartTolerance applies to a route's offset start.
	StartTolerance = 15 * time.Minute
	// MaxAttempts per cell, and MaxReruns per route over the window.
	MaxAttempts = 2
	MaxReruns   = 2
	// RunSeedSuffix ends the key a cell's run seed is drawn from.
	RunSeedSuffix = "/run-seed"
)

// SlotHoursUTC are the six slot starts.
var SlotHoursUTC = []int{0, 4, 8, 12, 16, 20}

// slotLength is the time from one slot start to the next.
const slotLength = 4 * time.Hour

// CellLength is one cell's ladder under the release pins: the warm-up,
// then every step's measured window and drain.
var CellLength = time.Duration(config.PinnedReleaseWarmupS+len(config.PinnedReleaseStepsRPS)*(config.PinnedReleaseStepS+config.PinnedReleaseDrainS)) * time.Second

// maxCounter bounds the re-derivation counter.
const maxCounter = 10000

// Input is what the schedule is derived from. Dates are UTC calendar
// dates in DateLayout; the window runs from First to Last inclusive.
type Input struct {
	MasterSeed string `json:"master_seed"`
	First      string `json:"first"`
	Last       string `json:"last"`
	// Routes are the route names; New stores them sorted.
	Routes []string `json:"routes"`
	// Configs are the frozen configurations of routes, at most one per
	// route; New stores them in route order.
	Configs []RouteConfig `json:"configs,omitempty"`
}

// RouteConfig is a route's configuration as frozen with the schedule:
// the file's SHA-256 and the routes of its model, its own among them,
// sorted.
type RouteConfig struct {
	Route        string   `json:"route"`
	ConfigSHA256 string   `json:"config_sha256"`
	HostGroup    []string `json:"host_group"`
}

// Offset is a route's pinned start offset inside every slot.
type Offset struct {
	Route   string `json:"route"`
	Minutes int    `json:"minutes"`
}

// Attempt is one dated run of a cell. SlotStart is the slot's start; a
// route starts at SlotStart plus its offset.
type Attempt struct {
	Attempt   int       `json:"attempt"`
	Date      string    `json:"date"`
	SlotStart time.Time `json:"slot_start"`
	Seed      int64     `json:"seed"`
}

// Cell is one slot on one day type. Attempts holds attempt 1 and, where
// the window has a later date of the cell's day type, attempt 2 on the
// first such date.
type Cell struct {
	Label       string    `json:"label"`
	DayType     string    `json:"day_type"`
	SlotHourUTC int       `json:"slot_hour_utc"`
	Attempts    []Attempt `json:"attempts"`
}

// Schedule is the derived schedule. Counter is the value appended to the
// master seed by the Saturday-and-Sunday rule; 0 means the bare seed.
// Cells are in order of attempt 1's slot start.
type Schedule struct {
	Input   Input    `json:"input"`
	Counter int      `json:"counter"`
	Offsets []Offset `json:"offsets"`
	Cells   []Cell   `json:"cells"`
}

// Run is one route's run of one cell attempt.
type Run struct {
	Route   string    `json:"route"`
	Label   string    `json:"label"`
	Attempt int       `json:"attempt"`
	Date    string    `json:"date"`
	Start   time.Time `json:"start"`
	Seed    int64     `json:"seed"`
}

// OnTime reports whether a run that started at t is this run's cell.
func (r Run) OnTime(t time.Time) bool {
	d := t.Sub(r.Start)
	return d >= -StartTolerance && d <= StartTolerance
}

// labels are the cell labels in the fixed order the dates are drawn in.
var labels = func() []Cell {
	var out []Cell
	for _, dt := range []string{Weekday, Weekend} {
		for _, h := range SlotHoursUTC {
			out = append(out, Cell{Label: fmt.Sprintf("%s-%02d", dt, h), DayType: dt, SlotHourUTC: h})
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

// runSeed is the first 8 bytes of the SHA-256 of seed, label, attempt
// and RunSeedSuffix, big-endian, top bit cleared (SPEC-HOSTED §4).
func runSeed(seed, label string, attempt int) int64 {
	sum := sha256.Sum256([]byte(seed + label + strconv.Itoa(attempt) + RunSeedSuffix))
	return int64(binary.BigEndian.Uint64(sum[:8]) &^ (1 << 63))
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
		return time.Time{}, fmt.Errorf("schedule: %s %q: want %s", field, s, DateLayout)
	}
	return d, nil
}

// window returns, per day type, the window's dates in calendar order.
func window(in Input) (map[string][]time.Time, error) {
	first, err1 := parseDate("first", in.First)
	last, err2 := parseDate("last", in.Last)
	if err := errors.Join(err1, err2); err != nil {
		return nil, err
	}
	if last.Before(first) {
		return nil, fmt.Errorf("schedule: last %s is before first %s", in.Last, in.First)
	}
	if days := int(last.Sub(first).Hours()/24) + 1; days > MaxWindowDays {
		return nil, fmt.Errorf("schedule: window %s to %s is %d days, at most %d", in.First, in.Last, days, MaxWindowDays)
	}
	out := map[string][]time.Time{}
	saturday, sunday := false, false
	for d := first; !d.After(last); d = d.AddDate(0, 0, 1) {
		out[dayType(d)] = append(out[dayType(d)], d)
		saturday = saturday || d.Weekday() == time.Saturday
		sunday = sunday || d.Weekday() == time.Sunday
	}
	if len(out[Weekday]) == 0 || !saturday || !sunday {
		return nil, fmt.Errorf("schedule: window %s to %s needs a weekday, a Saturday and a Sunday", in.First, in.Last)
	}
	return out, nil
}

// derive draws each cell's attempt 1 date from its day type's dates.
// ok is false when the weekend dates miss a Saturday or a Sunday.
func derive(seed string, pools map[string][]time.Time) (cells []Cell, ok bool) {
	weekend := map[time.Weekday]bool{}
	for _, c := range labels {
		pool := pools[c.DayType]
		i := draw(seed+c.Label, len(pool))
		if c.DayType == Weekend {
			weekend[pool[i].Weekday()] = true
		}
		for n := 1; n <= MaxAttempts && i < len(pool); n, i = n+1, i+1 {
			c.Attempts = append(c.Attempts, Attempt{
				Attempt:   n,
				Date:      pool[i].Format(DateLayout),
				SlotStart: pool[i].Add(time.Duration(c.SlotHourUTC) * time.Hour),
				Seed:      runSeed(seed, c.Label, n),
			})
		}
		cells = append(cells, c)
	}
	return cells, weekend[time.Saturday] && weekend[time.Sunday]
}

// New derives the schedule from in by the SPEC-HOSTED §4 procedure as
// amended by HA3 point 9. Each cell's attempt 1 date is drawn from its
// day type's dates in the window by SHA-256 of the seed and the cell
// label reduced modulo their count; attempt 2 is the next date of that
// day type in the window. Where the six weekend dates miss a Saturday or
// a Sunday, an incrementing counter is appended to the master seed and
// the whole schedule re-derived. The offsets 0, 10, 20 ... minutes are
// shuffled over the routes in name order.
func New(in Input) (*Schedule, error) {
	if in.MasterSeed == "" {
		return nil, errors.New("schedule: empty master seed")
	}
	routes := slices.Clone(in.Routes)
	slices.Sort(routes)
	if len(routes) == 0 {
		return nil, errors.New("schedule: no routes")
	}
	for i, r := range routes {
		if r == "" {
			return nil, errors.New("schedule: empty route name")
		}
		if i > 0 && r == routes[i-1] {
			return nil, fmt.Errorf("schedule: route %q named twice", r)
		}
	}
	// A route's next cell can start one slot later on the same date.
	if last := time.Duration(len(routes)-1) * OffsetStep; last+StartTolerance+CellLength > slotLength {
		return nil, fmt.Errorf("schedule: %d routes: the last offset %v plus the %v tolerance and a %v cell exceed the %v slot", len(routes), last, StartTolerance, CellLength, slotLength)
	}
	pools, err := window(in)
	if err != nil {
		return nil, err
	}
	in.Routes = routes
	if in.Configs, err = routeConfigs(routes, in.Configs); err != nil {
		return nil, err
	}

	for counter := 0; counter <= maxCounter; counter++ {
		seed := in.MasterSeed
		if counter > 0 {
			seed += strconv.Itoa(counter)
		}
		cells, ok := derive(seed, pools)
		if !ok {
			continue
		}
		slices.SortFunc(cells, func(a, b Cell) int { return a.Attempts[0].SlotStart.Compare(b.Attempts[0].SlotStart) })
		minutes := make([]int, len(routes))
		for i := range minutes {
			minutes[i] = i * int(OffsetStep/time.Minute)
		}
		for i := len(minutes) - 1; i > 0; i-- {
			j := draw(fmt.Sprintf("%soffset/%d", seed, i), i+1)
			minutes[i], minutes[j] = minutes[j], minutes[i]
		}
		offsets := make([]Offset, len(routes))
		for i, r := range routes {
			offsets[i] = Offset{Route: r, Minutes: minutes[i]}
		}
		return &Schedule{Input: in, Counter: counter, Offsets: offsets, Cells: cells}, nil
	}
	return nil, fmt.Errorf("schedule: no Saturday and Sunday among the weekend dates within counter %d", maxCounter)
}

// routeConfigs checks cs against the sorted routes and returns them in
// route order with each host group sorted.
func routeConfigs(routes []string, cs []RouteConfig) ([]RouteConfig, error) {
	if len(cs) == 0 {
		return nil, nil
	}
	out := make([]RouteConfig, len(cs))
	for i, c := range cs {
		if !slices.Contains(routes, c.Route) {
			return nil, fmt.Errorf("schedule: configuration of route %q, which is not scheduled", c.Route)
		}
		if b, err := hex.DecodeString(c.ConfigSHA256); err != nil || len(b) != sha256.Size || hex.EncodeToString(b) != c.ConfigSHA256 {
			return nil, fmt.Errorf("schedule: route %q: config_sha256 %q is not a lowercase hex SHA-256", c.Route, c.ConfigSHA256)
		}
		c.HostGroup = slices.Clone(c.HostGroup)
		slices.Sort(c.HostGroup)
		if !slices.Contains(c.HostGroup, c.Route) || len(slices.Compact(slices.Clone(c.HostGroup))) != len(c.HostGroup) {
			return nil, fmt.Errorf("schedule: route %q: host group %v must name the route and each host once", c.Route, c.HostGroup)
		}
		out[i] = c
	}
	slices.SortFunc(out, func(a, b RouteConfig) int { return strings.Compare(a.Route, b.Route) })
	for i := 1; i < len(out); i++ {
		if out[i].Route == out[i-1].Route {
			return nil, fmt.Errorf("schedule: route %q has two configurations", out[i].Route)
		}
	}
	return out, nil
}

// Config returns route's frozen configuration, false when the schedule
// records none.
func (s *Schedule) Config(route string) (RouteConfig, bool) {
	for _, c := range s.Input.Configs {
		if c.Route == route {
			return c, true
		}
	}
	return RouteConfig{}, false
}

func (s *Schedule) offset(route string) (time.Duration, error) {
	for _, o := range s.Offsets {
		if o.Route == route {
			return time.Duration(o.Minutes) * time.Minute, nil
		}
	}
	return 0, fmt.Errorf("schedule: no route %q", route)
}

func (s *Schedule) run(route string, off time.Duration, c Cell, a Attempt) Run {
	return Run{Route: route, Label: c.Label, Attempt: a.Attempt, Date: a.Date, Start: a.SlotStart.Add(off), Seed: a.Seed}
}

// Runs returns route's twelve attempt 1 runs in start order.
func (s *Schedule) Runs(route string) ([]Run, error) {
	off, err := s.offset(route)
	if err != nil {
		return nil, err
	}
	out := make([]Run, 0, len(s.Cells))
	for _, c := range s.Cells {
		out = append(out, s.run(route, off, c, c.Attempts[0]))
	}
	return out, nil
}

// Rerun returns route's attempt 2 of the cell label, given the re-runs
// the route has already taken in the window. It refuses when the route
// has taken MaxReruns or the window holds no attempt 2 for the cell.
func (s *Schedule) Rerun(route, label string, taken int) (Run, error) {
	off, err := s.offset(route)
	if err != nil {
		return Run{}, err
	}
	if taken >= MaxReruns {
		return Run{}, fmt.Errorf("schedule: route %q has taken %d re-runs, at most %d", route, taken, MaxReruns)
	}
	for _, c := range s.Cells {
		if c.Label != label {
			continue
		}
		if len(c.Attempts) < 2 {
			return Run{}, fmt.Errorf("schedule: cell %s has no later %s date in the window", label, c.DayType)
		}
		return s.run(route, off, c, c.Attempts[1]), nil
	}
	return Run{}, fmt.Errorf("schedule: no cell %q", label)
}

// JSON is the committed form: indented, ending in a newline.
func (s *Schedule) JSON() ([]byte, error) {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Load reads a committed schedule and refuses it unless its bytes are
// the JSON of the schedule its own input derives.
func Load(data []byte) (*Schedule, error) {
	var got struct {
		Input Input `json:"input"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		return nil, fmt.Errorf("schedule: %w", err)
	}
	want, err := New(got.Input)
	if err != nil {
		return nil, err
	}
	b, err := want.JSON()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(data, b) {
		return nil, errors.New("schedule: not the schedule derived from its input")
	}
	return want, nil
}
