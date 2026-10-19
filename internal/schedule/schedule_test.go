package schedule

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
)

var routes = []string{"groq", "baseten", "anthropic", "google", "azure", "bedrock"}

// target is the design's target window, Saturday 17 to Monday 26
// October 2026.
func target() Input {
	return Input{MasterSeed: "percentes-release-schedule-test", First: "2026-10-17", Last: "2026-10-26", Routes: slices.Clone(routes)}
}

func derived(t *testing.T, in Input) *Schedule {
	t.Helper()
	s, err := New(in)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func date(s string) time.Time {
	d, err := time.Parse(DateLayout, s)
	if err != nil {
		panic(err)
	}
	return d
}

func mod(key string, n int) int {
	sum := sha256.Sum256([]byte(key))
	return int(new(big.Int).Mod(new(big.Int).SetBytes(sum[:]), big.NewInt(int64(n))).Int64())
}

func seedOf(key string) int64 {
	sum := sha256.Sum256([]byte(key))
	top := new(big.Int).SetBytes(sum[:8])
	return top.SetBit(top, 63, 0).Int64()
}

func isWeekend(d time.Time) bool {
	return d.Weekday() == time.Saturday || d.Weekday() == time.Sunday
}

// typePools lists the window's dates per day type in calendar order.
func typePools(in Input) map[string][]time.Time {
	out := map[string][]time.Time{}
	for d := date(in.First); !d.After(date(in.Last)); d = d.AddDate(0, 0, 1) {
		dt := Weekday
		if isWeekend(d) {
			dt = Weekend
		}
		out[dt] = append(out[dt], d)
	}
	return out
}

// checkSchedule verifies every rule against an independent recomputation
// from in.
func checkSchedule(t *testing.T, in Input, s *Schedule) {
	t.Helper()
	seed := in.MasterSeed
	if s.Counter > 0 {
		seed += strconv.Itoa(s.Counter)
	}
	pools := typePools(in)
	sorted := slices.Clone(in.Routes)
	slices.Sort(sorted)
	if !slices.Equal(s.Input.Routes, sorted) || s.Input.MasterSeed != in.MasterSeed || s.Input.First != in.First || s.Input.Last != in.Last {
		t.Errorf("input %+v, want %+v with routes sorted", s.Input, in)
	}

	if len(s.Cells) != 12 {
		t.Fatalf("%d cells, want 12", len(s.Cells))
	}
	labels := map[string]bool{}
	pairs := map[string]string{}
	perDate := map[string]int{}
	weekend := map[time.Weekday]bool{}
	for i, c := range s.Cells {
		dt, hour, _ := strings.Cut(c.Label, "-")
		h, _ := strconv.Atoi(hour)
		if c.DayType != dt || c.SlotHourUTC != h || !slices.Contains(SlotHoursUTC, h) || len(hour) != 2 {
			t.Errorf("%s: day type %s, slot %d", c.Label, c.DayType, c.SlotHourUTC)
		}
		labels[c.Label] = true
		if i > 0 && !c.Attempts[0].SlotStart.After(s.Cells[i-1].Attempts[0].SlotStart) {
			t.Errorf("%s: attempt 1 not after the previous cell's", c.Label)
		}
		pool := pools[dt]
		k := mod(seed+c.Label, len(pool))
		var want []time.Time
		for n := k; n < len(pool) && n < k+2; n++ {
			want = append(want, pool[n])
		}
		if len(c.Attempts) != len(want) {
			t.Errorf("%s: %d attempts, want %d", c.Label, len(c.Attempts), len(want))
			continue
		}
		for n, a := range c.Attempts {
			if a.Attempt != n+1 || a.Date != want[n].Format(DateLayout) {
				t.Errorf("%s: attempt %d on %s, want attempt %d on %s", c.Label, a.Attempt, a.Date, n+1, want[n].Format(DateLayout))
			}
			if !a.SlotStart.Equal(want[n].Add(time.Duration(h)*time.Hour)) || a.SlotStart.Location() != time.UTC {
				t.Errorf("%s attempt %d: slot start %v", c.Label, a.Attempt, a.SlotStart)
			}
			if got, w := a.Seed, seedOf(fmt.Sprintf("%s%s%d/run-seed", seed, c.Label, n+1)); got != w || got < 0 {
				t.Errorf("%s attempt %d: seed %d, want %d", c.Label, a.Attempt, got, w)
			}
			key := a.SlotStart.String()
			if prev, ok := pairs[key]; ok {
				t.Errorf("%s attempt %d: slot %s already held by %s", c.Label, a.Attempt, key, prev)
			}
			pairs[key] = c.Label
			perDate[a.Date]++
		}
		if dt == Weekend {
			weekend[want[0].Weekday()] = true
		}
	}
	if len(labels) != 12 {
		t.Errorf("labels %v", labels)
	}
	for d, n := range perDate {
		if n > admission.ReleaseDayCells {
			t.Errorf("%s carries %d attempts, at most %d", d, n, admission.ReleaseDayCells)
		}
	}
	if !weekend[time.Saturday] || !weekend[time.Sunday] {
		t.Errorf("weekend attempt 1 days %v miss a Saturday or a Sunday", weekend)
	}

	minutes := make([]int, len(sorted))
	for i := range minutes {
		minutes[i] = 10 * i
	}
	for i := len(minutes) - 1; i > 0; i-- {
		j := mod(fmt.Sprintf("%soffset/%d", seed, i), i+1)
		minutes[i], minutes[j] = minutes[j], minutes[i]
	}
	var want []Offset
	for i, r := range sorted {
		want = append(want, Offset{Route: r, Minutes: minutes[i]})
	}
	if !reflect.DeepEqual(s.Offsets, want) {
		t.Errorf("offsets %v, want %v", s.Offsets, want)
	}
}

func TestScheduleIsDeterministic(t *testing.T) {
	in := target()
	a, b := derived(t, in), derived(t, in)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("two derivations from the same input differ")
	}
	checkSchedule(t, in, a)

	var got []string
	for _, c := range a.Cells {
		line := c.Label
		for _, x := range c.Attempts {
			line += " " + x.Date
		}
		got = append(got, line)
	}
	for _, o := range a.Offsets {
		got = append(got, fmt.Sprintf("%s +%d", o.Route, o.Minutes))
	}
	want := []string{
		"weekend-04 2026-10-17 2026-10-18",
		"weekday-08 2026-10-19 2026-10-20",
		"weekday-16 2026-10-19 2026-10-20",
		"weekday-04 2026-10-20 2026-10-21",
		"weekday-00 2026-10-21 2026-10-22",
		"weekday-12 2026-10-21 2026-10-22",
		"weekday-20 2026-10-22 2026-10-23",
		"weekend-08 2026-10-24 2026-10-25",
		"weekend-12 2026-10-24 2026-10-25",
		"weekend-20 2026-10-24 2026-10-25",
		"weekend-00 2026-10-25",
		"weekend-16 2026-10-25",
		"anthropic +10",
		"azure +40",
		"baseten +20",
		"bedrock +50",
		"google +30",
		"groq +0",
	}
	if !slices.Equal(got, want) || a.Counter != 0 {
		t.Errorf("counter %d, schedule\n%s\nwant counter 0,\n%s", a.Counter, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	j, err := a.JSON()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(j)
	if got, want := hex.EncodeToString(sum[:]), "502c5903b833834a2b5f88d2d859d8a93b31f66d521ff617101083a2f18b322e"; got != want {
		t.Errorf("schedule JSON sha256 %s, want %s", got, want)
	}

	shuffled := target()
	slices.Reverse(shuffled.Routes)
	if c := derived(t, shuffled); !reflect.DeepEqual(a, c) {
		t.Error("route order in the input changed the schedule")
	}
	in.MasterSeed += "x"
	if c := derived(t, in); reflect.DeepEqual(a.Cells, c.Cells) {
		t.Error("a different master seed gave the same cells")
	}
}

// TestScheduleWindows checks every rule over many seeds and the design's
// three windows.
func TestScheduleWindows(t *testing.T) {
	windows := [][2]string{
		{"2026-10-17", "2026-10-26"}, // target
		{"2026-10-21", "2026-10-26"}, // fallback
		{"2026-10-24", "2026-10-26"}, // last
		{"2026-10-12", "2026-10-25"}, // 14 days
	}
	counters := 0
	for _, w := range windows {
		for i := 0; i < 40; i++ {
			in := Input{MasterSeed: fmt.Sprintf("seed-%d", i), First: w[0], Last: w[1], Routes: routes[:1+i%len(routes)]}
			s := derived(t, in)
			checkSchedule(t, in, s)
			if s.Counter > 0 {
				counters++
			}
		}
	}
	if counters == 0 {
		t.Error("no derivation needed the counter; the Saturday-and-Sunday rule is untested")
	}
}

// TestScheduleCounter shows the counter in use: with one Saturday and one
// Sunday in the window, the bare seed draws every weekend cell onto one
// of them.
func TestScheduleCounter(t *testing.T) {
	for i := 0; i < 1000; i++ {
		in := Input{MasterSeed: fmt.Sprintf("counter-%d", i), First: "2026-10-24", Last: "2026-10-26", Routes: []string{"r"}}
		days := map[time.Weekday]bool{}
		for _, h := range SlotHoursUTC {
			k := mod(fmt.Sprintf("%sweekend-%02d", in.MasterSeed, h), 2)
			days[date("2026-10-24").AddDate(0, 0, k).Weekday()] = true
		}
		if len(days) == 2 {
			continue
		}
		s := derived(t, in)
		if s.Counter == 0 {
			t.Fatalf("%s: weekend draws %v only, counter 0", in.MasterSeed, days)
		}
		checkSchedule(t, in, s)
		return
	}
	t.Fatal("no seed found whose bare draws miss a weekend day")
}

func TestRunsAndReruns(t *testing.T) {
	s := derived(t, target())
	for _, o := range s.Offsets {
		runs, err := s.Runs(o.Route)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) != len(s.Cells) {
			t.Fatalf("%s: %d runs", o.Route, len(runs))
		}
		for i, r := range runs {
			c := s.Cells[i]
			a := c.Attempts[0]
			want := Run{Route: o.Route, Label: c.Label, Attempt: 1, Date: a.Date, Start: a.SlotStart.Add(time.Duration(o.Minutes) * time.Minute), Seed: a.Seed}
			if r != want {
				t.Errorf("%s run %d: %+v, want %+v", o.Route, i, r, want)
			}
		}
	}

	r, err := s.Rerun("bedrock", "weekday-08", 1)
	if err != nil {
		t.Fatal(err)
	}
	want := Run{Route: "bedrock", Label: "weekday-08", Attempt: 2, Date: "2026-10-20", Start: time.Date(2026, 10, 20, 8, 50, 0, 0, time.UTC), Seed: seedOf(s.Input.MasterSeed + "weekday-082/run-seed")}
	if r != want {
		t.Errorf("rerun %+v, want %+v", r, want)
	}
	for _, c := range []struct {
		route, label string
		taken        int
		err          string
	}{
		{"groq", "weekday-08", 2, "has taken 2 re-runs, at most 2"},
		{"groq", "weekend-00", 0, "cell weekend-00 has no later weekend date"},
		{"openai", "weekday-08", 0, `no route "openai"`},
		{"groq", "weekday-09", 0, `no cell "weekday-09"`},
	} {
		if _, err := s.Rerun(c.route, c.label, c.taken); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("Rerun(%s, %s, %d): %v, want %q", c.route, c.label, c.taken, err, c.err)
		}
	}
	if _, err := s.Runs("openai"); err == nil {
		t.Error("Runs accepted an unknown route")
	}
}

func TestOnTime(t *testing.T) {
	start := time.Date(2026, 10, 19, 8, 20, 0, 0, time.UTC)
	r := Run{Start: start}
	for _, c := range []struct {
		d  time.Duration
		ok bool
	}{
		{0, true}, {-15 * time.Minute, true}, {15 * time.Minute, true},
		{15*time.Minute + time.Second, false}, {-15*time.Minute - time.Second, false},
	} {
		if got := r.OnTime(start.Add(c.d)); got != c.ok {
			t.Errorf("start %+v: on time %v, want %v", c.d, got, c.ok)
		}
	}
}

func TestLoad(t *testing.T) {
	s := derived(t, target())
	j, err := s.JSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Load(j)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Error("loaded schedule differs from the derived one")
	}
	a := s.Cells[0].Attempts[0]
	for name, edit := range map[string][2]string{
		"date":    {`"date": "` + a.Date + `"`, `"date": "2026-10-26"`},
		"seed":    {strconv.FormatInt(a.Seed, 10), "1"},
		"offset":  {`"minutes": 0`, `"minutes": 60`},
		"counter": {`"counter": 0`, `"counter": 1`},
		"routes":  {`"anthropic",`, `"zeta",`},
		"newline": {"}\n", "}"},
	} {
		bad := bytes.Replace(j, []byte(edit[0]), []byte(edit[1]), 1)
		if bytes.Equal(bad, j) {
			t.Fatalf("%s: edit %q not found", name, edit[0])
		}
		if _, err := Load(bad); err == nil {
			t.Errorf("%s edited: loaded", name)
		}
	}
	if _, err := Load([]byte("{")); err == nil {
		t.Error("malformed JSON loaded")
	}
}

func TestNewRefuses(t *testing.T) {
	many := make([]string, 19)
	for i := range many {
		many[i] = fmt.Sprintf("r%02d", i)
	}
	for _, c := range []struct {
		name string
		edit func(*Input)
		err  string
	}{
		{"empty seed", func(in *Input) { in.MasterSeed = "" }, "empty master seed"},
		{"no routes", func(in *Input) { in.Routes = nil }, "no routes"},
		{"empty route", func(in *Input) { in.Routes[2] = "" }, "empty route name"},
		{"route twice", func(in *Input) { in.Routes[2] = "groq" }, `route "groq" named twice`},
		{"19 routes", func(in *Input) { in.Routes = many }, "19 routes"},
		{"bad first", func(in *Input) { in.First = "17/10/2026" }, `first "17/10/2026"`},
		{"bad last", func(in *Input) { in.Last = "" }, `last ""`},
		{"last before first", func(in *Input) { in.Last = "2026-10-16" }, "is before first"},
		{"15 days", func(in *Input) { in.Last = "2026-10-31" }, "is 15 days, at most 14"},
		{"no Sunday", func(in *Input) { in.First, in.Last = "2026-10-19", "2026-10-24" }, "needs a weekday, a Saturday and a Sunday"},
		{"no Saturday", func(in *Input) { in.First, in.Last = "2026-10-18", "2026-10-23" }, "needs a weekday, a Saturday and a Sunday"},
		{"no weekday", func(in *Input) { in.First, in.Last = "2026-10-17", "2026-10-18" }, "needs a weekday, a Saturday and a Sunday"},
	} {
		in := target()
		c.edit(&in)
		if _, err := New(in); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: %v, want %q", c.name, err, c.err)
		}
	}
	in := target()
	in.Routes = many[:18]
	if _, err := New(in); err != nil {
		t.Errorf("18 routes: %v", err)
	}
	in = target()
	in.First, in.Last = "2026-10-12", "2026-10-25"
	if _, err := New(in); err != nil {
		t.Errorf("14 days: %v", err)
	}
}

// New stores the frozen configurations in route order with each host
// group sorted, the committed form loads, and the draws are the ones of
// the bare input.
func TestNewConfigs(t *testing.T) {
	sum := strings.Repeat("ab", 32)
	in := target()
	in.Configs = []RouteConfig{
		{Route: "groq", ConfigSHA256: sum, HostGroup: []string{"groq", "baseten"}},
		{Route: "baseten", ConfigSHA256: sum, HostGroup: []string{"groq", "baseten"}},
	}
	s := derived(t, in)
	want := []RouteConfig{
		{Route: "baseten", ConfigSHA256: sum, HostGroup: []string{"baseten", "groq"}},
		{Route: "groq", ConfigSHA256: sum, HostGroup: []string{"baseten", "groq"}},
	}
	if !reflect.DeepEqual(s.Input.Configs, want) {
		t.Fatalf("configs %+v, want %+v", s.Input.Configs, want)
	}
	if c, ok := s.Config("groq"); !ok || !reflect.DeepEqual(c, want[1]) {
		t.Errorf("Config(groq): %+v, %t", c, ok)
	}
	if _, ok := s.Config("azure"); ok {
		t.Error("Config(azure) found a configuration none froze")
	}
	bare := derived(t, target())
	if !reflect.DeepEqual(s.Cells, bare.Cells) || !reflect.DeepEqual(s.Offsets, bare.Offsets) {
		t.Error("the frozen configurations changed the draws")
	}
	b, err := s.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if l, err := Load(b); err != nil || !reflect.DeepEqual(l.Input.Configs, want) {
		t.Fatalf("load: %v, configs %+v", err, l.Input.Configs)
	}

	for _, c := range []struct {
		name string
		cfg  RouteConfig
		err  string
	}{
		{"not scheduled", RouteConfig{Route: "mistral", ConfigSHA256: sum, HostGroup: []string{"mistral"}}, `configuration of route "mistral", which is not scheduled`},
		{"short hash", RouteConfig{Route: "azure", ConfigSHA256: "abcd", HostGroup: []string{"azure"}}, `route "azure": config_sha256 "abcd"`},
		{"upper-case hash", RouteConfig{Route: "azure", ConfigSHA256: strings.ToUpper(sum), HostGroup: []string{"azure"}}, "is not a lowercase hex SHA-256"},
		{"group without the route", RouteConfig{Route: "azure", ConfigSHA256: sum, HostGroup: []string{"bedrock"}}, `route "azure": host group [bedrock] must name the route`},
		{"host twice", RouteConfig{Route: "azure", ConfigSHA256: sum, HostGroup: []string{"azure", "bedrock", "azure"}}, "host group [azure azure bedrock]"},
		{"route twice", RouteConfig{Route: "groq", ConfigSHA256: sum, HostGroup: []string{"groq"}}, `route "groq" has two configurations`},
	} {
		bad := target()
		bad.Configs = append(slices.Clone(in.Configs), c.cfg)
		if _, err := New(bad); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: %v, want %q", c.name, err, c.err)
		}
	}
}

// New keeps the frozen ledger's hash in the committed form without
// changing the draws, and refuses one that is not a lowercase hex
// SHA-256.
func TestNewLedger(t *testing.T) {
	sum := strings.Repeat("cd", 32)
	in := target()
	in.LedgerSHA256 = sum
	s := derived(t, in)
	bare := derived(t, target())
	if !reflect.DeepEqual(s.Cells, bare.Cells) || !reflect.DeepEqual(s.Offsets, bare.Offsets) {
		t.Error("the frozen ledger changed the draws")
	}
	b, err := s.JSON()
	if err != nil {
		t.Fatal(err)
	}
	l, err := Load(b)
	if err != nil {
		t.Fatal(err)
	}
	if l.Input.LedgerSHA256 != sum || !bytes.Contains(b, []byte(`"ledger_sha256": "`+sum+`"`)) {
		t.Fatalf("committed ledger %q in\n%s", l.Input.LedgerSHA256, b)
	}
	for _, bad := range []string{"abcd", strings.ToUpper(sum), sum + "00"} {
		in.LedgerSHA256 = bad
		if _, err := New(in); err == nil || !strings.Contains(err.Error(), `ledger_sha256 "`+bad+`" is not a lowercase hex SHA-256`) {
			t.Errorf("%s: %v", bad, err)
		}
	}
}

// TestEdition ties the frame to the ceiling rule's edition and the ladder.
func TestEdition(t *testing.T) {
	if n := 2 * len(SlotHoursUTC); n != admission.ReleaseCells {
		t.Errorf("%d cells, admission plans %d", n, admission.ReleaseCells)
	}
	if MaxReruns != admission.ReleaseReruns {
		t.Errorf("%d re-runs, admission plans %d", MaxReruns, admission.ReleaseReruns)
	}
	if len(SlotHoursUTC) != admission.ReleaseDayCells {
		t.Errorf("%d slots a day, admission plans %d cells a day", len(SlotHoursUTC), admission.ReleaseDayCells)
	}
	if CellLength != 48*time.Minute+30*time.Second {
		t.Errorf("cell length %v, want 48m30s", CellLength)
	}
}
