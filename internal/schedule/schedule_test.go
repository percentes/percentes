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
	"github.com/percentes/percentes/internal/config"
)

var routes = []string{"anthropic-R", "azure-S", "azure-R", "gemini-S", "baseten-S"}

// target is the design's target window, Saturday 31 October to Monday 9
// November 2026.
func target() Input {
	return Input{MasterSeed: "percentes-release-schedule-test", First: "2026-10-31", Last: "2026-11-09", Routes: slices.Clone(routes)}
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
		k := mod(seed+c.Label, len(pool)-1)
		want := []time.Time{pool[k], pool[k+1]}
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
		if n > len(SlotHoursUTC) {
			t.Errorf("%s carries %d attempts, at most %d", d, n, len(SlotHoursUTC))
		}
	}
	if !weekend[time.Saturday] || !weekend[time.Sunday] {
		t.Errorf("weekend attempt 1 days %v miss a Saturday or a Sunday", weekend)
	}

	group := map[string]string{}
	for _, c := range in.Configs {
		group[c.Route] = c.AccountGroup
	}
	for counter := 0; ; counter++ {
		key := seed + "offset"
		if counter > 0 {
			key += strconv.Itoa(counter)
		}
		minutes := make([]int, len(sorted))
		for i := range minutes {
			minutes[i] = 40 * i
		}
		for i := len(minutes) - 1; i > 0; i-- {
			j := mod(fmt.Sprintf("%s/%d", key, i), i+1)
			minutes[i], minutes[j] = minutes[j], minutes[i]
		}
		apart := true
		for i := range sorted {
			for j := range sorted {
				if g := group[sorted[i]]; i == j || g == "" || g != group[sorted[j]] {
					continue
				}
				if d := minutes[i] - minutes[j]; d >= 0 && (d < 80 || 240-d < 80) {
					apart = false
				}
			}
		}
		if !apart {
			if counter > 100 {
				t.Fatal("no separated offsets within counter 100")
			}
			continue
		}
		var want []Offset
		for i, r := range sorted {
			want = append(want, Offset{Route: r, Minutes: minutes[i]})
		}
		if !reflect.DeepEqual(s.Offsets, want) || s.OffsetCounter != counter {
			t.Errorf("offsets %v at counter %d, want %v at %d", s.Offsets, s.OffsetCounter, want, counter)
		}
		return
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
		"weekend-00 2026-10-31 2026-11-01",
		"weekend-08 2026-10-31 2026-11-01",
		"weekend-12 2026-11-01 2026-11-07",
		"weekday-12 2026-11-02 2026-11-03",
		"weekday-20 2026-11-02 2026-11-03",
		"weekday-08 2026-11-04 2026-11-05",
		"weekday-16 2026-11-04 2026-11-05",
		"weekday-00 2026-11-05 2026-11-06",
		"weekday-04 2026-11-05 2026-11-06",
		"weekend-04 2026-11-07 2026-11-08",
		"weekend-16 2026-11-07 2026-11-08",
		"weekend-20 2026-11-07 2026-11-08",
		"anthropic-R +40",
		"azure-R +160",
		"azure-S +80",
		"baseten-S +0",
		"gemini-S +120",
	}
	if !slices.Equal(got, want) || a.Counter != 0 {
		t.Errorf("counter %d, schedule\n%s\nwant counter 0,\n%s", a.Counter, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	j, err := a.JSON()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(j)
	if got, want := hex.EncodeToString(sum[:]), "cbd3a52396348462e94b879f75a934997423a8fb4369e802141882a1ef091e95"; got != want {
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
		{"2026-10-31", "2026-11-09"}, // target
		{"2026-11-07", "2026-11-16"}, // fallback
		{"2026-10-24", "2026-10-31"}, // one Saturday and one Sunday before the last weekend date
		{"2026-10-26", "2026-11-08"}, // 14 days
		{"2026-11-14", "2026-11-23"}, // freeze 13 Nov
	}
	counters, offsetCounters := 0, 0
	for _, w := range windows {
		for i := 0; i < 40; i++ {
			in := Input{MasterSeed: fmt.Sprintf("seed-%d", i), First: w[0], Last: w[1], Routes: routes[:1+i%len(routes)]}
			if len(in.Routes) >= 3 {
				in.Configs = azureGroup()
			}
			s := derived(t, in)
			checkSchedule(t, in, s)
			if s.Counter > 0 {
				counters++
			}
			if s.OffsetCounter > 0 {
				offsetCounters++
			}
		}
	}
	if counters == 0 || offsetCounters == 0 {
		t.Errorf("%d derivations needed the counter and %d the offset counter; a rule is untested", counters, offsetCounters)
	}
}

// The longest window holds two weekdays, a Saturday and a Sunday before
// each day type's last date from any first day of the week; ten days from
// a Monday, Tuesday or Wednesday hold one weekend and are refused.
func TestMaxWindowEveryWeekday(t *testing.T) {
	monday := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 7; i++ {
		first := monday.AddDate(0, 0, i)
		in := Input{First: first.Format(DateLayout), Last: first.AddDate(0, 0, MaxWindowDays-1).Format(DateLayout)}
		if _, err := window(in); err != nil {
			t.Errorf("%s: %v", first.Weekday(), err)
		}
		in.Last = first.AddDate(0, 0, 9).Format(DateLayout)
		if _, err := window(in); (err != nil) != (i < 3) {
			t.Errorf("ten days from %s: %v", first.Weekday(), err)
		}
	}
}

// TestScheduleCounter shows the counter in use: with one Saturday and one
// Sunday before the window's last weekend date, the bare seed draws every
// weekend cell onto one of them.
func TestScheduleCounter(t *testing.T) {
	for i := 0; i < 1000; i++ {
		in := Input{MasterSeed: fmt.Sprintf("counter-%d", i), First: "2026-10-24", Last: "2026-10-31", Routes: []string{"r"}}
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

	for _, c := range s.Cells {
		if len(c.Attempts) != 2 {
			t.Errorf("%s: %d attempts, want 2", c.Label, len(c.Attempts))
		}
	}
	off, _ := s.offset("baseten-S")
	for _, c := range s.Cells {
		r, err := s.Rerun("baseten-S", c.Label, 1)
		if err != nil {
			t.Fatal(err)
		}
		a := c.Attempts[1]
		want := Run{Route: "baseten-S", Label: c.Label, Attempt: 2, Date: a.Date, Start: a.SlotStart.Add(off), Seed: seedOf(s.Input.MasterSeed + c.Label + "2/run-seed")}
		if r != want {
			t.Errorf("rerun %+v, want %+v", r, want)
		}
	}
	for _, c := range []struct {
		route, label string
		taken        int
		err          string
	}{
		{"gemini-S", "weekday-08", 2, "has taken 2 re-runs, at most 2"},
		{"openai", "weekday-08", 0, `no route "openai"`},
		{"gemini-S", "weekday-09", 0, `no cell "weekday-09"`},
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
		"date":           {`"date": "` + a.Date + `"`, `"date": "2026-11-09"`},
		"seed":           {strconv.FormatInt(a.Seed, 10), "1"},
		"offset":         {`"minutes": 0`, `"minutes": 60`},
		"counter":        {`"counter": 0`, `"counter": 1`},
		"offset counter": {`"offset_counter": 0`, `"offset_counter": 1`},
		"routes":         {`"anthropic-R",`, `"zeta",`},
		"newline":        {"}\n", "}"},
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
	six := append(slices.Clone(routes), "zeta")
	for _, c := range []struct {
		name string
		edit func(*Input)
		err  string
	}{
		{"empty seed", func(in *Input) { in.MasterSeed = "" }, "empty master seed"},
		{"no routes", func(in *Input) { in.Routes = nil }, "no routes"},
		{"empty route", func(in *Input) { in.Routes[2] = "" }, "empty route name"},
		{"route twice", func(in *Input) { in.Routes[2] = "azure-S" }, `route "azure-S" named twice`},
		{"6 routes", func(in *Input) { in.Routes = six }, "6 routes: the last offset 3h20m0s plus the 15m0s tolerance and a 28m30s cell exceed the 4h0m0s slot"},
		{"bad first", func(in *Input) { in.First = "31/10/2026" }, `first "31/10/2026"`},
		{"bad last", func(in *Input) { in.Last = "" }, `last ""`},
		{"last before first", func(in *Input) { in.Last = "2026-10-30" }, "is before first"},
		{"15 days", func(in *Input) { in.Last = "2026-11-14" }, "is 15 days, at most 14"},
		{"no Sunday", func(in *Input) { in.First, in.Last = "2026-10-19", "2026-10-24" }, "needs a weekday, a Saturday and a Sunday before the last date of each day type"},
		{"no Saturday", func(in *Input) { in.First, in.Last = "2026-10-18", "2026-10-23" }, "needs a weekday, a Saturday and a Sunday before the last date of each day type"},
		{"no weekday", func(in *Input) { in.First, in.Last = "2026-10-17", "2026-10-18" }, "needs a weekday, a Saturday and a Sunday before the last date of each day type"},
		{"one weekend", func(in *Input) { in.First, in.Last = "2026-10-31", "2026-11-05" }, "needs a weekday, a Saturday and a Sunday before the last date of each day type"},
		{"group of four", func(in *Input) {
			for _, r := range routes[:4] {
				in.Configs = append(in.Configs, RouteConfig{Route: r, ConfigSHA256: strings.Repeat("ab", 32), HostGroup: []string{r}, AccountGroup: "one"})
			}
		}, "no offsets within counter 10000 start each account group's routes 1h20m0s apart"},
	} {
		in := target()
		c.edit(&in)
		if _, err := New(in); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: %v, want %q", c.name, err, c.err)
		}
	}
	in := target()
	in.First, in.Last = "2026-10-26", "2026-11-08"
	if _, err := New(in); err != nil {
		t.Errorf("14 days: %v", err)
	}
}

// azureGroup freezes azure-S and azure-R as one account group.
func azureGroup() []RouteConfig {
	sum := strings.Repeat("ab", 32)
	return []RouteConfig{
		{Route: "azure-S", ConfigSHA256: sum, HostGroup: []string{"azure-S"}, AccountGroup: "azure"},
		{Route: "azure-R", ConfigSHA256: sum, HostGroup: []string{"azure-R"}, AccountGroup: "azure"},
	}
}

// The routes of one account group start at least 80 minutes apart both
// ways round the slot. Where a draw breaks that, the offset counter
// re-draws it; the committed counter loads, and the accepted offsets
// spread over the allowed pairs.
func TestAccountGroupSeparation(t *testing.T) {
	pairs := map[[2]int]int{}
	counters := 0
	for i := 0; i < 300; i++ {
		in := target()
		in.MasterSeed = fmt.Sprintf("group-%d", i)
		in.Configs = azureGroup()
		s := derived(t, in)
		checkSchedule(t, in, s)
		a, _ := s.offset("azure-S")
		b, _ := s.offset("azure-R")
		d := (a - b + slotLength) % slotLength
		if min(d, slotLength-d) < GroupSeparation {
			t.Fatalf("%s: azure-S at %v, azure-R at %v", in.MasterSeed, a, b)
		}
		pairs[[2]int{int(min(a, b) / time.Minute), int(max(a, b) / time.Minute)}]++
		if s.OffsetCounter == 0 {
			continue
		}
		counters++
		j, err := s.JSON()
		if err != nil {
			t.Fatal(err)
		}
		if l, err := Load(j); err != nil || l.OffsetCounter != s.OffsetCounter {
			t.Fatalf("load at offset counter %d: %v", s.OffsetCounter, err)
		}
		edit := fmt.Sprintf(`"offset_counter": %d`, s.OffsetCounter)
		if _, err := Load(bytes.Replace(j, []byte(edit), []byte(`"offset_counter": 0`), 1)); err == nil {
			t.Error("a schedule with its offset counter reset loaded")
		}
		bare := in
		bare.Configs = nil
		if reflect.DeepEqual(derived(t, bare).Offsets, s.Offsets) {
			t.Errorf("%s: the re-drawn offsets equal the bare draw's", in.MasterSeed)
		}
	}
	want := [][2]int{{0, 80}, {0, 120}, {0, 160}, {40, 120}, {40, 160}, {80, 160}}
	var got [][2]int
	for p := range pairs {
		got = append(got, p)
	}
	slices.SortFunc(got, func(a, b [2]int) int { return a[0]*1000 + a[1] - b[0]*1000 - b[1] })
	if !slices.Equal(got, want) || counters == 0 {
		t.Errorf("offset pairs %v, want %v; %d re-draws", got, want, counters)
	}
}

// New stores the frozen configurations in route order with each host
// group sorted, the committed form loads with each keyed hash, and the
// draws are the ones of the bare input.
func TestNewConfigs(t *testing.T) {
	sum, keyed := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	in := target()
	in.Configs = []RouteConfig{
		{Route: "gemini-S", ConfigSHA256: sum, ConfigHMACSHA256: keyed, HostGroup: []string{"gemini-S", "baseten-S"}, AccountGroup: "gemini", BindingRPS: 75.454, BindingLimit: config.LimitRPM, BindingWindowS: 1},
		{Route: "baseten-S", ConfigSHA256: sum, HostGroup: []string{"gemini-S", "baseten-S"}, AccountGroup: "baseten", BindingRPS: 0.249, BindingLimit: config.LimitRPM, BindingWindowS: 60},
	}
	s := derived(t, in)
	want := []RouteConfig{
		{Route: "baseten-S", ConfigSHA256: sum, HostGroup: []string{"baseten-S", "gemini-S"}, AccountGroup: "baseten", BindingRPS: 0.249, BindingLimit: config.LimitRPM, BindingWindowS: 60},
		{Route: "gemini-S", ConfigSHA256: sum, ConfigHMACSHA256: keyed, HostGroup: []string{"baseten-S", "gemini-S"}, AccountGroup: "gemini", BindingRPS: 75.454, BindingLimit: config.LimitRPM, BindingWindowS: 1},
	}
	if !reflect.DeepEqual(s.Input.Configs, want) {
		t.Fatalf("configs %+v, want %+v", s.Input.Configs, want)
	}
	if c, ok := s.Config("gemini-S"); !ok || !reflect.DeepEqual(c, want[1]) {
		t.Errorf("Config(gemini-S): %+v, %t", c, ok)
	}
	if _, ok := s.Config("azure-S"); ok {
		t.Error("Config(azure-S) found a configuration none froze")
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
		{"short hash", RouteConfig{Route: "azure-S", ConfigSHA256: "abcd", HostGroup: []string{"azure-S"}}, `route "azure-S": config_sha256 "abcd"`},
		{"upper-case hash", RouteConfig{Route: "azure-S", ConfigSHA256: strings.ToUpper(sum), HostGroup: []string{"azure-S"}}, "is not a lowercase hex SHA-256"},
		{"short keyed hash", RouteConfig{Route: "azure-S", ConfigSHA256: sum, ConfigHMACSHA256: "abcd", HostGroup: []string{"azure-S"}}, `route "azure-S": config_hmac_sha256 "abcd" is not a lowercase hex HMAC-SHA256`},
		{"group without the route", RouteConfig{Route: "azure-S", ConfigSHA256: sum, HostGroup: []string{"azure-R"}}, `route "azure-S": host group [azure-R] must name the route`},
		{"host twice", RouteConfig{Route: "azure-S", ConfigSHA256: sum, HostGroup: []string{"azure-S", "azure-R", "azure-S"}}, "host group [azure-R azure-S azure-S]"},
		{"route twice", RouteConfig{Route: "gemini-S", ConfigSHA256: sum, HostGroup: []string{"gemini-S"}}, `route "gemini-S" has two configurations`},
		{"binding rate unnamed", RouteConfig{Route: "azure-S", ConfigSHA256: sum, HostGroup: []string{"azure-S"}, BindingRPS: 2.429}, `route "azure-S": binding_rps 2.429 with no binding_limit`},
		{"binding rate unrounded", RouteConfig{Route: "azure-S", ConfigSHA256: sum, HostGroup: []string{"azure-S"}, BindingRPS: 2.429285, BindingLimit: config.LimitTPM, BindingWindowS: 60}, "binding_rps 2.429285 is not a positive rate rounded down to 0.001"},
		{"binding limit without a rate", RouteConfig{Route: "azure-S", ConfigSHA256: sum, HostGroup: []string{"azure-S"}, BindingLimit: config.LimitTPM}, "binding_rps 0 is not a positive rate"},
		{"binding window negative", RouteConfig{Route: "azure-S", ConfigSHA256: sum, HostGroup: []string{"azure-S"}, BindingRPS: 2.429, BindingLimit: config.LimitTPM, BindingWindowS: -60}, "binding_window_s -60: want 0 or more seconds"},
	} {
		bad := target()
		bad.Configs = append(slices.Clone(in.Configs), c.cfg)
		if _, err := New(bad); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: %v, want %q", c.name, err, c.err)
		}
	}
}

// New keeps the frozen ledger's hash and position in the committed form
// without changing the draws, and refuses a hash that is not a lowercase
// hex SHA-256 and a position given in part or without the ledger's hash.
func TestNewLedger(t *testing.T) {
	sum, head := strings.Repeat("cd", 32), strings.Repeat("ef", 32)
	in := target()
	in.LedgerSHA256, in.LedgerLines, in.LedgerHeadSHA256 = sum, 7, head
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
	if l.Input.LedgerLines != 7 || l.Input.LedgerHeadSHA256 != head || !bytes.Contains(b, []byte(`"ledger_lines": 7,`)) || !bytes.Contains(b, []byte(`"ledger_head_sha256": "`+head+`"`)) {
		t.Fatalf("committed ledger position %d, %q in\n%s", l.Input.LedgerLines, l.Input.LedgerHeadSHA256, b)
	}
	for _, bad := range []string{"abcd", strings.ToUpper(sum), sum + "00"} {
		in.LedgerSHA256 = bad
		if _, err := New(in); err == nil || !strings.Contains(err.Error(), `ledger_sha256 "`+bad+`" is not a lowercase hex SHA-256`) {
			t.Errorf("%s: %v", bad, err)
		}
		in.LedgerSHA256, in.LedgerHeadSHA256 = sum, bad
		if _, err := New(in); err == nil || !strings.Contains(err.Error(), `ledger_head_sha256 "`+bad+`" is not a lowercase hex SHA-256`) {
			t.Errorf("head %s: %v", bad, err)
		}
		in.LedgerHeadSHA256 = head
	}
	for _, c := range []struct {
		name, ledger, head string
		lines              int
		err                string
	}{
		{"no head", sum, "", 7, "both set or neither"},
		{"no lines", sum, head, 0, "both set or neither"},
		{"negative lines", sum, head, -1, "both set or neither"},
		{"no ledger", "", head, 7, "a ledger position with no ledger_sha256"},
	} {
		in.LedgerSHA256, in.LedgerHeadSHA256, in.LedgerLines = c.ledger, c.head, c.lines
		if _, err := New(in); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: %v, want %q", c.name, err, c.err)
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
	if StartTolerance != admission.ReleaseStartTolerance {
		t.Errorf("start tolerance %v, admission takes an attempt's interval at %v", StartTolerance, admission.ReleaseStartTolerance)
	}
	if last := 4 * OffsetStep; last+StartTolerance+CellLength != 203*time.Minute+30*time.Second {
		t.Errorf("five routes end by %v", last+StartTolerance+CellLength)
	}
	if CellLength != 28*time.Minute+30*time.Second {
		t.Errorf("cell length %v, want 28m30s", CellLength)
	}
}
