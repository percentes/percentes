package pilot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

var endpoints = [2]string{"groq", "baseten"}

func october() ScheduleInput {
	return ScheduleInput{MasterSeed: "percentes-pilot-schedule-test", Freeze: "2026-10-11", Last: "2026-10-31", Endpoints: endpoints}
}

// away lists 4 to 16 November 2026.
func away() []string {
	var out []string
	for d := 4; d <= 16; d++ {
		out = append(out, fmt.Sprintf("2026-11-%02d", d))
	}
	return out
}

func schedule(t *testing.T, in ScheduleInput) *Schedule {
	t.Helper()
	s, err := NewSchedule(in)
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

// pools recomputes, per day type, the reserve and the dates before it.
func pools(in ScheduleInput) (map[string]time.Time, map[string][]time.Time) {
	reserve, before := map[string]time.Time{}, map[string][]time.Time{}
	for d := date(in.Last); d.After(date(in.Freeze)); d = d.AddDate(0, 0, -1) {
		if slices.Contains(in.Exclude, d.Format(DateLayout)) {
			continue
		}
		dt := Weekday
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			dt = Weekend
		}
		if _, ok := reserve[dt]; !ok {
			reserve[dt] = d
			continue
		}
		before[dt] = append([]time.Time{d}, before[dt]...)
	}
	return reserve, before
}

func mod(key string, n int) int {
	sum := sha256.Sum256([]byte(key))
	return int(new(big.Int).Mod(new(big.Int).SetBytes(sum[:]), big.NewInt(int64(n))).Int64())
}

// checkSchedule verifies every schedule rule against an independent
// recomputation from in.
func checkSchedule(t *testing.T, in ScheduleInput, s *Schedule) {
	t.Helper()
	reserve, before := pools(in)
	seed := in.MasterSeed
	if s.Counter > 0 {
		seed += strconv.Itoa(s.Counter)
	}
	n := MeasuredPerLabel * len(Labels)
	identity := make([]int, n)
	for i := range identity {
		identity[i] = i
	}
	perms := map[string]string{}
	checkSlot := func(x Session) {
		d := date(x.Date)
		if !x.Start.Equal(d.Add(20*time.Hour)) || x.Start.Location() != time.UTC {
			t.Errorf("%s: start %v, want %s 20:00 UTC", x.Label, x.Start, x.Date)
		}
		if slices.Contains(in.Exclude, x.Date) {
			t.Errorf("%s: excluded date %s", x.Label, x.Date)
		}
		if !d.After(date(in.Freeze)) || d.After(date(in.Last)) {
			t.Errorf("%s: %s outside (%s, %s]", x.Label, x.Date, in.Freeze, in.Last)
		}
		sorted := slices.Clone(x.Permutation)
		slices.Sort(sorted)
		if !slices.Equal(sorted, identity) {
			t.Errorf("%s: permutation is not of 0..%d", x.Label, n-1)
		}
		key := fmt.Sprint(x.Permutation)
		if prev, ok := perms[key]; ok {
			t.Errorf("%s: same permutation as %s", x.Label, prev)
		}
		perms[key] = x.Label
	}

	if len(s.Reserves) != 2 {
		t.Fatalf("%d reserves, want 2", len(s.Reserves))
	}
	for i, dt := range []string{Weekday, Weekend} {
		r := s.Reserves[i]
		checkSlot(r)
		if r.Label != "reserve-"+dt || r.DayType != dt || r.Date != reserve[dt].Format(DateLayout) {
			t.Errorf("reserve %d: %s %s %s, want reserve-%s on %s", i, r.Label, r.DayType, r.Date, dt, reserve[dt].Format(DateLayout))
		}
		if len(r.Order) != 0 {
			t.Errorf("%s: order %v on a reserve", r.Label, r.Order)
		}
	}

	if len(s.Sessions) != 6 {
		t.Fatalf("%d sessions, want 6", len(s.Sessions))
	}
	dates, first := map[string]bool{}, map[string]int{}
	labels := map[string]bool{}
	for i, x := range s.Sessions {
		checkSlot(x)
		d := date(x.Date)
		if i > 0 && !d.After(date(s.Sessions[i-1].Date)) {
			t.Errorf("%s: %s not after the previous session", x.Label, x.Date)
		}
		if dates[x.Date] {
			t.Errorf("%s: date %s used twice", x.Label, x.Date)
		}
		dates[x.Date] = true
		labels[x.Label] = true
		dt := strings.SplitN(x.Label, "-", 2)[0]
		if x.DayType != dt || !slices.Contains(before[dt], d) {
			t.Errorf("%s: %s (%s) not a %s date before the reserve %s", x.Label, x.Date, d.Weekday(), dt, reserve[dt].Format(DateLayout))
		}
		pool := before[dt]
		if want := pool[mod(seed+x.Label, len(pool))]; !d.Equal(want) {
			t.Errorf("%s: %s, SHA-256 draw gives %s", x.Label, x.Date, want.Format(DateLayout))
		}
		if len(x.Order) != 2 || x.Order[0] == x.Order[1] || !slices.Contains(endpoints[:], x.Order[0]) || !slices.Contains(endpoints[:], x.Order[1]) {
			t.Errorf("%s: order %v", x.Label, x.Order)
			continue
		}
		first[x.Order[0]]++
	}
	if len(labels) != 6 {
		t.Errorf("labels %v", labels)
	}
	if first[endpoints[0]] != 3 || first[endpoints[1]] != 3 {
		t.Errorf("first in order: %v, want three each", first)
	}
}

func TestScheduleIsDeterministic(t *testing.T) {
	in := october()
	a, b := schedule(t, in), schedule(t, in)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("two derivations from the same input differ")
	}
	checkSchedule(t, in, a)

	want := []string{
		"weekday-2 2026-10-13 baseten,groq",
		"weekend-3 2026-10-17 baseten,groq",
		"weekend-1 2026-10-18 groq,baseten",
		"weekday-1 2026-10-19 groq,baseten",
		"weekday-3 2026-10-22 baseten,groq",
		"weekend-2 2026-10-24 groq,baseten",
		"reserve-weekday 2026-10-30 ",
		"reserve-weekend 2026-10-31 ",
	}
	var got []string
	for _, x := range append(slices.Clone(a.Sessions), a.Reserves...) {
		got = append(got, fmt.Sprintf("%s %s %s", x.Label, x.Date, strings.Join(x.Order, ",")))
	}
	if !slices.Equal(got, want) || a.Counter != 0 {
		t.Errorf("counter %d, schedule\n%s\nwant counter 0,\n%s", a.Counter, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	j, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sha(j), "23f47e8361dac1d3ac4c165b73244fe6f39b11e6ce35caa2f3c7b0aa5d7ca329"; got != want {
		t.Errorf("schedule JSON sha256 %s, want %s", got, want)
	}

	in.MasterSeed += "x"
	if c := schedule(t, in); reflect.DeepEqual(a.Sessions, c.Sessions) {
		t.Error("a different master seed gave the same sessions")
	}
}

func TestScheduleRules(t *testing.T) {
	inputs := []ScheduleInput{
		october(),
		{Freeze: "2026-10-16", Last: "2026-10-31", Endpoints: endpoints},
		{Freeze: "2026-10-18", Last: "2026-11-03", Exclude: away(), Endpoints: endpoints},
		{Freeze: "2026-10-25", Last: "2026-11-22", Exclude: away(), Endpoints: endpoints},
	}
	recounted := 0
	for _, in := range inputs {
		for i := 0; i < 100; i++ {
			in.MasterSeed = fmt.Sprintf("seed-%d", i)
			s := schedule(t, in)
			checkSchedule(t, in, s)
			if s.Counter > 0 {
				recounted++
			}
			_, before := pools(in)
			for c := 0; c < s.Counter; c++ {
				seed := in.MasterSeed
				if c > 0 {
					seed += strconv.Itoa(c)
				}
				if _, ok := derive(seed, before); ok {
					t.Errorf("%s: counter %d gives distinct dates, schedule took %d", in.MasterSeed, c, s.Counter)
				}
			}
			if t.Failed() {
				t.Fatalf("input %+v", in)
			}
		}
	}
	if recounted == 0 {
		t.Error("no derivation needed the counter")
	}
}

func TestScheduleReserves(t *testing.T) {
	for _, c := range []struct {
		name                       string
		freeze, last               string
		exclude                    []string
		weekdayReserve, weekendRes string
	}{
		{"october", "2026-10-11", "2026-10-31", nil, "2026-10-30", "2026-10-31"},
		{"last a sunday", "2026-10-11", "2026-10-25", nil, "2026-10-23", "2026-10-25"},
		{"last dates excluded", "2026-10-11", "2026-10-31", []string{"2026-10-30", "2026-10-31"}, "2026-10-29", "2026-10-25"},
		{"before 4 november", "2026-10-18", "2026-11-03", away(), "2026-11-03", "2026-11-01"},
		{"across the absence", "2026-10-18", "2026-11-20", away(), "2026-11-20", "2026-11-01"},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := october()
			in.Freeze, in.Last, in.Exclude = c.freeze, c.last, c.exclude
			s := schedule(t, in)
			if s.Reserves[0].Date != c.weekdayReserve || s.Reserves[1].Date != c.weekendRes {
				t.Errorf("reserves %s, %s; want %s, %s", s.Reserves[0].Date, s.Reserves[1].Date, c.weekdayReserve, c.weekendRes)
			}
			for _, x := range s.Sessions {
				r := s.Reserves[0]
				if x.DayType == Weekend {
					r = s.Reserves[1]
				}
				if x.Date >= r.Date {
					t.Errorf("%s on %s, reserve %s", x.Label, x.Date, r.Date)
				}
				if x.Date > "2026-11-03" && x.Date < "2026-11-17" {
					t.Errorf("%s on %s, inside the absence", x.Label, x.Date)
				}
			}
		})
	}
}

func TestScheduleRefuses(t *testing.T) {
	for _, c := range []struct {
		name string
		mut  func(*ScheduleInput)
		want string
	}{
		{"empty seed", func(in *ScheduleInput) { in.MasterSeed = "" }, "empty master seed"},
		{"empty endpoint", func(in *ScheduleInput) { in.Endpoints[1] = "" }, "empty endpoint name"},
		{"same endpoint", func(in *ScheduleInput) { in.Endpoints[1] = in.Endpoints[0] }, `endpoint "groq" named twice`},
		{"bad freeze", func(in *ScheduleInput) { in.Freeze = "11 Oct 2026" }, `freeze "11 Oct 2026"`},
		{"bad exclude", func(in *ScheduleInput) { in.Exclude = []string{"2026-11-4"} }, `exclude "2026-11-4"`},
		{"last before freeze", func(in *ScheduleInput) { in.Last = "2026-10-01" }, "last 2026-10-01 is not after freeze 2026-10-11"},
		{"last on freeze", func(in *ScheduleInput) { in.Last = in.Freeze }, "is not after freeze"},
		{"three weekend dates", func(in *ScheduleInput) {
			in.Freeze, in.Last, in.Exclude = "2026-10-18", "2026-11-03", []string{"2026-11-01"}
		}, "3 eligible weekend dates after 2026-10-18 up to 2026-11-03, want 4"},
		{"weekdays excluded", func(in *ScheduleInput) {
			in.Freeze, in.Last = "2026-11-01", "2026-11-17"
			in.Exclude = away()
		}, "3 eligible weekday dates after 2026-11-01 up to 2026-11-17, want 4"},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := october()
			c.mut(&in)
			s, err := NewSchedule(in)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("schedule %v, error %v; want an error containing %q", s != nil, err, c.want)
			}
		})
	}
}
