package hostqual

import (
	"math"
	"strings"
	"testing"
)

// Each limit fails the observation on its own and names itself in the reason.
func TestQualifyAppliesTheAllocation(t *testing.T) {
	l := Allocation
	cases := []struct {
		name string
		o    Observation
		ok   bool
		want string
	}{
		{"inside", Observation{WakeP99Us: 900, WakeMaxUs: 9000, GCPauseP99Ms: 0.5}, true, ""},
		{"wake p99", Observation{WakeP99Us: 1001, WakeMaxUs: 9000, GCPauseP99Ms: 0.5}, false, "wake lateness p99"},
		{"wake max", Observation{WakeP99Us: 900, WakeMaxUs: 10001, GCPauseP99Ms: 0.5}, false, "wake lateness max"},
		{"gc pause", Observation{WakeP99Us: 900, WakeMaxUs: 9000, GCPauseP99Ms: 1.01}, false, "GC pause p99 bucket edge"},
		{"gc open", Observation{WakeP99Us: 900, WakeMaxUs: 9000, GCPauseP99Ms: 0.5, GCPauseOpen: true}, false, "open-ended"},
	}
	for _, c := range cases {
		ok, reason := c.o.Qualify(l)
		if ok != c.ok || !strings.Contains(reason, c.want) {
			t.Errorf("%s: got ok=%v reason=%q", c.name, ok, reason)
		}
	}
}

// The p99 bucket is read from the difference of two snapshots, and an
// open-ended top bucket is reported as such.
func TestPauseP99FromSnapshots(t *testing.T) {
	buckets := []float64{0, 0.0005, 0.001, 0.002, math.Inf(1)}
	before := hist{counts: []uint64{5, 5, 0, 0}, buckets: buckets}
	after := hist{counts: []uint64{95, 15, 0, 0}, buckets: buckets}
	n, p99, open := pauseP99(before, after)
	if n != 100 || p99 != 1.0 || open {
		t.Fatalf("got n=%d p99=%.3f open=%v, want 100 pauses with the p99 in the [0.5, 1) ms bucket", n, p99, open)
	}
	after = hist{counts: []uint64{5, 5, 0, 1}, buckets: buckets}
	n, p99, open = pauseP99(before, after)
	if n != 1 || p99 != 2.0 || !open {
		t.Fatalf("got n=%d p99=%.3f open=%v, want the open bucket reported by its lower edge", n, p99, open)
	}
	if n, _, _ := pauseP99(before, before); n != 0 {
		t.Fatalf("no pauses between equal snapshots, got %d", n)
	}
}

// A measurement observes every wake and reports non-negative lateness.
func TestMeasureObserves(t *testing.T) {
	if testing.Short() {
		t.Skip("takes seconds")
	}
	o := Measure()
	if o.Wakes != wakes || o.WakeMaxUs < 0 || o.WakeP99Us > o.WakeMaxUs || o.NumCPU < 1 {
		t.Fatalf("implausible observation: %+v", o)
	}
	t.Logf("host: %+v", o)
}
