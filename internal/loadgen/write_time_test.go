package loadgen

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// A request whose bytes never reach the socket records no write time,
// over plain TCP and over TLS.
func TestSocketWriteTime(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		var hits atomic.Int64
		h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits.Add(1); w.WriteHeader(http.StatusTooManyRequests) })
		var srv *httptest.Server
		roots := x509.NewCertPool()
		if useTLS {
			srv = httptest.NewTLSServer(h)
			roots.AddCert(srv.Certificate())
		} else {
			srv = httptest.NewServer(h)
		}
		g := captureGen(srv)
		tr := NewTransport(2, roots)
		g.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
			expired := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { _ = info.Conn.SetWriteDeadline(time.Now().Add(-time.Second)) }}
			return tr.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), expired)))
		})
		r := new(Request)
		g.execute(r)
		srv.Close()
		tr.CloseIdleConnections()
		if hits.Load() != 0 || r.WroteRequestNs != 0 {
			t.Errorf("TLS %t: server hits %d, write time %d after a failed socket write", useTLS, hits.Load(), r.WroteRequestNs)
		}
	}
}

// The write time is the socket's acceptance of the body's last byte,
// however the headers and body are split across writes.
func TestObservedWriteCompletesWithBody(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &observedConn{Conn: a}
	var tick atomic.Int64
	tick.Store(5)
	o := WatchWrite(c, 3, tick.Load)
	for _, part := range []string{"POST / HTTP/1.1\r\nContent-length: 3\r", "\n\r", "\nab", "c"} {
		done := make(chan error, 1)
		go func(p string) { _, err := c.Write([]byte(p)); done <- err }(part)
		if n := o.Nanoseconds(); n != 0 {
			t.Fatalf("blocked or partial write recorded at %d", n)
		}
		if part == "c" {
			tick.Store(27)
		}
		if _, err := io.CopyN(io.Discard, b, int64(len(part))); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if got := o.Nanoseconds(); got != 27 {
		t.Fatalf("complete write timestamp %d, want 27", got)
	}
}

// Each window admits the least of the top rate over the window plus
// 0.1 s, rounded up, and the limits' allowance, counting each write for
// the window plus 50 ms.
func TestWriteWindowsCapacity(t *testing.T) {
	cfg := &config.Config{}
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 10000, EvaluationWindowsS: []float64{1, 60}}}
	now := func() int64 { return 0 }
	got := map[int64]int64{}
	for _, w := range newWriteWindows(cfg, 150.909, nil, now).windows {
		got[w.width-writeMarginNs] = w.capacity
	}
	if got[int64(time.Second)] != 166 || got[int64(time.Minute)] != 9070 || len(got) != 2 || writeMarginNs != int64(50*time.Millisecond) {
		t.Fatalf("capacities by window %v, want 166 over 1 s and 9070 over 60 s, each counted 50 ms longer", got)
	}
	got = map[int64]int64{}
	for _, w := range newWriteWindows(cfg, 150.909, map[float64]int64{1: 160, 10: 40}, now).windows {
		got[w.width-writeMarginNs] = w.capacity
	}
	if got[int64(time.Second)] != 160 || got[10*int64(time.Second)] != 40 || got[int64(time.Minute)] != 9070 {
		t.Fatalf("capacities by window %v, want 160 over 1 s, 40 over 10 s and 9070 over 60 s", got)
	}
}

// A write a full window holds is released 50 ms after the oldest
// completion leaves the window's length, and one that would wait past
// 50 ms from its first attempt is refused.
func TestWriteMarginPastWindow(t *testing.T) {
	cfg := &config.Config{}
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 1e6, EvaluationWindowsS: []float64{1}}}
	g := newWriteWindows(cfg, 1, map[float64]int64{1: 1}, nil)
	now, slept := fakeWindows(g)
	first := waiting()
	if !g.reserve(first) {
		t.Fatal("initial permit unavailable")
	}
	g.finish(first)
	*now = int64(time.Second)
	held := waiting()
	if !g.reserve(held) || *now != int64(time.Second)+writeMarginNs || !slices.Equal(*slept, []time.Duration{50 * time.Millisecond}) {
		t.Fatalf("a write at the window's end: admitted at %d ns after sleeping %v", *now, *slept)
	}
	g.finish(held)
	*now += int64(time.Second) - 10*int64(time.Millisecond)
	*slept = nil
	if late := waiting(); g.reserve(late) || !late.Denied() || len(*slept) != 0 {
		t.Fatalf("a write 60 ms from a permit: denied %t after sleeping %v", late.Denied(), *slept)
	}
}

// A permit is charged from the first byte until one window after the
// last.
func TestWritePermitHeldUntilLastWrite(t *testing.T) {
	now := int64(0)
	g := &writeWindows{now: func() int64 { return now }, windows: []writeWindow{{width: int64(time.Second), capacity: 1}}}
	a := new(WriteObservation)
	if !g.reserve(a) {
		t.Fatal("initial permit unavailable")
	}
	now = 2 * int64(time.Second)
	if g.reserve(new(WriteObservation)) {
		t.Fatal("pending write expired")
	}
	g.finish(a)
	now += int64(time.Second) - 1
	if g.reserve(new(WriteObservation)) {
		t.Fatal("completed write expired early")
	}
	now++
	if !g.reserve(new(WriteObservation)) {
		t.Fatal("completed write did not expire")
	}
}

// Closing a connection mid-request releases its permit.
func TestCloseIncompleteWrite(t *testing.T) {
	var now int64 = 1
	limits := &writeWindows{now: func() int64 { return now }, windows: []writeWindow{{width: int64(time.Second), capacity: 1}}}
	a, b := net.Pipe()
	obs := &observedConn{Conn: a}
	watchSocket(obs, 3, limits.now, limits)
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, b); done <- err; _ = b.Close() }()
	if _, err := obs.Write([]byte("POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := obs.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	now += 2 * int64(time.Second)
	if !limits.reserve(new(WriteObservation)) {
		t.Fatalf("two windows after closing a partial request: pending %d", limits.pending)
	}
}

// A write over the window's capacity is refused before any byte reaches
// the socket.
func TestWriteRefusedWritesNothing(t *testing.T) {
	limits := &writeWindows{now: func() int64 { return 1 }, windows: []writeWindow{{width: int64(time.Second), capacity: 2}}}
	a, b := net.Pipe()
	defer a.Close()
	c := &observedConn{Conn: a}
	var received atomic.Int64
	go func() { n, _ := io.Copy(io.Discard, b); received.Store(n) }()
	const req = "POST / HTTP/1.1\r\nContent-Length: 1\r\n\r\nx"
	for i := 0; i < 3; i++ {
		o := watchSocket(c, 1, limits.now, limits)
		n, err := c.Write([]byte(req))
		switch {
		case i < 2 && (err != nil || o.Nanoseconds() == 0):
			t.Fatalf("write %d: %v, write time %d", i+1, err, o.Nanoseconds())
		case i == 2 && (!errors.Is(err, errWriteWindow) || n != 0 || !o.Denied() || o.Nanoseconds() != 0):
			t.Fatalf("write 3: %d bytes, %v, denied %t", n, err, o.Denied())
		}
	}
	a.Close()
	time.Sleep(50 * time.Millisecond)
	if got := received.Load(); got != 2*int64(len(req)) {
		t.Fatalf("peer received %d bytes, want %d", got, 2*len(req))
	}
}

// A release run's writes stay within the window allowance its hooks
// carry, and a refused write fails the step's send-skew gate.
func TestReleaseWriteAllowance(t *testing.T) {
	var mu sync.Mutex
	var arrivals []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
	}))
	defer srv.Close()
	cfg := ladderRunCfg(srv.URL, 1)
	cfg.Release.BindingRPS = 20
	cfg.Release.Ladder.WarmupS = 0
	cfg.Release.Ladder.StepS = 2
	cfg.Release.Ladder.DrainS = 0.5
	res, err := Run(context.Background(), cfg, &Hooks{WriteAllowances: map[float64]int64{1: 10}})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	peak := peakIn(arrivals, time.Second)
	mu.Unlock()
	g := res.Ladder.Steps[0].Gates
	if peak > 10 || g.WritesDenied == 0 || g.SendSkewPass || g.Pass {
		t.Fatalf("peak arrivals in one second %d, want at most 10; gate %+v", peak, *g)
	}
}

// A pacer that wakes late leaves the overdue release requests unsent:
// the server never sees more in one second than the window admits, and
// the step fails its client-validity gate.
func TestLateReleaseWrites(t *testing.T) {
	var mu sync.Mutex
	var arrivals []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
	}))
	defer srv.Close()
	cfg := ladderRunCfg(srv.URL, 1)
	cfg.Release.BindingRPS = 150.909
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 10000, EvaluationWindowsS: []float64{1, 60}}}
	cfg.Release.Ladder.WarmupS = 0
	cfg.Release.Ladder.StepS = 2
	cfg.Release.Ladder.DrainS = 0.1
	res, err := Run(context.Background(), cfg, &Hooks{OnEpoch: func(time.Time) { time.Sleep(500 * time.Millisecond) }})
	if err != nil {
		t.Fatal(err)
	}
	var writes []int64
	for _, r := range res.Requests {
		if r.WroteRequestNs > 0 {
			writes = append(writes, r.WroteRequestNs)
		}
	}
	slices.Sort(writes)
	mu.Lock()
	peak := peakIn(arrivals, time.Second)
	mu.Unlock()
	if p := peakNs(writes, int64(time.Second)); p > 166 || peak > 166 {
		t.Fatalf("peak in one second: %d writes, %d arrivals; the window admits 166", p, peak)
	}
	if g := res.Ladder.Steps[0].Gates; g.Undispatched == 0 || g.Pass {
		t.Fatalf("late requests dispatched: gate %+v", *g)
	}
}

// peakIn is the most times in any half-open span of length d.
func peakIn(times []time.Time, d time.Duration) int {
	ns := make([]int64, len(times))
	for i, at := range times {
		ns[i] = at.UnixNano()
	}
	slices.Sort(ns)
	return peakNs(ns, d.Nanoseconds())
}

func peakNs(sorted []int64, d int64) int {
	peak, left := 0, 0
	for right, at := range sorted {
		for sorted[left] <= at-d {
			left++
		}
		peak = max(peak, right-left+1)
	}
	return peak
}

// fakeWindows is g on a clock that sleeping advances.
func fakeWindows(g *writeWindows) (clock *int64, slept *[]time.Duration) {
	var now int64
	var s []time.Duration
	g.now = func() int64 { return now }
	g.sleep = func(d time.Duration) { s = append(s, d); now += int64(d) }
	return &now, &s
}

// waiting is an observation that may wait the release wait for a permit.
func waiting() *WriteObservation { return &WriteObservation{wait: writePermitWaitNs} }

// A write a full window holds waits for the oldest completion to leave
// it, when that is within 50 ms of its first attempt; a write that would
// wait longer is refused at once, and one held by writes in progress is
// refused 50 ms after its first attempt.
func TestWritePermitWaitsForWindow(t *testing.T) {
	g := &writeWindows{windows: []writeWindow{{width: int64(time.Second), capacity: 1}}}
	now, slept := fakeWindows(g)
	first := waiting()
	if !g.reserve(first) {
		t.Fatal("initial permit unavailable")
	}
	g.finish(first)
	*now = 980 * int64(time.Millisecond)
	second := waiting()
	if !g.reserve(second) || *now != int64(time.Second) || !slices.Equal(*slept, []time.Duration{20 * time.Millisecond}) {
		t.Fatalf("second write at %d ns after sleeping %v, held %t", *now, *slept, second.held)
	}
	g.finish(second)
	*now += 940 * int64(time.Millisecond)
	*slept = nil
	if third := waiting(); g.reserve(third) || !third.Denied() || len(*slept) != 0 {
		t.Fatalf("a write 60 ms from a permit: denied %t after sleeping %v", third.Denied(), *slept)
	}
	if fourth := new(WriteObservation); g.reserve(fourth) || len(*slept) != 0 {
		t.Fatalf("a write with no wait slept %v", *slept)
	}

	g = &writeWindows{windows: []writeWindow{{width: int64(time.Second), capacity: 1}}}
	now, slept = fakeWindows(g)
	if !g.reserve(waiting()) {
		t.Fatal("initial permit unavailable")
	}
	start := *now
	held := waiting()
	if g.reserve(held) || !held.Denied() || *now-start != writePermitWaitNs || len(*slept) != 50 {
		t.Fatalf("a write behind one in progress: denied %t after %d ns in %d sleeps", held.Denied(), *now-start, len(*slept))
	}
}

// The wait is counted from the write's first attempt, whatever the
// request's intended time.
func TestWritePermitWaitFromFirstAttempt(t *testing.T) {
	g := &writeWindows{windows: []writeWindow{{width: int64(time.Second), capacity: 1}}}
	now, slept := fakeWindows(g)
	a := waiting()
	if !g.reserve(a) {
		t.Fatal("initial permit unavailable")
	}
	g.finish(a)
	*now = 5*int64(time.Second) + 960*int64(time.Millisecond)
	b := waiting()
	if !g.reserve(b) || b.latest != 6010*int64(time.Millisecond) || !slices.Equal(*slept, nil) {
		t.Fatalf("an on-time write: held %t, latest %d, slept %v", b.held, b.latest, *slept)
	}
	g.finish(b)
	*now += 960 * int64(time.Millisecond)
	c := waiting()
	if !g.reserve(c) || c.latest != *now-40*int64(time.Millisecond)+writePermitWaitNs {
		t.Fatalf("a write 40 ms from a permit: held %t, latest %d at %d", c.held, c.latest, *now)
	}
}

// A write whose wait for a full window's permit ends 49 or 50 ms after its
// first attempt is admitted, and one that wakes 51 or 70 ms after it is
// refused. Trying the write again keeps a permit it holds and keeps
// refusing a write once refused.
func TestWritePermitRefusedAfterWaitOversleeps(t *testing.T) {
	for _, c := range []struct {
		slept    time.Duration
		admitted bool
	}{
		{49 * time.Millisecond, true},
		{50 * time.Millisecond, true},
		{51 * time.Millisecond, false},
		{70 * time.Millisecond, false},
	} {
		now := int64(980 * time.Millisecond)
		g := &writeWindows{windows: []writeWindow{{width: int64(time.Second), capacity: 1, completions: []int64{0}}}}
		g.now = func() int64 { return now }
		g.sleep = func(time.Duration) { now += int64(c.slept) }
		o := waiting()
		if got := g.reserve(o); got != c.admitted || o.Denied() == c.admitted {
			t.Fatalf("woken %v after the first attempt: admitted %t, denied %t", c.slept, got, o.Denied())
		}
		now += int64(time.Second)
		if again := g.reserve(o); again != c.admitted || g.pending != map[bool]int64{true: 1}[c.admitted] {
			t.Fatalf("woken %v, tried again: admitted %t with %d pending", c.slept, again, g.pending)
		}
	}
}

// spanMost is the most of the ascending times ns inside any window of
// width ns, the limiter's own count: those after t-width up to t.
func spanMost(ns []int64, width int64) int {
	most, lo := 0, 0
	for hi := range ns {
		for ns[lo] <= ns[hi]-width {
			lo++
		}
		most = max(most, hi-lo+1)
	}
	return most
}

// Writes offered every 10 ms, and writes at random setup delays up to
// 1 s, never put more writes in a window than its capacity, with the
// limiter waiting up to 50 ms for a permit.
func TestWritePermitWaitInvariant(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, c := range []struct {
		name   string
		top    float64
		allow  map[float64]int64
		limits []float64
	}{
		{"azure-R", 3.636, map[float64]int64{1: 4, 10: 41, 60: 250}, []float64{1, 10, 60}},
		{"anthropic-R", 150.909, nil, []float64{1, 60}},
		{"tight", 2, map[float64]int64{1: 2}, []float64{1}},
	} {
		cfg := &config.Config{}
		cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 1e6, EvaluationWindowsS: c.limits}}
		for trial := 0; trial < 20; trial++ {
			g := newWriteWindows(cfg, c.top, c.allow, nil)
			now, _ := fakeWindows(g)
			n := int(c.top * 120)
			attempts := make([]int64, n)
			for i := range attempts {
				attempts[i] = int64(float64(i)/c.top*1e9) + rng.Int63n(int64(time.Second))
			}
			slices.Sort(attempts)
			var done []int64
			for _, at := range attempts {
				*now = max(*now, at)
				o := waiting()
				if g.reserve(o) {
					*now += rng.Int63n(int64(2 * time.Millisecond))
					g.finish(o)
					done = append(done, *now)
				}
			}
			for _, w := range g.windows {
				if got := spanMost(done, w.width); int64(got) > w.capacity {
					t.Fatalf("%s trial %d: %d writes in a %v window of capacity %d", c.name, trial, got, time.Duration(w.width), w.capacity)
				}
			}
		}
	}
}

// azure-S's token limit, counted at generation, gives the limiter a
// 150 s window of its 60 s allowance: at 0.972 and 0.746 rps no 150 s
// span holds more than 146 or 112 writes, however fast writes are
// offered, and a paced schedule with send skew inside G2's bounds (p99
// 5 ms, max 50 ms) and writes of up to 2 ms is never refused.
func TestWriteWindowsGenerationSpan(t *testing.T) {
	cfg := &config.Config{}
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 250, EvaluationWindowsS: []float64{1, 10, 60}}}
	span := int64(150 * time.Second)
	for _, c := range []struct {
		top   float64
		allow int64
	}{{0.972, 146}, {0.746, 112}} {
		allowances := map[float64]int64{1: 4, 10: 41, 60: c.allow, 150: c.allow}
		g := newWriteWindows(cfg, c.top, allowances, nil)
		capacity := map[int64]int64{}
		for _, w := range g.windows {
			capacity[w.width-writeMarginNs] = w.capacity
		}
		if capacity[span] != c.allow || len(capacity) != 4 {
			t.Fatalf("%g rps: capacities %v, want %d over 150 s", c.top, capacity, c.allow)
		}

		now, _ := fakeWindows(g)
		var done []int64
		for ; *now < int64(600*time.Second); *now += int64(10 * time.Millisecond) {
			if o := new(WriteObservation); g.reserve(o) {
				g.finish(o)
				done = append(done, *now)
			}
		}
		if got := spanMost(done, span); got > int(c.allow) {
			t.Errorf("%g rps saturated: %d writes in a 150 s span, allowance %d", c.top, got, c.allow)
		}

		rng := rand.New(rand.NewSource(2))
		g = newWriteWindows(cfg, c.top, allowances, nil)
		now, _ = fakeWindows(g)
		done, denied := nil, 0
		for i := 0; i < 20000; i++ {
			skew := rng.Int63n(int64(5 * time.Millisecond))
			if rng.Intn(200) == 0 {
				skew = rng.Int63n(int64(50 * time.Millisecond))
			}
			*now = max(*now, int64(float64(i)/c.top*1e9)+skew)
			o := waiting()
			if !g.reserve(o) {
				denied++
				continue
			}
			*now += rng.Int63n(int64(2 * time.Millisecond))
			g.finish(o)
			done = append(done, *now)
		}
		if got := spanMost(done, span); denied != 0 || got > int(c.allow) {
			t.Errorf("%g rps paced: %d refused, %d writes in a 150 s span", c.top, denied, got)
		}
	}
}
