package loadgen

import (
	"math/big"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// writeWindow is one evaluation window of the target's limits: its
// width in nanoseconds, the window's length plus writeMarginNs when
// newWriteWindows builds it, the request writes it admits and the
// completion times of the writes inside it.
type writeWindow struct {
	width       int64
	capacity    int64
	completions []int64
}

// writeWindows caps a release run's request writes in every evaluation
// window of the target's limits. A write holds a permit from its first
// byte to its last, and counts in each window by its completion time; a
// write that some window does not admit waits for a permit, up to
// writePermitWaitNs.
type writeWindows struct {
	mu      sync.Mutex
	now     func() int64
	sleep   func(time.Duration)
	pending int64
	windows []writeWindow
}

// newWriteWindows admits, over each evaluation window of w seconds of
// the target's limits and over each window allowances keys, the least of
// ceil(top x (w + 0.1)) writes and allowances[w], the requests the limits
// admit in that window, counting each write for w plus writeMarginNs. A
// key of w plus client.http_timeout_s carries a limit that counts output
// at generation (admission.WindowAllowances).
func newWriteWindows(cfg *config.Config, top float64, allowances map[float64]int64, now func() int64) *writeWindows {
	var widths []float64
	for i := range cfg.Target.Limits {
		for _, w := range cfg.Target.Limits[i].WindowsS() {
			if !slices.Contains(widths, w) {
				widths = append(widths, w)
			}
		}
	}
	for w := range allowances {
		if !slices.Contains(widths, w) {
			widths = append(widths, w)
		}
	}
	slices.Sort(widths)
	g := &writeWindows{now: now, sleep: time.Sleep}
	for _, w := range widths {
		seconds, _ := new(big.Rat).SetString(strconv.FormatFloat(w, 'f', -1, 64))
		rate, _ := new(big.Rat).SetString(strconv.FormatFloat(top, 'f', -1, 64))
		slots := new(big.Rat).Mul(rate, new(big.Rat).Add(seconds, big.NewRat(1, 10)))
		n := new(big.Int).Quo(slots.Num(), slots.Denom())
		if !slots.IsInt() {
			n.Add(n, big.NewInt(1))
		}
		capacity := n.Int64()
		if a, ok := allowances[w]; ok && a < capacity {
			capacity = a
		}
		g.windows = append(g.windows, writeWindow{width: int64(time.Duration(w*float64(time.Second))) + writeMarginNs, capacity: capacity})
	}
	return g
}

// writeMarginNs lengthens every window a write counts in, so a provider
// timing arrivals over the window's length counts at most its capacity
// while two requests' network delays differ by less than the margin.
const writeMarginNs = int64(50 * time.Millisecond)

// writePermitWaitNs is the longest a release write waits for a permit,
// counted from its first attempt.
const writePermitWaitNs = int64(config.PinnedSendSkewMaxMs) * int64(time.Millisecond)

// writeWaitPollNs is how often a write waits again on a window that
// writes still in progress fill.
const writeWaitPollNs = int64(time.Millisecond)

// reserve takes a permit for o's write, once. A write that some window
// does not admit waits until every window admits it, up to o.wait after
// its first attempt; it reports false, and keeps refusing o, when no
// permit came by then.
func (g *writeWindows) reserve(o *WriteObservation) bool {
	if g == nil {
		return true
	}
	for {
		wait, ok := g.tryReserve(o)
		if ok || wait == 0 {
			return ok
		}
		g.sleep(time.Duration(wait))
	}
}

// tryReserve takes a permit for o's write if every window admits it now.
// Otherwise it reports the nanoseconds to wait before trying again, or
// 0, refusing o, when o's wait has ended or would end first.
func (g *writeWindows) tryReserve(o *WriteObservation) (wait int64, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if o.held {
		return 0, true
	}
	if o.denied.Load() {
		return 0, false
	}
	now := g.now()
	if !o.attempted {
		o.attempted, o.latest = true, now+o.wait
	} else if now > o.latest {
		o.denied.Store(true)
		return 0, false
	}
	var poll bool
	for i := range g.windows {
		w := &g.windows[i]
		first := 0
		for first < len(w.completions) && w.completions[first] <= now-w.width {
			first++
		}
		w.completions = w.completions[first:]
		n := int64(len(w.completions))
		if g.pending+n < w.capacity {
			continue
		}
		// The completion whose leaving frees a permit, or none while
		// writes in progress alone fill the window.
		if k := n - w.capacity + g.pending; k < n {
			wait = max(wait, w.completions[k]+w.width-now)
		} else {
			poll = true
		}
	}
	switch {
	case wait == 0 && !poll:
		o.held = true
		g.pending++
		return 0, true
	case now >= o.latest || now+wait > o.latest:
		o.denied.Store(true)
		return 0, false
	case poll:
		wait = max(wait, writeWaitPollNs)
	}
	return min(wait, o.latest-now), false
}

// finish releases o's permit, counting the write as completed now.
func (g *writeWindows) finish(o *WriteObservation) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !o.held {
		return
	}
	o.held = false
	g.pending--
	now := g.now()
	for i := range g.windows {
		w := &g.windows[i]
		w.completions = append(w.completions, now)
	}
}
