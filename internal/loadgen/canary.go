package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

// The §2 loopback canary: one stream at a time against an in-process mock
// on the client host with fixed timing, read through execute, so observed
// minus configured timing bounds host-side receive delay. It says nothing
// about the network path.
const (
	CanaryTTFTMs = 20
	CanaryITLMs  = 10
	CanaryTokens = 32
	// Pause between one canary stream's end and the next one's dispatch.
	canaryPause = 100 * time.Millisecond
)

// CanaryStream is one canary stream as observed. Times are nanosecond
// offsets from the run epoch; TTFTUs and the gaps are observed values,
// against which the configured timing is the reference.
type CanaryStream struct {
	StartNs int64   `json:"start_ns"`
	DoneNs  int64   `json:"done_ns"`
	Outcome Outcome `json:"outcome"`
	Tokens  int     `json:"tokens"`
	TTFTUs  int64   `json:"ttft_us"`
	ITLsUs  []int64 `json:"-"`
}

// CanarySummary reduces the canary streams dispatched inside a window:
// observed minus configured timing, in microseconds, as order statistics
// over the completed streams' first tokens and pooled inter-token gaps.
type CanarySummary struct {
	TTFTMs    int `json:"ttft_ms"`
	ITLMs     int `json:"itl_ms"`
	Tokens    int `json:"tokens"`
	Streams   int `json:"streams"`
	Completed int `json:"completed"`

	TTFTDevP50Us int64 `json:"ttft_dev_p50_us"`
	TTFTDevMaxUs int64 `json:"ttft_dev_max_us"`
	ITLDevP50Us  int64 `json:"itl_dev_p50_us"`
	ITLDevP99Us  int64 `json:"itl_dev_p99_us"`
	ITLDevMaxUs  int64 `json:"itl_dev_max_us"`
}

// Canary drives the loopback streams from Start until Stop.
type Canary struct {
	srv    *mock.Server
	gen    *gen
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	streams []CanaryStream
}

// StartCanary starts the loopback mock and the stream loop, timed from
// epoch, and returns once the mock listens.
func StartCanary(ctx context.Context, epoch time.Time) (*Canary, error) {
	srv := mock.New(config.Mock{
		ListenAddr: "127.0.0.1:0",
		Seed:       1,
		TTFT:       config.LatencyDist{Distribution: config.DistributionFixed, FixedMs: CanaryTTFTMs},
		ITL:        config.LatencyDist{Distribution: config.DistributionFixed, FixedMs: CanaryITLMs},
	})
	if err := srv.Start(); err != nil {
		return nil, fmt.Errorf("canary: %w", err)
	}
	cfg := &config.Config{}
	cfg.Load.MaxTokens = CanaryTokens
	cfg.Target.BaseURL = "http://" + srv.Addr()
	c := &Canary{
		srv:  srv,
		gen:  &gen{cfg: cfg, client: &http.Client{Transport: &http.Transport{}}, epoch: epoch, model: "percentes-canary"},
		done: make(chan struct{}),
	}
	ctx, c.cancel = context.WithCancel(ctx)
	go c.loop(ctx)
	return c, nil
}

func (c *Canary) loop(ctx context.Context) {
	defer close(c.done)
	for i := int64(0); ctx.Err() == nil; i++ {
		r := Request{Index: i, IntendedNs: c.gen.now()}
		c.gen.execute(&r)
		s := CanaryStream{StartNs: r.DispatchNs, DoneNs: r.DoneNs, Outcome: r.Outcome, Tokens: r.Tokens, ITLsUs: r.ITLsUs}
		if r.FirstTokNs != 0 {
			s.TTFTUs = (r.FirstTokNs - r.DispatchNs) / 1000
		}
		c.mu.Lock()
		c.streams = append(c.streams, s)
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-time.After(canaryPause):
		}
	}
}

// Stop ends the loop after the stream in flight, closes the mock and
// returns every stream observed.
func (c *Canary) Stop() []CanaryStream {
	c.cancel()
	<-c.done
	c.srv.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CanaryStream(nil), c.streams...)
}

// SummarizeCanary reduces the streams dispatched in [startNs, endNs).
func SummarizeCanary(streams []CanaryStream, startNs, endNs int64) CanarySummary {
	sum := CanarySummary{TTFTMs: CanaryTTFTMs, ITLMs: CanaryITLMs, Tokens: CanaryTokens}
	var ttft, itl []int64
	for _, s := range streams {
		if s.StartNs < startNs || s.StartNs >= endNs {
			continue
		}
		sum.Streams++
		if s.Outcome != OutcomeCompleted {
			continue
		}
		sum.Completed++
		ttft = append(ttft, s.TTFTUs-CanaryTTFTMs*1000)
		for _, gap := range s.ITLsUs {
			itl = append(itl, gap-CanaryITLMs*1000)
		}
	}
	sort.Slice(ttft, func(a, b int) bool { return ttft[a] < ttft[b] })
	sort.Slice(itl, func(a, b int) bool { return itl[a] < itl[b] })
	if n := len(ttft); n > 0 {
		sum.TTFTDevP50Us, sum.TTFTDevMaxUs = orderStat(ttft, 50), ttft[n-1]
	}
	if n := len(itl); n > 0 {
		sum.ITLDevP50Us, sum.ITLDevP99Us, sum.ITLDevMaxUs = orderStat(itl, 50), orderStat(itl, 99), itl[n-1]
	}
	return sum
}

// orderStat returns the nearest-rank percentile of a sorted slice.
func orderStat(sorted []int64, pct int) int64 {
	i := (len(sorted)*pct + 99) / 100
	if i < 1 {
		i = 1
	}
	return sorted[i-1]
}
