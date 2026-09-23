// Package serverstats samples a replica's Prometheus text endpoint over a
// run and reduces the samples to the per-replica baseline-window mean the
// §10 G7 gate reads. Samples carry wall-clock times; the reduction maps
// them onto the run's monotonic phase boundaries through the run epoch.
package serverstats

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/percentes/percentes/internal/redact"
)

// Sample is one gauge reading from one endpoint.
type Sample struct {
	Replica string    `json:"replica"`
	At      time.Time `json:"at"`
	Value   float64   `json:"value"`
}

// Mean is a per-replica reduction over a window.
type Mean struct {
	Value   float64 `json:"value"`
	Samples int     `json:"samples"`
}

// fetch returns the raw text page. Parsing waits for Stop.
func fetch(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, redact.Wrap("serverstats: "+redact.URL(url), err, client.Timeout)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, redact.Wrap("serverstats: "+redact.URL(url), err, client.Timeout)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return nil, fmt.Errorf("serverstats: %s: status %d", redact.URL(url), resp.StatusCode)
	}
	page, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, redact.Wrap("serverstats: "+redact.URL(url), err, client.Timeout)
	}
	return page, nil
}

// extract returns the value of gauge in a text page, summed across label
// sets. An absent gauge is an error.
func extract(page []byte, gauge, url string) (float64, error) {
	families, err := (&expfmt.TextParser{}).TextToMetricFamilies(bytes.NewReader(page))
	if err != nil {
		return 0, fmt.Errorf("serverstats: %s: metrics text did not parse", redact.URL(url))
	}
	mf, ok := families[gauge]
	if !ok {
		return 0, fmt.Errorf("serverstats: %s: gauge %q not exposed", redact.URL(url), gauge)
	}
	var total float64
	for _, m := range mf.GetMetric() {
		var v float64
		switch mf.GetType() {
		case dto.MetricType_GAUGE:
			v = m.GetGauge().GetValue()
		case dto.MetricType_UNTYPED:
			v = m.GetUntyped().GetValue()
		default:
			return 0, fmt.Errorf("serverstats: %s: %q is a %s, not a gauge", redact.URL(url), gauge, mf.GetType())
		}
		// Per label set: a negative sample cancelling a positive one
		// sums to a plausible total.
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return 0, fmt.Errorf("serverstats: %s: gauge %q read %v; a waiting count is finite and non-negative", redact.URL(url), gauge, v)
		}
		total += v
	}
	return total, nil
}

// Scrape fetches url and returns the value of gauge, parsed at once. The
// Sampler parses at Stop.
func Scrape(ctx context.Context, client *http.Client, url, gauge string) (float64, error) {
	page, err := fetch(ctx, client, url)
	if err != nil {
		return 0, err
	}
	return extract(page, gauge, url)
}

// Sampler polls every endpoint on a fixed cadence from Start until Stop.
type Sampler struct {
	Client   *http.Client
	Gauge    string
	Interval time.Duration
	// Endpoints maps replica identity to its metrics URL.
	Endpoints map[string]string

	mu     sync.Mutex
	raw    []rawSample
	errs   []error
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// rawSample is an unparsed page; parsing waits for Stop.
type rawSample struct {
	replica string
	url     string
	at      time.Time
	page    []byte
}

// Start begins sampling, one goroutine per endpoint.
func (s *Sampler) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	if s.Client == nil {
		s.Client = &http.Client{Timeout: 5 * time.Second}
	}
	for replica, url := range s.Endpoints {
		s.wg.Add(1)
		go func(replica, url string) {
			defer s.wg.Done()
			t := time.NewTicker(s.Interval)
			defer t.Stop()
			for {
				s.take(ctx, replica, url)
				select {
				case <-t.C:
				case <-ctx.Done():
					return
				}
			}
		}(replica, url)
	}
}

func (s *Sampler) take(ctx context.Context, replica, url string) {
	at := time.Now()
	page, err := fetch(ctx, s.Client, url)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if ctx.Err() == nil {
			s.errs = append(s.errs, err)
		}
		return
	}
	s.raw = append(s.raw, rawSample{replica: replica, url: url, at: at, page: page})
}

// Stop ends sampling, parses every page taken, and returns the samples
// with every fetch or parse error.
func (s *Sampler) Stop() ([]Sample, []error) {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	samples := make([]Sample, 0, len(s.raw))
	errs := append([]error(nil), s.errs...)
	for _, r := range s.raw {
		v, err := extract(r.page, s.Gauge, r.url)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		samples = append(samples, Sample{Replica: r.replica, At: r.at, Value: v})
	}
	return samples, errs
}

// BaselineMeans reduces samples to a per-replica mean over the run's
// baseline window, given as monotonic nanosecond offsets from epoch. A
// replica with no sample inside the window is absent from the result.
func BaselineMeans(samples []Sample, epoch time.Time, warmupEndNs, baselineEndNs int64) map[string]Mean {
	start := epoch.Add(time.Duration(warmupEndNs))
	end := epoch.Add(time.Duration(baselineEndNs))
	sum := map[string]float64{}
	n := map[string]int{}
	for _, smp := range samples {
		if smp.At.Before(start) || !smp.At.Before(end) {
			continue
		}
		sum[smp.Replica] += smp.Value
		n[smp.Replica]++
	}
	out := make(map[string]Mean, len(n))
	for r, k := range n {
		out[r] = Mean{Value: sum[r] / float64(k), Samples: k}
	}
	return out
}

// ForRun builds the sampler a run configures, or nil when
// target.metrics_urls is empty. Replicas are keyed r0, r1, ... in
// configuration order; gauge and interval are the run's §6 pins.
func ForRun(urls []string, gauge string, interval time.Duration) *Sampler {
	if len(urls) == 0 {
		return nil
	}
	eps := make(map[string]string, len(urls))
	for i, u := range urls {
		eps[fmt.Sprintf("r%d", i)] = u
	}
	return &Sampler{Gauge: gauge, Interval: interval, Endpoints: eps}
}

// Reduce stops the sampler and returns the per-replica baseline-window
// means with the count of failed scrapes over the run.
func (s *Sampler) Reduce(epoch time.Time, warmupEndNs, baselineEndNs int64) (map[string]Mean, int) {
	samples, errs := s.Stop()
	return BaselineMeans(samples, epoch, warmupEndNs, baselineEndNs), len(errs)
}
