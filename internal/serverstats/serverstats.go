// Package serverstats samples a replica's Prometheus text endpoint over a
// run and reduces the samples to the per-replica baseline-window mean the
// §10 G7 gate reads, and to the per-window change in each metric family
// the run keeps (§2). Samples carry wall-clock times; the reductions map
// them onto the run's monotonic phase boundaries through the run epoch.
package serverstats

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/percentes/percentes/internal/redact"
)

// Sample is one gauge reading from one endpoint, with every kept family
// read from the same page.
type Sample struct {
	Replica  string            `json:"replica"`
	At       time.Time         `json:"at"`
	Value    float64           `json:"value"`
	Families map[string]Family `json:"families,omitempty"`
}

// Family is one metric family at a sample, summed across label sets:
// Value for a gauge, counter or untyped family; Count and Sum for a
// histogram, whose cumulative bucket counts stay in memory for the window
// reduction.
type Family struct {
	Type    string  `json:"type"`
	Value   float64 `json:"value,omitempty"`
	Count   uint64  `json:"count,omitempty"`
	Sum     float64 `json:"sum,omitempty"`
	bounds  []float64
	buckets []uint64
	series  []series
}

// series is one label set of a family at a sample; the family's fields
// are the sum over its label sets.
type series struct {
	labels  string
	value   float64
	count   uint64
	sum     float64
	buckets []uint64
}

// labelKey names a label set by its pairs in name order, values quoted
// so no two label sets share a key.
func labelKey(ls []*dto.LabelPair) string {
	parts := make([]string, 0, len(ls))
	for _, l := range ls {
		parts = append(parts, l.GetName()+"="+strconv.Quote(l.GetValue()))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// Bucket is one histogram bucket: the cumulative count at or below LE.
type Bucket struct {
	LE    float64 `json:"le"`
	Count uint64  `json:"count"`
}

// Reduction is one family over a window: a gauge's or untyped family's
// mean over the samples inside it; a counter's increase, and a histogram's
// increase in count, sum and buckets, measured per label set from that
// set's last sample before the window (or its first inside it) to its
// last inside it, and summed over the sets. A set missing from a sample
// adds nothing for that sample; a set first seen inside the window adds
// its whole value. The +Inf bucket is left out, since its count is Count.
// Reset marks a label set that fell between two of its consecutive
// samples, which a replica restart does, or a histogram whose bucket
// layout changed; that pair contributes the later value, and after a
// layout change no buckets are reported, since Count and Sum span the
// whole window and the buckets would not.
type Reduction struct {
	Type     string   `json:"type"`
	Samples  int      `json:"samples"`
	Mean     float64  `json:"mean,omitempty"`
	Increase float64  `json:"increase,omitempty"`
	Count    uint64   `json:"count,omitempty"`
	Sum      float64  `json:"sum,omitempty"`
	Buckets  []Bucket `json:"buckets,omitempty"`
	Reset    bool     `json:"reset,omitempty"`
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

type families map[string]*dto.MetricFamily

func parse(page []byte, url string) (families, error) {
	fams, err := (&expfmt.TextParser{}).TextToMetricFamilies(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("serverstats: %s: metrics text did not parse", redact.URL(url))
	}
	return fams, nil
}

// gaugeValue returns the value of gauge in a parsed page, summed across
// label sets. An absent gauge is an error.
func gaugeValue(fams families, gauge, url string) (float64, error) {
	mf, ok := fams[gauge]
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
	if !finite(total) {
		return 0, fmt.Errorf("serverstats: %s: gauge %q sums to %v over its label sets", redact.URL(url), gauge, total)
	}
	return total, nil
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// familyValue returns one family summed across label sets. An absent
// family or an unsupported type is an error; so is a non-finite value, or
// histogram label sets with different bounds.
func familyValue(fams families, name, url string) (Family, error) {
	mf, ok := fams[name]
	if !ok {
		return Family{}, fmt.Errorf("serverstats: %s: family %q not exposed", redact.URL(url), name)
	}
	var f Family
	add := func(m *dto.Metric, v float64) error {
		if !finite(v) {
			return fmt.Errorf("serverstats: %s: family %q read %v", redact.URL(url), name, v)
		}
		f.Value += v
		f.series = append(f.series, series{labels: labelKey(m.GetLabel()), value: v})
		return nil
	}
	switch mf.GetType() {
	case dto.MetricType_GAUGE:
		f.Type = "gauge"
		for _, m := range mf.GetMetric() {
			if err := add(m, m.GetGauge().GetValue()); err != nil {
				return Family{}, err
			}
		}
	case dto.MetricType_UNTYPED:
		f.Type = "untyped"
		for _, m := range mf.GetMetric() {
			if err := add(m, m.GetUntyped().GetValue()); err != nil {
				return Family{}, err
			}
		}
	case dto.MetricType_COUNTER:
		f.Type = "counter"
		for _, m := range mf.GetMetric() {
			if err := add(m, m.GetCounter().GetValue()); err != nil {
				return Family{}, err
			}
		}
	case dto.MetricType_HISTOGRAM:
		f.Type = "histogram"
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			if !finite(h.GetSampleSum()) {
				return Family{}, fmt.Errorf("serverstats: %s: histogram %q sum read %v", redact.URL(url), name, h.GetSampleSum())
			}
			f.Count += h.GetSampleCount()
			f.Sum += h.GetSampleSum()
			bs := h.GetBucket()
			if f.bounds == nil {
				f.bounds = make([]float64, len(bs))
				f.buckets = make([]uint64, len(bs))
				for i, b := range bs {
					f.bounds[i] = b.GetUpperBound()
				}
			}
			if len(bs) != len(f.bounds) {
				return Family{}, fmt.Errorf("serverstats: %s: histogram %q: label sets with different buckets", redact.URL(url), name)
			}
			bk := make([]uint64, len(bs))
			for i, b := range bs {
				if b.GetUpperBound() != f.bounds[i] {
					return Family{}, fmt.Errorf("serverstats: %s: histogram %q: label sets with different buckets", redact.URL(url), name)
				}
				bk[i] = b.GetCumulativeCount()
				f.buckets[i] += bk[i]
			}
			f.series = append(f.series, series{labels: labelKey(m.GetLabel()), count: h.GetSampleCount(), sum: h.GetSampleSum(), buckets: bk})
		}
	default:
		return Family{}, fmt.Errorf("serverstats: %s: %q is a %s", redact.URL(url), name, mf.GetType())
	}
	if !finite(f.Value) || !finite(f.Sum) {
		return Family{}, fmt.Errorf("serverstats: %s: family %q sums to %v over its label sets", redact.URL(url), name, f.Value+f.Sum)
	}
	return f, nil
}

// extract returns the value of gauge in a text page, summed across label
// sets. An absent gauge is an error.
func extract(page []byte, gauge, url string) (float64, error) {
	fams, err := parse(page, url)
	if err != nil {
		return 0, err
	}
	return gaugeValue(fams, gauge, url)
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

// Preflight fetches url once and returns an error when gauge or a family
// in names does not read. A non-empty histogram must name a histogram
// family.
func Preflight(ctx context.Context, client *http.Client, url, gauge string, names []string, histogram string) error {
	page, err := fetch(ctx, client, url)
	if err != nil {
		return err
	}
	fams, err := parse(page, url)
	if err != nil {
		return err
	}
	if _, err := gaugeValue(fams, gauge, url); err != nil {
		return err
	}
	for _, n := range names {
		if _, err := familyValue(fams, n, url); err != nil {
			return err
		}
	}
	if histogram != "" {
		f, err := familyValue(fams, histogram, url)
		if err != nil {
			return err
		}
		if f.Type != "histogram" {
			return fmt.Errorf("serverstats: %s: %q is a %s where a histogram is needed", redact.URL(url), histogram, f.Type)
		}
	}
	return nil
}

// Sampler polls every endpoint on a fixed cadence from Start until Stop.
type Sampler struct {
	Client   *http.Client
	Gauge    string
	Interval time.Duration
	// Endpoints maps replica identity to its metrics URL.
	Endpoints map[string]string
	// Families are the metric families kept per sample beside the gauge.
	Families []string

	mu      sync.Mutex
	raw     []rawSample
	errs    []error
	famErrs []error
	cancel  context.CancelFunc
	wg      sync.WaitGroup
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
// with every fetch, parse or gauge error. A page whose gauge does not
// read is no sample; a kept family that does not read is counted by
// FamilyErrors and the sample stays without it.
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
		fams, err := parse(r.page, r.url)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		v, err := gaugeValue(fams, s.Gauge, r.url)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		smp := Sample{Replica: r.replica, At: r.at, Value: v}
		if len(s.Families) > 0 {
			smp.Families = make(map[string]Family, len(s.Families))
		}
		for _, name := range s.Families {
			f, err := familyValue(fams, name, r.url)
			if err != nil {
				s.famErrs = append(s.famErrs, err)
				continue
			}
			smp.Families[name] = f
		}
		samples = append(samples, smp)
	}
	return samples, errs
}

// FamilyErrors returns the kept-family reads that failed, after Stop.
func (s *Sampler) FamilyErrors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.famErrs...)
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

// accumulator sums one family's samples over a window: gauge values for
// the mean, and for a counter or histogram the increase between
// consecutive samples, measured per label set and summed. A label set is
// measured from its own last sample, before or inside the window: one
// absent from a sample adds nothing there, and one first seen inside the
// window adds its whole value. The bucket layout follows the sample a
// step measures from, so a step compares like with like.
type accumulator struct {
	typ     string
	started bool
	prev    map[string]series
	n       int
	sum     float64
	inc     float64
	count   uint64
	hsum    float64
	bounds  []float64
	buckets []uint64
	reset   bool
	relaid  bool
}

// seriesOf returns the family's label sets, or the family itself as one
// unlabelled set when none were kept.
func seriesOf(f *Family) []series {
	if len(f.series) > 0 {
		return f.series
	}
	return []series{{value: f.Value, count: f.Count, sum: f.Sum, buckets: f.buckets}}
}

// start sets the sample the next step measures from. Label sets seen
// earlier keep their last sample unless the bucket layout changed.
func (a *accumulator) start(f *Family) {
	if !a.started || !sameBounds(a.bounds, f.bounds) {
		a.prev = map[string]series{}
		a.bounds, a.buckets = f.bounds, make([]uint64, len(f.buckets))
	}
	a.started = true
	for _, s := range seriesOf(f) {
		a.prev[s.labels] = s
	}
}

// step adds each label set's increase since its previous sample. A value
// that fell, or a histogram whose bucket layout changed, is a reset, and
// the whole of the value is added.
func (a *accumulator) step(cur *Family) {
	if !a.started {
		a.start(cur)
		return
	}
	switch cur.Type {
	case "counter":
		for _, s := range seriesOf(cur) {
			p, seen := a.prev[s.labels]
			switch {
			case seen && s.value < p.value:
				a.reset = true
				a.inc += s.value
			case seen:
				a.inc += s.value - p.value
			default:
				a.inc += s.value
			}
			a.prev[s.labels] = s
		}
	case "histogram":
		sameLayout := sameBounds(cur.bounds, a.bounds)
		if !sameLayout {
			a.relaid, a.reset = true, true
		}
		for _, s := range seriesOf(cur) {
			p, seen := a.prev[s.labels]
			fell := seen && sameLayout && (s.count < p.count || s.sum < p.sum || bucketsFell(s.buckets, p.buckets))
			if fell {
				a.reset = true
			}
			if !seen || !sameLayout || fell {
				a.count += s.count
				a.hsum += s.sum
				if sameLayout {
					for i := range s.buckets {
						a.buckets[i] += s.buckets[i]
					}
				}
				continue
			}
			a.count += s.count - p.count
			a.hsum += s.sum - p.sum
			for i := range s.buckets {
				a.buckets[i] += s.buckets[i] - p.buckets[i]
			}
		}
		if !sameLayout {
			a.start(cur)
			return
		}
		for _, s := range seriesOf(cur) {
			a.prev[s.labels] = s
		}
	}
}

func sameBounds(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// bucketsFell reports a cumulative count lower at cur than at prev.
func bucketsFell(cur, prev []uint64) bool {
	for i := range cur {
		if cur[i] < prev[i] {
			return true
		}
	}
	return false
}

// ReduceWindow reduces every kept family over [startNs, endNs) from
// epoch, per replica. A replica with no sample inside the window is
// absent; a family absent from a sample is skipped for that sample.
func ReduceWindow(samples []Sample, epoch time.Time, startNs, endNs int64) map[string]map[string]Reduction {
	start := epoch.Add(time.Duration(startNs))
	end := epoch.Add(time.Duration(endNs))
	sorted := append([]Sample(nil), samples...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].At.Before(sorted[j].At) })

	accs := map[string]map[string]*accumulator{}
	get := func(replica, name string, f *Family) *accumulator {
		if accs[replica] == nil {
			accs[replica] = map[string]*accumulator{}
		}
		a := accs[replica][name]
		if a == nil {
			a = &accumulator{typ: f.Type}
			accs[replica][name] = a
		}
		return a
	}
	inWindow := map[string]bool{}
	for i := range sorted {
		smp := &sorted[i]
		if !smp.At.Before(end) {
			break
		}
		for name, f := range smp.Families {
			f := f
			a := get(smp.Replica, name, &f)
			if smp.At.Before(start) {
				a.start(&f)
				continue
			}
			inWindow[smp.Replica] = true
			a.step(&f)
			a.sum += f.Value
			a.n++
		}
	}
	out := map[string]map[string]Reduction{}
	for replica, fams := range accs {
		if !inWindow[replica] {
			continue
		}
		out[replica] = map[string]Reduction{}
		for name, a := range fams {
			if a.n == 0 {
				continue
			}
			r := Reduction{Type: a.typ, Samples: a.n, Reset: a.reset}
			switch a.typ {
			case "gauge", "untyped":
				r.Mean = a.sum / float64(a.n)
			case "counter":
				r.Increase = a.inc
			case "histogram":
				r.Count, r.Sum = a.count, a.hsum
				for i := range a.buckets {
					if a.relaid || math.IsInf(a.bounds[i], 1) {
						continue
					}
					r.Buckets = append(r.Buckets, Bucket{LE: a.bounds[i], Count: a.buckets[i]})
				}
			}
			out[replica][name] = r
		}
	}
	return out
}

// ForRun builds the sampler a run configures, or nil when
// target.metrics_urls is empty. Replicas are keyed r0, r1, ... in
// configuration order; gauge and interval are the run's §6 pins and
// families the §2 collector list.
func ForRun(urls []string, gauge string, families []string, interval time.Duration) *Sampler {
	if len(urls) == 0 {
		return nil
	}
	eps := make(map[string]string, len(urls))
	for i, u := range urls {
		eps[fmt.Sprintf("r%d", i)] = u
	}
	return &Sampler{Gauge: gauge, Interval: interval, Endpoints: eps, Families: families}
}

// Reduce stops the sampler and returns the per-replica baseline-window
// means with the count of failed scrapes over the run.
func (s *Sampler) Reduce(epoch time.Time, warmupEndNs, baselineEndNs int64) (map[string]Mean, int) {
	samples, errs := s.Stop()
	return BaselineMeans(samples, epoch, warmupEndNs, baselineEndNs), len(errs)
}
