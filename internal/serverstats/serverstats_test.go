package serverstats

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func gaugeServer(t *testing.T, value *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "# HELP vllm:num_requests_waiting Requests waiting.\n# TYPE vllm:num_requests_waiting gauge\nvllm:num_requests_waiting %d\n", value.Load())
		fmt.Fprintf(w, "# TYPE other_counter counter\nother_counter 7\n")
	}))
}

func TestScrapeReadsNamedGauge(t *testing.T) {
	var v atomic.Int64
	v.Store(3)
	srv := gaugeServer(t, &v)
	defer srv.Close()
	got, err := Scrape(context.Background(), srv.Client(), srv.URL, "vllm:num_requests_waiting")
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("got %v, want 3", got)
	}
}

func TestScrapeRejectsMissingGaugeAndCounter(t *testing.T) {
	var v atomic.Int64
	srv := gaugeServer(t, &v)
	defer srv.Close()
	if _, err := Scrape(context.Background(), srv.Client(), srv.URL, "no_such_gauge"); err == nil {
		t.Fatal("absent gauge must be an error")
	}
	if _, err := Scrape(context.Background(), srv.Client(), srv.URL, "other_counter"); err == nil {
		t.Fatal("a counter must not be read as a gauge")
	}
}

func TestScrapeSumsLabelSets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "# TYPE q gauge\nq{model=\"a\"} 1.5\nq{model=\"b\"} 2\n")
	}))
	defer srv.Close()
	got, err := Scrape(context.Background(), srv.Client(), srv.URL, "q")
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.5 {
		t.Fatalf("got %v, want 3.5", got)
	}
}

func TestScrapeRejectsNonFiniteAndNegative(t *testing.T) {
	for _, body := range []string{"# TYPE q gauge\nq NaN\n", "# TYPE q gauge\nq +Inf\n", "# TYPE q gauge\nq -Inf\n", "# TYPE q gauge\nq -1\n"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
		_, err := Scrape(context.Background(), srv.Client(), srv.URL, "q")
		srv.Close()
		if err == nil {
			t.Fatalf("%q must not read as a waiting count", body)
		}
	}
}

func TestSamplerCollectsPerReplica(t *testing.T) {
	var a, b atomic.Int64
	a.Store(2)
	b.Store(0)
	sa, sb := gaugeServer(t, &a), gaugeServer(t, &b)
	defer sa.Close()
	defer sb.Close()
	s := &Sampler{Gauge: "vllm:num_requests_waiting", Interval: 20 * time.Millisecond,
		Endpoints: map[string]string{"r0": sa.URL, "r1": sb.URL}}
	s.Start(context.Background())
	time.Sleep(120 * time.Millisecond)
	samples, errs := s.Stop()
	if len(errs) != 0 {
		t.Fatalf("scrape errors: %v", errs)
	}
	n := map[string]int{}
	for _, smp := range samples {
		n[smp.Replica]++
		if smp.Replica == "r0" && smp.Value != 2 {
			t.Fatalf("r0 value %v, want 2", smp.Value)
		}
	}
	if n["r0"] < 3 || n["r1"] < 3 {
		t.Fatalf("too few samples per replica: %v", n)
	}
}

func TestSamplerRecordsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	s := &Sampler{Gauge: "q", Interval: 20 * time.Millisecond, Endpoints: map[string]string{"r0": srv.URL}}
	s.Start(context.Background())
	time.Sleep(60 * time.Millisecond)
	samples, errs := s.Stop()
	if len(samples) != 0 || len(errs) == 0 {
		t.Fatalf("want no samples and some errors, got %d samples, %d errors", len(samples), len(errs))
	}
}

func TestBaselineMeansWindowsOnEpoch(t *testing.T) {
	epoch := time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)
	sec := func(s float64) time.Time { return epoch.Add(time.Duration(s * float64(time.Second))) }
	samples := []Sample{
		{Replica: "r0", At: sec(1), Value: 9},   // warm-up: excluded
		{Replica: "r0", At: sec(5), Value: 1},   // baseline
		{Replica: "r0", At: sec(6), Value: 3},   // baseline
		{Replica: "r0", At: sec(10), Value: 99}, // at baseline end: excluded (half-open)
		{Replica: "r1", At: sec(7), Value: 0.5}, // baseline
		{Replica: "r2", At: sec(12), Value: 4},  // fault window: excluded, r2 absent
	}
	got := BaselineMeans(samples, epoch, 5e9, 10e9)
	if len(got) != 2 {
		t.Fatalf("replicas in window: got %d (%v), want 2", len(got), got)
	}
	if r0 := got["r0"]; r0.Value != 2 || r0.Samples != 2 {
		t.Fatalf("r0 = %+v, want mean 2 over 2 samples", r0)
	}
	if r1 := got["r1"]; r1.Value != 0.5 || r1.Samples != 1 {
		t.Fatalf("r1 = %+v, want 0.5 over 1", r1)
	}
}

func TestForRunKeysReplicasInOrder(t *testing.T) {
	if ForRun(nil, "g", nil, time.Second) != nil {
		t.Fatal("no URLs must mean no sampler")
	}
	s := ForRun([]string{"http://a/metrics", "http://b/metrics"}, "vllm:num_requests_waiting", nil, time.Second)
	if s.Endpoints["r0"] != "http://a/metrics" || s.Endpoints["r1"] != "http://b/metrics" {
		t.Fatalf("endpoints keyed out of order: %v", s.Endpoints)
	}
	if s.Interval != time.Second || s.Gauge != "vllm:num_requests_waiting" {
		t.Fatalf("pins not applied: %+v", s)
	}
}

func TestReduceStopsAndWindows(t *testing.T) {
	var v atomic.Int64
	v.Store(2)
	srv := gaugeServer(t, &v)
	defer srv.Close()
	s := ForRun([]string{srv.URL}, "vllm:num_requests_waiting", nil, 20*time.Millisecond)
	epoch := time.Now()
	s.Start(context.Background())
	time.Sleep(150 * time.Millisecond)
	means, errs := s.Reduce(epoch, 0, int64(time.Hour))
	if errs != 0 || means["r0"].Samples < 3 || means["r0"].Value != 2 {
		t.Fatalf("reduce over the whole run: errs=%d means=%v", errs, means)
	}
}

func TestExtractRejectsNegativeLabelSetThatSumsPlausibly(t *testing.T) {
	page := []byte(`# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0",model_name="a"} -1
vllm:num_requests_waiting{engine="1",model_name="b"} 2
`)
	if _, err := extract(page, "vllm:num_requests_waiting", "http://x/metrics"); err == nil {
		t.Fatal("a negative label set summing to 1 must be rejected")
	}
	ok := []byte(`# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0",model_name="a"} 1
vllm:num_requests_waiting{engine="1",model_name="b"} 2
`)
	got, err := extract(ok, "vllm:num_requests_waiting", "http://x/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("got %v, want 3", got)
	}
}

const familiesPage = `# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="m",engine="0"} 1
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="m",engine="0"} 4
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{model_name="m",engine="0"} 500
# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{model_name="m",engine="0",le="0.1"} 2
vllm:time_to_first_token_seconds_bucket{model_name="m",engine="0",le="1"} 3
vllm:time_to_first_token_seconds_bucket{model_name="m",engine="0",le="+Inf"} 3
vllm:time_to_first_token_seconds_sum{model_name="m",engine="0"} 0.6
vllm:time_to_first_token_seconds_count{model_name="m",engine="0"} 3
vllm:time_to_first_token_seconds_bucket{model_name="n",engine="0",le="0.1"} 0
vllm:time_to_first_token_seconds_bucket{model_name="n",engine="0",le="1"} 1
vllm:time_to_first_token_seconds_bucket{model_name="n",engine="0",le="+Inf"} 1
vllm:time_to_first_token_seconds_sum{model_name="n",engine="0"} 0.4
vllm:time_to_first_token_seconds_count{model_name="n",engine="0"} 1
# TYPE some_summary summary
some_summary_sum 1
some_summary_count 1
`

// Every kept family rides on the gauge's sample with its type, summed
// across label sets; a histogram's buckets stay in memory for the window
// reduction.
func TestSamplerKeepsFamilies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, familiesPage) }))
	defer srv.Close()
	names := []string{"vllm:num_requests_running", "vllm:generation_tokens_total", "vllm:time_to_first_token_seconds"}
	s := ForRun([]string{srv.URL}, "vllm:num_requests_waiting", names, 20*time.Millisecond)
	s.Start(context.Background())
	time.Sleep(60 * time.Millisecond)
	samples, errs := s.Stop()
	if len(errs) != 0 || len(samples) == 0 {
		t.Fatalf("samples %d errors %v", len(samples), errs)
	}
	f := samples[0].Families
	if f["vllm:num_requests_running"].Type != "gauge" || f["vllm:num_requests_running"].Value != 4 {
		t.Fatalf("running: %+v", f["vllm:num_requests_running"])
	}
	if f["vllm:generation_tokens_total"].Type != "counter" || f["vllm:generation_tokens_total"].Value != 500 {
		t.Fatalf("tokens: %+v", f["vllm:generation_tokens_total"])
	}
	h := f["vllm:time_to_first_token_seconds"]
	if h.Type != "histogram" || h.Count != 4 || h.Sum != 1.0 || len(h.buckets) != 3 || h.buckets[0] != 2 || h.buckets[1] != 4 || h.bounds[1] != 1 {
		t.Fatalf("ttft: %+v buckets %v bounds %v", h, h.buckets, h.bounds)
	}
	if samples[0].Value != 1 {
		t.Fatalf("gauge %v", samples[0].Value)
	}
}

// A kept family the page does not expose is counted apart from the
// scrape errors and the sample stays without it; the gauge still reads.
func TestSamplerCountsAnAbsentFamilyApart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, familiesPage) }))
	defer srv.Close()
	s := ForRun([]string{srv.URL}, "vllm:num_requests_waiting", []string{"vllm:num_requests_running", "no_such_family"}, 20*time.Millisecond)
	s.Start(context.Background())
	time.Sleep(60 * time.Millisecond)
	samples, errs := s.Stop()
	if len(samples) == 0 || len(errs) != 0 || len(s.FamilyErrors()) != len(samples) {
		t.Fatalf("samples %d scrape errors %d family errors %d", len(samples), len(errs), len(s.FamilyErrors()))
	}
	if _, ok := samples[0].Families["no_such_family"]; ok || samples[0].Families["vllm:num_requests_running"].Value != 4 {
		t.Fatalf("families: %+v", samples[0].Families)
	}
}

// A non-finite value in a kept family is an error, as it is for the gauge.
func TestFamilyValueRejectsNonFinite(t *testing.T) {
	for _, page := range []string{
		"# TYPE g gauge\ng NaN\n",
		"# TYPE c counter\nc_total +Inf\n",
		"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\nh_sum NaN\nh_count 1\n",
	} {
		fams, err := parse([]byte(page), "http://x/metrics")
		if err != nil {
			t.Fatal(err)
		}
		for name := range fams {
			if _, err := familyValue(fams, name, "http://x/metrics"); err == nil {
				t.Fatalf("%q must be refused", page)
			}
		}
	}
}

// Preflight refuses an unreadable gauge or family, and a histogram name
// that is a family of another type.
func TestPreflight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, familiesPage) }))
	defer srv.Close()
	ok := func(gauge string, names []string, hist string) error {
		return Preflight(context.Background(), srv.Client(), srv.URL, gauge, names, hist)
	}
	if err := ok("vllm:num_requests_waiting", []string{"vllm:num_requests_running", "vllm:time_to_first_token_seconds"}, "vllm:time_to_first_token_seconds"); err != nil {
		t.Fatal(err)
	}
	if err := ok("no_such_gauge", nil, ""); err == nil {
		t.Fatal("an absent gauge must be refused")
	}
	err := ok("vllm:num_requests_waiting", []string{"vllm:num_requests_running", "absent_family"}, "")
	if err == nil || !strings.Contains(err.Error(), "absent_family") {
		t.Fatalf("absent family: %v", err)
	}
	if err := ok("vllm:num_requests_waiting", []string{"some_summary"}, ""); err == nil {
		t.Fatal("a summary family must be refused")
	}
	err = ok("vllm:num_requests_waiting", []string{"vllm:num_requests_running"}, "vllm:num_requests_running")
	if err == nil || !strings.Contains(err.Error(), "where a histogram is needed") {
		t.Fatalf("a gauge named as the histogram: %v", err)
	}
}

// A reduction from a live page carries no +Inf bucket and marshals.
func TestReduceWindowMarshals(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, familiesPage) }))
	defer srv.Close()
	s := ForRun([]string{srv.URL}, "vllm:num_requests_waiting", []string{"vllm:time_to_first_token_seconds"}, 20*time.Millisecond)
	epoch := time.Now()
	s.Start(context.Background())
	time.Sleep(60 * time.Millisecond)
	samples, _ := s.Stop()
	red := ReduceWindow(samples, epoch, 0, int64(time.Hour))
	h := red["r0"]["vllm:time_to_first_token_seconds"]
	if len(h.Buckets) != 2 || h.Buckets[1].LE != 1 {
		t.Fatalf("buckets: %+v", h.Buckets)
	}
	if _, err := json.Marshal(red); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := json.Marshal(samples); err != nil {
		t.Fatalf("marshal samples: %v", err)
	}
}

// A counter or histogram that fell between two consecutive samples is a
// reset: that pair contributes the later value, and the pairs either side
// of it still count their increases.
func TestReduceWindowReset(t *testing.T) {
	epoch := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return epoch.Add(time.Duration(s * float64(time.Second))) }
	hist := func(count uint64, sum float64, b0, b1 uint64) Family {
		return Family{Type: "histogram", Count: count, Sum: sum, bounds: []float64{0.1, math.Inf(1)}, buckets: []uint64{b0, b1}}
	}
	mk := func(s float64, counter float64, h Family) Sample {
		return Sample{Replica: "r0", At: at(s), Families: map[string]Family{"c": {Type: "counter", Value: counter}, "h": h}}
	}
	samples := []Sample{
		mk(1, 1000, hist(100, 9, 53, 100)), // the last before the window
		mk(3, 1200, hist(120, 11, 60, 120)),
		mk(4, 20, hist(5, 0.5, 3, 5)),        // restart
		mk(9, 2150, hist(130, 6.5, 70, 130)), // above the earlier values again
	}
	got := ReduceWindow(samples, epoch, 2e9, 10e9)["r0"]
	if c := got["c"]; !c.Reset || c.Increase != 200+20+2130 {
		t.Fatalf("counter across a restart: %+v", c)
	}
	h := got["h"]
	if !h.Reset || h.Count != 20+5+125 || h.Sum != 2+0.5+6 || len(h.Buckets) != 1 || h.Buckets[0] != (Bucket{LE: 0.1, Count: 7 + 3 + 67}) {
		t.Fatalf("histogram across a restart: %+v", h)
	}
	// A restart that changes the bucket layout inside the window keeps
	// Count and Sum and reports no buckets.
	samples = append(samples, Sample{Replica: "r0", At: at(9.5), Families: map[string]Family{
		"h": {Type: "histogram", Count: 131, Sum: 6.6, bounds: []float64{0.5, math.Inf(1)}, buckets: []uint64{131, 131}},
	}})
	h = ReduceWindow(samples, epoch, 2e9, 10e9)["r0"]["h"]
	if !h.Reset || h.Count != 20+5+125+131 || h.Sum != 2+0.5+6+6.6 || len(h.Buckets) != 0 {
		t.Fatalf("histogram across a layout change: %+v", h)
	}
}

// The bucket layout follows the sample the increase is measured from, so
// a layout change before the window leaves the in-window increases whole.
func TestReduceWindowLayoutChangeBeforeTheWindow(t *testing.T) {
	epoch := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return epoch.Add(time.Duration(s * float64(time.Second))) }
	old := func(count uint64, b0 uint64) Family {
		return Family{Type: "histogram", Count: count, Sum: float64(count), bounds: []float64{0.1, 0.2, math.Inf(1)}, buckets: []uint64{b0, count, count}}
	}
	neu := func(count uint64, b0 uint64) Family {
		return Family{Type: "histogram", Count: count, Sum: float64(count), bounds: []float64{0.5, math.Inf(1)}, buckets: []uint64{b0, count}}
	}
	samples := []Sample{
		{Replica: "r0", At: at(0.5), Families: map[string]Family{"h": old(50, 10)}},
		{Replica: "r0", At: at(1), Families: map[string]Family{"h": neu(7, 2)}}, // the last before the window, new layout
		{Replica: "r0", At: at(3), Families: map[string]Family{"h": neu(17, 6)}},
		{Replica: "r0", At: at(4), Families: map[string]Family{"h": neu(27, 9)}},
	}
	h := ReduceWindow(samples, epoch, 2e9, 10e9)["r0"]["h"]
	if h.Reset || h.Count != 20 || h.Sum != 20 || len(h.Buckets) != 1 || h.Buckets[0] != (Bucket{LE: 0.5, Count: 7}) {
		t.Fatalf("increase over the new layout: %+v", h)
	}
}

// Over a window, a gauge reduces to its mean over the samples inside it,
// a counter to its increase, and a histogram to the increase in count,
// sum and every bucket, from the last sample before the window.
func TestReduceWindowOracle(t *testing.T) {
	epoch := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return epoch.Add(time.Duration(s * float64(time.Second))) }
	hist := func(count uint64, sum float64, b0, b1 uint64) Family {
		return Family{Type: "histogram", Count: count, Sum: sum, bounds: []float64{0.1, math.Inf(1)}, buckets: []uint64{b0, b1}}
	}
	mk := func(s float64, gauge float64, counter float64, h Family) Sample {
		return Sample{Replica: "r0", At: at(s), Value: 0, Families: map[string]Family{
			"g": {Type: "gauge", Value: gauge}, "c": {Type: "counter", Value: counter}, "h": h,
		}}
	}
	samples := []Sample{
		mk(3, 9, 100, hist(10, 1.0, 5, 10)),                                                     // before the window: the histogram and counter baseline
		mk(4, 9, 110, hist(12, 1.2, 6, 12)),                                                     // the last before the window
		mk(5, 1, 120, hist(14, 1.5, 7, 14)),                                                     // inside
		mk(6, 3, 130, hist(20, 2.5, 12, 20)),                                                    // inside, the last
		mk(10, 99, 999, hist(99, 9, 99, 99)),                                                    // at the end: excluded
		{Replica: "r1", At: at(1), Families: map[string]Family{"g": {Type: "gauge", Value: 5}}}, // never inside
	}
	got := ReduceWindow(samples, epoch, 5e9, 10e9)
	if len(got) != 1 {
		t.Fatalf("replicas: %v", got)
	}
	r := got["r0"]
	if g := r["g"]; g.Type != "gauge" || g.Samples != 2 || g.Mean != 2 {
		t.Fatalf("gauge: %+v", g)
	}
	if c := r["c"]; c.Type != "counter" || c.Increase != 20 || c.Samples != 2 {
		t.Fatalf("counter: %+v", c)
	}
	h := r["h"]
	if h.Type != "histogram" || h.Reset || h.Count != 8 || h.Sum != 1.3 || len(h.Buckets) != 1 || h.Buckets[0] != (Bucket{LE: 0.1, Count: 6}) {
		t.Fatalf("histogram: %+v", h)
	}

	// No sample before the window: the first inside is the baseline.
	got = ReduceWindow(samples[2:4], epoch, 5e9, 10e9)
	if c := got["r0"]["c"]; c.Increase != 10 {
		t.Fatalf("counter from the first inside sample: %+v", c)
	}
}

// Label sets are measured apart: one that resets while the family's sum
// still grows is a reset with its own increase, one absent from a sample
// adds nothing there and steps from its last sample when it returns.
func TestReduceWindowPerLabelSet(t *testing.T) {
	epoch := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return epoch.Add(time.Duration(s * float64(time.Second))) }
	ctr := func(a float64, b *float64) Family {
		f := Family{Type: "counter", Value: a, series: []series{{labels: "engine=a", value: a}}}
		if b != nil {
			f.Value += *b
			f.series = append(f.series, series{labels: "engine=b", value: *b})
		}
		return f
	}
	v := func(x float64) *float64 { return &x }
	samples := []Sample{
		{Replica: "r0", At: at(1), Families: map[string]Family{"c": ctr(100, v(100))}},
		{Replica: "r0", At: at(3), Families: map[string]Family{"c": ctr(1, v(201))}},  // a reset, b grew
		{Replica: "r0", At: at(5), Families: map[string]Family{"c": ctr(11, nil)}},    // b absent
		{Replica: "r0", At: at(7), Families: map[string]Family{"c": ctr(21, v(211))}}, // b back, measured from 201
	}
	c := ReduceWindow(samples, epoch, 2e9, 10e9)["r0"]["c"]
	if !c.Reset || c.Increase != 1+101+10+10+10 {
		t.Fatalf("counter per label set: %+v", c)
	}

	hist := func(count uint64, sum float64, b0 uint64) series {
		return series{count: count, sum: sum, buckets: []uint64{b0, count}}
	}
	hf := func(sa, sb series) Family {
		sa.labels, sb.labels = "m=a", "m=b"
		return Family{Type: "histogram", Count: sa.count + sb.count, Sum: sa.sum + sb.sum,
			bounds: []float64{0.1, math.Inf(1)}, buckets: []uint64{sa.buckets[0] + sb.buckets[0], sa.count + sb.count},
			series: []series{sa, sb}}
	}
	samples = []Sample{
		{Replica: "r0", At: at(1), Families: map[string]Family{"h": hf(hist(100, 9, 50), hist(100, 9, 50))}},
		{Replica: "r0", At: at(3), Families: map[string]Family{"h": hf(hist(2, 0.2, 1), hist(130, 12, 65))}}, // a restarted
	}
	h := ReduceWindow(samples, epoch, 2e9, 10e9)["r0"]["h"]
	if !h.Reset || h.Count != 2+30 || h.Sum != 0.2+3 || len(h.Buckets) != 1 || h.Buckets[0] != (Bucket{LE: 0.1, Count: 1 + 15}) {
		t.Fatalf("histogram per label set: %+v", h)
	}
}

// The parser keeps each label set, so a page's series reach the window
// reduction apart.
func TestFamilyValueKeepsLabelSets(t *testing.T) {
	fams, err := parse([]byte("# TYPE c counter\nc{engine=\"b\"} 5\nc{engine=\"a\"} 7\n"), "http://m")
	if err != nil {
		t.Fatal(err)
	}
	f, err := familyValue(fams, "c", "http://m")
	if err != nil {
		t.Fatal(err)
	}
	if f.Value != 12 || len(f.series) != 2 || f.series[0].labels != `engine="b"` || f.series[1].labels != `engine="a"` || f.series[1].value != 7 {
		t.Fatalf("series: %+v", f.series)
	}
}

// Two finite label sets whose sum overflows are an error.
func TestFamilyValueRejectsAnOverflowingSum(t *testing.T) {
	fams, err := parse([]byte("# TYPE c counter\nc{a=\"x\"} 1e308\nc{a=\"y\"} 1e308\n"), "http://m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := familyValue(fams, "c", "http://m"); err == nil {
		t.Fatal("an overflowing sum must be an error")
	}
}

// A label set missing from the last sample before the window keeps its
// earlier sample, so its return inside the window is a step, and two
// label sets whose values would collide unquoted stay apart.
func TestReduceWindowKeepsLabelSetsAcrossAbsence(t *testing.T) {
	epoch := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return epoch.Add(time.Duration(s * float64(time.Second))) }
	ctr := func(pairs ...any) Family {
		f := Family{Type: "counter"}
		for i := 0; i < len(pairs); i += 2 {
			v := pairs[i+1].(float64)
			f.Value += v
			f.series = append(f.series, series{labels: pairs[i].(string), value: v})
		}
		return f
	}
	samples := []Sample{
		{Replica: "r0", At: at(0.5), Families: map[string]Family{"c": ctr("m=a", 100.0, "m=b", 500.0)}},
		{Replica: "r0", At: at(1.5), Families: map[string]Family{"c": ctr("m=a", 110.0)}}, // last before the window, b absent
		{Replica: "r0", At: at(3), Families: map[string]Family{"c": ctr("m=a", 120.0, "m=b", 520.0)}},
	}
	c := ReduceWindow(samples, epoch, 2e9, 10e9)["r0"]["c"]
	if c.Reset || c.Increase != 10+20 {
		t.Fatalf("b must step from its 0.5 s sample: %+v", c)
	}
	fams, err := parse([]byte("# TYPE c counter\nc{a=\"x,b=y\"} 1\nc{a=\"x\",b=\"y\"} 2\n"), "http://m")
	if err != nil {
		t.Fatal(err)
	}
	f, err := familyValue(fams, "c", "http://m")
	if err != nil {
		t.Fatal(err)
	}
	if len(f.series) != 2 || f.series[0].labels == f.series[1].labels {
		t.Fatalf("label sets must not share a key: %+v", f.series)
	}
}

// Two finite gauge label sets whose sum overflows are an error.
func TestExtractRejectsAnOverflowingGaugeSum(t *testing.T) {
	if _, err := extract([]byte("# TYPE q gauge\nq{a=\"x\"} 1e308\nq{a=\"y\"} 1e308\n"), "q", "http://m"); err == nil {
		t.Fatal("an overflowing gauge sum must be an error")
	}
}
