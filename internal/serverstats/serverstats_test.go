package serverstats

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	if ForRun(nil, "g", time.Second) != nil {
		t.Fatal("no URLs must mean no sampler")
	}
	s := ForRun([]string{"http://a/metrics", "http://b/metrics"}, "vllm:num_requests_waiting", time.Second)
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
	s := ForRun([]string{srv.URL}, "vllm:num_requests_waiting", 20*time.Millisecond)
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
