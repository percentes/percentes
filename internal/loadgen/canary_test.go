package loadgen

import (
	"context"
	"testing"
	"time"
)

// The canary completes full streams through execute with the configured
// token count, and its observations are timed from the run epoch.
func TestCanaryStreamsThroughTheReadPath(t *testing.T) {
	epoch := time.Now()
	c, err := StartCanary(context.Background(), epoch)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	streams := c.Stop()
	if len(streams) < 2 {
		t.Fatalf("got %d streams in 1.5 s, want at least 2", len(streams))
	}
	for i, s := range streams[:len(streams)-1] {
		if s.Outcome != OutcomeCompleted || s.Tokens != CanaryTokens || len(s.ITLsUs) != CanaryTokens-1 {
			t.Fatalf("stream %d: %v tokens=%d gaps=%d", i, s.Outcome, s.Tokens, len(s.ITLsUs))
		}
		if s.TTFTUs < CanaryTTFTMs*1000 || s.StartNs <= 0 || s.DoneNs <= s.StartNs {
			t.Fatalf("stream %d: ttft=%dus start=%d done=%d", i, s.TTFTUs, s.StartNs, s.DoneNs)
		}
	}
	// The mock emits on a cumulative schedule from admit, so one gap can
	// come in under the configured value; the first token cannot.
	all := SummarizeCanary(streams, 0, 1<<62)
	if all.Streams != len(streams) || all.Completed < 2 || all.TTFTDevP50Us < 0 || all.ITLDevMaxUs < all.ITLDevP50Us {
		t.Fatalf("summary over everything: %+v", all)
	}
	if none := SummarizeCanary(streams, 1<<61, 1<<62); none.Streams != 0 || none.Completed != 0 {
		t.Fatalf("summary over an empty window: %+v", none)
	}
}

// Deviations are observed minus configured, reduced by nearest-rank order
// statistics over the streams dispatched inside the window.
func TestSummarizeCanaryOrderStatistics(t *testing.T) {
	ms := func(v int64) int64 { return v * 1000 }
	streams := []CanaryStream{
		{StartNs: 1e9, Outcome: OutcomeCompleted, TTFTUs: ms(21), ITLsUs: []int64{ms(10), ms(11), ms(12), ms(13)}},
		{StartNs: 2e9, Outcome: OutcomeCompleted, TTFTUs: ms(25), ITLsUs: []int64{ms(10), ms(10), ms(10), ms(30)}},
		{StartNs: 3e9, Outcome: OutcomeErrored},
		{StartNs: 9e9, Outcome: OutcomeCompleted, TTFTUs: ms(99), ITLsUs: []int64{ms(99)}},
	}
	got := SummarizeCanary(streams, 0, 5e9)
	want := CanarySummary{TTFTMs: CanaryTTFTMs, ITLMs: CanaryITLMs, Tokens: CanaryTokens, Streams: 3, Completed: 2,
		TTFTDevP50Us: ms(1), TTFTDevMaxUs: ms(5), ITLDevP50Us: 0, ITLDevP99Us: ms(20), ITLDevMaxUs: ms(20), EventLagP99Us: ms(25), EventLagMaxUs: ms(25)}
	if got != want {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

// Small positive gap errors accumulate into event lag that the per-gap
// statistic cannot show.
func TestSummarizeCanaryEventLagAccumulates(t *testing.T) {
	gaps := make([]int64, 31)
	for i := range gaps {
		gaps[i] = (CanaryITLMs + 1) * 1000
	}
	streams := []CanaryStream{{StartNs: 1, Outcome: OutcomeCompleted, Tokens: 32, TTFTUs: CanaryTTFTMs * 1000, ITLsUs: gaps}}
	sum := SummarizeCanary(streams, 0, 10e9)
	if sum.ITLDevMaxUs != 1000 || sum.TTFTDevMaxUs != 0 {
		t.Fatalf("per-gap deviation %d us, TTFT deviation %d us", sum.ITLDevMaxUs, sum.TTFTDevMaxUs)
	}
	if sum.EventLagMaxUs != 31000 || sum.EventLagP99Us < 30000 {
		t.Fatalf("event lag max %d us p99 %d us, want the accumulated 31 ms", sum.EventLagMaxUs, sum.EventLagP99Us)
	}
}
