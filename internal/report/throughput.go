package report

import "fmt"

// Throughput is a step's completions per second over its window
// (SPEC-HOSTED §8): the completions, in time or late, of the requests
// the step scheduled, over WindowS, the configured step window summed
// over the cells counted. Tokens sums the provider-reported completion
// tokens of the TokenCompletions completions whose records carry them;
// TokensPerS is nil where none does.
type Throughput struct {
	WindowS          float64  `json:"window_s"`
	Completions      int      `json:"completions"`
	PerS             float64  `json:"completions_per_s"`
	TokenCompletions int      `json:"completions_with_tokens"`
	Tokens           int      `json:"completion_tokens"`
	TokensPerS       *float64 `json:"completion_tokens_per_s,omitempty"`
}

// PooledThroughput is a step's throughput pooled over its cells, with
// the range of the cells' completions per second.
type PooledThroughput struct {
	Throughput
	CellMin float64 `json:"cell_completions_per_s_min"`
	CellMax float64 `json:"cell_completions_per_s_max"`
}

// throughput is the tallies' throughput over a window of stepS seconds
// each.
func throughput(ts []*stepTally, stepS float64) Throughput {
	tp := Throughput{WindowS: stepS * float64(len(ts))}
	for _, t := range ts {
		tp.Completions += len(t.events)
		tp.TokenCompletions += len(t.tokens)
		for _, n := range t.tokens {
			tp.Tokens += n
		}
	}
	tp.PerS = float64(tp.Completions) / tp.WindowS
	if tp.TokenCompletions > 0 {
		v := float64(tp.Tokens) / tp.WindowS
		tp.TokensPerS = &v
	}
	return tp
}

func pooledThroughput(ts []*stepTally, stepS float64) PooledThroughput {
	p := PooledThroughput{Throughput: throughput(ts, stepS)}
	for i, t := range ts {
		r := throughput([]*stepTally{t}, stepS).PerS
		if i == 0 || r < p.CellMin {
			p.CellMin = r
		}
		if i == 0 || r > p.CellMax {
			p.CellMax = r
		}
	}
	return p
}

// tokensText is a throughput's provider-reported completion tokens.
func tokensText(tp Throughput) string {
	if tp.TokensPerS == nil {
		return "no completion carried provider-reported completion tokens"
	}
	return fmt.Sprintf("%d provider-reported completion tokens from %d of %d completions, %.2f per second",
		tp.Tokens, tp.TokenCompletions, tp.Completions, *tp.TokensPerS)
}

// throughputText renders a cell step's throughput.
func throughputText(w func(string, ...any), tp Throughput) {
	w("  throughput over the step's configured %g s window: %d completions, %.3f per second; %s",
		tp.WindowS, tp.Completions, tp.PerS, tokensText(tp))
}

// pooledThroughputText renders a pooled step's throughput over its
// cells' windows of stepS seconds.
func pooledThroughputText(w func(string, ...any), p PooledThroughput, cells int, stepS float64) {
	w("  throughput over %d configured step windows of %g s: %d completions, %.3f per second; cell values %.3f to %.3f per second; %s",
		cells, stepS, p.Completions, p.PerS, p.CellMin, p.CellMax, tokensText(p.Throughput))
}
