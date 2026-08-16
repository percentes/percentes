package stats

import (
	"sort"
)

// comparison is one secondary hypothesis test entering the Holm family
// (§7: "Holm correction where several secondaries are formally
// compared"). The caller supplies the raw p-value; this package only
// controls the family-wise error rate over pre-registered secondary
// comparisons.
type comparison struct {
	Name string  `json:"name"`
	PRaw float64 `json:"p_raw"`
}

// holmResult is one comparison after Holm–Bonferroni step-down
// adjustment at family-wise alpha.
type holmResult struct {
	Name      string  `json:"name"`
	PRaw      float64 `json:"p_raw"`
	PAdjusted float64 `json:"p_adjusted"`
	Rejected  bool    `json:"rejected"`
	// Rank is the 1-based position in ascending raw-p order.
	Rank int `json:"rank"`
}

// holm applies the Holm–Bonferroni step-down procedure at family-wise
// error rate alpha. Adjusted p-values are made monotone non-decreasing
// in rank order (the standard reporting form), and rejection stops at
// the first comparison that fails to clear its step threshold.
func holm(comparisons []comparison, alpha float64) []holmResult {
	m := len(comparisons)
	if m == 0 {
		return nil
	}

	idx := make([]int, m)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		return comparisons[idx[a]].PRaw < comparisons[idx[b]].PRaw
	})

	// Step-down: at rank i (0-based), threshold is alpha/(m-i). Once a
	// comparison fails, it and all higher-p comparisons are not rejected.
	results := make([]holmResult, m)
	stillRejecting := true
	var prevAdj float64
	for i, oi := range idx {
		c := comparisons[oi]
		factor := float64(m - i)
		adj := c.PRaw * factor
		if adj > 1 {
			adj = 1
		}
		if adj < prevAdj { // enforce monotonicity in rank order
			adj = prevAdj
		}
		prevAdj = adj

		if stillRejecting && c.PRaw > alpha/factor {
			stillRejecting = false
		}
		results[oi] = holmResult{
			Name:      c.Name,
			PRaw:      c.PRaw,
			PAdjusted: adj,
			Rejected:  stillRejecting,
			Rank:      i + 1,
		}
	}
	return results
}
