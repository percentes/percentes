package collect

import (
	"sort"

	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/serverstats"
)

// ReceivePath is the §2 receive-path report for one window, neither check
// run-failing: the client-side time-to-first-token (TTFT) mean over
// completed requests against the server-side histogram's mean over the
// same window, pooled across replicas, and the loopback canary's event
// lag behind its known timing. The divergence also holds the network
// round trip and any difference in where the server starts its clock. A
// window filtered to one replica carries no receive-path report.
type ReceivePath struct {
	ClientTTFTMeanMs float64 `json:"client_ttft_mean_ms"`
	ClientTTFTCount  int     `json:"client_ttft_count"`
	ServerTTFTFamily string  `json:"server_ttft_family,omitempty"`
	ServerTTFTMeanMs float64 `json:"server_ttft_mean_ms,omitempty"`
	ServerTTFTCount  uint64  `json:"server_ttft_count,omitempty"`
	// DivergenceMs is client minus server, present when both means exist.
	DivergenceMs *float64               `json:"divergence_ms,omitempty"`
	Canary       *loadgen.CanarySummary `json:"canary,omitempty"`
	// CanaryError is the error that kept the canary from starting.
	CanaryError string `json:"canary_error,omitempty"`
	// ServerReset names the replicas whose histogram fell inside the
	// window, which a replica restart does (serverstats.Reduction).
	ServerReset []string `json:"server_reset,omitempty"`
}

// ReceivePathFor builds the window's report from its collected stats, the
// per-replica family reductions over the same window, the histogram
// family target.ttft_histogram names (in seconds), and the canary
// summary; server and canary may be nil.
func ReceivePathFor(st *Stats, server map[string]map[string]serverstats.Reduction, ttftFamily string, canary *loadgen.CanarySummary) *ReceivePath {
	rp := &ReceivePath{Canary: canary}
	if n := len(st.RawTTFTUs); n > 0 {
		var sum int64
		for _, v := range st.RawTTFTUs {
			sum += v
		}
		rp.ClientTTFTMeanMs = float64(sum) / float64(n) / 1000
		rp.ClientTTFTCount = n
	}
	if ttftFamily == "" {
		return rp
	}
	rp.ServerTTFTFamily = ttftFamily
	var count uint64
	var sum float64
	for _, replica := range sortedReplicas(server) {
		if r, ok := server[replica][ttftFamily]; ok && r.Type == "histogram" {
			count += r.Count
			sum += r.Sum
			if r.Reset {
				rp.ServerReset = append(rp.ServerReset, replica)
			}
		}
	}
	if count > 0 {
		rp.ServerTTFTCount = count
		rp.ServerTTFTMeanMs = sum / float64(count) * 1000
		if rp.ClientTTFTCount > 0 {
			d := rp.ClientTTFTMeanMs - rp.ServerTTFTMeanMs
			rp.DivergenceMs = &d
		}
	}
	return rp
}

func sortedReplicas(server map[string]map[string]serverstats.Reduction) []string {
	keys := make([]string, 0, len(server))
	for k := range server {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
