package collect

import (
	"testing"

	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/serverstats"
)

// The client mean is over the window's completed requests; the server
// mean pools the named histogram across replicas and converts seconds
// to milliseconds; the divergence is client minus server.
func TestReceivePathForPoolsReplicas(t *testing.T) {
	st := &Stats{RawTTFTUs: []int64{100_000, 300_000}}
	server := map[string]map[string]serverstats.Reduction{
		"r0": {"ttft": {Type: "histogram", Count: 3, Sum: 0.3}},
		"r1": {"ttft": {Type: "histogram", Count: 1, Sum: 0.5, Reset: true}, "other": {Type: "gauge", Mean: 9}},
	}
	canary := &loadgen.CanarySummary{Streams: 4}
	rp := ReceivePathFor(st, server, "ttft", canary)
	if rp.ClientTTFTMeanMs != 200 || rp.ClientTTFTCount != 2 {
		t.Fatalf("client: %+v", rp)
	}
	if rp.ServerTTFTFamily != "ttft" || rp.ServerTTFTCount != 4 || rp.ServerTTFTMeanMs != 200 {
		t.Fatalf("server: %+v", rp)
	}
	if rp.DivergenceMs == nil || *rp.DivergenceMs != 0 || rp.Canary != canary {
		t.Fatalf("divergence and canary: %+v", rp)
	}
	if len(rp.ServerReset) != 1 || rp.ServerReset[0] != "r1" {
		t.Fatalf("reset replicas: %v", rp.ServerReset)
	}

	// No histogram kept: the family is named, the server side is empty
	// and there is no divergence.
	rp = ReceivePathFor(st, nil, "ttft", nil)
	if rp.ServerTTFTCount != 0 || rp.DivergenceMs != nil || rp.ServerTTFTFamily != "ttft" || rp.Canary != nil {
		t.Fatalf("without server samples: %+v", rp)
	}
	// No family named: only the client side.
	rp = ReceivePathFor(&Stats{}, server, "", nil)
	if rp.ClientTTFTCount != 0 || rp.ServerTTFTFamily != "" || rp.ServerTTFTCount != 0 {
		t.Fatalf("without a family: %+v", rp)
	}
}
