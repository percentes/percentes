package run

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// baselineRequests builds a completed baseline window with the given
// per-replica request counts, all inside [WarmupEndNs, TInjectNs).
func baselineRequests(counts map[string]int) *loadgen.Result {
	res := &loadgen.Result{WarmupEndNs: 0, TInjectNs: 1e9}
	var i int64
	for rep, n := range counts {
		for k := 0; k < n; k++ {
			res.Requests = append(res.Requests, loadgen.Request{
				Index: i, IntendedNs: 1000, Replica: rep, Outcome: loadgen.OutcomeCompleted,
			})
			i++
		}
	}
	return res
}

func regimeCfg(dataplane string) *config.Config {
	cfg := &config.Config{}
	cfg.Target.Replicas = 2
	cfg.ShareGate = config.ShareGate{MinPct: 45, MaxPct: 55}
	cfg.Pins.Kubernetes.DataplaneMode = dataplane
	return cfg
}

// Under per-request load balancing the band is enforced: an out-of-band
// share fails the run.
func TestShareBandEnforcedUnderPerRequestBalancing(t *testing.T) {
	res := baselineRequests(map[string]int{"pod-a": 580, "pod-b": 420}) // 58/42
	sg := shareGate(regimeCfg("ebpf-cilium"), res, 0, "")

	if !sg.Applicable {
		t.Fatal("2-replica target must be applicable")
	}
	if sg.Pass {
		t.Errorf("58/42 is outside 45-55%% and must fail under per-request balancing: %+v", sg)
	}
	if !sg.BandEnforced {
		t.Error("BandEnforced must be true for an eBPF dataplane")
	}
}

// Under per-connection routing (kube-proxy) the share is descriptive: it
// is still measured and reported, but it cannot fail the run, because the
// quantity is a binomial draw over the connection count rather than a
// property of the system under test.
func TestShareDescriptiveUnderPerConnectionRouting(t *testing.T) {
	res := baselineRequests(map[string]int{"pod-a": 580, "pod-b": 420}) // 58/42
	sg := shareGate(regimeCfg("kube-proxy-iptables"), res, 0, "")

	if !sg.Applicable {
		t.Fatal("shares must still be measured and reported")
	}
	if sg.BandEnforced {
		t.Error("BandEnforced must be false for per-connection routing")
	}
	if !sg.Pass {
		t.Errorf("an out-of-band share must not fail the run in this regime: %+v", sg)
	}
	if !strings.Contains(sg.Note, "descriptive") {
		t.Errorf("the report must say the share is descriptive here, got note %q", sg.Note)
	}
	if got := sg.Shares["pod-a"]; got < 0.579 || got > 0.581 {
		t.Errorf("shares must be reported accurately regardless of regime: %v", sg.Shares)
	}
}

// A within-band share passes in both regimes.
func TestShareInBandPassesEitherRegime(t *testing.T) {
	for _, dp := range []string{"ebpf-cilium", "kube-proxy-iptables"} {
		res := baselineRequests(map[string]int{"pod-a": 505, "pod-b": 495})
		if sg := shareGate(regimeCfg(dp), res, 0, ""); !sg.Pass {
			t.Errorf("%s: 50.5/49.5 must pass: %+v", dp, sg)
		}
	}
}

// Structural faults are regime-independent: seeing fewer replicas than
// the topology declares means traffic never reached a replica at all,
// which is a real defect in any dataplane.
func TestMissingReplicaFailsInEveryRegime(t *testing.T) {
	for _, dp := range []string{"ebpf-cilium", "kube-proxy-iptables"} {
		res := baselineRequests(map[string]int{"pod-a": 1000})
		sg := shareGate(regimeCfg(dp), res, 0, "")
		if sg.Pass {
			t.Errorf("%s: one replica serving all traffic must fail: %+v", dp, sg)
		}
		if !strings.Contains(sg.Note, "expected 2") {
			t.Errorf("%s: note must name the replica-count mismatch, got %q", dp, sg.Note)
		}
	}
}
