package run

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// Nominal §1 phase offsets in run time: warm-up ends at 60 s, T_inject at
// 360 s, so the guard starts at 330 s (360 minus the pinned 30 s client
// timeout).
const (
	nominalWarmupEndNs  int64 = 60 * 1e9
	nominalGuardStartNs int64 = 330 * 1e9
	nominalTInjectNs    int64 = 360 * 1e9
)

// baselineRequests builds a completed baseline window with the given
// per-replica request counts, all intended inside the guard-bounded baseline
// window [nominalWarmupEndNs, nominalGuardStartNs).
func baselineRequests(counts map[string]int) *loadgen.Result {
	res := &loadgen.Result{WarmupEndNs: nominalWarmupEndNs, TInjectNs: nominalTInjectNs}
	var i int64
	for rep, n := range counts {
		for k := 0; k < n; k++ {
			res.Requests = append(res.Requests, loadgen.Request{
				Index: i, IntendedNs: nominalWarmupEndNs + 1000, Replica: rep, Outcome: loadgen.OutcomeCompleted,
			})
			i++
		}
	}
	return res
}

// addGuardRequests appends requests intended inside the guard window
// [nominalGuardStartNs, nominalTInjectNs).
func addGuardRequests(res *loadgen.Result, replica string, n int) {
	i := int64(len(res.Requests))
	for k := 0; k < n; k++ {
		res.Requests = append(res.Requests, loadgen.Request{
			Index: i, IntendedNs: nominalGuardStartNs + 1000, Replica: replica, Outcome: loadgen.OutcomeCompleted,
		})
		i++
	}
}

func regimeCfg(dataplane string) *config.Config {
	cfg := &config.Config{}
	cfg.Target.Replicas = 2
	cfg.ShareGate = config.ShareGate{MinPct: 45, MaxPct: 55}
	cfg.Pins.Kubernetes.DataplaneMode = dataplane
	return cfg
}

// Behind a layer-7 proxy the dataplane assigns each request independently
// and the band is enforced: an out-of-band share fails the run (§1).
func TestShareBandEnforcedUnderPerRequestBalancing(t *testing.T) {
	res := baselineRequests(map[string]int{"pod-a": 580, "pod-b": 420}) // 58/42
	sg := shareGate(regimeCfg("l7-envoy-cilium-ingress"), res, nominalGuardStartNs, "")

	if !sg.Applicable {
		t.Fatal("2-replica target must be applicable")
	}
	if sg.Pass {
		t.Errorf("58/42 is outside 45-55%% and must fail under per-request balancing: %+v", sg)
	}
	if !sg.BandEnforced {
		t.Error("BandEnforced must be true for a request-aware layer-7 proxy")
	}
}

// Under per-connection routing the share is descriptive: it is still
// measured and reported, but it cannot fail the run, because the quantity
// is a binomial draw over the connection count rather than a property of
// the system under test. A layer-4 dataplane written in eBPF binds the
// connection the same way kube-proxy does (§1).
func TestShareDescriptiveUnderPerConnectionRouting(t *testing.T) {
	for _, dp := range []string{"kube-proxy-iptables", "ebpf-cilium"} {
		res := baselineRequests(map[string]int{"pod-a": 580, "pod-b": 420}) // 58/42
		sg := shareGate(regimeCfg(dp), res, nominalGuardStartNs, "")

		if !sg.Applicable {
			t.Fatalf("%s: shares must still be measured and reported", dp)
		}
		if sg.BandEnforced {
			t.Errorf("%s: BandEnforced must be false for per-connection routing", dp)
		}
		if !sg.Pass {
			t.Errorf("%s: an out-of-band share must not fail the run in this regime: %+v", dp, sg)
		}
		if !strings.Contains(sg.Note, "descriptive") {
			t.Errorf("%s: the report must say the share is descriptive here, got note %q", dp, sg.Note)
		}
		if got := sg.Shares["pod-a"]; got < 0.579 || got > 0.581 {
			t.Errorf("%s: shares must be reported accurately regardless of regime: %v", dp, sg.Shares)
		}
	}
}

// A within-band share passes in both regimes.
func TestShareInBandPassesEitherRegime(t *testing.T) {
	for _, dp := range []string{"l7-gateway-api", "kube-proxy-iptables", "ebpf-cilium"} {
		res := baselineRequests(map[string]int{"pod-a": 505, "pod-b": 495})
		if sg := shareGate(regimeCfg(dp), res, nominalGuardStartNs, ""); !sg.Pass {
			t.Errorf("%s: 50.5/49.5 must pass: %+v", dp, sg)
		}
	}
}

// The share gate is baseline-derived, so guard-window requests stay out of
// it (§1). Oracle: 500/500 inside the baseline is 50/50 and passes;
// counting the 400 guard-window requests on pod-a as well would make it
// 900/1400 = 64.3 percent and fail the enforced band.
func TestShareGateExcludesGuardWindow(t *testing.T) {
	res := baselineRequests(map[string]int{"pod-a": 500, "pod-b": 500})
	addGuardRequests(res, "pod-a", 400)
	sg := shareGate(regimeCfg("l7-envoy-cilium-ingress"), res, nominalGuardStartNs, "")

	if got := sg.Shares["pod-a"]; got < 0.4999 || got > 0.5001 {
		t.Errorf("guard-window requests must not enter the share: pod-a share %v, want 0.5", got)
	}
	if !sg.Pass {
		t.Errorf("the guard-bounded baseline is 50/50 and must pass: %+v", sg)
	}
}

// Structural faults are regime-independent: seeing fewer replicas than
// the topology declares means traffic never reached a replica at all,
// which is a real defect in any dataplane.
func TestMissingReplicaFailsInEveryRegime(t *testing.T) {
	for _, dp := range []string{"l7-gateway-api", "kube-proxy-iptables", "ebpf-cilium"} {
		res := baselineRequests(map[string]int{"pod-a": 1000})
		sg := shareGate(regimeCfg(dp), res, nominalGuardStartNs, "")
		if sg.Pass {
			t.Errorf("%s: one replica serving all traffic must fail: %+v", dp, sg)
		}
		if !strings.Contains(sg.Note, "expected 2") {
			t.Errorf("%s: note must name the replica-count mismatch, got %q", dp, sg.Note)
		}
	}
}
