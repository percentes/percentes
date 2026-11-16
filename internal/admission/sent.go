package admission

import (
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

// ProbeSent counts the inference requests a probe dispatched that reached
// a connection (probe.LeftClient): the probe request and the wrong-model
// request. The limits read goes to another URL and is not counted. A nil
// record sent nothing.
func ProbeSent(rec *probe.Record) int {
	if rec == nil {
		return 0
	}
	n := 0
	if probe.LeftClient(&rec.Request) {
		n++
	}
	if rec.WrongModel != nil && probe.LeftClient(rec.WrongModel) {
		n++
	}
	return n
}

// RunSent counts a run's dispatched requests, split at the end of
// warm-up by intended dispatch time. Every dispatch counts, whatever its
// outcome, except one the write limit refused before its first byte. A
// nil result sent nothing.
func RunSent(res *loadgen.Result) (warmup, after int) {
	if res == nil {
		return 0, 0
	}
	for i := range res.Requests {
		r := &res.Requests[i]
		if r.DispatchNs == 0 || r.WriteDenied {
			continue
		}
		if r.IntendedNs < res.WarmupEndNs {
			warmup++
		} else {
			after++
		}
	}
	return warmup, after
}

// WindowSent is a window's settlement: its probe's requests and its run's
// warm-up and measured requests.
func WindowSent(rec *probe.Record, res *loadgen.Result) Counts {
	w, m := RunSent(res)
	return Counts{Probe: ProbeSent(rec), Warmup: w, Measured: m}
}
