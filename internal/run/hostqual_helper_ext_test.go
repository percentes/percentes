package run_test

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/hostqual"
	"github.com/percentes/percentes/internal/run"
)

// skipIfGateOnly skips when the §2 client-validity gate is the only
// reason the run was invalid, logging the host probe: the unit suite runs
// under the race detector, which the gate measures.
func skipIfGateOnly(t *testing.T, art *run.Artifacts) {
	t.Helper()
	if art.RunValid || len(art.InvalidReasons) == 0 {
		return
	}
	for _, r := range art.InvalidReasons {
		if !strings.HasPrefix(r, "client-validity gate failed") {
			return
		}
	}
	_, _, obs := hostqual.Qualified()
	t.Skipf("client-validity gate the only invalid reason under the race suite: %+v; host probe %+v", art.Loadgen.Gates, obs)
}
