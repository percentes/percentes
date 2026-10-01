package calibrate

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/loadgen"
)

// The step row names the canary's event lag beside its deviations.
func TestReceivePathRowNamesTheEventLag(t *testing.T) {
	rp := &collect.ReceivePath{ClientTTFTCount: 3, Canary: &loadgen.CanarySummary{Completed: 3, TTFTDevP50Us: 100, TTFTDevMaxUs: 400, ITLDevP99Us: 900, ITLDevMaxUs: 1200, EventLagP99Us: 2500, EventLagMaxUs: 7000}}
	got := receivePathRow(rp)
	for _, want := range []string{"event lag p99 2.50 ms max 7.00 ms", "TTFT deviation p50 0.10 ms max 0.40 ms"} {
		if !strings.Contains(got, want) {
			t.Fatalf("row lacks %q:\n%s", want, got)
		}
	}
}
