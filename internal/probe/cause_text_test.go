package probe

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/percentes/percentes/internal/loadgen"
)

// CauseText names loadgen's safety stops by their own text, wrapped or
// not, and leaves any other cause to redact.ErrorText.
func TestCauseTextNamesSafetyStops(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{loadgen.ErrOperatorAbort, "operator abort"},
		{fmt.Errorf("%w: secret", loadgen.ErrOperatorAbort), "operator abort"},
		{loadgen.ErrLedgerCap, "ledger cap reached"},
		{context.Canceled, "cancelled"},
		{errors.New("secret"), "*errors.errorString, text withheld"},
	} {
		if got := CauseText(c.err); got != c.want {
			t.Errorf("CauseText(%v) %q, want %q", c.err, got, c.want)
		}
	}
}
