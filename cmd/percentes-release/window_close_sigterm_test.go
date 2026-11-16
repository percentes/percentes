package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/schedule"
)

// A SIGTERM that arrives while a window close fails ends the route with
// the resumable termination exit and the close's error on standard
// error.
func TestSigtermDuringFailingWindowCloseResumes(t *testing.T) {
	var ran []schedule.Run
	r := clockRoute(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), counted, &ran)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.close = func(context.Context, *routeState) error {
		cancel(errTerminated)
		return errors.New("refresh failed")
	}
	var code int
	var err error
	var out string
	errText := stderrOf(t, func() { out = stdoutOf(t, func() { code, err = r.loop(ctx) }) })
	if err != nil || code != exitTerminated || !strings.Contains(out, "groq: ended on SIGTERM during the window close; no hold, the next start goes on") ||
		!strings.Contains(errText, "groq: window close: refresh failed") || len(ran) != 12 {
		t.Fatalf("loop: %d, %v, %d runs\n%s%s", code, err, len(ran), out, errText)
	}
}
