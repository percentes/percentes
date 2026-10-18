package evidence

import (
	"context"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// A balance command's run reads as sufficient only where it exits 0 and
// prints an amount at or above the reservation; an unavailable balance
// starts the cell only under the route's ruling; every other run is a
// failed check that blocks.
func TestCheckBalance(t *testing.T) {
	ruling := &config.BalanceUnavailable{Reason: "console only", Ruled: "2026-10-12", Ruling: "rulings.md"}
	for _, tc := range []struct {
		name     string
		script   string
		required string
		ruling   *config.BalanceUnavailable
		status   string
		blocks   string
	}{
		{"above", `echo '{"amount_usd": "30.00"}'`, "1.8123", nil, BalanceSufficient, ""},
		{"equal", `echo '{"amount_usd": "1.8123"}'`, "1.8123", nil, BalanceSufficient, ""},
		{"below", `echo '{"amount_usd": "1.81"}'`, "1.8123", nil, BalanceInsufficient, "balance US$1.81 is below the cell's reservation of US$1.8123"},
		{"owed", `echo '{"amount_usd": "-4"}'`, "1.8123", nil, BalanceInsufficient, "balance US$-4 is below"},
		{"unavailable under a ruling", `echo '{"unavailable": "shown in the console only"}'`, "1.8123", ruling, BalanceUnavailable, ""},
		{"unavailable without a ruling", `echo '{"unavailable": "shown in the console only"}'`, "1.8123", nil, BalanceUnavailable,
			"balance unavailable (shown in the console only), and the route records no balance_unavailable ruling"},
		{"unavailable with no reason", `echo '{"unavailable": " "}'`, "1.8123", ruling, BalanceCheckFailed, "balance check failed: unavailable gives no reason"},
		{"exit 3", `echo '{"amount_usd": "30.00"}'; exit 3`, "1.8123", ruling, BalanceCheckFailed, "balance check failed: the command failed: exit status 3"},
		{"no output", `true`, "1.8123", ruling, BalanceCheckFailed, "balance check failed: the output is not one JSON object"},
		{"text", `echo 'balance USD 30.00'`, "1.8123", ruling, BalanceCheckFailed, "balance check failed: the output is not one JSON object"},
		{"amount as a number", `echo '{"amount_usd": 30}'`, "1.8123", ruling, BalanceCheckFailed, "balance check failed: the output is not one JSON object"},
		{"amount not decimal", `echo '{"amount_usd": "30 USD"}'`, "1.8123", ruling, BalanceCheckFailed, `balance check failed: amount_usd "30 USD" is not a decimal number`},
		{"unknown field", `echo '{"amount_usd": "30", "currency": "GBP"}'`, "1.8123", ruling, BalanceCheckFailed, "balance check failed: the output is not one JSON object"},
		{"both", `echo '{"amount_usd": "30", "unavailable": "x"}'`, "1.8123", ruling, BalanceCheckFailed, "neither or both"},
		{"neither", `echo '{}'`, "1.8123", ruling, BalanceCheckFailed, "neither or both"},
		{"two objects", `echo '{"amount_usd": "30"}{"amount_usd": "30"}'`, "1.8123", ruling, BalanceCheckFailed, "more than the JSON object"},
		{"trailing text", `echo '{"amount_usd": "30"} ok'`, "1.8123", ruling, BalanceCheckFailed, "more than the JSON object"},
		{"no reservation", `echo '{"amount_usd": "30"}'`, "", ruling, BalanceCheckFailed, `the cell's reservation "" is not a decimal number`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := ReadBalance(context.Background(), []string{"/bin/sh", "-c", tc.script})
			c := CheckBalance(rec, tc.required, tc.ruling)
			got := c.Blocks()
			if c.Status != tc.status || (tc.blocks == "") != (got == "") || !strings.Contains(got, tc.blocks) {
				t.Fatalf("check %+v blocks %q; want status %s, blocks %q", c, got, tc.status, tc.blocks)
			}
			if c.RequiredUSD != tc.required {
				t.Errorf("required %q, want %q", c.RequiredUSD, tc.required)
			}
			if (c.Ruling != nil) != (tc.status == BalanceUnavailable && tc.ruling != nil) {
				t.Errorf("ruling %+v", c.Ruling)
			}
		})
	}
}

// A run that did not start, or whose exit code is missing, is a failed
// check.
func TestCheckBalanceNoExit(t *testing.T) {
	rec := ReadBalance(context.Background(), []string{"/nonexistent/balance"})
	if c := CheckBalance(rec, "1", nil); c.Status != BalanceCheckFailed || !strings.Contains(c.Reason, "the command failed") {
		t.Fatalf("command not started: %+v", c)
	}
	rec = &StatusRecord{Stdout: bytesOf([]byte(`{"amount_usd": "30"}`))}
	if c := CheckBalance(rec, "1", nil); c.Status != BalanceCheckFailed || c.Reason != "the command did not exit 0" {
		t.Fatalf("no exit code: %+v", c)
	}
}
