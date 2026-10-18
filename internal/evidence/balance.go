package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strings"

	"github.com/percentes/percentes/internal/config"
)

// Balance check statuses. A cell starts on BalanceSufficient, and on
// BalanceUnavailable under the route's balance_unavailable ruling.
const (
	BalanceSufficient   = "sufficient"
	BalanceInsufficient = "insufficient"
	BalanceUnavailable  = "unavailable"
	BalanceCheckFailed  = "check_failed"
)

// BalanceCheck is a cell's start balance record read against the cell's
// reservation.
type BalanceCheck struct {
	Status string `json:"status"`
	// AmountUSD is the balance the command printed, and RequiredUSD the
	// cell's probe and scheduled requests at the ledger's price.
	AmountUSD   string `json:"amount_usd,omitempty"`
	RequiredUSD string `json:"required_usd"`
	// Reason is the command's reason for an unavailable balance, or why
	// the check failed.
	Reason string `json:"reason,omitempty"`
	// Ruling is the route's balance_unavailable ruling an unavailable
	// balance starts under.
	Ruling *config.BalanceUnavailable `json:"ruling,omitempty"`
}

// usdRe is a balance command's amount: a decimal number, signed where
// the account owes.
var usdRe = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

// CheckBalance reads rec, a balance command's run, against requiredUSD,
// a decimal number. The command must exit 0 and print one JSON object,
// {"amount_usd": "<decimal>"} or {"unavailable": "<reason>"}, and
// nothing else. ruling is the route's balance_unavailable, or nil.
func CheckBalance(rec *StatusRecord, requiredUSD string, ruling *config.BalanceUnavailable) *BalanceCheck {
	c := &BalanceCheck{RequiredUSD: requiredUSD}
	fail := func(format string, a ...any) *BalanceCheck {
		c.Status, c.Reason = BalanceCheckFailed, fmt.Sprintf(format, a...)
		return c
	}
	if !usdRe.MatchString(requiredUSD) {
		return fail("the cell's reservation %q is not a decimal number", requiredUSD)
	}
	need, _ := new(big.Rat).SetString(requiredUSD)
	switch {
	case rec.Error != "":
		return fail("the command failed: %s", rec.Error)
	case rec.ExitCode == nil || *rec.ExitCode != 0:
		return fail("the command did not exit 0")
	}
	var out struct {
		AmountUSD   *string `json:"amount_usd"`
		Unavailable *string `json:"unavailable"`
	}
	dec := json.NewDecoder(bytes.NewReader(rec.Stdout.Raw()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return fail("the output is not one JSON object with amount_usd or unavailable: %v", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fail("the output holds more than the JSON object")
	}
	switch {
	case (out.AmountUSD == nil) == (out.Unavailable == nil):
		return fail("the output holds neither or both of amount_usd and unavailable")
	case out.Unavailable != nil:
		c.Status, c.Reason = BalanceUnavailable, *out.Unavailable
		if strings.TrimSpace(c.Reason) == "" {
			return fail("unavailable gives no reason")
		}
		c.Ruling = ruling
		return c
	}
	c.AmountUSD = *out.AmountUSD
	if !usdRe.MatchString(c.AmountUSD) {
		return fail("amount_usd %q is not a decimal number", c.AmountUSD)
	}
	have, _ := new(big.Rat).SetString(c.AmountUSD)
	c.Status = BalanceSufficient
	if have.Cmp(need) < 0 {
		c.Status = BalanceInsufficient
	}
	return c
}

// Blocks returns why the check blocks the cell's start, or "" when the
// cell may start.
func (c *BalanceCheck) Blocks() string {
	switch c.Status {
	case BalanceSufficient:
		return ""
	case BalanceInsufficient:
		return fmt.Sprintf("balance US$%s is below the cell's reservation of US$%s", c.AmountUSD, c.RequiredUSD)
	case BalanceUnavailable:
		if c.Ruling != nil {
			return ""
		}
		return fmt.Sprintf("balance unavailable (%s), and the route records no balance_unavailable ruling", c.Reason)
	default:
		return "balance check failed: " + c.Reason
	}
}
