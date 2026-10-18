package evidence

import (
	"context"
	"errors"
	"strings"
	"time"
)

// The endpoint-status records' file names in a run's output directory.
const (
	StatusStartFile = "status-start.json"
	StatusEndFile   = "status-end.json"
)

// The account balance or quota records' file names in a release cell's
// directory.
const (
	BalanceStartFile = "balance-start.json"
	BalanceEndFile   = "balance-end.json"
)

// statusTimeout bounds the endpoint-status command's run time.
var statusTimeout = 30 * time.Second

// statusOutputLimit bounds each of the command's output streams.
const statusOutputLimit = 1 << 20

// StatusRecord is one run of the operator's endpoint-status command, or
// of its balance command, at run start or run end, with its output
// verbatim and unparsed. Error is set when the command failed, timed out,
// or printed past the size limit.
type StatusRecord struct {
	Command  []string  `json:"command"`
	StartUTC time.Time `json:"start_utc"`
	EndUTC   time.Time `json:"end_utc"`
	// ExitCode is absent when the command did not start, and -1 when a
	// signal ended it.
	ExitCode *int  `json:"exit_code,omitempty"`
	Stdout   Bytes `json:"stdout"`
	Stderr   Bytes `json:"stderr"`
	// DroppedBytes counts output past statusOutputLimit, not kept.
	DroppedBytes int64  `json:"dropped_bytes,omitempty"`
	Error        string `json:"error,omitempty"`
	// Check is a release cell's start balance read against the cell's
	// reservation, and absent from every other record.
	Check *BalanceCheck `json:"check,omitempty"`
}

// ParseStatusCommand splits s on white space into the endpoint-status
// command's arguments.
func ParseStatusCommand(s string) ([]string, error) {
	args := strings.Fields(s)
	if len(args) == 0 {
		return nil, errors.New("evidence: no endpoint-status command")
	}
	return args, nil
}

// ParseBalanceCommand splits s on white space into the balance
// command's arguments.
func ParseBalanceCommand(s string) ([]string, error) {
	args := strings.Fields(s)
	if len(args) == 0 {
		return nil, errors.New("evidence: no balance command")
	}
	return args, nil
}

// ReadStatus runs the endpoint-status command args with only PATH and
// LC_ALL=C in its environment and records its output.
func ReadStatus(ctx context.Context, args []string) *StatusRecord {
	run := runCommand(ctx, args, statusTimeout, statusOutputLimit)
	return &StatusRecord{
		Command:      args,
		StartUTC:     run.start,
		EndUTC:       run.end,
		ExitCode:     run.exitCode,
		Stdout:       bytesOf(run.stdout),
		Stderr:       bytesOf(run.stderr),
		DroppedBytes: run.dropped,
		Error:        run.err,
	}
}

// ReadBalance runs the balance command args as ReadStatus runs the
// endpoint-status command; the route's key is not in its environment,
// so a command that reads the balance with a credential reads it from a
// file its arguments name.
func ReadBalance(ctx context.Context, args []string) *StatusRecord {
	return ReadStatus(ctx, args)
}

// WriteStatus writes rec to name in dir and returns the file's path.
func WriteStatus(dir, name string, rec *StatusRecord) (string, error) {
	return writeJSON(dir, name, rec)
}
