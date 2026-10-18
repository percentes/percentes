package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// ClockFile is the clock-offset record's file name in a run's output
// directory.
const ClockFile = "clock.json"

// Clock-sync tools whose output ReadClock parses.
const (
	// ClockSNTP is the macOS sntp client: one line "<offset> +/- <error>
	// <host> <address>".
	ClockSNTP = "sntp"
	// ClockChronyc is chronyc's tracking report in its human-readable
	// form; "System time" is the system clock's offset from chronyd's NTP
	// clock.
	ClockChronyc = "chronyc"
)

// clockTimeout bounds the clock-sync command's run time.
var clockTimeout = 30 * time.Second

// clockOutputLimit bounds each of the command's output streams.
const clockOutputLimit = 64 << 10

// ClockCommand is a host clock-sync command, run with no shell, and the
// tool whose output it prints.
type ClockCommand struct {
	Args []string
	Tool string
}

// ParseClockCommand splits s on white space into a ClockCommand. The
// program's base name selects the tool: sntp with at least one argument,
// or chronyc ending in "tracking" without -c, its comma-separated form.
func ParseClockCommand(s string) (ClockCommand, error) {
	args := strings.Fields(s)
	if len(args) == 0 {
		return ClockCommand{}, errors.New("evidence: no clock-sync command")
	}
	cmd := ClockCommand{Args: args, Tool: filepath.Base(args[0])}
	switch cmd.Tool {
	case ClockSNTP:
		if len(args) < 2 {
			return ClockCommand{}, errors.New("evidence: clock-sync command: sntp needs a server")
		}
	case ClockChronyc:
		if args[len(args)-1] != "tracking" {
			return ClockCommand{}, errors.New("evidence: clock-sync command: chronyc must end in tracking")
		}
		for _, a := range args[1:] {
			if a == "-c" {
				return ClockCommand{}, errors.New("evidence: clock-sync command: chronyc -c is not parsed")
			}
		}
	default:
		return ClockCommand{}, fmt.Errorf("evidence: clock-sync command: %q is not %s or %s", cmd.Tool, ClockSNTP, ClockChronyc)
	}
	return cmd, nil
}

// ClockRecord is the client host's clock offset at run start, read from
// the host's own clock-sync tool. Stdout and Stderr are the command's
// output verbatim. AbsOffsetS, the magnitude of the offset the tool
// reports, is present only for a reading; the direction is in Stdout.
// Flag is set when the run does not count: no reading, or an offset over
// config.PinnedClockOffsetMaxS.
type ClockRecord struct {
	Command  []string  `json:"command"`
	Tool     string    `json:"tool"`
	StartUTC time.Time `json:"start_utc"`
	EndUTC   time.Time `json:"end_utc"`
	// ExitCode is absent when the command did not start, and -1 when a
	// signal ended it.
	ExitCode *int  `json:"exit_code,omitempty"`
	Stdout   Bytes `json:"stdout"`
	Stderr   Bytes `json:"stderr"`
	// DroppedBytes counts output past clockOutputLimit, not kept.
	DroppedBytes int64    `json:"dropped_bytes,omitempty"`
	Error        string   `json:"error,omitempty"`
	AbsOffsetS   *float64 `json:"abs_offset_s,omitempty"`
	Source       string   `json:"source,omitempty"`
	Flag         string   `json:"flag,omitempty"`
}

// ReadClock runs cmd with only PATH and LC_ALL=C in its environment and
// records its output and the offset parsed from it. A command that fails,
// times out, or prints anything other than one reading gives no reading.
func ReadClock(ctx context.Context, cmd ClockCommand) *ClockRecord {
	rec := &ClockRecord{Command: cmd.Args, Tool: cmd.Tool}
	ctx, cancel := context.WithTimeout(ctx, clockTimeout)
	defer cancel()
	c := exec.CommandContext(ctx, cmd.Args[0], cmd.Args[1:]...)
	c.Env = []string{"PATH=" + os.Getenv("PATH"), "LC_ALL=C"}
	c.WaitDelay = time.Second
	var stdout, stderr cappedBuffer
	c.Stdout, c.Stderr = &stdout, &stderr
	rec.StartUTC = time.Now().UTC()
	err := c.Run()
	rec.EndUTC = time.Now().UTC()
	if c.ProcessState != nil {
		code := c.ProcessState.ExitCode()
		rec.ExitCode = &code
	}
	rec.Stdout, rec.Stderr = bytesOf(stdout.b), bytesOf(stderr.b)
	rec.DroppedBytes = stdout.dropped + stderr.dropped
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		rec.Error = fmt.Sprintf("timed out after %s", clockTimeout)
	case err != nil:
		rec.Error = err.Error()
	case rec.DroppedBytes > 0:
		rec.Error = "output over the size limit"
	default:
		off, src, perr := parseClock(cmd.Tool, stdout.b)
		if perr != nil {
			rec.Error = perr.Error()
		} else {
			rec.AbsOffsetS, rec.Source = &off, src
		}
	}
	if rec.AbsOffsetS == nil {
		rec.Flag = fmt.Sprintf("clock offset: no reading from %s: %s", cmd.Tool, rec.Error)
	} else if *rec.AbsOffsetS > config.PinnedClockOffsetMaxS {
		rec.Flag = fmt.Sprintf("clock offset: %s s from %s exceeds %d s", strconv.FormatFloat(*rec.AbsOffsetS, 'f', -1, 64), cmd.Tool, config.PinnedClockOffsetMaxS)
	}
	return rec
}

// WriteClock writes rec to ClockFile in dir and returns the file's path.
func WriteClock(dir string, rec *ClockRecord) (string, error) {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return "", fmt.Errorf("evidence: clock: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	path := filepath.Join(dir, ClockFile)
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	return path, nil
}

type cappedBuffer struct {
	b       []byte
	dropped int64
}

func (w *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if room := clockOutputLimit - len(w.b); room < len(p) {
		room = max(room, 0)
		w.dropped += int64(len(p) - room)
		p = p[:room]
	}
	w.b = append(w.b, p...)
	return n, nil
}

var (
	sntpLine      = regexp.MustCompile(`^([+-]?[0-9]+\.[0-9]+) \+/- [0-9]+\.[0-9]+ (\S+) (\S+)$`)
	chronySystem  = regexp.MustCompile(`^System time +: ([0-9]+\.[0-9]+) seconds (?:slow|fast) of NTP time$`)
	chronyRef     = regexp.MustCompile(`^Reference ID +: ([0-9A-F]{8}) \((.*)\)$`)
	chronyLeap    = regexp.MustCompile(`^Leap status +: (.*)$`)
	chronyLocalID = "7F7F0101"
)

// parseClock returns the offset magnitude in seconds and the time source
// named in out.
func parseClock(tool string, out []byte) (float64, string, error) {
	text, ok := strings.CutSuffix(string(out), "\n")
	if !ok {
		return 0, "", fmt.Errorf("%s: output does not end in a newline", tool)
	}
	lines := strings.Split(text, "\n")
	var offset, source string
	switch tool {
	case ClockSNTP:
		if len(lines) != 1 {
			return 0, "", fmt.Errorf("sntp: %d output lines, want 1", len(lines))
		}
		m := sntpLine.FindStringSubmatch(lines[0])
		if m == nil {
			return 0, "", errors.New("sntp: output is not an offset line")
		}
		offset, source = m[1], m[2]+" "+m[3]
	case ClockChronyc:
		var system, ref, leap []string
		for _, l := range lines {
			if m := chronySystem.FindStringSubmatch(l); m != nil {
				if system != nil {
					return 0, "", errors.New("chronyc: two System time lines")
				}
				system = m
			}
			if m := chronyRef.FindStringSubmatch(l); m != nil {
				if ref != nil {
					return 0, "", errors.New("chronyc: two Reference ID lines")
				}
				ref = m
			}
			if m := chronyLeap.FindStringSubmatch(l); m != nil {
				if leap != nil {
					return 0, "", errors.New("chronyc: two Leap status lines")
				}
				leap = m
			}
		}
		switch {
		case system == nil || ref == nil || leap == nil:
			return 0, "", errors.New("chronyc: no System time, Reference ID or Leap status line")
		case leap[1] != "Normal" && leap[1] != "Insert second" && leap[1] != "Delete second":
			return 0, "", fmt.Errorf("chronyc: leap status %q", leap[1])
		case ref[1] == chronyLocalID:
			return 0, "", errors.New("chronyc: local reference, no external source")
		}
		offset, source = system[1], ref[1]+" ("+ref[2]+")"
	default:
		return 0, "", fmt.Errorf("%s: no parser", tool)
	}
	v, err := strconv.ParseFloat(offset, 64)
	if err != nil || math.IsInf(v, 0) {
		return 0, "", fmt.Errorf("%s: offset %q", tool, offset)
	}
	return math.Abs(v), source, nil
}
