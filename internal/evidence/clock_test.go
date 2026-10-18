package evidence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// sntpOK is macOS 15.5 sntp output for one exchange with time.apple.com,
// captured 6 Oct 2026.
const sntpOK = "+0.005032 +/- 0.010257 time.apple.com 17.253.108.125\n"

// sntpDNSFailure is the start of the same tool's stdout when the server
// name does not resolve (exit status 69); its offset field reads zero.
const sntpDNSFailure = `sntp_exchange {
        result: 1 (DNS lookup failure)
        header: 00 (li:0 vn:0 mode:0)
       stratum: 00 (0)
          poll: 00 (1)
     precision: 00 (1.000000e+00)
         delay: 0000.0000 (0.000000000)
    dispersion: 0000.0000 (0.000000000)
           ref: 00000000 ("    ")
         t_ref: 00000000.00000000 (0.000000000)
            t1: 00000000.00000000 (0.000000000)
            t2: 00000000.00000000 (0.000000000)
            t3: 00000000.00000000 (0.000000000)
            t4: 00000000.00000000 (0.000000000)
        offset: 0000000000000000.0000000000000000 (0.000000000)
         delay: 0000000000000000.0000000000000000 (0.000000000)
          mean: 0000000000000000.0000000000000000 (0.000000000)
         error: 0000000000000000.0000000000000000 (0.000000000)
          addr:
}
`

// chronycTracking is the tracking report in the chronyc 4.6 manual; the
// lines follow print_report's format in its client.c.
func chronycTracking(system, ref, leap string) string {
	return "Reference ID    : " + ref + "\n" +
		"Stratum         : 3\n" +
		"Ref time (UTC)  : Fri Jan 27 09:49:17 2017\n" +
		"System time     : " + system + " of NTP time\n" +
		"Last offset     : -0.000006747 seconds\n" +
		"RMS offset      : 0.000035822 seconds\n" +
		"Frequency       : 3.225 ppm slow\n" +
		"Residual freq   : -0.000 ppm\n" +
		"Skew            : 0.129 ppm\n" +
		"Root delay      : 0.013639022 seconds\n" +
		"Root dispersion : 0.001100737 seconds\n" +
		"Update interval : 64.2 seconds\n" +
		"Leap status     : " + leap + "\n"
}

var chronycOK = chronycTracking("0.000006523 seconds slow", "CB00710F (ntp1.example.net)", "Normal")

// fakeTool writes an executable named tool that prints stdout and stderr
// verbatim and exits with code, and returns its path.
func fakeTool(t *testing.T, tool, stdout, stderr string, code int) string {
	t.Helper()
	return fakeScript(t, tool, func(dir string) string {
		out, errOut := filepath.Join(dir, "stdout"), filepath.Join(dir, "stderr")
		if err := os.WriteFile(out, []byte(stdout), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(errOut, []byte(stderr), 0o600); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("cat '%s'\ncat '%s' >&2\nexit %d\n", out, errOut, code)
	})
}

func fakeScript(t *testing.T, tool string, body func(dir string) string) string {
	t.Helper()
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, tool)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body(dir)), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func readClock(t *testing.T, path string, args ...string) *ClockRecord {
	t.Helper()
	cmd, err := ParseClockCommand(strings.Join(append([]string{path}, args...), " "))
	if err != nil {
		t.Fatal(err)
	}
	return ReadClock(context.Background(), cmd)
}

// A reading keeps the command, its output byte for byte, the offset and
// the source, and does not flag the run.
func TestClockReadingRecorded(t *testing.T) {
	for _, tc := range []struct {
		tool, stdout, source string
		args                 []string
		offset               float64
	}{
		{ClockSNTP, sntpOK, "time.apple.com 17.253.108.125", []string{"-t", "5", "time.apple.com"}, 0.005032},
		{ClockChronyc, chronycOK, "CB00710F (ntp1.example.net)", []string{"tracking"}, 0.000006523},
	} {
		path := fakeTool(t, tc.tool, tc.stdout, "", 0)
		rec := readClock(t, path, tc.args...)
		if rec.AbsOffsetS == nil || *rec.AbsOffsetS != tc.offset {
			t.Fatalf("%s: offset %v, want %g (error %q)", tc.tool, rec.AbsOffsetS, tc.offset, rec.Error)
		}
		if rec.Flag != "" || rec.Error != "" {
			t.Errorf("%s: flag %q, error %q", tc.tool, rec.Flag, rec.Error)
		}
		if rec.Stdout.Text != tc.stdout || rec.Stderr.Text != "" {
			t.Errorf("%s: stdout %q, stderr %q", tc.tool, rec.Stdout.Text, rec.Stderr.Text)
		}
		if want := append([]string{path}, tc.args...); !reflect.DeepEqual(rec.Command, want) || rec.Tool != tc.tool {
			t.Errorf("%s: command %q tool %q", tc.tool, rec.Command, rec.Tool)
		}
		if rec.Source != tc.source {
			t.Errorf("%s: source %q, want %q", tc.tool, rec.Source, tc.source)
		}
		if rec.ExitCode == nil || *rec.ExitCode != 0 {
			t.Errorf("%s: exit code %v", tc.tool, rec.ExitCode)
		}
		if rec.StartUTC.IsZero() || rec.EndUTC.Before(rec.StartUTC) || rec.StartUTC.Location() != time.UTC {
			t.Errorf("%s: start %v end %v", tc.tool, rec.StartUTC, rec.EndUTC)
		}
	}
}

// An offset over 60 s in either direction flags the run; 60 s does not.
func TestClockOffsetOverLimitFlags(t *testing.T) {
	for _, tc := range []struct {
		tool, stdout string
		flagged      bool
	}{
		{ClockSNTP, "+75.250000 +/- 0.010257 time.example 192.0.2.1\n", true},
		{ClockSNTP, "-60.000001 +/- 0.010257 time.example 192.0.2.1\n", true},
		{ClockSNTP, "+60.000000 +/- 0.010257 time.example 192.0.2.1\n", false},
		{ClockSNTP, "-59.999999 +/- 0.010257 time.example 192.0.2.1\n", false},
		{ClockChronyc, chronycTracking("61.000000000 seconds fast", "CB00710F (ntp1.example.net)", "Normal"), true},
		{ClockChronyc, chronycTracking("3600.000000000 seconds slow", "CB00710F (ntp1.example.net)", "Insert second"), true},
		{ClockChronyc, chronycTracking("60.000000000 seconds slow", "CB00710F (ntp1.example.net)", "Normal"), false},
	} {
		args := []string{"time.example"}
		if tc.tool == ClockChronyc {
			args = []string{"tracking"}
		}
		rec := readClock(t, fakeTool(t, tc.tool, tc.stdout, "", 0), args...)
		if rec.AbsOffsetS == nil {
			t.Fatalf("%q: no reading: %s", tc.stdout, rec.Error)
		}
		if got := rec.Flag != ""; got != tc.flagged {
			t.Errorf("%q: flagged %v, want %v (flag %q)", tc.stdout, got, tc.flagged, rec.Flag)
		}
		if tc.flagged && !strings.Contains(rec.Flag, "exceeds 60 s") {
			t.Errorf("%q: flag %q", tc.stdout, rec.Flag)
		}
	}
}

// Anything other than one well-formed reading is no reading, and flags
// the run.
func TestClockNoReadingFlags(t *testing.T) {
	sntpErr := strings.Repeat("sntp: Exchange failed: DNS lookup failure\n", 5) + "sntp: Clock select failed\n"
	cases := []struct {
		name, tool, stdout, stderr string
		code                       int
	}{
		{"sntp failure dump", ClockSNTP, sntpDNSFailure, sntpErr, 69},
		{"sntp dump with exit 0", ClockSNTP, sntpDNSFailure, "", 0},
		{"sntp line with failing exit", ClockSNTP, sntpOK, "", 1},
		{"sntp two lines", ClockSNTP, sntpOK + sntpOK, "", 0},
		{"sntp no newline", ClockSNTP, strings.TrimSuffix(sntpOK, "\n"), "", 0},
		{"sntp empty", ClockSNTP, "", "", 0},
		{"sntp no sign or fraction", ClockSNTP, "5 +/- 0.010257 time.apple.com 17.253.108.125\n", "", 0},
		{"sntp offset out of range", ClockSNTP, "+1" + strings.Repeat("0", 400) + ".0 +/- 0.1 a b\n", "", 0},
		{"chronyc not synchronised", ClockChronyc, chronycTracking("0.000000000 seconds slow", "00000000 ()", "Not synchronised"), "", 0},
		{"chronyc local reference", ClockChronyc, chronycTracking("0.000000000 seconds slow", "7F7F0101 ()", "Normal"), "", 0},
		{"chronyc csv", ClockChronyc, "CB00710F,ntp1.example.net,3,1485510557.0,-0.000006523,-0.000006747\n", "", 0},
		{"chronyc no system time", ClockChronyc, strings.Replace(chronycOK, "System time", "System clock", 1), "", 0},
		{"chronyc two reports", ClockChronyc, chronycOK + chronycOK, "", 0},
		{"chronyc two system times", ClockChronyc, chronycOK + "System time     : 99.000000000 seconds fast of NTP time\n", "", 0},
		{"chronyc two references", ClockChronyc, chronycOK + "Reference ID    : C0000201 (ntp2.example.net)\n", "", 0},
		{"chronyc two leap lines", ClockChronyc, chronycOK + "Leap status     : Normal\n", "", 0},
		{"chronyc daemon down", ClockChronyc, "", "506 Cannot talk to daemon\n", 1},
	}
	for _, tc := range cases {
		args := []string{"time.example"}
		if tc.tool == ClockChronyc {
			args = []string{"tracking"}
		}
		rec := readClock(t, fakeTool(t, tc.tool, tc.stdout, tc.stderr, tc.code), args...)
		if rec.AbsOffsetS != nil {
			t.Errorf("%s: reading %g", tc.name, *rec.AbsOffsetS)
		}
		if rec.Error == "" || !strings.HasPrefix(rec.Flag, "clock offset: no reading from "+tc.tool) {
			t.Errorf("%s: error %q, flag %q", tc.name, rec.Error, rec.Flag)
		}
		if rec.Stdout.Text != tc.stdout || rec.Stderr.Text != tc.stderr {
			t.Errorf("%s: output not kept verbatim", tc.name)
		}
		if rec.ExitCode == nil || *rec.ExitCode != tc.code {
			t.Errorf("%s: exit code %v, want %d", tc.name, rec.ExitCode, tc.code)
		}
	}
}

// A command that does not start, runs past the timeout, or prints past
// the output limit gives no reading.
func TestClockCommandFailures(t *testing.T) {
	rec := ReadClock(context.Background(), ClockCommand{Args: []string{filepath.Join(t.TempDir(), "sntp"), "time.example"}, Tool: ClockSNTP})
	if rec.AbsOffsetS != nil || rec.Flag == "" || rec.ExitCode != nil {
		t.Errorf("missing command: offset %v, flag %q, exit %v", rec.AbsOffsetS, rec.Flag, rec.ExitCode)
	}

	defer func(d time.Duration) { clockTimeout = d }(clockTimeout)
	clockTimeout = 2 * time.Second
	slow := fakeScript(t, ClockSNTP, func(string) string { return "printf '" + strings.TrimSuffix(sntpOK, "\n") + "\\n'\nexec sleep 10\n" })
	start := time.Now()
	rec = readClock(t, slow, "time.example")
	if rec.AbsOffsetS != nil || !strings.Contains(rec.Flag, "timed out") || time.Since(start) > 5*time.Second {
		t.Errorf("timeout: offset %v, flag %q, after %v", rec.AbsOffsetS, rec.Flag, time.Since(start))
	}
	if rec.Stdout.Text != sntpOK {
		t.Errorf("timeout: stdout %q", rec.Stdout.Text)
	}

	// The kept part is one well-formed line.
	head, tail := "+0.005032 +/- 0.010257 ", " 17.253.108.125\n"
	line := head + strings.Repeat("h", clockOutputLimit-len(head)-len(tail)) + tail
	rec = readClock(t, fakeTool(t, ClockSNTP, line+"more\n", "", 0), "time.example")
	if rec.AbsOffsetS != nil || rec.Flag == "" || rec.DroppedBytes != 5 || rec.Stdout.Text != line {
		t.Errorf("over limit: offset %v, flag %q, dropped %d, kept %d", rec.AbsOffsetS, rec.Flag, rec.DroppedBytes, len(rec.Stdout.Text))
	}
}

// The command sees PATH and LC_ALL=C only, so no credential in the
// run's environment reaches the recorded output.
func TestClockCommandEnvironment(t *testing.T) {
	t.Setenv("PERCENTES_CLOCK_TEST_KEY", "clock-secret-0123456789")
	path := fakeScript(t, ClockSNTP, func(string) string { return "env\n" })
	rec := readClock(t, path, "time.example")
	if strings.Contains(rec.Stdout.Text, "clock-secret-0123456789") {
		t.Errorf("environment reached the command: %q", rec.Stdout.Text)
	}
	if !strings.Contains(rec.Stdout.Text, "LC_ALL=C\n") || !strings.Contains(rec.Stdout.Text, "PATH=") {
		t.Errorf("environment %q", rec.Stdout.Text)
	}
}

func TestParseClockCommand(t *testing.T) {
	for _, s := range []string{"sntp time.apple.com", "/usr/bin/sntp -t 5 time.apple.com", "chronyc tracking", "  chronyc -n  tracking "} {
		cmd, err := ParseClockCommand(s)
		if err != nil {
			t.Errorf("%q: %v", s, err)
			continue
		}
		if !reflect.DeepEqual(cmd.Args, strings.Fields(s)) {
			t.Errorf("%q: args %q", s, cmd.Args)
		}
	}
	for _, s := range []string{"", "   ", "sntp", "ntpdate -q time.apple.com", "chronyc -c tracking", "chronyc sources", "chronyc", "sh -c sntp", "timedatectl timesync-status"} {
		if cmd, err := ParseClockCommand(s); err == nil {
			t.Errorf("%q: accepted as %+v", s, cmd)
		}
	}
}

func TestWriteClock(t *testing.T) {
	rec := readClock(t, fakeTool(t, ClockSNTP, "+75.250000 +/- 0.010257 time.example 192.0.2.1\n", "", 0), "time.example")
	dir := filepath.Join(t.TempDir(), "out")
	path, err := WriteClock(dir, rec)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(dir, ClockFile) {
		t.Errorf("path %s", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var back ClockRecord
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&back, rec) {
		t.Errorf("round trip:\n got %+v\nwant %+v", back, *rec)
	}
	for _, field := range []string{`"abs_offset_s": 75.25`, `"flag": "clock offset: 75.25 s from sntp exceeds 60 s"`, `"text": "+75.250000 +/- 0.010257 time.example 192.0.2.1\n"`} {
		if !strings.Contains(string(b), field) {
			t.Errorf("file lacks %s:\n%s", field, b)
		}
	}
}
