package evidence

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"
)

// commandRun is one run of a host command. err is empty when the command
// exited 0 and nothing was dropped.
type commandRun struct {
	start, end     time.Time
	exitCode       *int
	stdout, stderr []byte
	dropped        int64
	err            string
}

// runCommand runs args with no shell and only PATH and LC_ALL=C in its
// environment, for at most timeout, keeping up to limit bytes of each
// output stream. exitCode is nil when the command did not start, and -1
// when a signal ended it.
func runCommand(ctx context.Context, args []string, timeout time.Duration, limit int) commandRun {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c := exec.CommandContext(ctx, args[0], args[1:]...)
	c.Env = []string{"PATH=" + os.Getenv("PATH"), "LC_ALL=C"}
	c.WaitDelay = time.Second
	stdout, stderr := cappedBuffer{limit: limit}, cappedBuffer{limit: limit}
	c.Stdout, c.Stderr = &stdout, &stderr
	r := commandRun{start: time.Now().UTC()}
	err := c.Run()
	r.end = time.Now().UTC()
	if c.ProcessState != nil {
		code := c.ProcessState.ExitCode()
		r.exitCode = &code
	}
	r.stdout, r.stderr, r.dropped = stdout.b, stderr.b, stdout.dropped+stderr.dropped
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		r.err = fmt.Sprintf("timed out after %s", timeout)
	case err != nil:
		r.err = err.Error()
	case r.dropped > 0:
		r.err = "output over the size limit"
	}
	return r
}

type cappedBuffer struct {
	b       []byte
	limit   int
	dropped int64
}

func (w *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if room := w.limit - len(w.b); room < len(p) {
		room = max(room, 0)
		w.dropped += int64(len(p) - room)
		p = p[:room]
	}
	w.b = append(w.b, p...)
	return n, nil
}
