package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/redact"
)

// SSHContainerOps runs fixed scripts over one ControlMaster connection, or
// locally when Target is empty.
type SSHContainerOps struct {
	Target      string // user@host; "" runs the scripts locally
	Identity    string // ssh -i; "" uses ssh's defaults
	ControlPath string // "" means /tmp/percentes-cm-%C
	KillVia     string // "sudo" (default) | "docker-helper"
	SSH         string // defaults to "ssh"
}

// Per-call bounds; tests shorten them.
var (
	opTimeout   = ContainerOpTimeout
	logsTimeout = ContainerLogsTimeout
)

// clockSamples is the number of date round trips behind one ClockOffset.
const clockSamples = 5

// maxStderr caps the stderr folded into an error.
const maxStderr = 512

// The scripts reference $DOCKER, replaced before they are sent.
const (
	inspectScript = `$DOCKER inspect -f '{{.State.Running}} {{.State.Pid}} {{.Id}} {{.State.StartedAt}} {{.State.FinishedAt}} {{.RestartCount}} {{.HostConfig.RestartPolicy.Name}}' "$1"
`
	killScript = `pid=$1 cid=$2 sig=$3; grep -q "$cid" "/proc/$pid/cgroup" || { echo "pid $pid is not in container $cid" >&2; exit 3; }; date +%s%N; kill -"$sig" "$pid"; date +%s%N
`
	logsScript = `$DOCKER logs --timestamps --since "$1" "$2" 2>&1
`
	eventsScript = `$DOCKER events --since "$1" --until "$2" --filter "container=$3" --filter event=die --filter event=start --format '{{.TimeNano}} {{.Action}}'
`
	clockScript = `date +%s%N
`
	// fingerprintScript is the body of deploy/phase1/fingerprint.sh written
	// to stdout, with the container as $1.
	fingerprintScript = `set -eu
CONTAINER=${1:-}
GPU_FIELDS=name,uuid,driver_version,vbios_version,persistence_mode,pstate,clocks.sm,clocks.mem,clocks.max.sm,clocks.max.mem,power.limit,power.max_limit,temperature.gpu,clocks_throttle_reasons.active
printf '# fingerprint %s %s\n' "$(hostname)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '== nvidia-smi --query-gpu=%s ==\n' "$GPU_FIELDS"
nvidia-smi --query-gpu="$GPU_FIELDS" --format=csv
printf '== lscpu ==\n'
lscpu
printf '== nproc ==\n'
nproc
printf '== uname -a ==\n'
uname -a
printf '== /proc/meminfo, first three lines ==\n'
head -n 3 /proc/meminfo
if [ -n "$CONTAINER" ]; then
  printf '== container %s ==\n' "$CONTAINER"
  $DOCKER inspect --format 'image {{.Config.Image}}' "$CONTAINER"
  $DOCKER inspect --format 'started {{.State.StartedAt}}' "$CONTAINER"
  $DOCKER image inspect --format 'id {{.Id}}{{"\n"}}digest {{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}none{{end}}' "$($DOCKER inspect --format '{{.Image}}' "$CONTAINER")"
  $DOCKER version --format 'docker server {{.Server.Version}}'
  $DOCKER inspect --format 'mounts {{json .Mounts}}' "$CONTAINER"
fi
`
)

func (s SSHContainerOps) bin() string {
	if s.SSH != "" {
		return s.SSH
	}
	return "ssh"
}

func (s SSHContainerOps) controlPath() string {
	if s.ControlPath != "" {
		return s.ControlPath
	}
	return "/tmp/percentes-cm-%C"
}

func (s SSHContainerOps) docker() string {
	if s.Target == "" {
		return "docker"
	}
	return "sudo -n docker"
}

// command builds the process for one script: kill selects the privileged
// form, args follow "--".
func (s SSHContainerOps) command(ctx context.Context, kill bool, args []string) (*exec.Cmd, error) {
	if s.Target == "" {
		switch {
		case !kill:
			return exec.CommandContext(ctx, "sh", append([]string{"-s", "--"}, args...)...), nil
		case s.KillVia == "" || s.KillVia == "sudo":
			return exec.CommandContext(ctx, "sudo", append([]string{"-n", "sh", "-s", "--"}, args...)...), nil
		case s.KillVia == "docker-helper":
			return exec.CommandContext(ctx, "docker", append([]string{"run", "--rm", "-i", "--pid=host", "--privileged", "busybox", "sh", "-s", "--"}, args...)...), nil
		}
		return nil, fmt.Errorf("kill-via %q: want sudo or docker-helper", s.KillVia)
	}
	if kill && s.KillVia != "" && s.KillVia != "sudo" {
		return nil, fmt.Errorf("kill-via %q applies only without an ssh target", s.KillVia)
	}
	argv := []string{
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=accept-new",
		"-o", "ConnectTimeout=5",
		"-o", "ServerAliveInterval=5",
		"-o", "ServerAliveCountMax=3",
		"-o", "ControlMaster=auto",
		"-o", "ControlPath=" + s.controlPath(),
		"-o", "ControlPersist=1800",
	}
	if s.Identity != "" {
		argv = append(argv, "-i", s.Identity)
	}
	argv = append(argv, s.Target)
	if kill {
		argv = append(argv, "sudo", "-n")
	}
	argv = append(argv, "sh", "-s", "--")
	for _, a := range args {
		argv = append(argv, shellQuote(a))
	}
	return exec.CommandContext(ctx, s.bin(), argv...), nil
}

// run sends script on stdin under its own timeout and returns stdout and
// stderr. The error names op, the exit status and, unless keepStderr is
// false, the redacted stderr.
func (s SSHContainerOps) run(ctx context.Context, op string, timeout time.Duration, kill, keepStderr bool, script string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd, err := s.command(ctx, kill, args)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	cmd.Stdin = strings.NewReader(strings.ReplaceAll(script, "$DOCKER", s.docker()))
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if err == nil {
		return stdout.Bytes(), nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return stdout.Bytes(), fmt.Errorf("%s: timed out after %s: %w", op, timeout, context.DeadlineExceeded)
	}
	if ctx.Err() != nil {
		return stdout.Bytes(), fmt.Errorf("%s: %w", op, ctx.Err())
	}
	msg := err.Error()
	if keepStderr {
		if e := strings.TrimSpace(stderr.String()); e != "" {
			if len(e) > maxStderr {
				e = e[len(e)-maxStderr:]
			}
			msg += ": " + redact.Scrub(e)
		}
	}
	return stdout.Bytes(), fmt.Errorf("%s: %s", op, msg)
}

func (s SSHContainerOps) Inspect(ctx context.Context, container string) (ContainerState, error) {
	out, err := s.run(ctx, "docker inspect "+container, opTimeout, false, true, inspectScript, container)
	if err != nil {
		return ContainerState{}, err
	}
	return parseInspect(string(out))
}

// parseInspect reads the inspect script's one line; an empty restart policy
// leaves six fields.
func parseInspect(out string) (ContainerState, error) {
	f := strings.Fields(out)
	if len(f) != 6 && len(f) != 7 {
		return ContainerState{}, fmt.Errorf("docker inspect: %d fields, want 7", len(f))
	}
	var st ContainerState
	var err error
	if st.Running, err = strconv.ParseBool(f[0]); err != nil {
		return ContainerState{}, fmt.Errorf("docker inspect: running %q", f[0])
	}
	if st.Pid, err = strconv.Atoi(f[1]); err != nil {
		return ContainerState{}, fmt.Errorf("docker inspect: pid %q", f[1])
	}
	st.ID = f[2]
	if st.StartedAt, err = time.Parse(time.RFC3339Nano, f[3]); err != nil {
		return ContainerState{}, fmt.Errorf("docker inspect: started_at %q", f[3])
	}
	if st.FinishedAt, err = time.Parse(time.RFC3339Nano, f[4]); err != nil {
		return ContainerState{}, fmt.Errorf("docker inspect: finished_at %q", f[4])
	}
	if st.RestartCount, err = strconv.Atoi(f[5]); err != nil {
		return ContainerState{}, fmt.Errorf("docker inspect: restart_count %q", f[5])
	}
	if len(f) == 7 {
		st.RestartPolicy = f[6]
	}
	return st, nil
}

// KillInit sends signal to pid after checking containerID in /proc/pid/cgroup.
func (s SSHContainerOps) KillInit(ctx context.Context, pid int, containerID string, signal int) (KillRecord, error) {
	rec := KillRecord{Pid: pid, Signal: signal}
	if pid <= 1 || containerID == "" {
		return rec, fmt.Errorf("kill: refusing pid %d with container id %q", pid, containerID)
	}
	rec.SentAt = time.Now()
	out, err := s.run(ctx, "kill", opTimeout, true, true, killScript, strconv.Itoa(pid), containerID, strconv.Itoa(signal))
	rec.ReturnedAt = time.Now()
	if err != nil {
		return rec, err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 2 {
		rec.RemoteBeforeNs = parseStamp(lines[0])
		rec.RemoteAfterNs = parseStamp(lines[1])
	}
	return rec, nil
}

// parseStamp reads one date +%s%N line, 0 when it does not parse.
func parseStamp(s string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// Logs returns stdout of the logs script, which carries both of docker's
// streams; only the exit status is an error.
func (s SSHContainerOps) Logs(ctx context.Context, container string, since time.Time) ([]byte, error) {
	return s.run(ctx, "docker logs "+container, logsTimeout, false, false, logsScript, dockerTime(since), container)
}

func (s SSHContainerOps) Events(ctx context.Context, container string, since, until time.Time) ([]ContainerEvent, error) {
	out, err := s.run(ctx, "docker events "+container, opTimeout, false, true, eventsScript, dockerTime(since), dockerTime(until), container)
	if err != nil {
		return nil, err
	}
	return parseEvents(string(out))
}

// parseEvents reads "TimeNano Action" lines.
func parseEvents(out string) ([]ContainerEvent, error) {
	var evs []ContainerEvent
	for i, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		var n int64
		var err error
		if len(f) == 2 {
			n, err = strconv.ParseInt(f[0], 10, 64)
		}
		if len(f) != 2 || err != nil {
			return nil, fmt.Errorf("docker events: line %d does not parse", i+1)
		}
		evs = append(evs, ContainerEvent{Action: f[1], At: time.Unix(0, n)})
	}
	return evs, nil
}

// ClockOffset times clockSamples date round trips; locally it is zero with
// Samples 0.
func (s SSHContainerOps) ClockOffset(ctx context.Context) (ClockOffset, error) {
	if s.Target == "" {
		return ClockOffset{At: time.Now()}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	samples := make([]clockSample, 0, clockSamples)
	for i := 0; i < clockSamples; i++ {
		sent := time.Now()
		out, err := s.run(ctx, "clock", opTimeout, false, true, clockScript)
		returned := time.Now()
		if err != nil {
			return ClockOffset{}, err
		}
		remote := parseStamp(string(out))
		if remote == 0 {
			return ClockOffset{}, fmt.Errorf("clock: sample %d does not parse", i+1)
		}
		samples = append(samples, clockSample{sent: sent, returned: returned, remoteNs: remote})
	}
	return offsetFrom(samples), nil
}

// clockSample is one round trip: local send and return, remote stamp.
type clockSample struct {
	sent, returned time.Time
	remoteNs       int64
}

// offsetFrom takes the median of remote minus the local midpoint, bounded
// by the largest half round trip.
func offsetFrom(samples []clockSample) ClockOffset {
	if len(samples) == 0 {
		return ClockOffset{At: time.Now()}
	}
	offs := make([]int64, len(samples))
	var bound int64
	for i, c := range samples {
		half := c.returned.Sub(c.sent) / 2
		offs[i] = c.remoteNs - c.sent.Add(half).UnixNano()
		bound = max(bound, int64(half))
	}
	sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
	mid := len(offs) / 2
	med := offs[mid]
	if len(offs)%2 == 0 {
		med = (offs[mid-1] + offs[mid]) / 2
	}
	return ClockOffset{OffsetNs: med, BoundNs: bound, Samples: len(samples), At: samples[len(samples)-1].returned}
}

func (s SSHContainerOps) Fingerprint(ctx context.Context, container string) (string, error) {
	out, err := s.run(ctx, "fingerprint "+container, opTimeout, false, true, fingerprintScript, container)
	return string(out), err
}

// dockerTime formats t as the Unix seconds docker's --since and --until read.
func dockerTime(t time.Time) string {
	return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond())
}

var shellSafe = regexp.MustCompile(`^[A-Za-z0-9_.:/=@%+,-]+$`)

// shellQuote quotes a for the remote shell that ssh hands the command to.
func shellQuote(a string) string {
	if shellSafe.MatchString(a) {
		return a
	}
	return "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
}
