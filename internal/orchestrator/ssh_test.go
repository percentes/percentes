package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fakeHost puts ssh, sudo and docker stand-ins first on PATH. ssh records
// its arguments and stdin, then prints ssh-out when that file exists or
// runs the remote command locally; docker records its arguments and prints
// docker-out to stdout and docker-err to stderr.
type fakeHost struct{ dir string }

func newFakeHost(t *testing.T) fakeHost {
	t.Helper()
	dir := t.TempDir()
	stubs := map[string]string{
		"ssh": `#!/bin/sh
D=` + dir + `
printf '%s\n' "$@" > "$D/ssh-args"
cat > "$D/ssh-stdin"
[ -f "$D/ssh-sleep" ] && sleep "$(cat "$D/ssh-sleep")"
if [ -f "$D/ssh-out" ]; then cat "$D/ssh-out"; exit 0; fi
while [ $# -gt 0 ]; do
  case "$1" in -o|-i) shift 2 ;; *) break ;; esac
done
shift
exec sh -c "$*" < "$D/ssh-stdin"
`,
		"sudo": `#!/bin/sh
printf '%s\n' "$@" >> ` + dir + `/sudo-args
[ "$1" = -n ] && shift
exec "$@"
`,
		"docker": `#!/bin/sh
D=` + dir + `
printf '%s\n' "$@" > "$D/docker-args"
[ -f "$D/docker-out" ] && cat "$D/docker-out"
[ -f "$D/docker-err" ] && cat "$D/docker-err" >&2
exit 0
`,
	}
	for name, body := range stubs {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return fakeHost{dir: dir}
}

func (h fakeHost) put(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(h.dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (h fakeHost) read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func (h fakeHost) lines(t *testing.T, name string) []string {
	return strings.Split(strings.TrimSuffix(h.read(t, name), "\n"), "\n")
}

var sshOpts = []string{
	"-o", "BatchMode=yes",
	"-o", "StrictHostKeyChecking=accept-new",
	"-o", "ConnectTimeout=5",
	"-o", "ServerAliveInterval=5",
	"-o", "ServerAliveCountMax=3",
	"-o", "ControlMaster=auto",
}

func TestSSHOpsNamesTargetIdentityAndControlPath(t *testing.T) {
	h := newFakeHost(t)
	h.put(t, "ssh-out", "1790000000100000000\n1790000000100200000\n")
	ops := SSHContainerOps{Target: "ubuntu@10.0.0.5", Identity: "/keys/pk_gpu", ControlPath: "/run/cm-%C"}

	rec, err := ops.KillInit(context.Background(), 4242, "c0ffee", 9)
	if err != nil {
		t.Fatalf("kill: %v", err)
	}
	want := append(append([]string{}, sshOpts...),
		"-o", "ControlPath=/run/cm-%C", "-o", "ControlPersist=1800",
		"-i", "/keys/pk_gpu", "ubuntu@10.0.0.5", "sudo", "-n", "sh", "-s", "--", "4242", "c0ffee", "9")
	if got := h.lines(t, "ssh-args"); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("ssh arguments\n got %q\nwant %q", got, want)
	}
	if got := h.read(t, "ssh-stdin"); got != killScript {
		t.Errorf("kill script on stdin: %q", got)
	}
	if rec.RemoteBeforeNs != 1790000000100000000 || rec.RemoteAfterNs != 1790000000100200000 {
		t.Errorf("remote stamps not parsed: %+v", rec)
	}
	if rec.SentAt.IsZero() || rec.ReturnedAt.Before(rec.SentAt) {
		t.Errorf("local bracket: %+v", rec)
	}

	h.put(t, "ssh-out", "true 4242 c0ffee 2026-10-01T01:59:56.963Z 0001-01-01T00:00:00Z 1 on-failure\n")
	if _, err := (SSHContainerOps{Target: "ubuntu@10.0.0.5"}).Inspect(context.Background(), "vllm"); err != nil {
		t.Fatalf("inspect: %v", err)
	}
	args := h.lines(t, "ssh-args")
	if args[len(sshOpts)+1] != "ControlPath=/tmp/percentes-cm-%C" {
		t.Errorf("default control path: %q", args[len(sshOpts)+1])
	}
	if strings.Contains(strings.Join(args, " "), " -i ") {
		t.Errorf("-i passed with no identity: %q", args)
	}
	if tail := strings.Join(args[len(args)-5:], " "); tail != "ubuntu@10.0.0.5 sh -s -- vllm" {
		t.Errorf("inspect remote command %q", tail)
	}
	if got := h.read(t, "ssh-stdin"); !strings.HasPrefix(got, "sudo -n docker inspect -f ") {
		t.Errorf("$DOCKER not replaced for ssh: %q", got)
	}
	if got := h.read(t, "ssh-stdin"); !strings.Contains(got, " {{.RestartCount}} ") {
		t.Errorf("inspect script lacks {{.RestartCount}}: %q", got)
	}
}

func TestSSHOpsKillFoldsStderrIn(t *testing.T) {
	newFakeHost(t)
	ops := SSHContainerOps{Target: "ubuntu@10.0.0.5"}
	_, err := ops.KillInit(context.Background(), 999999, "c0ffee", 9)
	if err == nil || !strings.Contains(err.Error(), "pid 999999 is not in container c0ffee") {
		t.Fatalf("the guard's refusal did not reach the error: %v", err)
	}
	if _, err := ops.KillInit(context.Background(), 1, "c0ffee", 9); err == nil {
		t.Error("kill of pid 1 was sent")
	}
	if _, err := ops.KillInit(context.Background(), 4242, "", 9); err == nil {
		t.Error("kill with no container id was sent")
	}
}

func TestSSHOpsTimesOut(t *testing.T) {
	h := newFakeHost(t)
	saved := opTimeout
	opTimeout = 300 * time.Millisecond
	t.Cleanup(func() { opTimeout = saved })
	h.put(t, "ssh-sleep", "5")
	h.put(t, "ssh-out", "")

	start := time.Now()
	_, err := SSHContainerOps{Target: "ubuntu@10.0.0.5"}.Inspect(context.Background(), "vllm")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "timed out after 300ms") {
		t.Fatalf("want a timeout error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("inspect returned %v after the timeout", elapsed)
	}
}

func TestLogsKeepStderr(t *testing.T) {
	h := newFakeHost(t)
	h.put(t, "docker-err", "2026-10-01T01:59:56.963000000Z mockserver: serving on :8000\n")
	since := time.Unix(1790000000, 5)

	out, err := SSHContainerOps{Target: "ubuntu@10.0.0.5"}.Logs(context.Background(), "pct-mock", since)
	if err != nil {
		t.Fatalf("logs: %v", err)
	}
	if !strings.Contains(string(out), "mockserver: serving on") {
		t.Fatalf("docker's stderr missing from the logs: %q", out)
	}
	if got := strings.Join(h.lines(t, "docker-args"), " "); got != "logs --timestamps --since 1790000000.000000005 pct-mock" {
		t.Errorf("docker logs arguments %q", got)
	}
	if got := strings.Join(h.lines(t, "sudo-args"), " "); got != "-n docker logs --timestamps --since 1790000000.000000005 pct-mock" {
		t.Errorf("docker not run under sudo -n: %q", got)
	}
}

func TestEventsParse(t *testing.T) {
	h := newFakeHost(t)
	h.put(t, "docker-out", "1790000396763000000 die\n1790000396963000000 start\n")
	since, until := time.Unix(1790000390, 0), time.Unix(1790000400, 0)

	evs, err := SSHContainerOps{Target: "ubuntu@10.0.0.5"}.Events(context.Background(), "vllm", since, until)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(evs) != 2 || evs[0].Action != "die" || evs[1].Action != "start" ||
		evs[0].At.UnixNano() != 1790000396763000000 || evs[1].At.UnixNano() != 1790000396963000000 {
		t.Fatalf("events %+v", evs)
	}
	want := "events --since 1790000390.000000000 --until 1790000400.000000000 --filter container=vllm --filter event=die --filter event=start --format {{.TimeNano}} {{.Action}}"
	if got := strings.Join(h.lines(t, "docker-args"), " "); got != want {
		t.Errorf("docker events arguments\n got %q\nwant %q", got, want)
	}

	if _, err := parseEvents("1790000396763000000 die\nnot a line at all\n"); err == nil {
		t.Error("an unparseable events line was accepted")
	}
}

func TestInspectParse(t *testing.T) {
	st, err := parseInspect("true 4242 c0ffee 2026-10-01T01:59:56.963Z 2026-10-01T01:59:56.763Z 1 on-failure\n")
	if err != nil {
		t.Fatal(err)
	}
	want := ContainerState{
		Running: true, Pid: 4242, ID: "c0ffee",
		StartedAt:    time.Date(2026, 10, 1, 1, 59, 56, 963_000_000, time.UTC),
		FinishedAt:   time.Date(2026, 10, 1, 1, 59, 56, 763_000_000, time.UTC),
		RestartCount: 1, RestartPolicy: "on-failure",
	}
	if st != want {
		t.Errorf("got %+v\nwant %+v", st, want)
	}
	if st, err := parseInspect("false 0 c0ffee 2026-10-01T01:59:56.963Z 0001-01-01T00:00:00Z 0\n"); err != nil || st.RestartPolicy != "" || st.Running {
		t.Errorf("empty policy: %+v, %v", st, err)
	}
	if _, err := parseInspect("true 4242\n"); err == nil {
		t.Error("short inspect output was accepted")
	}
}

func TestLocalOpsRunWithoutSSH(t *testing.T) {
	h := newFakeHost(t)
	h.put(t, "docker-out", "true 4242 c0ffee 2026-10-01T01:59:56.963Z 0001-01-01T00:00:00Z 1 on-failure\n")
	ops := SSHContainerOps{}
	if _, err := ops.Inspect(context.Background(), "pct-mock"); err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "ssh-args")); err == nil {
		t.Error("ssh ran with no target")
	}
	if _, err := os.Stat(filepath.Join(h.dir, "sudo-args")); err == nil {
		t.Error("docker ran under sudo locally")
	}
	off, err := ops.ClockOffset(context.Background())
	if err != nil || off.Samples != 0 || off.OffsetNs != 0 {
		t.Errorf("local offset %+v, %v", off, err)
	}
	if _, err := (SSHContainerOps{KillVia: "nsenter"}).KillInit(context.Background(), 4242, "c0ffee", 0); err == nil {
		t.Error("an unknown kill-via was accepted")
	}
	if _, err := (SSHContainerOps{Target: "ubuntu@10.0.0.5", KillVia: "docker-helper"}).KillInit(context.Background(), 4242, "c0ffee", 0); err == nil {
		t.Error("docker-helper was accepted over ssh")
	}
}

func TestSSHClockOffsetTakesFiveSamples(t *testing.T) {
	h := newFakeHost(t)
	remote := time.Now().Add(time.Hour).UnixNano()
	h.put(t, "ssh-out", strconv.FormatInt(remote, 10)+"\n")
	off, err := SSHContainerOps{Target: "ubuntu@10.0.0.5"}.ClockOffset(context.Background())
	if err != nil {
		t.Fatalf("clock: %v", err)
	}
	if off.Samples != 5 || off.BoundNs <= 0 {
		t.Errorf("offset %+v", off)
	}
	if d := time.Duration(off.OffsetNs) - time.Hour; d > 5*time.Second || d < -5*time.Second {
		t.Errorf("offset %v, want about one hour less the elapsed time", time.Duration(off.OffsetNs))
	}
	if got := h.read(t, "ssh-stdin"); got != clockScript {
		t.Errorf("clock script on stdin: %q", got)
	}
}
