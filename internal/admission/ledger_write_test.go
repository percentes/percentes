package admission

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// An append the file-size limit cuts short leaves the ledger as it was.
func TestFailedAppendKeepsPriorLedger(t *testing.T) {
	if path := os.Getenv("PERCENTES_TEST_CAPPED_LEDGER"); path != "" {
		l, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		var lim syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &lim); err != nil {
			t.Fatal(err)
		}
		lim.Cur = uint64(fi.Size() + 8)
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &lim); err != nil {
			t.Fatal(err)
		}
		signal.Ignore(syscall.SIGXFSZ)
		if err := l.RecordHosting("1", "allocation test"); err == nil {
			t.Fatal("capped append succeeded")
		}
		return
	}
	path := filepath.Join(t.TempDir(), "release.jsonl")
	if _, err := Create(path, PilotCaps()); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFailedAppendKeepsPriorLedger$")
	cmd.Env = append(os.Environ(), "PERCENTES_TEST_CAPPED_LEDGER="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v: %s", err, out)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err != nil || string(after) != string(before) {
		t.Fatalf("failed append left %d extra bytes; reopen: %v", len(after)-len(before), err)
	}
}

// A route's lock is held by one opener at a time, through a symlink to
// the ledger too, and a lock file the process cannot write still serves.
func TestLockRoute(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "release.jsonl")
	l, err := Create(path, PilotCaps())
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "ledger.jsonl")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	viaLink, err := Open(link)
	if err != nil {
		t.Fatal(err)
	}
	f, err := l.LockRoute("groq")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := viaLink.LockRoute("groq"); err == nil || !strings.Contains(err.Error(), "another process holds it") {
		t.Fatalf("second lock: %v", err)
	}
	other, err := viaLink.LockRoute("gemini")
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
	f.Close()
	lock := path + ".groq.lock"
	if err := os.Chmod(lock, 0o444); err != nil {
		t.Fatal(err)
	}
	f, err = viaLink.LockRoute("groq")
	if err != nil {
		t.Fatalf("read-only lock file: %v", err)
	}
	f.Close()
	for _, bad := range []string{"", "../groq", "a/b"} {
		if _, err := l.LockRoute(bad); err == nil {
			t.Errorf("route %q locked", bad)
		}
	}
}

// LockRoute leaves the lock file it creates readable by other users
// whatever the umask.
func TestLockRouteFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release.jsonl")
	l, err := Create(path, PilotCaps())
	if err != nil {
		t.Fatal(err)
	}
	old := syscall.Umask(0o077)
	f, err := l.LockRoute("groq")
	syscall.Umask(old)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := os.Stat(path + ".groq.lock")
	if err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("lock file mode %v: %v", fi.Mode(), err)
	}
}
