package admission

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
