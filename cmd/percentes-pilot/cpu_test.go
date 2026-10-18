package main

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// The binary the Makefile's percentes-pilot target builds measures host
// CPU, as a session's client-validity gate needs: cgo on darwin, /proc on
// Linux.
func TestPilotBuildMeasuresCPU(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skipf("no CPU source on %s", runtime.GOOS)
	}
	makeBin, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not found")
	}
	bin := filepath.Join(t.TempDir(), "percentes-pilot")
	build := exec.Command(makeBin, "-s", "-C", "../..", "percentes-pilot", "PILOT_BIN="+bin)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("make percentes-pilot: %v\n%s", err, out)
	}
	out, err := exec.Command(bin, "check-cpu").Output()
	if err != nil || !bytes.Equal(out, []byte("cpu_measured: true\n")) {
		t.Fatalf("check-cpu: %v, output %q", err, out)
	}
}
