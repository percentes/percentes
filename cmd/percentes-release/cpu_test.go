package main

import (
	"bytes"
	"debug/elf"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// makeTarget builds a Makefile target with extra variables.
func makeTarget(t *testing.T, target string, vars ...string) {
	t.Helper()
	makeBin, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not found")
	}
	build := exec.Command(makeBin, append([]string{"-s", "-C", "../..", target}, vars...)...)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("make %s: %v\n%s", target, err, out)
	}
}

// The binary the Makefile's percentes-release target builds measures
// host CPU, as a cell's client-validity gate needs: cgo on darwin, /proc
// on Linux.
func TestReleaseBuildMeasuresCPU(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skipf("no CPU source on %s", runtime.GOOS)
	}
	bin := filepath.Join(t.TempDir(), "percentes-release")
	makeTarget(t, "percentes-release", "RELEASE_BIN="+bin)
	out, err := exec.Command(bin, "check-cpu").Output()
	if err != nil || !bytes.Equal(out, []byte("cpu_measured: true\n")) {
		t.Fatalf("check-cpu: %v, output %q", err, out)
	}
}

// The Linux build is a static x86-64 executable: no interpreter and no
// shared libraries to load.
func TestReleaseLinuxBuildIsStatic(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "percentes-release-linux")
	makeTarget(t, "percentes-release-linux", "RELEASE_LINUX_BIN="+bin)
	f, err := elf.Open(bin)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.Machine != elf.EM_X86_64 || f.Type != elf.ET_EXEC {
		t.Fatalf("machine %v, type %v", f.Machine, f.Type)
	}
	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP || p.Type == elf.PT_DYNAMIC {
			t.Fatalf("program header %v: dynamically linked", p.Type)
		}
	}
	if libs, err := f.ImportedLibraries(); err != nil || len(libs) > 0 {
		t.Fatalf("imported libraries %q, %v", libs, err)
	}
}
