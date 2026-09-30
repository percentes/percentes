package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// stubKubectl writes a kubectl stand-in that records its arguments and
// exits with the given status after writing a secret-bearing line to stderr.
func stubKubectl(t *testing.T, exit int) (bin, argsFile string) {
	return stubKubectlOut(t, exit, "")
}

// stubKubectlOut is stubKubectl with a fixed stdout.
func stubKubectlOut(t *testing.T, exit int, stdout string) (bin, argsFile string) {
	t.Helper()
	dir := t.TempDir()
	argsFile = filepath.Join(dir, "args")
	bin = filepath.Join(dir, "kubectl")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > " + argsFile + "\n" +
		"printf '%s' '" + stdout + "'\n" +
		"echo 'Error from server (NotFound): SYNTHETIC_KUBE_SECRET_29sep' >&2\n" +
		"exit " + strconv.Itoa(exit) + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argsFile
}

func TestKubectlDeleteNamesTheContextAndKeepsStderrOut(t *testing.T) {
	bin, argsFile := stubKubectl(t, 1)
	ops := KubectlPodOps{Kubectl: bin, Context: "kind-test"}
	_, err := ops.DeletePodGrace0(context.Background(), "percentes", "victim")
	if err == nil {
		t.Fatal("a failing kubectl returned no error")
	}
	if strings.Contains(err.Error(), "SYNTHETIC_KUBE_SECRET") {
		t.Fatalf("kubectl's stderr reached the error: %v", err)
	}
	if !strings.Contains(err.Error(), "(not found)") {
		t.Fatalf("the failure was not classified: %v", err)
	}
	got, readErr := os.ReadFile(argsFile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	args := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(args) < 2 || args[0] != "--context" || args[1] != "kind-test" {
		t.Fatalf("kubectl was not given the context first: %q", args)
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-n percentes delete pod victim --grace-period=0 --force --wait=false") {
		t.Fatalf("unexpected delete arguments: %q", joined)
	}
}

func TestKubectlRefusesTheAmbientContext(t *testing.T) {
	bin, argsFile := stubKubectl(t, 0)
	ops := KubectlPodOps{Kubectl: bin}
	if _, err := ops.DeletePodGrace0(context.Background(), "percentes", "victim"); err == nil {
		t.Fatal("a delete without a context was allowed")
	}
	if err := ops.PodExists(context.Background(), "percentes", "victim"); err == nil {
		t.Fatal("a lookup without a context was allowed")
	}
	if _, err := os.Stat(argsFile); err == nil {
		t.Fatal("kubectl ran without a context")
	}
}

func TestPodExistsClassifiesWithoutStderr(t *testing.T) {
	bin, _ := stubKubectl(t, 0)
	ops := KubectlPodOps{Kubectl: bin, Context: "kind-test"}
	if err := ops.PodExists(context.Background(), "percentes", "victim"); err != nil {
		t.Fatalf("a present pod reported an error: %v", err)
	}
	bin, _ = stubKubectl(t, 1)
	ops = KubectlPodOps{Kubectl: bin, Context: "kind-test"}
	err := ops.PodExists(context.Background(), "percentes", "victim")
	if err == nil || strings.Contains(err.Error(), "SYNTHETIC_KUBE_SECRET") || !strings.Contains(err.Error(), "(not found)") {
		t.Fatalf("lookup error not classified or leaking: %v", err)
	}
}

// ReadyPods keeps the Ready pods in name order; PodReady reads one pod.
func TestReadyPodsAndPodReady(t *testing.T) {
	bin, argsFile := stubKubectlOut(t, 0, "mock-c True\nmock-a True\nmock-b False\n")
	ops := KubectlPodOps{Kubectl: bin, Context: "kind-test"}
	pods, err := ops.ReadyPods(context.Background(), "percentes", "app=percentes-mock")
	if err != nil || len(pods) != 2 || pods[0] != "mock-a" || pods[1] != "mock-c" {
		t.Fatalf("got %v, %v", pods, err)
	}
	got, _ := os.ReadFile(argsFile)
	joined := strings.Join(strings.Split(strings.TrimSpace(string(got)), "\n"), " ")
	if !strings.Contains(joined, "--context kind-test -n percentes get pods -l app=percentes-mock -o jsonpath=") {
		t.Fatalf("unexpected lookup arguments: %q", joined)
	}
	bin, _ = stubKubectlOut(t, 0, "True")
	ops = KubectlPodOps{Kubectl: bin, Context: "kind-test"}
	if ok, err := ops.PodReady(context.Background(), "percentes", "mock-a"); err != nil || !ok {
		t.Fatalf("a Ready pod read as %v, %v", ok, err)
	}
	bin, _ = stubKubectlOut(t, 0, "False")
	ops = KubectlPodOps{Kubectl: bin, Context: "kind-test"}
	if ok, _ := ops.PodReady(context.Background(), "percentes", "mock-a"); ok {
		t.Fatal("an unready pod read as Ready")
	}
	bin, _ = stubKubectlOut(t, 1, "")
	ops = KubectlPodOps{Kubectl: bin, Context: "kind-test"}
	if _, err := ops.ReadyPods(context.Background(), "percentes", "app=percentes-mock"); err == nil || strings.Contains(err.Error(), "SYNTHETIC_KUBE_SECRET") {
		t.Fatalf("lookup error missing or leaking: %v", err)
	}
}
