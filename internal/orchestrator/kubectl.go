package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// KubectlPodOps implements PodOps with the kubectl command-line tool
// against one named kubeconfig context.
type KubectlPodOps struct {
	Kubectl string // defaults to "kubectl"
	Context string // kubeconfig context; every call names it
}

func (k KubectlPodOps) bin() string {
	if k.Kubectl != "" {
		return k.Kubectl
	}
	return "kubectl"
}

func (k KubectlPodOps) run(ctx context.Context, args ...string) (string, error) {
	if k.Context == "" {
		return "", errors.New("no kubeconfig context named; the current context is not used")
	}
	cmd := exec.CommandContext(ctx, k.bin(), append([]string{"--context", k.Context}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", errors.New(kubectlReason(err, stderr.String()))
	}
	return stdout.String(), nil
}

func (k KubectlPodOps) DeletePodGrace0(ctx context.Context, namespace, pod string) (time.Time, error) {
	if _, err := k.run(ctx, "-n", namespace, "delete", "pod", pod, "--grace-period=0", "--force", "--wait=false"); err != nil {
		return time.Time{}, fmt.Errorf("kubectl delete pod %s/%s: %w", namespace, pod, err)
	}
	return time.Now(), nil
}

// PodExists fails when the named pod is not visible in the context.
func (k KubectlPodOps) PodExists(ctx context.Context, namespace, pod string) error {
	if _, err := k.run(ctx, "-n", namespace, "get", "pod", pod, "-o", "name"); err != nil {
		return fmt.Errorf("kubectl get pod %s/%s: %w", namespace, pod, err)
	}
	return nil
}

// ReadyPods returns the names, sorted, of the selector's pods whose Ready
// condition is True.
func (k KubectlPodOps) ReadyPods(ctx context.Context, namespace, selector string) ([]string, error) {
	out, err := k.run(ctx, "-n", namespace, "get", "pods", "-l", selector, "-o", `jsonpath={range .items[*]}{.metadata.name} {.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}`)
	if err != nil {
		return nil, fmt.Errorf("kubectl get pods -l %s: %w", selector, err)
	}
	var ready []string
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[1] == "True" {
			ready = append(ready, f[0])
		}
	}
	sort.Strings(ready)
	return ready, nil
}

// PodReady reports whether the named pod's Ready condition is True.
func (k KubectlPodOps) PodReady(ctx context.Context, namespace, pod string) (bool, error) {
	out, err := k.run(ctx, "-n", namespace, "get", "pod", pod, "-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
	if err != nil {
		return false, fmt.Errorf("kubectl get pod %s/%s: %w", namespace, pod, err)
	}
	return strings.TrimSpace(out) == "True", nil
}

// kubectlReason names the failure from a fixed set of reasons.
func kubectlReason(err error, stderr string) string {
	s := strings.ToLower(stderr)
	switch {
	case strings.Contains(s, "notfound") || strings.Contains(s, "not found"):
		return err.Error() + " (not found)"
	case strings.Contains(s, "forbidden"):
		return err.Error() + " (forbidden)"
	case strings.Contains(s, "unauthorized"):
		return err.Error() + " (unauthorized)"
	case strings.Contains(s, "context") && strings.Contains(s, "does not exist"):
		return err.Error() + " (unknown context)"
	case strings.Contains(s, "connection refused") || strings.Contains(s, "unable to connect"):
		return err.Error() + " (unreachable)"
	}
	return err.Error()
}
