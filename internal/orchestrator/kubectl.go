package orchestrator

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// KubectlPodOps implements PodOps with the kubectl CLI (the orchestrator
// is agnostic beyond timestamps, §2; kubectl is a legitimate injector
// mechanism for Phase 1). It is exercised against kind for the clean-
// delete variant; the fake covers the lifecycle logic in unit tests.
type KubectlPodOps struct {
	Kubectl string // defaults to "kubectl"
}

func (k KubectlPodOps) bin() string {
	if k.Kubectl != "" {
		return k.Kubectl
	}
	return "kubectl"
}

func (k KubectlPodOps) DeletePodGrace0(ctx context.Context, namespace, pod string) (time.Time, error) {
	cmd := exec.CommandContext(ctx, k.bin(), "-n", namespace, "delete", "pod", pod,
		"--grace-period=0", "--force", "--wait=false")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return time.Time{}, fmt.Errorf("kubectl delete pod %s/%s: %w: %s", namespace, pod, err, strings.TrimSpace(stderr.String()))
	}
	return time.Now(), nil
}
