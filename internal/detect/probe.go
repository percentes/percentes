package detect

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ProbeRecovery backs the §5 replica-ready and traffic-restored probes.
// It is two-phase to be race-free against the fault fire: phase one waits
// until the fault is VISIBLE on this path (a failed probe, or — when
// requireReplica is set — any response not served by that replica); phase
// two returns the time of the first full success (from requireReplica
// when set). A success before the fault is visible never counts, so a
// probe racing the fire by milliseconds cannot record a bogus ~0 s
// segment. If the fault never becomes visible before ctx expires, the
// segment stays unmeasured and is reported N/A — never inferred.
func ProbeRecovery(ctx context.Context, baseURL string, interval time.Duration, requireReplica string) (recovered time.Time, err error) {
	client := &http.Client{Timeout: 5 * time.Second}
	body := `{"model":"probe","messages":[{"role":"user","content":"probe"}],"stream":true,"max_tokens":1,"ignore_eos":true}`

	faultSeen := false
	for {
		select {
		case <-ctx.Done():
			if !faultSeen {
				return time.Time{}, fmt.Errorf("probe: fault never visible on this path: %w", ctx.Err())
			}
			return time.Time{}, fmt.Errorf("probe: no recovery before deadline: %w", ctx.Err())
		default:
		}
		ok, replica := probeOnce(ctx, client, baseURL, body)
		success := ok && (requireReplica == "" || replica == requireReplica)
		if !faultSeen {
			if !success {
				faultSeen = true
			}
		} else if success {
			return time.Now(), nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
}

func probeOnce(ctx context.Context, client *http.Client, baseURL, body string) (bool, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return false, ""
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return false, ""
	}
	defer resp.Body.Close()
	replica := resp.Header.Get("X-Percentes-Replica")
	if resp.StatusCode != http.StatusOK {
		return false, replica
	}
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if strings.HasPrefix(strings.TrimSpace(line), "data: [DONE]") {
			return true, replica
		}
		if err != nil {
			return false, replica
		}
	}
}
