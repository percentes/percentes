package detect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/sse"
)

// Ceiling on one scanned line and on the data fields assembled into one
// event of a probe stream.
const probeEventLimit = 1 << 20

// probeClient opens a new connection per request, so a probe through the
// Service is routed afresh each time instead of staying on the endpoint a
// kept-alive connection first reached.
func probeClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
}

// ProbeRecovery backs the §5 replica-ready and traffic-restored probes.
// It is two-phase to be race-free against the fault fire: phase one waits
// until the fault is VISIBLE on this path (a failed probe, or, when
// requireReplica is set, any response not served by that replica); phase
// two returns the time of the first full success (from requireReplica
// when set). A success before the fault is visible never counts, so a
// probe racing the fire by milliseconds cannot record a bogus ~0 s
// segment. If the fault never becomes visible before ctx expires, the
// segment stays unmeasured and is reported N/A, never inferred.
func ProbeRecovery(ctx context.Context, baseURL string, interval time.Duration, requireReplica string, target ProbeTarget) (recovered time.Time, err error) {
	client := probeClient()
	body := target.body()

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
		ok, replica := probeOnce(ctx, client, baseURL, body, target.APIKey)
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

// probeOnce reports a served inference: status 200, at least one content
// event and a [DONE] terminator, surrounding whitespace ignored, read
// through the instrument's stream path. A payload that does not decode,
// an oversized event or a read error before [DONE] is a failed probe.
func probeOnce(ctx context.Context, client *http.Client, baseURL, body, apiKey string) (bool, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return false, ""
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, ""
	}
	defer resp.Body.Close()
	replica := resp.Header.Get("X-Percentes-Replica")
	if resp.StatusCode != http.StatusOK {
		return false, replica
	}
	var content, done, malformed bool
	_, dropped, err := sse.Events(resp.Body, probeEventLimit, func(payload []byte) bool {
		if string(bytes.TrimSpace(payload)) == "[DONE]" {
			done = true
			return true
		}
		c, ok := loadgen.ContentDelta(payload)
		if !ok {
			malformed = true
			return true
		}
		if c != "" {
			content = true
		}
		return false
	})
	return err == nil && dropped == 0 && !malformed && done && content, replica
}

// ProbeTarget is what a probe requests: the configured model with the
// run's bearer token, and no vLLM-only field on a hosted endpoint.
type ProbeTarget struct {
	Model  string
	APIKey string
	Hosted bool
}

// body is a one-token request for the target, the mock's model when none
// is configured.
func (t ProbeTarget) body() string {
	model := t.Model
	if model == "" {
		model = "percentes-mock"
	}
	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	b, _ := json.Marshal(struct {
		Model     string    `json:"model"`
		Messages  []message `json:"messages"`
		Stream    bool      `json:"stream"`
		MaxTokens int       `json:"max_tokens"`
		IgnoreEOS bool      `json:"ignore_eos,omitempty"`
	}{Model: model, Messages: []message{{Role: "user", Content: "probe"}}, Stream: true, MaxTokens: 1, IgnoreEOS: !t.Hosted})
	return string(b)
}
