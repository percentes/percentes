package mock

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/itsveems/chaosserve/internal/config"
)

func fixed(ms float64) config.LatencyDist {
	return config.LatencyDist{Distribution: "fixed", FixedMs: ms}
}

func baseMockCfg() config.Mock {
	return config.Mock{
		ListenAddr: "127.0.0.1:0",
		Seed:       42,
		TTFT:       fixed(20),
		ITL:        fixed(5),
		// ServeHealthDuringSilentHang deliberately left at its zero value:
		// the default must be the faithful blackhole.
	}
}

func startServer(t *testing.T, cfg config.Mock) *Server {
	t.Helper()
	s := New(cfg)
	if err := s.Start(); err != nil {
		t.Fatalf("start mock server: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func chatBody(maxTokens int) []byte {
	raw, _ := json.Marshal(map[string]any{
		"model":      "chaosserve-mock",
		"messages":   []map[string]string{{"role": "user", "content": "hello"}},
		"stream":     true,
		"max_tokens": maxTokens,
		"ignore_eos": true,
	})
	return raw
}

// streamResult captures one SSE request from the client side.
type streamResult struct {
	status        int
	start         time.Time
	firstByteAt   time.Time
	contentTimes  []time.Time // arrival time of each content chunk
	finishChunks  int
	done          bool
	err           error // terminal transport/stream error, nil on clean [DONE]
	bodyBytesSeen bool
}

func (r *streamResult) ttft() time.Duration { return r.firstByteAt.Sub(r.start) }

func (r *streamResult) total() time.Duration {
	if len(r.contentTimes) == 0 {
		return 0
	}
	return r.contentTimes[len(r.contentTimes)-1].Sub(r.start)
}

func (r *streamResult) maxGap() time.Duration {
	var worst time.Duration
	for i := 1; i < len(r.contentTimes); i++ {
		if g := r.contentTimes[i].Sub(r.contentTimes[i-1]); g > worst {
			worst = g
		}
	}
	return worst
}

// doStream performs one streaming chat request and records chunk timings.
func doStream(t *testing.T, baseURL string, maxTokens int) *streamResult {
	t.Helper()
	res := &streamResult{start: time.Now()}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(chatBody(maxTokens)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Fresh transport per call: no connection reuse across scenarios.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Do(req)
	if err != nil {
		res.err = err
		return res
	}
	defer resp.Body.Close()
	res.status = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return res
	}

	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 && res.firstByteAt.IsZero() {
			res.firstByteAt = time.Now()
			res.bodyBytesSeen = true
		}
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "data: [DONE]"):
			res.done = true
		case strings.HasPrefix(line, "data: "):
			var c chatChunk
			if jerr := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &c); jerr != nil {
				res.err = fmt.Errorf("malformed chunk %q: %w", line, jerr)
				return res
			}
			if len(c.Choices) == 1 && c.Choices[0].FinishReason != nil {
				res.finishChunks++
			} else if _, ok := c.Choices[0].Delta["content"]; ok {
				res.contentTimes = append(res.contentTimes, time.Now())
			}
		}
		if err != nil {
			if err != io.EOF {
				res.err = err
			} else if !res.done {
				res.err = fmt.Errorf("stream ended without [DONE]: %w", io.ErrUnexpectedEOF)
			}
			return res
		}
		if res.done {
			return res
		}
	}
}

func getStatus(t *testing.T, url string, timeout time.Duration) (int, error) {
	t.Helper()
	client := &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	return resp.StatusCode, nil
}

func armFault(t *testing.T, baseURL string, req adminArmRequest) (*FaultRecord, int) {
	t.Helper()
	raw, _ := json.Marshal(req)
	resp, err := http.Post(baseURL+"/admin/faults", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("arm fault: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return nil, resp.StatusCode
	}
	var rec FaultRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode fault record: %v", err)
	}
	return &rec, resp.StatusCode
}

func faultSnapshot(t *testing.T, baseURL string) []FaultRecord {
	t.Helper()
	resp, err := http.Get(baseURL + "/admin/faults")
	if err != nil {
		t.Fatalf("get faults: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Faults []FaultRecord `json:"faults"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode faults: %v", err)
	}
	return out.Faults
}

func metricsText(t *testing.T, baseURL string) string {
	t.Helper()
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func postJSON(t *testing.T, url, body string) (int, error) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	return resp.StatusCode, nil
}

func isReset(err error) bool {
	return err != nil && strings.Contains(err.Error(), "reset")
}
