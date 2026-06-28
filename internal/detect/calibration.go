package detect

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HealthCalibration is the §5 one-off Phase-1 calibration: it compares
// /health-200 timing against direct first-inference success on the pinned
// vLLM version, so the relationship between "health-ready" and
// "actually-serving" is DOCUMENTED, not assumed. Both probes run against
// the same pod (direct IP), from the same start, on the monotonic clock.
type HealthCalibration struct {
	HealthReadyAfterS    float64 `json:"health_ready_after_s"`
	InferenceReadyAfterS float64 `json:"inference_ready_after_s"`
	// GapS is inference-minus-health: positive means /health goes 200
	// before the pod can actually serve an inference (the boundary must
	// then be treated with care in the decomposition).
	GapS        float64 `json:"gap_s"`
	HealthOK    bool    `json:"health_ok"`
	InferenceOK bool    `json:"inference_ok"`
	Note        string  `json:"note"`
}

// CalibrateHealth polls /health and direct first-inference concurrently
// from the same start and reports when each first succeeds. It is a
// documentation instrument (§5: "the relationship is documented, not
// assumed"), not a gate.
func CalibrateHealth(ctx context.Context, baseURL string, interval time.Duration) (HealthCalibration, error) {
	start := time.Now()
	client := &http.Client{Timeout: 5 * time.Second}

	type result struct {
		at time.Time
		ok bool
	}
	healthCh := make(chan result, 1)
	infCh := make(chan result, 1)

	go func() {
		for {
			if ctx.Err() != nil {
				healthCh <- result{}
				return
			}
			if healthOnce(ctx, client, baseURL) {
				healthCh <- result{at: time.Now(), ok: true}
				return
			}
			select {
			case <-ctx.Done():
				healthCh <- result{}
				return
			case <-time.After(interval):
			}
		}
	}()
	go func() {
		body := `{"model":"cal","messages":[{"role":"user","content":"cal"}],"stream":true,"max_tokens":1,"ignore_eos":true}`
		for {
			if ctx.Err() != nil {
				infCh <- result{}
				return
			}
			if ok, _ := probeOnce(ctx, client, baseURL, body); ok {
				infCh <- result{at: time.Now(), ok: true}
				return
			}
			select {
			case <-ctx.Done():
				infCh <- result{}
				return
			case <-time.After(interval):
			}
		}
	}()

	h := <-healthCh
	inf := <-infCh
	cal := HealthCalibration{HealthOK: h.ok, InferenceOK: inf.ok}
	if h.ok {
		cal.HealthReadyAfterS = h.at.Sub(start).Seconds()
	}
	if inf.ok {
		cal.InferenceReadyAfterS = inf.at.Sub(start).Seconds()
	}
	if h.ok && inf.ok {
		cal.GapS = cal.InferenceReadyAfterS - cal.HealthReadyAfterS
		switch {
		case cal.GapS > 0.5:
			cal.Note = "health-ready precedes serve-ready: readiness on /health overstates availability; decomposition uses first-inference, not /health"
		case cal.GapS < -0.5:
			cal.Note = "serve-ready precedes health-ready: /health lags actual serving"
		default:
			cal.Note = "health-ready and serve-ready coincide within 0.5s on this version"
		}
	} else {
		return cal, fmt.Errorf("calibration incomplete before deadline (health_ok=%v inference_ok=%v)", h.ok, inf.ok)
	}
	return cal, nil
}

func healthOnce(ctx context.Context, client *http.Client, baseURL string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}
