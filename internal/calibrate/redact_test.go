package calibrate

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

const urlSecret = "SYNTHETIC_URL_SECRET_a1b2c3"

// Both calibration files record the endpoints without their credentials.
func TestOutputRedactsEndpoints(t *testing.T) {
	cfg := &config.Config{}
	cfg.Target.BaseURL = "https://user:" + urlSecret + "@gpu.example:8000?token=" + urlSecret
	o := &Output{
		TargetURL:   "https://user:" + urlSecret + "@gpu.example:8000?token=" + urlSecret,
		MetricsURL:  "http://gpu.example:8000/metrics?key=" + urlSecret,
		QueueGauge:  "vllm:num_requests_waiting",
		StartedWall: time.Unix(0, 0),
		Config:      cfg,
	}
	o.Redact()

	raw, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"calibration.json": string(raw), "calibration.txt": Human(o)} {
		if strings.Contains(text, urlSecret) {
			t.Errorf("%s carries the endpoint credential", name)
		}
	}
	if o.TargetURL != "https://gpu.example:8000" || o.MetricsURL != "http://gpu.example:8000/metrics" {
		t.Errorf("endpoints lost their host: %q %q", o.TargetURL, o.MetricsURL)
	}
	if !strings.Contains(cfg.Target.BaseURL, urlSecret) {
		t.Error("Redact altered the caller's configuration")
	}
}
