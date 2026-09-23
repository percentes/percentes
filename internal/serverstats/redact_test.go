package serverstats

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

const urlSecret = "SYNTHETIC_URL_SECRET_a1b2c3"

// A metrics endpoint's query credential appears in no error, whether the
// endpoint refused the connection, returned a status, or served text that
// did not parse.
func TestErrorsOmitEndpointCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	url := "http://127.0.0.1:1/metrics?token=" + urlSecret

	if _, err := fetch(ctx, &http.Client{Timeout: time.Second}, url); err == nil || strings.Contains(err.Error(), urlSecret) {
		t.Errorf("fetch error carries the credential or is nil: %v", err)
	}

	for name, page := range map[string]string{
		"unparseable text with the credential in it": "this is not # metrics text " + urlSecret + "\n",
		"absent gauge": "# HELP other_gauge x\n# TYPE other_gauge gauge\nother_gauge 1\n",
	} {
		_, err := extract([]byte(page), "vllm:num_requests_waiting", url)
		if err == nil {
			t.Errorf("%s: no error", name)
			continue
		}
		if strings.Contains(err.Error(), urlSecret) {
			t.Errorf("%s: error carries the credential: %v", name, err)
		}
		if !strings.Contains(err.Error(), "127.0.0.1:1/metrics") {
			t.Errorf("%s: error lost the endpoint host: %v", name, err)
		}
	}
}
