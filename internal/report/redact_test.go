package report

import (
	"strings"
	"testing"
)

const urlSecret = "SYNTHETIC_URL_SECRET_a1b2c3"

// Credentials carried in the configured endpoints never reach the
// published report, and the endpoint's host and path still do.
func TestReportRedactsEndpointCredentials(t *testing.T) {
	art := minimalArtifacts()
	art.Config.Target.BaseURL = "https://user:" + urlSecret + "@inference.example/v1?token=" + urlSecret
	art.Config.Target.MetricsURLs = []string{"http://metrics.example:9090/metrics?key=" + urlSecret}

	raw, human, err := Generate(art, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"report.json": string(raw), "report.txt": human} {
		if strings.Contains(text, urlSecret) {
			t.Errorf("%s carries the endpoint credential", name)
		}
	}
	if !strings.Contains(string(raw), `"base_url": "https://inference.example/v1"`) {
		t.Errorf("report.json lost the endpoint host and path:\n%s", raw)
	}
	if !strings.Contains(string(raw), `"http://metrics.example:9090/metrics"`) {
		t.Errorf("report.json lost the metrics endpoint")
	}
	// The caller's configuration is not modified by publishing it.
	if !strings.Contains(art.Config.Target.BaseURL, urlSecret) {
		t.Error("Generate altered the caller's configuration")
	}
}

// An endpoint url.Parse rejects is replaced whole, so its credential
// cannot reach the report.
func TestReportReplacesAnUnparseableEndpoint(t *testing.T) {
	art := minimalArtifacts()
	art.Config.Target.BaseURL = "https://user:" + urlSecret + "@inference.example/v1/%zz"
	raw, human, err := Generate(art, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"report.json": string(raw), "report.txt": human} {
		if strings.Contains(text, urlSecret) {
			t.Errorf("%s carries the endpoint credential", name)
		}
	}
}
