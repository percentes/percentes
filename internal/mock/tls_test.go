package mock

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A server started with StartTLS serves HTTP/1.1 under a certificate
// that SelfSignedCert's pool verifies, and a client with the system
// roots alone refuses it.
func TestStartTLS(t *testing.T) {
	cert, pool, err := SelfSignedCert("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := SelfSignedCert(); err == nil {
		t.Error("a certificate for no host was made")
	}
	s := New(baseMockCfg())
	if err := s.StartTLS(cert); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	url := "https://" + s.Addr() + "/v1/chat/completions"
	post := func(roots *x509.CertPool) (*http.Response, error) {
		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
		defer tr.CloseIdleConnections()
		return (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Post(url, "application/json", bytes.NewReader(chatBody(2)))
	}
	resp, err := post(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Proto != "HTTP/1.1" || resp.TLS == nil {
		t.Errorf("status %d over %s, TLS %v", resp.StatusCode, resp.Proto, resp.TLS != nil)
	}
	if _, err := post(nil); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("client without the pool: %v, want a certificate error", err)
	}
}

// SetRateLimit refuses with 429 each request that finds the limit's
// requests admitted in the window before it, admits again once the
// window has passed them, counts the refusals under outcome 429, and
// limits nothing once removed.
func TestRateLimit(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT, cfg.ITL = fixed(1), fixed(1)
	s := startServer(t, cfg)
	base := "http://" + s.Addr()
	s.SetRateLimit(2, 300*time.Millisecond)
	var got []int
	for i := 0; i < 3; i++ {
		got = append(got, doStream(t, base, 1).status)
	}
	time.Sleep(350 * time.Millisecond)
	got = append(got, doStream(t, base, 1).status)
	s.SetRateLimit(0, 0)
	for i := 0; i < 3; i++ {
		got = append(got, doStream(t, base, 1).status)
	}
	if want := []int{200, 200, 429, 200, 200, 200, 200}; !equalInts(got, want) {
		t.Errorf("statuses %v, want %v", got, want)
	}
	if m := metricsText(t, base); !strings.Contains(m, `percentes_mock_requests_total{outcome="429"} 1`) {
		t.Errorf("metrics do not count one 429:\n%s", m)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
