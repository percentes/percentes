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
