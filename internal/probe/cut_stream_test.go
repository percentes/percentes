package probe

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// A stream a server resets over TLS leaves a record that names the peer and
// no address of this client.
func TestCutStreamRecordOmitsClientAddress(t *testing.T) {
	noSpacing(t)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(answerFrame)) //nolint:errcheck
		w.(http.Flusher).Flush()
		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		if nc, ok := c.(interface{ NetConn() net.Conn }); ok {
			nc.NetConn().(*net.TCPConn).SetLinger(0) //nolint:errcheck
			nc.NetConn().Close()
		}
	}))
	srv.EnableHTTP2 = false
	srv.StartTLS()
	defer srv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	rec, err := RunRelease(context.Background(), ReleaseInput{Config: cfg, Key: newKey(t), Texts: texts(0), ByteCap: byteCap, LowestStepRPS: testStepRPS, RootCAs: pool})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(rec); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "connection reset by peer") {
		t.Fatalf("no reset on record: %s", b.String())
	}
	if m := regexp.MustCompile(`[^"]*->[^"]*`).FindString(b.String()); m != "" {
		t.Errorf("record holds a source address: %s", m)
	}
}
