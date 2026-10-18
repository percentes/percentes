package loadgen

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// h2Server is a TLS server that offers h2 in its handshake.
func h2Server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")) //nolint:errcheck
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// trustingTransport is the run transport, trusting srv's certificate.
// Protocol defaults are fixed on first use, before the root pool is
// added, so they match the transport Run builds.
func trustingTransport(t *testing.T, srv *httptest.Server) *http.Transport {
	t.Helper()
	tr := NewTransport(1)
	if _, err := tr.RoundTrip(&http.Request{}); err == nil {
		t.Fatal("a request with no URL must fail")
	}
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.RootCAs = pool
	t.Cleanup(tr.CloseIdleConnections)
	return tr
}

// The run transport speaks HTTP/1.1 to a TLS server that offers h2.
func TestTransportPinsHTTP11OverTLS(t *testing.T) {
	srv := h2Server(t)

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Proto != "HTTP/2.0" {
		t.Fatalf("server must offer h2 for this test to mean anything, got %s", resp.Proto)
	}

	resp, err = (&http.Client{Transport: trustingTransport(t, srv)}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Proto != "HTTP/1.1" {
		t.Fatalf("run transport negotiated %s, want HTTP/1.1", resp.Proto)
	}
}

// Each request records the protocol its response arrived over.
func TestExecuteRecordsProto(t *testing.T) {
	srv := h2Server(t)
	for _, c := range []struct {
		rt   http.RoundTripper
		want string
	}{
		{trustingTransport(t, srv), "HTTP/1.1"},
		{srv.Client().Transport, "HTTP/2.0"},
	} {
		cfg := &config.Config{Target: config.Target{BaseURL: srv.URL, Hosted: true}}
		cfg.Load.MaxTokens = 256
		g := &gen{cfg: cfg, client: &http.Client{Transport: c.rt}, epoch: time.Now(), filler: "xyz", model: "m"}
		r := &Request{Index: 1}
		g.execute(r)
		if r.Outcome != OutcomeCompleted || r.Proto != c.want {
			t.Errorf("want completed over %s, got %v/%q over %q", c.want, r.Outcome, r.ErrClass, r.Proto)
		}
	}
}

func TestCheckProtocols(t *testing.T) {
	hosted := &config.Config{Target: config.Target{Hosted: true}}
	for _, c := range []struct {
		cfg    *config.Config
		protos []string
		fail   bool
	}{
		{hosted, []string{"HTTP/1.1", "", "HTTP/1.1"}, false},
		{hosted, []string{"HTTP/1.1", "HTTP/2.0"}, true},
		{hosted, []string{"HTTP/1.0"}, true},
		{&config.Config{}, []string{"HTTP/2.0"}, false},
	} {
		reqs := make([]Request, len(c.protos))
		for i, p := range c.protos {
			reqs[i] = Request{Index: int64(i), Proto: p}
		}
		if err := checkProtocols(c.cfg, reqs); (err != nil) != c.fail {
			t.Errorf("hosted=%v protos=%q: err=%v, want failure %v", c.cfg.Target.Hosted, c.protos, err, c.fail)
		}
	}
}

// http10Server answers every request with an HTTP/1.0 stream.
func http10Server(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return
				}
				conn.Write([]byte("HTTP/1.0 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" + //nolint:errcheck
					"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))
			}(conn)
		}
	}()
	return "http://" + ln.Addr().String()
}

// A hosted run with a response over another protocol fails; the same
// run against a self-hosted target does not.
func TestRunFailsOnHostedProtocolMismatch(t *testing.T) {
	url := http10Server(t)
	for _, hosted := range []bool{true, false} {
		cfg := &config.Config{Target: config.Target{BaseURL: url, Hosted: hosted}}
		cfg.Load.RateRPS = 10
		cfg.Load.ArrivalProcess = "deterministic"
		cfg.Load.Connections = 2
		cfg.Load.MaxTokens = 8
		cfg.Run.Phases = config.Phases{BaselineS: 0.3}

		res, err := Run(context.Background(), cfg, nil)
		if res == nil {
			t.Fatalf("hosted=%v: no result: %v", hosted, err)
		}
		for i := range res.Requests {
			if r := &res.Requests[i]; r.Proto != "HTTP/1.0" {
				t.Fatalf("hosted=%v: request %d recorded %q over %v/%q", hosted, r.Index, r.Proto, r.Outcome, r.ErrClass)
			}
		}
		if hosted && (err == nil || !strings.Contains(err.Error(), "HTTP/1.1")) {
			t.Errorf("hosted run over HTTP/1.0 must fail, got %v", err)
		}
		if !hosted && err != nil {
			t.Errorf("self-hosted run must not fail on protocol: %v", err)
		}
	}
}
