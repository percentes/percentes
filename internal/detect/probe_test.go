package detect

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const probeBody = `{"model":"probe","messages":[{"role":"user","content":"probe"}],"stream":true,"max_tokens":1,"ignore_eos":true}`

func probeServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

// A probe succeeds only on a stream that carried content and ended with
// [DONE], whitespace around it ignored; every other body is a failed probe.
func TestProbeRequiresContentAndDone(t *testing.T) {
	const one = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n"
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"content then [DONE]", 200, one, true},
		{"carriage returns only", 200, strings.ReplaceAll(one, "\n", "\r"), true},
		{"trailing space after [DONE]", 200, strings.Replace(one, "[DONE]\n", "[DONE] \n", 1), true},
		{"[DONE] alone", 200, "data: [DONE]\n\n", false},
		{"[DONE] with trailing bytes", 200, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]garbage\n\n", false},
		{"undecodable payload before [DONE]", 200, "data: broken-json\n\ndata: [DONE]\n\n", false},
		{"content then an undecodable payload", 200, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: broken-json\n\ndata: [DONE]\n\n", false},
		{"empty content delta then [DONE]", 200, "data: {\"choices\":[{\"delta\":{\"content\":\"\"}}]}\n\ndata: [DONE]\n\n", false},
		{"content without [DONE]", 200, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n", false},
		{"status 503", 503, one, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := probeServer(t, c.status, c.body)
			defer srv.Close()
			if got, _ := probeOnce(context.Background(), probeClient(), srv.URL, probeBody); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

// Each probe opens its own connection, so a Service routes each one anew.
func TestProbeOpensAFreshConnectionEachTime(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	client := probeClient()
	for i := 0; i < 3; i++ {
		if ok, _ := probeOnce(context.Background(), client, srv.URL, probeBody); !ok {
			t.Fatalf("probe %d failed", i)
		}
	}
	if n := conns.Load(); n != 3 {
		t.Fatalf("three probes used %d connections, want 3", n)
	}
}
