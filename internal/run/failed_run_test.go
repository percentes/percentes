package run_test

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/run"
)

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

// A hosted run that fails the protocol check returns its artifacts with
// every scheduled request, marked invalid with the error as a reason.
func TestExecuteKeepsArtifactsOfAFailedRun(t *testing.T) {
	cfg := &config.Config{Target: config.Target{BaseURL: http10Server(t), Hosted: true}}
	cfg.Load.RateRPS = 10
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Load.Connections = 2
	cfg.Load.MaxTokens = 8
	cfg.Run.Phases = config.Phases{BaselineS: 0.3}

	art, err := run.Execute(context.Background(), cfg, run.Options{})
	if err == nil || !strings.Contains(err.Error(), "HTTP/1.1") {
		t.Fatalf("want a protocol failure, got %v", err)
	}
	if art == nil || art.Loadgen == nil {
		t.Fatalf("no artifacts with the error: %+v", art)
	}
	if n := len(art.Loadgen.Requests); n != 3 {
		t.Fatalf("%d requests kept, want 3", n)
	}
	if art.RunValid || len(art.InvalidReasons) != 1 || art.InvalidReasons[0] != err.Error() {
		t.Fatalf("valid=%v reasons %q, want the error %q", art.RunValid, art.InvalidReasons, err)
	}
}
