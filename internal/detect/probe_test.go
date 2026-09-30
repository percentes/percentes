package detect

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
			if got, _ := probeOnce(context.Background(), probeClient(), srv.URL, probeBody, ""); got != c.want {
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
		if ok, _ := probeOnce(context.Background(), client, srv.URL, probeBody, ""); !ok {
			t.Fatalf("probe %d failed", i)
		}
	}
	if n := conns.Load(); n != 3 {
		t.Fatalf("three probes used %d connections, want 3", n)
	}
}

// A probe asks for the configured model with the run's bearer token, and a
// hosted target's body carries no ignore_eos.
func TestProbeSendsTheConfiguredModelAndKey(t *testing.T) {
	const one = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n"
	var seen struct {
		model, auth string
		ignoreEOS   bool
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model     string `json:"model"`
			IgnoreEOS *bool  `json:"ignore_eos"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		seen.model, seen.auth, seen.ignoreEOS = req.Model, r.Header.Get("Authorization"), req.IgnoreEOS != nil
		if req.Model != "served-model" || r.Header.Get("Authorization") != "Bearer synthetic-key-29sep" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(one))
	}))
	defer srv.Close()
	if ok, _ := probeOnce(context.Background(), probeClient(), srv.URL, ProbeTarget{}.body(), ""); ok {
		t.Fatal("the default probe body was served by a target that requires its own model")
	}
	target := ProbeTarget{Model: "served-model", APIKey: "synthetic-key-29sep"}
	if ok, _ := probeOnce(context.Background(), probeClient(), srv.URL, target.body(), target.APIKey); !ok {
		t.Fatalf("the configured probe was refused: model=%q auth=%q", seen.model, seen.auth)
	}
	if !seen.ignoreEOS {
		t.Fatal("a self-hosted probe body must carry ignore_eos")
	}
	hosted := ProbeTarget{Model: "served-model", APIKey: "synthetic-key-29sep", Hosted: true}
	_, _ = probeOnce(context.Background(), probeClient(), srv.URL, hosted.body(), hosted.APIKey)
	if seen.ignoreEOS {
		t.Fatal("a hosted probe body must omit ignore_eos")
	}
}

// A note never replaces a measured segment.
func TestSetNoteLeavesMeasuredSegments(t *testing.T) {
	d := NewPhase0Decomposition()
	at := time.Now()
	d.SetMeasured("replica_ready", at, at.Add(time.Second))
	d.SetNote("replica_ready", "unmeasured: late")
	d.SetNote("traffic_restored", "unmeasured: late")
	for _, s := range d.Segments {
		if s.Name == "replica_ready" && (!s.Measured || s.Note == "unmeasured: late") {
			t.Fatalf("measured segment overwritten: %+v", s)
		}
		if s.Name == "traffic_restored" && s.Note != "unmeasured: late" {
			t.Fatalf("note not recorded: %+v", s)
		}
	}
}
