package orchestrator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const urlSecret = "SYNTHETIC_URL_SECRET_a1b2c3"

// Arming errors name neither the admin endpoint's credentials nor any
// bytes the admin endpoint returned, and a redirect is reported as its
// status rather than followed.
func TestArmErrorsOmitAdminCredentialsAndResponseBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	t.Run("refused connection", func(t *testing.T) {
		m := NewMockInjector("http://127.0.0.1:1/?token="+urlSecret, "stall", 0)
		err := m.Arm(ctx, time.Second, 1)
		if err == nil || strings.Contains(err.Error(), urlSecret) {
			t.Fatalf("error carries the credential or is nil: %v", err)
		}
	})

	t.Run("redirect is not followed", func(t *testing.T) {
		var hits int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			w.Header().Set("Location", "http://127.0.0.1:1/?token="+urlSecret+"%zz")
			w.WriteHeader(http.StatusFound)
		}))
		defer srv.Close()
		m := NewMockInjector(srv.URL, "stall", 0)
		err := m.Arm(ctx, time.Second, 1)
		if err == nil || err.Error() != "mock injector: arm returned 302" {
			t.Fatalf("got %v, want the status", err)
		}
		if n := atomic.LoadInt32(&hits); n != 1 {
			t.Errorf("admin endpoint received %d requests, want 1", n)
		}
	})

	t.Run("undecodable body is not echoed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("not json " + urlSecret))
		}))
		defer srv.Close()
		m := NewMockInjector(srv.URL, "stall", 0)
		err := m.Arm(ctx, time.Second, 1)
		if err == nil || strings.Contains(err.Error(), urlSecret) || strings.Contains(err.Error(), "not json") {
			t.Fatalf("error echoes the response body or is nil: %v", err)
		}
	})
}
