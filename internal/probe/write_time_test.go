package probe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// expiredWrite fails every socket write of the connection it acquires.
type expiredWrite struct{ *http.Transport }

func (t expiredWrite) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		_ = info.Conn.SetWriteDeadline(time.Now().Add(-time.Second))
	}})
	return t.Transport.RoundTrip(req.WithContext(ctx))
}

// An exchange whose bytes never reach the socket records no write time;
// one that is written records it.
func TestExchangeWriteTime(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	tr := loadgen.NewTransport(1, nil)
	defer tr.CloseIdleConnections()
	p := prober{rt: expiredWrite{tr}, target: &config.Target{}, mask: newMasker("test"), timeout: time.Second}
	ex := p.exchange(context.Background(), http.MethodPost, srv.URL, []byte("{}"), nil, 128, nil)
	if hits.Load() != 0 || ex.WroteRequestNs != 0 {
		t.Fatalf("failed socket write: server hits %d, write time %d", hits.Load(), ex.WroteRequestNs)
	}
	p.rt = tr
	for i := 0; i < 2; i++ {
		ex = p.exchange(context.Background(), http.MethodPost, srv.URL, []byte("{}"), nil, 128, nil)
		if ex.Status != http.StatusTooManyRequests || ex.WroteRequestNs <= 0 || ex.WroteRequestNs < ex.ConnAcquiredNs {
			t.Errorf("write %d: status %d, connection %d, write time %d", i+1, ex.Status, ex.ConnAcquiredNs, ex.WroteRequestNs)
		}
	}
}
