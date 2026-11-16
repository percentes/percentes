package probe

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/redact"
)

// endsLate is a context whose Err turns non-nil once ended is set and
// whose Done never closes, so a request under it never sees it end.
type endsLate struct {
	context.Context
	ended atomic.Bool
}

func (c *endsLate) Err() error {
	if c.ended.Load() {
		return context.Canceled
	}
	return nil
}

// closedAddr is a localhost address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	l.Close()
	return a
}

// dialTenth sets newTransport, for the test, to one that dials afresh
// for every request and dials the tenth with dial.
func dialTenth(t *testing.T, dial func(d *net.Dialer, ctx context.Context, network, addr string) (net.Conn, error)) {
	t.Helper()
	old := newTransport
	t.Cleanup(func() { newTransport = old })
	newTransport = func(p int, roots *x509.CertPool) *http.Transport {
		tr := loadgen.NewTransport(p, roots)
		tr.DisableKeepAlives = true
		var dials atomic.Int32
		d := &net.Dialer{}
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if dials.Add(1) == CeilingRequests {
				return dial(d, ctx, network, addr)
			}
			return d.DialContext(ctx, network, addr)
		}
		return tr
	}
}

// On each adapter, a tenth request whose own dial is refused, with the
// check's context ending after the dial returned and before the request
// is read, marks the record unreached and not interrupted; it stops the
// check and verifies.
func TestUnreachedRequestStopsCheckWhenContextEndsAfterIt(t *testing.T) {
	noSpacing(t)
	for _, r := range checkRoutes {
		ctx := &endsLate{Context: context.Background()}
		refused := closedAddr(t)
		dialTenth(t, func(d *net.Dialer, c context.Context, network, _ string) (net.Conn, error) {
			conn, err := d.DialContext(c, network, refused)
			ctx.ended.Store(true)
			return conn, err
		})
		cfg := r.config(t, faultAt(t, 99, r.pass, r.pass))
		rec := asWritten(t, releaseProbeCtx(t, ctx, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}))
		if len(rec.Requests) != CeilingRequests || rec.Sent() != CeilingRequests-1 || rec.Unreached == "" || rec.Interrupted != "" || rec.Outcome() != CheckStopped {
			t.Errorf("%s: %d requests, %d sent, unreached %q, interrupted %q, outcome %s", r.adapter, len(rec.Requests), rec.Sent(), rec.Unreached, rec.Interrupted, rec.Outcome())
		}
		if err := verifyRelease(rec, cfg); err != nil {
			t.Errorf("%s: Verify: %v", r.adapter, err)
		}
	}
}

// On each adapter, an abort during the tenth request's dial leaves that
// request cancelled and the record interrupted after nine, not
// unreached, whatever the dial then returns: the context's error, a
// refusal, or a real dial's result. Each record verifies.
func TestAbortDuringDialLeavesRequestCancelled(t *testing.T) {
	noSpacing(t)
	for _, mode := range []string{"the context's error", "a refusal", "a real dial"} {
		mode := mode
		for _, r := range checkRoutes {
			for i := 0; i < 5; i++ {
				name := fmt.Sprintf("%s: %s, run %d", r.adapter, mode, i)
				ctx, cancel := context.WithCancelCause(context.Background())
				dialTenth(t, func(d *net.Dialer, c context.Context, network, addr string) (net.Conn, error) {
					cancel(loadgen.ErrOperatorAbort)
					switch mode {
					case "the context's error":
						<-c.Done()
						return nil, c.Err()
					case "a refusal":
						<-c.Done()
						return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
					}
					return d.DialContext(c, network, addr)
				})
				cfg := r.config(t, faultAt(t, 99, r.pass, r.pass))
				rec := asWritten(t, releaseProbeCtx(t, ctx, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap}))
				cancel(nil)
				q := &rec.Requests[len(rec.Requests)-1]
				want := "the check's context ended after 9 of 10 requests: operator abort"
				if len(rec.Requests) != CeilingRequests || q.Exchange.Error != redact.Cancelled || Reached(&q.Exchange) || rec.Unreached != "" || rec.Interrupted != want || rec.Outcome() != CheckStopped {
					t.Errorf("%s: %d requests, last error %q, reached %t, unreached %q, interrupted %q, outcome %s",
						name, len(rec.Requests), q.Exchange.Error, Reached(&q.Exchange), rec.Unreached, rec.Interrupted, rec.Outcome())
				}
				if err := verifyRelease(rec, cfg); err != nil {
					t.Errorf("%s: Verify: %v", name, err)
				}
			}
		}
	}
}

// Verify refuses a record neither blocked nor interrupted in which a
// dispatched request reached no connection and Unreached does not name
// it.
func TestVerifyRefusesUnnamedUnreachedRequest(t *testing.T) {
	noSpacing(t)
	var hits atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 9 {
			w.Header().Set("Connection", "close")
			sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
			srv.Listener.Close()
			return
		}
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
	if len(rec.Requests) != CeilingRequests || rec.Outcome() != CheckStopped || !strings.HasPrefix(rec.Unreached, "request 9 reached no connection: ") {
		t.Fatalf("listener closed after request 9: %d requests, outcome %s, unreached %q", len(rec.Requests), rec.Outcome(), rec.Unreached)
	}
	verify := func(r *ReleaseRecord) error {
		return r.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens)
	}
	if err := verify(rec); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	stripped := *rec
	stripped.Unreached = ""
	if err := verify(&stripped); err == nil || err.Error() != "request 9 reached no connection and the record does not name it unreached" {
		t.Fatalf("Verify with Unreached cleared: %v", err)
	}
}
