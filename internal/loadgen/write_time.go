package loadgen

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
)

// WriteObservation records the socket write that completes a
// fixed-length HTTP/1.1 request, its headers and body.
type WriteObservation struct {
	windows *writeWindows
	// held and denied are a release run's write permit: held from the
	// request's first socket write to its last, denied once refused.
	held   bool
	denied atomic.Bool
	// wait is the nanoseconds the write may wait for a permit after its
	// first attempt, and latest the time that wait ends.
	wait, latest int64
	attempted    bool
	ns           atomic.Int64
	now          func() int64
	// remaining counts the body bytes still to write once the header
	// terminator has been written; separator counts its bytes matched.
	remaining int
	separator int
	headers   bool
}

// WatchWrite attaches an observation to the connection acquired for one
// request whose body is bodyBytes long. On a connection the run's
// transport did not dial, the observation records nothing.
func WatchWrite(conn net.Conn, bodyBytes int, now func() int64) *WriteObservation {
	return watchSocket(conn, bodyBytes, now, nil)
}

func watchSocket(conn net.Conn, bodyBytes int, now func() int64, windows *writeWindows) *WriteObservation {
	return watchRelease(conn, bodyBytes, now, windows, 0)
}

// watchRelease is watchSocket for a write that may wait up to wait
// nanoseconds for a permit of windows.
func watchRelease(conn net.Conn, bodyBytes int, now func() int64, windows *writeWindows, wait int64) *WriteObservation {
	o := &WriteObservation{remaining: bodyBytes, now: now, windows: windows, wait: wait}
	if c, ok := conn.(*observedConn); ok {
		c.current.Store(o)
	}
	return o
}

// Nanoseconds is the time the request's last byte was accepted by the
// socket, or 0.
func (o *WriteObservation) Nanoseconds() int64 {
	if o == nil {
		return 0
	}
	return o.ns.Load()
}

// Denied reports that the run's write limit refused the request's first
// byte.
func (o *WriteObservation) Denied() bool {
	return o != nil && o.denied.Load()
}

func (o *WriteObservation) accept(p []byte, at int64) {
	if o.ns.Load() != 0 {
		return
	}
	if !o.headers {
		const end = "\r\n\r\n"
		for i, b := range p {
			switch {
			case b == end[o.separator]:
				o.separator++
			case b == '\r':
				o.separator = 1
			default:
				o.separator = 0
			}
			if o.separator == len(end) {
				o.headers = true
				p = p[i+1:]
				break
			}
		}
		if !o.headers {
			return
		}
	}
	o.remaining -= len(p)
	if o.remaining <= 0 {
		o.ns.Store(at)
	}
}

var errWriteWindow = errors.New("loadgen: request write refused: it would exceed the writes a limit window admits")

// observedConn charges each write to the write permit of the request its
// current observation records, and stamps the observation once the
// socket accepts the request's last byte.
type observedConn struct {
	net.Conn
	writeMu sync.Mutex
	current atomic.Pointer[WriteObservation]
}

func (c *observedConn) Close() error {
	err := c.Conn.Close()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if o := c.current.Load(); o != nil {
		o.windows.finish(o)
	}
	return err
}

func (c *observedConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	o := c.current.Load()
	if o != nil && !o.windows.reserve(o) {
		return 0, errWriteWindow
	}
	n, err := c.Conn.Write(p)
	if o != nil && err == nil && n == len(p) {
		o.accept(p, o.now())
	}
	if o != nil && (err != nil || o.Nanoseconds() > 0) {
		o.windows.finish(o)
	}
	return n, err
}

// instrumentWrites dials every plain and TLS connection of tr as an
// observedConn. TLS handshake records are written below the observation.
func instrumentWrites(tr *http.Transport) {
	dialer := new(net.Dialer)
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &observedConn{Conn: c}, nil
	}
	tr.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		cfg := tr.TLSClientConfig.Clone()
		if cfg == nil {
			cfg = &tls.Config{}
		}
		if cfg.ServerName == "" {
			if cfg.ServerName, _, err = net.SplitHostPort(addr); err != nil {
				_ = raw.Close()
				return nil, err
			}
		}
		conn := tls.Client(raw, cfg)
		trace := httptrace.ContextClientTrace(ctx)
		if trace != nil && trace.TLSHandshakeStart != nil {
			trace.TLSHandshakeStart()
		}
		if err := conn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			if trace != nil && trace.TLSHandshakeDone != nil {
				trace.TLSHandshakeDone(tls.ConnectionState{}, err)
			}
			return nil, err
		}
		if trace != nil && trace.TLSHandshakeDone != nil {
			trace.TLSHandshakeDone(conn.ConnectionState(), nil)
		}
		return &observedConn{Conn: conn}, nil
	}
}
