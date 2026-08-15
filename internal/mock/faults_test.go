package mock

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// TestFaultStall (config-scripted): a mid-run stall freezes token
// emission server-wide for D, then emission resumes and the stream
// completes — the delay lands in the completion time. This is the fault
// AC2 later drives at D=10 s; here the mechanism is proven at test scale.
func TestFaultStall(t *testing.T) {
	const stallD = 1200 * time.Millisecond
	cfg := baseMockCfg()
	cfg.TTFT = fixed(50)
	cfg.ITL = fixed(40)
	cfg.FaultSchedule = []config.MockFault{
		{Mode: config.MockFaultStall, StartOffsetS: 0.4, DurationS: stallD.Seconds()},
	}
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	// Nominal 50ms + 29*40ms = 1210ms: the stream straddles the stall.
	res := doStream(t, base, 30)
	if res.err != nil || res.status != 200 {
		t.Fatalf("stalled stream must still complete: status=%d err=%v", res.status, res.err)
	}
	if len(res.contentTimes) != 30 {
		t.Fatalf("want all 30 tokens after stall resume, got %d", len(res.contentTimes))
	}
	// Emission targets are an absolute schedule: a stream straddling the
	// stall freezes, then flushes overdue tokens (staggered by <=100 ms
	// jitter), completing near stall end (0.4 s + 1.2 s = 1.6 s after
	// server start) rather than nominal + D.
	nominal := 50*time.Millisecond + 29*40*time.Millisecond
	if total := res.total(); total < nominal+250*time.Millisecond {
		t.Errorf("stall not reflected in completion time: total %v vs nominal %v", total, nominal)
	}
	if gap := res.maxGap(); gap < stallD-400*time.Millisecond {
		t.Errorf("stall not visible as an emission gap: max gap %v, want >= ~%v", gap, stallD)
	}

	// After expiry, streams are gap-free again.
	res2 := doStream(t, base, 10)
	if res2.err != nil || res2.status != 200 {
		t.Fatalf("post-stall stream failed: status=%d err=%v", res2.status, res2.err)
	}
	if gap := res2.maxGap(); gap > 500*time.Millisecond {
		t.Errorf("post-expiry stream still gapped: max gap %v", gap)
	}
}

// TestFaultErrorWindow (config-scripted): during the window new requests
// get an explicit 5xx (an Errored outcome, §3); before and after, they
// succeed; in-flight streams are not touched by error mode.
func TestFaultErrorWindow(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = fixed(10)
	cfg.ITL = fixed(2)
	cfg.FaultSchedule = []config.MockFault{
		{Mode: config.MockFaultError, StartOffsetS: 0.5, DurationS: 0.8},
	}
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	if res := doStream(t, base, 2); res.status != 200 || res.err != nil {
		t.Fatalf("pre-window request must succeed: status=%d err=%v", res.status, res.err)
	}

	time.Sleep(700 * time.Millisecond) // mid-window (t≈0.7s of [0.5,1.3))
	if res := doStream(t, base, 2); res.status != 500 {
		t.Fatalf("mid-window request must 500, got status=%d err=%v", res.status, res.err)
	}

	time.Sleep(800 * time.Millisecond) // past expiry (t≈1.5s)
	if res := doStream(t, base, 2); res.status != 200 || res.err != nil {
		t.Fatalf("post-window request must succeed: status=%d err=%v", res.status, res.err)
	}

	// Armed/fired/expired timestamps are recorded on the monotonic clock.
	recs := faultSnapshot(t, base)
	if len(recs) != 1 {
		t.Fatalf("want 1 fault record, got %d", len(recs))
	}
	r := recs[0]
	if r.Source != "schedule" || r.State != "expired" || r.FiredOffsetS == nil || r.ExpiredOffsetS == nil {
		t.Fatalf("fault record incomplete: %+v", r)
	}
	if d := *r.FiredOffsetS - 0.5; d < -0.05 || d > 0.25 {
		t.Errorf("fired offset: want ~0.5s, got %.3fs", *r.FiredOffsetS)
	}
	if d := *r.ExpiredOffsetS - 1.3; d < -0.05 || d > 0.25 {
		t.Errorf("expired offset: want ~1.3s, got %.3fs", *r.ExpiredOffsetS)
	}
}

// TestFaultStreamAbort (admin-armed): in-flight streams are RST
// immediately at fire time; new streams during the window are RST at
// admit (abort_after_tokens=0); after expiry, service is normal.
func TestFaultStreamAbort(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = fixed(30)
	cfg.ITL = fixed(30)
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	type outcome struct {
		res     *streamResult
		endedAt time.Time
	}
	inflight := make(chan outcome, 1)
	go func() {
		r := doStream(t, base, 100) // nominal ~3s; will be cut down
		inflight <- outcome{r, time.Now()}
	}()

	time.Sleep(400 * time.Millisecond) // stream is mid-flight
	rec, status := armFault(t, base, adminArmRequest{Mode: config.MockFaultStreamAbort, DelayS: 0, DurationS: 1.0})
	if status != 201 || rec == nil {
		t.Fatalf("arming stream_abort: status=%d", status)
	}
	armedAt := time.Now()

	got := <-inflight
	if !isReset(got.res.err) {
		t.Fatalf("in-flight stream must see a TCP RST, got err=%v (chunks=%d)", got.res.err, len(got.res.contentTimes))
	}
	if len(got.res.contentTimes) == 0 {
		t.Error("in-flight stream should have received tokens before the RST")
	}
	if lag := got.endedAt.Sub(armedAt); lag > 700*time.Millisecond {
		t.Errorf("in-flight RST must be immediate at fire time, took %v", lag)
	}

	// New request during the window: RST at admit, zero bytes. The error
	// must specifically be a connection reset — a FIN/EOF or timeout is a
	// different terminal event and must fail this test (§1 makes
	// RST-vs-no-RST load-bearing for outcome classification).
	res := doStream(t, base, 4)
	if !isReset(res.err) {
		t.Fatalf("mid-window request must be RST at admit: status=%d err=%v", res.status, res.err)
	}
	if res.bodyBytesSeen {
		t.Error("abort-at-admit must send zero bytes")
	}

	time.Sleep(1100 * time.Millisecond) // past expiry
	if res := doStream(t, base, 4); res.status != 200 || res.err != nil {
		t.Fatalf("post-window request must succeed: status=%d err=%v", res.status, res.err)
	}
}

// TestFaultStreamAbortAfterTokens: scheduled variant where new streams
// during the window are cut after N tokens (mid-stream RST).
func TestFaultStreamAbortAfterTokens(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = fixed(10)
	cfg.ITL = fixed(10)
	cfg.FaultSchedule = []config.MockFault{
		{Mode: config.MockFaultStreamAbort, StartOffsetS: 0.2, DurationS: 1.0, AbortAfterTokens: 3},
	}
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	time.Sleep(300 * time.Millisecond) // inside window
	res := doStream(t, base, 50)
	if !isReset(res.err) {
		t.Fatalf("mid-window stream must end in RST, got err=%v", res.err)
	}
	if len(res.contentTimes) != 3 {
		t.Errorf("want exactly abort_after_tokens=3 tokens before RST, got %d", len(res.contentTimes))
	}
	// The admitted stream must be served with normal TTFT/ITL pacing up
	// to the abort point, not emitted at zero latency: nominal time to
	// token 3 is TTFT 10ms + 2 x ITL 10ms = 30ms.
	if len(res.contentTimes) == 3 {
		if toThird := res.contentTimes[2].Sub(res.start); toThird < 25*time.Millisecond {
			t.Errorf("abort-window stream bypassed pacing: 3 tokens in %v (nominal 30ms)", toThird)
		}
	}
}

// TestFaultSilentHang (config-scripted) is the AC4b fault source: affected
// requests receive no bytes, no FIN, and no RST — ever. No terminal event
// arrives; the client's pinned 30 s timeout is where censoring is
// recorded (§3).
func TestFaultSilentHang(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = fixed(10)
	cfg.ITL = fixed(2)
	cfg.FaultSchedule = []config.MockFault{
		{Mode: config.MockFaultSilentHang, StartOffsetS: 0.2, DurationS: 1.5}, // window [0.2, 1.7)
	}
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	time.Sleep(300 * time.Millisecond) // inside the window

	// Health blackholes too (full-node-partition fidelity).
	healthDone := make(chan error, 1)
	go func() {
		_, err := getStatus(t, base+"/health", 400*time.Millisecond)
		healthDone <- err
	}()

	// Raw TCP so we can distinguish "no answer" from FIN/RST precisely.
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := chatBody(4)
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: mock\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)

	assertSilence := func(readFor time.Duration, phase string) {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(readFor)) //nolint:errcheck
		buf := make([]byte, 1)
		n, err := conn.Read(buf)
		if n != 0 {
			t.Fatalf("%s: silent-hang wrote %d bytes; must write none", phase, n)
		}
		nerr, ok := err.(net.Error)
		if !ok || !nerr.Timeout() {
			t.Fatalf("%s: want read timeout (no FIN, no RST), got %v", phase, err)
		}
	}
	// During the window: nothing.
	assertSilence(1200*time.Millisecond, "during window")
	// Past expiry (t≈1.5+... > 1.7): a captured request stays hung —
	// a healed partition does not resurrect dead in-flight work.
	assertSilence(800*time.Millisecond, "after window expiry")

	if herr := <-healthDone; herr == nil {
		t.Error("/health must blackhole during silent-hang by default")
	}

	// Out-of-band instrumentation stays reachable; the hang was counted.
	if !strings.Contains(metricsText(t, base), `percentes_mock_requests_total{outcome="hung"}`) {
		t.Error("hung requests must be counted on /metrics")
	}

	// New requests after expiry are served; health answers again.
	if res := doStream(t, base, 2); res.status != 200 || res.err != nil {
		t.Fatalf("post-expiry request must succeed: status=%d err=%v", res.status, res.err)
	}
	if status, err := getStatus(t, base+"/health", time.Second); err != nil || status != 200 {
		t.Fatalf("post-expiry health must be 200: status=%d err=%v", status, err)
	}
}

// TestFaultSilentHangMidStream: a stream already emitting tokens when
// silent_hang fires goes silent WITHOUT any terminal event — no more
// bytes, no FIN, no RST — and stays silent past window expiry. This is
// the killed-replica in-flight class AC4b's censoring depends on: any
// server-side terminal event before the client's 30 s timeout would flip
// the request from censored to errored.
func TestFaultSilentHangMidStream(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = fixed(10)
	cfg.ITL = fixed(100)
	cfg.FaultSchedule = []config.MockFault{
		{Mode: config.MockFaultSilentHang, StartOffsetS: 0.4, DurationS: 0.8}, // window [0.4, 1.2)
	}
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	// Nominal stream: 10ms + 49*100ms ≈ 4.9s — mid-flight at fire time.
	// The client gives up after 2.0s, well past the window's 1.2s expiry,
	// proving a captured stream is not resurrected by expiry.
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	start := time.Now()
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", bytes.NewReader(chatBody(50)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream must start before the window: %v", err)
	}
	defer resp.Body.Close()

	var tokens int
	var lastToken time.Time
	reader := bufio.NewReader(resp.Body)
	var readErr error
	for {
		line, err := reader.ReadString('\n')
		if strings.HasPrefix(strings.TrimSpace(line), "data: {") && strings.Contains(line, `"content"`) {
			tokens++
			lastToken = time.Now()
		}
		if err != nil {
			readErr = err
			break
		}
	}

	if tokens < 1 || tokens >= 50 {
		t.Fatalf("stream must be captured mid-flight: got %d tokens", tokens)
	}
	if gone := lastToken.Sub(start); gone > 800*time.Millisecond {
		t.Errorf("tokens kept flowing after fire (+0.4s): last at +%v", gone)
	}
	if isReset(readErr) || readErr == io.EOF {
		t.Fatalf("mid-stream capture must produce no server-side terminal event (no FIN/RST); client saw: %v", readErr)
	}
	if elapsed := time.Since(start); elapsed < 1900*time.Millisecond {
		t.Errorf("client should only have escaped via its own timeout, escaped after %v", elapsed)
	}
}

// TestServeHealthDuringSilentHang: the non-default escape hatch keeps
// /health answering while the data plane blackholes.
func TestServeHealthDuringSilentHang(t *testing.T) {
	cfg := baseMockCfg()
	cfg.ServeHealthDuringSilentHang = true
	cfg.FaultSchedule = []config.MockFault{
		{Mode: config.MockFaultSilentHang, StartOffsetS: 0.1, DurationS: 1.0},
	}
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	time.Sleep(300 * time.Millisecond) // inside the window
	status, err := getStatus(t, base+"/health", 500*time.Millisecond)
	if err != nil || status != 200 {
		t.Fatalf("health must answer 200 with serve_health_during_silent_hang: true, got %d err=%v", status, err)
	}
	// The data plane still blackholes.
	client := &http.Client{Timeout: 500 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
	_, derr := client.Post(base+"/v1/chat/completions", "application/json", bytes.NewReader(chatBody(2)))
	if derr == nil || isReset(derr) {
		t.Fatalf("inference must still blackhole (timeout, no reset): %v", derr)
	}
}

// TestSlowReload: slow-reload-on-reschedule is a startup property — for
// the configured duration after process start, health and inference are
// 503; afterwards the replica serves normally.
func TestSlowReload(t *testing.T) {
	const reload = 1200 * time.Millisecond
	cfg := baseMockCfg()
	cfg.TTFT = fixed(5)
	cfg.ITL = fixed(1)
	cfg.SlowReload = config.SlowReload{Enabled: true, DurationS: reload.Seconds()}
	s := startServer(t, cfg)
	base := "http://" + s.Addr()
	started := time.Now()

	if status, err := getStatus(t, base+"/health", time.Second); err != nil || status != 503 {
		t.Fatalf("health during reload: want 503, got %d err=%v", status, err)
	}
	if res := doStream(t, base, 2); res.status != 503 {
		t.Fatalf("inference during reload: want 503, got %d err=%v", res.status, res.err)
	}

	var readyAfter time.Duration
	for {
		if status, _ := getStatus(t, base+"/health", time.Second); status == 200 {
			readyAfter = time.Since(started)
			break
		}
		if time.Since(started) > 5*time.Second {
			t.Fatal("server never became ready")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if readyAfter < reload-100*time.Millisecond {
		t.Errorf("became ready after %v; want >= ~%v", readyAfter, reload)
	}
	if res := doStream(t, base, 2); res.status != 200 || res.err != nil {
		t.Fatalf("post-reload inference must succeed: status=%d err=%v", res.status, res.err)
	}
}

// TestAdminArmValidation: the orchestrator-facing API rejects bad modes,
// bad windows, and overlapping faults (one fault at a time).
func TestAdminArmValidation(t *testing.T) {
	s := startServer(t, baseMockCfg())
	base := "http://" + s.Addr()

	if _, status := armFault(t, base, adminArmRequest{Mode: "explode", DelayS: 0, DurationS: 1}); status != 400 {
		t.Errorf("unknown mode: want 400, got %d", status)
	}
	if _, status := armFault(t, base, adminArmRequest{Mode: config.MockFaultStall, DelayS: 0, DurationS: 0}); status != 400 {
		t.Errorf("zero duration: want 400, got %d", status)
	}
	if _, status := armFault(t, base, adminArmRequest{Mode: config.MockFaultStall, DelayS: 1, DurationS: 2, AbortAfterTokens: 3}); status != 400 {
		t.Errorf("abort_after_tokens on stall: want 400, got %d", status)
	}
	if rec, status := armFault(t, base, adminArmRequest{Mode: config.MockFaultError, DelayS: 5, DurationS: 5}); status != 201 || rec.State != "armed" {
		t.Fatalf("valid arm: want 201/armed, got %d %+v", status, rec)
	}
	if _, status := armFault(t, base, adminArmRequest{Mode: config.MockFaultStall, DelayS: 6, DurationS: 1}); status != 409 {
		t.Errorf("overlapping window: want 409, got %d", status)
	}
	if _, status := armFault(t, base, adminArmRequest{Mode: config.MockFaultStall, DelayS: 30, DurationS: 1}); status != 201 {
		t.Errorf("non-overlapping later window: want 201, got %d", status)
	}
}

// TestConfigFileDrivesFaults loads a complete run config through the same
// strict loader the mockserver binary uses and proves the scheduled fault
// fires from file configuration alone — "fault modes demonstrably
// scriptable from the config file".
func TestConfigFileDrivesFaults(t *testing.T) {
	raw, err := os.ReadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// Reference config, rescripted for test scale: ephemeral port, error
	// fault at +0.4s for 0.7s. Everything else stays pinned.
	y := string(raw)
	y = strings.Replace(y, `listen_addr: ":18000"`, `listen_addr: "127.0.0.1:0"`, 1)
	y = strings.Replace(y, `    fixed_ms: 200`, `    fixed_ms: 10`, 1)
	y = strings.Replace(y, "    - mode: stall\n      start_offset_s: 70\n      duration_s: 10",
		"    - mode: error\n      start_offset_s: 0.4\n      duration_s: 0.7", 1)
	path := t.TempDir() + "/run.yaml"
	if err := os.WriteFile(path, []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("rescripted reference config must still validate: %v", err)
	}
	s := startServer(t, *cfg.Mock)
	base := "http://" + s.Addr()

	if res := doStream(t, base, 2); res.status != 200 || res.err != nil {
		t.Fatalf("pre-window: want 200, got %d err=%v", res.status, res.err)
	}
	time.Sleep(600 * time.Millisecond) // t≈0.6s, inside [0.4, 1.1)
	if res := doStream(t, base, 2); res.status != 500 {
		t.Fatalf("mid-window: want 500 from config-scripted fault, got %d err=%v", res.status, res.err)
	}
	time.Sleep(700 * time.Millisecond) // t≈1.3s, past expiry
	if res := doStream(t, base, 2); res.status != 200 || res.err != nil {
		t.Fatalf("post-window: want 200, got %d err=%v", res.status, res.err)
	}

	recs := faultSnapshot(t, base)
	if len(recs) != 1 || recs[0].Source != "schedule" || recs[0].State != "expired" {
		t.Fatalf("config-scripted fault must be recorded and expired: %+v", recs)
	}
}
