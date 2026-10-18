package evidence

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/score"
)

var labels = []string{"billing", "technical", "account", "escalation"}

func testScorer(t *testing.T) *score.Scorer {
	t.Helper()
	c, err := score.NewContract(labels)
	if err != nil {
		t.Fatal(err)
	}
	s, err := score.NewScorer(c, score.Rule{Cutoff: 10 * time.Second, Accepted: []string{"stop"}, Length: []string{"length"}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func hostedCfg(baseURL string) *config.Config {
	cfg := &config.Config{Target: config.Target{BaseURL: baseURL, Hosted: true, ModelName: "openai/gpt-oss-120b"}}
	cfg.Load.RateRPS = 10
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Load.Connections = 2
	cfg.Load.MaxTokens = 64
	cfg.Run.Phases = config.Phases{BaselineS: 0.3}
	return cfg
}

func lines(t *testing.T, b []byte) []map[string]json.RawMessage {
	t.Helper()
	var out []map[string]json.RawMessage
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("line %d is not one JSON object: %v: %s", len(out), err, sc.Bytes())
		}
		out = append(out, m)
	}
	return out
}

// A hosted run against the D1 mock: every scheduled request has one
// line carrying the visible answer, reasoning presence and time, the
// transport record and the scorer's output; reasoning text never appears.
func TestRecordsFromAShapedHostedRun(t *testing.T) {
	fixed := config.LatencyDist{Distribution: "fixed", FixedMs: 1}
	srv := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed},
		mock.Shape{ReasoningKey: "reasoning", ReasoningTokens: 2, Answer: `{"route":"billing"}`, Usage: mock.UsageAlways})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	cfg := hostedCfg("http://" + srv.Addr())
	cfg.Target.ResponseShape = &config.ResponseShape{Reasoning: []string{"reasoning"}}

	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Requests) != 3 {
		t.Fatalf("%d requests scheduled, want 3", len(res.Requests))
	}
	var b bytes.Buffer
	sc := &Scoring{Scorer: testScorer(t), Expected: []string{"billing", "technical"}}
	if err := Write(&b, Records(cfg, res, Options{Scoring: sc})); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "r1 ") {
		t.Fatalf("reasoning text in the export: %s", b.Bytes())
	}
	got := lines(t, b.Bytes())
	if len(got) != 3 {
		t.Fatalf("%d lines, want 3", len(got))
	}
	wantScore := []string{
		`{"expected":"billing","class":"success","route":"billing"}`,
		`{"expected":"technical","class":"wrong_label","route":"billing"}`,
		"",
	}
	for i, m := range got {
		for k, want := range map[string]string{
			"index":             []string{"0", "1", "2"}[i],
			"outcome":           `"completed"`,
			"protocol":          `"HTTP/1.1"`,
			"status":            "200",
			"content_events":    "5",
			"completion_tokens": "7",
			"finish_reason":     `"stop"`,
			"reported_model":    `"openai/gpt-oss-120b"`,
			"answer":            `"{\"route\":\"billing\"}"`,
			"reasoning":         "true",
			"reasoning_chunks":  "2",
		} {
			if string(m[k]) != want {
				t.Errorf("line %d: %s = %s, want %s", i, k, m[k], want)
			}
		}
		if string(m["score"]) != wantScore[i] {
			t.Errorf("line %d: score %s, want %q", i, m["score"], wantScore[i])
		}
		var first, tok int64
		json.Unmarshal(m["first_reasoning_ns"], &first) //nolint:errcheck
		json.Unmarshal(m["first_token_ns"], &tok)       //nolint:errcheck
		if first <= 0 || first > tok {
			t.Errorf("line %d: first reasoning at %d, first content at %d", i, first, tok)
		}
		if _, ok := m["peer_addr"]; !ok {
			t.Errorf("line %d: no peer address", i)
		}
		if _, ok := m["run_failures"]; ok {
			t.Errorf("line %d: run_failures on a run that passed", i)
		}
	}
}

// Each record's literal form: a 429 with its allowlisted headers, a
// request never sent, and every run failure repeated on each line.
func TestRecordLiteralForm(t *testing.T) {
	res := &loadgen.Result{Requests: []loadgen.Request{
		{
			Index: 0, IntendedNs: 1000, DispatchNs: 1100, DoneNs: 5000,
			Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429,
			Proto: "HTTP/1.1", Status: 429, PeerAddr: "192.0.2.1:443",
			Headers: map[string][]string{"Retry-After": {"7"}, "X-Ratelimit-Remaining-Requests": {"0"}, "X-Request-Id": {"req_1"}},
		},
		{Index: 1, IntendedNs: 2000},
	}}
	fails := []string{"run: loadgen: hosted target: 1 responses not over HTTP/1.1 (first: request 0 over HTTP/2.0)", "client-validity gate failed (§2)"}
	var b bytes.Buffer
	if err := Write(&b, Records(hostedCfg(""), res, Options{RunFailures: fails, Scoring: &Scoring{Scorer: testScorer(t), Expected: []string{"billing"}}})); err != nil {
		t.Fatal(err)
	}
	want := `{"index":0,"intended_ns":1000,"dispatch_ns":1100,"first_token_ns":0,"done_ns":5000,"outcome":"errored","err_class":"status_429","protocol":"HTTP/1.1","status":429,"retry_after":"7","headers":{"Retry-After":["7"],"X-Ratelimit-Remaining-Requests":["0"],"X-Request-Id":["req_1"]},"peer_addr":"192.0.2.1:443","content_events":0,"answer":"","reasoning":false,"score":{"expected":"billing","class":"no_answer"},"run_failures":["run: loadgen: hosted target: 1 responses not over HTTP/1.1 (first: request 0 over HTTP/2.0)","client-validity gate failed (§2)"]}` + "\n" +
		`{"index":1,"intended_ns":2000,"dispatch_ns":0,"first_token_ns":0,"done_ns":0,"outcome":"","content_events":0,"answer":"","reasoning":false,"run_failures":["run: loadgen: hosted target: 1 responses not over HTTP/1.1 (first: request 0 over HTTP/2.0)","client-validity gate failed (§2)"]}` + "\n"
	if b.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", b.String(), want)
	}
}

// Values that need care: a multi-line usage object stays on one line, a
// zero completion count is kept apart from an absent one, the answer is
// kept byte for byte with no HTML escaping, and a self-hosted record
// carries no answer field.
func TestRecordValues(t *testing.T) {
	answer := "{\"route\":\"billing\"}\n<&> "
	res := &loadgen.Result{Requests: []loadgen.Request{
		{Index: 0, Outcome: loadgen.OutcomeCompleted, Tokens: 3, Content: answer,
			Usage: json.RawMessage("{\"completion_tokens\": 0,\n \"prompt_tokens\": 5}"), UsageSeen: true,
			SystemFingerprint: "fp_1", ModelMismatch: []string{"other"}, StreamError: `{"message":"x"}`, UnknownEvents: 2},
		{Index: 1, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrConnect, ErrText: "dial tcp 192.0.2.1:443: connect: connection refused"},
	}}
	var b bytes.Buffer
	if err := Write(&b, Records(hostedCfg(""), res, Options{})); err != nil {
		t.Fatal(err)
	}
	got := lines(t, b.Bytes())
	if len(got) != 2 {
		t.Fatalf("%d lines, want 2: %s", len(got), b.Bytes())
	}
	var a string
	if err := json.Unmarshal(got[0]["answer"], &a); err != nil || a != answer {
		t.Fatalf("answer %q, want %q", a, answer)
	}
	if !bytes.Contains(b.Bytes(), []byte(`\n<&>`)) {
		t.Fatalf("answer escaped beyond JSON's need: %s", b.Bytes())
	}
	for k, want := range map[string]string{
		"usage":              `{"completion_tokens":0,"prompt_tokens":5}`,
		"completion_tokens":  "0",
		"system_fingerprint": `"fp_1"`,
		"model_mismatch":     `["other"]`,
		"stream_error":       `"{\"message\":\"x\"}"`,
		"unknown_events":     "2",
	} {
		if string(got[0][k]) != want {
			t.Errorf("%s = %s, want %s", k, got[0][k], want)
		}
	}
	if string(got[1]["err_text"]) != `"dial tcp 192.0.2.1:443: connect: connection refused"` {
		t.Errorf("err_text = %s", got[1]["err_text"])
	}
	for _, k := range []string{"completion_tokens", "protocol", "status", "score"} {
		if _, ok := got[1][k]; ok {
			t.Errorf("connect failure carries %s: %s", k, got[1][k])
		}
	}

	self := hostedCfg("")
	self.Target.Hosted = false
	for _, rec := range Records(self, res, Options{}) {
		if rec.Answer != nil {
			t.Fatalf("self-hosted record %d carries an answer", rec.Index)
		}
	}
}

// The scorer reads the record: an empty stream that hit the ceiling is
// a length limit, an index with no expected label is unscored, and a
// refused attempt keeps its record with the scorer's error.
func TestScoreFromRecords(t *testing.T) {
	res := &loadgen.Result{Requests: []loadgen.Request{
		{Index: 0, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrEmptyStream, FinishReason: "length", ReasoningChunks: 9, DoneNs: 1},
		{Index: 1, Outcome: loadgen.OutcomeCompleted, Content: `{"route":"billing"}`, FinishReason: "stop", IntendedNs: 0, DoneNs: int64(11 * time.Second)},
		{Index: 2, Outcome: loadgen.OutcomeCompleted, Content: `{"route":"billing"}`, FinishReason: "stop"},
		{Index: 3, Outcome: loadgen.OutcomeCompleted, Content: `{"route":"billing"}`, FinishReason: "stop"},
		{Index: 4, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrMalformedStream, Content: `{"route":`},
	}}
	recs := Records(hostedCfg(""), res, Options{Scoring: &Scoring{Scorer: testScorer(t), Expected: []string{"billing", "billing", "", "refund", "billing"}}})
	want := []*Score{
		{Expected: "billing", Class: score.LengthLimit},
		{Expected: "billing", Class: score.LateCorrect, Route: "billing", Late: true},
		nil,
		{Expected: "refund", Error: `score: expected label "refund" is not configured`},
		{Expected: "billing", Class: score.PartialAnswer},
	}
	for i, rec := range recs {
		if (rec.Score == nil) != (want[i] == nil) || rec.Score != nil && *rec.Score != *want[i] {
			t.Errorf("record %d: score %+v, want %+v", i, rec.Score, want[i])
		}
	}

	recs = Records(hostedCfg(""), res, Options{Scoring: &Scoring{Expected: []string{"billing"}}})
	if s := recs[0].Score; s == nil || s.Error == "" || s.Class != "" {
		t.Fatalf("no scorer: %+v", s)
	}
}

// Where the shape names a refusal path, each record carries refusal
// presence and length and never the text; the scorer reads the text from
// the request; the request read back keeps the length. With no refusal
// path the record has no refusal field.
func TestRefusalFromRecords(t *testing.T) {
	res := &loadgen.Result{Requests: []loadgen.Request{
		{Index: 0, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrEmptyStream, FinishReason: "stop", Refusal: "I cannot help.", RefusalBytes: 14},
		{Index: 1, Outcome: loadgen.OutcomeCompleted, Content: `{"route":"billing"}`, FinishReason: "stop"},
	}}
	cfg := hostedCfg("")
	cfg.Target.ResponseShape = &config.ResponseShape{Refusal: []string{"choices.0.delta.refusal"}}
	recs := Records(cfg, res, Options{Scoring: &Scoring{Scorer: testScorer(t), Expected: []string{"billing", "billing"}}})
	var b bytes.Buffer
	if err := Write(&b, recs); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "cannot") {
		t.Fatalf("refusal text in the records: %s", b.Bytes())
	}
	got := lines(t, b.Bytes())
	if string(got[0]["refusal"]) != "true" || string(got[0]["refusal_bytes"]) != "14" {
		t.Errorf("record 0: refusal %s bytes %s, want true and 14", got[0]["refusal"], got[0]["refusal_bytes"])
	}
	if _, ok := got[1]["refusal_bytes"]; string(got[1]["refusal"]) != "false" || ok {
		t.Errorf("record 1: refusal %s bytes %s, want false and no length", got[1]["refusal"], got[1]["refusal_bytes"])
	}
	if s := recs[0].Score; s == nil || s.Class != score.Refusal {
		t.Errorf("record 0: score %+v, want %s", s, score.Refusal)
	}
	if s := recs[1].Score; s == nil || s.Class != score.Success {
		t.Errorf("record 1: score %+v, want %s", s, score.Success)
	}
	if r := recs[0].Request(); r.RefusalBytes != 14 || r.Refusal != "" {
		t.Errorf("request 0 read back: refusal %q bytes %d", r.Refusal, r.RefusalBytes)
	}

	for _, rec := range Records(hostedCfg(""), res, Options{}) {
		if rec.Refusal != nil || rec.RefusalBytes != 0 {
			t.Errorf("record %d with no refusal path: refusal present %v, bytes %d", rec.Index, rec.Refusal != nil, rec.RefusalBytes)
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
					"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
			}(conn)
		}
	}()
	return "http://" + ln.Addr().String()
}

// A hosted run that fails the protocol check keeps a line for every
// scheduled request, each stating the failure.
func TestFailedRunKeepsEveryAttempt(t *testing.T) {
	cfg := hostedCfg(http10Server(t))
	res, runErr := loadgen.Run(context.Background(), cfg, nil)
	if runErr == nil || res == nil {
		t.Fatalf("want a failed run with a result, got %v, %v", res, runErr)
	}
	dir := filepath.Join(t.TempDir(), "out")
	path, err := WriteRun(dir, cfg, res, Options{RunFailures: []string{runErr.Error()}})
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(dir, AttemptsFile) {
		t.Fatalf("path %q", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := lines(t, b)
	if len(got) != len(res.Requests) || len(got) == 0 {
		t.Fatalf("%d lines for %d scheduled requests", len(got), len(res.Requests))
	}
	for i, m := range got {
		var fails []string
		json.Unmarshal(m["run_failures"], &fails) //nolint:errcheck
		if string(m["protocol"]) != `"HTTP/1.0"` || len(fails) != 1 || !strings.Contains(fails[0], "not over HTTP/1.1") {
			t.Errorf("line %d: protocol %s, run_failures %q", i, m["protocol"], fails)
		}
	}

	none := filepath.Join(t.TempDir(), "none")
	if path, err := WriteRun(none, cfg, nil, Options{}); path != "" || err != nil {
		t.Fatalf("nil result: %q, %v", path, err)
	}
	if _, err := os.Stat(none); !os.IsNotExist(err) {
		t.Fatalf("nil result created %s: %v", none, err)
	}
}
