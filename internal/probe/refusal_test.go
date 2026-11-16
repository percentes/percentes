package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// A configured billing refusal stops the ceiling check at the first
// response it matches, by status alone (Gemini's 402) or by status and
// body (Anthropic's spend-limit 429 and its 400), with the billing
// rule's reason; the record is stopped and verifies.
func TestCeilingCheckBillingStop(t *testing.T) {
	noSpacing(t)
	for _, tc := range []struct {
		status      int
		match, body string
	}{
		{402, "", "{}"},
		{429, "enforced_spend_limit_reached", `{"error":{"details":{"error_code":"enforced_spend_limit_reached"}}}`},
		{400, "You have reached your specified", `{"error":{"message":"You have reached your specified workspace API usage limits"}}`},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			var hits atomic.Int64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body) //nolint:errcheck
			}))
			t.Cleanup(s.Close)
			cfg := releaseConfig(t, s.URL)
			cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: tc.status, BodyContains: tc.match, Meaning: "spend limit reached",
				Source: "https://example.test/errors", Retrieved: "2026-11-02"}}
			r := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
			if hits.Load() != 1 || !r.Start.Blocked || !strings.Contains(r.Start.Reason, BillingSetting) || r.Outcome() != CheckStopped {
				t.Fatalf("HTTP %d: server requests=%d, start %+v, outcome %s", tc.status, hits.Load(), r.Start, r.Outcome())
			}
			if err := r.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
				t.Fatal(err)
			}
			r.Start = Start{}
			if err := r.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err == nil {
				t.Fatal("a record not blocked after a billing refusal verifies")
			}
		})
	}
}

// On a route with a confidential limit the record withholds the
// refusal's body, so Verify of the record as written accepts the billing
// stop where the recorded reason is the rule's at the response's status,
// and refuses another reason.
func TestCeilingCheckBillingStopWithheld(t *testing.T) {
	noSpacing(t)
	var hits atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"details":{"error_code":"enforced_spend_limit_reached"}}}`) //nolint:errcheck
	}))
	t.Cleanup(s.Close)
	cfg := releaseConfig(t, s.URL)
	l := &cfg.Target.Limits[1]
	l.Marking, l.SHA256, l.HMACSHA256 = config.MarkingConfidential, "", l.SHA256
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 429, BodyContains: "enforced_spend_limit_reached", Meaning: "spend limit reached",
		Source: "https://example.test/errors", Retrieved: "2026-11-02"}}
	r := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap, Key: newKey(t)})
	if hits.Load() != 1 || !r.Start.Blocked || r.Requests[0].Exchange.Body == nil || !r.Requests[0].Exchange.Body.Retained {
		t.Fatalf("server requests=%d, start %+v, body %+v", hits.Load(), r.Start, r.Requests[0].Exchange.Body)
	}
	b, err := Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "enforced_spend_limit_reached") {
		t.Fatal("the record carries the withheld body")
	}
	r = &ReleaseRecord{}
	if err := json.Unmarshal(b, r); err != nil {
		t.Fatal(err)
	}
	length, bound := LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens
	if err := r.Verify(&cfg.Target, length, bound); err != nil {
		t.Fatal(err)
	}
	r.Start.Reason = "probe request returned 429"
	if err := r.Verify(&cfg.Target, length, bound); err == nil {
		t.Fatal("a withheld 429 verifies as blocked under a reason that is not the billing rule's")
	}
}

// A check whose context ends before its ten requests are answered is
// interrupted: stopped, with the requests it sent, and it verifies.
func TestCeilingCheckInterrupted(t *testing.T) {
	noSpacing(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var hits atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 3 {
			io.Copy(io.Discard, r.Body) //nolint:errcheck
			cancel()
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			return
		}
		sseHandler(answerFrame, stopFrame, doneFrame)(w, r)
	}))
	t.Cleanup(s.Close)
	cfg := releaseConfig(t, s.URL)
	rec, err := RunRelease(ctx, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap, LowestStepRPS: testStepRPS})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Interrupted == "" || rec.Outcome() != CheckStopped || len(rec.Requests) != 3 || rec.Start.Blocked {
		t.Fatalf("interrupted %q, outcome %s, %d requests, start %+v", rec.Interrupted, rec.Outcome(), len(rec.Requests), rec.Start)
	}
	if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
		t.Fatal(err)
	}
	rec.Interrupted = ""
	if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err == nil {
		t.Fatal("three requests verify in a record neither blocked nor interrupted")
	}
}

// The record of a check whose process ended holds no request, is
// stopped and verifies; CheckReleaseInput refuses what RunRelease
// refuses before any request.
func TestInterruptedRecord(t *testing.T) {
	cfg := releaseConfig(t, "https://127.0.0.1:1/v1/chat/completions")
	at := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	rec, err := InterruptedRecord(ReleaseInput{Config: cfg, ByteCap: byteCap}, at, "the process ended")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Outcome() != CheckStopped || rec.Sent() != 0 || !rec.StartUTC.Equal(at) || rec.Ceiling.Ceiling != cfg.Release.Shape.CeilingTokens {
		t.Fatalf("%+v", rec)
	}
	if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
		t.Fatal(err)
	}
	if _, err := InterruptedRecord(ReleaseInput{Config: cfg, ByteCap: byteCap}, at, ""); err == nil {
		t.Fatal("an interrupted record with no reason")
	}
	t.Setenv(cfg.Target.APIKeyEnv, "")
	if err := CheckReleaseInput(ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap, LowestStepRPS: 1}); err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("empty key: %v", err)
	}
}

// refused is route's Shape R record at ceiling admitted at ledger line
// seq whose fifth request was answered an ordinary 429.
func refused(route string, ceiling, seq int) *ReleaseRecord {
	r := checkedR(route, ceiling, 9)
	r.LedgerSeq = seq
	r.Requests = make([]CheckRequest, CeilingRequests)
	for i := range r.Requests {
		r.Requests[i] = CheckRequest{Index: i, Exchange: Exchange{Status: http.StatusOK}, Pass: true}
	}
	r.Requests[4] = CheckRequest{Index: 4, Exchange: Exchange{Status: http.StatusTooManyRequests}, Fail: "status 429"}
	r.InputBound.Reported, r.InputBound.Pass = 9, false
	r.OutputBound.Reported, r.OutputBound.Pass = 9, false
	return r
}

// at is r admitted at ledger line seq.
func at(r *ReleaseRecord, seq int) *ReleaseRecord {
	r.LedgerSeq = seq
	return r
}

// stoppedAt is r blocked at the start gate.
func stoppedAt(r *ReleaseRecord, reason string) *ReleaseRecord {
	r.Start = Start{Blocked: true, Reason: reason}
	return r
}

// A check with an ordinary 429 is refused, apart from a shape failure,
// for both shapes: a refused check that decides excludes the host group
// as inconclusive at its ceiling, without moving to the fallback. A
// refused check may be repeated once at its ceiling, and the repeat
// decides, and a choice of a ceiling names the repeat; a check that was
// not refused is not repeated. Stopped checks (billing, setup,
// interrupted) decide nothing.
func TestChooseCeilingRefused(t *testing.T) {
	yes := true
	r := &config.Shape{Name: config.ShapeR, Fallback: &yes}
	sh := &config.Shape{Name: config.ShapeS}
	azure := []string{"azure-R"}
	refusedS := func(route string, ceiling, seq int) *ReleaseRecord {
		x := refused(route, ceiling, seq)
		x.Shape, x.InputBound.BoundTokens = config.ShapeS, 1200
		return x
	}
	for _, c := range []struct {
		name  string
		shape *config.Shape
		hosts []string
		recs  []*ReleaseRecord
		want  CeilingChoice
		err   string
	}{
		{"refused at 16", r, azure, []*ReleaseRecord{refused("azure-R", 16, 3)},
			CeilingChoice{Excluded: true, Reason: "azure-R: ceiling check inconclusive, HTTP 429 at 16"}, ""},
		{"refused, repeat passes", r, azure, []*ReleaseRecord{at(checkedR("azure-R", 16, 10), 7), refused("azure-R", 16, 3)},
			CeilingChoice{Ceiling: 16, Reason: "every host passed at 16; azure-R: the check refused at 16 (ledger line 3) was repeated"}, ""},
		{"refused twice", r, azure, []*ReleaseRecord{refused("azure-R", 16, 3), refused("azure-R", 16, 7)},
			CeilingChoice{Excluded: true, Reason: "azure-R: ceiling check inconclusive, HTTP 429 at 16"}, ""},
		{"refused, repeat fails, fallback passes", r, azure, []*ReleaseRecord{refused("azure-R", 16, 3), at(checkedR("azure-R", 16, 8), 7), at(checkedR("azure-R", 256, 10), 9)},
			CeilingChoice{Ceiling: 256, Reason: "azure-R failed at 16; every host passed at 256; azure-R: the check refused at 16 (ledger line 3) was repeated"}, ""},
		{"refused at the fallback", r, azure, []*ReleaseRecord{at(checkedR("azure-R", 16, 8), 3), refused("azure-R", 256, 5)},
			CeilingChoice{Excluded: true, Reason: "azure-R failed at 16; azure-R: ceiling check inconclusive, HTTP 429 at 256"}, ""},
		{"a check that ran is not repeated", r, azure, []*ReleaseRecord{at(checkedR("azure-R", 16, 8), 3), at(checkedR("azure-R", 16, 10), 5)},
			CeilingChoice{}, "probe: route azure-R at ceiling 16: the check admitted at ledger line 3 was not refused and decides; the check at line 5 repeats it"},
		{"three checks", r, azure, []*ReleaseRecord{refused("azure-R", 16, 3), refused("azure-R", 16, 5), at(checkedR("azure-R", 16, 10), 7)},
			CeilingChoice{}, "probe: route azure-R at ceiling 16: 3 checks not stopped; a refused check is repeated once"},
		{"stopped checks decide nothing", r, azure, []*ReleaseRecord{stoppedAt(at(checkedR("azure-R", 16, 0), 3), "HTTP 402, billing"),
			at(checkedR("azure-R", 16, 10), 5), stoppedAt(at(checkedR("azure-R", 16, 0), 7), "probe request returned 401")},
			CeilingChoice{Ceiling: 16, Reason: "every host passed at 16"}, ""},
		{"only stopped checks", r, azure, []*ReleaseRecord{stoppedAt(at(checkedR("azure-R", 16, 0), 3), "HTTP 402, billing")},
			CeilingChoice{}, "probe: route azure-R has no check at 16 that was not stopped (1 stopped)"},
		{"Shape S refused at 512", sh, []string{"gemini"}, []*ReleaseRecord{refusedS("gemini", 512, 4)},
			CeilingChoice{Excluded: true, Reason: "gemini: ceiling check inconclusive, HTTP 429 at 512"}, ""},
		{"Shape S refused, repeat passes", sh, []string{"gemini"}, []*ReleaseRecord{refusedS("gemini", 512, 4), at(checked("gemini", 512, 10), 6)},
			CeilingChoice{Ceiling: 512, Reason: "every host passed at 512; gemini: the check refused at 512 (ledger line 4) was repeated"}, ""},
	} {
		got, err := ChooseCeiling(c.shape, c.hosts, c.recs)
		if !reflect.DeepEqual(got, c.want) || fmt.Sprint(err) != map[bool]string{true: "<nil>", false: c.err}[c.err == ""] {
			t.Errorf("%s: got %+v, %v; want %+v, %q", c.name, got, err, c.want, c.err)
		}
	}
}

// The quiet interval before a refused check's repeat is the target's
// longest limit window plus the 90 s hard timeout.
func TestRepeatQuiet(t *testing.T) {
	tg := &config.Target{Limits: []config.Limit{
		{Dimension: config.LimitRPM, EvaluationWindowsS: []float64{1, 10, 60}},
		{Dimension: config.LimitSpendWindow, WindowS: 600},
		{Dimension: config.LimitRPD},
	}}
	if got := RepeatQuiet(tg); got != 690*time.Second {
		t.Fatalf("%s", got)
	}
	if got := RepeatQuiet(&config.Target{}); got != 90*time.Second {
		t.Fatalf("%s", got)
	}
}
