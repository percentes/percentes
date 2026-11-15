package report

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

// secs is s seconds in nanoseconds.
func secs(s float64) int64 { return int64(s * 1e9) }

// sent is a completed record at step, dispatched and written at atS
// seconds from the epoch, with prompt tokens reported where prompt is
// not negative.
func sent(step int, atS float64, prompt int) evidence.Record {
	r := evidence.Record{Step: step, IntendedNs: secs(atS), DispatchNs: secs(atS), WroteRequestNs: secs(atS), FirstTokNs: secs(atS + 0.5),
		DoneNs: secs(atS + 1), Outcome: loadgen.OutcomeCompleted, InTime: true}
	if prompt >= 0 {
		r.Usage = json.RawMessage(fmt.Sprintf(`{"prompt_tokens":%d}`, prompt))
	}
	return r
}

// A cell's records give one send each dispatched request, at the epoch
// plus its offsets, its write time where recorded and its reported prompt
// count; a probe or ceiling-check exchange gives one where it was
// dispatched. A send ends at its done time where it completed, returned
// an empty stream or got a status other than 200, and otherwise at the
// hard timeout after its dispatch, a reset or malformed stream's error
// time included.
func TestHistorySends(t *testing.T) {
	epoch := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	unsent := evidence.Record{Step: 1, IntendedNs: secs(3)}
	hung := evidence.Record{Step: 1, IntendedNs: secs(4), DispatchNs: secs(4), WroteRequestNs: secs(4.001), Outcome: loadgen.OutcomeCensored}
	reset := evidence.Record{Step: 1, IntendedNs: secs(5), DispatchNs: secs(5), WroteRequestNs: secs(5), DoneNs: secs(7), Status: 200,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrReset}
	refused := evidence.Record{Step: 1, IntendedNs: secs(6), DispatchNs: secs(6), WroteRequestNs: secs(6), DoneNs: secs(6.2), Status: 429,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429}
	empty := evidence.Record{Step: 1, IntendedNs: secs(8), DispatchNs: secs(8), WroteRequestNs: secs(8), DoneNs: secs(10), Status: 200,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrEmptyStream}
	malformed := evidence.Record{Step: 1, IntendedNs: secs(9), DispatchNs: secs(9), WroteRequestNs: secs(9), DoneNs: secs(11), Status: 200,
		Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrMalformedStream}
	warm := sent(1, 1, 7)
	warm.Warmup = true
	recs := []evidence.Record{warm, sent(1, 2, -1), unsent, hung, reset, refused, empty, malformed}
	for i := range recs {
		recs[i].Index, recs[i].CeilingTokens = int64(i), 512
	}
	got := RecordSends("groq", "c", epoch, recs, 1200, 90)
	if len(got) != 7 {
		t.Fatalf("%d sends, want 7: %+v", len(got), got)
	}
	for i, want := range []time.Time{epoch.Add(2 * time.Second), epoch.Add(3 * time.Second), epoch.Add(94 * time.Second), epoch.Add(95 * time.Second), epoch.Add(6200 * time.Millisecond),
		epoch.Add(10 * time.Second), epoch.Add(99 * time.Second)} {
		if !got[i].EndUTC.Equal(want) {
			t.Errorf("send %d ends %s, want %s", i, got[i].EndUTC, want)
		}
	}
	if !got[3].LatestUTC.Equal(epoch.Add(7 * time.Second)) {
		t.Errorf("reset send latest %s, want its error time", got[3].LatestUTC)
	}
	if !got[6].LatestUTC.Equal(epoch.Add(11 * time.Second)) {
		t.Errorf("malformed send latest %s, want its error time", got[6].LatestUTC)
	}
	w := epoch.Add(time.Second)
	if s := got[0]; s.Kind != SendWarmup || !s.DispatchUTC.Equal(w) || !s.WroteUTC.Equal(w) || !s.LatestUTC.Equal(epoch.Add(2*time.Second)) ||
		s.PromptTokens == nil || *s.PromptTokens != 7 || s.InputBound != 1200 || s.Ceiling != 512 {
		t.Errorf("warm-up send %+v", s)
	}
	if s := got[1]; s.Kind != SendLadder || s.PromptTokens != nil || s.Index != 1 {
		t.Errorf("ladder send %+v", s)
	}
	if s := got[2]; !s.LatestUTC.Equal(epoch.Add(94*time.Second)) || !s.WroteUTC.Equal(epoch.Add(4001*time.Millisecond)) {
		t.Errorf("censored send %+v, latest at the hard timeout", s)
	}

	ex := &probe.Exchange{DispatchUTC: epoch, WroteRequestNs: 3_000_000, DoneNs: 1_000_000_000}
	prompt := 900
	s, ok := ExchangeSend("groq", "ceiling check", SendCheck, 4, ex, nil, &prompt, 1200, 1024, 90)
	if !ok || !s.WroteUTC.Equal(epoch.Add(3*time.Millisecond)) || !s.LatestUTC.Equal(epoch.Add(time.Second)) || *s.PromptTokens != 900 || s.Ceiling != 1024 {
		t.Errorf("check send %+v", s)
	}
	if !s.EndUTC.Equal(epoch.Add(90 * time.Second)) {
		t.Errorf("check send with no status ends %s, want the hard timeout", s.EndUTC)
	}
	ex.Status = 200
	for _, c := range []struct {
		sh  *probe.Shape
		end time.Time
	}{
		{&probe.Shape{Done: true}, epoch.Add(time.Second)},
		{&probe.Shape{Done: true, ReadError: "reset"}, epoch.Add(90 * time.Second)},
		{&probe.Shape{}, epoch.Add(90 * time.Second)},
		{nil, epoch.Add(90 * time.Second)},
	} {
		if s, _ := ExchangeSend("groq", "ceiling check", SendCheck, 4, ex, c.sh, &prompt, 1200, 1024, 90); !s.EndUTC.Equal(c.end) {
			t.Errorf("status 200, stream %+v: ends %s, want %s", c.sh, s.EndUTC, c.end)
		}
	}
	ex.Status = 429
	if s, _ := ExchangeSend("groq", "ceiling check", SendCheck, 4, ex, nil, &prompt, 1200, 1024, 90); !s.EndUTC.Equal(epoch.Add(time.Second)) {
		t.Errorf("status 429 ends %s, want its done time", s.EndUTC)
	}
	if s, ok := ExchangeSend("groq", "probe", SendProbe, 0, &probe.Exchange{DispatchUTC: epoch}, nil, nil, 1200, 512, 90); !ok || s.WroteUTC != nil || !s.LatestUTC.Equal(epoch.Add(90*time.Second)) {
		t.Errorf("unwritten probe send %+v", s)
	}
	if _, ok := ExchangeSend("groq", "probe", SendProbe, 1, nil, nil, nil, 1200, 512, 90); ok {
		t.Error("an absent wrong-model exchange gave a send")
	}
	if _, ok := ExchangeSend("groq", "probe", SendProbe, 1, &probe.Exchange{}, nil, nil, 1200, 512, 90); ok {
		t.Error("an undispatched exchange gave a send")
	}
}

// A dispatch the write limit refused sent nothing, and is no send.
func TestRecordSendsSkipsRefusedWrites(t *testing.T) {
	epoch := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	recs := []evidence.Record{
		{Index: 0, Step: 1, DispatchNs: 1e9, WroteRequestNs: 1e9 + 1},
		{Index: 1, Step: 1, DispatchNs: 2e9, WriteDenied: true},
		{Index: 2, Step: 1},
	}
	sends := RecordSends("azure-S", "cell", epoch, recs, 4000, 90)
	if len(sends) != 1 || sends[0].Index != 0 {
		t.Fatalf("sends %+v, want request 0 alone", sends)
	}
}

// A send's prompt count in exponent form is taken at its value.
func TestRecordSendsExponentPromptCount(t *testing.T) {
	epoch := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	recs := []evidence.Record{{Index: 1, DispatchNs: 1e9, WroteRequestNs: 1e9 + 1, Usage: json.RawMessage(`{"prompt_tokens":1.5e4,"completion_tokens":5.12e3}`)}}
	s := RecordSends("azure", "weekday-00", epoch, recs, 1200, 90)
	if len(s) != 1 || s[0].PromptTokens == nil || *s[0].PromptTokens != 15000 {
		t.Fatalf("sends %+v", s)
	}
}
