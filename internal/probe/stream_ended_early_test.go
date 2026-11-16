package probe

import (
	"net/http/httptest"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// A check whose requests are answered 200 on streams that end with valid
// usage and no terminator, and no read error, is refused as stream ended
// early. Unrepeated, it excludes the host group at 512 with no check at
// the 1,024 fallback; a repeat that passes decides for 512, and a repeat
// refused the same way excludes the group.
func TestStreamEndedEarlyRefusesCheck(t *testing.T) {
	noSpacing(t)
	srv := httptest.NewServer(sseHandler(answerFrame, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":5}}\n\n"))
	defer srv.Close()
	cfg := releaseConfig(t, srv.URL+"/v1/chat/completions")
	early := func(seq int) *ReleaseRecord {
		rec := releaseProbe(t, ReleaseInput{Config: cfg, Texts: texts(0), ByteCap: byteCap})
		rec.LedgerSeq = seq
		if q := rec.Requests[0]; rec.Outcome() != CheckRefused || rec.RefusedBy() != RefusedEndedEarly || q.Fail != "no terminator" || q.Shape.ReadError != "" || rec.Interrupted != "" {
			t.Fatalf("outcome %s, refused by %q, request 0 fail %q, read error %q", rec.Outcome(), rec.RefusedBy(), q.Fail, q.Shape.ReadError)
		}
		if err := rec.Verify(&cfg.Target, LengthValue(cfg.Target.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
			t.Fatalf("Verify: %v", err)
		}
		return rec
	}
	shape := &config.Shape{Name: config.ShapeS}
	first := early(1)
	const inconclusive = "groq: ceiling check inconclusive, stream ended early at 512"
	choice, err := ChooseCeiling(shape, []string{"groq"}, []*ReleaseRecord{first})
	if err != nil || !choice.Excluded || choice.Reason != inconclusive {
		t.Fatalf("unrepeated: %+v, %v", choice, err)
	}
	passed := checked("groq", 512, 10)
	passed.LedgerSeq, passed.RequestedModel, passed.ExpectedModel = 2, first.RequestedModel, first.ExpectedModel
	choice, err = ChooseCeiling(shape, []string{"groq"}, []*ReleaseRecord{first, passed})
	if want := "every host passed at 512; groq: the check refused at 512 (ledger line 1) was repeated"; err != nil || choice.Excluded || choice.Ceiling != 512 || choice.Reason != want {
		t.Fatalf("repeated and passed: %+v, %v; want %q", choice, err, want)
	}
	choice, err = ChooseCeiling(shape, []string{"groq"}, []*ReleaseRecord{first, early(2)})
	if err != nil || !choice.Excluded || choice.Reason != inconclusive {
		t.Fatalf("repeated and refused again: %+v, %v", choice, err)
	}
}
