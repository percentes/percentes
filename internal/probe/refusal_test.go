package probe

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

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
