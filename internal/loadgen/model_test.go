package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

func modelFrame(model, content string) string {
	return fmt.Sprintf("data: {\"model\":%q,\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", model, content)
}

// Every chunk's reported model is compared with the requested one ("m"
// under shapeGen); each distinct mismatch is kept once, in arrival order.
func TestModelMismatchPerResponse(t *testing.T) {
	for _, c := range []struct {
		name     string
		hosted   bool
		frames   []string
		reported string
		want     []string
	}{
		{"matches", true, []string{modelFrame("m", "a"), modelFrame("m", "b")}, "m", nil},
		{"absent", true, []string{answerFrame}, "", nil},
		{"other throughout", true, []string{modelFrame("openai/gpt-oss-20b", "a"), modelFrame("openai/gpt-oss-20b", "b")}, "openai/gpt-oss-20b", []string{"openai/gpt-oss-20b"}},
		{"changes mid-stream", true, []string{modelFrame("m", "a"), modelFrame("m-v2", "b"), modelFrame("m", "c")}, "m", []string{"m-v2"}},
		{"case differs", true, []string{modelFrame("M", "a")}, "M", []string{"M"}},
		{"self-hosted", false, []string{modelFrame("served", "a")}, "served", []string{"served"}},
		{"capped", true, []string{
			modelFrame("x1", "a"), modelFrame("x2", "a"), modelFrame("x1", "a"), modelFrame("x3", "a"),
			modelFrame("x4", "a"), modelFrame("x5", "a"), modelFrame("x6", "a"),
		}, "x1", []string{"x1", "x2", "x3", "x4"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := executeShaped(t, c.hosted, nil, append(c.frames, stopFrame, doneFrame)...)
			if r.Outcome != OutcomeCompleted {
				t.Fatalf("got %v/%q", r.Outcome, r.ErrClass)
			}
			if r.ReportedModel != c.reported || !slices.Equal(r.ModelMismatch, c.want) {
				t.Fatalf("reported %q mismatch %q, want %q and %q", r.ReportedModel, r.ModelMismatch, c.reported, c.want)
			}
			b, _ := json.Marshal(r)
			if strings.Contains(string(b), `"model_mismatch"`) != (c.want != nil) {
				t.Fatalf("model_mismatch in JSON, want present %v: %s", c.want != nil, b)
			}
		})
	}
}

func TestCompareModels(t *testing.T) {
	reqs := func(models ...[2]string) []Request {
		out := make([]Request, len(models))
		for i, m := range models {
			out[i] = Request{Index: int64(i), ReportedModel: m[0]}
			if m[1] != "" {
				out[i].ModelMismatch = strings.Split(m[1], ",")
			}
		}
		return out
	}
	for _, c := range []struct {
		name     string
		hosted   bool
		requests []Request
		reported []string
		count    int
		split    bool
	}{
		{"all match", true, reqs([2]string{"m", ""}, [2]string{"", ""}, [2]string{"m", ""}), []string{"m"}, 0, false},
		{"none reported", true, reqs([2]string{"", ""}), nil, 0, false},
		{"one mismatch", true, reqs([2]string{"m", ""}, [2]string{"m", "m-v2"}, [2]string{"m-v2", "m-v2"}), []string{"m", "m-v2"}, 2, true},
		{"several ids", true, reqs([2]string{"a", "a,b"}, [2]string{"m", ""}), []string{"a", "b", "m"}, 1, true},
		{"self-hosted", false, reqs([2]string{"m", ""}, [2]string{"m-v2", "m-v2"}), []string{"m", "m-v2"}, 1, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := &config.Config{Target: config.Target{Hosted: c.hosted}}
			res := &Result{Requests: c.requests}
			compareModels(cfg, "m", res)
			if res.RequestedModel != "m" || !slices.Equal(res.ReportedModels, c.reported) ||
				res.ModelMismatches != c.count || res.SplitSeries != c.split {
				t.Fatalf("requested %q reported %q mismatches %d split %v, want %q %d %v",
					res.RequestedModel, res.ReportedModels, res.ModelMismatches, res.SplitSeries, c.reported, c.count, c.split)
			}
		})
	}
}

// indexRE reads the request index from the prompt prefix cs-<seed>-<index>.
var indexRE = regexp.MustCompile(`cs-\d+-(\d+) `)

// A hosted run whose responses report another model from the third
// request on records each mismatch and flags the series split; the same
// run against a self-hosted target records the mismatches only.
func TestRunRecordsModelSplit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		model := "openai/gpt-oss-120b"
		if m := indexRE.FindSubmatch(body); m != nil {
			if i, _ := strconv.Atoi(string(m[1])); i >= 2 {
				model = "openai/gpt-oss-120b-v2"
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(modelFrame(model, "hi") + stopFrame + doneFrame)) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	for _, hosted := range []bool{true, false} {
		cfg := &config.Config{Target: config.Target{BaseURL: srv.URL, Hosted: hosted, ModelName: "openai/gpt-oss-120b"}}
		cfg.Load.RateRPS = 10
		cfg.Load.ArrivalProcess = "deterministic"
		cfg.Load.Connections = 2
		cfg.Load.MaxTokens = 8
		cfg.Run.Phases = config.Phases{BaselineS: 0.5}

		res, err := Run(context.Background(), cfg, nil)
		if res == nil {
			t.Fatalf("hosted=%v: no result: %v", hosted, err)
		}
		n := len(res.Requests)
		if n < 3 {
			t.Fatalf("hosted=%v: %d requests, want at least 3", hosted, n)
		}
		for i := range res.Requests {
			r := &res.Requests[i]
			if r.Outcome != OutcomeCompleted || (len(r.ModelMismatch) > 0) != (r.Index >= 2) {
				t.Fatalf("hosted=%v: request %d %v/%q mismatch %q", hosted, r.Index, r.Outcome, r.ErrClass, r.ModelMismatch)
			}
		}
		want := []string{"openai/gpt-oss-120b", "openai/gpt-oss-120b-v2"}
		if res.RequestedModel != "openai/gpt-oss-120b" || !slices.Equal(res.ReportedModels, want) ||
			res.ModelMismatches != n-2 || res.SplitSeries != hosted {
			t.Fatalf("hosted=%v: requested %q reported %q mismatches %d of %d split %v",
				hosted, res.RequestedModel, res.ReportedModels, res.ModelMismatches, n, res.SplitSeries)
		}
		b, _ := json.Marshal(res)
		if strings.Contains(string(b), `"split_series":true`) != hosted {
			t.Fatalf("hosted=%v: split_series in JSON: %s", hosted, b)
		}
	}
}
