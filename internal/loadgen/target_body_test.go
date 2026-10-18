package loadgen

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

const targetBody = ` {"model":"claude-sonnet-5-5","system":"Summarise.",
  "messages":[{"role":"user","content":"{{case}}"}],"stream":true,"max_tokens":512} `

// literalBodyCfg is pilotBodyCfg with the pilot body removed and the
// target carrying targetBody.
func literalBodyCfg(t *testing.T, endpoint string, n int) *config.Config {
	t.Helper()
	cfg := pilotBodyCfg(t, endpoint, n)
	cfg.Pilot = nil
	cfg.Target.Body = targetBody
	return cfg
}

// Each auth_header choice sends the key in exactly one header, and in no
// other.
func TestKeyHeaderChoices(t *testing.T) {
	const key = "SYNTHETIC_KEY_c0ffee27"
	creds := []string{"Authorization", "X-Api-Key", "Api-Key", "Proxy-Authorization"}
	for _, tc := range []struct{ kind, header, value string }{
		{"", "Authorization", "Bearer " + key},
		{config.AuthBearer, "Authorization", "Bearer " + key},
		{config.AuthXAPIKey, "X-Api-Key", key},
		{config.AuthAPIKey, "Api-Key", key},
	} {
		var mu sync.Mutex
		var got http.Header
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			got = r.Header.Clone()
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, completeStream) //nolint:errcheck
		}))
		cfg := &config.Config{Target: config.Target{Hosted: true, AuthHeader: tc.kind}}
		if r := executeCfg(srv, cfg, key); r.Outcome != OutcomeCompleted {
			t.Fatalf("%q: outcome %v/%q", tc.kind, r.Outcome, r.ErrClass)
		}
		srv.Close()
		mu.Lock()
		for _, h := range creds {
			switch v := got.Values(h); {
			case h == tc.header && (len(v) != 1 || v[0] != tc.value):
				t.Errorf("%q: %s = %q, want %q", tc.kind, h, v, tc.value)
			case h != tc.header && len(v) > 0:
				t.Errorf("%q: %s also sent: %q", tc.kind, h, v)
			}
		}
		for name, vs := range got {
			for _, v := range vs {
				if strings.Contains(v, key) && name != tc.header {
					t.Errorf("%q: key in %s", tc.kind, name)
				}
			}
		}
		mu.Unlock()
	}
}

// A target's literal body is sent with each request's case text in place
// of the slot and every other byte as configured, to the complete
// endpoint URL.
func TestTargetLiteralBody(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got[string(b)] = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, completeStream) //nolint:errcheck
	}))
	defer srv.Close()

	cfg := literalBodyCfg(t, srv.URL+"/anthropic/v1/messages", 3)
	cases := []string{"first ticket", "second \"ticket\"", "third <ticket> & é"}
	res, err := Run(context.Background(), cfg, &Hooks{Cases: cases})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Requests) != 3 || len(got) != 3 {
		t.Fatalf("%d requests, %d distinct bodies received", len(res.Requests), len(got))
	}
	i := strings.Index(targetBody, config.PilotCaseSlot)
	for _, c := range cases {
		want := targetBody[:i] + `"` + strings.ReplaceAll(c, `"`, `\"`) + `"` + targetBody[i+len(config.PilotCaseSlot):]
		path, ok := got[want]
		if !ok {
			t.Fatalf("no request carried\n%s\nreceived %q", want, got)
		}
		if path != "/anthropic/v1/messages" {
			t.Errorf("case %q posted to %s", c, path)
		}
	}
}

// A literal-body run that cannot send its body sends nothing.
func TestTargetLiteralBodyRefusals(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer srv.Close()
	three := &Hooks{Cases: []string{"a", "b", "c"}}
	for _, tc := range []struct {
		name   string
		mutate func(*config.Config)
		hooks  *Hooks
		want   string
	}{
		{"no case texts", func(*config.Config) {}, nil, "target body: 0 case texts for 3"},
		{"short case texts", func(*config.Config) {}, &Hooks{Cases: []string{"a", "b"}}, "target body: 2 case texts for 3"},
		{"no slot", func(c *config.Config) { c.Target.Body = `{"stream":true}` }, three, "target.body: \"{{case}}\" must appear once"},
		{"stream off", func(c *config.Config) { c.Target.Body = `{"stream":false,"x":"{{case}}"}` }, three, "target.body: \"stream\" must be true"},
		{"pilot body too", func(c *config.Config) {
			p := pilotBodyCfg(t, srv.URL+"/v1/chat/completions", 3)
			c.Pilot = p.Pilot
		}, three, "target.body and pilot.body both set"},
	} {
		cfg := literalBodyCfg(t, srv.URL+"/v1/messages", 3)
		tc.mutate(cfg)
		res, err := Run(context.Background(), cfg, tc.hooks)
		if err == nil || res != nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: res %v, err %v", tc.name, res, err)
		}
	}
	if hits != 0 {
		t.Errorf("%d requests sent", hits)
	}
}
