package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/run"
)

// mainEnv makes the test binary run main in place of the tests.
const mainEnv = "PERCENTES_TEST_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(mainEnv) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// The command refuses a hosted target, and a self-hosted run given
// --frames, before any request.
func TestRefusesBeforeAnyRequest(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	ref, err := os.ReadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, pairs ...string) string {
		s := string(ref)
		for i := 0; i < len(pairs); i += 2 {
			if strings.Count(s, pairs[i]) != 1 {
				t.Fatalf("%q not once in the reference", pairs[i])
			}
			s = strings.Replace(s, pairs[i], pairs[i+1], 1)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	baseURL := "  base_url: http://127.0.0.1:18000\n"
	self := write("self.yaml", baseURL, "  base_url: "+srv.URL+"\n")
	hosted := write("hosted.yaml", baseURL, "  base_url: "+srv.URL+"\n  hosted: true\n  model_name: test-model\n  api_key_env: PERCENTES_TEST_KEY\n",
		"  ignore_eos: true        # pinned (§6)\n", "  ignore_eos: false\n")
	for _, path := range []string{self, hosted} {
		cfg, err := config.LoadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := run.CheckConfig(cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.Target.Hosted != (path == hosted) {
			t.Fatalf("%s: hosted %v", path, cfg.Target.Hosted)
		}
	}

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"hosted", []string{"--config", hosted}, "percentes: a hosted target runs under percentes-release or percentes-pilot only"},
		{"self-hosted with frames", []string{"--config", self, "--frames", t.TempDir()}, "percentes: --frames applies to --check-release only"},
		{"self-hosted with a release key", []string{"--config", self, "--release-key", filepath.Join(dir, "key")}, "percentes: --release-key applies to --check-release only"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], append(tc.args, "--out", filepath.Join(dir, "out"))...)
		cmd.Env = append(os.Environ(), mainEnv+"=1", "PERCENTES_TEST_KEY=test")
		cmd.WaitDelay = 5 * time.Second
		out, err := cmd.CombinedOutput()
		cancel()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), tc.want) {
			t.Errorf("%s: %v\n%s", tc.name, err, out)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("%d requests sent", n)
	}
	if _, err := os.Stat(filepath.Join(dir, "out")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("out directory: %v", err)
	}
}
