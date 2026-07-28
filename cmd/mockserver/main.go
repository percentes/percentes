// mockserver runs the Phase 0 mock inference server from the single run
// config file (SPEC.md: one config drives the run; the mock reads its
// `mock` section, including the scriptable fault schedule).
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
)

func main() {
	configPath := flag.String("config", "", "path to the run config YAML (required)")
	addrFile := flag.String("addr-file", "", "write the bound address to this file once listening (for ephemeral ports)")
	flag.Parse()
	if *configPath == "" {
		log.Fatal("mockserver: --config is required")
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("mockserver: %v", err)
	}
	if cfg.Mock == nil {
		log.Fatal("mockserver: config has no mock section (fault.variant must be \"mock\")")
	}

	srv := mock.New(*cfg.Mock)
	if err := srv.Start(); err != nil {
		log.Fatalf("mockserver: %v", err)
	}
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile, []byte(srv.Addr()), 0o644); err != nil {
			log.Fatalf("mockserver: write addr-file: %v", err)
		}
	}
	log.Printf("mockserver: serving on %s (seed=%d, scheduled faults=%d, slow_reload=%v)",
		srv.Addr(), cfg.Mock.Seed, len(cfg.Mock.FaultSchedule), cfg.Mock.SlowReload.Enabled)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Print("mockserver: shutting down")
	if err := srv.Close(); err != nil {
		log.Printf("mockserver: close: %v", err)
	}
}
