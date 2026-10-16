package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// Server wires routes to a Config.
type Server struct {
	cfg  Config
	mux  *http.ServeMux
	log  *slog.Logger
	gate *gate
}

// NewServer lives in fetch.go.

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// run starts the HTTP server; split out for the healthcheck subcommand in PR5.
func run(cfg Config, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           NewServer(cfg, log),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Info("fetchd listening", "config", cfg.logValue())
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	return nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := LoadConfig(nil)
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheckMain(cfg))
	}
	if err := run(cfg, log); err != nil {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}

// healthcheckMain probes /healthz without curl (distroless has none).
// Full implementation lands with the Dockerfile in PR5.
func healthcheckMain(_ Config) int {
	fmt.Fprintln(os.Stderr, "healthcheck: not yet implemented")
	return 1
}
