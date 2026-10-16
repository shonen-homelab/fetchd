package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Server wires routes to a Config.
type Server struct {
	cfg   Config
	mux   *http.ServeMux
	log   *slog.Logger
	gate  *gate
	retry retryPolicy
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
	// Healthcheck needs only the listen address, not API keys.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheckMain(os.Getenv("FETCHD_LISTEN")))
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := LoadConfig(nil)
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	if err := run(cfg, log); err != nil {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}

// healthcheckMain probes /healthz without curl (distroless has none).
// It dials the configured listen address, so it works in compose healthchecks.
func healthcheckMain(listen string) int {
	addr := healthAddr(listen)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + addr + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck: "+err.Error())
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status "+resp.Status)
		return 1
	}
	return 0
}

// healthAddr normalizes a listen address to a dialable one.
func healthAddr(listen string) string {
	if listen == "" {
		listen = ":8080"
	}
	if host, port, err := net.SplitHostPort(listen); err == nil {
		if host == "" {
			host = "127.0.0.1"
		}
		return net.JoinHostPort(host, port)
	}
	if strings.HasPrefix(listen, ":") {
		return net.JoinHostPort("127.0.0.1", strings.TrimPrefix(listen, ":"))
	}
	return listen
}
