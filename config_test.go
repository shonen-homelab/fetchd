package main

import (
	"strings"
	"testing"
	"time"
)

func testEnv(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(testEnv(map[string]string{
		"FETCHD_API_KEYS": "secret",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listen != ":8080" || cfg.MaxConcurrency != 8 || cfg.MaxQueue != 64 {
		t.Fatalf("bad defaults: %+v", cfg)
	}
	if cfg.QueueTimeout != 30*time.Second || cfg.PerHostInterval != 500*time.Millisecond {
		t.Fatalf("bad duration defaults: %+v", cfg)
	}
	if cfg.MaxBodyBytes != 10<<20 || cfg.MaxRedirects != 10 || cfg.MaxRetries != 4 {
		t.Fatalf("bad numeric defaults: %+v", cfg)
	}
	if cfg.UserAgent == "" || !strings.Contains(cfg.UserAgent, "fetchd") {
		t.Fatalf("bad user agent default: %q", cfg.UserAgent)
	}
}

func TestLoadConfigRequiresKeys(t *testing.T) {
	if _, err := LoadConfig(testEnv(nil)); err == nil {
		t.Fatal("expected error with no keys and no allow flag")
	}
	cfg, err := LoadConfig(testEnv(map[string]string{"FETCHD_ALLOW_NO_AUTH": "true"}))
	if err != nil {
		t.Fatalf("allow-no-auth should pass: %v", err)
	}
	if len(cfg.APIKeys) != 0 {
		t.Fatalf("expected no keys, got %v", cfg.APIKeys)
	}
}

func TestLoadConfigParsesAndSplitsKeys(t *testing.T) {
	cfg, err := LoadConfig(testEnv(map[string]string{
		"FETCHD_API_KEYS":        " a ,, b ",
		"FETCHD_MAX_CONCURRENCY": "3",
		"FETCHD_QUEUE_TIMEOUT":   "5s",
		"FETCHD_LISTEN":          "127.0.0.1:9090",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.APIKeys) != 2 || cfg.APIKeys[0] != "a" || cfg.APIKeys[1] != "b" {
		t.Fatalf("bad key split: %q", cfg.APIKeys)
	}
	if cfg.MaxConcurrency != 3 || cfg.QueueTimeout != 5*time.Second {
		t.Fatalf("bad overrides: %+v", cfg)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	cases := []map[string]string{
		{"FETCHD_API_KEYS": "k", "FETCHD_MAX_CONCURRENCY": "nope"},
		{"FETCHD_API_KEYS": "k", "FETCHD_TIMEOUT": "soon"},
		{"FETCHD_API_KEYS": "k", "FETCHD_MAX_BODY_BYTES": "-1"},
		{"FETCHD_API_KEYS": "k", "FETCHD_ALLOW_PRIVATE": "maybe"},
		{"FETCHD_API_KEYS": "k", "FETCHD_LISTEN": "example.com"},
	}
	for i, vars := range cases {
		if _, err := LoadConfig(testEnv(vars)); err == nil {
			t.Fatalf("case %d: expected error", i)
		}
	}
}
