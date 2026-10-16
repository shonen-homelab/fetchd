package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every fetchd setting. All values come from the environment;
// defaults match the documented behavior.
type Config struct {
	Listen       string   `json:"listen"`
	APIKeys      []string `json:"-"`
	AllowNoAuth  bool     `json:"allow_no_auth"`
	AllowPrivate bool     `json:"allow_private"`

	MaxConcurrency     uint          `json:"max_concurrency"`
	MaxQueue           uint          `json:"max_queue"`
	QueueTimeout       time.Duration `json:"queue_timeout"`
	PerHostConcurrency uint          `json:"per_host_concurrency"`
	PerHostInterval    time.Duration `json:"per_host_interval"`

	Timeout      time.Duration `json:"timeout"`
	MaxBodyBytes int64         `json:"max_body_bytes"`
	MaxRedirects uint          `json:"max_redirects"`
	UserAgent    string        `json:"user_agent"`

	MaxRetries     uint          `json:"max_retries"`
	RetryBaseDelay time.Duration `json:"retry_base_delay"`
	RetryMaxDelay  time.Duration `json:"retry_max_delay"`
	MaxRetryWait   time.Duration `json:"max_retry_wait"`
}

// DefaultConfig returns the documented defaults.
func DefaultConfig() Config {
	return Config{
		Listen:             ":8080",
		MaxConcurrency:     8,
		MaxQueue:           64,
		QueueTimeout:       30 * time.Second,
		PerHostConcurrency: 2,
		PerHostInterval:    500 * time.Millisecond,
		Timeout:            30 * time.Second,
		MaxBodyBytes:       10 << 20,
		MaxRedirects:       10,
		UserAgent:          "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) fetchd/1.0 Chrome/126.0 Safari/537.36",
		MaxRetries:         4,
		RetryBaseDelay:     500 * time.Millisecond,
		RetryMaxDelay:      30 * time.Second,
		MaxRetryWait:       60 * time.Second,
	}
}

// LoadConfig parses the environment into a Config. API keys are never logged.
func LoadConfig(env func(string) string) (Config, error) {
	if env == nil {
		env = os.Getenv
	}
	cfg := DefaultConfig()
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	setStr := func(key string, dst *string) {
		if v, ok := lookup(env, key); ok {
			*dst = v
		}
	}
	setUint := func(key string, dst *uint) {
		v, ok := lookup(env, key)
		if !ok {
			return
		}
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			fail("invalid %s %q: must be a non-negative integer", key, redact(key, v))
			return
		}
		*dst = uint(n)
	}
	setDur := func(key string, dst *time.Duration) {
		v, ok := lookup(env, key)
		if !ok {
			return
		}
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil || d < 0 {
			fail("invalid %s %q: must be a non-negative duration like \"500ms\" or \"30s\"", key, redact(key, v))
			return
		}
		*dst = d
	}
	setInt64 := func(key string, dst *int64) {
		v, ok := lookup(env, key)
		if !ok {
			return
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || n < 0 {
			fail("invalid %s %q: must be a non-negative integer", key, redact(key, v))
			return
		}
		*dst = n
	}
	setBool := func(key string, dst *bool) {
		v, ok := lookup(env, key)
		if !ok {
			return
		}
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			fail("invalid %s %q: must be true or false", key, redact(key, v))
			return
		}
		*dst = b
	}

	setStr("FETCHD_LISTEN", &cfg.Listen)
	setUint("FETCHD_MAX_CONCURRENCY", &cfg.MaxConcurrency)
	setUint("FETCHD_MAX_QUEUE", &cfg.MaxQueue)
	setDur("FETCHD_QUEUE_TIMEOUT", &cfg.QueueTimeout)
	setUint("FETCHD_PER_HOST_CONCURRENCY", &cfg.PerHostConcurrency)
	setDur("FETCHD_PER_HOST_INTERVAL", &cfg.PerHostInterval)
	setDur("FETCHD_TIMEOUT", &cfg.Timeout)
	setInt64("FETCHD_MAX_BODY_BYTES", &cfg.MaxBodyBytes)
	setUint("FETCHD_MAX_REDIRECTS", &cfg.MaxRedirects)
	setStr("FETCHD_USER_AGENT", &cfg.UserAgent)
	setUint("FETCHD_MAX_RETRIES", &cfg.MaxRetries)
	setDur("FETCHD_RETRY_BASE", &cfg.RetryBaseDelay)
	setDur("FETCHD_RETRY_MAX_DELAY", &cfg.RetryMaxDelay)
	setDur("FETCHD_MAX_RETRY_WAIT", &cfg.MaxRetryWait)
	setBool("FETCHD_ALLOW_NO_AUTH", &cfg.AllowNoAuth)
	setBool("FETCHD_ALLOW_PRIVATE", &cfg.AllowPrivate)

	if raw, ok := lookup(env, "FETCHD_API_KEYS"); ok {
		for _, k := range strings.Split(raw, ",") {
			if k = strings.TrimSpace(k); k != "" {
				cfg.APIKeys = append(cfg.APIKeys, k)
			}
		}
	}
	if len(cfg.APIKeys) == 0 && !cfg.AllowNoAuth {
		fail("no API keys configured: set FETCHD_API_KEYS or FETCHD_ALLOW_NO_AUTH=true")
	}
	if _, _, err := net.SplitHostPort(cfg.Listen); err != nil {
		// Allow bare ":8080" style; net.Listen validates later with a clear error.
		if !strings.HasPrefix(cfg.Listen, ":") {
			fail("invalid FETCHD_LISTEN %q: must be host:port or :port", cfg.Listen)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func lookup(env func(string) string, key string) (string, bool) {
	if env == nil {
		return os.LookupEnv(key)
	}
	v := env(key)
	return v, v != ""
}

// redact avoids echoing secrets back in error messages.
func redact(key, v string) string {
	if strings.Contains(key, "KEY") {
		return "***"
	}
	return v
}

// logValue returns a slog-safe view of the config with secrets stripped.
func (c Config) logValue() slog.Value {
	return slog.GroupValue(
		slog.String("listen", c.Listen),
		slog.Bool("allow_no_auth", c.AllowNoAuth),
		slog.Bool("allow_private", c.AllowPrivate),
		slog.Uint64("max_concurrency", uint64(c.MaxConcurrency)),
		slog.Uint64("max_queue", uint64(c.MaxQueue)),
		slog.Duration("queue_timeout", c.QueueTimeout),
		slog.Uint64("per_host_concurrency", uint64(c.PerHostConcurrency)),
		slog.Duration("per_host_interval", c.PerHostInterval),
		slog.Duration("timeout", c.Timeout),
		slog.Int64("max_body_bytes", c.MaxBodyBytes),
		slog.Uint64("max_redirects", uint64(c.MaxRedirects)),
		slog.String("user_agent", c.UserAgent),
		slog.Uint64("max_retries", uint64(c.MaxRetries)),
		slog.Duration("retry_base_delay", c.RetryBaseDelay),
		slog.Duration("retry_max_delay", c.RetryMaxDelay),
		slog.Duration("max_retry_wait", c.MaxRetryWait),
		slog.Int("api_keys_configured", len(c.APIKeys)),
	)
}
