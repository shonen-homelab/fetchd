package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fastRetry swaps sleep for a recorder and jitter for a fixed mid value.
func fastRetry(srv *Server, sleeps *[]time.Duration) {
	srv.retry.sleep = func(_ context.Context, d time.Duration) bool {
		*sleeps = append(*sleeps, d)
		return true
	}
	srv.retry.jitter = func(n int64) int64 { return n / 2 }
}

func TestBackoffBounds(t *testing.T) {
	p := retryPolicy{maxRetries: 4, base: 500 * time.Millisecond, capDelay: 30 * time.Second, budget: time.Minute}
	// Deterministic jitter: max value.
	p.jitter = func(n int64) int64 { return n - 1 }
	p.sleep = func(context.Context, time.Duration) bool { return true }
	if got := p.backoff(1); got != 500*time.Millisecond {
		t.Fatalf("attempt1: %v", got)
	}
	if got := p.backoff(2); got != time.Second {
		t.Fatalf("attempt2: %v", got)
	}
	if got := p.backoff(3); got != 2*time.Second {
		t.Fatalf("attempt3: %v", got)
	}
	// Cap: base*2^10 way exceeds 30s.
	if got := p.backoff(11); got != 30*time.Second {
		t.Fatalf("cap: %v", got)
	}
}

func TestRetryAfterParsing(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	h := http.Header{"Retry-After": {"2"}}
	if d, ok := retryAfterDelay(h, now); !ok || d != 2*time.Second {
		t.Fatalf("seconds: %v %v", d, ok)
	}
	future := now.Add(90 * time.Second).UTC().Format(http.TimeFormat)
	h.Set("Retry-After", future)
	if d, ok := retryAfterDelay(h, now); !ok || d < 89*time.Second || d > 91*time.Second {
		t.Fatalf("date: %v %v", d, ok)
	}
	h.Set("Retry-After", "nonsense")
	if _, ok := retryAfterDelay(h, now); ok {
		t.Fatal("invalid should not parse")
	}
	past := now.Add(-time.Minute).UTC().Format(http.TimeFormat)
	h.Set("Retry-After", past)
	if _, ok := retryAfterDelay(h, now); ok {
		t.Fatal("past date should not parse")
	}
}

func TestRetryOn429WithRetryAfter(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	var sleeps []time.Duration
	fastRetry(srv, &sleeps)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
			return
		}
		_, _ = w.Write([]byte("recovered"))
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 200 || rec.Body.String() != "recovered" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if calls.Load() != 3 {
		t.Fatalf("attempts: %d want 3", calls.Load())
	}
	if got := rec.Header().Get("X-Fetchd-Attempts"); got != "3" {
		t.Fatalf("attempt header: %q", got)
	}
	if len(sleeps) != 2 {
		t.Fatalf("sleeps: %d want 2", len(sleeps))
	}
}

func TestRetryOnConnectionRefused(t *testing.T) {
	srv := testServer(map[string]string{"FETCHD_MAX_RETRIES": "2"})
	srv.cfg.AllowPrivate = true
	var sleeps []time.Duration
	fastRetry(srv, &sleeps)

	// Closed port on loopback: connection refused, no server needed.
	dead := "http://127.0.0.1:1/"
	rec := doFetch(t, srv, http.MethodGet, dead, authHeaders())
	if rec.Code != 502 {
		t.Fatalf("got %d want 502 (%s)", rec.Code, rec.Body.String())
	}
	// 1 initial + 2 retries = 3 attempts => 2 sleeps.
	if len(sleeps) != 2 {
		t.Fatalf("sleeps: %d want 2", len(sleeps))
	}
}

func TestNoRetryOnClientError(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	var sleeps []time.Duration
	fastRetry(srv, &sleeps)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(404)
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 404 {
		t.Fatalf("got %d", rec.Code)
	}
	if calls.Load() != 1 || len(sleeps) != 0 {
		t.Fatalf("should not retry 404: calls=%d sleeps=%d", calls.Load(), len(sleeps))
	}
	if got := rec.Header().Get("X-Fetchd-Attempts"); got != "1" {
		t.Fatalf("attempt header: %q", got)
	}
}

func TestNoRetryOnSSRFBlock(t *testing.T) {
	srv := testServer(nil) // AllowPrivate=false
	var sleeps []time.Duration
	fastRetry(srv, &sleeps)
	rec := doFetch(t, srv, http.MethodGet, "http://127.0.0.1:9/", authHeaders())
	if rec.Code != 403 {
		t.Fatalf("got %d want 403", rec.Code)
	}
	if len(sleeps) != 0 {
		t.Fatalf("SSRF block retried: %d sleeps", len(sleeps))
	}
}

func TestBudgetExhaustionReturnsLastResponse(t *testing.T) {
	srv := testServer(map[string]string{
		"FETCHD_MAX_RETRIES":    "10",
		"FETCHD_MAX_RETRY_WAIT": "0s", // no wait budget: first retryable ends the loop
	})
	srv.cfg.AllowPrivate = true
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
		_, _ = w.Write([]byte("still down"))
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 503 || rec.Body.String() != "still down" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("zero budget should stop after 1 attempt: %d", calls.Load())
	}
}

func TestClientCancelStopsRetries(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	srv.retry.sleep = func(ctx context.Context, _ time.Duration) bool {
		<-ctx.Done()
		return false
	}
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
	}))
	defer upstream.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/fetch?url="+upstream.URL, nil).WithContext(ctx)
	req.Header.Set("X-API-Key", "k1")
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if calls.Load() < 1 {
		t.Fatal("expected at least one attempt")
	}
	// Cancel during backoff sleep aborts the loop; no assertion on code since
	// the handler returns without writing (client gone).
}

func TestRetryableErrorClassification(t *testing.T) {
	if retryableError(nil) {
		t.Fatal("nil must not retry")
	}
	if !retryableError(context.DeadlineExceeded) {
		t.Fatal("deadline must retry")
	}
	if retryableError(&ssrfBlockedError{reason: "x"}) {
		t.Fatal("SSRF block must not retry")
	}
}
