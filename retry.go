package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// retryPolicy carries the knobs; sleep/jitter are fields for test injection.
type retryPolicy struct {
	maxRetries int
	base       time.Duration
	capDelay   time.Duration
	budget     time.Duration

	sleep  func(context.Context, time.Duration) bool
	jitter func(n int64) int64
}

func retryPolicyFromConfig(cfg Config) retryPolicy {
	return retryPolicy{
		maxRetries: int(cfg.MaxRetries),
		base:       cfg.RetryBaseDelay,
		capDelay:   cfg.RetryMaxDelay,
		budget:     cfg.MaxRetryWait,
		sleep:      sleepContext,
		jitter:     rand.Int64N,
	}
}

// sleepContext waits d or until ctx ends; false means stop retrying.
func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// retryableStatus: retry on 429, 502, 503, 504 only.
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests ||
		code == http.StatusBadGateway ||
		code == http.StatusServiceUnavailable ||
		code == http.StatusGatewayTimeout
}

// retryableError: connection errors and timeouts. SSRF blocks never retry.
func retryableError(err error) bool {
	if err == nil {
		return false
	}
	var blocked *ssrfBlockedError
	if errors.As(err, &blocked) {
		return false
	}
	var urlErr interface{ Timeout() bool }
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	return isConnError(err)
}

// isConnError matches common connection failure strings.
func isConnError(err error) bool {
	msg := err.Error()
	for _, sub := range []string{
		"connection reset", "broken pipe", "no such host",
		"network is unreachable", "EOF",
	} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}

// retryAfterDelay parses Retry-After (seconds or HTTP date); ok=false if absent/invalid/past.
func retryAfterDelay(h http.Header, now time.Time) (time.Duration, bool) {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		if secs > 3600 {
			secs = 3600 // sanity cap; budget check applies next
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			return 0, false
		}
		return d, true
	}
	return 0, false
}

// backoff computes full-jitter delay for retry number n (1-based):
// uniform[0, min(cap, base*2^(n-1))].
func (p retryPolicy) backoff(n int) time.Duration {
	d := p.base
	for range n - 1 {
		d *= 2
		if d >= p.capDelay || d <= 0 { // overflow guard
			d = p.capDelay
			break
		}
	}
	if d > p.capDelay || d <= 0 {
		d = p.capDelay
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(p.jitter(int64(d) + 1))
}

// waitBeforeRetry honors Retry-After when it fits the remaining budget, else
// falls back to jittered backoff. Returns whether to proceed and how long it
// slept, so callers can track the total budget.
func (p retryPolicy) waitBeforeRetry(ctx context.Context, resp *http.Response, n int, spent time.Duration) (bool, time.Duration) {
	delay := p.backoff(n)
	if resp != nil {
		if ra, ok := retryAfterDelay(resp.Header, time.Now()); ok && ra <= p.budget-spent {
			delay = ra
		}
	}
	if spent+delay > p.budget {
		return false, 0
	}
	if p.sleep(ctx, delay) {
		return true, delay
	}
	return false, 0
}
