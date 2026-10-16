package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gateTestServer builds a server whose gate uses the given overrides.
func gateTestServer(t *testing.T, extra map[string]string) *Server {
	t.Helper()
	srv := testServer(extra)
	srv.cfg.AllowPrivate = true
	return srv
}

func TestQueueFullReturns429(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_MAX_CONCURRENCY":      "1",
		"FETCHD_MAX_QUEUE":            "0", // no waiting room at all
		"FETCHD_PER_HOST_CONCURRENCY": "0",
		"FETCHD_PER_HOST_INTERVAL":    "0s",
	})
	releaseBlock := make(chan struct{})
	blocker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-releaseBlock
		_, _ = w.Write([]byte("done"))
	}))
	defer blocker.Close()
	defer close(releaseBlock)

	// Occupy the single global slot directly at the gate.
	rel, ok, full := srv.gate.acquire(context.Background(), "blocked.example")
	if !ok || full {
		t.Fatalf("setup acquire: ok=%v full=%v", ok, full)
	}
	defer rel()

	rec := doFetch(t, srv, http.MethodGet, blocker.URL, authHeaders())
	if rec.Code != 429 {
		t.Fatalf("got %d want 429 (%s)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After on 429")
	}
}

func TestQueueTimeoutReturns503(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_MAX_CONCURRENCY":      "1",
		"FETCHD_MAX_QUEUE":            "16",
		"FETCHD_QUEUE_TIMEOUT":        "50ms",
		"FETCHD_PER_HOST_CONCURRENCY": "0",
		"FETCHD_PER_HOST_INTERVAL":    "0s",
	})
	rel, ok, _ := srv.gate.acquire(context.Background(), "blocked.example")
	if !ok {
		t.Fatal("setup acquire failed")
	}
	defer rel()

	start := time.Now()
	rec := doFetch(t, srv, http.MethodGet, "http://blocked.example/", authHeaders())
	if rec.Code != 503 {
		t.Fatalf("got %d want 503 (%s)", rec.Code, rec.Body.String())
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("queue wait took too long: %v", elapsed)
	}
}

func TestPerHostConcurrencyLimit(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_MAX_CONCURRENCY":      "0", // unlimited global
		"FETCHD_MAX_QUEUE":            "64",
		"FETCHD_PER_HOST_CONCURRENCY": "1",
		"FETCHD_PER_HOST_INTERVAL":    "0s",
	})
	var current, peak atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := current.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		current.Add(-1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	const callers = 6
	var wg sync.WaitGroup
	codes := make([]int, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
			codes[i] = rec.Code
		}()
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Fatalf("caller %d: got %d", i, c)
		}
	}
	if peak.Load() != 1 {
		t.Fatalf("per-host concurrency violated: peak=%d want 1", peak.Load())
	}
}

func TestPerHostIntervalSpacing(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_MAX_CONCURRENCY":      "0",
		"FETCHD_MAX_QUEUE":            "64",
		"FETCHD_QUEUE_TIMEOUT":        "10s",
		"FETCHD_PER_HOST_CONCURRENCY": "0",
		"FETCHD_PER_HOST_INTERVAL":    "200ms",
	})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	start := time.Now()
	for range 3 {
		rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
		if rec.Code != 200 {
			t.Fatalf("got %d (%s)", rec.Code, rec.Body.String())
		}
	}
	// 3 starts spaced 200ms apart => >= 400ms total.
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("interval not enforced: %v", elapsed)
	}
}

func TestClientCancelLeavesQueue(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_MAX_CONCURRENCY":      "1",
		"FETCHD_MAX_QUEUE":            "16",
		"FETCHD_PER_HOST_CONCURRENCY": "0",
		"FETCHD_PER_HOST_INTERVAL":    "0s",
	})
	rel, ok, _ := srv.gate.acquire(context.Background(), "x.example")
	if !ok {
		t.Fatal("setup acquire failed")
	}
	defer rel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok, _ := srv.gate.acquire(ctx, "x.example"); ok {
			t.Error("canceled acquire admitted")
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled acquire did not return")
	}
	srv.gate.mu.Lock()
	waiting := srv.gate.waiting
	srv.gate.mu.Unlock()
	if waiting != 0 {
		t.Fatalf("waiters leaked: %d", waiting)
	}
}

func TestHostSlotsPruned(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_PER_HOST_INTERVAL": "0s",
	})
	for range 5 {
		rel, ok, _ := srv.gate.acquire(context.Background(), "prune.example")
		if !ok {
			t.Fatal("acquire failed")
		}
		rel()
	}
	if n := srv.gate.hostCount(); n != 0 {
		t.Fatalf("host slots not pruned: %d", n)
	}
}

func TestWaiterNotStarvedByNewcomers(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_MAX_CONCURRENCY":      "1",
		"FETCHD_MAX_QUEUE":            "64",
		"FETCHD_QUEUE_TIMEOUT":        "10s",
		"FETCHD_PER_HOST_CONCURRENCY": "1",
		"FETCHD_PER_HOST_INTERVAL":    "0s",
	})
	holder, ok, _ := srv.gate.acquire(context.Background(), "churn.example")
	if !ok {
		t.Fatal("setup acquire failed")
	}
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		rel, ok, full := srv.gate.acquire(context.Background(), "churn.example")
		if !ok || full {
			t.Errorf("waiter: ok=%v full=%v", ok, full)
			return
		}
		rel()
	}()
	// Let the waiter queue up, then churn newcomers for the same host.
	time.Sleep(50 * time.Millisecond)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					rel, ok, _ := srv.gate.acquire(context.Background(), "churn.example")
					if ok {
						time.Sleep(time.Millisecond)
						rel()
					}
				}
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	holder() // free the slot; waiter must win over churning newcomers
	select {
	case <-waiterDone:
	case <-time.After(8 * time.Second):
		t.Fatal("waiter starved by newcomers")
	}
	close(stop)
	wg.Wait()
}

func TestQueueConcurrencyEndToEnd(t *testing.T) {
	srv := gateTestServer(t, map[string]string{
		"FETCHD_MAX_CONCURRENCY":      "2",
		"FETCHD_MAX_QUEUE":            "64",
		"FETCHD_QUEUE_TIMEOUT":        "10s",
		"FETCHD_PER_HOST_CONCURRENCY": "0",
		"FETCHD_PER_HOST_INTERVAL":    "0s",
	})
	var current, peak atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := current.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		current.Add(-1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	const callers = 8
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
			if rec.Code != 200 {
				t.Errorf("got %d", rec.Code)
			}
		}()
	}
	wg.Wait()
	if peak.Load() > 2 {
		t.Fatalf("global concurrency violated: peak=%d want<=2", peak.Load())
	}
}
