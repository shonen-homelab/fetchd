package main

import (
	"compress/gzip"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func authHeaders() map[string]string { return map[string]string{"X-API-Key": "k2"} }

func TestAuth(t *testing.T) {
	srv := testServer(nil)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hi"))
	}))
	defer upstream.Close()
	// Allow httptest's 127.0.0.1 for auth tests; SSRF suite covers blocking.
	srv.cfg.AllowPrivate = true
	cases := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"no key", nil, 401},
		{"wrong key", map[string]string{"X-API-Key": "nope"}, 401},
		{"bad bearer prefix", map[string]string{"Authorization": "Token k1"}, 401},
		{"x-api-key first key", map[string]string{"X-API-Key": "k1"}, 200},
		{"x-api-key second key", authHeaders(), 200},
		{"bearer", map[string]string{"Authorization": "Bearer k1"}, 200},
	}
	for _, c := range cases {
		rec := doFetch(t, srv, http.MethodGet, upstream.URL, c.headers)
		if rec.Code != c.want {
			t.Errorf("%s: got %d want %d (%s)", c.name, rec.Code, c.want, rec.Body.String())
		}
	}

	// Healthz stays open.
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != 200 {
		t.Fatalf("healthz: got %d", rec.Code)
	}
}

func TestFetchPassthrough(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	var gotUA, gotCustom string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA, gotCustom = r.Header.Get("User-Agent"), r.Header.Get("X-Custom")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(201)
		_, _ = w.Write([]byte("<html>hi</html>"))
	}))
	defer upstream.Close()

	req := httptest.NewRequest(http.MethodPost, "/fetch", strings.NewReader(
		`{"url":`+strconv.Quote(upstream.URL)+`,"headers":{"X-Custom":"yes"}}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != 201 {
		t.Fatalf("status: got %d want 201 (%s)", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "<html>hi</html>" {
		t.Fatalf("body: %q", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Fatalf("content-type: %q", ct)
	}
	if etag := rec.Header().Get("ETag"); etag != `"v1"` {
		t.Fatalf("etag: %q", etag)
	}
	if rec.Header().Get("X-Fetchd-Final-URL") != upstream.URL {
		t.Fatalf("final-url: %q", rec.Header().Get("X-Fetchd-Final-URL"))
	}
	if rec.Header().Get("X-Fetchd-Upstream-Status") != "201" {
		t.Fatalf("upstream-status: %q", rec.Header().Get("X-Fetchd-Upstream-Status"))
	}
	if rec.Header().Get("X-Fetchd-Attempts") != "1" {
		t.Fatalf("attempts: %q", rec.Header().Get("X-Fetchd-Attempts"))
	}
	if !strings.Contains(gotUA, "fetchd") {
		t.Fatalf("user-agent not applied: %q", gotUA)
	}
	if gotCustom != "yes" {
		t.Fatalf("custom header not forwarded: %q", gotCustom)
	}
}

func TestUpstreamConnectionReuse(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	var mu sync.Mutex
	conns := 0
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			mu.Lock()
			conns++
			mu.Unlock()
		}
	}
	upstream.Start()
	defer upstream.Close()

	for range 3 {
		rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
		if rec.Code != 200 {
			t.Fatalf("got %d (%s)", rec.Code, rec.Body.String())
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if conns != 1 {
		t.Fatalf("upstream connections: %d want 1 (shared transport must reuse)", conns)
	}
}

func TestFetchBadURLAndScheme(t *testing.T) {
	srv := testServer(nil)
	for _, target := range []string{"ftp://example.com/x", "notaurl", ""} {
		rec := doFetch(t, srv, http.MethodGet, target, authHeaders())
		if rec.Code != 400 {
			t.Errorf("%q: got %d want 400", target, rec.Code)
		}
	}
	// POST with bad method.
	req := httptest.NewRequest(http.MethodPost, "/fetch", strings.NewReader(`{"url":"http://example.com","method":"POST"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Errorf("POST method: got %d want 400", rec.Code)
	}
}

func TestSSRFBlockedByDefault(t *testing.T) {
	srv := testServer(nil) // AllowPrivate=false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer upstream.Close() // 127.0.0.1 must be refused before dialing.

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 403 {
		t.Fatalf("got %d want 403 (%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "blocked_target") {
		t.Fatalf("body: %s", rec.Body.String())
	}

	// Literal loopback also blocked.
	rec = doFetch(t, srv, http.MethodGet, "http://127.0.0.1:9/", authHeaders())
	if rec.Code != 403 {
		t.Fatalf("loopback: got %d want 403", rec.Code)
	}
}

func TestDNSFailureIs502AndRetried(t *testing.T) {
	srv := testServer(nil)
	var sleeps []time.Duration
	fastRetry(srv, &sleeps)
	// .invalid never resolves (RFC 2606): fast, no network needed.
	rec := doFetch(t, srv, http.MethodGet, "http://nonexistent.invalid/", authHeaders())
	if rec.Code != 502 {
		t.Fatalf("got %d want 502 (%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "upstream_error") {
		t.Fatalf("body: %s", rec.Body.String())
	}
	if len(sleeps) == 0 {
		t.Fatal("DNS failure should be retryable")
	}
}

func TestNAT64And6to4Blocked(t *testing.T) {
	for _, ip := range []string{"64:ff9b::7f00:1", "2002:7f00:1::1"} {
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			t.Fatalf("parse %s: %v", ip, err)
		}
		if !ipBlocked(addr) {
			t.Errorf("%s should be blocked", ip)
		}
	}
}

func TestSSRFAllowedWithSwitch(t *testing.T) {
	srv := testServer(map[string]string{"FETCHD_ALLOW_PRIVATE": "true"})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("private ok"))
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 200 || rec.Body.String() != "private ok" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestGzipDecompressed(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "text/html")
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("<p>gz</p>"))
		_ = gz.Close()
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 200 {
		t.Fatalf("got %d (%s)", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "<p>gz</p>" {
		t.Fatalf("not decompressed: %q", rec.Body.String())
	}
	if ce := rec.Header().Get("Content-Encoding"); ce != "" {
		t.Fatalf("content-encoding leaked: %q", ce)
	}
}

func TestMaxBodySize(t *testing.T) {
	srv := testServer(map[string]string{"FETCHD_MAX_BODY_BYTES": "10"})
	srv.cfg.AllowPrivate = true
	big := strings.Repeat("a", 64)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "64")
		_, _ = w.Write([]byte(big))
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	// Known length over limit is rejected up front with 413.
	if rec.Code != 413 {
		t.Fatalf("got %d want 413 (%s)", rec.Code, rec.Body.String())
	}
}

func TestMaxBodySizeChunkedTruncates(t *testing.T) {
	srv := testServer(map[string]string{"FETCHD_MAX_BODY_BYTES": "10"})
	srv.cfg.AllowPrivate = true
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Flush headers first so Go uses chunked encoding with no Content-Length.
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte(strings.Repeat("b", 64)))
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 200 {
		t.Fatalf("got %d (%s)", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 11 { // limit+1 cap read, truncated stream
		t.Fatalf("truncated length: %d", rec.Body.Len())
	}
}

func TestRedirectLimit(t *testing.T) {
	mkChain := func(t *testing.T) (entry, dest *httptest.Server) {
		t.Helper()
		dest = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("final"))
		}))
		t.Cleanup(dest.Close)
		entry = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, dest.URL, http.StatusFound)
		}))
		t.Cleanup(entry.Close)
		return entry, dest
	}

	// MaxRedirects=0: no following, 302 passes through with Location.
	srv := testServer(map[string]string{"FETCHD_MAX_REDIRECTS": "0"})
	srv.cfg.AllowPrivate = true
	entry, _ := mkChain(t)
	rec := doFetch(t, srv, http.MethodGet, entry.URL, authHeaders())
	if rec.Code != 302 {
		t.Fatalf("no-follow: got %d want 302", rec.Code)
	}
	if rec.Header().Get("Location") == "" {
		t.Fatal("no-follow: Location not forwarded")
	}

	// Default MaxRedirects=10: single redirect followed to 200.
	srv2 := testServer(nil)
	srv2.cfg.AllowPrivate = true
	entry2, dest := mkChain(t)
	rec = doFetch(t, srv2, http.MethodGet, entry2.URL, authHeaders())
	if rec.Code != 200 || rec.Body.String() != "final" {
		t.Fatalf("follow: got %d %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Fetchd-Final-URL"); got != dest.URL && got != dest.URL+"/" {
		t.Fatalf("final-url not updated: %q want %q", got, dest.URL)
	}
}

func TestUpstreamTimeout(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer slow.Close()

	// Per-request timeout_ms keeps the test fast; default 30s is not exercised.
	req := httptest.NewRequest(http.MethodPost, "/fetch",
		strings.NewReader(`{"url":`+strconv.Quote(slow.URL)+`,"timeout_ms":200}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 504 {
		t.Fatalf("got %d want 504 (%s)", rec.Code, rec.Body.String())
	}
}

func TestUpstreamStatusPassthrough(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte("nope"))
	}))
	defer upstream.Close()

	rec := doFetch(t, srv, http.MethodGet, upstream.URL, authHeaders())
	if rec.Code != 404 || rec.Body.String() != "nope" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Fetchd-Upstream-Status") != "404" {
		t.Fatalf("upstream-status: %q", rec.Header().Get("X-Fetchd-Upstream-Status"))
	}
}

func TestHEADMethod(t *testing.T) {
	srv := testServer(nil)
	srv.cfg.AllowPrivate = true
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Marker", "head-ok")
	}))
	defer upstream.Close()

	req := httptest.NewRequest(http.MethodPost, "/fetch",
		strings.NewReader(`{"url":`+strconv.Quote(upstream.URL)+`,"method":"HEAD"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("got %d (%s)", rec.Code, rec.Body.String())
	}
}
