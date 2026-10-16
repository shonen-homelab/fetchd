package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Error codes returned in {"error":code,"message"} envelopes.
const (
	errUnauthorized = "unauthorized"
	errBadRequest   = "bad_request"
	errBlocked      = "blocked_target"
	errTooLarge     = "response_too_large"
	errUpstream     = "upstream_error"
	errTimeout      = "upstream_timeout"
)

// fetchRequest is the POST /fetch envelope. PR3 adds queuing, PR4 retries;
// this revision performs one upstream attempt with streaming passthrough.
type fetchRequest struct {
	URL       string            `json:"url"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	TimeoutMs *int64            `json:"timeout_ms"`
}

// Headers forwarded from upstream. Content-Encoding is intentionally absent:
// we decompress gzip and drop it (fetchd always returns identity bytes).
var forwardedResponseHeaders = []string{
	"Content-Type", "Content-Length", "Last-Modified", "ETag", "Location",
}

// Hop-by-hop headers must never be forwarded from the client.
var hopHeaders = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true,
	"proxy-authorization": true, "te": true, "trailer": true,
	"transfer-encoding": true, "upgrade": true,
}

// errorResponse is fetchd's own error envelope. Upstream bodies stream as-is.
type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// writeError renders a fetchd-side failure as JSON.
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: code, Message: message})
}

// NewServer builds routes.
func NewServer(cfg Config, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, mux: http.NewServeMux(), log: log}
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /fetch", s.requireAuth(s.handleFetchGet))
	s.mux.HandleFunc("POST /fetch", s.requireAuth(s.handleFetchPost))
	return s
}

func (s *Server) clientFor(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		DialContext:           guardedDialer(s.cfg.AllowPrivate, timeout),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
	}
	checkRedirect := func(_ *http.Request, via []*http.Request) error {
		if uint(len(via)) >= s.cfg.MaxRedirects {
			return http.ErrUseLastResponse
		}
		return nil
	}
	return &http.Client{Transport: transport, CheckRedirect: checkRedirect}
}

func (s *Server) handleFetchGet(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("url")
	if raw == "" {
		writeError(w, http.StatusBadRequest, errBadRequest, "missing url query parameter")
		return
	}
	s.serveFetch(w, r, fetchRequest{URL: raw, Method: http.MethodGet})
}

func (s *Server) handleFetchPost(w http.ResponseWriter, r *http.Request) {
	var req fetchRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, errBadRequest, "invalid JSON body")
		return
	}
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, errBadRequest, "missing url field")
		return
	}
	if req.Method == "" {
		req.Method = http.MethodGet
	}
	s.serveFetch(w, r, req)
}

// parseFetchTarget validates method, URL, and scheme (http/https only).
func parseFetchTarget(req fetchRequest) (*url.URL, string, *errorResponse, int) {
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method != http.MethodGet && method != http.MethodHead {
		return nil, "", &errorResponse{Error: errBadRequest, Message: "method must be GET or HEAD"}, http.StatusBadRequest
	}
	u, err := url.ParseRequestURI(strings.TrimSpace(req.URL))
	if err != nil {
		return nil, "", &errorResponse{Error: errBadRequest, Message: "invalid url"}, http.StatusBadRequest
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, "", &errorResponse{Error: errBadRequest, Message: "only http and https URLs are allowed"}, http.StatusBadRequest
	}
	if u.Host == "" {
		return nil, "", &errorResponse{Error: errBadRequest, Message: "url must have a host"}, http.StatusBadRequest
	}
	return u, method, nil, 0
}

// serveFetch performs one upstream attempt and streams the result.
func (s *Server) serveFetch(w http.ResponseWriter, r *http.Request, req fetchRequest) {
	target, method, ferr, status := parseFetchTarget(req)
	if ferr != nil {
		writeError(w, status, ferr.Error, ferr.Message)
		return
	}

	timeout := s.cfg.Timeout
	if req.TimeoutMs != nil {
		if *req.TimeoutMs <= 0 {
			writeError(w, http.StatusBadRequest, errBadRequest, "timeout_ms must be positive")
			return
		}
		timeout = time.Duration(*req.TimeoutMs) * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	upReq, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		writeError(w, http.StatusBadRequest, errBadRequest, "invalid url")
		return
	}
	if s.cfg.UserAgent != "" {
		upReq.Header.Set("User-Agent", s.cfg.UserAgent)
	}
	upReq.Header.Set("Accept-Encoding", "gzip")
	for k, v := range req.Headers {
		if strings.TrimSpace(v) == "" {
			continue
		}
		lk := strings.ToLower(k)
		if hopHeaders[lk] || lk == "host" || lk == "content-length" {
			continue
		}
		upReq.Header.Set(k, v)
	}

	resp, err := s.clientFor(timeout).Do(upReq)
	if err != nil {
		s.writeUpstreamError(w, target, err)
		return
	}
	defer resp.Body.Close()

	gzipped := strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip")
	if !gzipped && resp.ContentLength > s.cfg.MaxBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, errTooLarge,
			"upstream response exceeds limit of "+strconv.FormatInt(s.cfg.MaxBodyBytes, 10)+" bytes")
		return
	}

	body := resp.Body
	if gzipped {
		if gr, gerr := gzip.NewReader(resp.Body); gerr == nil {
			defer gr.Close()
			body = gr
		}
		// Undecodable gzip (e.g. identity mislabel) streams raw, header dropped.
	}

	for _, h := range forwardedResponseHeaders {
		if v := resp.Header.Get(h); v != "" {
			// A gzipped Content-Length describes compressed bytes, not the
			// decompressed stream, so never forward it.
			if h == "Content-Length" && gzipped {
				continue
			}
			w.Header().Set(h, v)
		}
	}
	w.Header().Set("X-Fetchd-Final-URL", resp.Request.URL.String())
	w.Header().Set("X-Fetchd-Attempts", "1")
	w.Header().Set("X-Fetchd-Upstream-Status", strconv.Itoa(resp.StatusCode))
	w.WriteHeader(resp.StatusCode)

	limited := &countReader{R: body, N: s.cfg.MaxBodyBytes + 1}
	if _, err := io.Copy(w, limited); err != nil {
		s.log.Warn("stream to client failed", "url", redactURL(target), "error", err)
		return
	}
	if limited.N <= 0 {
		s.log.Warn("upstream body exceeded limit; stream truncated",
			"url", redactURL(target), "limit", s.cfg.MaxBodyBytes)
	}
}

// writeUpstreamError maps transport failures to the error envelope.
func (s *Server) writeUpstreamError(w http.ResponseWriter, target *url.URL, err error) {
	var blocked *ssrfBlockedError
	if errors.As(err, &blocked) {
		writeError(w, http.StatusForbidden, errBlocked, blocked.reason)
		return
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			writeError(w, http.StatusGatewayTimeout, errTimeout, "upstream timed out")
			return
		}
		if errors.As(urlErr.Err, &blocked) {
			writeError(w, http.StatusForbidden, errBlocked, blocked.reason)
			return
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusGatewayTimeout, errTimeout, "upstream timed out")
		return
	}
	s.log.Warn("upstream unreachable", "url", redactURL(target), "error", err)
	writeError(w, http.StatusBadGateway, errUpstream, "upstream unreachable: "+firstLine(err.Error()))
}

// countReader caps how much we'll stream before truncating.
type countReader struct {
	R io.Reader
	N int64
}

func (c *countReader) Read(p []byte) (int, error) {
	if c.N <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > c.N {
		p = p[:c.N]
	}
	n, err := c.R.Read(p)
	c.N -= int64(n)
	return n, err
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// redactURL strips credentials and query strings; never log API keys.
func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	c.User = nil
	c.RawQuery = ""
	return c.String()
}
