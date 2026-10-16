package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testServer(extra map[string]string) *Server {
	vars := map[string]string{"FETCHD_API_KEYS": "k1, k2"}
	for k, v := range extra {
		vars[k] = v
	}
	cfg, err := LoadConfig(testEnv(vars))
	if err != nil {
		panic(err)
	}
	return NewServer(cfg, testLogger())
}

func doFetch(t *testing.T, srv *Server, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if method == http.MethodPost {
		body := `{"url":` + strconv.Quote(target) + `,"method":"GET"}`
		req = httptest.NewRequest(http.MethodPost, "/fetch", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodGet, "/fetch?url="+url.QueryEscape(target), nil)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}
