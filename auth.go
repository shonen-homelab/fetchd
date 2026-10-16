package main

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// requestKeys returns every API key candidate the client offered.
// Either header alone is sufficient; both are checked.
func requestKeys(r *http.Request) []string {
	var out []string
	if h := r.Header.Get("Authorization"); h != "" {
		if rest, ok := strings.CutPrefix(h, "Bearer "); ok && rest != "" {
			out = append(out, rest)
		}
	}
	if k := r.Header.Get("X-API-Key"); k != "" {
		out = append(out, k)
	}
	return out
}

// validKey compares candidates against configured keys in constant time.
func validKey(configured, candidates []string) bool {
	for _, cand := range candidates {
		if cand == "" {
			continue
		}
		for _, key := range configured {
			if subtle.ConstantTimeCompare([]byte(cand), []byte(key)) == 1 {
				return true
			}
		}
	}
	return false
}

// requireAuth gates fetch endpoints. Healthz stays open.
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(s.cfg.APIKeys) == 0 {
			if s.cfg.AllowNoAuth {
				next(w, r)
				return
			}
			writeError(w, http.StatusUnauthorized, errUnauthorized, "server misconfigured: no API keys")
			return
		}
		if !validKey(s.cfg.APIKeys, requestKeys(r)) {
			writeError(w, http.StatusUnauthorized, errUnauthorized, "invalid or missing API key")
			return
		}
		next(w, r)
	}
}
