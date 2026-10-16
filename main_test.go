package main

import "testing"

func TestHealthAddr(t *testing.T) {
	cases := map[string]string{
		"":                 "127.0.0.1:8080",
		":8080":            "127.0.0.1:8080",
		":9090":            "127.0.0.1:9090",
		"0.0.0.0:8080":     "0.0.0.0:8080",
		"127.0.0.1:8080":   "127.0.0.1:8080",
		"example.com:8080": "example.com:8080",
	}
	for in, want := range cases {
		if got := healthAddr(in); got != want {
			t.Errorf("healthAddr(%q) = %q want %q", in, got, want)
		}
	}
}
