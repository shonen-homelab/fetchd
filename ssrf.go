package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"
)

var errNoAddress = errors.New("no addresses returned")

// ssrfBlockedError marks dials refused by the SSRF guard. The handler maps
// these to 403; everything else from the transport is 502/504.
type ssrfBlockedError struct{ reason string }

func (e *ssrfBlockedError) Error() string { return "blocked target: " + e.reason }

// dnsError marks resolution failure: not a policy block, so the handler maps
// it to 502 upstream-unreachable (retryable like other connection errors).
type dnsError struct {
	host string
	err  error
}

func (e *dnsError) Error() string { return fmt.Sprintf("dns resolve %q: %v", e.host, e.err) }
func (e *dnsError) Unwrap() error { return e.err }

// blockedPrefixes are never dialed unless FETCHD_ALLOW_PRIVATE=true.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("100.64.0.0/10"), // CGNAT
	netip.MustParsePrefix("0.0.0.0/32"),    // unspecified (dials localhost)
	netip.MustParsePrefix("224.0.0.0/4"),   // multicast
	netip.MustParsePrefix("192.0.2.0/24"),  // documentation (TEST-NET-1)
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("::/128"),        // unspecified
	netip.MustParsePrefix("fc00::/7"),      // ULA
	netip.MustParsePrefix("fe80::/10"),     // link-local
	netip.MustParsePrefix("ff00::/8"),      // multicast
	netip.MustParsePrefix("2001:db8::/32"), // documentation
	netip.MustParsePrefix("64:ff9b::/96"),  // NAT64 (embeds IPv4)
	netip.MustParsePrefix("2002::/16"),     // 6to4 (embeds IPv4)
}

// ipBlocked reports whether an address must never be dialed.
func ipBlocked(ip netip.Addr) bool {
	ip = ip.Unmap()
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	// Belt and suspenders in case a range above is ever trimmed.
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()
}

// resolveIPs returns every address a dial target may use. Literals pass
// through; names are resolved here so each result can be checked.
func resolveIPs(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(strings.TrimSuffix(host, "%")); err == nil {
		return []netip.Addr{ip.WithZone("")}, nil
	} else if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip.WithZone("")}, nil
	}
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	out := make([]netip.Addr, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.WithZone(""))
	}
	return out, nil
}

// guardedDialer returns a DialContext that refuses blocked addresses.
// Resolution happens inside the guard and dials go to the checked IPs, so
// redirects and DNS rebinding between check and dial are covered. The same
// dialer serves every request, including redirect follow-ups.
// allowPrivate is a predicate (not a bool) so the shared Transport always
// honors the server's current setting.
func guardedDialer(allowPrivate func() bool, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	base := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if allowPrivate() {
			return base.DialContext(ctx, network, address)
		}
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, &ssrfBlockedError{reason: "unsupported network " + network}
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, &ssrfBlockedError{reason: "invalid address"}
		}
		ips, err := resolveIPs(ctx, host)
		if err != nil {
			// DNS failure is not a policy block: upstream-unreachable (502, retryable).
			return nil, &dnsError{host: host, err: err}
		}
		if len(ips) == 0 {
			return nil, &dnsError{host: host, err: errNoAddress}
		}
		var filtered []netip.Addr
		for _, ip := range ips {
			if network == "tcp4" && !ip.Unmap().Is4() {
				continue
			}
			if network == "tcp6" && !ip.Is6() {
				continue
			}
			if ipBlocked(ip) {
				return nil, &ssrfBlockedError{reason: fmt.Sprintf("host %q resolves to a blocked address", host)}
			}
			filtered = append(filtered, ip)
		}
		if len(filtered) == 0 {
			return nil, &ssrfBlockedError{reason: fmt.Sprintf("host %q has no usable address", host)}
		}
		var firstErr error
		for _, ip := range filtered {
			conn, derr := base.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if derr == nil {
				return conn, nil
			}
			if firstErr == nil {
				firstErr = derr
			}
		}
		return nil, firstErr
	}
}
