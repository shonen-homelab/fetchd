package main

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Error codes for queue outcomes.
const (
	errQueueFull    = "queue_full"
	errQueueTimeout = "queue_timeout"
)

// gate controls admission: a global concurrency semaphore, a bounded queue,
// and per-host concurrency + spacing. Non-positive limits disable that check.
type gate struct {
	maxConc         int
	maxQueue        int
	queueTimeout    time.Duration
	perHostConc     int
	perHostInterval time.Duration

	mu       sync.Mutex
	inflight int
	waiting  int

	hosts map[string]*hostSlot
}

type hostSlot struct {
	inflight int
	waiters  int
	next     time.Time // earliest next start (spacing)
	lastUse  time.Time
}

// newGate builds admission control from config.
func newGate(cfg Config) *gate {
	return &gate{
		maxConc:         int(cfg.MaxConcurrency),
		maxQueue:        int(cfg.MaxQueue),
		queueTimeout:    cfg.QueueTimeout,
		perHostConc:     int(cfg.PerHostConcurrency),
		perHostInterval: cfg.PerHostInterval,
		hosts:           map[string]*hostSlot{},
	}
}

// acquire blocks until the request may start.
// Returns release (call exactly once after ok=true), ok, and full:
//   - ok=true: admitted; call release when done.
//   - full=true: queue full; caller responds 429 with Retry-After.
//   - ok=false, full=false: queue timeout (caller responds 503) or client
//     cancellation (caller just returns).
//
// Fairness: waiters already queued for a host beat newcomers for that host,
// so a steady stream of arrivals cannot starve a waiter indefinitely.
// There is no strict FIFO across hosts or across waiters waking together;
// waiters re-poll on a short tick and compete evenly.
func (g *gate) acquire(ctx context.Context, host string) (release func(), ok bool, full bool) {
	host = normalizeHost(host)
	var timer *time.Timer
	var timerCh <-chan time.Time
	if g.queueTimeout > 0 {
		timer = time.NewTimer(g.queueTimeout)
		defer timer.Stop()
		timerCh = timer.C
	}

	queued := false
	for {
		g.mu.Lock()
		slot := g.slotLocked(host)
		// First pass is a newcomer: yield to waiters already queued for
		// this host. Later passes are a waiter competing evenly.
		if (queued || slot.waiters == 0) && g.admittableLocked(slot) {
			now := time.Now()
			g.inflight++
			slot.inflight++
			slot.next = now.Add(g.perHostInterval)
			slot.lastUse = now
			g.mu.Unlock()
			return func() { g.release(host) }, true, false
		}
		if g.maxQueue <= 0 || g.waiting >= g.maxQueue {
			g.mu.Unlock()
			return nil, false, true
		}
		g.waiting++
		slot.waiters++
		queued = true
		wait := time.Until(slot.next)
		g.mu.Unlock()

		select {
		case <-ctx.Done():
			g.unwait(host)
			return nil, false, false
		case <-timerCh:
			g.unwait(host)
			return nil, false, false
		case <-time.After(g.pollDelay(wait)):
			g.unwait(host)
			// Re-loop to re-evaluate.
		}
		if timerCh == nil {
			// QueueTimeout <= 0: try once, never wait.
			return nil, false, false
		}
	}
}

// admittableLocked reports whether a request may start right now.
func (g *gate) admittableLocked(slot *hostSlot) bool {
	if g.maxConc > 0 && g.inflight >= g.maxConc {
		return false
	}
	if g.perHostConc > 0 && slot.inflight >= g.perHostConc {
		return false
	}
	if g.perHostInterval > 0 && time.Now().Before(slot.next) {
		return false
	}
	return true
}

// unwait leaves the queue (timeout, cancel, or re-poll).
func (g *gate) unwait(host string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.waiting--
	if s, ok := g.hosts[host]; ok {
		s.waiters--
	}
}

// pollDelay sleeps until per-host spacing elapses, else re-polls shortly.
func (g *gate) pollDelay(hostWait time.Duration) time.Duration {
	if hostWait > 0 {
		return hostWait
	}
	return 5 * time.Millisecond
}

func (g *gate) slotLocked(host string) *hostSlot {
	if s, ok := g.hosts[host]; ok {
		return s
	}
	// Opportunistic sweep: drop slots that owe no spacing and hold nothing.
	now := time.Now()
	for h, s := range g.hosts {
		if s.inflight <= 0 && s.waiters <= 0 && !now.Before(s.next) {
			delete(g.hosts, h)
		}
	}
	s := &hostSlot{}
	g.hosts[host] = s
	return s
}

func (g *gate) release(host string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inflight--
	if s, ok := g.hosts[host]; ok {
		s.inflight--
		s.lastUse = time.Now()
		if s.inflight <= 0 && s.waiters <= 0 && !time.Now().Before(s.next) {
			delete(g.hosts, host) // prune only when no spacing is owed
		}
	}
}

// normalizeHost lowercases the host for per-host accounting.
func normalizeHost(host string) string { return strings.ToLower(host) }

// hostCount reports tracked hosts (tests + debug).
func (g *gate) hostCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.hosts)
}
