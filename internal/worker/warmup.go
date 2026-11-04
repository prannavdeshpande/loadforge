package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"
)

// maxWarmConns caps the TCP connections opened at PREPARE, so that preparing
// a run with a large in-flight cap does not flood the target with connects.
const maxWarmConns = 64

// warmPool holds TCP connections opened at PREPARE (design.md §6.2). Warm-up
// is DNS resolution and TCP connects only: no HTTP request reaches the
// target before t0. The run's http.Transport takes these connections from
// its dialer before dialing new ones.
type warmPool struct {
	addr   string // host:port as the transport will ask for it
	dialer *net.Dialer

	mu    sync.Mutex
	conns []net.Conn
}

// warmUp resolves target's host and opens up to n connections to it. It
// fails if the name does not resolve or no connection could be opened, so
// that an unreachable target is refused at PREPARE.
func warmUp(ctx context.Context, target string, n int, dialer *net.Dialer) (*warmPool, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	ips, err := net.DefaultResolver.LookupHost(ctx, u.Hostname())
	if err != nil {
		return nil, fmt.Errorf("warm-up: resolve %s: %w", u.Hostname(), err)
	}

	p := &warmPool{addr: net.JoinHostPort(u.Hostname(), port), dialer: dialer}
	var (
		wg   sync.WaitGroup
		errs = make([]error, n)
	)
	for i := range n {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			c, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip, port))
			if err != nil {
				errs[i] = err
				return
			}
			p.mu.Lock()
			p.conns = append(p.conns, c)
			p.mu.Unlock()
		}(ips[i%len(ips)])
	}
	wg.Wait()
	if len(p.conns) == 0 {
		return nil, fmt.Errorf("warm-up: connect to %s: %w", p.addr, errors.Join(errs...))
	}
	return p, nil
}

// size is the number of warm connections not yet taken.
func (p *warmPool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// take returns a warm connection to addr that is still open, or nil. The
// target may have closed an idle connection between PREPARE and the
// request; handing that one out would fail a request through no fault of
// the target, so dead ones are discarded.
func (p *warmPool) take(addr string) net.Conn {
	if addr != p.addr {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.conns) > 0 {
		c := p.conns[len(p.conns)-1]
		p.conns = p.conns[:len(p.conns)-1]
		if connAlive(c) {
			return c
		}
		c.Close()
	}
	return nil
}

// DialContext is the run's http.Transport dialer: warm connections first,
// then new ones.
func (p *warmPool) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network == "tcp" {
		if c := p.take(addr); c != nil {
			return c, nil
		}
	}
	return p.dialer.DialContext(ctx, network, addr)
}

// Close closes the connections never taken.
func (p *warmPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

// warmTimeout bounds warm-up by the PREPARE deadline, leaving time to ack.
func warmTimeout(deadline time.Time) time.Duration {
	d := time.Until(deadline) - 500*time.Millisecond
	return min(max(d, 100*time.Millisecond), 5*time.Second)
}
