package worker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/transport"
)

// countingTarget counts TCP connections and HTTP requests reaching it.
type countingTarget struct {
	srv      *httptest.Server
	conns    atomic.Int64
	requests atomic.Int64
}

func newCountingTarget(t *testing.T) *countingTarget {
	ct := &countingTarget{}
	ct.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ct.requests.Add(1)
		time.Sleep(time.Millisecond)
	}))
	ct.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			ct.conns.Add(1)
		}
	}
	ct.srv.Start()
	t.Cleanup(ct.srv.Close)
	return ct
}

// eventually waits for cond, for server-side counters that lag the client.
func eventually(cond func() bool) bool {
	for range 100 {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func TestPrepareWarmsTCPButSendsNoHTTP(t *testing.T) {
	ct := newCountingTarget(t)
	h := newHarness(t, &memPub{})
	h.target = ct.srv.URL
	expectStatus(t, h.send(h.prepare("run-1", 1, 1, func(p *transport.Prepare) { p.MaxInFlight = 8 })), transport.StatusReady)

	if !eventually(func() bool { return ct.conns.Load() == 8 }) {
		t.Errorf("target saw %d connections after PREPARE, want 8 (one per in-flight slot)", ct.conns.Load())
	}
	time.Sleep(100 * time.Millisecond)
	if n := ct.requests.Load(); n != 0 {
		t.Fatalf("target received %d HTTP requests before START: warm-up must be TCP only", n)
	}

	h.heartbeats("run-1", 1)
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 2, 1)), transport.StatusArmed)
	time.Sleep(400 * time.Millisecond)
	a := h.send(cmd(transport.CmdStop, "run-1", 3, 1))
	expectStatus(t, a, transport.StatusStopped)

	var intended, ok int64
	for _, s := range h.pub.delivered() {
		intended += s.Intended
		ok += s.OK
	}
	if ok == 0 || ok != intended || ct.requests.Load() != ok {
		t.Errorf("intended=%d ok=%d, target saw %d requests", intended, ok, ct.requests.Load())
	}
	// The requests ran over the warm connections instead of dialing anew.
	if n := ct.conns.Load(); n > 10 {
		t.Errorf("target saw %d connections for 8 in-flight slots: warm connections were not used", n)
	}
	t.Logf("connections=%d requests=%d", ct.conns.Load(), ct.requests.Load())
}

func TestWarmUpIsCapped(t *testing.T) {
	ct := newCountingTarget(t)
	h := newHarness(t, &memPub{})
	h.target = ct.srv.URL
	expectStatus(t, h.send(h.prepare("run-1", 1, 1, func(p *transport.Prepare) { p.MaxInFlight = 500 })), transport.StatusReady)
	if !eventually(func() bool { return ct.conns.Load() == maxWarmConns }) {
		t.Errorf("target saw %d connections, want the cap of %d", ct.conns.Load(), maxWarmConns)
	}
}

func TestRefusedPrepareDoesNotContactTarget(t *testing.T) {
	ct := newCountingTarget(t)
	h := newHarness(t, &memPub{}, func(c *Config) { c.RateCap = 1000 })
	h.target = ct.srv.URL
	expectStatus(t, h.send(h.prepare("run-1", 1, 1, func(p *transport.Prepare) { p.Rate = 1001 })), transport.StatusNack)
	expectStatus(t, h.send(h.prepare("run-2", 1, 1, func(p *transport.Prepare) { p.DrainTimeoutMS = 1 })), transport.StatusNack)
	// Busy: run-3 holds the worker, so run-4 must not be warmed.
	expectStatus(t, h.send(h.prepare("run-3", 1, 1, func(p *transport.Prepare) { p.MaxInFlight = 2 })), transport.StatusReady)
	expectStatus(t, h.send(h.prepare("run-4", 1, 1, func(p *transport.Prepare) { p.MaxInFlight = 2 })), transport.StatusNack)
	time.Sleep(100 * time.Millisecond)
	if n := ct.conns.Load(); n != 2 {
		t.Errorf("target saw %d connections, want only run-3's 2", n)
	}
}

func TestPrepareNacksUnreachableTarget(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedPort := "http://" + l.Addr().String()
	l.Close()

	h := newHarness(t, &memPub{})
	for name, target := range map[string]string{
		"connection refused": closedPort,
		"unresolvable name":  "http://loadforge-test.invalid/", // .invalid never resolves (RFC 6761)
	} {
		h.target = target
		a := h.send(h.prepare("run-"+strings.Fields(name)[0], 1, 1))
		expectStatus(t, a, transport.StatusNack)
		if !strings.Contains(a.Detail, "warm-up") {
			t.Errorf("%s: detail %q does not name the warm-up", name, a.Detail)
		}
	}
}

func TestWarmPoolDiscardsConnectionsTheTargetClosed(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 10)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := warmUp(ctx, "http://"+l.Addr().String(), 3, &net.Dialer{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// The target closes 2 of the 3 idle connections, as an idle timeout
	// between PREPARE and START would.
	var server []net.Conn
	for range 3 {
		server = append(server, <-accepted)
	}
	server[0].Close()
	server[1].Close()
	defer server[2].Close()
	time.Sleep(50 * time.Millisecond) // let the FINs arrive

	c := p.take(p.addr)
	if c == nil {
		t.Fatal("no live warm connection handed out")
	}
	defer c.Close()
	if c.LocalAddr().String() != server[2].RemoteAddr().String() {
		t.Errorf("handed out %v, want the one the target kept open (%v)", c.LocalAddr(), server[2].RemoteAddr())
	}
	if c := p.take(p.addr); c != nil {
		t.Errorf("handed out %v, which the target had closed", c.LocalAddr())
	}
	if c := p.take("elsewhere:80"); c != nil {
		t.Error("handed out a warm connection for another address")
	}
	// Once the pool is empty, the dialer falls back to a new connection.
	fresh, err := p.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		t.Fatal(err)
	}
	fresh.Close()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Error("no new connection dialed after the pool ran out")
	}
}
