package exp

import (
	"io"
	"net"
	"sync"
)

// proxy forwards TCP connections to a target and can be cut off all at once.
//
// The broker offers no way to close its listener abruptly, and the experiment
// that kills a broker needs exactly that: connections reset in the middle of a
// call, and a port that refuses new ones. Fronting the broker with a proxy that
// does so gives the clients what a crashed broker process gives them, while the
// broker itself runs on.
type proxy struct {
	lis    net.Listener
	target string

	mu     sync.Mutex
	conns  map[net.Conn]bool // every open connection, and whether it is a client's
	killed bool
	wg     sync.WaitGroup
}

// newProxy listens on a loopback port and forwards to target.
func newProxy(target string) (*proxy, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{lis: lis, target: target, conns: map[net.Conn]bool{}}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the address clients connect to.
func (p *proxy) Addr() string { return p.lis.Addr().String() }

func (p *proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.lis.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		if !p.track(client, upstream) {
			resetAndClose(client)
			resetAndClose(upstream)
			return
		}
		p.wg.Add(2)
		go p.pipe(client, upstream)
		go p.pipe(upstream, client)
	}
}

// track records a client connection and its upstream, unless the proxy was
// killed in the meantime.
func (p *proxy) track(client, upstream net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.killed {
		return false
	}
	p.conns[client] = true
	p.conns[upstream] = false
	return true
}

// pipe copies from src to dst until either side ends, then closes both.
func (p *proxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

// Kill stops listening and resets every open connection, on both sides, and
// returns the number of client connections it reset. It may be called more than
// once; the later calls do nothing and return 0.
func (p *proxy) Kill() int {
	p.mu.Lock()
	if p.killed {
		p.mu.Unlock()
		return 0
	}
	p.killed = true
	open := make([]net.Conn, 0, len(p.conns))
	clients := 0
	for c, isClient := range p.conns {
		open = append(open, c)
		if isClient {
			clients++
		}
	}
	p.mu.Unlock()
	_ = p.lis.Close()
	// Every connection is made to reset on close before any is closed. The pipe
	// goroutine of a connection closes both of its ends as soon as one is cut, so
	// an end that was not yet set to reset would be closed with an orderly FIN,
	// and its client would see a clean end of stream instead of a reset.
	for _, c := range open {
		resetOnClose(c)
	}
	for _, c := range open {
		_ = c.Close()
	}
	return clients
}

// Close kills the proxy and waits for its goroutines to end.
func (p *proxy) Close() {
	p.Kill()
	p.wg.Wait()
}

// resetOnClose makes closing c send a reset, as the kernel does for the sockets
// of a crashed process, instead of the orderly shutdown that Close alone sends.
func resetOnClose(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
}

// resetAndClose is resetOnClose followed by Close.
func resetAndClose(c net.Conn) {
	resetOnClose(c)
	_ = c.Close()
}
