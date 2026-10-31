package exp

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// echoServer accepts connections and echoes what it reads, until the test ends.
func echoServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func() { defer func() { _ = c.Close() }(); _, _ = io.Copy(c, c) }()
		}
	}()
	return lis.Addr().String()
}

func echo(t *testing.T, c net.Conn, msg string) error {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		return err
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	if string(buf) != msg {
		t.Fatalf("echoed %q, want %q", buf, msg)
	}
	return nil
}

func TestProxyForwardsBytesBothWays(t *testing.T) {
	p, err := newProxy(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if err := echo(t, c, "hello"); err != nil {
		t.Fatalf("echo through the proxy: %v", err)
	}
}

func TestProxyKillCutsOpenConnectionsAndRefusesNewOnes(t *testing.T) {
	p, err := newProxy(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if err := echo(t, c, "before"); err != nil {
		t.Fatal(err)
	}

	p.Kill()

	// The open connection is reset, not closed politely: the read fails with an
	// error that is not a clean end of stream.
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = c.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("a read on a connection of a killed proxy succeeded")
	}
	if errors.Is(err, io.EOF) {
		t.Errorf("read error = %v, want a reset, not a clean end of stream", err)
	}
	if nc, err := net.DialTimeout("tcp", p.Addr(), time.Second); err == nil {
		_ = nc.Close()
		t.Error("a killed proxy still accepts connections")
	}
}

func TestProxyCloseIsIdempotentAfterKill(t *testing.T) {
	p, err := newProxy(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	p.Kill()
	p.Kill()
	p.Close()
	p.Close()
}
