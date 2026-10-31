//go:build chaos

package chaos

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// smallCluster starts one broker and no worker: enough to test what a
// partition and Heal do to the proxies and to the readiness of a broker.
func smallCluster(t *testing.T) *Cluster {
	t.Helper()
	cfg := clusterConfig(t, 1)
	cfg.Brokers, cfg.Workers = 1, 0
	return StartCluster(t, cfg)
}

// connectThroughPartition opens a connection to the proxy of the broker while
// it is partitioned, and checks that the connection is open and gets nothing
// through. The broker's own first bytes may be queued behind the cut and are
// dropped by it, so a read that returns data would be a sign that the
// partition does not hold.
func connectThroughPartition(t *testing.T, c *Cluster) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", c.BrokerAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("connect through the partitioned proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Read(make([]byte, 16)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("during the partition the connection should be open and silent, but Read returned %d bytes and %v", n, err)
	}
	return conn
}

// readsUntilClosed reads from conn until it fails, for at most within, and
// reports whether it failed because the connection was closed. A connection
// that stays open and silent runs into the deadline instead.
func readsUntilClosed(t *testing.T, conn net.Conn, within time.Duration) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	for {
		if _, err := conn.Read(buf); err != nil {
			return !errors.Is(err, os.ErrDeadlineExceeded)
		}
	}
}

// TestPartitionEndsWithTheConnectionsClosed pins that a partition that ends by
// itself closes the connections that went through it, with no reset of the
// proxy on top: the connection has lost data and must not be left open and
// wedged. Cluster.Partition relies on toxiproxy for that.
func TestPartitionEndsWithTheConnectionsClosed(t *testing.T) {
	c := smallCluster(t)
	c.Partition(BrokerProxy(0), 2*time.Second)
	conn := connectThroughPartition(t, c)

	if !readsUntilClosed(t, conn, 10*time.Second) {
		t.Fatal("the partition ended and the connection through it is still open")
	}
	fresh, err := net.DialTimeout("tcp", c.BrokerAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("connect through the proxy after the partition: %v", err)
	}
	_ = fresh.Close()
}

// TestHealEndsAPartitionThatIsStillActive pins the same for a partition that
// Heal ends: its scheduled end is cancelled, Heal removes the toxics, and the
// connection through the partition is closed like one whose partition ran its
// course.
func TestHealEndsAPartitionThatIsStillActive(t *testing.T) {
	c := smallCluster(t)
	c.Partition(BrokerProxy(0), time.Hour)
	conn := connectThroughPartition(t, c)

	c.Heal()

	if !readsUntilClosed(t, conn, 5*time.Second) {
		t.Fatal("5s after Heal the connection through the partition is still open")
	}
	fresh, err := net.DialTimeout("tcp", c.BrokerAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("connect through the proxy after Heal: %v", err)
	}
	_ = fresh.Close()
}

// TestHealWaitsForTheBrokersToBeReadyNow pins that Heal returns when the
// brokers are ready at that moment, not when they once were: a broker that
// served before a partition of its database is not ready while the partition
// lasts, and not at once when it ends either.
func TestHealWaitsForTheBrokersToBeReadyNow(t *testing.T) {
	c := smallCluster(t)
	if !c.Ready(0) {
		t.Fatal("the broker is not ready after StartCluster")
	}
	c.Partition(pgProxy, time.Hour)

	deadline := time.Now().Add(30 * time.Second)
	for c.Ready(0) {
		if time.Now().After(deadline) {
			t.Fatal("30s into a partition of its database the broker still says it is ready")
		}
		time.Sleep(200 * time.Millisecond)
	}

	c.Heal()

	if !c.Ready(0) {
		t.Fatal("Heal returned while the broker did not answer /readyz with 200")
	}
}

// TestBrokerThatFailsAtBootIsRecordedAndStartedAgain pins what the cluster
// does with a broker that cannot start because its database is out: it is not
// reported as a crash, the exit and its status are written to
// extra/excused-exits.txt, and the broker is started again, and is ready once
// the database is back.
func TestBrokerThatFailsAtBootIsRecordedAndStartedAgain(t *testing.T) {
	c := smallCluster(t)
	c.Partition(pgProxy, time.Hour)
	c.KillBroker(0)
	c.RestartBroker(0)

	// The new broker connects into the partition and gives up at the connect
	// timeout of its database URL.
	deadline := time.Now().Add(30 * time.Second)
	for len(c.Excused()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("30s after it was started into a partition of its database the broker has no excused exit")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := c.Unexpected(); len(got) != 0 {
		t.Errorf("a broker that failed at boot was reported as a crash: %v", got)
	}
	line := c.Excused()[0]
	if !strings.Contains(line, "exit status 1") || !strings.Contains(line, "broker-0 (start 2)") {
		t.Errorf("the excused exit does not say which broker and which status: %q", line)
	}
	data, err := os.ReadFile(filepath.Join(c.Dir(), "extra", "excused-exits.txt"))
	if err != nil || !strings.Contains(string(data), "exit status 1") {
		t.Errorf("extra/excused-exits.txt = %q (%v), want the exit status", data, err)
	}

	c.Heal()
	if !c.Ready(0) {
		t.Error("the broker is not ready after Heal")
	}
	if got := c.Unexpected(); len(got) != 0 {
		t.Errorf("unexpected exits after Heal: %v", got)
	}
}
