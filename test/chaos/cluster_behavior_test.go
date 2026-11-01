//go:build chaos

package chaos

import (
	"bufio"
	"errors"
	"io"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// clusterOf starts a cluster of the given size, with the environment's
// PostgreSQL and toxiproxy-server.
func clusterOf(t *testing.T, brokers, workers int) *Cluster {
	t.Helper()
	cfg := clusterConfig(t, 1)
	cfg.Brokers, cfg.Workers = brokers, workers
	return StartCluster(t, cfg)
}

// smallCluster starts one broker and no worker: enough to test what a
// partition and Heal do to the proxies and to the readiness of a broker.
func smallCluster(t *testing.T) *Cluster { return clusterOf(t, 1, 0) }

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

// The frames of HTTP/2 that the partition test sends to a broker's gRPC port,
// and what it expects back. A frame is a 9-byte header (3 bytes of length, a
// type, flags, 4 bytes of stream) and a payload.
const (
	h2Preface  = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	h2TypePing = 6
	h2FlagACK  = 1
	h2MaxFrame = 16 << 10
)

// h2Frame builds a frame on stream 0.
func h2Frame(typ, flags byte, payload []byte) []byte {
	n := len(payload)
	return append([]byte{byte(n >> 16), byte(n >> 8), byte(n), typ, flags, 0, 0, 0, 0}, payload...)
}

// readH2Frame reads one frame.
func readH2Frame(conn net.Conn) (typ, flags byte, payload []byte, err error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, 0, nil, err
	}
	n := int(header[0])<<16 | int(header[1])<<8 | int(header[2])
	if n > h2MaxFrame {
		return 0, 0, nil, errors.New("a frame of an absurd length: the connection is not HTTP/2")
	}
	payload = make([]byte, n)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, 0, nil, err
	}
	return header[3], header[4], payload, nil
}

// ping sends a PING with the payload, and returns whether the matching PING
// ACK arrived within within. Any other frame that comes first is skipped: the
// broker's SETTINGS and their acknowledgement, for one.
func ping(t *testing.T, conn net.Conn, payload []byte, within time.Duration) bool {
	t.Helper()
	if _, err := conn.Write(h2Frame(h2TypePing, 0, payload)); err != nil {
		t.Fatalf("write a PING: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatal(err)
	}
	for {
		typ, flags, got, err := readH2Frame(conn)
		if err != nil {
			return false
		}
		if typ == h2TypePing && flags&h2FlagACK != 0 && string(got) == string(payload) {
			return true
		}
	}
}

// establishedConnection opens a connection to the proxy of the broker before
// any partition, the way a pool or a channel that has been up for a while is
// connected: it reads the server's first bytes, which are its SETTINGS frame,
// sends the client's preface and checks that the broker answers a PING.
//
// It first waits until the proxy carries no toxic. The end of an earlier
// partition removes its two toxics one after the other, and the first removal
// already closes the connections that went through the cut, so a caller that
// saw its connection close may come here while the second toxic is still on:
// a connection made then would lose the server's SETTINGS to it and be closed
// by its removal.
func establishedConnection(t *testing.T, c *Cluster) net.Conn {
	t.Helper()
	waitNoToxics(t, c, BrokerProxy(0), 5*time.Second)
	conn, err := net.DialTimeout("tcp", c.BrokerAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("connect to the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if typ, _, _, err := readH2Frame(conn); err != nil || typ != 4 { // SETTINGS
		t.Fatalf("the server's first frame: type %d, error %v; want its SETTINGS", typ, err)
	}
	if _, err := conn.Write(append([]byte(h2Preface), h2Frame(4, 0, nil)...)); err != nil {
		t.Fatalf("write the client preface: %v", err)
	}
	if !ping(t, conn, []byte("before!!"), 5*time.Second) {
		t.Fatal("before any partition the broker did not answer a PING")
	}
	return conn
}

// waitNoToxics waits until the proxy named proxy carries no toxic, and fails
// the test if it still does after within.
func waitNoToxics(t *testing.T, c *Cluster, proxy string, within time.Duration) {
	t.Helper()
	c.mu.Lock()
	p, ok := c.proxies[proxy]
	c.mu.Unlock()
	if !ok {
		t.Fatalf("no proxy %q", proxy)
	}
	deadline := time.Now().Add(within)
	for {
		toxics, err := p.Toxics()
		if err != nil {
			t.Fatalf("list the toxics of %s: %v", proxy, err)
		}
		if len(toxics) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still carries %d toxic(s) %s after the partition ended", proxy, len(toxics), within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requireSilent checks that nothing comes back through a partition: a PING that
// the broker would answer gets no answer, and the connection stays open.
func requireSilent(t *testing.T, conn net.Conn) {
	t.Helper()
	if ping(t, conn, []byte("during!!"), time.Second) {
		t.Fatal("the broker answered a PING through the partition")
	}
	// ping reports false for a closed connection too, so look at why it failed.
	if err := conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("during the partition the connection should be open and silent, but Read returned %v", err)
	}
}

// TestPartitionSilencesAConnectionThatWasOpenBeforeIt covers what the pool of
// pgx and the channels of gRPC are: connections that were made, and used,
// before the cut. The connection must go silent during the cut, with the
// broker's answers dropped, and be closed when the toxics are removed, whether
// the partition ends by itself or Heal ends it.
func TestPartitionSilencesAConnectionThatWasOpenBeforeIt(t *testing.T) {
	c := smallCluster(t)

	t.Run("the partition ends by itself", func(t *testing.T) {
		conn := establishedConnection(t, c)
		c.Partition(BrokerProxy(0), 5*time.Second)
		requireSilent(t, conn)
		if !readsUntilClosed(t, conn, 15*time.Second) {
			t.Fatal("the partition ended and the connection that was open before it is still open")
		}
	})

	t.Run("Heal ends the partition", func(t *testing.T) {
		conn := establishedConnection(t, c)
		c.Partition(BrokerProxy(0), time.Hour)
		requireSilent(t, conn)
		c.Heal()
		if !readsUntilClosed(t, conn, 5*time.Second) {
			t.Fatal("5s after Heal the connection that was open before the partition is still open")
		}
	})
}

// TestKillWorkerRecordsOnlyTheKilledStartsHistoryFile pins the recording that
// the attribution of torn history tails rests on: a worker ended with SIGKILL
// has its history file recorded, by base name; the file of the start that
// replaces it is not recorded just because the worker before it was; and a
// worker that exits when it is asked to is never recorded.
func TestKillWorkerRecordsOnlyTheKilledStartsHistoryFile(t *testing.T) {
	c := clusterOf(t, 1, 1)
	killed := func(want ...string) {
		t.Helper()
		wantSet := map[string]bool{}
		for _, name := range want {
			wantSet[name] = true
		}
		if got := c.KilledHistoryFiles(); !maps.Equal(got, wantSet) {
			t.Fatalf("killed history files = %v, want %v", got, wantSet)
		}
	}
	killed()

	c.KillWorker(0)
	killed("worker-w0-g1.jsonl")

	c.RestartWorker(0)
	killed("worker-w0-g1.jsonl")
	g2 := filepath.Join(c.HistoryDir(), "worker-w0-g2.jsonl")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(g2); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the restarted worker never created %s", g2)
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.StopProcesses(stopGrace)
	killed("worker-w0-g1.jsonl")
}

// fakeProcess starts a process from a shell script, which must print a line
// when it is set up, and returns it as an instance of the cluster, with the
// history file name given. It returns after that line has been read, so that a
// signal sent next meets the disposition that the script has set.
func fakeProcess(t *testing.T, script, history string) *instance {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %q: %v", script, err)
	}
	if line, err := bufio.NewReader(out).ReadString('\n'); err != nil || line != "ready\n" {
		_ = cmd.Process.Kill()
		t.Fatalf("%q printed %q (%v) instead of ready", script, line, err)
	}
	inst := &instance{cmd: cmd, done: make(chan struct{}), history: history}
	go func() {
		_ = cmd.Wait()
		close(inst.done)
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return inst
}

// TestStopProcessesRecordsAWorkerThatOutlastsTheGrace pins the other place a
// worker is killed: at the end of the grace period of StopProcesses. The worker
// that ignores SIGTERM is killed and recorded; the one that exits on it and the
// broker are not. The processes are real, and need no cluster: a worker that
// ignores SIGTERM is a shell that ignores it and becomes sleep, which keeps the
// disposition.
func TestStopProcessesRecordsAWorkerThatOutlastsTheGrace(t *testing.T) {
	stubborn := fakeProcess(t, "trap '' TERM; echo ready; exec sleep 60", "worker-w0-g1.jsonl")
	obedient := fakeProcess(t, "echo ready; exec sleep 60", "worker-w1-g1.jsonl")
	broker := fakeProcess(t, "trap '' TERM; echo ready; exec sleep 60", "")
	c := &Cluster{
		t:             t,
		killedHistory: map[string]bool{},
		workers: []*node{
			{kind: kindWorker, index: 0, inst: stubborn},
			{kind: kindWorker, index: 1, inst: obedient},
		},
		brokers: []*node{{kind: kindBroker, index: 0, inst: broker}},
	}

	c.StopProcesses(500 * time.Millisecond)

	want := map[string]bool{"worker-w0-g1.jsonl": true}
	if got := c.KilledHistoryFiles(); !maps.Equal(got, want) {
		t.Errorf("killed history files = %v, want %v: only the worker that outlasted the grace was killed", got, want)
	}
	for name, inst := range map[string]*instance{"stubborn worker": stubborn, "obedient worker": obedient, "broker": broker} {
		select {
		case <-inst.done:
		default:
			t.Errorf("the %s is still running after StopProcesses", name)
		}
	}
}
