//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

// The kinds of fault, which are the keys of FaultCounts.
const (
	// FaultKillBroker is a broker killed with SIGKILL.
	FaultKillBroker = "kill_broker"
	// FaultKillWorker is a worker killed with SIGKILL.
	FaultKillWorker = "kill_worker"
	// FaultStallWorker is a worker frozen with SIGSTOP.
	FaultStallWorker = "stall_worker"
	// FaultPartitionBroker is a partition of the proxy of one broker.
	FaultPartitionBroker = "partition_broker"
	// FaultPartitionPG is a partition of the proxy between the brokers and
	// PostgreSQL.
	FaultPartitionPG = "partition_pg"
	// FaultLatency is latency and jitter on one proxy.
	FaultLatency = "latency"
	// FaultRestartPostgres is a restart of the PostgreSQL container.
	FaultRestartPostgres = "restart_postgres"
)

// pgProxy is the name of the proxy between the brokers and PostgreSQL. The
// proxies between the workers and the brokers are named by BrokerProxy.
const pgProxy = "pg"

// queueName is the queue the producer fills and the workers drain.
const queueName = "chaos"

// The two kinds of node.
const (
	kindBroker = "broker"
	kindWorker = "worker"
)

// Bounds on how long the harness waits for something to happen.
const (
	readyTimeout  = 60 * time.Second
	exitTimeout   = 10 * time.Second
	buildTimeout  = 5 * time.Minute
	pgWaitTimeout = 60 * time.Second
)

// ClusterConfig describes the processes of a Cluster and where its parts are.
type ClusterConfig struct {
	// Brokers and Workers are the number of processes of each kind, and
	// Concurrency is the number of handlers each worker runs at once.
	Brokers, Workers, Concurrency int
	// TTL and Margin are the lease TTL and the margin of every worker.
	TTL, Margin time.Duration
	// PGContainer is the name of the Docker container that runs PostgreSQL,
	// which RestartPostgres restarts.
	PGContainer string
	// PGDSN is the postgres:// URL of an administrator of that server. The
	// cluster creates a database of its own on it and never touches another.
	PGDSN string
	// ToxiproxyAddr is the address of a running toxiproxy-server.
	ToxiproxyAddr string
	// Seed seeds what the worker processes draw and names the database.
	Seed uint64
}

// instance is one run of a process: a start, up to its exit.
type instance struct {
	cmd *exec.Cmd
	// done is closed after the process has exited and the cluster has
	// recorded that.
	done chan struct{}
	// ready is set once a broker answers /readyz with 200.
	ready atomic.Bool
	// expected is set before the cluster ends the process on purpose, so
	// that its exit is not reported as a crash.
	expected atomic.Bool
	// stopped says that the process is frozen with SIGSTOP. Guarded by the
	// cluster's mutex.
	stopped bool
}

// node is a broker or a worker, which is started again after it has exited.
type node struct {
	kind  string
	index int
	// grpcPort and httpPort are the ports of a broker.
	grpcPort, httpPort int
	logPath            string
	// starts counts the instances started so far.
	starts int
	// inst is the running instance, nil while the node is down. Guarded by
	// the cluster's mutex.
	inst *instance
}

func (n *node) name() string { return fmt.Sprintf("%s-%d", n.kind, n.index) }

// Cluster is the system under test: broker and worker processes, the
// PostgreSQL they share, and the Toxiproxy proxies between them. StartCluster
// builds it, and the cleanup of the test takes it down.
//
// Topology: every broker reaches PostgreSQL through the proxy "pg", and every
// worker and the test's producer reach broker i through the proxy "broker-i".
// The workers write their sink straight to PostgreSQL, like a system outside
// the queue would.
type Cluster struct {
	t   *testing.T
	cfg ClusterConfig

	dir        string // scratch space of the run
	pkgDir     string // the directory of the test; artifacts go below it
	serverBin  string
	workerBin  string
	dbName     string
	directDSN  string   // the run's database, not through a proxy
	brokerDSN  string   // the run's database, through the pg proxy
	addrs      []string // the addresses of the broker proxies
	tp         *toxiproxy.Client
	proxies    map[string]*toxiproxy.Proxy
	pool       *pgxpool.Pool
	httpClient *http.Client

	// stop is closed by Heal and Close: it ends the delayed actions that
	// faults schedule. bg counts them.
	stop     chan struct{}
	stopOnce sync.Once
	bg       sync.WaitGroup

	mu         sync.Mutex // guards the fields below
	brokers    []*node
	workers    []*node
	faults     map[string]int
	toxicSeq   int
	sigkills   int // workers ended with SIGKILL, by a fault or by the shutdown
	unexpected []string
	healed     bool
	closed     bool
}

// StartCluster builds the broker and the worker, creates a database of its own
// with the schema and the sink table, creates the proxies, and starts the
// brokers and then the workers. The brokers are ready when it returns. It
// fails the test if toxiproxy-server or PostgreSQL is not there: chaos tests
// never skip.
func StartCluster(t *testing.T, cfg ClusterConfig) *Cluster {
	t.Helper()
	pkgDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	c := &Cluster{
		t: t, cfg: cfg, pkgDir: pkgDir, dir: t.TempDir(),
		proxies:    map[string]*toxiproxy.Proxy{},
		faults:     map[string]int{},
		stop:       make(chan struct{}),
		httpClient: &http.Client{Timeout: time.Second},
	}
	t.Cleanup(c.cleanup)
	for _, sub := range []string{"bin", "logs", "history", "extra"} {
		if err := os.Mkdir(filepath.Join(c.dir, sub), 0o755); err != nil {
			t.Fatalf("create %s: %v", sub, err)
		}
	}

	c.build()
	c.connectToxiproxy()
	c.createDatabase()
	c.createProxies()

	for i := range cfg.Brokers {
		n := &node{kind: kindBroker, index: i, logPath: filepath.Join(c.dir, "logs", fmt.Sprintf("broker-%d.log", i))}
		c.brokers = append(c.brokers, n)
	}
	for i := range cfg.Workers {
		n := &node{kind: kindWorker, index: i, logPath: filepath.Join(c.dir, "logs", fmt.Sprintf("worker-%d.log", i))}
		c.workers = append(c.workers, n)
	}
	ports := freePorts(t, 2*cfg.Brokers)
	for i, n := range c.brokers {
		n.grpcPort, n.httpPort = ports[2*i], ports[2*i+1]
	}
	// The proxies of the brokers need the brokers' ports, so they come after.
	c.createBrokerProxies()

	c.mu.Lock()
	for _, n := range c.brokers {
		c.startLocked(n)
	}
	c.mu.Unlock()
	if !c.waitBrokersReady(readyTimeout) {
		t.Fatalf("the brokers were not ready within %s; see their logs", readyTimeout)
	}
	c.mu.Lock()
	for _, n := range c.workers {
		c.startLocked(n)
	}
	c.mu.Unlock()
	return c
}

// Dir is the scratch directory of the run: history files go to its history
// subdirectory, and what a failed run should keep goes to extra.
func (c *Cluster) Dir() string { return c.dir }

// HistoryDir is where every process of the run writes its history file.
func (c *Cluster) HistoryDir() string { return filepath.Join(c.dir, "history") }

// BrokerAddrs are the addresses of the proxies in front of the brokers, one
// for each broker, for clients that spread their calls over all of them.
func (c *Cluster) BrokerAddrs() []string { return slices.Clone(c.addrs) }

// Pool is a pool on the run's database, straight to PostgreSQL.
func (c *Cluster) Pool() *pgxpool.Pool { return c.pool }

// BrokerProxy is the name of the proxy in front of broker i.
func BrokerProxy(i int) string { return fmt.Sprintf("broker-%d", i) }

// ProxyNames lists every proxy: "pg" and one for each broker.
func (c *Cluster) ProxyNames() []string {
	names := []string{pgProxy}
	for i := range c.cfg.Brokers {
		names = append(names, BrokerProxy(i))
	}
	return names
}

// Up reports which brokers and which workers have a running process.
func (c *Cluster) Up() (brokers, workers []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.brokers {
		if n.inst != nil {
			brokers = append(brokers, n.index)
		}
	}
	for _, n := range c.workers {
		if n.inst != nil && !n.inst.stopped {
			workers = append(workers, n.index)
		}
	}
	return brokers, workers
}

// FaultCounts returns how many faults of each kind have been fired.
func (c *Cluster) FaultCounts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.faults))
	for k, v := range c.faults {
		out[k] = v
	}
	return out
}

// Unexpected lists the processes that exited without the cluster having
// ended them, apart from a broker that failed to start: a broker started
// while PostgreSQL is out fails and is started again, as an orchestrator
// restarts a pod that crashes at boot. Anything else is a crash.
func (c *Cluster) Unexpected() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.unexpected)
}

// WorkerKills is how many worker processes the cluster ended with SIGKILL.
// Each of them can leave one torn last line in its history file.
func (c *Cluster) WorkerKills() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sigkills
}

// SaveExtra stores data under name in the run's extra files, which a failed
// run copies to the artifacts directory.
func (c *Cluster) SaveExtra(name string, data []byte) {
	if err := os.WriteFile(filepath.Join(c.dir, "extra", name), data, 0o644); err != nil {
		c.t.Errorf("save %s: %v", name, err)
	}
}

// KillBroker ends broker i with SIGKILL and waits for it to be gone. A broker
// that is already down is left alone, and counts as no fault.
func (c *Cluster) KillBroker(i int) { c.kill(c.brokers[i], FaultKillBroker) }

// RestartBroker starts broker i again with the same flags, unless it is
// running. It does not wait for the broker to be ready.
func (c *Cluster) RestartBroker(i int) { c.ensure(c.brokers[i]) }

// KillWorker ends worker i with SIGKILL and waits for it to be gone.
func (c *Cluster) KillWorker(i int) { c.kill(c.workers[i], FaultKillWorker) }

// RestartWorker starts worker i again, as a new process with a new worker id
// and a history file of its own, unless it is running.
func (c *Cluster) RestartWorker(i int) { c.ensure(c.workers[i]) }

// StallWorker freezes worker i with SIGSTOP and thaws it with SIGCONT after d,
// in the background: the call returns at once. A worker that is down or
// already frozen is left alone.
func (c *Cluster) StallWorker(i int, d time.Duration) {
	c.mu.Lock()
	n := c.workers[i]
	inst := n.inst
	if !c.canFaultLocked() || inst == nil || inst.stopped {
		c.mu.Unlock()
		return
	}
	if err := inst.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		c.mu.Unlock()
		c.t.Errorf("SIGSTOP %s: %v", n.name(), err)
		return
	}
	inst.stopped = true
	c.faults[FaultStallWorker]++
	c.mu.Unlock()
	c.after(d, func() { c.thaw(inst) })
}

// Partition cuts the proxy named proxy for d: no data passes in either
// direction, and the connections stay open, which is how a partition looks to
// the two ends. It returns at once, and after d the toxics are removed. The
// connections that went through the cut have lost data, so the proxy is
// then switched off and on to reset them, which is how such connections end
// in practice (a timeout or a reset), rather than staying wedged.
func (c *Cluster) Partition(proxy string, d time.Duration) {
	c.mu.Lock()
	p, ok := c.proxies[proxy]
	if !ok || !c.canFaultLocked() {
		c.mu.Unlock()
		if !ok {
			c.t.Errorf("Partition: no proxy %q", proxy)
		}
		return
	}
	names := c.addToxicsLocked(p, "timeout", toxiproxy.Attributes{"timeout": 0})
	kind := FaultPartitionBroker
	if proxy == pgProxy {
		kind = FaultPartitionPG
	}
	c.faults[kind]++
	c.mu.Unlock()
	c.after(d, func() { c.removeToxics(p, names, true) })
}

// Latency adds latency and jitter to every call through the proxy named proxy
// for d, in both directions. It returns at once, and after d the toxics are
// removed.
func (c *Cluster) Latency(proxy string, latency, jitter time.Duration, d time.Duration) {
	c.mu.Lock()
	p, ok := c.proxies[proxy]
	if !ok || !c.canFaultLocked() {
		c.mu.Unlock()
		if !ok {
			c.t.Errorf("Latency: no proxy %q", proxy)
		}
		return
	}
	names := c.addToxicsLocked(p, "latency", toxiproxy.Attributes{
		"latency": latency.Milliseconds(), "jitter": jitter.Milliseconds(),
	})
	c.faults[FaultLatency]++
	c.mu.Unlock()
	c.after(d, func() { c.removeToxics(p, names, false) })
}

// RestartPostgres restarts the PostgreSQL container with docker restart and
// returns when the command has. PostgreSQL may need a moment more before it
// accepts connections.
func (c *Cluster) RestartPostgres() {
	c.mu.Lock()
	ok := c.canFaultLocked()
	c.mu.Unlock()
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "restart", c.cfg.PGContainer).CombinedOutput()
	if err != nil {
		c.t.Errorf("docker restart %s: %v\n%s", c.cfg.PGContainer, err, out)
		return
	}
	c.mu.Lock()
	c.faults[FaultRestartPostgres]++
	c.mu.Unlock()
}

// Heal ends every fault: it cancels what faults have scheduled, removes every
// toxic and resets the proxies, thaws every frozen worker, waits for
// PostgreSQL to accept connections, and starts every process that is down. It
// returns when the brokers are ready, and fails the test if they do not
// become so. No fault may be fired after it.
func (c *Cluster) Heal() {
	c.t.Helper()
	c.mu.Lock()
	c.healed = true
	c.mu.Unlock()
	c.stopOnce.Do(func() { close(c.stop) })
	c.bg.Wait()

	c.mu.Lock()
	for _, name := range c.ProxyNames() {
		p := c.proxies[name]
		toxics, err := p.Toxics()
		if err != nil {
			c.t.Errorf("list the toxics of %s: %v", name, err)
			continue
		}
		for _, tx := range toxics {
			if err := p.RemoveToxic(tx.Name); err != nil {
				c.t.Errorf("remove toxic %s of %s: %v", tx.Name, name, err)
			}
		}
		if err := p.Enable(); err != nil {
			c.t.Errorf("enable %s: %v", name, err)
		}
	}
	for _, n := range c.workers {
		c.thawLocked(n.inst)
	}
	c.mu.Unlock()

	if err := c.waitPostgres(pgWaitTimeout); err != nil {
		c.t.Fatalf("PostgreSQL did not come back after the faults: %v", err)
	}
	if !c.waitBrokersReady(readyTimeout) {
		c.t.Fatalf("the brokers were not ready %s after the faults ended; see their logs", readyTimeout)
	}
	c.mu.Lock()
	for _, n := range c.workers {
		if n.inst == nil {
			c.startLocked(n)
		}
	}
	c.mu.Unlock()
}

// StopProcesses ends every broker and worker: with SIGTERM first, which lets
// a process finish what it is doing, and with SIGKILL for those still running
// after grace. The processes are not started again.
func (c *Cluster) StopProcesses(grace time.Duration) {
	c.mu.Lock()
	var insts []*instance
	for _, n := range slices.Concat(c.workers, c.brokers) {
		if inst := n.inst; inst != nil {
			inst.expected.Store(true)
			c.thawLocked(inst)
			_ = inst.cmd.Process.Signal(syscall.SIGTERM)
			insts = append(insts, inst)
		}
	}
	c.closed = true
	c.mu.Unlock()

	timer := time.NewTimer(grace)
	defer timer.Stop()
	timedOut := false
	for _, inst := range insts {
		if !timedOut {
			select {
			case <-inst.done:
				continue
			case <-timer.C:
				timedOut = true
			}
		}
		select {
		case <-inst.done:
			continue
		default:
		}
		c.mu.Lock()
		if runsIn(c.workers, inst) {
			c.sigkills++
		}
		_ = inst.cmd.Process.Kill()
		c.mu.Unlock()
	}
	for _, inst := range insts {
		select {
		case <-inst.done:
		case <-time.After(exitTimeout):
			c.t.Errorf("a process did not exit after SIGKILL")
		}
	}
}

// runsIn reports whether inst is the running instance of one of nodes.
func runsIn(nodes []*node, inst *instance) bool {
	return slices.ContainsFunc(nodes, func(n *node) bool { return n.inst == inst })
}

// canFaultLocked reports whether a fault may be fired now. A fault after Heal
// is a bug of the test, which no later step could undo.
func (c *Cluster) canFaultLocked() bool {
	if c.healed || c.closed {
		c.t.Errorf("a fault was fired after Heal or after the processes were stopped")
		return false
	}
	return true
}

// after runs fn in the background after d, unless Heal or the end of the
// test comes first, in which case it does not run: they undo what it would.
func (c *Cluster) after(d time.Duration, fn func()) {
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			fn()
		case <-c.stop:
		}
	}()
}

// kill ends a process of n with SIGKILL, counts the fault and waits for the
// process to be gone.
func (c *Cluster) kill(n *node, fault string) {
	c.mu.Lock()
	inst := n.inst
	if inst == nil || !c.canFaultLocked() {
		c.mu.Unlock()
		return
	}
	inst.expected.Store(true)
	if err := inst.cmd.Process.Kill(); err != nil {
		c.mu.Unlock()
		c.t.Errorf("kill %s: %v", n.name(), err)
		return
	}
	c.faults[fault]++
	if n.kind == kindWorker {
		c.sigkills++
	}
	c.mu.Unlock()
	select {
	case <-inst.done:
	case <-time.After(exitTimeout):
		c.t.Errorf("%s did not exit after SIGKILL", n.name())
	}
}

// ensure starts n unless it is running.
func (c *Cluster) ensure(n *node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.inst == nil && !c.closed {
		c.startLocked(n)
	}
}

// thaw resumes inst if it is frozen.
func (c *Cluster) thaw(inst *instance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.thawLocked(inst)
}

func (c *Cluster) thawLocked(inst *instance) {
	if inst == nil || !inst.stopped {
		return
	}
	inst.stopped = false
	// An error means the process has exited since, which is nothing to undo.
	_ = inst.cmd.Process.Signal(syscall.SIGCONT)
}

// addToxicsLocked adds a toxic of type typ to both directions of p and returns
// their names.
func (c *Cluster) addToxicsLocked(p *toxiproxy.Proxy, typ string, attrs toxiproxy.Attributes) []string {
	var names []string
	for _, stream := range []string{"upstream", "downstream"} {
		c.toxicSeq++
		name := fmt.Sprintf("%s-%d-%s", typ, c.toxicSeq, stream)
		if _, err := p.AddToxic(name, typ, stream, 1, attrs); err != nil {
			c.t.Errorf("add toxic %s to %s: %v", name, p.Name, err)
			continue
		}
		names = append(names, name)
	}
	return names
}

// removeToxics removes the toxics names from p and, when reset is set, drops
// the connections of p by switching it off and on.
func (c *Cluster) removeToxics(p *toxiproxy.Proxy, names []string, reset bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, name := range names {
		if err := p.RemoveToxic(name); err != nil {
			c.t.Errorf("remove toxic %s of %s: %v", name, p.Name, err)
		}
	}
	if !reset {
		return
	}
	if err := p.Disable(); err != nil {
		c.t.Errorf("disable %s: %v", p.Name, err)
	}
	if err := p.Enable(); err != nil {
		c.t.Errorf("enable %s: %v", p.Name, err)
	}
}

// startLocked starts a new instance of n.
func (c *Cluster) startLocked(n *node) {
	n.starts++
	bin, args := c.command(n)
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.t.Errorf("open the log of %s: %v", n.name(), err)
		return
	}
	_, _ = fmt.Fprintf(logFile, "=== start %d of %s at %s\n", n.starts, n.name(), time.Now().UTC().Format(time.RFC3339Nano))
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		c.t.Errorf("start %s: %v", n.name(), err)
		return
	}
	inst := &instance{cmd: cmd, done: make(chan struct{})}
	n.inst = inst
	go func() {
		err := cmd.Wait()
		_ = logFile.Close()
		c.exited(n, inst, err)
		close(inst.done)
	}()
	if n.kind == kindBroker {
		go c.watchReady(n, inst)
	}
}

// exited records the end of inst. A process that ended on its own is a crash,
// except for a broker that failed before it was ready, which is started again
// after a pause, as an orchestrator would; a crash is started again too, so
// that the run goes on, but it is reported.
func (c *Cluster) exited(n *node, inst *instance, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.inst == inst {
		n.inst = nil
	}
	if inst.expected.Load() || c.closed {
		return
	}
	if n.kind != kindBroker || inst.ready.Load() {
		c.unexpected = append(c.unexpected, fmt.Sprintf("%s (start %d) exited by itself: %v", n.name(), n.starts, err))
	}
	if !c.healed {
		c.after(2*time.Second, func() { c.ensure(n) })
	}
}

// watchReady polls /readyz of a broker until it answers 200 or the process
// is gone.
func (c *Cluster) watchReady(n *node, inst *instance) {
	readyz := fmt.Sprintf("http://127.0.0.1:%d/readyz", n.httpPort)
	for {
		select {
		case <-inst.done:
			return
		case <-time.After(200 * time.Millisecond):
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, readyz, nil)
		if err != nil {
			return
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			inst.ready.Store(true)
			return
		}
	}
}

// waitBrokersReady waits until every broker is running and ready, starting
// those that are down, and reports whether that happened within timeout.
func (c *Cluster) waitBrokersReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		c.mu.Lock()
		for _, n := range c.brokers {
			if n.inst == nil {
				c.startLocked(n)
			}
			if n.inst == nil || !n.inst.ready.Load() {
				all = false
			}
		}
		c.mu.Unlock()
		if all {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// waitPostgres waits until the run's database answers a query.
func (c *Cluster) waitPostgres(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var last error
	for ctx.Err() == nil {
		var one int
		qctx, qcancel := context.WithTimeout(ctx, 3*time.Second)
		last = c.pool.QueryRow(qctx, "SELECT 1").Scan(&one)
		qcancel()
		if last == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.Join(ctx.Err(), last)
}

// command returns the executable and the arguments of the next instance of n.
func (c *Cluster) command(n *node) (string, []string) {
	if n.kind == kindBroker {
		return c.serverBin, []string{
			"--grpc-addr", "127.0.0.1:" + strconv.Itoa(n.grpcPort),
			"--http-addr", "127.0.0.1:" + strconv.Itoa(n.httpPort),
			"--database-url", c.brokerDSN,
			"--insecure",
			"--reap-interval", "1s",
			"--retention-done", "24h",
			"--max-db-conns", "12",
			"--log-level", "info",
		}
	}
	id := fmt.Sprintf("w%d-g%d", n.index, n.starts)
	return c.workerBin, []string{
		"--addr", strings.Join(c.addrs, ","),
		"--queue", queueName,
		"--id", id,
		"--concurrency", strconv.Itoa(c.cfg.Concurrency),
		"--ttl", c.cfg.TTL.String(),
		"--margin", c.cfg.Margin.String(),
		// One file for each start: a killed process can tear the last line of
		// its file, and a later start must not append to a torn line.
		"--history", filepath.Join(c.HistoryDir(), "worker-"+id+".jsonl"),
		"--sink-dsn", c.directDSN,
		"--seed", strconv.FormatUint(c.cfg.Seed*1000+uint64(n.index)*100+uint64(n.starts), 10),
	}
}

// build compiles the broker and the worker into the scratch directory.
func (c *Cluster) build() {
	c.t.Helper()
	root := moduleRoot(c.t)
	c.serverBin = filepath.Join(c.dir, "bin", "jobqueue-server")
	c.workerBin = filepath.Join(c.dir, "bin", "chaosworker")
	for _, b := range []struct{ out, pkg string }{
		{c.serverBin, "./cmd/jobqueue-server"},
		{c.workerBin, "./test/chaos/chaosworker"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), buildTimeout)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", b.out, b.pkg)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			c.t.Fatalf("go build %s: %v\n%s", b.pkg, err, out)
		}
	}
}

// connectToxiproxy checks that toxiproxy-server answers.
func (c *Cluster) connectToxiproxy() {
	c.t.Helper()
	c.tp = toxiproxy.NewClient(c.cfg.ToxiproxyAddr)
	version, err := c.tp.Version()
	if err != nil {
		c.t.Fatalf("toxiproxy-server is not reachable at %s: %v\n"+
			"start it with: toxiproxy-server -host 127.0.0.1 -port 8474", c.cfg.ToxiproxyAddr, err)
	}
	c.t.Logf("toxiproxy-server %s at %s", version, c.cfg.ToxiproxyAddr)
}

// createDatabase creates the run's database on the server, installs the
// queue's schema in it, creates the sink table, and opens the pool.
func (c *Cluster) createDatabase() {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, c.cfg.PGDSN)
	if err != nil {
		c.t.Fatalf("connect to PostgreSQL (JOBQUEUE_CHAOS_PG_DSN): %v", err)
	}
	defer func() { _ = admin.Close(context.WithoutCancel(ctx)) }()

	c.dbName = fmt.Sprintf("chaos_%d_%d", c.cfg.Seed, time.Now().Unix())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{c.dbName}.Sanitize()); err != nil {
		c.t.Fatalf("create database %s: %v", c.dbName, err)
	}
	c.directDSN = c.dsnFor(c.cfg.PGDSN, "")
	// The pool outlives this call, so it is not tied to ctx.
	pool, err := pgxpool.New(context.Background(), c.directDSN)
	if err != nil {
		c.t.Fatalf("open a pool on %s: %v", c.dbName, err)
	}
	c.pool = pool

	// jobqueuectl migrate does the same; the chaos test applies the
	// embedded migrations itself so that it needs no third binary.
	if _, err := migrate.Up(ctx, pool); err != nil {
		c.t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE chaos_sink (
		job_id bigint PRIMARY KEY, token bigint NOT NULL, worker text NOT NULL)`); err != nil {
		c.t.Fatalf("create chaos_sink: %v", err)
	}
}

// dsnFor returns the URL dsn with the run's database in place of its own and,
// when hostport is not empty, that host and port. It adds a connect timeout,
// so that a connection attempt into a partition ends.
func (c *Cluster) dsnFor(dsn, hostport string) string {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		c.t.Fatalf("JOBQUEUE_CHAOS_PG_DSN must be a postgres:// URL, got %q (%v)", dsn, err)
	}
	u.Path = "/" + c.dbName
	if hostport != "" {
		u.Host = hostport
	}
	q := u.Query()
	q.Set("connect_timeout", "5")
	u.RawQuery = q.Encode()
	return u.String()
}

// createProxies creates the pg proxy.
func (c *Cluster) createProxies() {
	c.t.Helper()
	upstream, err := url.Parse(c.cfg.PGDSN)
	if err != nil {
		c.t.Fatalf("parse JOBQUEUE_CHAOS_PG_DSN: %v", err)
	}
	c.createProxy(pgProxy, upstream.Host)
	c.brokerDSN = c.dsnFor(c.cfg.PGDSN, c.proxies[pgProxy].Listen)
}

// createBrokerProxies creates the proxy of each broker and records the
// addresses that clients use.
func (c *Cluster) createBrokerProxies() {
	c.t.Helper()
	for i, n := range c.brokers {
		c.createProxy(BrokerProxy(i), "127.0.0.1:"+strconv.Itoa(n.grpcPort))
		c.addrs = append(c.addrs, c.proxies[BrokerProxy(i)].Listen)
	}
}

// createProxy creates a proxy on a free port, replacing a proxy of the same
// name that an earlier run left in toxiproxy-server.
func (c *Cluster) createProxy(name, upstream string) {
	c.t.Helper()
	if old, err := c.tp.Proxy(name); err == nil {
		if err := old.Delete(); err != nil {
			c.t.Fatalf("delete the stale proxy %s: %v", name, err)
		}
	}
	p, err := c.tp.CreateProxy(name, "127.0.0.1:0", upstream)
	if err != nil {
		c.t.Fatalf("create proxy %s: %v", name, err)
	}
	c.proxies[name] = p
}

// cleanup takes the run down at the end of the test, and keeps the evidence
// of a failed one.
func (c *Cluster) cleanup() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.bg.Wait()
	c.StopProcesses(0)
	if c.t.Failed() {
		c.saveArtifacts()
	}
	if c.pool != nil {
		c.pool.Close()
	}
	for _, p := range c.proxies {
		if err := p.Delete(); err != nil {
			c.t.Logf("delete proxy %s: %v", p.Name, err)
		}
	}
	c.dropDatabase()
}

// dropDatabase drops the run's database, trying for a while because
// PostgreSQL may still be restarting.
func (c *Cluster) dropDatabase() {
	if c.dbName == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var last error
	for ctx.Err() == nil {
		var conn *pgx.Conn
		conn, last = pgx.Connect(ctx, c.cfg.PGDSN)
		if last == nil {
			_, last = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{c.dbName}.Sanitize()+" WITH (FORCE)")
			_ = conn.Close(context.WithoutCancel(ctx))
			if last == nil {
				return
			}
		}
		time.Sleep(time.Second)
	}
	c.t.Logf("drop database %s: %v", c.dbName, last)
}

// saveArtifacts copies the logs, the history files and the extra files of the
// run to artifacts/ next to the test, where CI picks them up.
func (c *Cluster) saveArtifacts() {
	dst := filepath.Join(c.pkgDir, "artifacts")
	if err := os.RemoveAll(dst); err != nil {
		c.t.Logf("clear %s: %v", dst, err)
	}
	for _, sub := range []string{"logs", "history", "extra"} {
		if err := copyTree(filepath.Join(c.dir, sub), filepath.Join(dst, sub)); err != nil {
			c.t.Logf("save the %s artifacts: %v", sub, err)
		}
	}
	summary := fmt.Sprintf("seed: %d\nfaults: %v\nworker kills: %d\nunexpected exits: %q\n",
		c.cfg.Seed, c.faults, c.sigkills, c.unexpected)
	if err := os.WriteFile(filepath.Join(dst, "summary.txt"), []byte(summary), 0o644); err != nil {
		c.t.Logf("save the summary: %v", err)
	}
	c.t.Logf("the logs and the history of this run are in %s", dst)
}

// copyTree copies the files below src to dst.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

// freePorts returns n distinct ports that were free a moment ago.
func freePorts(t testing.TB, n int) []int {
	t.Helper()
	var listeners []net.Listener
	var ports []int
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("find a free port: %v", err)
		}
		listeners = append(listeners, l)
		addr, ok := l.Addr().(*net.TCPAddr)
		if !ok {
			t.Fatalf("listener address %v is not TCP", l.Addr())
		}
		ports = append(ports, addr.Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports
}

// moduleRoot returns the directory of go.mod above the test's directory.
func moduleRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("look for go.mod: %v", err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's directory")
		}
		dir = parent
	}
}
