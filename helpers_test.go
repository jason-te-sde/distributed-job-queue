package jobqueue_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// quiet is a logger that drops everything, for the brokers of the tests.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// signalWriter is a log sink that closes a channel the first time a line
// holds the text, so that a test can wait for a log line instead of a clock.
type signalWriter struct {
	text string
	once sync.Once
	seen chan struct{}
}

func newSignalWriter(text string) *signalWriter {
	return &signalWriter{text: text, seen: make(chan struct{})}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(w.text)) {
		w.once.Do(func() { close(w.seen) })
	}
	return len(p), nil
}

// feed wakes whoever waits on the store: it is told about every row change
// and replaces its channel each time, so a waiter that took the channel
// before looking at the store cannot miss a change. It also keeps the changes,
// for tests that need to know what a call did, not only what the row is now.
type feed struct {
	mu  sync.Mutex
	ch  chan struct{}
	log []memstore.Change
}

func newFeed() *feed { return &feed{ch: make(chan struct{})} }

func (f *feed) changed(c memstore.Change) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, c)
	close(f.ch)
	f.ch = make(chan struct{})
}

func (f *feed) next() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ch
}

// testBroker is one broker of an in-process cluster: the Server behind a gRPC
// server on a loopback port, with a hook that sees every call first.
type testBroker struct {
	addr string
	srv  *broker.Server
	gs   *grpc.Server

	mu       sync.Mutex
	calls    map[string]int
	fault    func(ctx context.Context, method string) error
	override func(method string, req any) (resp any, ok bool)
}

func (b *testBroker) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	method := path.Base(info.FullMethod)
	b.mu.Lock()
	b.calls[method]++
	fault, override := b.fault, b.override
	b.mu.Unlock()
	if fault != nil {
		if err := fault(ctx, method); err != nil {
			return nil, err
		}
	}
	if override != nil {
		if resp, ok := override(method, req); ok {
			return resp, nil
		}
	}
	return h(ctx, req)
}

// setFault installs a hook that runs before every handler of the broker. A
// non-nil error is the call's answer.
func (b *testBroker) setFault(f func(ctx context.Context, method string) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fault = f
}

// setOverride installs a hook that may answer a call itself, instead of the
// handler, with a response of its own making.
func (b *testBroker) setOverride(f func(method string, req any) (resp any, ok bool)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.override = f
}

// callCount is the number of calls of the method that reached the broker.
func (b *testBroker) callCount(method string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[method]
}

// cluster is an in-process broker over a memstore with the real clock: brokers
// on loopback ports that share one dispatcher and one reaper, as brokers of a
// deployment share a database.
type cluster struct {
	t       *testing.T
	mem     *memstore.Store
	reg     *prometheus.Registry
	brokers []*testBroker
	feed    *feed
}

type clusterOpts struct {
	// brokers is the number of brokers; 0 means 1.
	brokers int
	// auth, if set, is the tokens the brokers demand; nil makes every caller
	// an admin.
	auth *broker.Authenticator
	// creds, if set, makes the brokers serve TLS.
	creds grpc.ServerOption
}

func newCluster(t *testing.T, o clusterOpts) *cluster {
	t.Helper()
	c := &cluster{t: t, reg: prometheus.NewRegistry(), feed: newFeed()}
	c.mem = memstore.New(memstore.Options{Clock: time.Now, Observer: c.feed.changed})
	metrics := broker.NewMetrics(c.reg)
	disp := broker.NewDispatcher(c.mem, broker.DispatcherConfig{
		Scheduler: "drr", PollInterval: 10 * time.Millisecond, ActivityInterval: 10 * time.Millisecond,
	}, metrics, quiet)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = disp.Run(ctx) }()
	go func() { defer wg.Done(); broker.RunReaper(ctx, c.mem, 20*time.Millisecond, 1000, 1, metrics, quiet) }()

	n := max(o.brokers, 1)
	for i := range n {
		b := &testBroker{calls: map[string]int{}}
		b.srv = broker.NewServer(c.mem, disp, metrics, quiet, uint64(i+1))
		opts := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(b.intercept, broker.UnaryAuth(o.auth)),
			grpc.MaxRecvMsgSize(20 << 20), grpc.MaxSendMsgSize(20 << 20),
		}
		if o.creds != nil {
			opts = append(opts, o.creds)
		}
		b.gs = grpc.NewServer(opts...)
		jobqueuev1.RegisterJobQueueServiceServer(b.gs, b.srv)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		b.addr = lis.Addr().String()
		wg.Add(1)
		go func() { defer wg.Done(); _ = b.gs.Serve(lis) }()
		c.brokers = append(c.brokers, b)
	}
	t.Cleanup(func() {
		for _, b := range c.brokers {
			b.gs.Stop()
		}
		cancel()
		wg.Wait()
	})
	return c
}

// target returns a target that names every broker, and the option that makes
// the connection resolve it.
func (c *cluster) target() (string, grpc.DialOption) {
	r := manual.NewBuilderWithScheme("cluster")
	var addrs []resolver.Address
	for _, b := range c.brokers {
		addrs = append(addrs, resolver.Address{Addr: b.addr})
	}
	r.InitialState(resolver.State{Addresses: addrs})
	return "cluster:///brokers", grpc.WithResolvers(r)
}

// dial opens a plaintext connection to every broker of the cluster through
// the SDK's Dial, and closes it when the test ends. Extra options come after
// WithInsecure.
func (c *cluster) dial(opts ...jobqueue.DialOption) *grpc.ClientConn {
	c.t.Helper()
	target, resolve := c.target()
	all := append([]jobqueue.DialOption{jobqueue.WithInsecure(), jobqueue.WithGRPCOptions(resolve)}, opts...)
	conn, err := jobqueue.Dial(target, all...)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// producer returns a producer with retries that are quick enough for a test.
func (c *cluster) producer(opts ...jobqueue.DialOption) *jobqueue.Producer {
	c.t.Helper()
	return jobqueue.NewProducer(c.dial(opts...), jobqueue.ProducerOptions{
		Retry: jobqueue.RetryPolicy{Attempts: 5, Base: time.Millisecond, Cap: 5 * time.Millisecond},
	})
}

// add stores n ready jobs of the queue directly and returns their ids.
func (c *cluster) add(queue string, n int) []int64 {
	c.t.Helper()
	specs := make([]job.Spec, n)
	for i := range specs {
		specs[i] = job.Spec{Queue: queue, Tenant: "default", Payload: []byte("payload")}
	}
	var ids []int64
	for len(specs) > 0 {
		batch := specs[:min(len(specs), job.MaxBatch)]
		specs = specs[len(batch):]
		res, err := c.mem.Enqueue(c.t.Context(), batch)
		if err != nil {
			c.t.Fatal(err)
		}
		for _, r := range res {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

// get reads a job from the store, whatever its tenant.
func (c *cluster) get(id int64) job.Job {
	c.t.Helper()
	j, err := c.mem.Get(c.t.Context(), id, "")
	if err != nil {
		c.t.Fatal(err)
	}
	return j
}

// count is how many jobs of the queue are in the state.
func (c *cluster) count(queue string, state job.State) int {
	c.t.Helper()
	snap, err := c.mem.Snapshot(c.t.Context())
	if err != nil {
		c.t.Fatal(err)
	}
	n := 0
	for _, j := range snap {
		if j.Queue == queue && j.State == state {
			n++
		}
	}
	return n
}

// changesOf returns every change the store made to the job so far, oldest
// first.
func (c *cluster) changesOf(id int64) []memstore.Change {
	c.feed.mu.Lock()
	defer c.feed.mu.Unlock()
	var out []memstore.Change
	for _, ch := range c.feed.log {
		if ch.After.ID == id || ch.Before.ID == id {
			out = append(out, ch)
		}
	}
	return out
}

// await blocks until cond holds, looking again at every change of the store,
// and fails the test if that takes longer than within.
func (c *cluster) await(what string, within time.Duration, cond func() bool) {
	c.t.Helper()
	timeout := time.NewTimer(within)
	defer timeout.Stop()
	for {
		changed := c.feed.next()
		if cond() {
			return
		}
		select {
		case <-changed:
		case <-timeout.C:
			c.t.Fatalf("timed out after %v waiting for %s", within, what)
		}
	}
}

// ackCount reads jobqueue_acks_total{op, result} from the brokers' registry.
func (c *cluster) ackCount(op, result string) float64 {
	c.t.Helper()
	families, err := c.reg.Gather()
	if err != nil {
		c.t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "jobqueue_acks_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["op"] == op && labels["result"] == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// fast fills the options of a test worker. A lease of 1s with a margin of
// 200ms satisfies the rule that the TTL exceeds four margins, and keeps the
// heartbeat period, a quarter of the TTL, at 250ms.
func fast(o jobqueue.WorkerOptions) jobqueue.WorkerOptions {
	if o.Queue == "" {
		o.Queue = "q"
	}
	if o.LeaseTTL == 0 {
		o.LeaseTTL = time.Second
	}
	if o.Margin == 0 {
		o.Margin = 200 * time.Millisecond
	}
	if o.LongPoll == 0 {
		o.LongPoll = time.Second
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 2 * time.Second
	}
	if o.Logger == nil {
		o.Logger = quiet
	}
	return o
}

// runningWorker is a worker whose Run is going in the background.
type runningWorker struct {
	t      *testing.T
	cancel context.CancelFunc
	// done is closed when Run has returned, and err is what it returned.
	done chan struct{}
	err  error

	once sync.Once
}

// startWorker runs a worker with the handler in the background, over its own
// connection. The test's end stops it if the test has not.
func (c *cluster) startWorker(opts jobqueue.WorkerOptions, h jobqueue.Handler, dialOpts ...jobqueue.DialOption) *runningWorker {
	c.t.Helper()
	w := jobqueue.NewWorker(c.dial(dialOpts...), fast(opts))
	if err := w.Handle(h); err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(c.t.Context())
	rw := &runningWorker{t: c.t, cancel: cancel, done: make(chan struct{})}
	go func() {
		rw.err = w.Run(ctx)
		close(rw.done)
	}()
	c.t.Cleanup(func() { _ = rw.stop() })
	return rw
}

// stop cancels the worker's context and returns what Run returned once it
// has. Run must return within 10s, however long it was told to drain.
func (r *runningWorker) stop() error {
	r.once.Do(func() {
		r.cancel()
		select {
		case <-r.done:
		case <-time.After(10 * time.Second):
			r.t.Error("Run did not return within 10s of its context ending")
		}
	})
	select {
	case <-r.done:
		return r.err
	default:
		return errors.New("Run has not returned")
	}
}
