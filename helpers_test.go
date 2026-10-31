package jobqueue_test

import (
	"context"
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

// testBroker is one broker of an in-process cluster: the Server behind a gRPC
// server on a loopback port, with a hook that sees every call first.
type testBroker struct {
	addr string
	srv  *broker.Server
	gs   *grpc.Server

	mu    sync.Mutex
	calls map[string]int
	fault func(ctx context.Context, method string) error
}

func (b *testBroker) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	method := path.Base(info.FullMethod)
	b.mu.Lock()
	b.calls[method]++
	fault := b.fault
	b.mu.Unlock()
	if fault != nil {
		if err := fault(ctx, method); err != nil {
			return nil, err
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
	c := &cluster{t: t, reg: prometheus.NewRegistry()}
	c.mem = memstore.New(memstore.Options{Clock: time.Now})
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
