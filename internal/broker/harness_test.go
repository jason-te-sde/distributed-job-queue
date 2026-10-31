package broker

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// The tokens of the test cluster.
const (
	tokProducerA = "tok-producer-a"
	tokProducerB = "tok-producer-b"
	tokWorkerQ1  = "tok-worker-q1"
	tokWorkerQ12 = "tok-worker-q1-q2"
	tokAdmin     = "tok-admin"
)

var testTokens = strings.Join([]string{
	"producer tenant=a " + hashOf(tokProducerA),
	"producer tenant=b " + hashOf(tokProducerB),
	"worker queues=q1 " + hashOf(tokWorkerQ1),
	"worker queues=q1,q2 " + hashOf(tokWorkerQ12),
	"admin - " + hashOf(tokAdmin),
}, "\n")

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fakeClock is the store's clock, which a test moves by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeDispatcher stands in for the dispatcher of Task 25. It leases from the
// store at once, in FIFO order across tenants, and never waits.
type fakeDispatcher struct {
	st store.Store

	mu     sync.Mutex
	calls  []LeaseCall
	err    error         // when set, returned instead of leasing
	waited time.Duration // what the answer reports
}

func (f *fakeDispatcher) Lease(ctx context.Context, c LeaseCall) (LeaseAnswer, error) {
	f.mu.Lock()
	f.calls = append(f.calls, c)
	err, waited := f.err, f.waited
	f.mu.Unlock()
	if err != nil {
		return LeaseAnswer{}, err
	}
	grants, err := f.st.Lease(ctx, store.LeaseRequest{
		Queue: c.Queue, Takes: []store.TenantTake{{Limit: c.Max}}, TTL: c.TTL, Worker: c.Worker,
	})
	if err != nil {
		return LeaseAnswer{}, toStatus(err)
	}
	return LeaseAnswer{Grants: grants, Waited: waited}, nil
}

func (f *fakeDispatcher) seen() []LeaseCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// syncBuffer is a log sink that several goroutines may write.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type testOpts struct {
	// noAuth runs the server without a tokens file: every caller is an admin.
	noAuth bool
	// seed is the jitter seed; 0 means 1.
	seed uint64
	// wrap, if set, wraps the store the Server uses; the test's own handle
	// stays the bare memstore.
	wrap func(store.Store) store.Store
}

// spyStore counts the calls that reach the store through the Server, so that
// a test can show that a rejected request never got there. The test's own
// calls through ts.mem, and the fake dispatcher's, are not counted.
type spyStore struct {
	store.Store
	n *atomic.Int64
}

func (s spyStore) Enqueue(ctx context.Context, specs []job.Spec) ([]store.EnqueueResult, error) {
	s.n.Add(1)
	return s.Store.Enqueue(ctx, specs)
}

func (s spyStore) Heartbeat(ctx context.Context, refs []store.Ref, ttl time.Duration) ([]store.HeartbeatResult, error) {
	s.n.Add(1)
	return s.Store.Heartbeat(ctx, refs, ttl)
}

func (s spyStore) Complete(ctx context.Context, reqs []store.CompleteRequest) ([]job.Outcome, error) {
	s.n.Add(1)
	return s.Store.Complete(ctx, reqs)
}

func (s spyStore) Fail(ctx context.Context, reqs []store.FailRequest) ([]job.Outcome, error) {
	s.n.Add(1)
	return s.Store.Fail(ctx, reqs)
}

func (s spyStore) Release(ctx context.Context, refs []store.Ref) ([]job.Outcome, error) {
	s.n.Add(1)
	return s.Store.Release(ctx, refs)
}

func (s spyStore) Cancel(ctx context.Context, id int64, tenant string) (job.Outcome, error) {
	s.n.Add(1)
	return s.Store.Cancel(ctx, id, tenant)
}

func (s spyStore) Requeue(ctx context.Context, id int64) (job.Outcome, error) {
	s.n.Add(1)
	return s.Store.Requeue(ctx, id)
}

func (s spyStore) Purge(ctx context.Context, req store.PurgeRequest) (int, error) {
	s.n.Add(1)
	return s.Store.Purge(ctx, req)
}

func (s spyStore) Get(ctx context.Context, id int64, tenant string) (job.Job, error) {
	s.n.Add(1)
	return s.Store.Get(ctx, id, tenant)
}

func (s spyStore) ListDead(ctx context.Context, queue string, afterID int64, limit int) ([]job.Job, error) {
	s.n.Add(1)
	return s.Store.ListDead(ctx, queue, afterID, limit)
}

func (s spyStore) Stats(ctx context.Context, queue string) ([]store.TenantStats, error) {
	s.n.Add(1)
	return s.Store.Stats(ctx, queue)
}

type testServer struct {
	t      *testing.T
	calls  atomic.Int64 // calls that reached the store through the Server
	clock  *fakeClock
	mem    *memstore.Store
	srv    *Server
	disp   *fakeDispatcher
	reg    *prometheus.Registry
	logs   *syncBuffer
	client jobqueuev1.JobQueueServiceClient
}

// newTestServer starts a gRPC server over bufconn with UnaryAuth, a Server on
// a memstore whose clock the test controls, and a fake dispatcher.
func newTestServer(t *testing.T, opts testOpts) *testServer {
	t.Helper()
	ts := &testServer{t: t, clock: &fakeClock{now: t0}, reg: prometheus.NewRegistry(), logs: &syncBuffer{}}
	ts.mem = memstore.New(memstore.Options{Clock: ts.clock.Now})
	ts.disp = &fakeDispatcher{st: ts.mem}
	var inner store.Store = ts.mem
	if opts.wrap != nil {
		inner = opts.wrap(inner)
	}
	st := spyStore{Store: inner, n: &ts.calls}
	seed := opts.seed
	if seed == 0 {
		seed = 1
	}
	log := slog.New(slog.NewTextHandler(ts.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ts.srv = NewServer(st, ts.disp, NewMetrics(ts.reg), log, seed)

	var auth *Authenticator
	if !opts.noAuth {
		var err error
		if auth, err = LoadTokens(strings.NewReader(testTokens)); err != nil {
			t.Fatal(err)
		}
	}
	const maxMsg = 20 << 20
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(UnaryAuth(auth)), grpc.MaxRecvMsgSize(maxMsg), grpc.MaxSendMsgSize(maxMsg))
	jobqueuev1.RegisterJobQueueServiceServer(gs, ts.srv)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsg), grpc.MaxCallRecvMsgSize(maxMsg)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})
	ts.client = jobqueuev1.NewJobQueueServiceClient(conn)
	return ts
}

// as returns a context that carries the token of the named caller.
func (ts *testServer) as(token string) context.Context {
	return metadata.AppendToOutgoingContext(ts.t.Context(), "authorization", "Bearer "+token)
}

// anon returns a context without credentials. Against a server without a
// tokens file it is an admin; against one with a tokens file it is nobody.
func (ts *testServer) anon() context.Context { return ts.t.Context() }

// add stores a ready job directly, bypassing the handlers.
func (ts *testServer) add(queue, tenant string) int64 {
	ts.t.Helper()
	res, err := ts.mem.Enqueue(ts.t.Context(), []job.Spec{{Queue: queue, Tenant: tenant, Payload: []byte("payload")}})
	if err != nil {
		ts.t.Fatal(err)
	}
	return res[0].ID
}

// leaseDirect leases up to n jobs of a queue straight from the store, for a
// worker named "w", with the given ttl.
func (ts *testServer) leaseDirect(queue string, n int, ttl time.Duration) []store.Grant {
	ts.t.Helper()
	grants, err := ts.mem.Lease(ts.t.Context(), store.LeaseRequest{
		Queue: queue, Takes: []store.TenantTake{{Limit: n}}, TTL: ttl, Worker: "w",
	})
	if err != nil {
		ts.t.Fatal(err)
	}
	if len(grants) != n {
		ts.t.Fatalf("leased %d jobs, want %d", len(grants), n)
	}
	return grants
}

// leased adds a job and leases the oldest ready job of that tenant in the
// queue, which is the new one unless the tenant had others waiting. It returns
// the job as leased.
func (ts *testServer) leased(queue, tenant string) job.Job {
	ts.t.Helper()
	ts.add(queue, tenant)
	grants, err := ts.mem.Lease(ts.t.Context(), store.LeaseRequest{
		Queue: queue, Takes: []store.TenantTake{{Tenant: tenant, Limit: 1}}, TTL: 30 * time.Second, Worker: "w",
	})
	if err != nil || len(grants) != 1 {
		ts.t.Fatalf("Lease = %v, %v", grants, err)
	}
	return grants[0].Job
}

func (ts *testServer) ref(j job.Job) *jobqueuev1.LeaseRef {
	return &jobqueuev1.LeaseRef{Id: j.ID, Queue: j.Queue, Token: j.Epoch}
}

func (ts *testServer) succeeded(queue, tenant string) int64 {
	ts.t.Helper()
	j := ts.leased(queue, tenant)
	out, err := ts.mem.Complete(ts.t.Context(), []store.CompleteRequest{{Ref: store.Ref{ID: j.ID, Queue: j.Queue, Token: j.Epoch}}})
	if err != nil || out[0] != job.Applied {
		ts.t.Fatalf("Complete = %v, %v", out, err)
	}
	return j.ID
}

func (ts *testServer) dead(queue, tenant string) int64 {
	ts.t.Helper()
	j := ts.leased(queue, tenant)
	out, err := ts.mem.Fail(ts.t.Context(), []store.FailRequest{{Ref: store.Ref{ID: j.ID, Queue: j.Queue, Token: j.Epoch}, Permanent: true, Error: "gave up"}})
	if err != nil || out[0] != job.Applied {
		ts.t.Fatalf("Fail = %v, %v", out, err)
	}
	return j.ID
}

func (ts *testServer) job(id int64) job.Job {
	ts.t.Helper()
	j, err := ts.mem.Get(ts.t.Context(), id, "")
	if err != nil {
		ts.t.Fatalf("Get(%d): %v", id, err)
	}
	return j
}

// storeCalls returns how many calls the Server has made to the store.
func (ts *testServer) storeCalls() int64 { return ts.calls.Load() }

// noStoreCalls fails the test if the Server has called the store at all.
func (ts *testServer) noStoreCalls(t *testing.T) {
	t.Helper()
	if n := ts.storeCalls(); n != 0 {
		t.Fatalf("the Server made %d calls to the store, want none", n)
	}
}

func (ts *testServer) stored() int {
	ts.t.Helper()
	rows, err := ts.mem.Snapshot(ts.t.Context())
	if err != nil {
		ts.t.Fatal(err)
	}
	return len(rows)
}

// counter reads one series of a counter from the test registry; a series
// that does not exist reads as 0.
func (ts *testServer) counter(name string, labels map[string]string) float64 {
	ts.t.Helper()
	families, err := ts.reg.Gather()
	if err != nil {
		ts.t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			for k, v := range labels {
				if got[k] != v {
					continue metric
				}
			}
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v, want %v (error: %v)", got, want, err)
	}
}

func item(queue, tenant string) *jobqueuev1.EnqueueItem {
	return &jobqueuev1.EnqueueItem{Queue: queue, Tenant: tenant, Payload: []byte("payload")}
}

func enqueueReq(items ...*jobqueuev1.EnqueueItem) *jobqueuev1.EnqueueRequest {
	return &jobqueuev1.EnqueueRequest{Items: items}
}

func leaseReq(queue string, n int32, ttl time.Duration) *jobqueuev1.LeaseRequest {
	return &jobqueuev1.LeaseRequest{Queue: queue, MaxJobs: n, Ttl: durationpb.New(ttl), WorkerId: "w1"}
}

func successItem(ref *jobqueuev1.LeaseRef, result string) *jobqueuev1.AckItem {
	return &jobqueuev1.AckItem{Lease: ref, Outcome: &jobqueuev1.AckItem_Success{Success: &jobqueuev1.Success{Result: []byte(result)}}}
}

func failureItem(ref *jobqueuev1.LeaseRef, f *jobqueuev1.Failure) *jobqueuev1.AckItem {
	return &jobqueuev1.AckItem{Lease: ref, Outcome: &jobqueuev1.AckItem_Failure{Failure: f}}
}

// failingStore makes the store fail the calls that TestServerMapsStoreErrors
// makes.
type failingStore struct {
	store.Store
	err error
}

func (f failingStore) Enqueue(context.Context, []job.Spec) ([]store.EnqueueResult, error) {
	return nil, f.err
}

func (f failingStore) Complete(context.Context, []store.CompleteRequest) ([]job.Outcome, error) {
	return nil, f.err
}

func (f failingStore) Get(context.Context, int64, string) (job.Job, error) { return job.Job{}, f.err }

// scriptedStore answers Enqueue, Heartbeat, Complete, Fail, Release, Cancel and
// Requeue with what a test puts in it, however many items were asked about.
type scriptedStore struct {
	store.Store
	outcomes  []job.Outcome
	single    job.Outcome // the answer of Cancel and Requeue
	enqueued  []store.EnqueueResult
	heartbeat []store.HeartbeatResult
}

func (f scriptedStore) Enqueue(context.Context, []job.Spec) ([]store.EnqueueResult, error) {
	return f.enqueued, nil
}

func (f scriptedStore) Heartbeat(context.Context, []store.Ref, time.Duration) ([]store.HeartbeatResult, error) {
	return f.heartbeat, nil
}

func (f scriptedStore) Complete(context.Context, []store.CompleteRequest) ([]job.Outcome, error) {
	return f.outcomes, nil
}

func (f scriptedStore) Fail(context.Context, []store.FailRequest) ([]job.Outcome, error) {
	return f.outcomes, nil
}

func (f scriptedStore) Release(context.Context, []store.Ref) ([]job.Outcome, error) {
	return f.outcomes, nil
}

func (f scriptedStore) Cancel(context.Context, int64, string) (job.Outcome, error) {
	return f.single, nil
}

func (f scriptedStore) Requeue(context.Context, int64) (job.Outcome, error) {
	return f.single, nil
}
