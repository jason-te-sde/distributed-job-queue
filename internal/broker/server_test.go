package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/protobuf/types/known/timestamppb"

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

// hostileNames are the queue and tenant names of Review Focus 5.
var hostileNames = []string{"", "a b", "q\t1", "-lead", "x\x00", strings.Repeat("n", 129), "ünï"}

func TestServerEnqueueSizeLimits(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokAdmin)
	payload := func(n int) []byte { return bytes.Repeat([]byte("x"), n) }
	withPayload := func(n int) *jobqueuev1.EnqueueItem {
		it := item("q1", "")
		it.Payload = payload(n)
		return it
	}
	many := func(n int) []*jobqueuev1.EnqueueItem {
		items := make([]*jobqueuev1.EnqueueItem, n)
		for i := range items {
			items[i] = item("q1", "")
		}
		return items
	}
	badKey := item("q1", "")
	badKey.IdempotencyKey = strings.Repeat("k", 256)
	nulKey := item("q1", "")
	nulKey.IdempotencyKey = "a\x00b"

	rejected := []struct {
		name  string
		items []*jobqueuev1.EnqueueItem
		edit  func(*jobqueuev1.EnqueueItem)
	}{
		{name: "payload one byte over", items: []*jobqueuev1.EnqueueItem{withPayload(job.MaxPayloadBytes + 1)}},
		{name: "no items"},
		{name: "one item too many", items: many(job.MaxBatch + 1)},
		{name: "a bad item in the middle fails the batch", items: []*jobqueuev1.EnqueueItem{item("q1", ""), withPayload(job.MaxPayloadBytes + 1), item("q1", "")}},
		{name: "a bad last item fails the batch", items: []*jobqueuev1.EnqueueItem{item("q1", ""), item("q1", ""), item("bad name", "")}},
		{name: "key one byte over", items: []*jobqueuev1.EnqueueItem{badKey}},
		{name: "key with NUL", items: []*jobqueuev1.EnqueueItem{nulKey}},
		{name: "max_attempts above the limit", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.MaxAttempts = job.MaxMaxAttempts + 1 }},
		{name: "negative max_attempts", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.MaxAttempts = -1 }},
		{name: "delay above a year", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.Delay = durationpb.New(job.MaxDelay + time.Second) }},
		{name: "negative delay", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.Delay = durationpb.New(-time.Second) }},
		{name: "delay not in whole microseconds", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.Delay = durationpb.New(1500 * time.Nanosecond) }},
		{name: "malformed delay", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.Delay = &durationpb.Duration{Seconds: 1, Nanos: -1} }},
		{name: "delay with a billion nanoseconds", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.Delay = &durationpb.Duration{Seconds: 1, Nanos: 1_000_000_000} }},
		{name: "run_at before the year 2000", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) {
			it.RunAt = timestamppb.New(time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC))
		}},
		{name: "run_at in the year 3000", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) {
			it.RunAt = timestamppb.New(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
		{name: "run_at at the epoch", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.RunAt = &timestamppb.Timestamp{} }},
		{name: "malformed run_at", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) { it.RunAt = &timestamppb.Timestamp{Seconds: 1 << 40, Nanos: 5} }},
		{name: "delay together with run_at", items: []*jobqueuev1.EnqueueItem{item("q1", "")}, edit: func(it *jobqueuev1.EnqueueItem) {
			it.Delay = durationpb.New(time.Second)
			it.RunAt = timestamppb.New(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
	}
	for _, tc := range rejected {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			if tc.edit != nil {
				tc.edit(tc.items[0])
			}
			_, err := ts.client.Enqueue(ctx, enqueueReq(tc.items...))
			wantCode(t, err, codes.InvalidArgument)
			if n := ts.stored(); n != 0 {
				t.Fatalf("%d jobs were stored by a rejected request", n)
			}
			ts.noStoreCalls(t)
		})
	}

	accepted := []struct {
		name  string
		items []*jobqueuev1.EnqueueItem
	}{
		{"an empty payload", []*jobqueuev1.EnqueueItem{withPayload(0)}},
		{"a payload at the limit", []*jobqueuev1.EnqueueItem{withPayload(job.MaxPayloadBytes)}},
		{"a key at the limit", func() []*jobqueuev1.EnqueueItem {
			it := item("q1", "")
			it.IdempotencyKey = strings.Repeat("k", 255)
			return []*jobqueuev1.EnqueueItem{it}
		}()},
		{"a full batch of full payloads", func() []*jobqueuev1.EnqueueItem {
			items := make([]*jobqueuev1.EnqueueItem, job.MaxBatch)
			for i := range items {
				items[i] = withPayload(job.MaxPayloadBytes)
			}
			return items
		}()},
	}
	stored := 0
	for _, tc := range accepted {
		t.Run("accepts "+tc.name, func(t *testing.T) {
			resp, err := ts.client.Enqueue(ctx, enqueueReq(tc.items...))
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.GetResults()) != len(tc.items) {
				t.Fatalf("%d results for %d items", len(resp.GetResults()), len(tc.items))
			}
			stored += len(tc.items)
			if n := ts.stored(); n != stored {
				t.Fatalf("%d jobs stored, want %d", n, stored)
			}
		})
	}
}

func TestServerRejectsBadNames(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokAdmin)
	for _, name := range hostileNames {
		label := fmt.Sprintf("%q", name)
		if len(name) > 40 {
			label = fmt.Sprintf("%d bytes", len(name))
		}
		t.Run(label, func(t *testing.T) {
			callsBefore := ts.storeCalls()
			_, err := ts.client.Enqueue(ctx, enqueueReq(item(name, "t")))
			wantCode(t, err, codes.InvalidArgument)

			// An empty tenant is not a bad name: it means the caller's own.
			_, err = ts.client.Enqueue(ctx, enqueueReq(item("q1", name)))
			if name == "" {
				if err != nil {
					t.Fatalf("an empty tenant must default, got %v", err)
				}
			} else {
				wantCode(t, err, codes.InvalidArgument)
			}

			ref := &jobqueuev1.LeaseRef{Id: 1, Queue: name, Token: 1}
			_, err = ts.client.Lease(ctx, leaseReq(name, 1, 30*time.Second))
			wantCode(t, err, codes.InvalidArgument)
			_, err = ts.client.Heartbeat(ctx, &jobqueuev1.HeartbeatRequest{Leases: []*jobqueuev1.LeaseRef{ref}, Ttl: durationpb.New(30 * time.Second)})
			wantCode(t, err, codes.InvalidArgument)
			_, err = ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(ref, "")}})
			wantCode(t, err, codes.InvalidArgument)
			_, err = ts.client.Release(ctx, &jobqueuev1.ReleaseRequest{Leases: []*jobqueuev1.LeaseRef{ref}})
			wantCode(t, err, codes.InvalidArgument)
			_, err = ts.client.GetStats(ctx, &jobqueuev1.GetStatsRequest{Queue: name})
			wantCode(t, err, codes.InvalidArgument)
			_, err = ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: name, Limit: 10})
			wantCode(t, err, codes.InvalidArgument)
			_, err = ts.client.PurgeJobs(ctx, &jobqueuev1.PurgeJobsRequest{Queue: name, State: jobqueuev1.JobState_JOB_STATE_SUCCEEDED, FinishedBefore: timestamppb.New(t0)})
			wantCode(t, err, codes.InvalidArgument)

			// Of all these calls only the Enqueue with an empty tenant is
			// valid, and it is the only one that may reach the store.
			wantCalls := int64(0)
			if name == "" {
				wantCalls = 1
			}
			if got := ts.storeCalls() - callsBefore; got != wantCalls {
				t.Errorf("%d calls reached the store, want %d", got, wantCalls)
			}
		})
	}
	// Only the empty tenant of the loop above may have been stored, once per
	// loop pass.
	rows, err := ts.mem.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range rows {
		if j.Queue != "q1" || j.Tenant != "default" {
			t.Errorf("a hostile name was stored: %+v", j)
		}
	}
	if calls := ts.disp.seen(); len(calls) != 0 {
		t.Errorf("the dispatcher was called %d times for invalid leases", len(calls))
	}
}

func TestServerEnqueueDefaultsTenant(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts testOpts
		ctx  func(*testServer) context.Context
	}{
		{"admin", testOpts{}, func(ts *testServer) context.Context { return ts.as(tokAdmin) }},
		{"no tokens file", testOpts{noAuth: true}, func(ts *testServer) context.Context { return ts.anon() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, tc.opts)
			ctx := tc.ctx(ts)
			resp, err := ts.client.Enqueue(ctx, enqueueReq(item("q1", ""), item("q1", "acme")))
			if err != nil {
				t.Fatal(err)
			}
			if got := ts.job(resp.GetResults()[0].GetId()).Tenant; got != "default" {
				t.Errorf("an empty tenant became %q, want default", got)
			}
			if got := ts.job(resp.GetResults()[1].GetId()).Tenant; got != "acme" {
				t.Errorf("tenant acme became %q", got)
			}
		})
	}
}

func TestServerEnqueueResultsAndDeduplication(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokAdmin)
	keyed := func(key string) *jobqueuev1.EnqueueItem {
		it := item("q1", "")
		it.IdempotencyKey = key
		return it
	}
	first, err := ts.client.Enqueue(ctx, enqueueReq(keyed("k1"), keyed("k2"), item("q2", "")))
	if err != nil {
		t.Fatal(err)
	}
	second, err := ts.client.Enqueue(ctx, enqueueReq(keyed("k1"), item("q1", "")))
	if err != nil {
		t.Fatal(err)
	}
	f, s := first.GetResults(), second.GetResults()
	if len(f) != 3 || len(s) != 2 {
		t.Fatalf("results = %d and %d", len(f), len(s))
	}
	if f[0].GetDeduplicated() || f[1].GetDeduplicated() || f[2].GetDeduplicated() {
		t.Errorf("the first call deduplicated: %v", f)
	}
	if !s[0].GetDeduplicated() || s[0].GetId() != f[0].GetId() {
		t.Errorf("the repeated key = %v, want the first job %d deduplicated", s[0], f[0].GetId())
	}
	if s[1].GetDeduplicated() {
		t.Errorf("an unkeyed item was deduplicated")
	}
	if n := ts.stored(); n != 4 {
		t.Errorf("%d jobs stored, want 4", n)
	}
	// 4 created (q1 three times, q2 once) and 1 deduplicated.
	for queue, want := range map[string]float64{"q1": 3, "q2": 1} {
		if got := ts.counter("jobqueue_jobs_enqueued_total", map[string]string{"queue": queue}); got != want {
			t.Errorf("enqueued{%s} = %v, want %v", queue, got, want)
		}
	}
	if got := ts.counter("jobqueue_jobs_deduplicated_total", map[string]string{"queue": "q1"}); got != 1 {
		t.Errorf("deduplicated{q1} = %v, want 1", got)
	}
	if got := ts.counter("jobqueue_jobs_deduplicated_total", map[string]string{"queue": "q2"}); got != 0 {
		t.Errorf("deduplicated{q2} = %v, want 0", got)
	}
}

func TestServerProducerTenantForced(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokProducerA)

	_, err := ts.client.Enqueue(ctx, enqueueReq(item("q1", "b")))
	wantCode(t, err, codes.PermissionDenied)
	if !strings.Contains(status.Convert(err).Message(), "tenant") {
		t.Errorf("message %q does not name the tenant", status.Convert(err).Message())
	}
	_, err = ts.client.Enqueue(ctx, enqueueReq(item("q1", "a"), item("q1", "b")))
	wantCode(t, err, codes.PermissionDenied)
	if n := ts.stored(); n != 0 {
		t.Fatalf("%d jobs stored by a denied request", n)
	}
	ts.noStoreCalls(t)

	resp, err := ts.client.Enqueue(ctx, enqueueReq(item("q1", ""), item("q1", "a")))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp.GetResults() {
		if got := ts.job(r.GetId()).Tenant; got != "a" {
			t.Errorf("job %d has tenant %q, want a", r.GetId(), got)
		}
	}
}

func TestServerWorkerQueueScope(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	ts.add("q1", "t")
	ts.add("q2", "t")

	_, err := ts.client.Lease(ctx, leaseReq("q2", 1, 30*time.Second))
	wantCode(t, err, codes.PermissionDenied)
	if calls := ts.disp.seen(); len(calls) != 0 {
		t.Fatalf("the dispatcher served a denied lease: %v", calls)
	}
	if resp, err := ts.client.Lease(ctx, leaseReq("q1", 1, 30*time.Second)); err != nil || len(resp.GetJobs()) != 1 {
		t.Fatalf("Lease on its own queue = %v, %v", resp, err)
	}

	own := ts.leased("q1", "t")
	foreign := ts.leased("q2", "t")
	good, bad := ts.ref(own), ts.ref(foreign)
	hb := durationpb.New(30 * time.Second)

	_, err = ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(good, ""), successItem(bad, "")}})
	wantCode(t, err, codes.PermissionDenied)
	_, err = ts.client.Heartbeat(ctx, &jobqueuev1.HeartbeatRequest{Leases: []*jobqueuev1.LeaseRef{good, bad}, Ttl: hb})
	wantCode(t, err, codes.PermissionDenied)
	_, err = ts.client.Release(ctx, &jobqueuev1.ReleaseRequest{Leases: []*jobqueuev1.LeaseRef{good, bad}})
	wantCode(t, err, codes.PermissionDenied)
	// A denied batch applies nothing, not even the item that was allowed.
	ts.noStoreCalls(t)
	for _, j := range []job.Job{own, foreign} {
		if got := ts.job(j.ID); got.State != job.StateLeased {
			t.Errorf("job %d is %v after denied calls", j.ID, got.State)
		}
	}

	// The same refs pass for a worker that owns both queues, and for an admin.
	for _, token := range []string{tokWorkerQ12, tokAdmin} {
		resp, err := ts.client.Heartbeat(ts.as(token), &jobqueuev1.HeartbeatRequest{Leases: []*jobqueuev1.LeaseRef{good, bad}, Ttl: hb})
		if err != nil {
			t.Fatalf("Heartbeat as %s: %v", token, err)
		}
		for i, r := range resp.GetResults() {
			if r.GetStatus() != jobqueuev1.LeaseStatus_LEASE_STATUS_HELD {
				t.Errorf("as %s: result %d = %v", token, i, r.GetStatus())
			}
		}
	}
	if _, err := ts.client.Lease(ts.as(tokAdmin), leaseReq("q2", 1, 30*time.Second)); err != nil {
		t.Errorf("an admin may lease from any queue: %v", err)
	}
}

func TestServerValidatesBeforeAuthorizing(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	big := item("q1", "b") // another tenant's name, and too large
	big.Payload = bytes.Repeat([]byte("x"), job.MaxPayloadBytes+1)
	_, err := ts.client.Enqueue(ts.as(tokProducerA), enqueueReq(big))
	wantCode(t, err, codes.InvalidArgument)
	_, err = ts.client.Enqueue(ts.as(tokProducerA), enqueueReq(item("q1", "b")))
	wantCode(t, err, codes.PermissionDenied)

	_, err = ts.client.Lease(ts.as(tokWorkerQ1), leaseReq("-bad", 1, 30*time.Second))
	wantCode(t, err, codes.InvalidArgument)
	_, err = ts.client.Lease(ts.as(tokWorkerQ1), leaseReq("q2", 1, 30*time.Second))
	wantCode(t, err, codes.PermissionDenied)

	// An Ack from a producer is denied, or invalid, before the store is asked.
	_, err = ts.client.Ack(ts.as(tokProducerA), &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(&jobqueuev1.LeaseRef{Id: 1, Queue: "q1", Token: 1}, "")}})
	wantCode(t, err, codes.PermissionDenied)
	_, err = ts.client.Ack(ts.as(tokProducerA), &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{{}}})
	wantCode(t, err, codes.InvalidArgument)
	ts.noStoreCalls(t)
}

func TestServerUnauthenticated(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	_, err := ts.client.Enqueue(ts.anon(), enqueueReq(item("q1", "")))
	wantCode(t, err, codes.Unauthenticated)
	_, err = ts.client.Enqueue(ts.as("not-a-token"), enqueueReq(item("q1", "")))
	wantCode(t, err, codes.Unauthenticated)
	if n := ts.stored(); n != 0 {
		t.Fatalf("%d jobs stored without credentials", n)
	}
}

func TestServerAdminOnlyRPCs(t *testing.T) {
	calls := map[string]func(ts *testServer, id int64) func(ctx context.Context) error{
		"GetStats": func(ts *testServer, _ int64) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := ts.client.GetStats(ctx, &jobqueuev1.GetStatsRequest{Queue: "q1"})
				return err
			}
		},
		"ListDeadJobs": func(ts *testServer, _ int64) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", Limit: 10})
				return err
			}
		},
		"RequeueJob": func(ts *testServer, id int64) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := ts.client.RequeueJob(ctx, &jobqueuev1.RequeueJobRequest{Id: id})
				return err
			}
		},
		"PurgeJobs": func(ts *testServer, _ int64) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := ts.client.PurgeJobs(ctx, &jobqueuev1.PurgeJobsRequest{
					Queue: "q1", State: jobqueuev1.JobState_JOB_STATE_DEAD, FinishedBefore: timestamppb.New(t0.Add(time.Hour)),
				})
				return err
			}
		},
	}
	for name, build := range calls {
		t.Run(name, func(t *testing.T) {
			ts := newTestServer(t, testOpts{})
			id := ts.dead("q1", "a")
			call := build(ts, id)
			for _, token := range []string{tokProducerA, tokWorkerQ1, tokWorkerQ12} {
				wantCode(t, call(ts.as(token)), codes.PermissionDenied)
			}
			ts.noStoreCalls(t)
			if got := ts.job(id).State; got != job.StateDead {
				t.Fatalf("a denied call changed the job to %v", got)
			}
			if err := call(ts.as(tokAdmin)); err != nil {
				t.Fatalf("as admin: %v", err)
			}
		})
	}
}

// TestServerEveryRPCChecksRole calls every method as every role with a
// request that is valid for all of them, so that a handler that forgets to
// authorize, or names the wrong method, is caught.
func TestServerEveryRPCChecksRole(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ref := &jobqueuev1.LeaseRef{Id: 1, Queue: "q1", Token: 1}
	ttl := durationpb.New(30 * time.Second)
	calls := map[string]func(ctx context.Context) error{
		"Enqueue": func(ctx context.Context) error {
			_, err := ts.client.Enqueue(ctx, enqueueReq(item("q1", "")))
			return err
		},
		"Lease": func(ctx context.Context) error {
			_, err := ts.client.Lease(ctx, leaseReq("q1", 1, 30*time.Second))
			return err
		},
		"Heartbeat": func(ctx context.Context) error {
			_, err := ts.client.Heartbeat(ctx, &jobqueuev1.HeartbeatRequest{Leases: []*jobqueuev1.LeaseRef{ref}, Ttl: ttl})
			return err
		},
		"Ack": func(ctx context.Context) error {
			_, err := ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(ref, "")}})
			return err
		},
		"Release": func(ctx context.Context) error {
			_, err := ts.client.Release(ctx, &jobqueuev1.ReleaseRequest{Leases: []*jobqueuev1.LeaseRef{ref}})
			return err
		},
		"GetJob": func(ctx context.Context) error {
			_, err := ts.client.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: 1})
			return err
		},
		"CancelJob": func(ctx context.Context) error {
			_, err := ts.client.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: 1})
			return err
		},
		"GetStats": func(ctx context.Context) error {
			_, err := ts.client.GetStats(ctx, &jobqueuev1.GetStatsRequest{Queue: "q1"})
			return err
		},
		"ListDeadJobs": func(ctx context.Context) error {
			_, err := ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", Limit: 1})
			return err
		},
		"RequeueJob": func(ctx context.Context) error {
			_, err := ts.client.RequeueJob(ctx, &jobqueuev1.RequeueJobRequest{Id: 1})
			return err
		},
		"PurgeJobs": func(ctx context.Context) error {
			_, err := ts.client.PurgeJobs(ctx, &jobqueuev1.PurgeJobsRequest{Queue: "q1", State: jobqueuev1.JobState_JOB_STATE_SUCCEEDED, FinishedBefore: timestamppb.New(t0)})
			return err
		},
	}
	if len(calls) != len(allowedRoles) {
		t.Fatalf("%d calls for %d methods", len(calls), len(allowedRoles))
	}
	callers := map[Role]string{RoleProducer: tokProducerA, RoleWorker: tokWorkerQ1, RoleAdmin: tokAdmin}
	for method, call := range calls {
		for role, token := range callers {
			err := call(ts.as(token))
			if slices.Contains(allowedRoles[method], role) {
				if status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.Unauthenticated {
					t.Errorf("%s as %s: %v", method, role, err)
				}
				continue
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("%s as %s: code = %v, want PermissionDenied (%v)", method, role, status.Code(err), err)
			}
		}
	}
}

func TestServerLeaseReturnsTheGrants(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	id := ts.add("q1", "acme")
	ts.clock.Advance(5 * time.Second)
	ts.disp.waited = 1500 * time.Millisecond

	resp, err := ts.client.Lease(ts.as(tokWorkerQ1), leaseReq("q1", 10, 45*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetJobs()) != 1 {
		t.Fatalf("jobs = %v", resp.GetJobs())
	}
	j := resp.GetJobs()[0]
	if j.GetId() != id || j.GetQueue() != "q1" || j.GetTenant() != "acme" || string(j.GetPayload()) != "payload" {
		t.Errorf("job = %v", j)
	}
	if j.GetToken() != 1 || j.GetAttempt() != 1 || j.GetMaxAttempts() != job.DefaultMaxAttempts {
		t.Errorf("token, attempt, max_attempts = %d, %d, %d", j.GetToken(), j.GetAttempt(), j.GetMaxAttempts())
	}
	if got := j.GetRemaining().AsDuration(); got != 45*time.Second {
		t.Errorf("remaining = %v, want 45s", got)
	}
	if got := j.GetEnqueuedAt().AsTime(); !got.Equal(t0) {
		t.Errorf("enqueued_at = %v, want %v", got, t0)
	}
	if got := resp.GetWaited().AsDuration(); got != 1500*time.Millisecond {
		t.Errorf("waited = %v", got)
	}
	calls := ts.disp.seen()
	if len(calls) != 1 || calls[0] != (LeaseCall{Queue: "q1", Max: 10, Wait: 20 * time.Second, TTL: 45 * time.Second, Worker: "w1"}) {
		t.Errorf("the dispatcher saw %+v", calls)
	}
}

func TestServerLeaseClampsWait(t *testing.T) {
	tests := []struct {
		name string
		wait *durationpb.Duration
		want time.Duration
	}{
		{"60s is clamped to 30s", durationpb.New(60 * time.Second), 30 * time.Second},
		{"30s stays", durationpb.New(30 * time.Second), 30 * time.Second},
		{"nil means 20s", nil, 20 * time.Second},
		{"5s stays", durationpb.New(5 * time.Second), 5 * time.Second},
		{"an explicit zero is no wait", durationpb.New(0), 0},
		{"negative is no wait", durationpb.New(-5 * time.Second), 0},
		{"huge is clamped", &durationpb.Duration{Seconds: 315576000000}, 30 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, testOpts{})
			req := leaseReq("q1", 1, 30*time.Second)
			req.Wait = tc.wait
			if _, err := ts.client.Lease(ts.as(tokWorkerQ1), req); err != nil {
				t.Fatal(err)
			}
			calls := ts.disp.seen()
			if len(calls) != 1 || calls[0].Wait != tc.want {
				t.Fatalf("the dispatcher saw %+v, want a wait of %v", calls, tc.want)
			}
		})
	}
	t.Run("a malformed wait is invalid", func(t *testing.T) {
		ts := newTestServer(t, testOpts{})
		req := leaseReq("q1", 1, 30*time.Second)
		req.Wait = &durationpb.Duration{Seconds: 1, Nanos: -1}
		_, err := ts.client.Lease(ts.as(tokWorkerQ1), req)
		wantCode(t, err, codes.InvalidArgument)
	})
}

func TestServerLeaseValidatesTTL(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	validTTLs := 0
	tests := []struct {
		name string
		ttl  *durationpb.Duration
		ok   bool
	}{
		{"nil", nil, false},
		{"500ms", durationpb.New(500 * time.Millisecond), false},
		{"zero", durationpb.New(0), false},
		{"negative", durationpb.New(-time.Second), false},
		{"1s", durationpb.New(time.Second), true},
		{"1h", durationpb.New(time.Hour), true},
		{"1h and 1us", durationpb.New(time.Hour + time.Microsecond), false},
		{"1s and 1ns", durationpb.New(time.Second + time.Nanosecond), false},
		{"1s and 1us", durationpb.New(time.Second + time.Microsecond), true},
		{"malformed", &durationpb.Duration{Seconds: 5, Nanos: -1}, false},
		{"a billion nanoseconds", &durationpb.Duration{Seconds: 5, Nanos: 1_000_000_000}, false},
	}
	for _, tc := range tests {
		t.Run("ttl "+tc.name, func(t *testing.T) {
			req := leaseReq("q1", 1, time.Second)
			req.Ttl = tc.ttl
			_, err := ts.client.Lease(ctx, req)
			if tc.ok {
				validTTLs++
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantCode(t, err, codes.InvalidArgument)
		})
	}
	if got := len(ts.disp.seen()); got != validTTLs {
		t.Errorf("%d leases with a bad ttl reached the dispatcher", got-validTTLs)
	}
	before := len(ts.disp.seen())

	for _, n := range []int32{0, -1, job.MaxBatch + 1, 1 << 30} {
		_, err := ts.client.Lease(ctx, leaseReq("q1", n, 30*time.Second))
		wantCode(t, err, codes.InvalidArgument)
	}
	if _, err := ts.client.Lease(ctx, leaseReq("q1", job.MaxBatch, 30*time.Second)); err != nil {
		t.Errorf("max_jobs of %d: %v", job.MaxBatch, err)
	}
	for _, w := range []string{"", "has space", "tab\t", strings.Repeat("w", 129), "ünï"} {
		req := leaseReq("q1", 1, 30*time.Second)
		req.WorkerId = w
		_, err := ts.client.Lease(ctx, req)
		wantCode(t, err, codes.InvalidArgument)
	}
	// Of the calls since `before`, only the valid max_jobs reached the
	// dispatcher.
	if got := len(ts.disp.seen()) - before; got != 1 {
		t.Errorf("%d invalid leases reached the dispatcher", got-1)
	}
}

func TestServerDrainingLeaseUnavailable(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ts.add("q1", "t")
	ts.srv.SetDraining()

	_, err := ts.client.Lease(ts.as(tokWorkerQ1), leaseReq("q1", 1, 30*time.Second))
	wantCode(t, err, codes.Unavailable)
	if msg := status.Convert(err).Message(); msg != "broker is draining" {
		t.Errorf("message = %q", msg)
	}
	if calls := ts.disp.seen(); len(calls) != 0 {
		t.Errorf("the dispatcher was called while draining: %v", calls)
	}

	// Everything else keeps working while the broker drains: workers must be
	// able to finish and report what they hold.
	j := ts.leased("q1", "t")
	if _, err := ts.client.Enqueue(ts.as(tokAdmin), enqueueReq(item("q1", ""))); err != nil {
		t.Errorf("Enqueue while draining: %v", err)
	}
	resp, err := ts.client.Ack(ts.as(tokWorkerQ1), &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(ts.ref(j), "ok")}})
	if err != nil || resp.GetResults()[0] != jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED {
		t.Errorf("Ack while draining = %v, %v", resp, err)
	}
	// An invalid lease is still an invalid argument, not a drain.
	_, err = ts.client.Lease(ts.as(tokWorkerQ1), leaseReq("-bad", 1, 30*time.Second))
	wantCode(t, err, codes.InvalidArgument)
}

func TestServerAckMixedBatch(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	for i := 0; i < 3; i++ {
		ts.add("q1", "t")
	}
	grants := ts.leaseDirect("q1", 3, 30*time.Second)
	j1, j2, j3 := grants[0].Job, grants[1].Job, grants[2].Job
	stale := ts.ref(j3)
	stale.Token--

	resp, err := ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{
		successItem(ts.ref(j1), "done"),
		failureItem(ts.ref(j2), &jobqueuev1.Failure{Error: "boom", Attempt: 1}),
		successItem(stale, "late"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []jobqueuev1.AckStatus{
		jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED, jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED, jobqueuev1.AckStatus_ACK_STATUS_FENCED,
	}
	if !slices.Equal(resp.GetResults(), want) {
		t.Fatalf("results = %v, want %v", resp.GetResults(), want)
	}
	if got := ts.job(j1.ID); got.State != job.StateSucceeded || string(got.Result) != "done" {
		t.Errorf("job 1 = %v %q", got.State, got.Result)
	}
	if got := ts.job(j2.ID); got.State != job.StateReady || got.LastError != "boom" {
		t.Errorf("job 2 = %v %q", got.State, got.LastError)
	}
	if got := ts.job(j3.ID); got.State != job.StateLeased {
		t.Errorf("job 3 = %v, want it still leased", got.State)
	}
	for result, n := range map[string]float64{"accepted": 2, "fenced": 1, "duplicate": 0} {
		if got := ts.counter("jobqueue_acks_total", map[string]string{"op": "complete", "result": result}) +
			ts.counter("jobqueue_acks_total", map[string]string{"op": "fail", "result": result}); got != n {
			t.Errorf("acks{%s} = %v, want %v", result, got, n)
		}
	}
}

func TestServerAckKeepsRequestOrder(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	for i := 0; i < 4; i++ {
		ts.add("q1", "t")
	}
	grants := ts.leaseDirect("q1", 4, 30*time.Second)
	refs := make([]*jobqueuev1.LeaseRef, 4)
	for i, g := range grants {
		refs[i] = ts.ref(g.Job)
	}
	staleFail := &jobqueuev1.LeaseRef{Id: refs[0].Id, Queue: "q1", Token: refs[0].Token + 7}
	unknown := &jobqueuev1.LeaseRef{Id: 999, Queue: "q1", Token: 1}

	// Fail, success, fail, success, interleaved with stale and unknown refs.
	resp, err := ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{
		failureItem(staleFail, &jobqueuev1.Failure{Error: "x", Attempt: 1}),
		successItem(refs[1], "r1"),
		failureItem(refs[2], &jobqueuev1.Failure{Error: "y", Attempt: 1}),
		successItem(unknown, "r2"),
		successItem(refs[3], "r3"),
		failureItem(unknown, &jobqueuev1.Failure{Error: "z", Attempt: 1}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	acc, fen := jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED, jobqueuev1.AckStatus_ACK_STATUS_FENCED
	if want := []jobqueuev1.AckStatus{fen, acc, acc, fen, acc, fen}; !slices.Equal(resp.GetResults(), want) {
		t.Fatalf("results = %v, want %v", resp.GetResults(), want)
	}

	// The same batch again: the completions are duplicates, the failures that
	// were applied no longer hold their leases.
	resp, err = ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{
		successItem(refs[1], "r1"),
		failureItem(refs[2], &jobqueuev1.Failure{Error: "y", Attempt: 1}),
		successItem(refs[3], "r3"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	dup := jobqueuev1.AckStatus_ACK_STATUS_DUPLICATE
	if want := []jobqueuev1.AckStatus{dup, fen, dup}; !slices.Equal(resp.GetResults(), want) {
		t.Fatalf("repeated results = %v, want %v", resp.GetResults(), want)
	}
	if got := ts.counter("jobqueue_acks_total", map[string]string{"op": "complete", "result": "duplicate"}); got != 2 {
		t.Errorf("acks{complete, duplicate} = %v, want 2", got)
	}
}

func TestServerAckRejectsMalformedItems(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	j := ts.leased("q1", "t")
	ref := ts.ref(j)
	many := make([]*jobqueuev1.AckItem, job.MaxAckBatch+1)
	for i := range many {
		many[i] = successItem(ref, "")
	}
	tests := []struct {
		name  string
		items []*jobqueuev1.AckItem
	}{
		{"no items", nil},
		{"too many items", many},
		{"neither success nor failure", []*jobqueuev1.AckItem{successItem(ref, "ok"), {Lease: ref}}},
		{"no lease", []*jobqueuev1.AckItem{successItem(ref, "ok"), successItem(nil, "")}},
		{"a nil item", []*jobqueuev1.AckItem{successItem(ref, "ok"), nil}},
		{"a result over the limit", []*jobqueuev1.AckItem{successItem(ref, strings.Repeat("r", job.MaxResultBytes+1))}},
		{"a negative retry_after", []*jobqueuev1.AckItem{failureItem(ref, &jobqueuev1.Failure{Error: "x", Attempt: 1, RetryAfter: durationpb.New(-time.Second)})}},
		{"a retry_after over a year", []*jobqueuev1.AckItem{failureItem(ref, &jobqueuev1.Failure{Error: "x", Attempt: 1, RetryAfter: durationpb.New(job.MaxDelay + time.Second)})}},
		{"a retry_after in nanoseconds", []*jobqueuev1.AckItem{failureItem(ref, &jobqueuev1.Failure{Error: "x", Attempt: 1, RetryAfter: durationpb.New(1500 * time.Nanosecond)})}},
		{"a malformed retry_after", []*jobqueuev1.AckItem{failureItem(ref, &jobqueuev1.Failure{Error: "x", Attempt: 1, RetryAfter: &durationpb.Duration{Seconds: 1, Nanos: -1}})}},
		{"a retry_after with a billion nanoseconds", []*jobqueuev1.AckItem{failureItem(ref, &jobqueuev1.Failure{Error: "x", Attempt: 1, RetryAfter: &durationpb.Duration{Seconds: 1, Nanos: 1_000_000_000}})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: tc.items})
			wantCode(t, err, codes.InvalidArgument)
			ts.noStoreCalls(t)
			if got := ts.job(j.ID); got.State != job.StateLeased {
				t.Fatalf("a rejected batch changed the job to %v", got.State)
			}
		})
	}
}

func TestServerAckResultLimit(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	j := ts.leased("q1", "t")
	_, err := ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(ts.ref(j), strings.Repeat("r", job.MaxResultBytes+1))}})
	wantCode(t, err, codes.InvalidArgument)
	resp, err := ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(ts.ref(j), strings.Repeat("r", job.MaxResultBytes))}})
	if err != nil || resp.GetResults()[0] != jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED {
		t.Fatalf("a result at the limit: %v, %v", resp, err)
	}
	if got := len(ts.job(j.ID).Result); got != job.MaxResultBytes {
		t.Errorf("stored result is %d bytes", got)
	}
}

// failDelays leases n jobs, fails them all in one Ack with the given client
// attempt and no retry_after, and returns how long each now waits to run.
func failDelays(t *testing.T, seed uint64, n int, attempt int32) []time.Duration {
	t.Helper()
	ts := newTestServer(t, testOpts{seed: seed})
	for i := 0; i < n; i++ {
		ts.add("q1", "t")
	}
	grants := ts.leaseDirect("q1", n, 30*time.Second)
	items := make([]*jobqueuev1.AckItem, n)
	for i, g := range grants {
		items[i] = failureItem(ts.ref(g.Job), &jobqueuev1.Failure{Error: "boom", Attempt: attempt})
	}
	resp, err := ts.client.Ack(ts.as(tokWorkerQ1), &jobqueuev1.AckRequest{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	delays := make([]time.Duration, n)
	for i, g := range grants {
		if resp.GetResults()[i] != jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED {
			t.Fatalf("item %d: %v", i, resp.GetResults()[i])
		}
		j := ts.job(g.Job.ID)
		if j.State != job.StateReady {
			t.Fatalf("job %d is %v after a retryable failure", j.ID, j.State)
		}
		delays[i] = j.RunAt.Sub(ts.clock.Now())
	}
	return delays
}

func TestServerFailUsesClientAttemptForBackoff(t *testing.T) {
	const n = 40
	delays := failDelays(t, 42, n, 3)
	// Attempt 3 has a ceiling of 4s. The jobs were leased once, so a broker
	// that used the job's own attempt count would draw below 1s every time;
	// with 40 draws, one in [1s, 4s) is certain.
	var over1s int
	distinct := map[time.Duration]bool{}
	for i, d := range delays {
		if d < 0 || d >= 4*time.Second {
			t.Errorf("delay %d = %v, want [0, 4s)", i, d)
		}
		if d%time.Microsecond != 0 {
			t.Errorf("delay %d = %v is not a whole number of microseconds", i, d)
		}
		if d >= time.Second {
			over1s++
		}
		distinct[d] = true
	}
	if over1s == 0 {
		t.Error("no delay reached 1s: the broker did not use the client's attempt")
	}
	if len(distinct) < n/2 {
		t.Errorf("only %d distinct delays of %d: the jitter is not drawn per job", len(distinct), n)
	}

	for _, d := range failDelays(t, 42, n, 1) {
		if d < 0 || d >= time.Second {
			t.Errorf("attempt 1: delay %v, want [0, 1s)", d)
		}
	}
	long := 0
	for _, d := range failDelays(t, 42, n, 30) {
		if d < 0 || d >= time.Hour {
			t.Errorf("attempt 30: delay %v, want [0, 1h)", d)
		}
		if d > 4*time.Second {
			long++
		}
	}
	if long == 0 {
		t.Error("attempt 30 never waited more than 4s")
	}
}

func TestServerBackoffIsReproducibleFromTheSeed(t *testing.T) {
	a := failDelays(t, 7, 16, 3)
	b := failDelays(t, 7, 16, 3)
	c := failDelays(t, 8, 16, 3)
	if !slices.Equal(a, b) {
		t.Errorf("the same seed gave different delays:\n%v\n%v", a, b)
	}
	if slices.Equal(a, c) {
		t.Errorf("different seeds gave the same delays: %v", a)
	}
}

func TestServerRetryAfterOverridesBackoff(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	j := ts.leased("q1", "t")
	resp, err := ts.client.Ack(ctx, &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{
		failureItem(ts.ref(j), &jobqueuev1.Failure{Error: "later", Attempt: 3, RetryAfter: durationpb.New(90 * time.Second)}),
	}})
	if err != nil || resp.GetResults()[0] != jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED {
		t.Fatalf("Ack = %v, %v", resp, err)
	}
	got := ts.job(j.ID)
	if d := got.RunAt.Sub(ts.clock.Now()); d != 90*time.Second {
		t.Fatalf("run_at - now = %v, want exactly 90s", d)
	}
	if got.LastError != "later" {
		t.Errorf("last_error = %q", got.LastError)
	}
}

func TestServerFailPermanent(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	j := ts.leased("q1", "t")
	resp, err := ts.client.Ack(ts.as(tokWorkerQ1), &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{
		failureItem(ts.ref(j), &jobqueuev1.Failure{Error: "bad input", Permanent: true, Attempt: 1}),
	}})
	if err != nil || resp.GetResults()[0] != jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED {
		t.Fatalf("Ack = %v, %v", resp, err)
	}
	if got := ts.job(j.ID); got.State != job.StateDead || got.LastError != "bad input" {
		t.Fatalf("job = %v %q", got.State, got.LastError)
	}
}

func TestServerHeartbeatLostStatus(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	live := ts.leased("q1", "t")
	ts.add("q1", "t")
	soon := ts.leaseDirect("q1", 1, time.Second)[0].Job
	stale := ts.ref(live)
	stale.Token++
	unknown := &jobqueuev1.LeaseRef{Id: 12345, Queue: "q1", Token: 1}
	ts.clock.Advance(2 * time.Second) // soon's lease ran out; live's has 28s left

	resp, err := ts.client.Heartbeat(ctx, &jobqueuev1.HeartbeatRequest{
		Leases: []*jobqueuev1.LeaseRef{ts.ref(live), stale, unknown, ts.ref(soon)},
		Ttl:    durationpb.New(60 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	held, lost := jobqueuev1.LeaseStatus_LEASE_STATUS_HELD, jobqueuev1.LeaseStatus_LEASE_STATUS_LOST
	got := resp.GetResults()
	if len(got) != 4 || got[0].GetStatus() != held || got[1].GetStatus() != lost || got[2].GetStatus() != lost || got[3].GetStatus() != lost {
		t.Fatalf("results = %v, want [HELD LOST LOST LOST]", got)
	}
	if d := got[0].GetRemaining().AsDuration(); d != 60*time.Second {
		t.Errorf("remaining of the extended lease = %v, want 60s", d)
	}
	for i := 1; i < 4; i++ {
		if got[i].GetRemaining() != nil && got[i].GetRemaining().AsDuration() != 0 {
			t.Errorf("a lost lease reports remaining %v", got[i].GetRemaining())
		}
	}
	if c := ts.counter("jobqueue_acks_total", map[string]string{"op": "heartbeat", "result": "accepted"}); c != 1 {
		t.Errorf("acks{heartbeat, accepted} = %v, want 1", c)
	}
	if c := ts.counter("jobqueue_acks_total", map[string]string{"op": "heartbeat", "result": "fenced"}); c != 3 {
		t.Errorf("acks{heartbeat, fenced} = %v, want 3", c)
	}
}

func TestServerHeartbeatValidates(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	ref := &jobqueuev1.LeaseRef{Id: 1, Queue: "q1", Token: 1}
	many := make([]*jobqueuev1.LeaseRef, job.MaxAckBatch+1)
	for i := range many {
		many[i] = ref
	}
	for name, req := range map[string]*jobqueuev1.HeartbeatRequest{
		"no leases":     {Ttl: durationpb.New(30 * time.Second)},
		"too many":      {Leases: many, Ttl: durationpb.New(30 * time.Second)},
		"no ttl":        {Leases: []*jobqueuev1.LeaseRef{ref}},
		"a short ttl":   {Leases: []*jobqueuev1.LeaseRef{ref}, Ttl: durationpb.New(time.Millisecond)},
		"a nil lease":   {Leases: []*jobqueuev1.LeaseRef{ref, nil}, Ttl: durationpb.New(30 * time.Second)},
		"a ttl in ns":   {Leases: []*jobqueuev1.LeaseRef{ref}, Ttl: durationpb.New(time.Second + time.Nanosecond)},
		"a long ttl":    {Leases: []*jobqueuev1.LeaseRef{ref}, Ttl: durationpb.New(time.Hour + time.Second)},
		"a bad queue":   {Leases: []*jobqueuev1.LeaseRef{{Id: 1, Queue: "no way", Token: 1}}, Ttl: durationpb.New(30 * time.Second)},
		"an empty name": {Leases: []*jobqueuev1.LeaseRef{{Id: 1, Token: 1}}, Ttl: durationpb.New(30 * time.Second)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ts.client.Heartbeat(ctx, req)
			wantCode(t, err, codes.InvalidArgument)
			ts.noStoreCalls(t)
		})
	}
	full := &jobqueuev1.HeartbeatRequest{Leases: many[:job.MaxAckBatch], Ttl: durationpb.New(30 * time.Second)}
	if resp, err := ts.client.Heartbeat(ctx, full); err != nil || len(resp.GetResults()) != job.MaxAckBatch {
		t.Fatalf("a full batch: %v, %v", len(resp.GetResults()), err)
	}
}

func TestServerReleaseStatuses(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokWorkerQ1)
	live := ts.leased("q1", "t")
	stale := ts.ref(live)
	stale.Token++
	unknown := &jobqueuev1.LeaseRef{Id: 777, Queue: "q1", Token: 1}

	resp, err := ts.client.Release(ctx, &jobqueuev1.ReleaseRequest{Leases: []*jobqueuev1.LeaseRef{unknown, ts.ref(live), stale}})
	if err != nil {
		t.Fatal(err)
	}
	acc, fen := jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED, jobqueuev1.AckStatus_ACK_STATUS_FENCED
	if want := []jobqueuev1.AckStatus{fen, acc, fen}; !slices.Equal(resp.GetResults(), want) {
		t.Fatalf("results = %v, want %v", resp.GetResults(), want)
	}
	if got := ts.job(live.ID); got.State != job.StateReady || got.Attempts != 0 {
		t.Errorf("released job = %v with %d attempts, want ready with the attempt refunded", got.State, got.Attempts)
	}
	resp, err = ts.client.Release(ctx, &jobqueuev1.ReleaseRequest{Leases: []*jobqueuev1.LeaseRef{ts.ref(live)}})
	if err != nil || resp.GetResults()[0] != fen {
		t.Fatalf("releasing twice = %v, %v; want FENCED", resp, err)
	}
	if got := ts.counter("jobqueue_acks_total", map[string]string{"op": "release", "result": "accepted"}); got != 1 {
		t.Errorf("acks{release, accepted} = %v, want 1", got)
	}
	if got := ts.counter("jobqueue_acks_total", map[string]string{"op": "release", "result": "fenced"}); got != 3 {
		t.Errorf("acks{release, fenced} = %v, want 3", got)
	}

	for _, req := range []*jobqueuev1.ReleaseRequest{{}, {Leases: []*jobqueuev1.LeaseRef{nil}}} {
		_, err := ts.client.Release(ctx, req)
		wantCode(t, err, codes.InvalidArgument)
	}
}

func TestServerGetJob(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	mine := ts.leased("q1", "a")
	other := ts.add("q1", "b")

	for _, token := range []string{tokProducerA, tokAdmin} {
		resp, err := ts.client.GetJob(ts.as(token), &jobqueuev1.GetJobRequest{Id: mine.ID})
		if err != nil {
			t.Fatalf("as %s: %v", token, err)
		}
		j := resp.GetJob()
		if j.GetId() != mine.ID || j.GetQueue() != "q1" || j.GetTenant() != "a" || string(j.GetPayload()) != "payload" ||
			j.GetState() != jobqueuev1.JobState_JOB_STATE_LEASED || j.GetAttempts() != 1 || j.GetMaxAttempts() != job.DefaultMaxAttempts ||
			j.GetEpoch() != 1 || !j.GetCreatedAt().AsTime().Equal(t0) || !j.GetRunAt().AsTime().Equal(t0) ||
			!j.GetLeaseExpiresAt().AsTime().Equal(t0.Add(30*time.Second)) {
			t.Errorf("as %s: job = %v", token, j)
		}
		if j.GetFinishedAt() != nil {
			t.Errorf("a leased job has finished_at %v", j.GetFinishedAt())
		}
	}

	_, err := ts.client.GetJob(ts.as(tokProducerA), &jobqueuev1.GetJobRequest{Id: other})
	wantCode(t, err, codes.NotFound)
	if resp, err := ts.client.GetJob(ts.as(tokAdmin), &jobqueuev1.GetJobRequest{Id: other}); err != nil || resp.GetJob().GetTenant() != "b" {
		t.Errorf("an admin sees every tenant: %v, %v", resp, err)
	}
	_, err = ts.client.GetJob(ts.as(tokAdmin), &jobqueuev1.GetJobRequest{Id: 9999})
	wantCode(t, err, codes.NotFound)

	done := ts.succeeded("q1", "a")
	ts.clock.Advance(time.Minute)
	resp, err := ts.client.GetJob(ts.as(tokProducerA), &jobqueuev1.GetJobRequest{Id: done})
	if err != nil {
		t.Fatal(err)
	}
	if j := resp.GetJob(); j.GetState() != jobqueuev1.JobState_JOB_STATE_SUCCEEDED || j.GetFinishedAt() == nil || j.GetLeaseExpiresAt() != nil {
		t.Errorf("succeeded job = %v", j)
	}
}

func TestServerCancelWrongState(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokAdmin)
	done := ts.succeeded("q1", "a")

	_, err := ts.client.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: done})
	wantCode(t, err, codes.FailedPrecondition)
	if msg := status.Convert(err).Message(); msg != "job is succeeded" {
		t.Errorf("message = %q", msg)
	}
	dead := ts.dead("q1", "a")
	_, err = ts.client.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: dead})
	wantCode(t, err, codes.FailedPrecondition)
	if msg := status.Convert(err).Message(); msg != "job is dead" {
		t.Errorf("message = %q", msg)
	}
	if got := ts.job(done).State; got != job.StateSucceeded {
		t.Errorf("the succeeded job became %v", got)
	}
}

func TestServerCancelJob(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	mine := ts.add("q1", "a")
	theirs := ts.add("q1", "b")
	leased := ts.leased("q2", "a")

	// A producer cannot touch another tenant's job, and cannot tell that it
	// exists.
	_, err := ts.client.CancelJob(ts.as(tokProducerA), &jobqueuev1.CancelJobRequest{Id: theirs})
	wantCode(t, err, codes.NotFound)
	if got := ts.job(theirs).State; got != job.StateReady {
		t.Fatalf("another tenant's job is %v after a denied cancel", got)
	}

	for i := 0; i < 2; i++ { // the second call finds it already cancelled
		resp, err := ts.client.CancelJob(ts.as(tokProducerA), &jobqueuev1.CancelJobRequest{Id: mine})
		if err != nil {
			t.Fatalf("cancel %d: %v", i, err)
		}
		if resp.GetState() != jobqueuev1.JobState_JOB_STATE_CANCELLED {
			t.Errorf("cancel %d: state = %v", i, resp.GetState())
		}
	}
	resp, err := ts.client.CancelJob(ts.as(tokAdmin), &jobqueuev1.CancelJobRequest{Id: leased.ID})
	if err != nil || resp.GetState() != jobqueuev1.JobState_JOB_STATE_CANCELLED {
		t.Fatalf("cancelling a leased job: %v, %v", resp, err)
	}
	if out, err := ts.mem.Heartbeat(t.Context(), []store.Ref{{ID: leased.ID, Queue: "q2", Token: leased.Epoch}}, time.Minute); err != nil || out[0].Outcome == job.Applied {
		t.Errorf("the worker of a cancelled job still holds its lease: %v, %v", out, err)
	}
	_, err = ts.client.CancelJob(ts.as(tokAdmin), &jobqueuev1.CancelJobRequest{Id: 424242})
	wantCode(t, err, codes.NotFound)
}

func TestServerRequeueJob(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokAdmin)
	dead := ts.dead("q1", "a")

	if _, err := ts.client.RequeueJob(ctx, &jobqueuev1.RequeueJobRequest{Id: dead}); err != nil {
		t.Fatal(err)
	}
	if got := ts.job(dead); got.State != job.StateReady || got.Attempts != 0 {
		t.Fatalf("requeued job = %v with %d attempts", got.State, got.Attempts)
	}
	_, err := ts.client.RequeueJob(ctx, &jobqueuev1.RequeueJobRequest{Id: dead})
	wantCode(t, err, codes.FailedPrecondition)
	if msg := status.Convert(err).Message(); msg != "job is ready" {
		t.Errorf("message = %q", msg)
	}
	_, err = ts.client.RequeueJob(ctx, &jobqueuev1.RequeueJobRequest{Id: 31337})
	wantCode(t, err, codes.NotFound)
}

func TestServerPurgeJobs(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokAdmin)
	ts.succeeded("q1", "a")
	ts.succeeded("q1", "a")
	ts.succeeded("q2", "a")
	ts.dead("q1", "a")
	ready := ts.add("q1", "a")
	ts.clock.Advance(time.Minute)
	before := timestamppb.New(ts.clock.Now())
	req := func(queue string, state jobqueuev1.JobState, finishedBefore *timestamppb.Timestamp) *jobqueuev1.PurgeJobsRequest {
		return &jobqueuev1.PurgeJobsRequest{Queue: queue, State: state, FinishedBefore: finishedBefore}
	}

	for _, state := range []jobqueuev1.JobState{
		jobqueuev1.JobState_JOB_STATE_UNSPECIFIED, jobqueuev1.JobState_JOB_STATE_READY, jobqueuev1.JobState_JOB_STATE_LEASED, jobqueuev1.JobState(99),
	} {
		_, err := ts.client.PurgeJobs(ctx, req("q1", state, before))
		wantCode(t, err, codes.InvalidArgument)
	}
	_, err := ts.client.PurgeJobs(ctx, req("q1", jobqueuev1.JobState_JOB_STATE_SUCCEEDED, nil))
	wantCode(t, err, codes.InvalidArgument)
	_, err = ts.client.PurgeJobs(ctx, req("q1", jobqueuev1.JobState_JOB_STATE_SUCCEEDED, &timestamppb.Timestamp{Seconds: 1, Nanos: -1}))
	wantCode(t, err, codes.InvalidArgument)
	ts.noStoreCalls(t)
	if n := ts.stored(); n != 5 {
		t.Fatalf("%d jobs left after rejected purges, want 5", n)
	}

	// Nothing finished before the first instant.
	resp, err := ts.client.PurgeJobs(ctx, req("q1", jobqueuev1.JobState_JOB_STATE_SUCCEEDED, timestamppb.New(t0)))
	if err != nil || resp.GetDeleted() != 0 {
		t.Fatalf("purge before t0 = %v, %v", resp, err)
	}
	resp, err = ts.client.PurgeJobs(ctx, req("q1", jobqueuev1.JobState_JOB_STATE_SUCCEEDED, before))
	if err != nil || resp.GetDeleted() != 2 {
		t.Fatalf("purge succeeded = %v, %v; want 2 deleted", resp, err)
	}
	resp, err = ts.client.PurgeJobs(ctx, req("q1", jobqueuev1.JobState_JOB_STATE_DEAD, before))
	if err != nil || resp.GetDeleted() != 1 {
		t.Fatalf("purge dead = %v, %v; want 1 deleted", resp, err)
	}
	// The other queue's job and the ready one are untouched.
	if n := ts.stored(); n != 2 {
		t.Errorf("%d jobs left, want 2", n)
	}
	if got := ts.job(ready).State; got != job.StateReady {
		t.Errorf("the ready job is %v", got)
	}
}

func TestServerGetStats(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ts.add("q1", "a")
	ts.add("q1", "a")
	ts.add("q1", "b")
	ts.leased("q2", "c")
	ts.clock.Advance(3 * time.Second)

	resp, err := ts.client.GetStats(ts.as(tokAdmin), &jobqueuev1.GetStatsRequest{Queue: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetTenants()
	if len(got) != 2 || got[0].GetTenant() != "a" || got[1].GetTenant() != "b" {
		t.Fatalf("tenants = %v", got)
	}
	if got[0].GetReady() != 2 || got[1].GetReady() != 1 || got[0].GetLeased() != 0 {
		t.Errorf("counts = %v", got)
	}
	if age := got[0].GetOldestReadyAge().AsDuration(); age != 3*time.Second {
		t.Errorf("oldest_ready_age = %v, want 3s", age)
	}
	resp, err = ts.client.GetStats(ts.as(tokAdmin), &jobqueuev1.GetStatsRequest{Queue: "q2"})
	if err != nil || len(resp.GetTenants()) != 1 || resp.GetTenants()[0].GetLeased() != 1 {
		t.Fatalf("q2 stats = %v, %v", resp, err)
	}
}

func TestServerListDeadJobsPaginates(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ctx := ts.as(tokAdmin)
	var ids []int64
	for i := 0; i < 5; i++ {
		ids = append(ids, ts.dead("q1", "a"))
	}
	ts.dead("q2", "a")

	var seen []int64
	var after int64
	for page := 0; page < 10; page++ {
		resp, err := ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", AfterId: after, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range resp.GetJobs() {
			if j.GetState() != jobqueuev1.JobState_JOB_STATE_DEAD || j.GetQueue() != "q1" || j.GetLastError() != "gave up" {
				t.Errorf("job = %v", j)
			}
			seen = append(seen, j.GetId())
		}
		if resp.GetNextAfterId() == 0 {
			break
		}
		after = resp.GetNextAfterId()
	}
	if !slices.Equal(seen, ids) {
		t.Fatalf("pages gave %v, want %v", seen, ids)
	}

	// A full page that happens to be the last still names a next page; the
	// one after it is empty and ends the walk.
	resp, err := ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", Limit: 5})
	if err != nil || len(resp.GetJobs()) != 5 || resp.GetNextAfterId() != ids[4] {
		t.Fatalf("a full page = %v, %v", resp, err)
	}
	resp, err = ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", AfterId: ids[4], Limit: 5})
	if err != nil || len(resp.GetJobs()) != 0 || resp.GetNextAfterId() != 0 {
		t.Fatalf("the page after the last = %v, %v", resp, err)
	}
	// A page that is not full ends the list.
	resp, err = ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", AfterId: ids[2], Limit: 3})
	if err != nil || len(resp.GetJobs()) != 2 || resp.GetNextAfterId() != 0 {
		t.Fatalf("a short page = %v, %v; want 2 jobs and next_after_id 0", resp, err)
	}
	// A limit of 0 means the default page.
	resp, err = ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1"})
	if err != nil || len(resp.GetJobs()) != 5 {
		t.Fatalf("the default page = %v, %v", resp, err)
	}
	callsBefore := ts.storeCalls()
	for _, limit := range []int32{-1, job.MaxBatch + 1} {
		_, err := ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", Limit: limit})
		wantCode(t, err, codes.InvalidArgument)
	}
	if got := ts.storeCalls() - callsBefore; got != 0 {
		t.Errorf("%d calls reached the store for invalid limits", got)
	}
	if resp, err := ts.client.ListDeadJobs(ctx, &jobqueuev1.ListDeadJobsRequest{Queue: "q1", Limit: job.MaxBatch}); err != nil || len(resp.GetJobs()) != 5 {
		t.Fatalf("a page of %d: %v, %v", job.MaxBatch, resp, err)
	}
}

// failing makes the store fail the calls the tests below use.
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

func TestServerMapsStoreErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
		logged   bool
	}{
		{"unavailable", fmt.Errorf("%w: dial tcp 10.9.8.7:5432: refused", store.ErrUnavailable), codes.Unavailable, "store unavailable", false},
		{"internal", &store.InternalError{ID: 77, State: job.StateLeased, Msg: "row has no lease"}, codes.Internal, "internal error", true},
		{"anything else", errors.New("pq: syntax error at or near FROM"), codes.Internal, "internal error", true},
		{"a deadline", context.DeadlineExceeded, codes.DeadlineExceeded, "deadline exceeded", false},
		{"a caller error from the store", &job.InvalidError{Field: "payload", Reason: "too big"}, codes.InvalidArgument, "invalid payload: too big", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, testOpts{wrap: func(s store.Store) store.Store { return failingStore{Store: s, err: tc.err} }})
			j := job.Job{ID: 1, Queue: "q1", Epoch: 1}
			ref := &jobqueuev1.LeaseRef{Id: j.ID, Queue: j.Queue, Token: j.Epoch}

			calls := map[string]func() error{
				"Enqueue": func() error {
					_, err := ts.client.Enqueue(ts.as(tokAdmin), enqueueReq(item("q1", "")))
					return err
				},
				"Ack": func() error {
					_, err := ts.client.Ack(ts.as(tokWorkerQ1), &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(ref, "")}})
					return err
				},
				"GetJob": func() error {
					_, err := ts.client.GetJob(ts.as(tokAdmin), &jobqueuev1.GetJobRequest{Id: 1})
					return err
				},
			}
			for name, call := range calls {
				err := call()
				wantCode(t, err, tc.wantCode)
				if msg := status.Convert(err).Message(); msg != tc.wantMsg {
					t.Errorf("%s: message = %q, want %q", name, msg, tc.wantMsg)
				}
				if strings.Contains(status.Convert(err).Message(), "10.9.8.7") || strings.Contains(status.Convert(err).Message(), "syntax") {
					t.Errorf("%s: the message leaks the cause: %q", name, status.Convert(err).Message())
				}
			}
			log := ts.logs.String()
			if tc.logged != strings.Contains(log, "level=ERROR") {
				t.Errorf("logged = %v, want %v:\n%s", strings.Contains(log, "level=ERROR"), tc.logged, log)
			}
			if tc.logged && tc.name == "internal" && (!strings.Contains(log, "job_id=77") || !strings.Contains(log, "state=leased")) {
				t.Errorf("the log lacks the job id and state:\n%s", log)
			}
		})
	}
}

func TestServerLeaseDispatcherErrorsPassThrough(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	ts.disp.err = status.Error(codes.Canceled, "the lease request was abandoned")
	_, err := ts.client.Lease(ts.as(tokWorkerQ1), leaseReq("q1", 1, 30*time.Second))
	wantCode(t, err, codes.Canceled)
	if msg := status.Convert(err).Message(); msg != "the lease request was abandoned" {
		t.Errorf("message = %q", msg)
	}
	ts.disp.err = errors.New("dispatcher exploded")
	_, err = ts.client.Lease(ts.as(tokWorkerQ1), leaseReq("q1", 1, 30*time.Second))
	wantCode(t, err, codes.Internal)
	if !strings.Contains(ts.logs.String(), "dispatcher exploded") {
		t.Errorf("an unexpected dispatcher error was not logged:\n%s", ts.logs.String())
	}
}

func TestServerConcurrentFailsShareOneJitterSource(t *testing.T) {
	ts := newTestServer(t, testOpts{})
	const workers, perWorker = 8, 16
	for i := 0; i < workers*perWorker; i++ {
		ts.add("q1", "t")
	}
	var grants []store.Grant
	for i := 0; i < workers*perWorker/job.MaxBatch; i++ {
		grants = append(grants, ts.leaseDirect("q1", job.MaxBatch, 30*time.Second)...)
	}
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		mine := grants[w*perWorker : (w+1)*perWorker]
		wg.Add(1)
		go func() {
			defer wg.Done()
			items := make([]*jobqueuev1.AckItem, len(mine))
			for i, g := range mine {
				items[i] = failureItem(ts.ref(g.Job), &jobqueuev1.Failure{Error: "x", Attempt: 2})
			}
			_, err := ts.client.Ack(ts.as(tokWorkerQ1), &jobqueuev1.AckRequest{Items: items})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestNewServerPanicsOnNilDependencies(t *testing.T) {
	mem := memstore.New(memstore.Options{Clock: func() time.Time { return t0 }})
	disp := &fakeDispatcher{st: mem}
	m := NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(&syncBuffer{}, nil))
	for name, build := range map[string]func(){
		"store":      func() { NewServer(nil, disp, m, log, 1) },
		"dispatcher": func() { NewServer(mem, nil, m, log, 1) },
		"metrics":    func() { NewServer(mem, disp, nil, log, 1) },
		"logger":     func() { NewServer(mem, disp, m, nil, 1) },
	} {
		func() {
			defer func() {
				msg, _ := recover().(string)
				if !strings.Contains(msg, "NewServer") || !strings.Contains(msg, name) {
					t.Errorf("nil %s: panic = %q, want a message naming NewServer and %s", name, msg, name)
				}
			}()
			build()
		}()
	}
}

// scriptedStore answers Enqueue, Heartbeat, Complete, Fail and Release with
// what a test puts in it, however many items were asked about.
type scriptedStore struct {
	store.Store
	outcomes  []job.Outcome
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

func TestServerAckMapsEveryOutcome(t *testing.T) {
	acc, dup, fen := jobqueuev1.AckStatus_ACK_STATUS_ACCEPTED, jobqueuev1.AckStatus_ACK_STATUS_DUPLICATE, jobqueuev1.AckStatus_ACK_STATUS_FENCED
	tests := []struct {
		outcome job.Outcome
		want    jobqueuev1.AckStatus
	}{
		{job.Applied, acc}, {job.Duplicate, dup}, {job.Fenced, fen}, {job.Expired, fen}, {job.NotFound, fen},
	}
	ref := &jobqueuev1.LeaseRef{Id: 1, Queue: "q1", Token: 1}
	calls := map[string]func(ts *testServer) ([]jobqueuev1.AckStatus, error){
		"complete": func(ts *testServer) ([]jobqueuev1.AckStatus, error) {
			r, err := ts.client.Ack(ts.as(tokWorkerQ1), &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{successItem(ref, "")}})
			return r.GetResults(), err
		},
		"fail": func(ts *testServer) ([]jobqueuev1.AckStatus, error) {
			r, err := ts.client.Ack(ts.as(tokWorkerQ1), &jobqueuev1.AckRequest{Items: []*jobqueuev1.AckItem{failureItem(ref, &jobqueuev1.Failure{Error: "x", Attempt: 1})}})
			return r.GetResults(), err
		},
		"release": func(ts *testServer) ([]jobqueuev1.AckStatus, error) {
			r, err := ts.client.Release(ts.as(tokWorkerQ1), &jobqueuev1.ReleaseRequest{Leases: []*jobqueuev1.LeaseRef{ref}})
			return r.GetResults(), err
		},
	}
	for op, call := range calls {
		for _, tc := range tests {
			t.Run(op+" "+tc.outcome.String(), func(t *testing.T) {
				ts := newTestServer(t, testOpts{wrap: func(s store.Store) store.Store {
					return scriptedStore{Store: s, outcomes: []job.Outcome{tc.outcome}}
				}})
				got, err := call(ts)
				if err != nil || len(got) != 1 || got[0] != tc.want {
					t.Fatalf("results = %v, %v; want [%v]", got, err, tc.want)
				}
			})
		}
		t.Run(op+" wrong_state is a store bug", func(t *testing.T) {
			ts := newTestServer(t, testOpts{wrap: func(s store.Store) store.Store {
				return scriptedStore{Store: s, outcomes: []job.Outcome{job.WrongState}}
			}})
			_, err := call(ts)
			wantCode(t, err, codes.Internal)
			if !strings.Contains(ts.logs.String(), "wrong_state") {
				t.Errorf("the contract breach was not logged:\n%s", ts.logs.String())
			}
		})
		t.Run(op+" too few outcomes is a store bug", func(t *testing.T) {
			ts := newTestServer(t, testOpts{wrap: func(s store.Store) store.Store { return scriptedStore{Store: s} }})
			_, err := call(ts)
			wantCode(t, err, codes.Internal)
		})
	}
}

func TestServerRejectsAStoreThatMiscountsItsResults(t *testing.T) {
	ts := newTestServer(t, testOpts{wrap: func(s store.Store) store.Store { return scriptedStore{Store: s} }})
	_, err := ts.client.Enqueue(ts.as(tokAdmin), enqueueReq(item("q1", ""), item("q1", "")))
	wantCode(t, err, codes.Internal)
	_, err = ts.client.Heartbeat(ts.as(tokWorkerQ1), &jobqueuev1.HeartbeatRequest{
		Leases: []*jobqueuev1.LeaseRef{{Id: 1, Queue: "q1", Token: 1}}, Ttl: durationpb.New(30 * time.Second),
	})
	wantCode(t, err, codes.Internal)
}

func TestServerTenantScope(t *testing.T) {
	for _, tc := range []struct {
		p       Principal
		want    string
		wantErr bool
	}{
		{Principal{Role: RoleProducer, Tenant: "a"}, "a", false},
		{Principal{Role: RoleAdmin}, "", false},
		// UnaryAuth never makes a producer without a tenant; if one is ever
		// made, it must not be read as an admin, whose empty tenant means
		// every tenant.
		{Principal{Role: RoleProducer}, "", true},
	} {
		got, err := tenantScope(tc.p)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("tenantScope(%+v) = %q, %v; want %q, error %v", tc.p, got, err, tc.want, tc.wantErr)
		}
		if tc.wantErr {
			wantCode(t, err, codes.PermissionDenied)
		}
	}
}
