package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/gen/externalscaler"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// scalerPush is the push interval of the Scaler under test, short enough for
// a test to watch a stream change its mind.
const scalerPush = 50 * time.Millisecond

// scalerEnv is a Scaler over a memstore, served over bufconn without an
// interceptor, as KEDA reaches it: it sends no token.
type scalerEnv struct {
	t      *testing.T
	clock  *fakeClock
	mem    *memstore.Store
	calls  atomic.Int64 // store calls that reached the Scaler
	scaler *Scaler
	client externalscaler.ExternalScalerClient
}

// newScalerEnv starts the Scaler with the push interval scalerPush. wrap, if
// set, wraps the store the Scaler uses; the test's own handle stays the bare
// memstore.
func newScalerEnv(t *testing.T, wrap func(store.Store) store.Store) *scalerEnv {
	t.Helper()
	return newScalerEnvPush(t, scalerPush, wrap)
}

// newScalerEnvPush is newScalerEnv with a push interval of its own.
func newScalerEnvPush(t *testing.T, push time.Duration, wrap func(store.Store) store.Store) *scalerEnv {
	t.Helper()
	env := &scalerEnv{t: t, clock: &fakeClock{now: t0}}
	env.mem = memstore.New(memstore.Options{Clock: env.clock.Now})
	var inner store.Store = env.mem
	if wrap != nil {
		inner = wrap(inner)
	}
	env.scaler = NewScaler(spyStore{Store: inner, n: &env.calls}, push)

	gs := grpc.NewServer()
	externalscaler.RegisterExternalScalerServer(gs, env.scaler)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		env.scaler.Drain()
		gs.Stop()
	})
	env.client = externalscaler.NewExternalScalerClient(conn)
	return env
}

// scaledRef builds the reference KEDA sends: a ScaledObject with the given
// scaler metadata, as key, value pairs.
func scaledRef(kv ...string) *externalscaler.ScaledObjectRef {
	md := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		md[kv[i]] = kv[i+1]
	}
	return &externalscaler.ScaledObjectRef{Name: "worker", Namespace: "jobs", ScalerMetadata: md}
}

// add stores n ready jobs of a tenant.
func (e *scalerEnv) add(queue, tenant string, n int) {
	e.t.Helper()
	specs := make([]job.Spec, n)
	for i := range specs {
		specs[i] = job.Spec{Queue: queue, Tenant: tenant, Payload: []byte("p")}
	}
	if _, err := e.mem.Enqueue(e.t.Context(), specs); err != nil {
		e.t.Fatal(err)
	}
}

// addDelayed stores one job that falls due after delay.
func (e *scalerEnv) addDelayed(queue, tenant string, delay time.Duration) {
	e.t.Helper()
	spec := job.Spec{Queue: queue, Tenant: tenant, Payload: []byte("p"), Delay: delay}
	if _, err := e.mem.Enqueue(e.t.Context(), []job.Spec{spec}); err != nil {
		e.t.Fatal(err)
	}
}

// lease leases n ready jobs of a tenant for a worker.
func (e *scalerEnv) lease(queue, tenant string, n int) []store.Grant {
	e.t.Helper()
	grants, err := e.mem.Lease(e.t.Context(), store.LeaseRequest{
		Queue: queue, Takes: []store.TenantTake{{Tenant: tenant, Limit: n}}, TTL: 30 * time.Second, Worker: "w",
	})
	if err != nil || len(grants) != n {
		e.t.Fatalf("Lease = %d grants, %v; want %d", len(grants), err, n)
	}
	return grants
}

func refOf(g store.Grant) store.Ref {
	return store.Ref{ID: g.Job.ID, Queue: g.Job.Queue, Token: g.Job.Epoch}
}

// complete finishes leased jobs.
func (e *scalerEnv) complete(grants []store.Grant) {
	e.t.Helper()
	reqs := make([]store.CompleteRequest, len(grants))
	for i, g := range grants {
		reqs[i] = store.CompleteRequest{Ref: refOf(g)}
	}
	if _, err := e.mem.Complete(e.t.Context(), reqs); err != nil {
		e.t.Fatal(err)
	}
}

// isActive asks the Scaler whether the ScaledObject is active.
func (e *scalerEnv) isActive(ref *externalscaler.ScaledObjectRef) bool {
	e.t.Helper()
	resp, err := e.client.IsActive(e.t.Context(), ref)
	if err != nil {
		e.t.Fatalf("IsActive(%v) = %v", ref.GetScalerMetadata(), err)
	}
	return resp.GetResult()
}

// wantBacklog asks for the metric of a queue and fails unless it is want.
func (e *scalerEnv) wantBacklog(queue string, want int64) {
	e.t.Helper()
	resp, err := e.client.GetMetrics(e.t.Context(), &externalscaler.GetMetricsRequest{
		ScaledObjectRef: scaledRef("queue", queue), MetricName: "jobqueue-" + queue + "-backlog",
	})
	if err != nil {
		e.t.Fatalf("GetMetrics = %v", err)
	}
	values := resp.GetMetricValues()
	if len(values) != 1 {
		e.t.Fatalf("GetMetrics returned %d values, want 1: %v", len(values), values)
	}
	v := values[0]
	if v.GetMetricName() != "jobqueue-"+queue+"-backlog" || v.GetMetricValue() != want || v.GetMetricValueFloat() != float64(want) {
		e.t.Errorf("GetMetrics = {%q %d %v}, want {%q %d %d}",
			v.GetMetricName(), v.GetMetricValue(), v.GetMetricValueFloat(), "jobqueue-"+queue+"-backlog", want, want)
	}
}

// rpcs lists the calls that take a ScaledObjectRef, each as a function that
// makes the call and returns its error. A stream reports the error of its
// first Recv.
func (e *scalerEnv) rpcs() map[string]func(*externalscaler.ScaledObjectRef) error {
	ctx := e.t.Context()
	return map[string]func(*externalscaler.ScaledObjectRef) error{
		"IsActive": func(ref *externalscaler.ScaledObjectRef) error {
			_, err := e.client.IsActive(ctx, ref)
			return err
		},
		"GetMetricSpec": func(ref *externalscaler.ScaledObjectRef) error {
			_, err := e.client.GetMetricSpec(ctx, ref)
			return err
		},
		"GetMetrics": func(ref *externalscaler.ScaledObjectRef) error {
			_, err := e.client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: ref, MetricName: "jobqueue-q-backlog"})
			return err
		},
		"StreamIsActive": func(ref *externalscaler.ScaledObjectRef) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			stream, err := e.client.StreamIsActive(ctx, ref)
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		},
	}
}

// isStream is a stream's Recv loop, run in the background: every message goes
// to msgs, and the error that ends the stream to done.
type isStream struct {
	msgs chan bool
	done chan error
}

func (e *scalerEnv) openStream(ref *externalscaler.ScaledObjectRef) *isStream {
	e.t.Helper()
	ctx, cancel := context.WithCancel(e.t.Context())
	e.t.Cleanup(cancel)
	stream, err := e.client.StreamIsActive(ctx, ref)
	if err != nil {
		e.t.Fatal(err)
	}
	s := &isStream{msgs: make(chan bool, 64), done: make(chan error, 1)}
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				s.done <- err
				return
			}
			s.msgs <- resp.GetResult()
		}
	}()
	return s
}

// next returns the next message, and fails if none comes within d or the
// stream ends first.
func (s *isStream) next(t *testing.T, within time.Duration, what string) bool {
	t.Helper()
	select {
	case v := <-s.msgs:
		return v
	case err := <-s.done:
		t.Fatalf("the stream ended with %v while waiting for %s", err, what)
	case <-time.After(within):
		t.Fatalf("no message within %v while waiting for %s", within, what)
	}
	return false
}

// silent fails if a message arrives, or the stream ends, within d.
func (s *isStream) silent(t *testing.T, d time.Duration, what string) {
	t.Helper()
	select {
	case v := <-s.msgs:
		t.Fatalf("unexpected message %v: %s", v, what)
	case err := <-s.done:
		t.Fatalf("the stream ended with %v: %s", err, what)
	case <-time.After(d):
	}
}

func TestScalerIsActive(t *testing.T) {
	env := newScalerEnv(t, nil)

	// An empty queue is not active.
	if env.isActive(scaledRef("queue", "empty")) {
		t.Error("IsActive on an empty queue = true, want false")
	}

	// One ready job is enough with the default activation of 0.
	env.add("one", "t", 1)
	if !env.isActive(scaledRef("queue", "one")) {
		t.Error("IsActive with one ready job = false, want true")
	}

	// activationBacklog is a threshold the backlog must exceed.
	env.add("threshold", "t", 3)
	if env.isActive(scaledRef("queue", "threshold", "activationBacklog", "5")) {
		t.Error("IsActive with 3 ready jobs and activationBacklog 5 = true, want false")
	}
	if env.isActive(scaledRef("queue", "threshold", "activationBacklog", "3")) {
		t.Error("IsActive with a backlog equal to activationBacklog = true, want false: it must exceed it")
	}
	if !env.isActive(scaledRef("queue", "threshold", "activationBacklog", "2")) {
		t.Error("IsActive with 3 ready jobs and activationBacklog 2 = false, want true")
	}

	// A leased job is still backlog, so that the workers that hold leases are
	// not scaled away.
	env.add("held", "t", 1)
	grants := env.lease("held", "t", 1)
	if !env.isActive(scaledRef("queue", "held")) {
		t.Error("IsActive with only a leased job = false, want true")
	}
	env.complete(grants)
	if env.isActive(scaledRef("queue", "held")) {
		t.Error("IsActive after the last job completed = true, want false")
	}

	// KEDA sends its own keys among the metadata; they are not an error.
	if !env.isActive(scaledRef("queue", "one", "scalerAddress", "broker:7072", "somethingElse", "x")) {
		t.Error("IsActive with extra metadata keys = false, want true")
	}
}

func TestScalerMetricSpecDefaults(t *testing.T) {
	env := newScalerEnv(t, nil)
	resp, err := env.client.GetMetricSpec(t.Context(), scaledRef("queue", "emails"))
	if err != nil {
		t.Fatal(err)
	}
	specs := resp.GetMetricSpecs()
	if len(specs) != 1 {
		t.Fatalf("GetMetricSpec returned %d specs, want 1: %v", len(specs), specs)
	}
	if got := specs[0]; got.GetMetricName() != "jobqueue-emails-backlog" || got.GetTargetSize() != 10 || got.GetTargetSizeFloat() != 10 {
		t.Errorf("spec = {%q %d %v}, want {\"jobqueue-emails-backlog\" 10 10}", got.GetMetricName(), got.GetTargetSize(), got.GetTargetSizeFloat())
	}

	resp, err = env.client.GetMetricSpec(t.Context(), scaledRef("queue", "emails", "targetBacklog", "25"))
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetMetricSpecs()[0]; got.GetTargetSize() != 25 || got.GetTargetSizeFloat() != 25 {
		t.Errorf("spec with targetBacklog 25 = {%d %v}, want 25", got.GetTargetSize(), got.GetTargetSizeFloat())
	}
}

func TestScalerBacklogCountsReadyAndLeasedNotScheduled(t *testing.T) {
	env := newScalerEnv(t, nil)
	env.wantBacklog("q", 0)

	env.add("q", "a", 2)
	env.add("q", "b", 1)
	env.addDelayed("q", "a", time.Hour)
	env.add("other", "a", 5) // another queue does not count
	grants := env.lease("q", "a", 1)

	// Ready 2 (a, b), leased 1 (a), scheduled 1 (a): the backlog is the sum
	// over both tenants of ready and leased, and leaves the scheduled job out.
	env.wantBacklog("q", 3)

	// The scheduled job joins the backlog when it falls due.
	env.clock.Advance(time.Hour)
	env.wantBacklog("q", 4)

	// A job that finished is no longer backlog.
	env.complete(grants)
	env.wantBacklog("q", 3)

	// Nor is a dead one: workers have nothing to do for it.
	dead := env.lease("q", "b", 1)
	if _, err := env.mem.Fail(t.Context(), []store.FailRequest{{Ref: refOf(dead[0]), Permanent: true, Error: "boom"}}); err != nil {
		t.Fatal(err)
	}
	stats, err := env.mem.Stats(t.Context(), "q")
	if err != nil {
		t.Fatal(err)
	}
	var deadJobs int64
	for _, s := range stats {
		deadJobs += s.Dead
	}
	if deadJobs != 1 {
		t.Fatalf("the store has %d dead jobs, want 1: the setup of this case is wrong", deadJobs)
	}
	env.wantBacklog("q", 2)
}

func TestScalerRejectsMissingOrBadQueue(t *testing.T) {
	env := newScalerEnv(t, nil)
	bad := map[string]*externalscaler.ScaledObjectRef{
		"no reference":      nil,
		"no metadata":       {Name: "worker", Namespace: "jobs"},
		"no queue key":      scaledRef("targetBacklog", "10"),
		"empty":             scaledRef("queue", ""),
		"space":             scaledRef("queue", "a b"),
		"tab":               scaledRef("queue", "q\t1"),
		"leading dash":      scaledRef("queue", "-lead"),
		"NUL":               scaledRef("queue", "x\x00"),
		"129 bytes":         scaledRef("queue", strings.Repeat("a", 129)),
		"non-ASCII letters": scaledRef("queue", "ünï"),
	}
	for rpc, call := range env.rpcs() {
		for name, ref := range bad {
			t.Run(rpc+"/"+name, func(t *testing.T) {
				if err := call(ref); status.Code(err) != codes.InvalidArgument {
					t.Errorf("%s with %s = %v, want InvalidArgument", rpc, name, err)
				}
			})
		}
	}
	if n := env.calls.Load(); n != 0 {
		t.Errorf("%d calls reached the store, want 0: a request that is not valid must not get there", n)
	}
}

func TestScalerRejectsBadTarget(t *testing.T) {
	env := newScalerEnv(t, nil)
	bad := map[string]*externalscaler.ScaledObjectRef{
		"target 0":           scaledRef("queue", "q", "targetBacklog", "0"),
		"target negative":    scaledRef("queue", "q", "targetBacklog", "-3"),
		"target not a count": scaledRef("queue", "q", "targetBacklog", "x"),
		"target fractional":  scaledRef("queue", "q", "targetBacklog", "2.5"),
		"target empty":       scaledRef("queue", "q", "targetBacklog", ""),
		"target too large":   scaledRef("queue", "q", "targetBacklog", "9223372036854775808"),
		"activation -1":      scaledRef("queue", "q", "activationBacklog", "-1"),
		"activation x":       scaledRef("queue", "q", "activationBacklog", "x"),
		"activation empty":   scaledRef("queue", "q", "activationBacklog", ""),
	}
	for rpc, call := range env.rpcs() {
		for name, ref := range bad {
			t.Run(rpc+"/"+name, func(t *testing.T) {
				if err := call(ref); status.Code(err) != codes.InvalidArgument {
					t.Errorf("%s with %s = %v, want InvalidArgument", rpc, name, err)
				}
			})
		}
	}
	if n := env.calls.Load(); n != 0 {
		t.Errorf("%d calls reached the store, want 0", n)
	}

	// The edges that are valid: a target of 1, and an activation of 0.
	for name, ref := range map[string]*externalscaler.ScaledObjectRef{
		"target 1":     scaledRef("queue", "q", "targetBacklog", "1"),
		"activation 0": scaledRef("queue", "q", "activationBacklog", "0"),
	} {
		for rpc, call := range env.rpcs() {
			if err := call(ref); err != nil {
				t.Errorf("%s with %s = %v, want success", rpc, name, err)
			}
		}
	}
}

func TestScalerStreamIsActivePushesChanges(t *testing.T) {
	env := newScalerEnv(t, nil)
	s := env.openStream(scaledRef("queue", "q"))

	// The first message is the current state, sent without waiting for a change.
	if s.next(t, 5*time.Second, "the current state") {
		t.Fatal("the first message = true on an empty queue, want false")
	}

	// A change is pushed within a few push intervals, not at some later poll.
	within := 3 * scalerPush
	env.add("q", "t", 1)
	if !s.next(t, within, "true after an enqueue") {
		t.Fatal("after an enqueue the stream said false, want true")
	}

	// A job that is leased is still backlog; completing it ends the backlog.
	grants := env.lease("q", "t", 1)
	env.complete(grants)
	if s.next(t, within, "false after the completion") {
		t.Fatal("after the completion the stream said true, want false")
	}
}

func TestScalerStreamIsActiveSendsOnlyChanges(t *testing.T) {
	env := newScalerEnv(t, nil)
	env.add("q", "t", 1)
	s := env.openStream(scaledRef("queue", "q"))
	if !s.next(t, 5*time.Second, "the current state") {
		t.Fatal("the first message = false with a ready job, want true")
	}
	s.silent(t, 4*scalerPush, "the state did not change")

	// Backlog that moves but stays above the threshold is no change.
	env.add("q", "t", 2)
	s.silent(t, 4*scalerPush, "the backlog changed but the answer did not")

	env.complete(env.lease("q", "t", 3))
	if s.next(t, 3*scalerPush, "false after the backlog cleared") {
		t.Fatal("the stream said true after the backlog cleared, want false")
	}
	s.silent(t, 4*scalerPush, "the state did not change again")
}

func TestScalerStreamMetricSpecUnimplemented(t *testing.T) {
	// KEDA treats Unimplemented as "not offered" and keeps asking
	// GetMetricSpec, which is what the proto documents.
	env := newScalerEnv(t, nil)
	stream, err := env.client.StreamMetricSpec(t.Context(), scaledRef("queue", "q"))
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("StreamMetricSpec = %v, want Unimplemented", err)
	}
}

func TestScalerDrainEndsStreams(t *testing.T) {
	// The push interval is an hour, so that a stream that ends does so because
	// Drain woke it, not because its next look at the backlog came round.
	env := newScalerEnvPush(t, time.Hour, nil)
	s := env.openStream(scaledRef("queue", "q"))
	s.next(t, 5*time.Second, "the current state")

	env.scaler.Drain()
	select {
	case err := <-s.done:
		if !errors.Is(err, io.EOF) {
			t.Errorf("the stream ended with %v, want a clean end (io.EOF)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream was still open 5s after Drain: a graceful stop would wait for it")
	}

	// A stream opened afterwards ends at once, without a message; the unary
	// calls keep working, so that a graceful stop can finish the ones running.
	late := env.openStream(scaledRef("queue", "q"))
	select {
	case err := <-late.done:
		if !errors.Is(err, io.EOF) {
			t.Errorf("a stream opened after Drain ended with %v, want io.EOF", err)
		}
	case v := <-late.msgs:
		t.Errorf("a stream opened after Drain sent %v, want nothing", v)
	case <-time.After(5 * time.Second):
		t.Fatal("a stream opened after Drain stayed open")
	}
	env.scaler.Drain() // twice is fine
	env.wantBacklog("q", 0)
}

// failingStats is a store whose Stats fails while fail is set.
type failingStats struct {
	store.Store
	fail *atomic.Bool
}

func (f failingStats) Stats(ctx context.Context, queue string) ([]store.TenantStats, error) {
	if f.fail.Load() {
		return nil, store.ErrUnavailable
	}
	return f.Store.Stats(ctx, queue)
}

func TestScalerReportsStoreFailures(t *testing.T) {
	var fail atomic.Bool
	env := newScalerEnv(t, func(s store.Store) store.Store { return failingStats{Store: s, fail: &fail} })
	s := env.openStream(scaledRef("queue", "q"))
	s.next(t, 5*time.Second, "the current state")

	// A store that cannot answer is an error, never an answer of "idle", which
	// would let KEDA scale the workers to zero.
	fail.Store(true)
	for rpc, call := range env.rpcs() {
		if rpc == "StreamIsActive" || rpc == "GetMetricSpec" {
			continue // the stream is checked below; the spec holds no backlog
		}
		if err := call(scaledRef("queue", "q")); status.Code(err) != codes.Unavailable {
			t.Errorf("%s with the store down = %v, want Unavailable", rpc, err)
		}
	}
	// An open stream ends with the error, and KEDA reconnects.
	select {
	case err := <-s.done:
		if status.Code(err) != codes.Unavailable {
			t.Errorf("the stream ended with %v, want Unavailable", err)
		}
	case v := <-s.msgs:
		t.Fatalf("the stream sent %v while the store was down", v)
	case <-time.After(5 * time.Second):
		t.Fatal("the stream stayed open with the store down")
	}
}

func TestNewScalerDefaultsThePushInterval(t *testing.T) {
	mem := memstore.New(memstore.Options{Clock: (&fakeClock{now: t0}).Now})
	for _, push := range []time.Duration{0, -time.Second} {
		if got := NewScaler(mem, push).push; got != time.Second {
			t.Errorf("NewScaler(_, %v).push = %v, want 1s", push, got)
		}
	}
	if got := NewScaler(mem, 3*time.Second).push; got != 3*time.Second {
		t.Errorf("NewScaler(_, 3s).push = %v, want 3s", got)
	}
}
