package broker

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/gen/externalscaler"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

const (
	// defaultScalerPush is how often StreamIsActive looks at the backlog when
	// NewScaler is not given an interval.
	defaultScalerPush = time.Second
	// defaultTargetBacklog is the backlog per replica that the autoscaler aims
	// for when the ScaledObject does not say.
	defaultTargetBacklog int64 = 10
	// defaultActivationBacklog is the backlog that must be exceeded to leave
	// zero replicas when the ScaledObject does not say.
	defaultActivationBacklog int64 = 0
	// defaultBacklogTTL is how long a backlog that was read from the store is
	// reused, by every stream and call that asks about the queue meanwhile.
	defaultBacklogTTL = 500 * time.Millisecond
	// defaultReadTimeout bounds a read of the store that the cache makes. The
	// read belongs to every caller that waits for it and not to the one that
	// started it, so it has a limit of its own and not that caller's deadline.
	defaultReadTimeout = 5 * time.Second
	// maxCachedQueues is the most queues the backlog cache holds. The port is
	// open to whoever the network policy admits, and a queue name is theirs to
	// choose, so the cache cannot be allowed to grow with them.
	maxCachedQueues = 1024
)

// The keys of the scaler metadata of a ScaledObject that the Scaler reads.
// KEDA sends the others (scalerAddress, for one) too, and they are ignored.
const (
	metaQueue             = "queue"
	metaTargetBacklog     = "targetBacklog"
	metaActivationBacklog = "activationBacklog"
)

// Scaler answers KEDA's external scaler protocol from the backlog of a queue,
// which lets KEDA scale a worker Deployment on the number of jobs waiting,
// down to zero when there are none.
//
// The ScaledObject says which queue to watch and when to scale in its scaler
// metadata: "queue" is required and is a queue name; "targetBacklog" is the
// backlog one replica should carry, an integer of at least 1, default 10;
// "activationBacklog" is the backlog that must be exceeded for the scaler to
// be active, an integer of at least 0, default 0. Every call checks all three,
// so that a ScaledObject with a typo is refused by every call. Other keys are
// ignored.
//
// The backlog of a queue is the number of its ready and leased jobs, summed
// over its tenants. Leased jobs count so that KEDA does not scale the workers
// that hold them to zero, and scheduled jobs do not, because nothing can
// run them yet; they join the backlog when they fall due. For the same reason
// the Scaler is active whenever a job is leased, whatever activationBacklog
// is, and otherwise when the backlog exceeds activationBacklog. A store that
// cannot answer is an error and never an empty backlog.
//
// A backlog that was read from the store is reused for half a second by every
// stream and call that asks about the queue meanwhile, so that the number of
// streams does not set the number of store queries. Callers that find it stale
// share one read of the store, which runs under a limit of its own. Each of
// them stops waiting when its own context ends or when Drain is called, and
// none is held by another's.
//
// The Scaler is served on a gRPC server of its own, without UnaryAuth: KEDA
// cannot send bearer tokens. What protects the port is the network policy of
// the deployment, which admits only the namespace of KEDA. It reads job
// counts only and changes nothing. StreamMetricSpec stays unimplemented, which
// KEDA takes to mean that it should keep asking GetMetricSpec.
type Scaler struct {
	externalscaler.UnimplementedExternalScalerServer

	st   store.Store
	push time.Duration
	// log receives the internal errors of the store, which the client sees only
	// as "internal error". NewScaler gives it a logger that discards; the
	// broker sets its own before it serves.
	log *slog.Logger

	// cacheTTL is how long a backlog is reused, maxCached the most queues the
	// cache holds, and readTimeout the limit of one read of the store. They
	// are fields so that a test can change them before the Scaler serves.
	cacheTTL    time.Duration
	maxCached   int
	readTimeout time.Duration
	// cacheMu guards the map and every field of the entries in it.
	cacheMu sync.Mutex
	cache   map[string]*backlogEntry

	drainOnce sync.Once
	drained   chan struct{}
	// readCtx is the parent of every read of the store that the cache makes;
	// Drain cancels it.
	readCtx     context.Context
	cancelReads context.CancelFunc
}

// backlogCount is what the Scaler reads from the store about one queue.
type backlogCount struct {
	ready, leased int64
}

// total is the backlog: the ready and the leased jobs.
func (c backlogCount) total() int64 { return c.ready + c.leased }

// backlogEntry is the cached backlog of one queue. The Scaler's cacheMu
// guards all of it.
type backlogEntry struct {
	val backlogCount
	// at is when the read that gave val completed; zero until one has.
	at time.Time
	// flight is the read under way, or nil.
	flight *backlogFlight
}

// backlogFlight is one read of the store that any number of callers wait for.
// Its fields are written once, before done is closed, and read after.
type backlogFlight struct {
	done chan struct{}
	val  backlogCount
	err  error
}

// NewScaler returns a Scaler over st. StreamIsActive looks at the backlog
// every push; a push of 0 or less means one second.
func NewScaler(st store.Store, push time.Duration) *Scaler {
	if push <= 0 {
		push = defaultScalerPush
	}
	readCtx, cancelReads := context.WithCancel(context.Background())
	return &Scaler{
		readCtx: readCtx, cancelReads: cancelReads,
		st: st, push: push, log: slog.New(slog.DiscardHandler),
		cacheTTL: defaultBacklogTTL, maxCached: maxCachedQueues, readTimeout: defaultReadTimeout,
		cache:   make(map[string]*backlogEntry),
		drained: make(chan struct{}),
	}
}

// Drain ends every StreamIsActive call, those open and those still to come,
// with no further message. A graceful stop of the gRPC server that serves the
// Scaler waits for running calls, and a stream never ends on its own, so the
// stop would otherwise run out its whole grace period. KEDA reconnects.
//
// It also releases the calls that wait for a read of the store, with
// Unavailable, and cancels the reads under way: nobody waits for them any more,
// and a read stuck in the database would keep the connection that the shutdown
// has to close. After Drain a call is answered from the backlog the Scaler
// already has, or refused with Unavailable; it does not start a read. Drain
// may be called more than once.
func (s *Scaler) Drain() {
	s.drainOnce.Do(func() {
		close(s.drained)
		s.cancelReads()
	})
}

// scalerSpec is what a ScaledObject asks of the Scaler.
type scalerSpec struct {
	queue      string
	target     int64
	activation int64
}

// metricName is the name of the one metric of the queue. KEDA prefixes it with
// the index of the trigger.
func (p scalerSpec) metricName() string { return "jobqueue-" + p.queue + "-backlog" }

// parseScalerRef reads the scaler metadata of a ScaledObject. Anything wrong
// with it is an InvalidArgument status, and nothing has touched the store.
func parseScalerRef(ref *externalscaler.ScaledObjectRef) (scalerSpec, error) {
	md := ref.GetScalerMetadata()
	queue, ok := md[metaQueue]
	if !ok {
		return scalerSpec{}, status.Errorf(codes.InvalidArgument, "scaler metadata %q is required", metaQueue)
	}
	if err := job.ValidateName(metaQueue, queue); err != nil {
		return scalerSpec{}, status.Errorf(codes.InvalidArgument, "scaler metadata %v", err)
	}
	target, err := metaCount(md, metaTargetBacklog, defaultTargetBacklog, 1)
	if err != nil {
		return scalerSpec{}, err
	}
	activation, err := metaCount(md, metaActivationBacklog, defaultActivationBacklog, 0)
	if err != nil {
		return scalerSpec{}, err
	}
	return scalerSpec{queue: queue, target: target, activation: activation}, nil
}

// metaCount reads the integer that md holds under key, which must be at least
// least, or returns def if there is no such key. A key that is there with a
// value that is not an integer, or is too small, is an error: it is not read as
// the default, because a ScaledObject that says something wrong should hear it.
func metaCount(md map[string]string, key string, def, least int64) (int64, error) {
	raw, ok := md[key]
	if !ok {
		return def, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < least {
		return 0, status.Errorf(codes.InvalidArgument, "scaler metadata %q must be an integer of at least %d", key, least)
	}
	return n, nil
}

// read asks the store for the ready and leased jobs of the queue, over all
// tenants. Its errors are statuses, and the internal ones are logged.
func (s *Scaler) read(ctx context.Context, queue string) (backlogCount, error) {
	stats, err := s.st.Stats(ctx, queue)
	if err != nil {
		return backlogCount{}, toStatusLogged(s.log, "scaler_backlog", err)
	}
	var c backlogCount
	for _, t := range stats {
		c.ready += t.Ready
		c.leased += t.Leased
	}
	return c, nil
}

// counts returns the backlog of the queue: what the store says, unless it said
// so less than cacheTTL ago to someone else. Only an answer is kept; an error
// is not, so a store that is down is asked again by the next caller.
//
// A caller that finds the backlog stale waits for a read of the store, which
// it starts if none is under way. The wait ends with the caller's own context
// and with Drain, whatever becomes of the read.
func (s *Scaler) counts(ctx context.Context, queue string) (backlogCount, error) {
	if s.cacheTTL <= 0 {
		return s.read(ctx, queue)
	}
	flight, val, fresh := s.join(queue)
	if fresh {
		return val, nil
	}
	if flight == nil { // the cache is full of fresh entries
		return s.read(ctx, queue)
	}
	select {
	case <-flight.done:
		return flight.val, flight.err
	case <-ctx.Done():
		return backlogCount{}, status.FromContextError(ctx.Err()).Err()
	case <-s.drained:
		select {
		case <-flight.done: // the read has finished too; an answer beats a refusal
			if flight.err == nil {
				return flight.val, nil
			}
		default:
		}
		return backlogCount{}, errScalerDraining
	}
}

// errScalerDraining is what a call that waited for a read gets when Drain
// ended its wait. KEDA asks again, and another broker answers.
var errScalerDraining = status.Error(codes.Unavailable, "the scaler is shutting down")

// refused is the read that a call gets when the Scaler is draining and the
// backlog it has is not fresh: one that has already failed.
var refused = func() *backlogFlight {
	f := &backlogFlight{done: make(chan struct{}), err: errScalerDraining}
	close(f.done)
	return f
}()

// join returns what a caller asking about the queue should do. If the cached
// backlog is fresh it returns it, with fresh set. Otherwise it returns the
// read to wait for, starting one if none is under way, or nil if the cache has
// no room for the queue and the caller must read for itself.
func (s *Scaler) join(queue string) (flight *backlogFlight, val backlogCount, fresh bool) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	e := s.entryLocked(queue)
	if e == nil {
		return nil, backlogCount{}, false
	}
	if !e.at.IsZero() && time.Since(e.at) < s.cacheTTL {
		return nil, e.val, true
	}
	if e.flight == nil {
		select {
		case <-s.drained:
			return refused, backlogCount{}, false
		default:
		}
		e.flight = &backlogFlight{done: make(chan struct{})}
		go s.fill(queue, e, e.flight)
	}
	return e.flight, backlogCount{}, false
}

// fill carries out the read of a flight and publishes it. It runs under a
// limit of its own and is cancelled by Drain, but not by the context of the
// caller that started it: the other callers wait for it too, and one of them
// leaving must not fail the rest.
// The entry is stamped when the read completes, so that a slow read is still
// shared by the callers that come right after it.
func (s *Scaler) fill(queue string, e *backlogEntry, f *backlogFlight) {
	ctx, cancel := context.WithTimeout(s.readCtx, s.readTimeout)
	defer cancel()
	f.val, f.err = s.read(ctx, queue)
	if f.err != nil {
		select {
		case <-s.drained:
			f.err = errScalerDraining // a read that Drain cancelled is a refusal, not a failure of the store
		default:
		}
	}

	s.cacheMu.Lock()
	if f.err == nil {
		e.val, e.at = f.val, time.Now()
	}
	e.flight = nil
	s.cacheMu.Unlock()
	close(f.done)
}

// entryLocked returns the cache entry of the queue, making it if there is
// none. It returns nil if there is none and no room for one: a full cache
// first drops the entries that have gone stale and are not being read, and if
// none can go the new queue is read through without being kept. The caller
// holds cacheMu.
func (s *Scaler) entryLocked(queue string) *backlogEntry {
	if e, ok := s.cache[queue]; ok {
		return e
	}
	if len(s.cache) >= s.maxCached {
		for q, e := range s.cache {
			if e.flight == nil && (e.at.IsZero() || time.Since(e.at) >= s.cacheTTL) {
				delete(s.cache, q)
			}
		}
		if len(s.cache) >= s.maxCached {
			return nil
		}
	}
	e := &backlogEntry{}
	s.cache[queue] = e
	return e
}

// cachedQueues returns the number of queues in the cache.
func (s *Scaler) cachedQueues() int {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	return len(s.cache)
}

// backlog returns the number of ready and leased jobs of the queue.
func (s *Scaler) backlog(ctx context.Context, queue string) (int64, error) {
	c, err := s.counts(ctx, queue)
	return c.total(), err
}

// active says whether a job of the queue is leased, or the backlog exceeds the
// activation backlog.
func (s *Scaler) active(ctx context.Context, p scalerSpec) (bool, error) {
	c, err := s.counts(ctx, p.queue)
	if err != nil {
		return false, err
	}
	return c.leased > 0 || c.total() > p.activation, nil
}

// IsActive says whether a job of the queue is leased or its backlog exceeds
// activationBacklog.
func (s *Scaler) IsActive(ctx context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.IsActiveResponse, error) {
	p, err := parseScalerRef(ref)
	if err != nil {
		return nil, err
	}
	active, err := s.active(ctx, p)
	if err != nil {
		return nil, err
	}
	return &externalscaler.IsActiveResponse{Result: active}, nil
}

// StreamIsActive sends whether the queue is active as soon as the stream
// opens, and then again whenever the answer differs from the one last sent,
// looking at the backlog every push interval, through the cache that every
// other stream and call shares. It ends when the client goes away, when Drain
// is called, or with the error of a store that cannot answer; KEDA opens a new
// stream after any of them.
func (s *Scaler) StreamIsActive(ref *externalscaler.ScaledObjectRef, stream externalscaler.ExternalScaler_StreamIsActiveServer) error {
	p, err := parseScalerRef(ref)
	if err != nil {
		return err
	}
	ctx := stream.Context()
	tick := time.NewTicker(s.push)
	defer tick.Stop()

	var last, sent bool
	for {
		select {
		case <-s.drained:
			return nil
		default:
		}
		active, err := s.active(ctx, p)
		if err != nil {
			select {
			case <-s.drained:
				return nil // Drain ended the wait for the store; it is a clean end
			default:
			}
			return err
		}
		if !sent || active != last {
			if err := stream.Send(&externalscaler.IsActiveResponse{Result: active}); err != nil {
				return err
			}
			last, sent = active, true
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-s.drained:
			return nil
		case <-tick.C:
		}
	}
}

// GetMetricSpec describes the one metric of the queue, with targetBacklog as
// the target per replica.
func (s *Scaler) GetMetricSpec(_ context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.GetMetricSpecResponse, error) {
	p, err := parseScalerRef(ref)
	if err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricSpecResponse{MetricSpecs: []*externalscaler.MetricSpec{{
		MetricName:      p.metricName(),
		TargetSize:      p.target,
		TargetSizeFloat: float64(p.target),
	}}}, nil
}

// GetMetrics returns the backlog of the queue as the value of its one metric.
// The metric name in the request is not consulted: the ScaledObject names one
// queue, so there is only one metric to return.
func (s *Scaler) GetMetrics(ctx context.Context, req *externalscaler.GetMetricsRequest) (*externalscaler.GetMetricsResponse, error) {
	p, err := parseScalerRef(req.GetScaledObjectRef())
	if err != nil {
		return nil, err
	}
	n, err := s.backlog(ctx, p.queue)
	if err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricsResponse{MetricValues: []*externalscaler.MetricValue{{
		MetricName:       p.metricName(),
		MetricValue:      n,
		MetricValueFloat: float64(n),
	}}}, nil
}
