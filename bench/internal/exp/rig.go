package exp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

// The defaults that the commands and the experiments share.
const (
	DefaultBrokers      = 1
	DefaultWorkers      = 4
	DefaultConcurrency  = 64
	DefaultPayloadBytes = 64
	DefaultDuration     = 30 * time.Second
	DefaultLo           = 100.0
	DefaultHi           = 20000.0
	DefaultSteps        = 8
)

const (
	// benchTenant is the tenant of the jobs of every experiment that has one.
	benchTenant = "bench"
	// startDelay is how long before the first job of a probe the workers get to
	// connect and park their first Lease call. It is part of every intended
	// time, so the latency of the first jobs includes it if it was not enough.
	startDelay = 500 * time.Millisecond
	// cleanupTimeout bounds deleting the rows of a probe.
	cleanupTimeout = 10 * time.Minute
	// maxBrokers bounds --brokers: every broker holds a pool of connections, and
	// PostgreSQL's default allows 100.
	maxBrokers = 8
	// maxPayload is the payload limit of the queue.
	maxPayload = job.MaxPayloadBytes
	// maxConcurrency is the SDK's limit of handlers per worker.
	maxConcurrency = 10000
)

// Setup is what every experiment that runs brokers is told about them.
type Setup struct {
	// DatabaseURL is the PostgreSQL database.
	DatabaseURL string
	// Brokers is the number of brokers; 0 means DefaultBrokers.
	Brokers int
	// MaxDBConns is the size of each broker's database pool; 0 means the
	// broker's default.
	MaxDBConns int32
	// Log receives a line of progress per probe. It may be nil.
	Log func(format string, args ...any)
}

func (s Setup) validate() error {
	switch {
	case s.DatabaseURL == "":
		return ErrNoDatabase
	case s.Brokers < 0 || s.Brokers > maxBrokers:
		return fmt.Errorf("--brokers must be between 1 and %d, got %d", maxBrokers, s.Brokers)
	case s.MaxDBConns < 0:
		return fmt.Errorf("--db-conns must not be negative, got %d", s.MaxDBConns)
	}
	return nil
}

func (s Setup) brokers() int {
	if s.Brokers == 0 {
		return DefaultBrokers
	}
	return s.Brokers
}

func (s Setup) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log(format, args...)
	}
}

// rig is a running cluster with the facts every result of the run shares.
type rig struct {
	Setup
	cluster *Cluster
	env     map[string]string
	prefix  string // starts the name of every queue of the run
	seq     int
}

// openRig starts the cluster and describes the machine and the database.
func openRig(ctx context.Context, s Setup, o ClusterOptions) (*rig, error) {
	o.DatabaseURL, o.Brokers, o.MaxDBConns = s.DatabaseURL, s.brokers(), s.MaxDBConns
	c, err := StartCluster(ctx, o)
	if err != nil {
		return nil, err
	}
	return &rig{
		Setup:   s,
		cluster: c,
		env:     CaptureEnv(ctx, c.Pool()),
		prefix:  "bench-" + strconv.FormatInt(time.Now().Unix(), 36),
	}, nil
}

func (r *rig) close() error { return r.cluster.Close() }

// nextQueue returns a queue name that no earlier probe of this run, and no
// earlier run, used, so that a probe never sees the jobs of another.
func (r *rig) nextQueue() string {
	r.seq++
	return r.prefix + "-" + strconv.Itoa(r.seq)
}

// cleanup deletes the rows of the queue. It does not stop when the experiment is
// interrupted, because the rows would be left behind.
func (r *rig) cleanup(queue string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if _, err := r.cluster.Pool().Exec(ctx, "DELETE FROM jobqueue.jobs WHERE queue = $1", queue); err != nil {
		r.logf("clean up queue %s: %v", queue, err)
	}
}

// workerSpec describes the workers of a probe.
type workerSpec struct {
	// Count is the number of workers, each with a connection of its own, and
	// Concurrency the handlers each runs.
	Count, Concurrency int
	// LeaseMaxJobs, if above zero, caps the max_jobs of every Lease call, which
	// turns batched leasing off at 1.
	LeaseMaxJobs int
	// AckBatch is WorkerOptions.AckBatch; 0 is the default, 1 no coalescing.
	AckBatch int
	// LeaseTTL and Margin are the worker's, if set.
	LeaseTTL, Margin time.Duration
	// Observe, if set, sees every call a worker makes and its error.
	Observe grpc.UnaryClientInterceptor
}

// startWorkers runs the workers on the queue until stop is called. stop waits
// for them to drain and returns the first error any of them ended with, which
// is a broken protocol and never an ordinary end.
func startWorkers(ctx context.Context, c *Cluster, queue string, spec workerSpec, handler jobqueue.Handler) (stop func() error, err error) {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
		conns []*grpc.ClientConn
	)
	closeAll := func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}
	for i := range spec.Count {
		var interceptors []grpc.UnaryClientInterceptor
		if spec.LeaseMaxJobs > 0 {
			interceptors = append(interceptors, capLeaseJobs(spec.LeaseMaxJobs))
		}
		if spec.Observe != nil {
			interceptors = append(interceptors, spec.Observe)
		}
		var dialOpts []jobqueue.DialOption
		if len(interceptors) > 0 {
			dialOpts = append(dialOpts, jobqueue.WithGRPCOptions(grpc.WithChainUnaryInterceptor(interceptors...)))
		}
		conn, err := c.Dial(dialOpts...)
		if err != nil {
			cancel()
			wg.Wait()
			closeAll()
			return nil, fmt.Errorf("dial for worker %d: %w", i, err)
		}
		conns = append(conns, conn)
		w := jobqueue.NewWorker(conn, jobqueue.WorkerOptions{
			Queue: queue, Concurrency: spec.Concurrency, AckBatch: spec.AckBatch,
			LeaseTTL: spec.LeaseTTL, Margin: spec.Margin,
			WorkerID: fmt.Sprintf("bench-w%d", i), Logger: discardLog(), DrainTimeout: 5 * time.Second,
		})
		if err := w.Handle(handler); err != nil {
			cancel()
			wg.Wait()
			closeAll()
			return nil, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(runCtx); err != nil {
				mu.Lock()
				if first == nil {
					first = fmt.Errorf("worker %d: %w", i, err)
				}
				mu.Unlock()
			}
		}()
	}
	var once sync.Once
	return func() error {
		once.Do(func() {
			cancel()
			wg.Wait()
			closeAll()
		})
		mu.Lock()
		defer mu.Unlock()
		return first
	}, nil
}

// capLeaseJobs returns an interceptor that lowers the max_jobs of every Lease
// call to at most n. With n = 1 a worker takes one job per call, which is what
// the benchmark compares batched leasing against. The request is the worker's own
// for this one call, so it is changed in place.
func capLeaseJobs(n int) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if r, ok := req.(*jobqueuev1.LeaseRequest); ok && r.GetMaxJobs() > int32(n) {
			r.MaxJobs = int32(n)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// meterHandler is the handler of every experiment that measures latency: it
// records when the job started, and does nothing else for service time.
func meterHandler(m *Meter, service time.Duration) jobqueue.Handler {
	return func(ctx context.Context, j *jobqueue.Job) error {
		if err := m.ObservePayload(j.Tenant, j.Payload); err != nil {
			return jobqueue.Permanent(err)
		}
		if service > 0 {
			t := time.NewTimer(service)
			defer t.Stop()
			select {
			case <-t.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

// harnessError reports whether err from a send is the harness's own fault, which
// no rate would cure, as opposed to the system failing under load.
func harnessError(err error) bool {
	if errors.Is(err, jobqueue.ErrInvalid) {
		return true
	}
	code := status.Code(err)
	return code == codes.InvalidArgument || code == codes.Unauthenticated || code == codes.PermissionDenied || code == codes.FailedPrecondition
}

// sdkProbe runs probes of the SDK against the cluster: workers with empty
// handlers, and a producer that enqueues one job per send.
type sdkProbe struct {
	*rig
	spec         workerSpec
	payloadBytes int
}

// run is a ProbeFunc.
func (s *sdkProbe) run(ctx context.Context, rate float64, duration, drainWait time.Duration) (*Outcome, error) {
	queue := s.nextQueue()
	defer s.cleanup(queue)

	m := NewMeter(benchTenant)
	stop, err := startWorkers(ctx, s.cluster, queue, s.spec, meterHandler(m, 0))
	if err != nil {
		return nil, err
	}
	conn, err := s.cluster.Dial()
	if err != nil {
		_ = stop()
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	producer := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{PerCallTimeout: 30 * time.Second})

	out, err := Probe(ctx, ProbeConfig{Rate: rate, Duration: duration, DrainWait: drainWait, StartDelay: startDelay}, m, benchTenant,
		func(ctx context.Context, _ int, intended time.Time) error {
			_, err := producer.Enqueue(ctx, jobqueue.EnqueueParams{Queue: queue, Tenant: benchTenant, Payload: m.Payload(intended, s.payloadBytes)})
			return err
		})
	if werr := stop(); werr != nil && err == nil {
		err = werr
	}
	if err != nil {
		return nil, err
	}
	if out.SendErr != nil {
		if harnessError(out.SendErr) {
			return nil, out.SendErr
		}
		s.logf("enqueue failed at %.0f/s, which counts as not sustained: %v", rate, out.SendErr)
	}
	return out, nil
}
