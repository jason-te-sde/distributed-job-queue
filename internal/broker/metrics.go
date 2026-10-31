package broker

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/jason-te-sde/distributed-job-queue/internal/broker/queuelabel"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// The values of the "op" and "result" labels of jobqueue_acks_total.
const (
	opComplete  = "complete"
	opFail      = "fail"
	opRelease   = "release"
	opHeartbeat = "heartbeat"

	resultAccepted  = "accepted"
	resultDuplicate = "duplicate"
	resultFenced    = "fenced"
)

// The values of the "result" label of jobqueue_dispatch_fetches_total.
const (
	fetchJobs  = "jobs"
	fetchEmpty = "empty"
	fetchError = "error"
)

const (
	// maxQueueLabels is how many distinct queue names get a label value of
	// their own. A queue name comes from a request, so without a cap any
	// producer could create series without bound.
	maxQueueLabels = 100
	// otherQueueLabel is the label value of every queue name past the cap.
	otherQueueLabel = "other"
)

// Metrics holds the collectors of the broker. NewMetrics is its only
// constructor, so that every collector is registered exactly once. The
// handlers of Server use the counters below; the maintenance loops, the
// dispatcher and the health endpoint add theirs here.
//
// A collector with a queue label is a queuelabel type, whose vector is
// unexported there: the only way to reach a series is For(queue), which
// applies the cap on distinct queue labels, so a series cannot be labelled
// with a raw queue name.
type Metrics struct {
	// queues caps the distinct values of the queue label.
	queues *queuelabel.Cap
	// enqueued and deduplicated count the jobs that an Enqueue created, and
	// the items that named a job an idempotency key already held, per queue.
	enqueued, deduplicated *queuelabel.Counters
	// acks counts what became of the leases that workers reported on, by
	// operation (complete, fail, release, heartbeat) and result (accepted,
	// duplicate, fenced). A heartbeat that held its lease counts as accepted
	// and one that lost it as fenced.
	acks *prometheus.CounterVec
	// leased counts the jobs the store leased to the dispatcher, per queue.
	leased *queuelabel.Counters
	// fetches counts the dispatcher's store fetches, by queue and result
	// (jobs, empty, error).
	fetches *queuelabel.Counters
	// waiters is the number of long-polling Lease calls in the dispatcher.
	waiters *queuelabel.Gauges
	// leaseWait is how long a Lease call waited before it was given jobs.
	leaseWait prometheus.Histogram
	// orphanReleases counts jobs the dispatcher gave back because the caller
	// they were fetched for had gone.
	orphanReleases prometheus.Counter
	// notifyReconnects counts the times the listener lost its connection,
	// and notifyMalformed the notifications it could not parse.
	notifyReconnects, notifyMalformed prometheus.Counter
	// reaped and pruned count the jobs the maintenance loops took back from
	// expired leases and deleted after their retention.
	reaped, pruned prometheus.Counter
	// storeCall is how long each store call took, by operation.
	storeCall *prometheus.HistogramVec
	// queueJobs is the number of jobs of a queue in each state, refreshed
	// periodically from the store.
	queueJobs *queuelabel.Gauges
	// buildInfo is 1 for the version of this build.
	buildInfoMu sync.Mutex
	buildInfo   *prometheus.GaugeVec
}

// NewMetrics creates the collectors and registers them with reg, together with
// the collectors of the Go runtime and of the process. It panics, as
// prometheus does, if one of them is already registered there, which is why a
// broker registers on a registry of its own and not on the default one.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	if reg != nil {
		reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	}
	queues := queuelabel.NewCap(maxQueueLabels, otherQueueLabel)
	return &Metrics{
		queues: queues,
		enqueued: queuelabel.NewCounters(f, queues, prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_enqueued_total",
			Help: "Jobs created by Enqueue, by queue.",
		}),
		deduplicated: queuelabel.NewCounters(f, queues, prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_deduplicated_total",
			Help: "Enqueue items whose idempotency key already named a job, by queue.",
		}),
		acks: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "acks_total",
			Help: "Reports from workers on their leases, by operation and result.",
		}, []string{"op", "result"}),
		leased: queuelabel.NewCounters(f, queues, prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_leased_total",
			Help: "Jobs the store leased to the dispatcher, by queue.",
		}),
		fetches: queuelabel.NewCounters(f, queues, prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "dispatch_fetches_total",
			Help: "Store fetches of the dispatcher, by queue and result (jobs, empty, error).",
		}, "result"),
		waiters: queuelabel.NewGauges(f, queues, prometheus.GaugeOpts{
			Namespace: "jobqueue", Name: "waiters",
			Help: "Long-polling Lease calls waiting in the dispatcher, by queue.",
		}),
		leaseWait: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "jobqueue", Name: "lease_wait_seconds",
			Help:    "How long a Lease call waited before it was given jobs.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		}),
		orphanReleases: f.NewCounter(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "orphan_releases_total",
			Help: "Jobs given back to the queue because the Lease call they were fetched for had gone.",
		}),
		notifyReconnects: f.NewCounter(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "notify_reconnects_total",
			Help: "Times the NOTIFY listener lost its connection and reconnected.",
		}),
		notifyMalformed: f.NewCounter(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "notify_malformed_total",
			Help: "Notifications whose payload was not a queue and a tenant.",
		}),
		reaped: f.NewCounter(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_reaped_total",
			Help: "Jobs taken back from expired leases by the reaper.",
		}),
		pruned: f.NewCounter(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_pruned_total",
			Help: "Finished jobs deleted by the pruner after their retention.",
		}),
		storeCall: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "jobqueue", Name: "store_call_seconds",
			Help:    "How long store calls took, by operation.",
			Buckets: prometheus.ExponentialBucketsRange(0.0001, 10, 18),
		}, []string{"op"}),
		queueJobs: queuelabel.NewGauges(f, queues, prometheus.GaugeOpts{
			Namespace: "jobqueue", Name: "queue_jobs",
			Help: "Jobs of a queue by state (ready, scheduled, leased, dead), refreshed every 15s for the queues this broker has seen.",
		}, "state"),
		buildInfo: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "jobqueue", Name: "build_info",
			Help: "A constant 1, labelled with the version of the build and the Go version that built it.",
		}, []string{"version", "go_version"}),
	}
}

// queueJobCounts is the number of jobs of one queue in each state.
type queueJobCounts struct{ ready, scheduled, leased, dead int64 }

// setQueueJobs sets the jobs-by-state gauges of a queue.
func (m *Metrics) setQueueJobs(queue string, c queueJobCounts) {
	m.queueJobs.For(queue, "ready").Set(float64(c.ready))
	m.queueJobs.For(queue, "scheduled").Set(float64(c.scheduled))
	m.queueJobs.For(queue, "leased").Set(float64(c.leased))
	m.queueJobs.For(queue, "dead").Set(float64(c.dead))
}

// observeStoreCall records how long a store call of the given operation took.
func (m *Metrics) observeStoreCall(op string, seconds float64) {
	m.storeCall.WithLabelValues(op).Observe(seconds)
}

// SetBuildInfo sets jobqueue_build_info to the version, replacing the series of
// an earlier version: one build runs in one process.
func (m *Metrics) SetBuildInfo(version string) {
	m.buildInfoMu.Lock()
	defer m.buildInfoMu.Unlock()
	m.buildInfo.Reset()
	m.buildInfo.WithLabelValues(version, runtime.Version()).Set(1)
}

// timedStore is a store.Store that records how long each call takes, as
// jobqueue_store_call_seconds. A test lists the methods of the interface and
// fails when one is missing here.
type timedStore struct {
	store.Store
	m *Metrics
}

// done records the time since start for op; it is deferred with the start
// time taken at the defer statement.
func (t timedStore) done(op string, start time.Time) {
	t.m.observeStoreCall(op, time.Since(start).Seconds())
}

func (t timedStore) Enqueue(ctx context.Context, specs []job.Spec) ([]store.EnqueueResult, error) {
	defer t.done("enqueue", time.Now())
	return t.Store.Enqueue(ctx, specs)
}

func (t timedStore) Lease(ctx context.Context, req store.LeaseRequest) ([]store.Grant, error) {
	defer t.done("lease", time.Now())
	return t.Store.Lease(ctx, req)
}

func (t timedStore) Heartbeat(ctx context.Context, refs []store.Ref, ttl time.Duration) ([]store.HeartbeatResult, error) {
	defer t.done("heartbeat", time.Now())
	return t.Store.Heartbeat(ctx, refs, ttl)
}

func (t timedStore) Complete(ctx context.Context, reqs []store.CompleteRequest) ([]job.Outcome, error) {
	defer t.done("complete", time.Now())
	return t.Store.Complete(ctx, reqs)
}

func (t timedStore) Fail(ctx context.Context, reqs []store.FailRequest) ([]job.Outcome, error) {
	defer t.done("fail", time.Now())
	return t.Store.Fail(ctx, reqs)
}

func (t timedStore) Release(ctx context.Context, refs []store.Ref) ([]job.Outcome, error) {
	defer t.done("release", time.Now())
	return t.Store.Release(ctx, refs)
}

func (t timedStore) Cancel(ctx context.Context, id int64, tenant string) (job.Outcome, error) {
	defer t.done("cancel", time.Now())
	return t.Store.Cancel(ctx, id, tenant)
}

func (t timedStore) Requeue(ctx context.Context, id int64) (job.Outcome, error) {
	defer t.done("requeue", time.Now())
	return t.Store.Requeue(ctx, id)
}

func (t timedStore) Reap(ctx context.Context, limit int, backoff store.BackoffFunc) (int, error) {
	defer t.done("reap", time.Now())
	return t.Store.Reap(ctx, limit, backoff)
}

func (t timedStore) Prune(ctx context.Context, r store.Retention, limit int) (int, error) {
	defer t.done("prune", time.Now())
	return t.Store.Prune(ctx, r, limit)
}

func (t timedStore) Purge(ctx context.Context, req store.PurgeRequest) (int, error) {
	defer t.done("purge", time.Now())
	return t.Store.Purge(ctx, req)
}

func (t timedStore) Get(ctx context.Context, id int64, tenant string) (job.Job, error) {
	defer t.done("get", time.Now())
	return t.Store.Get(ctx, id, tenant)
}

func (t timedStore) ListDead(ctx context.Context, queue string, afterID int64, limit int) ([]job.Job, error) {
	defer t.done("list_dead", time.Now())
	return t.Store.ListDead(ctx, queue, afterID, limit)
}

func (t timedStore) Stats(ctx context.Context, queue string) ([]store.TenantStats, error) {
	defer t.done("stats", time.Now())
	return t.Store.Stats(ctx, queue)
}

func (t timedStore) Activity(ctx context.Context, queue string) (store.Activity, error) {
	defer t.done("activity", time.Now())
	return t.Store.Activity(ctx, queue)
}
