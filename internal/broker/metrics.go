package broker

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
// A collector with a queue label is reached only through a method that takes
// the queue name, such as enqueuedFor, and that method applies queueLabel.
type Metrics struct {
	// enqueued and deduplicated count the jobs that an Enqueue created, and
	// the items that named a job an idempotency key already held, per queue.
	enqueued, deduplicated *prometheus.CounterVec
	// acks counts what became of the leases that workers reported on, by
	// operation (complete, fail, release, heartbeat) and result (accepted,
	// duplicate, fenced). A heartbeat that held its lease counts as accepted
	// and one that lost it as fenced.
	acks *prometheus.CounterVec
	// leased counts the jobs the store leased to the dispatcher, per queue.
	leased *prometheus.CounterVec
	// fetches counts the dispatcher's store fetches, by queue and result
	// (jobs, empty, error).
	fetches *prometheus.CounterVec
	// waiters is the number of long-polling Lease calls in the dispatcher.
	waiters *prometheus.GaugeVec
	// leaseWait is how long a Lease call waited before it was given jobs.
	leaseWait prometheus.Histogram
	// orphanReleases counts jobs the dispatcher gave back because the caller
	// they were fetched for had gone.
	orphanReleases prometheus.Counter
	// notifyReconnects counts the times the listener lost its connection,
	// and notifyMalformed the notifications it could not parse.
	notifyReconnects, notifyMalformed prometheus.Counter

	// labeled holds the queue names that have a label value of their own.
	labelMu sync.Mutex
	labeled map[string]struct{}
}

// NewMetrics creates the collectors and registers them with reg. It panics,
// as prometheus does, if one of them is already registered there.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		enqueued: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_enqueued_total",
			Help: "Jobs created by Enqueue, by queue.",
		}, []string{"queue"}),
		deduplicated: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_deduplicated_total",
			Help: "Enqueue items whose idempotency key already named a job, by queue.",
		}, []string{"queue"}),
		acks: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "acks_total",
			Help: "Reports from workers on their leases, by operation and result.",
		}, []string{"op", "result"}),
		leased: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "jobs_leased_total",
			Help: "Jobs the store leased to the dispatcher, by queue.",
		}, []string{"queue"}),
		fetches: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue", Name: "dispatch_fetches_total",
			Help: "Store fetches of the dispatcher, by queue and result (jobs, empty, error).",
		}, []string{"queue", "result"}),
		waiters: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "jobqueue", Name: "waiters",
			Help: "Long-polling Lease calls waiting in the dispatcher, by queue.",
		}, []string{"queue"}),
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
		labeled: map[string]struct{}{},
	}
}

// queueLabel returns the value of the queue label for a queue name: the name
// itself for each of the first maxQueueLabels distinct names the broker sees,
// and "other" for every later one. A name keeps its label for the life of the
// process.
func (m *Metrics) queueLabel(queue string) string {
	m.labelMu.Lock()
	defer m.labelMu.Unlock()
	if _, ok := m.labeled[queue]; ok {
		return queue
	}
	if len(m.labeled) >= maxQueueLabels {
		return otherQueueLabel
	}
	m.labeled[queue] = struct{}{}
	return queue
}

func (m *Metrics) enqueuedFor(queue string) prometheus.Counter {
	return m.enqueued.WithLabelValues(m.queueLabel(queue))
}

func (m *Metrics) deduplicatedFor(queue string) prometheus.Counter {
	return m.deduplicated.WithLabelValues(m.queueLabel(queue))
}

func (m *Metrics) leasedFor(queue string) prometheus.Counter {
	return m.leased.WithLabelValues(m.queueLabel(queue))
}

func (m *Metrics) fetchesFor(queue, result string) prometheus.Counter {
	return m.fetches.WithLabelValues(m.queueLabel(queue), result)
}

func (m *Metrics) waitersFor(queue string) prometheus.Gauge {
	return m.waiters.WithLabelValues(m.queueLabel(queue))
}
