package broker

import (
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

// Metrics holds the collectors of the broker. NewMetrics is its only
// constructor, so that every collector is registered exactly once. The
// handlers of Server use the counters below; the maintenance loops, the
// dispatcher and the health endpoint add theirs here.
type Metrics struct {
	// enqueued and deduplicated count the jobs that an Enqueue created, and
	// the items that named a job an idempotency key already held, per queue.
	enqueued, deduplicated *prometheus.CounterVec
	// acks counts what became of the leases that workers reported on, by
	// operation (complete, fail, release, heartbeat) and result (accepted,
	// duplicate, fenced). A heartbeat that held its lease counts as accepted
	// and one that lost it as fenced.
	acks *prometheus.CounterVec
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
	}
}
