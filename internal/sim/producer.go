package sim

import (
	"fmt"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

const (
	// producerTimeout is how long a producer waits for a broker's answer
	// before it sends the request again.
	producerTimeout = 5 * time.Second
	// retryBackoffMin and retryBackoffMax bound the wait before a resend: it
	// starts at the minimum and doubles up to the maximum.
	retryBackoffMin = 50 * time.Millisecond
	retryBackoffMax = 2 * time.Second
	// delayMin and delayMax bound the delay of a job that is delayed.
	delayMin = 100 * time.Millisecond
	delayMax = 5 * time.Second
	// cancelAfterMax bounds how long after a confirmation a producer cancels.
	cancelAfterMax = 2 * time.Second
)

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	// ID names the node on the network.
	ID NodeID
	// Brokers are the brokers it sends to, picked at random for each try.
	Brokers []NodeID
	// Jobs is how many jobs to enqueue, and Over is the span of virtual
	// time, from the start, in which their first sends are spread.
	Jobs int
	Over time.Duration
	// Tenants and Queues are what each job's tenant and queue are drawn
	// from.
	Tenants, Queues []string
	// DelayP is the chance that a job is delayed by 100ms to 5s.
	DelayP float64
	// MaxAttempts are the lease budgets a job's is drawn from. None means
	// the default budget.
	MaxAttempts []int32
	// CancelP is the chance that a job is cancelled after it is confirmed.
	CancelP float64
}

// pjob is one job a producer enqueues.
type pjob struct {
	spec      job.Spec
	cancel    bool // to be cancelled once confirmed
	confirmed bool
	id        int64
}

// Producer is a simulated client that enqueues jobs, each with its own
// idempotency key, and resends a request, with backoff, until a broker
// confirms it. It may then cancel the job, and resends that request until a
// broker answers.
type Producer struct {
	w         *World
	net       *Network
	cfg       ProducerConfig
	rng       *Rand
	onConfirm func(key string, id int64)
	onCancel  func(id int64)
	confirmed int
}

// NewProducer starts a producer: it spreads Jobs first sends over Over from
// now. onConfirm is called for every confirmation of a key, so a retry that
// is answered with another id shows up; onCancel is called once for each job
// that the store cancelled. Either may be nil. It panics if the config could
// not produce a job.
func NewProducer(w *World, net *Network, cfg ProducerConfig, onConfirm func(key string, id int64), onCancel func(id int64)) *Producer {
	if len(cfg.Brokers) == 0 || len(cfg.Tenants) == 0 || len(cfg.Queues) == 0 || cfg.Jobs < 0 || cfg.Over < 0 {
		panic("sim: NewProducer called with no Brokers, Tenants or Queues, or a negative Jobs or Over")
	}
	p := &Producer{w: w, net: net, cfg: cfg, rng: w.Rand.Fork(), onConfirm: onConfirm, onCancel: onCancel}
	for i := range cfg.Jobs {
		pj := &pjob{spec: p.spec(i)}
		pj.cancel = p.rng.Chance(cfg.CancelP)
		w.After(p.rng.Between(0, cfg.Over), "producer enqueue", func() { p.enqueue(pj, 0) })
	}
	return p
}

// Confirmed returns how many distinct jobs a broker has confirmed.
func (p *Producer) Confirmed() int {
	return p.confirmed
}

// spec draws the i-th job's spec. The key is unique to the producer and the
// job.
func (p *Producer) spec(i int) job.Spec {
	s := job.Spec{
		Queue:          p.cfg.Queues[p.rng.IntN(len(p.cfg.Queues))],
		Tenant:         p.cfg.Tenants[p.rng.IntN(len(p.cfg.Tenants))],
		Payload:        []byte(fmt.Sprintf("%s/%d", p.cfg.ID, i)),
		IdempotencyKey: fmt.Sprintf("%s/%d", p.cfg.ID, i),
	}
	if p.rng.Chance(p.cfg.DelayP) {
		s.Delay = p.rng.Between(delayMin, delayMax)
	}
	if len(p.cfg.MaxAttempts) > 0 {
		s.MaxAttempts = p.cfg.MaxAttempts[p.rng.IntN(len(p.cfg.MaxAttempts))]
	}
	return s
}

// backoff is the wait before resend number try, counting from 0.
func backoff(try int) time.Duration {
	d := retryBackoffMin
	for i := 0; i < try && d < retryBackoffMax; i++ {
		d *= 2
	}
	return min(d, retryBackoffMax)
}

// enqueue sends the job's Enqueue to a broker. A lost request or reply is
// noticed by the timeout and an error reply at once; either way the same key
// is sent again.
func (p *Producer) enqueue(pj *pjob, try int) {
	if pj.confirmed {
		return
	}
	answered := false
	retry := func() {
		if pj.confirmed || answered {
			return
		}
		answered = true
		p.w.After(backoff(try), "producer retry", func() { p.enqueue(pj, try+1) })
	}
	to := p.cfg.Brokers[p.rng.IntN(len(p.cfg.Brokers))]
	req := enqueueReq{from: p.cfg.ID, spec: pj.spec, reply: func(r enqueueResp) {
		if r.err != nil {
			retry()
			return
		}
		answered = true
		p.confirm(pj, r.id)
	}}
	p.net.Send(p.cfg.ID, to, true, "enqueue-req", func() { p.net.broker(to).handleEnqueue(req) })
	p.w.After(producerTimeout, "enqueue-timeout", retry)
}

// confirm records an answer to an Enqueue. The first one may start a cancel.
func (p *Producer) confirm(pj *pjob, id int64) {
	if p.onConfirm != nil {
		p.onConfirm(pj.spec.IdempotencyKey, id)
	}
	if pj.confirmed {
		return
	}
	pj.confirmed, pj.id = true, id
	p.confirmed++
	if pj.cancel {
		p.w.After(p.rng.Between(0, cancelAfterMax), "producer cancel", func() { p.cancelJob(pj, 0) })
	}
}

// cancelJob asks a broker to cancel the job, again after a lost request or
// reply. The cancel counts once the store reports the job cancelled, or
// already cancelled; a job that finished first cannot be, and a store that
// no longer has it has nothing to cancel.
func (p *Producer) cancelJob(pj *pjob, try int) {
	answered := false
	retry := func() {
		if answered {
			return
		}
		answered = true
		p.w.After(backoff(try), "producer retry cancel", func() { p.cancelJob(pj, try+1) })
	}
	to := p.cfg.Brokers[p.rng.IntN(len(p.cfg.Brokers))]
	req := cancelJobReq{from: p.cfg.ID, id: pj.id, tenant: pj.spec.Tenant, reply: func(r cancelJobResp) {
		if r.err != nil {
			retry()
			return
		}
		if answered {
			return
		}
		answered = true
		if (r.outcome == job.Applied || r.outcome == job.Duplicate) && p.onCancel != nil {
			p.onCancel(pj.id)
		}
	}}
	p.net.Send(p.cfg.ID, to, true, "cancel-req", func() { p.net.broker(to).handleCancelJob(req) })
	p.w.After(producerTimeout, "cancel-timeout", retry)
}
