package exp

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/bench/internal/load"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// The defaults of E3. The flood and B's rate are the experiment's definition.
// The capacity of the workers, 8 slots of 2ms or 4000 jobs a second, is far
// below the rate at which the flood is enqueued, so that the flood leaves a
// backlog that takes 25 seconds to drain.
const (
	DefaultFlood              = 100000
	DefaultBRate              = 10.0
	DefaultServiceTime        = 2 * time.Millisecond
	DefaultNoisyWorkers       = 2
	DefaultNoisyConcurrency   = 4
	floodEnqueuers            = 4
	tenantA, tenantB          = "a", "b"
	awaitSlack                = time.Minute
	awaitDrainFactor          = 3
	minServiceForAwaitSeconds = 1e-6
)

// NoisyOptions configures E3.
type NoisyOptions struct {
	Setup
	// Scheduler is "drr" or "fifo".
	Scheduler string
	// Flood is the number of jobs tenant A enqueues as fast as it can, and BRate
	// the jobs per second that tenant B sends, for Duration.
	Flood    int
	BRate    float64
	Duration time.Duration
	// ServiceTime is how long each handler takes, so that the workers have a
	// capacity the flood can exceed. Workers and Concurrency say how many slots
	// there are.
	ServiceTime          time.Duration
	Workers, Concurrency int
	// PayloadBytes is the size of every payload, at least StampBytes.
	PayloadBytes int
}

func (o NoisyOptions) withDefaults() NoisyOptions {
	if o.Flood == 0 {
		o.Flood = DefaultFlood
	}
	if o.BRate == 0 {
		o.BRate = DefaultBRate
	}
	if o.Duration == 0 {
		o.Duration = DefaultDuration
	}
	if o.ServiceTime == 0 {
		o.ServiceTime = DefaultServiceTime
	}
	if o.Workers == 0 {
		o.Workers = DefaultNoisyWorkers
	}
	if o.Concurrency == 0 {
		o.Concurrency = DefaultNoisyConcurrency
	}
	if o.PayloadBytes == 0 {
		o.PayloadBytes = DefaultPayloadBytes
	}
	return o
}

func (o NoisyOptions) validate() error {
	if err := o.Setup.validate(); err != nil {
		return err
	}
	switch {
	case o.Scheduler != "drr" && o.Scheduler != "fifo":
		return fmt.Errorf("--scheduler must be drr or fifo, got %q", o.Scheduler)
	case o.Flood < 1:
		return fmt.Errorf("--flood must be at least 1, got %d", o.Flood)
	case !(o.BRate > 0):
		return fmt.Errorf("--b-rate must be above zero, got %v", o.BRate)
	case o.Duration <= 0:
		return fmt.Errorf("--duration must be positive, got %v", o.Duration)
	case o.ServiceTime < 0:
		return fmt.Errorf("--service-time must not be negative, got %v", o.ServiceTime)
	case o.Workers < 1:
		return fmt.Errorf("--workers must be at least 1, got %d", o.Workers)
	case o.Concurrency < 1 || o.Concurrency > maxConcurrency:
		return fmt.Errorf("--concurrency must be between 1 and %d, got %d", maxConcurrency, o.Concurrency)
	case o.PayloadBytes < StampBytes || o.PayloadBytes > maxPayload:
		return fmt.Errorf("--payload-bytes must be between %d and %d, got %d", StampBytes, maxPayload, o.PayloadBytes)
	}
	return nil
}

func (o NoisyOptions) params() map[string]string {
	return map[string]string{
		"scheduler":     o.Scheduler,
		"brokers":       strconv.Itoa(o.brokers()),
		"flood":         strconv.Itoa(o.Flood),
		"b_rate":        formatRate(o.BRate),
		"duration":      o.Duration.String(),
		"service_time":  o.ServiceTime.String(),
		"workers":       strconv.Itoa(o.Workers),
		"concurrency":   strconv.Itoa(o.Concurrency),
		"payload_bytes": strconv.Itoa(o.PayloadBytes),
	}
}

// Noisy is E3, the noisy neighbour: tenant A enqueues a flood of jobs as fast as
// it can while tenant B sends a few per second, both into one queue, and the
// result is how long B's jobs wait before they start under the scheduler.
//
// Under "fifo" B's job waits behind whatever A has already queued. Under "drr"
// the broker shares each lease between the tenants that have work, so B's job
// does not wait for A's backlog. The result's quantiles and its CDF are B's
// wait, measured from the time each job was meant to be sent.
func Noisy(ctx context.Context, o NoisyOptions, emit Emit) error {
	o = o.withDefaults()
	if err := o.validate(); err != nil {
		return err
	}
	r, err := openRig(ctx, o.Setup, ClusterOptions{Scheduler: o.Scheduler})
	if err != nil {
		return err
	}
	res, err := r.noisy(ctx, o)
	if cerr := r.close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	emit(res)
	return nil
}

func (r *rig) noisy(ctx context.Context, o NoisyOptions) (Result, error) {
	queue := r.nextQueue()
	defer r.cleanup(queue)

	m := NewMeter(tenantA, tenantB)
	spec := workerSpec{Count: o.Workers, Concurrency: o.Concurrency}
	stop, err := startWorkers(ctx, r.cluster, queue, spec, meterHandler(m, o.ServiceTime))
	if err != nil {
		return Result{}, err
	}
	stopped := false
	stopWorkers := func() error {
		stopped = true
		return stop()
	}
	defer func() {
		if !stopped {
			_ = stopWorkers()
		}
	}()

	connA, err := r.cluster.Dial()
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = connA.Close() }()
	connB, err := r.cluster.Dial()
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = connB.Close() }()
	producerA := jobqueue.NewProducer(connA, jobqueue.ProducerOptions{PerCallTimeout: 60 * time.Second})
	producerB := jobqueue.NewProducer(connB, jobqueue.ProducerOptions{PerCallTimeout: 60 * time.Second})

	began := time.Now()
	start := began.Add(startDelay)
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var (
		wg           sync.WaitGroup
		bSent, bLate int
		aEnqueuedAt  atomic.Int64 // nanoseconds since start, when the last flood job was in
		remaining    atomic.Int64
		failOnce     sync.Once
		failure      error
		fail         = func(err error) { failOnce.Do(func() { failure = err; cancel(err) }) }
		floodSent    atomic.Int64
		nB           = max(1, int(math.Round(o.BRate*o.Duration.Seconds())))
	)
	remaining.Store(int64(o.Flood))

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		bSent, bLate, err = load.Run(runCtx, load.Schedule{Start: start, Rate: o.BRate}, nB,
			func(ctx context.Context, _ int, intended time.Time) error {
				_, err := producerB.Enqueue(ctx, jobqueue.EnqueueParams{Queue: queue, Tenant: tenantB, Payload: m.Payload(intended, o.PayloadBytes)})
				return err
			})
		if err != nil {
			fail(fmt.Errorf("tenant B: %w", err))
		}
	}()
	for range floodEnqueuers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if wait := time.Until(start); wait > 0 {
				select {
				case <-time.After(wait):
				case <-runCtx.Done():
					return
				}
			}
			for runCtx.Err() == nil {
				// Claim up to a batch of the jobs still to be enqueued: Add returns
				// what is left after this claim, so before it there were that
				// plus a batch. Once nothing is left the claim is not above zero.
				n := int(min(remaining.Add(-job.MaxBatch)+job.MaxBatch, job.MaxBatch))
				if n <= 0 {
					return
				}
				batch := make([]jobqueue.EnqueueParams, n)
				for i := range batch {
					batch[i] = jobqueue.EnqueueParams{Queue: queue, Tenant: tenantA, Payload: m.Payload(time.Now(), o.PayloadBytes)}
				}
				if _, err := producerA.EnqueueBatch(runCtx, batch); err != nil {
					fail(fmt.Errorf("tenant A: %w", err))
					return
				}
				if floodSent.Add(int64(n)) >= int64(o.Flood) {
					aEnqueuedAt.CompareAndSwap(0, int64(time.Since(start)))
				}
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if failure != nil {
		return Result{}, failure
	}

	total := int64(o.Flood + bSent)
	capacity := float64(o.Workers*o.Concurrency) / max(o.ServiceTime.Seconds(), minServiceForAwaitSeconds)
	wait := time.Duration((awaitDrainFactor*float64(o.Flood)/capacity)*float64(time.Second)) + awaitSlack
	if !m.Await(ctx, total, wait) {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		return Result{}, fmt.Errorf("only %d of %d jobs started within %v of the end of the load", m.StartedTotal(), total, wait)
	}
	if err := stopWorkers(); err != nil {
		return Result{}, err
	}

	b := m.Hist(tenantB)
	metrics := map[string]float64{
		"a_jobs":            float64(o.Flood),
		"b_jobs":            float64(bSent),
		"b_late_sends":      float64(bLate),
		"a_enqueue_seconds": time.Duration(aEnqueuedAt.Load()).Seconds(),
		"capacity_per_s":    capacity,
		"duplicate_starts":  float64(m.DuplicatesTotal()),
	}
	if last := m.LastStartOf(tenantA); !last.IsZero() {
		metrics["a_drain_seconds"] = last.Sub(start).Seconds()
	}
	r.logf("noisy %s: B waited p50 %v, p99 %v, max %v over %d jobs; A drained in %.1fs",
		o.Scheduler, b.Quantiles()["p50"], b.Quantiles()["p99"], b.Quantiles()["max"], bSent, metrics["a_drain_seconds"])
	return Result{
		Experiment: "noisy", Params: o.params(), Metrics: metrics, Quantiles: b.Quantiles(), CDF: b.CDF(),
		Env: r.env, Started: began, Duration: time.Since(began),
	}, nil
}
