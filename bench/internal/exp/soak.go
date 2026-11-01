package exp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// The defaults of E5.
const (
	DefaultSoakDuration  = 30 * time.Minute
	DefaultSoakRetention = 5 * time.Minute
	DefaultSoakInterval  = time.Minute
	// DefaultPruneInterval is the broker's own default.
	DefaultPruneInterval = time.Minute
	// driftWindow is how many samples at each end of the run the summary
	// averages to compare the throughput of the start and of the end.
	driftWindow = 5
)

// SoakOptions configures E5.
type SoakOptions struct {
	Setup
	// Rate is the offered load, which should be about half of the maximum that E1
	// found, and Duration how long the run lasts.
	Rate     float64
	Duration time.Duration
	// Interval is how often the run is sampled.
	Interval time.Duration
	// Retention is how long finished jobs are kept and PruneInterval how often
	// they are deleted. The production retention is 24h; a soak of half an hour
	// shortens it so that the pruner reaches a steady state during the run, and
	// every result states the value.
	Retention     time.Duration
	PruneInterval time.Duration
	// PayloadBytes is the size of each payload, at least StampBytes; Workers and
	// Concurrency are as in E1.
	PayloadBytes         int
	Workers, Concurrency int

	// statsFn replaces the read of the jobs table at each sample, for a test that
	// needs a sample to fail.
	statsFn func(ctx context.Context, r *rig) (tableStats, error)
}

func (o SoakOptions) withDefaults() SoakOptions {
	if o.Duration == 0 {
		o.Duration = DefaultSoakDuration
	}
	if o.Interval == 0 {
		o.Interval = DefaultSoakInterval
	}
	if o.Retention == 0 {
		o.Retention = DefaultSoakRetention
	}
	if o.PruneInterval == 0 {
		o.PruneInterval = DefaultPruneInterval
	}
	if o.PayloadBytes == 0 {
		o.PayloadBytes = DefaultPayloadBytes
	}
	if o.Workers == 0 {
		o.Workers = DefaultWorkers
	}
	if o.Concurrency == 0 {
		o.Concurrency = DefaultConcurrency
	}
	return o
}

func (o SoakOptions) validate() error {
	if err := o.Setup.validate(); err != nil {
		return err
	}
	switch {
	case !(o.Rate > 0):
		return fmt.Errorf("--rate must be above zero: give about half of the maximum rate that throughput --find-max reported, got %v", o.Rate)
	case o.Duration <= 0:
		return fmt.Errorf("--duration must be positive, got %v", o.Duration)
	case o.Interval <= 0:
		return fmt.Errorf("--interval must be positive, got %v", o.Interval)
	case o.Retention <= 0 || o.Retention%time.Microsecond != 0:
		return fmt.Errorf("--retention must be a positive whole number of microseconds, got %v", o.Retention)
	case o.PruneInterval <= 0:
		return fmt.Errorf("--prune-interval must be positive, got %v", o.PruneInterval)
	case o.PayloadBytes < StampBytes || o.PayloadBytes > maxPayload:
		return fmt.Errorf("--payload-bytes must be between %d and %d, got %d", StampBytes, maxPayload, o.PayloadBytes)
	case o.Workers < 1:
		return fmt.Errorf("--workers must be at least 1, got %d", o.Workers)
	case o.Concurrency < 1 || o.Concurrency > maxConcurrency:
		return fmt.Errorf("--concurrency must be between 1 and %d, got %d", maxConcurrency, o.Concurrency)
	}
	return nil
}

func (o SoakOptions) params() map[string]string {
	return map[string]string{
		"brokers":        strconv.Itoa(o.brokers()),
		"rate":           formatRate(o.Rate),
		"duration":       o.Duration.String(),
		"interval":       o.Interval.String(),
		"retention":      o.Retention.String(),
		"prune_interval": o.PruneInterval.String(),
		"payload_bytes":  strconv.Itoa(o.PayloadBytes),
		"workers":        strconv.Itoa(o.Workers),
		"concurrency":    strconv.Itoa(o.Concurrency),
	}
}

// tableStats is what a sample reads of the jobs table.
type tableStats struct {
	total, heap, indexes int64
	rows, dead           int64
}

func (r *rig) tableStats(ctx context.Context) (tableStats, error) {
	var t tableStats
	err := r.cluster.Pool().QueryRow(ctx, `
		SELECT pg_total_relation_size('jobqueue.jobs'), pg_relation_size('jobqueue.jobs'), pg_indexes_size('jobqueue.jobs'),
		       (SELECT count(*) FROM jobqueue.jobs),
		       coalesce((SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid = 'jobqueue.jobs'::regclass), 0)`).
		Scan(&t.total, &t.heap, &t.indexes, &t.rows, &t.dead)
	return t, err
}

// Soak is E5: a steady load for a long time, sampled at intervals, to see
// whether throughput drifts and whether the jobs table keeps its size when the
// pruner is on.
//
// Every Interval it emits a result of phase "sample" with the throughput and
// latency of that interval and pg_total_relation_size of the jobs table, and at
// the end one of phase "summary". Finished jobs are kept for Retention, not the
// production 24h, so that the table reaches its steady size during the run; the
// value is in the params of every result.
//
// A job counts once, at its first start. started_total is a running total of
// those, and duplicate_starts one of the later starts of jobs that had started.
// The short retention makes duplicates possible: a producer call that commits
// after its deadline is retried, and a retry that finds its first job pruned
// makes a second job of the same send.
func Soak(ctx context.Context, o SoakOptions, emit Emit) error {
	o = o.withDefaults()
	if err := o.validate(); err != nil {
		return err
	}
	r, err := openRig(ctx, o.Setup, ClusterOptions{
		Retention: store.Retention{Done: o.Retention, Dead: o.Retention}, PruneInterval: o.PruneInterval,
	})
	if err != nil {
		return err
	}
	err = r.soak(ctx, o, emit)
	if cerr := r.close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

func (r *rig) soak(ctx context.Context, o SoakOptions, emit Emit) error {
	queue := r.nextQueue()
	defer r.cleanup(queue)
	began := time.Now()

	m := NewMeter(benchTenant)
	stop, err := startWorkers(ctx, r.cluster, queue, workerSpec{Count: o.Workers, Concurrency: o.Concurrency}, meterHandler(m, 0))
	if err != nil {
		return err
	}
	conn, err := r.cluster.Dial()
	if err != nil {
		_ = stop()
		return err
	}
	defer func() { _ = conn.Close() }()
	producer := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{PerCallTimeout: 30 * time.Second})

	// The probe runs on a context of its own, so that a failed sample can stop it:
	// otherwise the run would wait out the whole soak, half an hour by default, to
	// report an error that is known at once.
	probeCtx, stopProbe := context.WithCancel(ctx)
	defer stopProbe()
	type probeEnd struct {
		out *Outcome
		err error
	}
	ended := make(chan probeEnd, 1)
	go func() {
		out, err := Probe(probeCtx, ProbeConfig{Rate: o.Rate, Duration: o.Duration, DrainWait: MeasureDrainWait, StartDelay: startDelay}, m, benchTenant,
			func(ctx context.Context, _ int, intended time.Time) error {
				_, err := producer.Enqueue(ctx, jobqueue.EnqueueParams{Queue: queue, Tenant: benchTenant, Payload: m.Payload(intended, o.PayloadBytes)})
				return err
			})
		ended <- probeEnd{out, err}
	}()

	var (
		total      = hist.New()
		throughput []float64
		sizes      []int64
		maxRows    int64
		lastCount  int64
		lastAt     = began
		tick       = time.NewTicker(o.Interval)
	)
	defer tick.Stop()
	var end probeEnd
sampling:
	for n := 1; ; n++ {
		select {
		case end = <-ended:
			break sampling
		case <-ctx.Done():
			end = <-ended
			_ = stop()
			return ctx.Err()
		case <-tick.C:
		}
		now := time.Now()
		started, duplicates := m.Started(benchTenant), m.Duplicates(benchTenant)
		perSecond := float64(started-lastCount) / now.Sub(lastAt).Seconds()
		lastCount, lastAt = started, now

		interval := m.Hist(benchTenant).Take()
		total.Merge(interval)
		stats, err := sampleStats(ctx, r, o)
		if err != nil {
			stopProbe()
			end = <-ended
			_ = stop()
			return fmt.Errorf("sample the jobs table: %w", err)
		}
		throughput = append(throughput, perSecond)
		sizes = append(sizes, stats.total)
		maxRows = max(maxRows, stats.rows)

		params := o.params()
		params["phase"], params["interval_index"] = "sample", strconv.Itoa(n)
		emit(Result{
			Experiment: "soak", Params: params,
			Metrics: map[string]float64{
				"throughput_per_s": perSecond, "started_total": float64(started), "duplicate_starts": float64(duplicates),
				"table_bytes": float64(stats.total), "heap_bytes": float64(stats.heap), "index_bytes": float64(stats.indexes),
				"rows": float64(stats.rows), "dead_tuples": float64(stats.dead),
			},
			Quantiles: interval.Quantiles(), Env: r.env, Started: began, Duration: now.Sub(began),
		})
		r.logf("soak interval %d: %.0f/s, p99 %v, table %.1f MiB, %d rows, %d dead tuples",
			n, perSecond, interval.Quantiles()["p99"], float64(stats.total)/(1<<20), stats.rows, stats.dead)
	}
	if werr := stop(); werr != nil && end.err == nil {
		end.err = werr
	}
	if end.err != nil {
		return end.err
	}
	if end.out.SendErr != nil {
		return fmt.Errorf("the producer failed during the soak: %w", end.out.SendErr)
	}
	total.Merge(m.Hist(benchTenant))
	if len(sizes) == 0 {
		return errors.New("the soak ended before its first sample: lower --interval or raise --duration")
	}

	first := mean(throughput[:min(driftWindow, len(throughput))])
	last := mean(throughput[max(0, len(throughput)-driftWindow):])
	drift := 0.0
	if first > 0 {
		drift = (last - first) / first * 100
	}
	params := o.params()
	params["phase"] = "summary"
	emit(Result{
		Experiment: "soak", Params: params,
		Metrics: map[string]float64{
			"sent": float64(end.out.Sent), "started": float64(end.out.Started), "late_sends": float64(end.out.Late),
			"duplicate_starts":            float64(end.out.Duplicates),
			"samples":                     float64(len(sizes)),
			"throughput_first_mean_per_s": first, "throughput_last_mean_per_s": last, "throughput_drift_pct": drift,
			"table_bytes_first": float64(sizes[0]), "table_bytes_final": float64(sizes[len(sizes)-1]),
			"table_bytes_min": float64(slices.Min(sizes)), "table_bytes_max": float64(slices.Max(sizes)),
			"rows_max": float64(maxRows),
		},
		Quantiles: total.Quantiles(), Env: r.env, Started: began, Duration: time.Since(began),
	})
	return nil
}

// sampleStats reads the jobs table, or calls the replacement a test installed.
func sampleStats(ctx context.Context, r *rig, o SoakOptions) (tableStats, error) {
	if o.statsFn != nil {
		return o.statsFn(ctx, r)
	}
	return r.tableStats(ctx)
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}
