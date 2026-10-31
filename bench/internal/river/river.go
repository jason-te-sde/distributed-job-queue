// Package river is E6: the throughput of River on the same PostgreSQL, as a
// reference for the broker's numbers.
//
// River is a library. It inserts and works jobs in this process, over the
// database, with no network hop and no broker between the producer and the
// worker, so its numbers are not those of a like-for-like system: they say what
// the same database does when a library drives it directly. Every result is
// labelled so.
package river

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/exp"
)

const (
	// schema holds River's tables for the run. River needs tables of its own,
	// and a schema of its own keeps them apart from the broker's and lets the
	// run remove everything it made.
	schema = "jobqueue_bench_river"
	// tenant is the name the latency meter files the jobs under.
	tenant = "bench"
	// DefaultConcurrency is River's MaxWorkers for the queue: as many handler
	// slots as E1's default workers have in all.
	DefaultConcurrency = exp.DefaultWorkers * exp.DefaultConcurrency
	// DefaultDBConns is the size of River's database pool, the broker's default.
	DefaultDBConns = 20
	// DefaultFetchCooldown is the shortest wait between River's fetches. River's
	// own default is 100ms and it documents that this limits throughput, so the
	// benchmark uses River's minimum, and states it in every result, instead of
	// handing River a ceiling that the broker does not have.
	DefaultFetchCooldown = time.Millisecond
	// fetchPollInterval is River's own default fallback poll.
	fetchPollInterval = time.Second
	startDelay        = 500 * time.Millisecond
	stopTimeout       = time.Minute
	cleanupTimeout    = 10 * time.Minute
	mode              = "library: River inserts and works jobs in this process over the database, with no network hop; not a like-for-like comparison with the broker"
)

// Options configures E6. The load is as in E1: one fixed Rate, or FindMax to
// search for the highest sustained rate and measure at half and four fifths of
// it.
type Options struct {
	// DatabaseURL is the PostgreSQL database.
	DatabaseURL string
	// Rate, FindMax, Lo, Hi, Steps and Duration are as in exp.SweepConfig.
	Rate     float64
	FindMax  bool
	Lo, Hi   float64
	Steps    int
	Duration time.Duration
	// PayloadBytes is the size of each job's arguments, at least 8.
	PayloadBytes int
	// Concurrency is River's MaxWorkers for the queue, and DBConns the size of
	// its database pool.
	Concurrency int
	DBConns     int32
	// FetchCooldown is River's FetchCooldown.
	FetchCooldown time.Duration
	// Log receives a line of progress per probe. It may be nil.
	Log func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	if o.Duration == 0 {
		o.Duration = exp.DefaultDuration
	}
	if o.PayloadBytes == 0 {
		o.PayloadBytes = exp.DefaultPayloadBytes
	}
	if o.Concurrency == 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.DBConns == 0 {
		o.DBConns = DefaultDBConns
	}
	if o.FetchCooldown == 0 {
		o.FetchCooldown = DefaultFetchCooldown
	}
	if o.FindMax {
		if o.Lo == 0 {
			o.Lo = exp.DefaultLo
		}
		if o.Hi == 0 {
			o.Hi = exp.DefaultHi
		}
		if o.Steps == 0 {
			o.Steps = exp.DefaultSteps
		}
	}
	return o
}

func (o Options) sweep(env, params map[string]string) exp.SweepConfig {
	return exp.SweepConfig{
		Experiment: "river", Params: params, Env: env,
		FindMax: o.FindMax, Rate: o.Rate, Lo: o.Lo, Hi: o.Hi, Steps: o.Steps, Duration: o.Duration, Log: o.Log,
	}
}

func (o Options) validate() error {
	switch {
	case o.DatabaseURL == "":
		return exp.ErrNoDatabase
	case o.PayloadBytes < 8:
		return fmt.Errorf("--payload-bytes must be at least 8, got %d", o.PayloadBytes)
	case o.Concurrency < 1:
		return fmt.Errorf("--concurrency must be at least 1, got %d", o.Concurrency)
	case o.DBConns < 1:
		return fmt.Errorf("--db-conns must be at least 1, got %d", o.DBConns)
	case o.FetchCooldown < river.FetchCooldownMin || o.FetchCooldown > fetchPollInterval:
		return fmt.Errorf("--fetch-cooldown must be between %v and %v, got %v", river.FetchCooldownMin, fetchPollInterval, o.FetchCooldown)
	}
	return o.sweep(nil, nil).Validate()
}

func (o Options) params() map[string]string {
	p := map[string]string{
		"mode":           mode,
		"duration":       o.Duration.String(),
		"payload_bytes":  strconv.Itoa(o.PayloadBytes),
		"concurrency":    strconv.Itoa(o.Concurrency),
		"db_conns":       strconv.FormatInt(int64(o.DBConns), 10),
		"fetch_cooldown": o.FetchCooldown.String(),
		"find_max":       strconv.FormatBool(o.FindMax),
		"river_version":  riverVersion(),
	}
	if o.FindMax {
		p["lo"], p["hi"], p["steps"] = strconv.FormatFloat(o.Lo, 'f', -1, 64), strconv.FormatFloat(o.Hi, 'f', -1, 64), strconv.Itoa(o.Steps)
	}
	return p
}

// riverVersion is the version of River this binary was built with, from the
// module information the toolchain embeds.
func riverVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/riverqueue/river" {
				return dep.Version
			}
		}
	}
	return "unknown"
}

// benchArgs are the arguments of a job: the stamp of its intended time, and
// padding to the size asked for.
type benchArgs struct {
	Stamp int64  `json:"stamp"`
	Pad   string `json:"pad,omitempty"`
}

// Kind is the name River files the job under.
func (benchArgs) Kind() string { return "bench" }

// benchWorker works a job by recording that it started, as the empty handler of
// E1 does.
type benchWorker struct {
	river.WorkerDefaults[benchArgs]
	m *exp.Meter
}

// Work records the start of the job.
func (w *benchWorker) Work(_ context.Context, job *river.Job[benchArgs]) error {
	return w.m.Observe(tenant, job.Args.Stamp)
}

// Run is E6. It creates a schema of its own for River's tables, migrates it,
// probes River with the same open-loop load and the same sustained-rate bar as
// E1, and drops the schema.
func Run(ctx context.Context, o Options, emit exp.Emit) error {
	o = o.withDefaults()
	if err := o.validate(); err != nil {
		return err
	}
	cfg, err := pgxpool.ParseConfig(o.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse the database URL: %w", err)
	}
	cfg.MaxConns = o.DBConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open the database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("reach the database: %w", err)
	}

	// The schema is the run's own, so a leftover of an aborted run is dropped.
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		return fmt.Errorf("drop the schema of an earlier run: %w", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		return fmt.Errorf("create the schema: %w", err)
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := pool.Exec(dropCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil && o.Log != nil {
			o.Log("drop the schema %s: %v", schema, err)
		}
	}()
	driver := riverpgxv5.New(pool)
	migrator, err := rivermigrate.New(driver, &rivermigrate.Config{Schema: schema})
	if err != nil {
		return fmt.Errorf("river migrator: %w", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("migrate river's schema: %w", err)
	}

	r := &runner{o: o, pool: pool, driver: driver}
	return exp.Sweep(ctx, o.sweep(exp.CaptureEnv(ctx, pool), o.params()), r.probe, emit)
}

type runner struct {
	o      Options
	pool   *pgxpool.Pool
	driver *riverpgxv5.Driver
	seq    int
}

// probe is an exp.ProbeFunc: it starts a River client on a queue of its own,
// offers the load by inserting one job per send, and stops the client.
func (r *runner) probe(ctx context.Context, rate float64, duration, drainWait time.Duration) (*exp.Outcome, error) {
	r.seq++
	queue := "bench_" + strconv.Itoa(r.seq)
	defer func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := r.pool.Exec(cleanCtx, "DELETE FROM "+schema+".river_job WHERE queue = $1", queue); err != nil && r.o.Log != nil {
			r.o.Log("clean up queue %s: %v", queue, err)
		}
	}()

	m := exp.NewMeter(tenant)
	workers := river.NewWorkers()
	river.AddWorker(workers, &benchWorker{m: m})
	client, err := river.NewClient(r.driver, &river.Config{
		Queues:            map[string]river.QueueConfig{queue: {MaxWorkers: r.o.Concurrency}},
		Workers:           workers,
		Schema:            schema,
		FetchCooldown:     r.o.FetchCooldown,
		FetchPollInterval: fetchPollInterval,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return nil, fmt.Errorf("create the river client: %w", err)
	}
	runCtx := context.WithoutCancel(ctx)
	if err := client.Start(runCtx); err != nil {
		return nil, fmt.Errorf("start the river client: %w", err)
	}
	pad := strings.Repeat("x", max(0, r.o.PayloadBytes-8))
	out, err := exp.Probe(ctx, exp.ProbeConfig{Rate: rate, Duration: duration, DrainWait: drainWait, StartDelay: startDelay}, m, tenant,
		func(ctx context.Context, _ int, intended time.Time) error {
			_, err := client.Insert(ctx, benchArgs{Stamp: m.Stamp(intended), Pad: pad}, &river.InsertOpts{Queue: queue})
			return err
		})
	stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if serr := client.Stop(stopCtx); serr != nil && err == nil {
		err = fmt.Errorf("stop the river client: %w", serr)
	}
	return out, err
}
