// Command chaosworker is the worker process of the chaos test. It runs the
// public SDK's worker against a set of brokers, and its handler writes each
// delivery to a sink table that only accepts a fencing token higher than the
// one it holds, so that a handler running under a lost lease is refused. It
// records what it did to a history file, which the test checks afterwards.
//
// It is a test fixture, not a product: it is built and started by the chaos
// test (test/chaos) and by nothing else.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/history"
)

// upsertSQL is the fenced write: the row of a job changes only for a higher
// token, so a write under a token that is not the newest changes nothing. Its
// command tag says which: one row for an accepted write, none for a refused
// one.
const upsertSQL = `
INSERT INTO chaos_sink(job_id, token, worker) VALUES ($1, $2, $3)
ON CONFLICT (job_id) DO UPDATE SET token = EXCLUDED.token, worker = EXCLUDED.worker
WHERE chaos_sink.token < EXCLUDED.token`

// The pause of every handler, in milliseconds, and the chance that a handler
// also sleeps through its lease.
const (
	minPauseMillis = 5
	maxPauseMillis = 50
	staleChance    = 0.05
)

// sinkTimeout bounds a write to the sink. The write does not use the handler's
// context: a handler whose lease was lost still writes, which is the case the
// sink's fence exists for.
const sinkTimeout = 10 * time.Second

type config struct {
	addr, queue, id string
	concurrency     int
	ttl, margin     time.Duration
	historyPath     string
	sinkDSN         string
	seed            uint64
}

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

// run is the command apart from the process: it returns the exit status.
func run(args []string, stderr io.Writer) int {
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		_, _ = fmt.Fprintf(stderr, "chaosworker: %v\n", err)
		return 2
	}
	log := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := serve(cfg, log); err != nil {
		log.Error("chaosworker failed", slog.Any("err", err))
		return 1
	}
	return 0
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("chaosworker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.addr, "addr", "", "comma-separated host:port of every broker, for example 127.0.0.1:7001,127.0.0.1:7002")
	fs.StringVar(&c.queue, "queue", "chaos", "queue to take jobs from")
	fs.StringVar(&c.id, "id", "", "worker id; defaults to chaos-<pid>")
	fs.IntVar(&c.concurrency, "concurrency", 4, "handlers that may run at once")
	fs.DurationVar(&c.ttl, "ttl", 5*time.Second, "lease TTL")
	fs.DurationVar(&c.margin, "margin", time.Second, "worker margin before a lease expires")
	fs.StringVar(&c.historyPath, "history", "", "file that the worker appends its history events to")
	fs.StringVar(&c.sinkDSN, "sink-dsn", "", "PostgreSQL connection URL of the database that holds chaos_sink")
	fs.Uint64Var(&c.seed, "seed", 1, "seed of the handler's random pauses")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	for _, req := range []struct{ name, value string }{
		{"addr", c.addr}, {"history", c.historyPath}, {"sink-dsn", c.sinkDSN},
	} {
		if req.value == "" {
			return config{}, fmt.Errorf("--%s is required", req.name)
		}
	}
	if c.id == "" {
		c.id = fmt.Sprintf("chaos-%d", os.Getpid())
	}
	return c, nil
}

// serve runs the worker until a SIGTERM or an interrupt, and returns when it
// has drained.
func serve(cfg config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	hist, err := os.OpenFile(cfg.historyPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open the history file: %w", err)
	}
	defer func() { _ = hist.Close() }()

	poolCfg, err := pgxpool.ParseConfig(cfg.sinkDSN)
	if err != nil {
		return fmt.Errorf("parse --sink-dsn: %w", err)
	}
	poolCfg.MaxConns = int32(cfg.concurrency) + 2
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg) // connects on first use
	if err != nil {
		return fmt.Errorf("open the sink pool: %w", err)
	}
	defer pool.Close()

	// A static resolver over every broker: Dial spreads the calls over its
	// addresses round-robin and keeps going when some of them are down.
	brokers := manual.NewBuilderWithScheme("chaos")
	var addrs []resolver.Address
	for _, a := range strings.Split(cfg.addr, ",") {
		addrs = append(addrs, resolver.Address{Addr: strings.TrimSpace(a)})
	}
	brokers.InitialState(resolver.State{Addresses: addrs})
	conn, err := jobqueue.Dial("chaos:///brokers", jobqueue.WithInsecure(), jobqueue.WithGRPCOptions(grpc.WithResolvers(brokers)))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	h := &handler{
		log:  log,
		id:   cfg.id,
		ttl:  cfg.ttl,
		pool: pool,
		rec:  history.NewRecorder(hist),
		rng:  rand.New(rand.NewPCG(cfg.seed, cfg.seed^0x9e3779b97f4a7c15)),
	}
	w := jobqueue.NewWorker(conn, jobqueue.WorkerOptions{
		Queue:       cfg.queue,
		Concurrency: cfg.concurrency,
		LeaseTTL:    cfg.ttl,
		Margin:      cfg.margin,
		WorkerID:    cfg.id,
		Logger:      log,
	})
	if err := w.Handle(h.handle); err != nil {
		return err
	}
	return w.Run(ctx)
}

// handler is the worker's job handler and what it shares between handlers.
type handler struct {
	log  *slog.Logger
	id   string
	ttl  time.Duration
	pool *pgxpool.Pool
	rec  *history.Recorder

	// rng is not safe for concurrent use, and handlers run concurrently.
	mu  sync.Mutex
	rng *rand.Rand
}

// pauses draws how long a handler sleeps, and whether it then also sleeps
// through its lease.
func (h *handler) pauses() (pause time.Duration, stale bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	span := int64(maxPauseMillis - minPauseMillis + 1)
	pause = time.Duration(minPauseMillis+h.rng.Int64N(span)) * time.Millisecond
	return pause, h.rng.Float64() < staleChance
}

// record appends one event. The history is the evidence of the run, so a
// worker that cannot write it stops: a gap in it could hide or fake a
// violation.
func (h *handler) record(e history.Event) {
	if err := h.rec.Record(e); err != nil {
		h.log.Error("cannot write the history, exiting", slog.Any("err", err))
		os.Exit(3)
	}
}

// handle runs one delivery of a job.
func (h *handler) handle(_ context.Context, j *jobqueue.Job) error {
	base := history.Event{ID: j.ID, Token: j.Token(), Worker: h.id}
	start := base
	start.At, start.Kind = time.Now(), history.HandlerStart
	h.record(start)

	pause, stale := h.pauses()
	time.Sleep(pause)
	if stale {
		// The handler ignores its context and outlives its lease, so a later
		// delivery of the job runs while this one is still going.
		time.Sleep(2 * h.ttl)
	}

	err := h.write(base)
	end := base
	end.Kind, end.Result = history.HandlerEnd, "ok"
	if err != nil {
		end.Result = "error"
	}
	end.At = time.Now()
	h.record(end)
	return err
}

// write makes the fenced write for the delivery and records it. A write whose
// outcome is unknown, because the database failed, is not recorded: the event
// would be a guess. A refused write is recorded and is an error, so that the
// job is tried again.
func (h *handler) write(base history.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), sinkTimeout)
	defer cancel()
	tag, err := h.pool.Exec(ctx, upsertSQL, base.ID, base.Token, h.id)
	at := time.Now() // as soon as the write returns: see history.Event.At
	if err != nil {
		return fmt.Errorf("sink write for job %d token %d: %w", base.ID, base.Token, err)
	}
	applied := tag.RowsAffected() == 1
	ev := base
	ev.At, ev.Kind, ev.Applied = at, history.SinkWrite, applied
	h.record(ev)
	if !applied {
		return fmt.Errorf("sink refused job %d token %d: it holds a newer token", base.ID, base.Token)
	}
	return nil
}
