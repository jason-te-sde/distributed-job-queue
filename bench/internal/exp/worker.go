package exp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
)

// WorkerProcessOptions configures the worker process that the recovery
// experiment kills.
type WorkerProcessOptions struct {
	// Addr is the broker's address, Queue the queue to take jobs from and
	// WorkerID the name the worker gives the broker.
	Addr, Queue, WorkerID string
	// Concurrency is how many jobs the worker holds at once.
	Concurrency int
	// TTL and Margin are the worker's lease length and safety margin.
	TTL, Margin time.Duration
}

// RunWorkerProcess runs a worker whose handlers never finish, so that it holds
// Concurrency leases and keeps them alive with heartbeats until the process is
// killed, which is the state E4 kills it in. It returns when ctx ends or when
// stdin reaches its end, which is how a parent that died ends it: a process that
// outlived its parent would hold leases and a connection for ever.
func RunWorkerProcess(ctx context.Context, o WorkerProcessOptions, stdin io.Reader) error {
	if o.Addr == "" || o.Queue == "" {
		return fmt.Errorf("worker: --addr and --queue are required")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if stdin != nil {
		go func() {
			_, _ = io.Copy(io.Discard, stdin)
			cancel()
		}()
	}
	conn, err := jobqueue.Dial(o.Addr, jobqueue.WithInsecure())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	w := jobqueue.NewWorker(conn, jobqueue.WorkerOptions{
		Queue: o.Queue, Concurrency: o.Concurrency, LeaseTTL: o.TTL, Margin: o.Margin, WorkerID: o.WorkerID,
		DrainTimeout: time.Second,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err := w.Handle(func(ctx context.Context, _ *jobqueue.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		return err
	}
	return w.Run(ctx)
}
