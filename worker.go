package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
)

// maxHostBytes is how much of the host name a generated worker id keeps. The
// rest of the id is a dash, a process id of up to 10 digits, a dash and 8 hex
// digits, and the whole must fit the 128 bytes of a worker id.
const maxHostBytes = 100

// WorkerOptions configures a Worker. Every field but Queue has a default.
type WorkerOptions struct {
	// Queue is the queue to take jobs from.
	Queue string
	// Concurrency is how many handlers may run at once: 1 to 10000. Zero
	// means 1.
	Concurrency int
	// LeaseTTL is how long the broker holds a job for the worker without
	// hearing from it, from 1s to 1h. The worker extends its leases every
	// quarter of it. It must be more than four times Margin, which is what
	// lets two heartbeats in a row be lost without losing a lease. Zero
	// means 30s.
	LeaseTTL time.Duration
	// Margin is how long before a lease would expire the worker stops
	// trusting it and cancels its handler, so that the handler is told to
	// stop before the broker can give the job to someone else. Zero means 3s.
	Margin time.Duration
	// LongPoll is how long a request for work may wait at the broker when
	// there is none, up to 30s. Zero means 20s.
	LongPoll time.Duration
	// DrainTimeout is how long a shutdown waits for running handlers before
	// it cancels them and gives their jobs back. Zero means 25s.
	DrainTimeout time.Duration
	// WorkerID names the worker to the broker, for its logs: 1 to 128
	// printable ASCII characters. Empty means "<hostname>-<pid>-<8 hex
	// digits>", which is different for every Worker.
	WorkerID string
	// Logger receives the worker's log lines. Nil means slog.Default().
	Logger *slog.Logger
}

// Worker takes jobs of one queue from the broker and runs a Handler on each,
// keeping their leases alive while the handlers run and reporting how they
// ended. Create one with NewWorker, set the handler with Handle and start it
// with Run. A Worker runs once.
type Worker struct {
	conn grpc.ClientConnInterface
	opts WorkerOptions

	mu      sync.Mutex
	handler Handler
	site    string // where Run was first called; empty until then
}

// NewWorker returns a worker that calls the broker through conn, which Dial
// opens. The caller keeps conn and closes it, after Run has returned.
func NewWorker(conn grpc.ClientConnInterface, opts WorkerOptions) *Worker {
	if opts.WorkerID == "" {
		host, _ := os.Hostname()
		random, _ := randomSeed()
		opts.WorkerID = generateWorkerID(host, os.Getpid(), uint32(random))
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Worker{conn: conn, opts: opts}
}

// generateWorkerID builds a worker id from the host name, the process id and a
// random number. Characters that a worker id may not hold become dashes, and
// a host name that is too long is cut.
func generateWorkerID(host string, pid int, random uint32) string {
	host = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		}
		return '-'
	}, host)
	if host == "" {
		host = "worker"
	}
	if len(host) > maxHostBytes {
		host = host[:maxHostBytes]
	}
	return fmt.Sprintf("%s-%d-%08x", host, pid, random)
}

// Handle sets the function that runs each job. It must be called before Run,
// and may be called again until then to replace the handler. After Run it
// returns an error that wraps ErrAlreadyRunning and names the call of Run.
func (w *Worker) Handle(h Handler) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.site != "" {
		return fmt.Errorf("%w: Run was called at %s", ErrAlreadyRunning, w.site)
	}
	if h == nil {
		return fmt.Errorf("%w: the handler is nil", ErrInvalid)
	}
	w.handler = h
	return nil
}

// Run takes jobs and runs them until ctx is cancelled, then drains: it asks
// for no more work, gives the handlers that are running DrainTimeout to
// finish, reports their results, and gives back what was still running. Run
// returns nil when that is done.
//
// It returns an error at once, before taking anything, if the options are
// invalid (the error wraps ErrInvalid) or no handler was set. A second call
// returns an error that wraps ErrAlreadyRunning and names where the first
// call was made. If the broker or the worker breaks the protocol between them,
// Run cancels the handlers and returns an error that wraps ErrInternal;
// leases that were still held run out on their own.
//
// Handlers run on goroutines of their own, and their contexts do not end
// when ctx does: the drain is what ends them.
func (w *Worker) Run(ctx context.Context) error {
	site := "an unknown call site"
	if _, file, line, ok := runtime.Caller(1); ok {
		site = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	cfg, handler, err := w.begin(site)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	seed, _ := randomSeed()
	tracker, err := lease.New(cfg, seed)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err) // begin has validated cfg
	}
	return newWorkerLoop(w.conn, w.opts, handler, tracker).run(ctx)
}

// begin checks that the worker may run, with the options it has and a
// handler, and if so records site as the call that runs it. It returns the
// tracker's configuration and the handler.
func (w *Worker) begin(site string) (lease.Config, Handler, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.site != "" {
		return lease.Config{}, nil, fmt.Errorf("%w: Run was called at %s", ErrAlreadyRunning, w.site)
	}
	cfg, err := w.config()
	if err != nil {
		return lease.Config{}, nil, err
	}
	if w.handler == nil {
		return lease.Config{}, nil, fmt.Errorf("%w: no handler was set; call Handle before Run", ErrInvalid)
	}
	w.site = site
	return cfg, w.handler, nil
}

// config validates the options and returns the configuration of the tracker.
func (w *Worker) config() (lease.Config, error) {
	invalid := func(err error) (lease.Config, error) { return lease.Config{}, fmt.Errorf("%w: %w", ErrInvalid, err) }
	if w.conn == nil {
		return invalid(errors.New("the connection is nil"))
	}
	if err := job.ValidateName("queue", w.opts.Queue); err != nil {
		return invalid(err)
	}
	if err := job.ValidateWorkerID(w.opts.WorkerID); err != nil {
		return invalid(err)
	}
	cfg := lease.Config{
		Concurrency:  w.opts.Concurrency,
		TTL:          w.opts.LeaseTTL,
		Margin:       w.opts.Margin,
		LongPoll:     w.opts.LongPoll,
		DrainTimeout: w.opts.DrainTimeout,
	}.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return invalid(err)
	}
	return cfg, nil
}
