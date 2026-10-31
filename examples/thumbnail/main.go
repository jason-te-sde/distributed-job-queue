// Command thumbnail is an example worker for the job queue. It does CPU work
// that is the same on every run, and writes its results through a sink that
// the fencing token of the lease protects.
//
//	thumbnail produce --addr A --insecure --count 100
//	thumbnail work --addr A --insecure --database-url postgres://... --concurrency 4
//
// A job's payload is {"seed": N, "size": S}: the worker draws a picture of S
// by S pixels from the seed, shrinks it to 128 by 128 and records the SHA-256
// of the PNG in a PostgreSQL table, under the job's fencing token. The worker
// writes nothing to disk, so it runs on a read-only root file system.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
)

const (
	// queueName is the queue the producer fills and the worker drains.
	queueName = "thumbnails"
	// sourceSize is the edge, in pixels, of the pictures that produce asks for.
	sourceSize = 1024
	// enqueueBatch is how many jobs produce sends in one call, the most the
	// broker accepts.
	enqueueBatch = 64

	defaultConcurrency = 4
	defaultCount       = 10

	// databaseURLEnv and tokenEnv name the variables that stand in for
	// --database-url and --token, so that neither has to be on a command line.
	databaseURLEnv = "JOBQUEUE_DATABASE_URL"
	tokenEnv       = "JOBQUEUE_TOKEN"
)

// errSuperseded is the failure of a write that a newer lease had already
// replaced.
var errSuperseded = errors.New("superseded by a newer lease")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

const usage = `usage: thumbnail <command> [flags]

commands:
  produce   enqueue thumbnail jobs
  work      run a worker that renders thumbnails and records them in PostgreSQL

Run "thumbnail <command> -h" for the flags of a command.`

// run is the command, apart from the process: it returns the exit status.
// Help goes to stdout and succeeds. A usage error exits with 2 and a failure
// of the work with 1; both write one line to stderr.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	var err error
	switch cmd, rest := args[0], args[1:]; cmd {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(stdout, usage)
		return 0
	case "produce":
		var cfg produceConfig
		if cfg, err = parseProduce(rest, getenv); err == nil {
			if err = runProduce(ctx, cfg, stdout); err != nil {
				return fail(stderr, 1, err)
			}
			return 0
		}
	case "work":
		var cfg workConfig
		if cfg, err = parseWork(rest, getenv); err == nil {
			if err = runWork(ctx, cfg, newLogger(stderr)); err != nil {
				return fail(stderr, 1, err)
			}
			return 0
		}
	default:
		return fail(stderr, 2, fmt.Errorf("unknown command %q; run thumbnail -h", cmd))
	}
	if h, ok := errors.AsType[*helpError](err); ok {
		_, _ = fmt.Fprintln(stdout, h.text)
		return 0
	}
	return fail(stderr, 2, err)
}

// fail writes the error as one line and returns status.
func fail(stderr io.Writer, status int, err error) int {
	_, _ = fmt.Fprintf(stderr, "thumbnail: %s\n", strings.Join(strings.Fields(err.Error()), " "))
	return status
}

func newLogger(w io.Writer) *slog.Logger { return slog.New(slog.NewJSONHandler(w, nil)) }

// transport says how a command reaches the broker, and with what credential.
type transport struct {
	insecure bool
	token    string
	caFile   string
}

// bind adds the transport flags to fs.
func (t *transport) bind(fs *flag.FlagSet) {
	fs.BoolVar(&t.insecure, "insecure", false, "connect without TLS; a token is then sent in the clear")
	fs.StringVar(&t.caFile, "tls-ca", "", "connect over TLS, trusting the CAs in this PEM `file`")
	fs.StringVar(&t.token, "token", "", "bearer `token` for the broker; defaults to $"+tokenEnv)
}

// resolve completes the transport after parsing: the token falls back to the
// environment, and the choices must make sense.
func (t *transport) resolve(fs *flag.FlagSet, getenv func(string) string) error {
	given := false
	fs.Visit(func(f *flag.Flag) { given = given || f.Name == "token" })
	switch {
	case given && t.token == "":
		// A variable that was never set must not turn into an unauthenticated
		// client unnoticed.
		return errors.New("--token is empty")
	case !given:
		t.token = getenv(tokenEnv)
	}
	return t.validate()
}

// validate checks that exactly one way of connecting was chosen. There is no
// default, as there is none in jobqueue.Dial: a command that guessed would
// either send a token in the clear or refuse a broker it could have reached.
func (t transport) validate() error {
	switch {
	case t.insecure && t.caFile != "":
		return errors.New("--tls-ca and --insecure contradict each other: choose one")
	case !t.insecure && t.caFile == "":
		return errors.New("choose how to reach the broker: --tls-ca FILE connects over TLS, --insecure connects without it")
	}
	return nil
}

// dialOptions returns the options of jobqueue.Dial for the transport. It reads
// the CA file, if there is one; nothing in the command writes a file.
func (t transport) dialOptions() ([]jobqueue.DialOption, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}
	var opts []jobqueue.DialOption
	if t.insecure {
		opts = append(opts, jobqueue.WithInsecure())
	} else {
		pemBytes, err := os.ReadFile(t.caFile)
		if err != nil {
			return nil, fmt.Errorf("--tls-ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("--tls-ca: no certificate in %s", t.caFile)
		}
		opts = append(opts, jobqueue.WithTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	}
	if t.token != "" {
		opts = append(opts, jobqueue.WithToken(t.token))
	}
	return opts, nil
}

// helpError is what parsing returns for a request for help; text is the help.
type helpError struct{ text string }

func (e *helpError) Error() string { return e.text }

// newFlagSet returns the flag set of a command. The flag package's own
// reporting is silenced: parse turns its errors into ours.
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet("thumbnail "+name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// parse parses args into fs. A request for help comes back as a *helpError. An
// argument that is not a flag is refused, as every parameter is a flag.
func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return &helpError{text: flagHelp(fs)}
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return nil
}

// flagHelp describes the flags of fs, spelled with two dashes as the errors
// spell them.
func flagHelp(fs *flag.FlagSet) string {
	var b strings.Builder
	fmt.Fprintf(&b, "usage: %s [flags]\n\nflags:\n", fs.Name())
	fs.VisitAll(func(f *flag.Flag) {
		arg, text := flag.UnquoteUsage(f)
		name := "--" + f.Name
		if arg != "" {
			name += " " + arg
		}
		fmt.Fprintf(&b, "  %s\n    \t%s", name, text)
		if f.DefValue != "" && f.DefValue != "false" {
			fmt.Fprintf(&b, " (default %s)", f.DefValue)
		}
		b.WriteString("\n")
	})
	return strings.TrimRight(b.String(), "\n")
}

// produceConfig is the configuration of thumbnail produce.
type produceConfig struct {
	addr      string
	count     int
	tenant    string
	transport transport
}

func parseProduce(args []string, getenv func(string) string) (produceConfig, error) {
	var c produceConfig
	fs := newFlagSet("produce")
	fs.StringVar(&c.addr, "addr", "", "broker `address`, for example dns:///jobqueue-grpc:7070 (required)")
	fs.IntVar(&c.count, "count", defaultCount, "how many jobs to enqueue, with the seeds 0 to count-1")
	fs.StringVar(&c.tenant, "tenant", "", "tenant of the jobs; empty leaves the choice to the broker")
	c.transport.bind(fs)
	if err := parse(fs, args); err != nil {
		return produceConfig{}, err
	}
	switch {
	case c.addr == "":
		return produceConfig{}, errors.New("--addr is required")
	case c.count < 1:
		return produceConfig{}, fmt.Errorf("--count must be at least 1, got %d", c.count)
	}
	if err := c.transport.resolve(fs, getenv); err != nil {
		return produceConfig{}, err
	}
	return c, nil
}

// workConfig is the configuration of thumbnail work.
type workConfig struct {
	addr        string
	databaseURL string
	concurrency int
	transport   transport
}

func parseWork(args []string, getenv func(string) string) (workConfig, error) {
	var c workConfig
	fs := newFlagSet("work")
	fs.StringVar(&c.addr, "addr", "", "broker `address`, for example dns:///jobqueue-grpc:7070 (required)")
	fs.StringVar(&c.databaseURL, "database-url", "", "PostgreSQL `URL` of the sink; defaults to $"+databaseURLEnv)
	fs.IntVar(&c.concurrency, "concurrency", defaultConcurrency, "how many jobs to render at once")
	c.transport.bind(fs)
	if err := parse(fs, args); err != nil {
		return workConfig{}, err
	}
	if c.databaseURL == "" {
		c.databaseURL = getenv(databaseURLEnv)
	}
	switch {
	case c.addr == "":
		return workConfig{}, errors.New("--addr is required")
	case c.databaseURL == "":
		return workConfig{}, errors.New("no database URL: pass --database-url or set " + databaseURLEnv)
	case c.concurrency < 1:
		return workConfig{}, fmt.Errorf("--concurrency must be at least 1, got %d", c.concurrency)
	}
	if err := c.transport.resolve(fs, getenv); err != nil {
		return workConfig{}, err
	}
	return c, nil
}

// runProduce enqueues cfg.count jobs with the seeds 0 to count-1. A job has no
// idempotency key of its own, so running the command twice enqueues twice.
func runProduce(ctx context.Context, cfg produceConfig, stdout io.Writer) error {
	opts, err := cfg.transport.dialOptions()
	if err != nil {
		return err
	}
	conn, err := jobqueue.Dial(cfg.addr, opts...)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	producer := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{Tenant: cfg.tenant})

	for start := 0; start < cfg.count; start += enqueueBatch {
		end := min(start+enqueueBatch, cfg.count)
		batch := make([]jobqueue.EnqueueParams, 0, end-start)
		for seed := start; seed < end; seed++ {
			payload, err := json.Marshal(Payload{Seed: int64(seed), Size: sourceSize})
			if err != nil {
				return fmt.Errorf("encode the payload of seed %d: %w", seed, err)
			}
			batch = append(batch, jobqueue.EnqueueParams{Queue: queueName, Payload: payload})
		}
		if _, err := producer.EnqueueBatch(ctx, batch); err != nil {
			return fmt.Errorf("enqueue the jobs with seeds %d to %d: %w", start, end-1, err)
		}
	}
	_, _ = fmt.Fprintf(stdout, "enqueued %d jobs on queue %s\n", cfg.count, queueName)
	return nil
}

// runWork applies the schema and runs the worker until ctx ends.
func runWork(ctx context.Context, cfg workConfig, log *slog.Logger) error {
	opts, err := cfg.transport.dialOptions()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("open the database: %w", err)
	}
	defer pool.Close()
	sink := NewSink(pool)
	if err := sink.EnsureSchema(ctx); err != nil {
		return err
	}

	conn, err := jobqueue.Dial(cfg.addr, opts...)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	worker := jobqueue.NewWorker(conn, jobqueue.WorkerOptions{Queue: queueName, Concurrency: cfg.concurrency, Logger: log})
	if err := worker.Handle(handler(sink, log)); err != nil {
		return err
	}
	log.Info("worker started", slog.String("queue", queueName), slog.Int("concurrency", cfg.concurrency))
	return worker.Run(ctx)
}

// handler returns the job handler of the worker.
func handler(sink *Sink, log *slog.Logger) jobqueue.Handler {
	return func(ctx context.Context, j *jobqueue.Job) error {
		err := process(ctx, sink, j.ID, j.Token(), j.Payload)
		if errors.Is(err, errSuperseded) {
			log.Warn("a newer lease already wrote this job", slog.Int64("job", j.ID), slog.Int64("token", j.Token()))
		}
		return err
	}
}

// process renders the thumbnail of a payload and records it under the token.
//
// A payload that cannot be rendered fails for good: the same bytes would fail
// again. So does a write that the sink did not apply: a newer holder of the
// lease has already written, so a retry would only repeat work that is done.
// A failure of the database is retried like any other error.
func process(ctx context.Context, sink *Sink, jobID, token int64, payload []byte) error {
	var p Payload
	if err := json.Unmarshal(payload, &p); err != nil {
		return jobqueue.Permanent(fmt.Errorf("invalid payload: %w", err))
	}
	data, sum, err := Thumbnail(p)
	if err != nil {
		return jobqueue.Permanent(fmt.Errorf("invalid payload: %w", err))
	}
	applied, err := sink.Write(ctx, jobID, token, sum, len(data))
	if err != nil {
		return err
	}
	if !applied {
		return jobqueue.Permanent(errSuperseded)
	}
	return nil
}
