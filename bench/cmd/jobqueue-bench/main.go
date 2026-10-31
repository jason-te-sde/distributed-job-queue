// Command jobqueue-bench runs the benchmarks of the job queue: the six
// experiments of the design, a worker process that one of them kills, and the
// plot of the noisy-neighbour result.
//
//	jobqueue-bench throughput --find-max          E1: maximum sustained rate and latency under load
//	jobqueue-bench ablate --what batch|coalesce|notify   E2: what each feature buys
//	jobqueue-bench noisy --scheduler drr|fifo     E3: a tenant's wait behind another's flood
//	jobqueue-bench recovery --kill worker|broker  E4: the cost of a killed process
//	jobqueue-bench soak --rate R                  E5: a steady load for half an hour
//	jobqueue-bench river --find-max               E6: River on the same database
//	jobqueue-bench plot --in A --in B --out F     the noisy-neighbour graph
//
// Every experiment writes one line of JSON per result to stdout and a summary to
// stderr, appends the JSON to --out if given, and takes the database from
// --database-url or else from $JOBQUEUE_DATABASE_URL.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/exp"
	"github.com/jason-te-sde/distributed-job-queue/bench/internal/plot"
	"github.com/jason-te-sde/distributed-job-queue/bench/internal/river"
)

const databaseURLEnv = "JOBQUEUE_DATABASE_URL"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

const usage = `usage: jobqueue-bench <command> [flags]

commands:
  throughput  E1: the maximum sustained rate, and latency at 50% and 80% of it
  ablate      E2: switch batching, ack coalescing or NOTIFY off and measure
  noisy       E3: a tenant's wait while another tenant floods the queue
  recovery    E4: how long a killed worker or broker costs
  soak        E5: a steady load for 30 minutes, with the table size per minute
  river       E6: River on the same PostgreSQL, as a library with no network hop
  worker      the worker process that recovery --kill worker kills
  plot        draw the noisy-neighbour graph from two noisy results

Run a command with -h for its flags. Every experiment prints one line of JSON per
result on stdout and a summary on stderr; --out appends the JSON to a file. The
database is --database-url, or else $JOBQUEUE_DATABASE_URL.
`

// run is main with its inputs and outputs passed in. It returns the exit code: 0
// on success, 1 when the command failed, 2 when it was called wrongly.
func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	name, rest := args[0], args[1:]
	if name == "help" || name == "-h" || name == "--help" {
		_, _ = fmt.Fprint(stdout, usage)
		return 0
	}
	cmds := map[string]func(*invocation) error{
		"throughput": cmdThroughput, "ablate": cmdAblate, "noisy": cmdNoisy, "recovery": cmdRecovery,
		"soak": cmdSoak, "river": cmdRiver, "worker": cmdWorker, "plot": cmdPlot,
	}
	cmd, ok := cmds[name]
	if !ok {
		_, _ = fmt.Fprintf(stderr, "jobqueue-bench: unknown command %q\n\n%s", name, usage)
		return 2
	}
	inv := &invocation{ctx: ctx, name: name, args: rest, stdout: stdout, stderr: stderr, getenv: getenv}
	err := cmd(inv)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.As(err, new(usageError)):
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "jobqueue-bench %s: %v\n", name, err)
		return 1
	}
}

// usageError is a command line that could not be parsed. The flag package has
// already told the user why.
type usageError struct{ error }

// invocation is one run of a command.
type invocation struct {
	ctx            context.Context
	name           string
	args           []string
	stdout, stderr io.Writer
	getenv         func(string) string
}

// flags returns a flag set for the command whose usage goes to stderr.
func (inv *invocation) flags() *flag.FlagSet {
	fs := flag.NewFlagSet("jobqueue-bench "+inv.name, flag.ContinueOnError)
	fs.SetOutput(inv.stderr)
	return fs
}

// parse parses the command's flags, which take no arguments.
func (inv *invocation) parse(fs *flag.FlagSet) error {
	if err := fs.Parse(inv.args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usageError{err}
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(inv.stderr, "jobqueue-bench %s takes flags only, got %q\n", inv.name, fs.Arg(0))
		return usageError{errors.New("unexpected argument")}
	}
	return nil
}

// common holds the flags that every experiment that uses the database takes.
type common struct {
	databaseURL string
	out         string
	brokers     int
	dbConns     int
}

func (c *common) register(fs *flag.FlagSet, brokers bool) {
	fs.StringVar(&c.databaseURL, "database-url", "", "PostgreSQL connection URL; defaults to $"+databaseURLEnv)
	fs.StringVar(&c.out, "out", "", "append the JSON results to this file")
	fs.IntVar(&c.dbConns, "db-conns", 0, "size of each broker's database pool; 0 is the broker's default (20)")
	if brokers {
		fs.IntVar(&c.brokers, "brokers", exp.DefaultBrokers, "number of brokers, run in this process")
	}
}

// setup resolves the database URL, flag over environment, and builds the Setup.
func (c *common) setup(inv *invocation, log func(string, ...any)) exp.Setup {
	url := c.databaseURL
	if url == "" {
		url = inv.getenv(databaseURLEnv)
	}
	return exp.Setup{DatabaseURL: url, Brokers: c.brokers, MaxDBConns: int32(c.dbConns), Log: log}
}

// execute runs an experiment with a sink and a progress log, and closes the sink.
func (c *common) execute(inv *invocation, experiment func(log func(string, ...any), emit exp.Emit) error) error {
	s, err := newSink(inv.stdout, inv.stderr, c.out)
	if err != nil {
		return err
	}
	logf := func(format string, args ...any) {
		_, _ = fmt.Fprintf(inv.stderr, "%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
	}
	err = experiment(logf, s.emit)
	if cerr := s.close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// load holds the flags of the commands that offer a rate or search for one.
type load struct {
	rate         float64
	findMax      bool
	lo, hi       float64
	steps        int
	duration     time.Duration
	payloadBytes int
}

func (l *load) register(fs *flag.FlagSet, duration time.Duration) {
	fs.Float64Var(&l.rate, "rate", 0, "offered load in jobs per second (an alternative to --find-max)")
	fs.BoolVar(&l.findMax, "find-max", false, "search for the highest sustained rate, then measure at 50% and 80% of it")
	fs.Float64Var(&l.lo, "lo", exp.DefaultLo, "lowest rate of the --find-max search, jobs per second")
	fs.Float64Var(&l.hi, "hi", exp.DefaultHi, "highest rate of the --find-max search, jobs per second")
	fs.IntVar(&l.steps, "steps", exp.DefaultSteps, "bisection steps of the --find-max search")
	fs.DurationVar(&l.duration, "duration", duration, "how long each probe offers load")
	fs.IntVar(&l.payloadBytes, "payload-bytes", exp.DefaultPayloadBytes, "payload size in bytes, at least 8")
}

// workers holds the flags that size the workers.
type workers struct{ workers, concurrency int }

func (w *workers) register(fs *flag.FlagSet, workersDefault, concurrencyDefault int) {
	fs.IntVar(&w.workers, "workers", workersDefault, "number of workers, each with a connection of its own")
	fs.IntVar(&w.concurrency, "concurrency", concurrencyDefault, "handlers each worker runs at once")
}

func cmdThroughput(inv *invocation) error {
	fs := inv.flags()
	var c common
	var l load
	var w workers
	c.register(fs, true)
	l.register(fs, exp.DefaultDuration)
	w.register(fs, exp.DefaultWorkers, exp.DefaultConcurrency)
	if err := inv.parse(fs); err != nil {
		return err
	}
	return c.execute(inv, func(log func(string, ...any), emit exp.Emit) error {
		return exp.Throughput(inv.ctx, exp.ThroughputOptions{
			Setup: c.setup(inv, log), Rate: l.rate, FindMax: l.findMax, Lo: l.lo, Hi: l.hi, Steps: l.steps,
			Duration: l.duration, PayloadBytes: l.payloadBytes, Workers: w.workers, Concurrency: w.concurrency,
		}, emit)
	})
}

func cmdAblate(inv *invocation) error {
	fs := inv.flags()
	var c common
	var l load
	var w workers
	var what string
	fs.StringVar(&what, "what", "", "the feature to switch off: batch (max_jobs 1 vs 64), coalesce (AckBatch 1 vs default) or notify (LISTEN vs --disable-listen)")
	c.register(fs, true)
	l.register(fs, exp.DefaultDuration)
	w.register(fs, exp.DefaultWorkers, exp.DefaultConcurrency)
	if err := inv.parse(fs); err != nil {
		return err
	}
	if what == "notify" && l.rate == 0 && !l.findMax {
		l.rate = 20 // an idle queue: far below what one worker can take
	}
	return c.execute(inv, func(log func(string, ...any), emit exp.Emit) error {
		return exp.Ablate(inv.ctx, exp.AblateOptions{What: what, ThroughputOptions: exp.ThroughputOptions{
			Setup: c.setup(inv, log), Rate: l.rate, FindMax: l.findMax, Lo: l.lo, Hi: l.hi, Steps: l.steps,
			Duration: l.duration, PayloadBytes: l.payloadBytes, Workers: w.workers, Concurrency: w.concurrency,
		}}, emit)
	})
}

func cmdNoisy(inv *invocation) error {
	fs := inv.flags()
	var c common
	var w workers
	var o exp.NoisyOptions
	c.register(fs, true)
	w.register(fs, exp.DefaultNoisyWorkers, exp.DefaultNoisyConcurrency)
	fs.StringVar(&o.Scheduler, "scheduler", "", "how the broker shares leases between tenants: drr or fifo")
	fs.IntVar(&o.Flood, "flood", exp.DefaultFlood, "jobs that tenant A enqueues as fast as it can")
	fs.Float64Var(&o.BRate, "b-rate", exp.DefaultBRate, "jobs per second that tenant B sends")
	fs.DurationVar(&o.Duration, "duration", exp.DefaultDuration, "how long tenant B sends")
	fs.DurationVar(&o.ServiceTime, "service-time", exp.DefaultServiceTime, "how long each handler takes, which sets the capacity the flood exceeds")
	fs.IntVar(&o.PayloadBytes, "payload-bytes", exp.DefaultPayloadBytes, "payload size in bytes, at least 8")
	if err := inv.parse(fs); err != nil {
		return err
	}
	o.Workers, o.Concurrency = w.workers, w.concurrency
	return c.execute(inv, func(log func(string, ...any), emit exp.Emit) error {
		o.Setup = c.setup(inv, log)
		return exp.Noisy(inv.ctx, o, emit)
	})
}

func cmdRecovery(inv *invocation) error {
	fs := inv.flags()
	var c common
	var o exp.RecoveryOptions
	c.register(fs, false)
	fs.IntVar(&c.brokers, "brokers", 0, "number of brokers; 1 for --kill worker, and 3 for --kill broker")
	fs.StringVar(&o.Kill, "kill", "", "what to kill: worker (a process holding --k leases) or broker (one of three, under load)")
	fs.IntVar(&o.K, "k", exp.DefaultKillK, "jobs the worker holds when it is killed")
	fs.DurationVar(&o.TTL, "ttl", exp.DefaultKillTTL, "lease TTL of the worker that is killed")
	fs.DurationVar(&o.Margin, "margin", exp.DefaultKillMargin, "safety margin of that worker")
	fs.Float64Var(&o.Rate, "rate", exp.DefaultKillRate, "jobs per second of the load while a broker is killed")
	fs.DurationVar(&o.Duration, "duration", exp.DefaultKillDuration, "length of that load; the broker is killed half way")
	fs.StringVar(&o.Exe, "exe", "", "the jobqueue-bench binary to run as the worker process; default this binary")
	if err := inv.parse(fs); err != nil {
		return err
	}
	return c.execute(inv, func(log func(string, ...any), emit exp.Emit) error {
		o.Setup = c.setup(inv, log)
		if o.Kill == "worker" && o.Brokers == 0 {
			o.Brokers = 1
		}
		return exp.Recovery(inv.ctx, o, emit)
	})
}

func cmdSoak(inv *invocation) error {
	fs := inv.flags()
	var c common
	var w workers
	var o exp.SoakOptions
	c.register(fs, true)
	w.register(fs, exp.DefaultWorkers, exp.DefaultConcurrency)
	fs.Float64Var(&o.Rate, "rate", 0, "offered load in jobs per second: about half of the maximum that throughput --find-max reported")
	fs.DurationVar(&o.Duration, "duration", exp.DefaultSoakDuration, "length of the run")
	fs.DurationVar(&o.Interval, "interval", exp.DefaultSoakInterval, "how often the run is sampled")
	fs.DurationVar(&o.Retention, "retention", exp.DefaultSoakRetention, "how long finished jobs are kept; shorter than the 24h of production, and stated in every result")
	fs.DurationVar(&o.PruneInterval, "prune-interval", exp.DefaultPruneInterval, "how often finished jobs are deleted")
	fs.IntVar(&o.PayloadBytes, "payload-bytes", exp.DefaultPayloadBytes, "payload size in bytes, at least 8")
	if err := inv.parse(fs); err != nil {
		return err
	}
	o.Workers, o.Concurrency = w.workers, w.concurrency
	return c.execute(inv, func(log func(string, ...any), emit exp.Emit) error {
		o.Setup = c.setup(inv, log)
		return exp.Soak(inv.ctx, o, emit)
	})
}

func cmdRiver(inv *invocation) error {
	fs := inv.flags()
	var c common
	var l load
	var o river.Options
	c.register(fs, false)
	l.register(fs, exp.DefaultDuration)
	fs.IntVar(&o.Concurrency, "concurrency", river.DefaultConcurrency, "River's MaxWorkers for the queue; E1's default is this many handler slots in all")
	fs.DurationVar(&o.FetchCooldown, "fetch-cooldown", river.DefaultFetchCooldown, "River's FetchCooldown, which limits its throughput; its own default is 100ms")
	if err := inv.parse(fs); err != nil {
		return err
	}
	return c.execute(inv, func(log func(string, ...any), emit exp.Emit) error {
		setup := c.setup(inv, log)
		o.DatabaseURL, o.DBConns, o.Log = setup.DatabaseURL, setup.MaxDBConns, log
		o.Rate, o.FindMax, o.Lo, o.Hi, o.Steps, o.Duration, o.PayloadBytes = l.rate, l.findMax, l.lo, l.hi, l.steps, l.duration, l.payloadBytes
		return river.Run(inv.ctx, o, emit)
	})
}

func cmdWorker(inv *invocation) error {
	fs := inv.flags()
	var o exp.WorkerProcessOptions
	fs.StringVar(&o.Addr, "addr", "", "address of the broker")
	fs.StringVar(&o.Queue, "queue", "", "queue to take jobs from")
	fs.IntVar(&o.Concurrency, "concurrency", 1, "jobs to hold at once")
	fs.DurationVar(&o.TTL, "ttl", 0, "lease TTL; 0 is the SDK's default")
	fs.DurationVar(&o.Margin, "margin", 0, "safety margin; 0 is the SDK's default")
	fs.StringVar(&o.WorkerID, "worker-id", "", "name the worker gives the broker")
	if err := inv.parse(fs); err != nil {
		return err
	}
	return exp.RunWorkerProcess(inv.ctx, o, os.Stdin)
}

// stringsFlag is a flag that may be given more than once.
type stringsFlag []string

func (s *stringsFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error { *s = append(*s, v); return nil }

func cmdPlot(inv *invocation) error {
	fs := inv.flags()
	var in stringsFlag
	var out string
	fs.Var(&in, "in", "a file of results of the noisy experiment; give it twice, once for each scheduler")
	fs.StringVar(&out, "out", "", "the SVG file to write")
	if err := inv.parse(fs); err != nil {
		return err
	}
	if len(in) != 2 || out == "" {
		return fmt.Errorf("plot needs exactly two --in files and an --out file, got %d and %q", len(in), out)
	}
	svg, err := plotNoisy(in[0], in[1])
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(out, svg, 0o644); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(inv.stderr, "wrote %s\n", out)
	return nil
}

// plotNoisy draws the CDF of tenant B's wait from the last noisy result of each
// file. The two runs must have been made with the same parameters but for the
// scheduler, or the graph would compare two things that differ for another
// reason.
func plotNoisy(pathA, pathB string) ([]byte, error) {
	var runs [2]exp.Result
	var series []plot.Series
	for i, path := range []string{pathA, pathB} {
		r, err := exp.ReadResult(path, "noisy")
		if err != nil {
			return nil, err
		}
		if len(r.CDF) == 0 {
			return nil, fmt.Errorf("%s: the result has no CDF to draw (was it made by a version that did not record one?)", path)
		}
		runs[i] = r
		pts := make([]plot.Point, len(r.CDF))
		for j, p := range r.CDF {
			pts[j] = plot.Point{Wait: p.Wait, Fraction: p.Fraction}
		}
		series = append(series, plot.Series{Label: strings.ToUpper(r.Params["scheduler"]), Points: pts})
	}
	if runs[0].Params["scheduler"] == runs[1].Params["scheduler"] {
		return nil, fmt.Errorf("both runs used the scheduler %q: the graph compares fifo with drr", runs[0].Params["scheduler"])
	}
	for _, key := range sortedKeys(runs[0].Params, runs[1].Params) {
		if key != "scheduler" && runs[0].Params[key] != runs[1].Params[key] {
			return nil, fmt.Errorf("the runs differ in %s (%q and %q), not only in the scheduler, so their waits cannot be compared",
				key, runs[0].Params[key], runs[1].Params[key])
		}
	}
	p := runs[0].Params
	title := fmt.Sprintf("tenant A floods %s jobs, tenant B sends %s/s for %s; %s broker(s)\n"+
		"%s workers x %s handlers, %s per job, %s-byte payloads; wait is measured from each job's intended send time",
		p["flood"], p["b_rate"], p["duration"], p["brokers"], p["workers"], p["concurrency"], p["service_time"], p["payload_bytes"])
	var buf bytes.Buffer
	if err := plot.Render(&buf, series, title); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func sortedKeys(maps ...map[string]string) []string {
	seen := map[string]bool{}
	for _, m := range maps {
		for k := range m {
			seen[k] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// sink writes results: JSON lines to stdout and, if asked, appended to a file,
// and a summary of each to stderr.
type sink struct {
	stdout, stderr io.Writer
	file           *os.File

	mu  sync.Mutex
	err error
}

// newSink opens the file at out for appending, if out is not empty.
func newSink(stdout, stderr io.Writer, out string) (*sink, error) {
	s := &sink{stdout: stdout, stderr: stderr}
	if out != "" {
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open --out: %w", err)
		}
		s.file = f
	}
	return s, nil
}

// emit writes one result. A failure to write is kept and returned by close,
// because an experiment has no use for it and must not stop for it.
func (s *sink) emit(r exp.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	line, err := r.JSON()
	if err != nil {
		s.fail(fmt.Errorf("encode a result: %w", err))
		return
	}
	line = append(line, '\n')
	if _, err := s.stdout.Write(line); err != nil {
		s.fail(fmt.Errorf("write stdout: %w", err))
	}
	if s.file != nil {
		if _, err := s.file.Write(line); err != nil {
			s.fail(fmt.Errorf("write --out: %w", err))
		}
	}
	if _, err := fmt.Fprintln(s.stderr, summarize(r)); err != nil {
		s.fail(fmt.Errorf("write stderr: %w", err))
	}
}

func (s *sink) fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

// close closes the file and returns the first error of the sink.
func (s *sink) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			s.fail(fmt.Errorf("close --out: %w", err))
		}
	}
	return s.err
}

// summaryParams are the params that tell results of one experiment apart.
var summaryParams = []string{"what", "variant", "phase", "kill", "scheduler", "rate", "interval_index"}

// quantileOrder is the order the quantiles are printed in.
var quantileOrder = []string{"p50", "p90", "p99", "p99.9", "max"}

// summarize is the one-line human form of a result.
func summarize(r exp.Result) string {
	var b strings.Builder
	b.WriteString(r.Experiment)
	for _, k := range summaryParams {
		if v, ok := r.Params[k]; ok {
			fmt.Fprintf(&b, " %s=%s", k, v)
		}
	}
	b.WriteString(":")
	names := make([]string, 0, len(r.Metrics))
	for k := range r.Metrics {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		fmt.Fprintf(&b, " %s=%.4g", k, r.Metrics[k])
	}
	for _, k := range quantileOrder {
		if v, ok := r.Quantiles[k]; ok {
			fmt.Fprintf(&b, " %s=%v", k, roundDuration(v))
		}
	}
	return b.String()
}

// roundDuration keeps three significant digits' worth of a latency.
func roundDuration(d time.Duration) time.Duration {
	switch {
	case d >= time.Second:
		return d.Round(time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}
