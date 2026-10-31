package exp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// The defaults of E4.
const (
	DefaultKillK        = 100
	DefaultKillTTL      = 30 * time.Second
	DefaultKillMargin   = 3 * time.Second
	DefaultKillRate     = 200.0
	DefaultKillDuration = 20 * time.Second
	// recoveryBrokers is the number of brokers when a broker is killed: one of
	// three.
	recoveryBrokers = 3
	// heldTimeout bounds waiting for the worker process to take its leases.
	heldTimeout = time.Minute
	// restartSlack is added to twice the bound when waiting for the restarts, so
	// that a run that misses its bound still reports how long it took.
	restartSlack = 30 * time.Second
)

// RecoveryOptions configures E4.
type RecoveryOptions struct {
	Setup
	// Kill is what to kill: "worker" or "broker".
	Kill string
	// K is the number of jobs the worker holds when it is killed, TTL and Margin
	// its lease length and safety margin. They are for Kill "worker".
	K      int
	TTL    time.Duration
	Margin time.Duration
	// Rate and Duration are the load that runs while a broker is killed half
	// way through. They are for Kill "broker", which needs three brokers.
	Rate     float64
	Duration time.Duration
	// Exe is the jobqueue-bench binary to run as the worker process; the default
	// is the running binary.
	Exe string
}

func (o RecoveryOptions) withDefaults() RecoveryOptions {
	if o.K == 0 {
		o.K = DefaultKillK
	}
	if o.TTL == 0 {
		o.TTL = DefaultKillTTL
	}
	if o.Margin == 0 {
		o.Margin = DefaultKillMargin
	}
	if o.Rate == 0 {
		o.Rate = DefaultKillRate
	}
	if o.Duration == 0 {
		o.Duration = DefaultKillDuration
	}
	if o.Kill == "broker" && o.Brokers == 0 {
		o.Brokers = recoveryBrokers
	}
	return o
}

func (o RecoveryOptions) validate() error {
	if err := o.Setup.validate(); err != nil {
		return err
	}
	switch {
	case o.Kill != "worker" && o.Kill != "broker":
		return fmt.Errorf("--kill must be worker or broker, got %q", o.Kill)
	case o.K < 1 || o.K > maxConcurrency:
		return fmt.Errorf("--k must be between 1 and %d, got %d", maxConcurrency, o.K)
	case o.TTL < time.Second:
		return fmt.Errorf("--ttl must be at least 1s, got %v", o.TTL)
	case o.Margin < 0 || o.Margin >= o.TTL/4:
		return fmt.Errorf("--margin must be under a quarter of the --ttl (%v), got %v", o.TTL/4, o.Margin)
	case !(o.Rate > 0):
		return fmt.Errorf("--rate must be above zero, got %v", o.Rate)
	case o.Duration <= 0:
		return fmt.Errorf("--duration must be positive, got %v", o.Duration)
	case o.Kill == "broker" && o.brokers() < 2:
		return fmt.Errorf("--kill broker needs at least 2 brokers to leave one standing, got %d", o.brokers())
	}
	return nil
}

// Recovery is E4: what a killed process costs.
//
// With Kill "worker" a worker process holding K leases is killed with SIGKILL,
// and the result is how long it takes until all K jobs have started again on
// another worker. The theoretical bound is the lease TTL, plus the reap
// interval, plus the 1s ceiling of the first retry backoff, plus the dispatch
// delay, which is the fallback poll of the brokers; the result states each part
// and whether the measurement stayed within their sum. A killed worker cannot
// release its leases, and its last heartbeat was at most a quarter of a TTL
// before the kill, so the leases run out between three quarters of a TTL and a
// whole one after it.
//
// With Kill "broker" three brokers serve a steady load and, half way through,
// one of them is cut off: its connections are reset and its port refuses new
// ones, while the process keeps running. The result is the errors that the
// workers saw, and how long they went on, and that no job was lost.
func Recovery(ctx context.Context, o RecoveryOptions, emit Emit) error {
	o = o.withDefaults()
	if err := o.validate(); err != nil {
		return err
	}
	r, err := openRig(ctx, o.Setup, ClusterOptions{Killable: o.Kill == "broker"})
	if err != nil {
		return err
	}
	var res Result
	if o.Kill == "worker" {
		res, err = r.killWorker(ctx, o)
	} else {
		res, err = r.killBroker(ctx, o)
	}
	if cerr := r.close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	emit(res)
	return nil
}

// heldBy counts the leased jobs of the queue.
func (r *rig) leased(ctx context.Context, queue string) (int, error) {
	var n int
	err := r.cluster.Pool().QueryRow(ctx, "SELECT count(*) FROM jobqueue.jobs WHERE queue = $1 AND state = 'leased'", queue).Scan(&n)
	return n, err
}

// enqueueN puts n jobs of one tenant in the queue, in batches.
func enqueueN(ctx context.Context, p *jobqueue.Producer, queue string, n int) error {
	for n > 0 {
		batch := make([]jobqueue.EnqueueParams, min(n, job.MaxBatch))
		for i := range batch {
			batch[i] = jobqueue.EnqueueParams{Queue: queue, Tenant: benchTenant, Payload: make([]byte, stampBytes)}
		}
		if _, err := p.EnqueueBatch(ctx, batch); err != nil {
			return err
		}
		n -= len(batch)
	}
	return nil
}

func (r *rig) killWorker(ctx context.Context, o RecoveryOptions) (Result, error) {
	queue := r.nextQueue()
	defer r.cleanup(queue)
	began := time.Now()

	conn, err := r.cluster.Dial()
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = conn.Close() }()
	if err := enqueueN(ctx, jobqueue.NewProducer(conn, jobqueue.ProducerOptions{}), queue, o.K); err != nil {
		return Result{}, fmt.Errorf("enqueue the %d jobs: %w", o.K, err)
	}

	// The victim is a process of its own, so that killing it is a kill.
	exe := o.Exe
	if exe == "" {
		if exe, err = os.Executable(); err != nil {
			return Result{}, fmt.Errorf("find the binary to run as the worker process: %w", err)
		}
	}
	cmd := exec.Command(exe, "worker", "--addr", r.cluster.Addrs()[0], "--queue", queue,
		"--concurrency", strconv.Itoa(o.K), "--ttl", o.TTL.String(), "--margin", o.Margin.String(), "--worker-id", "bench-victim")
	proc := &workerProc{cmd: cmd, done: make(chan struct{})}
	cmd.Stderr = &proc.stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return Result{}, err
	}
	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("start the worker process %s: %w", exe, err)
	}
	go proc.wait()
	killed := false
	defer func() {
		if !killed {
			_ = cmd.Process.Kill()
			<-proc.done
		}
		_ = stdin.Close()
	}()

	if err := r.awaitHeld(ctx, queue, o.K, proc); err != nil {
		return Result{}, err
	}

	// The survivor is a worker in this process. It starts after the victim has
	// every job, so it finds the queue empty and parks a Lease call, and takes
	// the jobs when the leases are taken back.
	epoch := time.Now()
	var (
		mu        sync.Mutex
		restarted = map[int64]time.Duration{}
		premature []int64
	)
	handler := func(_ context.Context, j *jobqueue.Job) error {
		at := time.Since(epoch)
		mu.Lock()
		defer mu.Unlock()
		if j.Attempt < 2 {
			premature = append(premature, j.ID)
		}
		if _, seen := restarted[j.ID]; !seen {
			restarted[j.ID] = at
		}
		return nil
	}
	stop, err := startWorkers(ctx, r.cluster, queue, workerSpec{Count: 1, Concurrency: min(o.K, 256)}, handler)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = stop() }()

	if err := cmd.Process.Kill(); err != nil {
		return Result{}, fmt.Errorf("kill the worker process: %w", err)
	}
	killed = true
	killedAt := time.Since(epoch)
	<-proc.done

	cfg := r.cluster.Config()
	dispatch := max(cfg.PollInterval, cfg.ActivityInterval)
	// The reaper delays a retry by a draw from the default backoff after the job's
	// attempt, which is the first, so the ceiling of that draw is the part of the bound.
	ttl, reap, backoff := o.TTL.Seconds(), cfg.ReapInterval.Seconds(), job.DefaultBackoff.Ceiling(1).Seconds()
	bound := ttl + reap + backoff + dispatch.Seconds()
	deadline := time.After(time.Duration(2*bound*float64(time.Second)) + restartSlack)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		mu.Lock()
		n := len(restarted)
		mu.Unlock()
		if n >= o.K {
			break
		}
		select {
		case <-tick.C:
		case <-deadline:
			return Result{}, fmt.Errorf("only %d of %d jobs started again within %v of the kill", n, o.K, time.Duration(2*bound*float64(time.Second))+restartSlack)
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	if err := stop(); err != nil {
		return Result{}, err
	}

	mu.Lock()
	defer mu.Unlock()
	if len(premature) > 0 {
		return Result{}, fmt.Errorf("%d jobs started on the survivor at their first attempt (ids %v): the victim had not held them all", len(premature), premature)
	}
	delays := hist.New()
	var all, first time.Duration
	first = time.Duration(1<<63 - 1)
	for _, at := range restarted {
		d := at - killedAt
		delays.Record(d)
		all, first = max(all, d), min(first, d)
	}
	r.logf("recovery worker: %d held jobs started again %.2fs after the kill, bound %.2fs", o.K, all.Seconds(), bound)
	return Result{
		Experiment: "recovery",
		Params: map[string]string{
			"kill": "worker", "brokers": strconv.Itoa(o.brokers()), "k": strconv.Itoa(o.K),
			"ttl": o.TTL.String(), "margin": o.Margin.String(),
		},
		Metrics: map[string]float64{
			"k": float64(o.K), "restarted": float64(len(restarted)),
			"ttl_s": ttl, "reap_interval_s": reap, "first_backoff_s": backoff, "dispatch_s": dispatch.Seconds(), "bound_s": bound,
			"time_to_all_restart_s": all.Seconds(), "first_restart_s": first.Seconds(),
			"within_bound": flag(all.Seconds() <= bound),
		},
		Quantiles: delays.Quantiles(), Env: r.env, Started: began, Duration: time.Since(began),
	}, nil
}

// workerProc is the worker process of the experiment and how it ended.
type workerProc struct {
	cmd    *exec.Cmd
	stderr bytes.Buffer
	// done is closed when the process has ended and been waited for, and err is
	// then how it ended. Neither it nor stderr is read before.
	done chan struct{}
	err  error
}

func (p *workerProc) wait() {
	p.err = p.cmd.Wait()
	close(p.done)
}

// awaitHeld waits until the worker process holds k leases of the queue. It
// fails if the process ends first, with what it wrote to stderr.
func (r *rig) awaitHeld(ctx context.Context, queue string, k int, proc *workerProc) error {
	deadline := time.After(heldTimeout)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		n, err := r.leased(ctx, queue)
		if err != nil {
			return fmt.Errorf("count the leases: %w", err)
		}
		if n >= k {
			return nil
		}
		select {
		case <-tick.C:
		case <-proc.done:
			err := proc.err
			if err == nil {
				err = errors.New("exited with status 0")
			}
			return fmt.Errorf("the worker process ended before it held %d leases (it held %d): %w\n%s", k, n, err, proc.stderr.String())
		case <-deadline:
			return fmt.Errorf("the worker process held %d of %d leases after %v", n, k, heldTimeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// errObserver is a client interceptor that records the time of every failed
// call, other than a call that its caller cancelled.
type errObserver struct {
	epoch time.Time

	mu    sync.Mutex
	times []time.Duration
	byOp  map[string][]time.Duration
}

func newErrObserver(epoch time.Time) *errObserver {
	return &errObserver{epoch: epoch, byOp: map[string][]time.Duration{}}
}

func (e *errObserver) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil && status.Code(err) != codes.Canceled {
		at := time.Since(e.epoch)
		e.mu.Lock()
		e.times = append(e.times, at)
		op := path.Base(method)
		e.byOp[op] = append(e.byOp[op], at)
		e.mu.Unlock()
	}
	return err
}

// brokerKillMode is what the E4 broker experiment kills, in the words of its
// result: a broker cannot be stopped abruptly from here, so the connections to it
// are cut.
const brokerKillMode = "connections reset through a proxy; broker process stays up"

// brokerParams are the params of the result of killing a broker.
func (o RecoveryOptions) brokerParams() map[string]string {
	return map[string]string{
		"kill": "broker", "mode": brokerKillMode, "brokers": strconv.Itoa(o.brokers()),
		"rate": formatRate(o.Rate), "duration": o.Duration.String(),
		"workers": strconv.Itoa(DefaultWorkers), "concurrency": strconv.Itoa(DefaultConcurrency),
	}
}

func (r *rig) killBroker(ctx context.Context, o RecoveryOptions) (Result, error) {
	queue := r.nextQueue()
	defer r.cleanup(queue)
	began := time.Now()

	m := NewMeter(benchTenant)
	obs := newErrObserver(m.Epoch())
	stop, err := startWorkers(ctx, r.cluster, queue, workerSpec{Count: DefaultWorkers, Concurrency: DefaultConcurrency, Observe: obs.intercept}, meterHandler(m, 0))
	if err != nil {
		return Result{}, err
	}
	conn, err := r.cluster.Dial()
	if err != nil {
		_ = stop()
		return Result{}, err
	}
	defer func() { _ = conn.Close() }()
	producer := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{PerCallTimeout: 30 * time.Second})

	start := time.Now().Add(startDelay)
	killAt := start.Add(o.Duration / 2)
	var (
		killWG   sync.WaitGroup
		killedAt time.Duration
		reset    int
		killErr  error
	)
	killWG.Add(1)
	go func() {
		defer killWG.Done()
		select {
		case <-time.After(time.Until(killAt)):
		case <-ctx.Done():
			killErr = ctx.Err()
			return
		}
		// The instant is taken before the call, not after it: the resets that the
		// kill causes reach the workers while it runs, and an error that they
		// observe in that time is the kill's, not an error before it.
		killedAt = time.Since(m.Epoch())
		reset, killErr = r.cluster.KillBroker(0)
	}()
	out, err := probeFrom(ctx, ProbeConfig{Rate: o.Rate, Duration: o.Duration, DrainWait: MeasureDrainWait}, start, m, benchTenant,
		func(ctx context.Context, _ int, intended time.Time) error {
			_, err := producer.Enqueue(ctx, jobqueue.EnqueueParams{Queue: queue, Tenant: benchTenant, Payload: m.Payload(intended, DefaultPayloadBytes)})
			return err
		})
	killWG.Wait()
	if werr := stop(); werr != nil && err == nil {
		err = werr
	}
	if err == nil {
		err = killErr
	}
	if err == nil && out.SendErr != nil {
		err = fmt.Errorf("the producer could not enqueue through the dead broker: %w", out.SendErr)
	}
	if err != nil {
		return Result{}, err
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	before, after := 0, []time.Duration{}
	for _, t := range obs.times {
		if t < killedAt {
			before++
		} else {
			after = append(after, t)
		}
	}
	metrics := map[string]float64{
		"connections_reset":  float64(reset),
		"kill_at_s":          killedAt.Seconds() - start.Sub(m.Epoch()).Seconds(),
		"errors_before_kill": float64(before),
		"errors_after_kill":  float64(len(after)),
		"sent":               float64(out.Sent),
		"started":            float64(out.Started),
		"late_sends":         float64(out.Late),
	}
	for _, op := range []string{"Lease", "Heartbeat", "Ack", "Release"} {
		n := 0
		for _, t := range obs.byOp[op] {
			if t >= killedAt {
				n++
			}
		}
		metrics["errors_"+strings.ToLower(op)] = float64(n)
	}
	if len(after) > 0 {
		firstErr, lastErr := slices.Min(after), slices.Max(after)
		metrics["first_error_after_kill_s"] = (firstErr - killedAt).Seconds()
		metrics["last_error_after_kill_s"] = (lastErr - killedAt).Seconds()
		metrics["error_window_s"] = (lastErr - firstErr).Seconds()
	}
	r.logf("recovery broker: %d connections reset, %d worker errors after the kill (%d before), %d of %d jobs started",
		reset, len(after), before, out.Started, out.Sent)
	if out.Started != int64(out.Sent) {
		return Result{}, errors.New("jobs were lost: " + strconv.FormatInt(int64(out.Sent)-out.Started, 10) + " of " + strconv.Itoa(out.Sent) + " never started")
	}
	return Result{
		Experiment: "recovery",
		Params:     o.brokerParams(),
		Metrics:    metrics, Quantiles: out.Hist.Quantiles(), Env: r.env, Started: began, Duration: time.Since(began),
	}, nil
}
