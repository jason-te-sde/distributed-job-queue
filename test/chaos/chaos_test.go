//go:build chaos

// Package chaos is the multi-process chaos test of the job queue. It starts
// real broker and worker processes over a real PostgreSQL, puts Toxiproxy
// between them, and runs a producer while a seeded fault generator kills and
// freezes processes, cuts and slows the network, and restarts the database.
// Every process writes a history of what it saw, and after the faults end
// internal/history checks the invariants against the final state of the
// database.
//
// The test needs a PostgreSQL container and a toxiproxy-server, which
// .github/workflows/ci.yml starts; see the environment variables below. It
// fails when they are missing: a chaos test that skips looks like one that
// passes.
//
//	JOBQUEUE_CHAOS_PG_CONTAINER  name of the Docker container that runs PostgreSQL
//	JOBQUEUE_CHAOS_PG_DSN        postgres:// URL of a superuser of that server
//	JOBQUEUE_CHAOS_TOXIPROXY_ADDR  toxiproxy-server API address (default 127.0.0.1:8474)
//	JOBQUEUE_CHAOS_SEED          seed of the faults and of the workload (default 1)
//	JOBQUEUE_CHAOS_DURATION      length of the fault phase (default 2m); the
//	                             number of jobs scales with it
package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/history"
)

// The shape of a run.
const (
	numBrokers        = 3
	numWorkers        = 4
	handlersPerWorker = 4
	// leaseTTL is more than four times leaseMargin, as the SDK requires.
	leaseTTL    = 5 * time.Second
	leaseMargin = time.Second

	// totalJobs are enqueued over faultPhase; a shorter phase scales them.
	totalJobs  = 2000
	faultPhase = 120 * time.Second
	// drainBudget is how long after the faults end every job has to finish.
	drainBudget = 180 * time.Second
	// cancelPercent of the confirmed jobs are cancelled.
	cancelPercent = 1
	// maxPostgresRestarts bounds the restarts of PostgreSQL in one run.
	maxPostgresRestarts = 2
	// stopGrace is how long the processes get to exit after SIGTERM, which
	// is longer than the sleep of a handler that outlives its lease.
	stopGrace = 20 * time.Second
)

// allFaults are the kinds of fault that a run must fire at least once.
var allFaults = []string{
	FaultKillBroker, FaultKillWorker, FaultStallWorker,
	FaultPartitionBroker, FaultPartitionPG, FaultLatency, FaultRestartPostgres,
}

func TestChaos(t *testing.T) {
	phase := faultPhase
	if v := os.Getenv("JOBQUEUE_CHAOS_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			t.Fatalf("JOBQUEUE_CHAOS_DURATION %q is not a positive duration", v)
		}
		phase = d
	}
	jobs := int(int64(totalJobs) * int64(phase) / int64(faultPhase))
	seed := uint64(1)
	if v := os.Getenv("JOBQUEUE_CHAOS_SEED"); v != "" {
		s, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("JOBQUEUE_CHAOS_SEED %q is not an unsigned integer", v)
		}
		seed = s
	}
	toxiproxyAddr := os.Getenv("JOBQUEUE_CHAOS_TOXIPROXY_ADDR")
	if toxiproxyAddr == "" {
		toxiproxyAddr = "127.0.0.1:8474"
	}
	t.Logf("seed %d, fault phase %s, %d jobs", seed, phase, jobs)

	c := StartCluster(t, ClusterConfig{
		Brokers: numBrokers, Workers: numWorkers, Concurrency: handlersPerWorker,
		TTL: leaseTTL, Margin: leaseMargin,
		PGContainer:   requireEnv(t, "JOBQUEUE_CHAOS_PG_CONTAINER"),
		PGDSN:         requireEnv(t, "JOBQUEUE_CHAOS_PG_DSN"),
		ToxiproxyAddr: toxiproxyAddr,
		Seed:          seed,
	})

	// The producer outlives the fault phase if it fell behind, but not the
	// drain budget.
	start := time.Now()
	prodCtx, stopProducer := context.WithDeadline(context.Background(), start.Add(phase+drainBudget))
	defer stopProducer()
	prodDone := make(chan producerStats, 1)
	go func() { prodDone <- runProducer(prodCtx, t, c, seed, jobs, phase) }()

	faultCtx, stopFaults := context.WithTimeout(context.Background(), phase)
	runFaults(faultCtx, t, c, seed)
	stopFaults()
	c.Heal()
	healedAt := time.Now()
	t.Logf("faults fired: %v", c.FaultCounts())

	stats := <-prodDone
	t.Logf("producer: %+v", stats)

	drainCtx, stopDrain := context.WithDeadline(context.Background(), healedAt.Add(drainBudget))
	defer stopDrain()
	states, drained := waitDrained(drainCtx, t, c.Pool())
	if !drained {
		t.Errorf("after %s the jobs were still not all finished: %v", drainBudget, states)
	}
	c.StopProcesses(stopGrace)

	final, sink, states, err := readFinalState(c.Pool())
	if err != nil {
		t.Fatalf("read the final state: %v", err)
	}
	events := readHistory(t, c)
	rep := history.Check(events, final, sink)

	fenced := 0
	for _, e := range events {
		if e.Kind == history.SinkWrite && !e.Applied {
			fenced++
		}
	}
	summary := fmt.Sprintf("seed %d\nfaults %v\nstates %v\nevents %v\nduplicate deliveries %d\nfenced sink writes %d\nviolations %d\n",
		seed, c.FaultCounts(), states, rep.Counts, rep.DuplicateDeliveries, fenced, len(rep.Violations))
	c.SaveExtra("result.txt", []byte(summary))
	t.Log("result:\n" + summary)

	for i, v := range rep.Violations {
		if i == 25 {
			t.Errorf("... and %d more violations", len(rep.Violations)-i)
			break
		}
		t.Errorf("violation %s (job %d): %s", v.Invariant, v.JobID, v.Detail)
	}
	counts := c.FaultCounts()
	for _, kind := range allFaults {
		if counts[kind] == 0 {
			t.Errorf("no fault of kind %s was fired", kind)
		}
	}
	if rep.DuplicateDeliveries == 0 {
		t.Error("no job was delivered twice, so the run did not push on redelivery")
	}
	if stats.confirmed != jobs {
		t.Errorf("%d of %d enqueues were confirmed", stats.confirmed, jobs)
	}
	if stats.unconfirmedCancels > 0 {
		t.Errorf("%d cancels were never confirmed, so their jobs are not known to be cancelled", stats.unconfirmedCancels)
	}
	if states["dead"] > 0 {
		t.Errorf("%d jobs are dead: this workload never fails for good, so each used up its attempts", states["dead"])
	}
	for _, u := range c.Unexpected() {
		t.Errorf("process crash: %s", u)
	}
}

// requireEnv returns the environment variable name, and fails the test when
// it is not set.
func requireEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("%s is not set; the chaos test never skips (see the package comment)", name)
	}
	return v
}

// runFaults fires a fault every 3 to 8 seconds until ctx ends. The first
// faults are one of each kind, in an order drawn from the seed, so that a run
// covers every kind whatever the draws are; after those the kind is drawn.
// PostgreSQL is restarted at most maxPostgresRestarts times.
func runFaults(ctx context.Context, t *testing.T, c *Cluster, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 0xfa017))
	kinds := slices.Clone(allFaults)
	rng.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
	pending := slices.Clone(kinds)
	pgRestarts := 0
	between := func(lo, hi time.Duration) time.Duration { return lo + time.Duration(rng.Int64N(int64(hi-lo)+1)) }

	for {
		if !sleepCtx(ctx, between(3*time.Second, 8*time.Second)) {
			return
		}
		var kind string
		if len(pending) > 0 {
			kind = pending[0]
		} else {
			for kind = kinds[rng.IntN(len(kinds))]; kind == FaultRestartPostgres && pgRestarts >= maxPostgresRestarts; {
				kind = kinds[rng.IntN(len(kinds))]
			}
		}

		brokersUp, workersUp := c.Up()
		fired := true
		switch kind {
		case FaultKillBroker:
			if fired = len(brokersUp) > 0; fired {
				i := brokersUp[rng.IntN(len(brokersUp))]
				c.KillBroker(i)
				c.after(between(2*time.Second, 6*time.Second), func() { c.RestartBroker(i) })
			}
		case FaultKillWorker:
			if fired = len(workersUp) > 0; fired {
				i := workersUp[rng.IntN(len(workersUp))]
				c.KillWorker(i)
				c.after(between(2*time.Second, 6*time.Second), func() { c.RestartWorker(i) })
			}
		case FaultStallWorker:
			if fired = len(workersUp) > 0; fired {
				c.StallWorker(workersUp[rng.IntN(len(workersUp))], 2*leaseTTL)
			}
		case FaultPartitionBroker:
			c.Partition(BrokerProxy(rng.IntN(numBrokers)), between(2*time.Second, 10*time.Second))
		case FaultPartitionPG:
			c.Partition(pgProxy, between(2*time.Second, 10*time.Second))
		case FaultLatency:
			names := c.ProxyNames()
			c.Latency(names[rng.IntN(len(names))], 300*time.Millisecond, 200*time.Millisecond, 10*time.Second)
		case FaultRestartPostgres:
			c.RestartPostgres()
			pgRestarts++
		default:
			t.Fatalf("unknown fault kind %q", kind)
		}
		if !fired {
			continue
		}
		t.Logf("%s: fired %s", time.Now().Format("15:04:05.000"), kind)
		if len(pending) > 0 && pending[0] == kind {
			pending = pending[1:]
		}
	}
}

// sleepCtx waits for d and reports true, or returns false if ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// producerStats is what the producer did.
type producerStats struct {
	// confirmed is the number of enqueues the broker acknowledged.
	confirmed int
	// cancelled is the number of cancels the broker acknowledged, and
	// tooLate the number that found the job already finished.
	cancelled, tooLate int
	// unconfirmedEnqueues and unconfirmedCancels are those that were still
	// being retried when the producer's context ended.
	unconfirmedEnqueues, unconfirmedCancels int
	// failedCalls is the number of enqueue and cancel calls that failed and
	// were repeated by the test, on top of the SDK's own retries.
	failedCalls int
}

// runProducer enqueues total jobs over the time over, each with an
// idempotency key of its own, and cancels about cancelPercent of them once
// they are confirmed. It repeats every failed call, with the same key for an
// enqueue, until the call succeeds or ctx ends; and it records a confirmation
// in the history only after the call has returned success. A cancel that is
// not confirmed would make its job look like one that has to finish, so a
// cancel is repeated for as long as it can be.
func runProducer(ctx context.Context, t *testing.T, c *Cluster, seed uint64, total int, over time.Duration) producerStats {
	hist, err := os.Create(filepath.Join(c.HistoryDir(), "producer.jsonl"))
	if err != nil {
		t.Errorf("create the producer's history: %v", err)
		return producerStats{}
	}
	defer func() { _ = hist.Close() }()
	rec := history.NewRecorder(hist)
	record := func(e history.Event) {
		if err := rec.Record(e); err != nil {
			t.Errorf("record %s: %v", e.Kind, err)
		}
	}

	// The same static resolver as the workers': every call goes to a broker
	// through its proxy, round-robin.
	brokers := manual.NewBuilderWithScheme("chaos")
	var addrs []resolver.Address
	for _, a := range c.BrokerAddrs() {
		addrs = append(addrs, resolver.Address{Addr: a})
	}
	brokers.InitialState(resolver.State{Addresses: addrs})
	conn, err := jobqueue.Dial("chaos:///brokers", jobqueue.WithInsecure(), jobqueue.WithGRPCOptions(grpc.WithResolvers(brokers)))
	if err != nil {
		t.Errorf("dial the brokers: %v", err)
		return producerStats{}
	}
	defer func() { _ = conn.Close() }()
	p := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{
		Retry:          jobqueue.RetryPolicy{Attempts: 3, Base: 100 * time.Millisecond, Cap: time.Second},
		PerCallTimeout: 5 * time.Second,
	})

	// Which jobs are cancelled, and how long after their confirmation, are
	// drawn up front from the seed, on one goroutine. The delay is short
	// because a job is done within tens of milliseconds when nothing is
	// wrong, and a cancel that comes after that proves nothing.
	rng := rand.New(rand.NewPCG(seed, 0xc0ffee))
	cancelAfter := map[int]time.Duration{}
	for i := range total {
		if rng.IntN(100) < cancelPercent {
			cancelAfter[i] = time.Duration(rng.Int64N(int64(300 * time.Millisecond)))
		}
	}

	var (
		confirmed, cancelled, tooLate  atomic.Int64
		unconfirmedEnq, unconfirmedCnl atomic.Int64
		failedCalls                    atomic.Int64
		wg                             sync.WaitGroup
		sem                            = make(chan struct{}, 128)
	)
	one := func(i int) {
		key := fmt.Sprintf("chaos-%d-%06d", seed, i)
		var id int64
		for {
			res, err := p.Enqueue(ctx, jobqueue.EnqueueParams{Queue: queueName, Payload: []byte(key), IdempotencyKey: key})
			if err == nil {
				id = res.ID
				record(history.Event{At: time.Now(), Kind: history.EnqueueConfirmed, Key: key, ID: id})
				confirmed.Add(1)
				break
			}
			if errors.Is(err, jobqueue.ErrInvalid) {
				t.Errorf("enqueue %s was refused for good: %v", key, err)
				return
			}
			if failedCalls.Add(1) == 1 {
				t.Logf("the first failed enqueue (it will be repeated): %v", err)
			}
			if !sleepCtx(ctx, 200*time.Millisecond) {
				unconfirmedEnq.Add(1)
				return
			}
		}
		delay, cancelIt := cancelAfter[i]
		if !cancelIt {
			return
		}
		if !sleepCtx(ctx, delay) {
			unconfirmedCnl.Add(1)
			return
		}
		for {
			err := p.Cancel(ctx, id)
			if err == nil {
				record(history.Event{At: time.Now(), Kind: history.CancelConfirmed, ID: id})
				cancelled.Add(1)
				return
			}
			if status.Code(err) == codes.FailedPrecondition {
				// The job finished before the cancel got to it: it is not
				// cancelled, and it must count as an ordinary job.
				tooLate.Add(1)
				return
			}
			failedCalls.Add(1)
			if !sleepCtx(ctx, 200*time.Millisecond) {
				unconfirmedCnl.Add(1)
				return
			}
		}
	}

	begin := time.Now()
	interval := over / time.Duration(total)
schedule:
	for i := range total {
		if !sleepCtx(ctx, time.Until(begin.Add(time.Duration(i)*interval))) {
			unconfirmedEnq.Add(int64(total - i))
			break schedule
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			unconfirmedEnq.Add(int64(total - i))
			break schedule
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			one(i)
		}()
	}
	wg.Wait()
	return producerStats{
		confirmed: int(confirmed.Load()), cancelled: int(cancelled.Load()), tooLate: int(tooLate.Load()),
		unconfirmedEnqueues: int(unconfirmedEnq.Load()), unconfirmedCancels: int(unconfirmedCnl.Load()),
		failedCalls: int(failedCalls.Load()),
	}
}

// waitDrained polls the database until no job is ready or leased, or ctx
// ends, and returns the number of jobs in each state and whether all
// finished.
func waitDrained(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (map[string]int, bool) {
	var last map[string]int
	logged := time.Now()
	for {
		counts, err := stateCounts(ctx, pool)
		if err == nil {
			last = counts
			if counts["ready"]+counts["leased"] == 0 {
				return counts, true
			}
			if time.Since(logged) >= 10*time.Second {
				logged = time.Now()
				t.Logf("draining: %v", counts)
			}
		}
		if !sleepCtx(ctx, time.Second) {
			return last, false
		}
	}
}

// stateCounts returns the number of jobs in each state.
func stateCounts(ctx context.Context, pool *pgxpool.Pool) (map[string]int, error) {
	rows, err := pool.Query(ctx, "SELECT state, count(*) FROM jobqueue.jobs GROUP BY state")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		counts[state] = n
	}
	return counts, rows.Err()
}

// readFinalState reads the final state of every job and the whole sink table,
// and the number of jobs in each state.
func readFinalState(pool *pgxpool.Pool) ([]history.FinalJob, []history.SinkRow, map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	rows, err := pool.Query(ctx, "SELECT id, state, lease_epoch FROM jobqueue.jobs ORDER BY id")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read the jobs: %w", err)
	}
	var final []history.FinalJob
	states := map[string]int{}
	for rows.Next() {
		var f history.FinalJob
		var state string
		if err := rows.Scan(&f.ID, &state, &f.Epoch); err != nil {
			rows.Close()
			return nil, nil, nil, fmt.Errorf("scan a job: %w", err)
		}
		if f.State, err = job.ParseState(state); err != nil {
			rows.Close()
			return nil, nil, nil, err
		}
		states[state]++
		final = append(final, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("read the jobs: %w", err)
	}

	srows, err := pool.Query(ctx, "SELECT job_id, token FROM chaos_sink")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read the sink: %w", err)
	}
	defer srows.Close()
	var sink []history.SinkRow
	for srows.Next() {
		var s history.SinkRow
		if err := srows.Scan(&s.JobID, &s.Token); err != nil {
			return nil, nil, nil, fmt.Errorf("scan a sink row: %w", err)
		}
		sink = append(sink, s)
	}
	return final, sink, states, srows.Err()
}

// readHistory reads every history file of the run.
//
// A worker that was killed with SIGKILL may have died in the middle of a
// write, which leaves its file without a final newline and with half a line.
// Each start of a worker has a file of its own, so that can only be the last
// line of a file. That line is dropped, and counted, because the event it
// would have been was never seen by anything else. Nothing else is repaired:
// an unreadable line anywhere else, or a torn line in the file of a process
// that was not killed, fails the test, as does more torn lines than there
// were workers killed.
func readHistory(t *testing.T, c *Cluster) []history.Event {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(c.HistoryDir(), "*.jsonl"))
	if err != nil {
		t.Fatalf("list the history files: %v", err)
	}
	slices.Sort(paths)
	var (
		readers []io.Reader
		torn    []string
	)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if kept, dropped := dropTornTail(data); dropped > 0 {
			t.Logf("%s ends in a torn line of %d bytes, which a killed worker left; dropped it", filepath.Base(path), dropped)
			torn = append(torn, filepath.Base(path))
			data = kept
		}
		readers = append(readers, bytes.NewReader(data))
	}
	events, err := history.ReadAll(readers...)
	if err != nil {
		t.Fatalf("read the history: %v (files in order: %s)", err, strings.Join(baseNames(paths), ", "))
	}
	if kills := c.WorkerKills(); len(torn) > kills {
		t.Errorf("%d history files end in a torn line (%v) but only %d workers were killed", len(torn), torn, kills)
	}
	for _, name := range torn {
		if strings.HasPrefix(name, "producer") {
			t.Errorf("the producer's history %s has a torn line, but the test never kills the producer", name)
		}
	}
	return events
}

// TestDropTornTail pins what readHistory repairs: an unfinished last line
// goes, and nothing else does.
func TestDropTornTail(t *testing.T) {
	const whole = `{"at":"2026-10-31T00:00:00Z","kind":"handler_start","id":1}`
	for _, tc := range []struct {
		name        string
		in          string
		wantKept    string
		wantDropped int
	}{
		{"empty file", "", "", 0},
		{"ends in a newline", whole + "\n", whole + "\n", 0},
		{"one torn line", `{"at":"2026-10`, "", len(`{"at":"2026-10`)},
		{"torn line after whole lines", whole + "\n" + whole + "\n" + `{"at":`, whole + "\n" + whole + "\n", len(`{"at":`)},
		{"whole last line without a newline", whole, whole, 0},
		{"whole last line without a newline after others", whole + "\n" + whole, whole + "\n" + whole, 0},
		{"a torn line in the middle is not repaired", `{"at":` + "\n" + whole + "\n", `{"at":` + "\n" + whole + "\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kept, dropped := dropTornTail([]byte(tc.in))
			if string(kept) != tc.wantKept || dropped != tc.wantDropped {
				t.Errorf("dropTornTail(%q) = (%q, %d), want (%q, %d)", tc.in, kept, dropped, tc.wantKept, tc.wantDropped)
			}
		})
	}
}

// dropTornTail removes an unfinished last line from data and returns the
// rest and the size of what it removed. A last line without its newline that
// is a whole JSON value is kept: a reader can parse it.
func dropTornTail(data []byte) (kept []byte, dropped int) {
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return data, 0
	}
	cut := bytes.LastIndexByte(data, '\n') + 1
	if json.Valid(data[cut:]) {
		return data, 0
	}
	return data[:cut], len(data) - cut
}

func baseNames(paths []string) []string {
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = filepath.Base(p)
	}
	return out
}
