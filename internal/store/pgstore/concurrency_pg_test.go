//go:build pg

package pgstore_test

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/pgtest"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
)

// enqueueN enqueues n jobs in batches, job i for tenants[i % len(tenants)],
// and returns their ids.
func enqueueN(t *testing.T, s store.Store, queue string, tenants []string, n int) []int64 {
	t.Helper()
	var ids []int64
	for i := 0; i < n; {
		specs := make([]job.Spec, 0, job.MaxBatch)
		for len(specs) < job.MaxBatch && i < n {
			specs = append(specs, job.Spec{Queue: queue, Tenant: tenants[i%len(tenants)], Payload: []byte{byte(i)}})
			i++
		}
		res, err := s.Enqueue(t.Context(), specs)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		for _, r := range res {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

// snapshotByID returns every row of the store by id.
func snapshotByID(t *testing.T, s *pgstore.Store) map[int64]job.Job {
	t.Helper()
	rows, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	byID := make(map[int64]job.Job, len(rows))
	for _, j := range rows {
		byID[j.ID] = j
	}
	return byID
}

// TestPGConcurrentLeaseNoDoubleGrant leases 2000 jobs from 32 goroutines at
// once. Two statements that pick the same rows must not both lease them, so
// each job is granted exactly once.
func TestPGConcurrentLeaseNoDoubleGrant(t *testing.T) {
	const (
		jobs    = 2000
		workers = 32
	)
	tenants := []string{"t0", "t1", "t2", "t3"}
	_, pg := newConcurrentStore(t, workers)
	enqueueN(t, pg, "q", tenants, jobs)

	var (
		mu     sync.Mutex
		grants = map[int64]int{}
		wg     sync.WaitGroup
	)
	for w := range workers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			worker := fmt.Sprintf("w%d", w)
			// Each pass takes from one to three distinct tenants. A worker
			// stops after 50 passes in a row that find nothing.
			for empty := 0; empty < 50; {
				var takes []store.TenantTake
				for _, k := range rng.Perm(len(tenants))[:1+rng.IntN(3)] {
					takes = append(takes, store.TenantTake{Tenant: tenants[k], Limit: 16})
				}
				got, err := pg.Lease(t.Context(), store.LeaseRequest{Queue: "q", Takes: takes, TTL: 10 * time.Minute, Worker: worker})
				if err != nil {
					t.Errorf("worker %s: Lease: %v", worker, err)
					return
				}
				if len(got) == 0 {
					empty++
					continue
				}
				empty = 0
				mu.Lock()
				for _, g := range got {
					grants[g.Job.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	for id, n := range grants {
		if n != 1 {
			t.Errorf("job %d was granted %d times", id, n)
		}
	}
	if len(grants) != jobs {
		t.Errorf("%d distinct jobs were granted, want %d", len(grants), jobs)
	}
	for _, j := range snapshotByID(t, pg) {
		if j.State != job.StateLeased || j.Epoch != 1 || j.Attempts != 1 {
			t.Errorf("job %d is %s with epoch %d and %d attempts, want leased with epoch 1 and 1 attempt", j.ID, j.State, j.Epoch, j.Attempts)
		}
	}
}

// TestPGConcurrentEnqueueSameKey enqueues one idempotency key from 32
// goroutines released together. Every call must succeed with the one job, and
// exactly one must have created it. A check-then-insert is caught only when
// some caller reads before the first one commits, which a busy machine can
// prevent in one round, so the test runs ten rounds, each on a new key, and
// every round must hold.
func TestPGConcurrentEnqueueSameKey(t *testing.T) {
	const (
		callers = 32
		rounds  = 10
	)
	pool, pg := newConcurrentStore(t, callers)

	for round := range rounds {
		var (
			key     = fmt.Sprintf("k%d", round)
			start   = make(chan struct{})
			results = make([]store.EnqueueResult, callers)
			errs    = make([]error, callers)
			wg      sync.WaitGroup
		)
		for i := range callers {
			wg.Go(func() {
				<-start
				res, err := pg.Enqueue(t.Context(), []job.Spec{{Queue: "q", Tenant: "a", IdempotencyKey: key, Payload: []byte("p")}})
				if err == nil {
					results[i] = res[0]
				}
				errs[i] = err
			})
		}
		close(start)
		wg.Wait()

		created := 0
		for i, err := range errs {
			if err != nil {
				t.Errorf("key %s, caller %d: %v", key, i, err)
				continue
			}
			if results[i].ID != results[0].ID {
				t.Errorf("key %s: caller %d got job %d, caller 0 got job %d", key, i, results[i].ID, results[0].ID)
			}
			if !results[i].Deduplicated {
				created++
			}
		}
		if created != 1 {
			t.Errorf("key %s: %d callers created the job, want exactly 1", key, created)
		}
	}

	var n int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobqueue.jobs").Scan(&n); err != nil {
		t.Fatalf("count the jobs: %v", err)
	}
	if n != rounds {
		t.Errorf("the table holds %d jobs, want %d, one for each key", n, rounds)
	}
}

// TestPGConcurrentAckRacesReap lets 500 leases run out, then completes the
// jobs from 16 goroutines while 4 more reap. A job is either completed or
// taken back, never both and never neither: whichever statement gets a job
// first wins, and the loser must see that.
func TestPGConcurrentAckRacesReap(t *testing.T) {
	const (
		jobs       = 500
		completers = 16
		reapers    = 4
		chunk      = 4 // refs per Complete call; small, so that many calls race
	)
	pool, pg := newConcurrentStore(t, completers+reapers)
	ids := enqueueN(t, pg, "q", []string{"a", "b"}, jobs)

	refs := make([]store.Ref, 0, jobs)
	for len(refs) < jobs {
		grants, err := pg.Lease(t.Context(), store.LeaseRequest{
			Queue: "q", Takes: []store.TenantTake{{Limit: 50}}, TTL: 5 * time.Second, Worker: "w",
		})
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if len(grants) == 0 {
			t.Fatalf("leased %d of %d jobs", len(refs), jobs)
		}
		for _, g := range grants {
			refs = append(refs, store.Ref{ID: g.Job.ID, Queue: g.Job.Queue, Token: g.Job.Epoch})
		}
	}
	if err := pgtest.AdvanceClock(t.Context(), pool, 6*time.Second); err != nil {
		t.Fatal(err)
	}

	var (
		start    = make(chan struct{})
		outcomes = make(map[int64]job.Outcome, jobs)
		mu       sync.Mutex
		reaped   atomic.Int64
		wg       sync.WaitGroup
		reaperWG sync.WaitGroup
		stop     = make(chan struct{})
	)
	backoff := func(int32) time.Duration { return time.Second }
	for r := range reapers {
		reaperWG.Go(func() {
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				n, err := pg.Reap(t.Context(), 50, backoff)
				if err != nil {
					t.Errorf("reaper %d: Reap: %v", r, err)
					return
				}
				reaped.Add(int64(n))
			}
		})
	}
	per := jobs / completers
	for c := range completers {
		mine := refs[c*per : min((c+1)*per, jobs)]
		if c == completers-1 {
			mine = refs[c*per:]
		}
		wg.Go(func() {
			<-start
			for len(mine) > 0 {
				part := mine[:min(chunk, len(mine))]
				mine = mine[len(part):]
				reqs := make([]store.CompleteRequest, len(part))
				for i, ref := range part {
					reqs[i] = store.CompleteRequest{Ref: ref, Result: []byte("done")}
				}
				out, err := pg.Complete(t.Context(), reqs)
				if err != nil {
					t.Errorf("completer %d: Complete: %v", c, err)
					return
				}
				mu.Lock()
				for i, ref := range part {
					outcomes[ref.ID] = out[i]
				}
				mu.Unlock()
			}
		})
	}
	close(start)
	wg.Wait()
	close(stop)
	reaperWG.Wait()
	// What the reapers left behind, because a row that a Complete held was
	// skipped, or because they stopped first.
	for {
		n, err := pg.Reap(t.Context(), 50, backoff)
		if err != nil {
			t.Fatalf("final Reap: %v", err)
		}
		if n == 0 {
			break
		}
		reaped.Add(int64(n))
	}

	applied := 0
	rows := snapshotByID(t, pg)
	for _, id := range ids {
		j, ok := rows[id]
		if !ok {
			t.Errorf("job %d is gone", id)
			continue
		}
		switch out := outcomes[id]; out {
		case job.Applied:
			applied++
			if j.State != job.StateSucceeded || string(j.Result) != "done" {
				t.Errorf("job %d: Complete applied, but the job is %s with result %q", id, j.State, j.Result)
			}
		case job.Fenced:
			if j.State != job.StateReady || j.Attempts != 1 || j.LastError != "lease expired" || j.Epoch != 1 {
				t.Errorf("job %d: Complete was fenced, but the job is %s with %d attempts, epoch %d and error %q, "+
					"want ready with 1 attempt, epoch 1 and \"lease expired\"", id, j.State, j.Attempts, j.Epoch, j.LastError)
			}
		case job.Duplicate, job.Expired, job.NotFound, job.WrongState:
			fallthrough
		default: // also no outcome at all, because the completer gave up
			t.Errorf("job %d: Complete returned %s, want applied or fenced", id, out)
		}
	}
	// The test races two sides. If the completers took every job, or the
	// reapers did, nothing was raced and the checks above held vacuously.
	if applied == 0 || reaped.Load() == 0 {
		t.Errorf("%d completions applied and %d jobs reaped, want some of each: the two sides did not race", applied, reaped.Load())
	}
	if total := int64(applied) + reaped.Load(); total != jobs {
		t.Errorf("%d completions applied and %d jobs reaped make %d, want %d", applied, reaped.Load(), total, jobs)
	}
	t.Logf("%d of %d completions beat the reapers", applied, jobs)
}

// convLog records what the store reported to the goroutines of
// TestPGConcurrentConservation.
type convLog struct {
	mu     sync.Mutex
	ids    []int64        // every id an Enqueue returned, new or deduplicated, once
	known  map[int64]bool // the same, for lookup
	grants map[int64]int  // leases granted per job
}

func (l *convLog) enqueued(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.known[id] {
		l.known[id] = true
		l.ids = append(l.ids, id)
	}
}

func (l *convLog) granted(id int64) {
	l.mu.Lock()
	l.grants[id]++
	l.mu.Unlock()
}

// some returns a random id that an Enqueue has returned, or false if none has.
func (l *convLog) some(rng *rand.Rand) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ids) == 0 {
		return 0, false
	}
	return l.ids[rng.IntN(len(l.ids))], true
}

// conserver is one goroutine of TestPGConcurrentConservation. It acknowledges
// only leases it holds, so most of its tokens are current; the ones that other
// goroutines have since made stale (a reaped lease, a cancelled job) are the
// fenced calls.
type conserver struct {
	id   int
	rng  *rand.Rand
	pg   *pgstore.Store
	pool *pgxpool.Pool
	log  *convLog
	held []store.Ref
}

// oneOf returns a random element of s.
func oneOf[T any](rng *rand.Rand, s ...T) T { return s[rng.IntN(len(s))] }

// pick returns up to n of the held refs, chosen at random, and keeps them.
func (c *conserver) pick(n int) []store.Ref {
	var out []store.Ref
	for _, i := range c.rng.Perm(len(c.held))[:min(n, len(c.held))] {
		out = append(out, c.held[i])
	}
	return out
}

// spend returns up to n of the held refs, chosen at random, for a call that
// ends the lease. A ref stays held with probability 1/4, so that some tokens
// are used again once the lease is gone.
func (c *conserver) spend(n int) []store.Ref {
	var out []store.Ref
	for range n {
		if len(c.held) == 0 {
			break
		}
		i := c.rng.IntN(len(c.held))
		out = append(out, c.held[i])
		if c.rng.IntN(4) != 0 {
			c.held = slices.Delete(c.held, i, i+1)
		}
	}
	return out
}

// step does one random operation. It returns an error for a call that fails
// and for an outcome the operation cannot have.
func (c *conserver) step(ctx context.Context) error {
	switch r := c.rng.IntN(100); {
	case r < 28:
		return c.enqueue(ctx)
	case r < 48:
		return c.lease(ctx)
	case r < 56:
		return c.heartbeat(ctx)
	case r < 70:
		return c.complete(ctx)
	case r < 80:
		return c.fail(ctx)
	case r < 85:
		return c.release(ctx)
	case r < 90:
		return c.cancel(ctx)
	case r < 98:
		return c.reap(ctx)
	default:
		// The clock moves, so that leases run out and delays pass.
		return pgtest.AdvanceClock(ctx, c.pool, time.Second)
	}
}

func (c *conserver) enqueue(ctx context.Context) error {
	specs := make([]job.Spec, 1+c.rng.IntN(3))
	for i := range specs {
		specs[i] = job.Spec{
			Queue:          oneOf(c.rng, "q1", "q2"),
			Tenant:         oneOf(c.rng, "a", "b", "c"),
			Payload:        []byte{byte(c.id)},
			Delay:          oneOf(c.rng, 0, 0, time.Second),
			MaxAttempts:    oneOf[int32](c.rng, 0, 1, 2, 3),
			IdempotencyKey: oneOf(c.rng, "", "", "k0", "k1", "k2", "k3", "k4", "k5"),
		}
	}
	// A batch holds a key from its first insert to its commit, so two batches
	// that take the same keys in opposite orders wait for each other, and
	// PostgreSQL aborts one of them with a deadlock error, which the store
	// reports as store.ErrUnavailable (TestPGOppositeOrderEnqueuesDeadlockAsRetryable
	// covers that). This test is about what holds after the calls, so it keeps
	// its keys in one order and no call is aborted.
	slices.SortFunc(specs, func(a, b job.Spec) int {
		return cmp.Or(cmp.Compare(a.Tenant, b.Tenant), cmp.Compare(a.Queue, b.Queue), cmp.Compare(a.IdempotencyKey, b.IdempotencyKey))
	})
	res, err := c.pg.Enqueue(ctx, specs)
	if err != nil {
		return fmt.Errorf("Enqueue: %w", err)
	}
	for _, r := range res {
		c.log.enqueued(r.ID)
	}
	return nil
}

func (c *conserver) lease(ctx context.Context) error {
	req := store.LeaseRequest{
		Queue:  oneOf(c.rng, "q1", "q2"),
		TTL:    oneOf(c.rng, time.Second, 2*time.Second, 5*time.Second),
		Worker: fmt.Sprintf("w%d", c.id),
	}
	if c.rng.IntN(2) == 0 {
		req.Takes = []store.TenantTake{{Limit: 1 + c.rng.IntN(5)}}
	} else {
		for range 1 + c.rng.IntN(2) {
			req.Takes = append(req.Takes, store.TenantTake{Tenant: oneOf(c.rng, "a", "b", "c"), Limit: 1 + c.rng.IntN(5)})
		}
	}
	grants, err := c.pg.Lease(ctx, req)
	if err != nil {
		return fmt.Errorf("Lease: %w", err)
	}
	for _, g := range grants {
		c.log.granted(g.Job.ID)
		c.held = append(c.held, store.Ref{ID: g.Job.ID, Queue: g.Job.Queue, Token: g.Job.Epoch})
	}
	return nil
}

// wantOutcome returns an error unless every outcome is one of allowed.
func wantOutcome(op string, got []job.Outcome, allowed ...job.Outcome) error {
	for _, o := range got {
		if !slices.Contains(allowed, o) {
			return fmt.Errorf("%s returned %s, which it cannot (allowed: %v)", op, o, allowed)
		}
	}
	return nil
}

func (c *conserver) heartbeat(ctx context.Context) error {
	refs := c.pick(1 + c.rng.IntN(3))
	if len(refs) == 0 {
		return nil
	}
	res, err := c.pg.Heartbeat(ctx, refs, oneOf(c.rng, time.Second, 2*time.Second, 5*time.Second))
	if err != nil {
		return fmt.Errorf("Heartbeat: %w", err)
	}
	out := make([]job.Outcome, len(res))
	for i, r := range res {
		out[i] = r.Outcome
	}
	return wantOutcome("Heartbeat", out, job.Applied, job.Fenced, job.Expired)
}

func (c *conserver) complete(ctx context.Context) error {
	refs := c.spend(1 + c.rng.IntN(3))
	if len(refs) == 0 {
		return nil
	}
	reqs := make([]store.CompleteRequest, len(refs))
	for i, r := range refs {
		reqs[i] = store.CompleteRequest{Ref: r, Result: []byte("ok")[:c.rng.IntN(3)]}
	}
	out, err := c.pg.Complete(ctx, reqs)
	if err != nil {
		return fmt.Errorf("Complete: %w", err)
	}
	return wantOutcome("Complete", out, job.Applied, job.Duplicate, job.Fenced)
}

func (c *conserver) fail(ctx context.Context) error {
	refs := c.spend(1 + c.rng.IntN(3))
	if len(refs) == 0 {
		return nil
	}
	reqs := make([]store.FailRequest, len(refs))
	for i, r := range refs {
		reqs[i] = store.FailRequest{Ref: r, Permanent: c.rng.IntN(8) == 0, Delay: oneOf(c.rng, 0, time.Second), Error: "boom"}
	}
	out, err := c.pg.Fail(ctx, reqs)
	if err != nil {
		return fmt.Errorf("Fail: %w", err)
	}
	return wantOutcome("Fail", out, job.Applied, job.Fenced)
}

func (c *conserver) release(ctx context.Context) error {
	refs := c.spend(1 + c.rng.IntN(3))
	if len(refs) == 0 {
		return nil
	}
	out, err := c.pg.Release(ctx, refs)
	if err != nil {
		return fmt.Errorf("Release: %w", err)
	}
	return wantOutcome("Release", out, job.Applied, job.Fenced)
}

func (c *conserver) cancel(ctx context.Context) error {
	id, ok := c.log.some(c.rng)
	if !ok {
		return nil
	}
	out, err := c.pg.Cancel(ctx, id, "")
	if err != nil {
		return fmt.Errorf("Cancel: %w", err)
	}
	return wantOutcome("Cancel", []job.Outcome{out}, job.Applied, job.Duplicate, job.WrongState)
}

func (c *conserver) reap(ctx context.Context) error {
	if _, err := c.pg.Reap(ctx, 1+c.rng.IntN(20), func(int32) time.Duration { return time.Second }); err != nil {
		return fmt.Errorf("Reap: %w", err)
	}
	return nil
}

// TestPGConcurrentConservation runs 16 goroutines of random operations for
// three seconds and then checks what no interleaving may change: every job an
// Enqueue returned exists and no other does, a job's lease epoch is the number
// of leases it was granted, and no job has used more attempts than it has.
//
// Unlike the other concurrency tests this one moves the clock, by a second at
// a time, so that leases expire and the reaper has work to race with.
func TestPGConcurrentConservation(t *testing.T) {
	const (
		workers  = 16
		duration = 3 * time.Second
	)
	pool, pg := newConcurrentStore(t, workers)
	log := &convLog{known: map[int64]bool{}, grants: map[int64]int{}}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for i := range workers {
		wg.Go(func() {
			c := &conserver{id: i, rng: rand.New(rand.NewPCG(uint64(i), 42)), pg: pg, pool: pool, log: log}
			for time.Now().Before(deadline) && ctx.Err() == nil {
				if err := c.step(ctx); err != nil && ctx.Err() == nil {
					t.Errorf("goroutine %d: %v", i, err)
					cancel() // one failure is enough, and the others would only repeat it
				}
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	rows := snapshotByID(t, pg)
	if len(rows) != len(log.ids) {
		t.Errorf("the table holds %d jobs, the enqueue calls returned %d distinct ids", len(rows), len(log.ids))
	}
	for _, id := range log.ids {
		if _, ok := rows[id]; !ok {
			t.Errorf("job %d was returned by Enqueue and is not in the table", id)
		}
	}
	leased := 0
	for id, j := range rows {
		if !log.known[id] {
			t.Errorf("job %d is in the table and no Enqueue returned it", id)
		}
		if got := log.grants[id]; j.Epoch != int64(got) {
			t.Errorf("job %d has lease epoch %d but was granted %d leases", id, j.Epoch, got)
		}
		if j.Attempts > j.MaxAttempts {
			t.Errorf("job %d has %d attempts of at most %d", id, j.Attempts, j.MaxAttempts)
		}
		leased += int(j.Epoch)
	}
	// If the goroutines did nothing, every check above held vacuously.
	if len(rows) == 0 || leased == 0 {
		t.Errorf("%d jobs and %d leases, want some of each: the goroutines did no work to check", len(rows), leased)
	}
	t.Logf("%d jobs, %d leases", len(rows), leased)
}
