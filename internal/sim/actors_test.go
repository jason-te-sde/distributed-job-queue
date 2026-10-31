package sim_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
	"github.com/jason-te-sde/distributed-job-queue/internal/sim"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

const (
	testTTL    = 30 * time.Second
	testMargin = 3 * time.Second
	testQueue  = "q"
)

// rig is a small simulated cluster for one scenario: one store, any number of
// brokers and workers, and a record of every row change the store made.
type rig struct {
	t    *testing.T
	w    *sim.World
	net  *sim.Network
	st   *sim.StoreNode
	sink *sim.Sink

	brokers []*sim.BrokerNode
	workers []*sim.WorkerNode
	ids     []sim.NodeID // broker ids, in creation order

	changes   []memstore.Change
	hooks     []func(memstore.Change)
	completed int // OpComplete changes so far
}

// newRig builds a cluster over a quiet network with 1ms to 3ms latency, and
// prints the seed if the test fails.
func newRig(t *testing.T, seed uint64) *rig {
	t.Helper()
	return newRigNet(t, seed, sim.NetConfig{Latency: [2]time.Duration{time.Millisecond, 3 * time.Millisecond}})
}

func newRigNet(t *testing.T, seed uint64, cfg sim.NetConfig) *rig {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed %d", seed)
		}
	})
	w := sim.NewWorld(seed)
	r := &rig{t: t, w: w, net: sim.NewNetwork(w, cfg), sink: sim.NewSink()}
	r.st = sim.NewStoreNode(w, r.observe)
	return r
}

// observe is the store observer: it records the change, then runs the hooks.
func (r *rig) observe(ch memstore.Change) {
	r.changes = append(r.changes, ch)
	if ch.Op == memstore.OpComplete {
		r.completed++
	}
	for _, h := range r.hooks {
		h(ch)
	}
}

// onChange registers a hook that sees every later change.
func (r *rig) onChange(h func(memstore.Change)) { r.hooks = append(r.hooks, h) }

// addBroker adds a broker with the given id; mod may adjust its config.
func (r *rig) addBroker(id string, mod func(*sim.BrokerConfig)) *sim.BrokerNode {
	cfg := sim.BrokerConfig{
		ID:               sim.NodeID(id),
		Clock:            sim.Clock{Rate: 1},
		Scheduler:        "drr",
		PollInterval:     500 * time.Millisecond,
		ReapInterval:     time.Second,
		PruneInterval:    time.Minute,
		ActivityInterval: 500 * time.Millisecond,
		Retention:        store.Retention{Done: 24 * time.Hour, Dead: 336 * time.Hour},
	}
	if mod != nil {
		mod(&cfg)
	}
	b := sim.NewBrokerNode(r.w, r.net, r.st, cfg)
	r.brokers = append(r.brokers, b)
	r.ids = append(r.ids, cfg.ID)
	return b
}

// addWorker adds a worker that serves testQueue through every broker added so
// far; mod may adjust its config. Handlers take 100ms to 300ms and succeed.
func (r *rig) addWorker(id string, mod func(*sim.WorkerConfig)) *sim.WorkerNode {
	cfg := sim.WorkerConfig{
		ID:    sim.NodeID(id),
		Queue: testQueue,
		Clock: sim.Clock{Rate: 1},
		Lease: lease.Config{
			Concurrency: 4,
			TTL:         testTTL,
			Margin:      testMargin,
			LongPoll:    2 * time.Second,
		},
		Brokers:      slices.Clone(r.ids),
		CooperativeP: 1,
		Handler:      sim.HandlerModel{Duration: [2]time.Duration{100 * time.Millisecond, 300 * time.Millisecond}},
	}
	if mod != nil {
		mod(&cfg)
	}
	wk := sim.NewWorkerNode(r.w, r.net, r.sink, cfg, uint64(len(r.workers))+1000)
	r.workers = append(r.workers, wk)
	return wk
}

// producer adds a producer sending jobs through every broker.
func (r *rig) producer(id string, mod func(*sim.ProducerConfig), onConfirm func(string, int64), onCancel func(int64)) *sim.Producer {
	cfg := sim.ProducerConfig{
		ID:      sim.NodeID(id),
		Brokers: slices.Clone(r.ids),
		Jobs:    1,
		Over:    100 * time.Millisecond,
		Tenants: []string{"t1"},
		Queues:  []string{testQueue},
	}
	if mod != nil {
		mod(&cfg)
	}
	return sim.NewProducer(r.w, r.net, cfg, onConfirm, onCancel)
}

// runUntil fires events until cond holds or virtual time reaches limit, and
// reports whether cond holds.
func (r *rig) runUntil(limit time.Duration, cond func() bool) bool {
	for !cond() {
		if r.w.Now >= limit || !r.w.Step() {
			return false
		}
	}
	return true
}

// runFor fires events until d of virtual time has passed.
func (r *rig) runFor(d time.Duration) {
	end := r.w.Now + d
	for r.w.Now < end && r.w.Step() {
	}
}

// snapshot returns every row of the store.
func (r *rig) snapshot() []job.Job {
	r.t.Helper()
	jobs, err := r.st.Mem().Snapshot(context.Background())
	if err != nil {
		r.t.Fatalf("snapshot: %v", err)
	}
	return jobs
}

// changesOf returns the recorded changes of one operation.
func (r *rig) changesOf(op memstore.Op) []memstore.Change {
	var out []memstore.Change
	for _, ch := range r.changes {
		if ch.Op == op {
			out = append(out, ch)
		}
	}
	return out
}

// requireState fails unless every row is in want, and returns the rows.
func (r *rig) requireState(want job.State) []job.Job {
	r.t.Helper()
	jobs := r.snapshot()
	for _, j := range jobs {
		if j.State != want {
			r.t.Errorf("job %d is %v after %v, want %v (epoch %d, attempts %d, error %q)",
				j.ID, j.State, r.w.Now, want, j.Epoch, j.Attempts, j.LastError)
		}
	}
	return jobs
}

func TestHappyPathAllJobsSucceed(t *testing.T) {
	r := newRig(t, 11)
	r.addBroker("b1", nil)
	r.addWorker("w1", nil)
	const n = 20
	confirmed := map[string]int64{}
	p := r.producer("p1", func(c *sim.ProducerConfig) { c.Jobs, c.Over = n, time.Second },
		func(key string, id int64) { confirmed[key] = id }, nil)

	if !r.runUntil(2*time.Minute, func() bool { return r.completed == n }) {
		t.Fatalf("%d of %d jobs completed by %v", r.completed, n, r.w.Now)
	}
	jobs := r.requireState(job.StateSucceeded)
	if len(jobs) != n {
		t.Fatalf("%d rows, want %d", len(jobs), n)
	}
	for _, j := range jobs {
		last, ok := r.sink.Last(j.ID)
		if !ok || last != j.Epoch {
			t.Errorf("job %d: sink last token = %d (written %v), want the job's epoch %d", j.ID, last, ok, j.Epoch)
		}
	}
	if len(confirmed) != n || p.Confirmed() != n {
		t.Errorf("confirmed %d keys, producer says %d, want %d", len(confirmed), p.Confirmed(), n)
	}
	if rej := r.sink.Rejections(); rej != 0 {
		t.Errorf("%d sink rejections in a fault-free run", rej)
	}
}

func TestStalledWorkerIsFencedAndJobRedelivered(t *testing.T) {
	// Two workers that never cooperate with cancellation share one job. The
	// one that leases it is stalled for twice the TTL just after the handler
	// starts. Its lease expires, the reaper gives the job to the other worker
	// and, when the stalled one wakes up, its late handler is fenced.
	for _, tc := range []struct {
		name string
		// handler is how long every handler runs.
		handler time.Duration
		// rejected says that the stalled worker's sink write comes after
		// the other worker's, so that the sink rejects it. Otherwise it
		// comes first, the sink takes it, and the store fences the Complete.
		rejected bool
	}{
		{"sink rejects the late write", time.Second, true},
		{"store fences the late Complete", 40 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, 21)
			r.addBroker("b1", nil)
			byID := map[string]*sim.WorkerNode{}
			for _, id := range []string{"wa", "wb"} {
				byID[id] = r.addWorker(id, func(c *sim.WorkerConfig) {
					c.Lease.Concurrency = 1
					c.CooperativeP = 0
					c.Handler = sim.HandlerModel{Duration: [2]time.Duration{tc.handler, tc.handler}}
				})
			}
			stalled := false
			r.onChange(func(ch memstore.Change) {
				if ch.Op != memstore.OpLease || stalled {
					return
				}
				stalled = true
				holder := byID[ch.After.LeasedBy]
				r.w.After(100*time.Millisecond, "stall holder", func() { holder.Stall(2 * testTTL) })
			})
			r.producer("p1", nil, nil, nil)

			if !r.runUntil(5*time.Minute, func() bool { return r.completed == 1 }) {
				t.Fatalf("job did not complete by %v", r.w.Now)
			}
			// Let the stalled worker wake up and send its late ack.
			r.runFor(2 * testTTL)

			jobs := r.requireState(job.StateSucceeded)
			if len(jobs) != 1 || jobs[0].Epoch != 2 {
				t.Fatalf("rows = %+v, want one job redelivered once (epoch 2)", jobs)
			}
			if last, _ := r.sink.Last(jobs[0].ID); last != 2 {
				t.Errorf("sink last token = %d, want 2, the redelivery's", last)
			}
			cs := r.changesOf(memstore.OpComplete)
			if len(cs) != 1 {
				t.Errorf("%d Complete changes, want exactly one", len(cs))
			} else if cs[0].Token != 2 {
				t.Errorf("the Complete carries token %d, want 2", cs[0].Token)
			}
			if got := len(r.changesOf(memstore.OpReap)); got != 1 {
				t.Errorf("%d reaps, want 1", got)
			}
			if got := r.brokers[0].Stats().FencedAcks; got < 1 {
				t.Errorf("FencedAcks = %d, want the stalled worker's late ack fenced", got)
			}
			wantRej := 0
			if tc.rejected {
				wantRej = 1
			}
			if got := r.sink.Rejections(); got != wantRej {
				t.Errorf("sink rejections = %d, want %d", got, wantRej)
			}
		})
	}
}

func TestBrokerCrashMidFetchLeavesLeaseToExpire(t *testing.T) {
	r := newRig(t, 31)
	b := r.addBroker("b1", nil)
	r.addWorker("w1", func(c *sim.WorkerConfig) { c.Lease.Concurrency = 1 })
	crashed := false
	r.onChange(func(ch memstore.Change) {
		if ch.Op != memstore.OpLease || crashed {
			return
		}
		// The lease is in the store and the broker has not yet answered.
		crashed = true
		b.Crash()
		r.w.After(100*time.Millisecond, "restart broker", b.Restart)
	})
	r.producer("p1", nil, nil, nil)

	if !r.runUntil(5*time.Minute, func() bool { return r.completed == 1 }) {
		t.Fatalf("job did not complete by %v", r.w.Now)
	}
	jobs := r.requireState(job.StateSucceeded)
	if len(jobs) != 1 || jobs[0].Epoch != 2 {
		t.Fatalf("rows = %+v, want one job leased twice (epoch 2)", jobs)
	}
	leases, reaps := r.changesOf(memstore.OpLease), r.changesOf(memstore.OpReap)
	if len(leases) != 2 || len(reaps) != 1 {
		t.Fatalf("%d leases and %d reaps, want 2 and 1", len(leases), len(reaps))
	}
	expiry := leases[0].After.LeaseExpiresAt
	if reaps[0].Now.Before(expiry) {
		t.Errorf("reaped at %v, before the lease expired at %v", reaps[0].Now, expiry)
	}
	if late := reaps[0].Now.Sub(expiry); late > time.Second+10*time.Millisecond {
		t.Errorf("reaped %v after expiry, want within the reap interval (1s)", late)
	}
}

func TestStoreNodeCallSchedulesAndFansOut(t *testing.T) {
	w := sim.NewWorld(41)
	var seen []memstore.Op
	var inObserver *sim.StoreNode
	// The observer reads the store while the store is committing. That works
	// only if the fan-out runs after the store has released its lock.
	st := sim.NewStoreNode(w, func(ch memstore.Change) {
		seen = append(seen, ch.Op)
		if _, err := inObserver.Mem().Snapshot(context.Background()); err != nil {
			t.Errorf("snapshot inside observer: %v", err)
		}
	})
	inObserver = st

	var ranAt, doneAt time.Duration
	var results []store.EnqueueResult
	st.Call("enqueue", func(s store.Store) (err error) {
		ranAt = w.Now
		results, err = s.Enqueue(context.Background(), []job.Spec{{Queue: "q", Tenant: "t", IdempotencyKey: "k"}})
		return err
	}, func(err error) {
		doneAt = w.Now
		if err != nil {
			t.Errorf("done err = %v", err)
		}
	})
	for w.Step() {
	}
	if ranAt < 100*time.Microsecond || ranAt > 2*time.Millisecond {
		t.Errorf("fn ran at %v, want within [100µs, 2ms] of the call", ranAt)
	}
	if d := doneAt - ranAt; d < 100*time.Microsecond || d > 2*time.Millisecond {
		t.Errorf("done came %v after fn, want within [100µs, 2ms]", d)
	}
	if len(results) != 1 || !slices.Equal(seen, []memstore.Op{memstore.OpEnqueue}) {
		t.Errorf("results = %v, observed ops = %v, want one insert seen once", results, seen)
	}
}

func TestStoreOutageAnswersUnavailableAndRecovers(t *testing.T) {
	t.Run("call", func(t *testing.T) {
		w := sim.NewWorld(51)
		st := sim.NewStoreNode(w)
		st.SetAvailable(false)
		ran := false
		var got error
		st.Call("enqueue", func(s store.Store) error {
			ran = true
			return nil
		}, func(err error) { got = err })
		for w.Step() {
		}
		if ran || !errors.Is(got, store.ErrUnavailable) {
			t.Fatalf("while unavailable: fn ran = %v, done err = %v, want fn skipped and ErrUnavailable", ran, got)
		}

		st.SetAvailable(true)
		got = errors.New("unset")
		st.Call("enqueue", func(s store.Store) error {
			ran = true
			return nil
		}, func(err error) { got = err })
		for w.Step() {
		}
		if !ran || got != nil {
			t.Fatalf("after recovery: fn ran = %v, done err = %v, want fn run and nil", ran, got)
		}
	})

	t.Run("cluster", func(t *testing.T) {
		r := newRig(t, 52)
		b := r.addBroker("b1", nil)
		r.addWorker("w1", nil)
		const n = 12
		confirmed := map[int64]bool{}
		r.producer("p1", func(c *sim.ProducerConfig) { c.Jobs, c.Over = n, 4*time.Second },
			func(_ string, id int64) { confirmed[id] = true }, nil)
		r.w.At(time.Second, "outage starts", func() { r.st.SetAvailable(false) })
		r.w.At(8*time.Second, "outage ends", func() { r.st.SetAvailable(true) })

		if !r.runUntil(3*time.Minute, func() bool { return r.completed == n }) {
			t.Fatalf("%d of %d jobs completed by %v", r.completed, n, r.w.Now)
		}
		if len(r.requireState(job.StateSucceeded)) != n || len(confirmed) != n {
			t.Errorf("rows or confirmations lost: %d distinct ids confirmed, want %d", len(confirmed), n)
		}
		if got := b.Stats().Unavailable; got == 0 {
			t.Error("the broker answered no store call as unavailable during a 7s outage")
		}
	})
}

func TestProducerRetryDeduplicates(t *testing.T) {
	r := newRig(t, 61)
	r.addBroker("b1", nil)
	// Cut the producer off from the broker as the store commits the first
	// Enqueue, so that the reply is lost.
	dropped := false
	r.onChange(func(ch memstore.Change) {
		if ch.Op == memstore.OpEnqueue && !dropped {
			dropped = true
			r.net.Partition("p1", "b1", r.w.Now+time.Second)
		}
	})
	var ids []int64
	p := r.producer("p1", nil, func(_ string, id int64) { ids = append(ids, id) }, nil)

	if !r.runUntil(time.Minute, func() bool { return p.Confirmed() == 1 }) {
		t.Fatalf("the producer never got a confirmation by %v", r.w.Now)
	}
	r.runFor(10 * time.Second)

	if got := r.net.Stats().Partitioned; got == 0 {
		t.Fatal("no message was lost to the partition, so the retry path was not exercised")
	}
	jobs := r.snapshot()
	if len(jobs) != 1 {
		t.Fatalf("%d rows, want exactly one", len(jobs))
	}
	if got := len(r.changesOf(memstore.OpEnqueue)); got != 1 {
		t.Errorf("%d inserts, want 1", got)
	}
	if len(ids) == 0 || ids[0] != jobs[0].ID {
		t.Errorf("confirmed ids = %v, want the row's id %d", ids, jobs[0].ID)
	}
	for _, id := range ids {
		if id != jobs[0].ID {
			t.Errorf("a retry was confirmed with id %d, want %d", id, jobs[0].ID)
		}
	}
}

func TestDelayedJobNotLeasedEarly(t *testing.T) {
	r := newRig(t, 71)
	// A long fallback poll and a long activity refresh leave the due-time
	// wakeup as the only thing that can start a fetch for a job that was not
	// due when the worker asked. The worker arrives after the jobs are
	// enqueued, so its first fetch is what tells the broker when they fall
	// due: a delayed enqueue sends no notification.
	r.addBroker("b1", func(c *sim.BrokerConfig) {
		c.PollInterval = 10 * time.Second
		c.ActivityInterval = time.Hour
	})
	const n = 6
	r.producer("p1", func(c *sim.ProducerConfig) { c.Jobs, c.DelayP = n, 1 }, nil, nil)
	const workerStart = time.Second
	r.w.At(workerStart, "worker starts", func() { r.addWorker("w1", nil) })

	if !r.runUntil(3*time.Minute, func() bool { return r.completed == n }) {
		t.Fatalf("%d of %d jobs completed by %v", r.completed, n, r.w.Now)
	}
	delayed, waited := 0, 0
	for _, ch := range r.changesOf(memstore.OpLease) {
		runAt := ch.Before.RunAt
		if ch.Now.Before(runAt) {
			t.Errorf("job %d leased at %v, before its run_at %v", ch.After.ID, ch.Now, runAt)
		}
		if runAt.After(ch.Before.CreatedAt) {
			delayed++
		}
		if !runAt.After(sim.Epoch.Add(workerStart)) {
			continue // due before the worker asked, so there was nothing to wake for
		}
		waited++
		if late := ch.Now.Sub(runAt); late > 2*time.Second {
			t.Errorf("job %d leased %v after it fell due, want the due-time wakeup to start the fetch", ch.After.ID, late)
		}
	}
	if delayed != n {
		t.Errorf("%d of %d jobs were delayed, want all", delayed, n)
	}
	if waited < 2 {
		t.Errorf("only %d jobs fell due after the worker started, so the wakeup was barely exercised", waited)
	}
}

func TestDRRServesSmallTenantUnderFlood(t *testing.T) {
	r := newRig(t, 81)
	r.addBroker("b1", nil)
	r.addWorker("w1", func(c *sim.WorkerConfig) {
		c.Lease.Concurrency = 8
		c.Handler = sim.HandlerModel{Duration: [2]time.Duration{300 * time.Millisecond, 300 * time.Millisecond}}
	})
	r.producer("pa", func(c *sim.ProducerConfig) { c.Jobs, c.Tenants = 300, []string{"A"} }, nil, nil)
	r.producer("pb", func(c *sim.ProducerConfig) { c.Jobs, c.Tenants = 5, []string{"B"} }, nil, nil)

	if !r.runUntil(2*time.Minute, func() bool { return len(r.changesOf(memstore.OpLease)) >= 120 }) {
		t.Fatalf("only %d leases by %v", len(r.changesOf(memstore.OpLease)), r.w.Now)
	}
	aLeases, bLeases, aBeforeLastB := 0, 0, 0
	for _, ch := range r.changesOf(memstore.OpLease) {
		switch ch.After.Tenant {
		case "A":
			aLeases++
		case "B":
			bLeases++
			aBeforeLastB = aLeases
		}
	}
	if bLeases != 5 {
		t.Fatalf("%d of tenant B's 5 jobs were leased among the first %d leases, want all", bLeases, aLeases+bLeases)
	}
	t.Logf("tenant A had %d leases when tenant B's last job was leased", aBeforeLastB)
	if aBeforeLastB >= 100 {
		t.Errorf("tenant B's last job started after %d of tenant A's jobs, want before A's 100th", aBeforeLastB)
	}
}

func TestNotifyWakesLongPollAndDropFallsBackToPoll(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dropP float64
		// maxLatency bounds the time from enqueue to lease.
		maxLatency time.Duration
	}{
		{"a notification wakes the broker at once", 0, 100 * time.Millisecond},
		{"a lost notification waits for the poll", 1, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, 91)
			b := r.addBroker("b1", func(c *sim.BrokerConfig) {
				c.PollInterval = time.Second
				c.NotifyDropP = tc.dropP
				// Slow enough that the periodic refresh cannot stand in
				// for a notification.
				c.ActivityInterval = time.Minute
			})
			r.addWorker("w1", func(c *sim.WorkerConfig) { c.Lease.LongPoll = 20 * time.Second })
			// Start the producer after the worker is parked at the broker.
			r.w.At(5*time.Second, "producer starts", func() { r.producer("p1", nil, nil, nil) })

			if !r.runUntil(time.Minute, func() bool { return r.completed == 1 }) {
				t.Fatalf("the job was not completed by %v", r.w.Now)
			}
			enq, leased := r.changesOf(memstore.OpEnqueue)[0], r.changesOf(memstore.OpLease)[0]
			if got := leased.Now.Sub(enq.Now); got > tc.maxLatency {
				t.Errorf("job leased %v after it was enqueued, want at most %v", got, tc.maxLatency)
			}
			if drops := b.Stats().NotifyDrops; (drops > 0) != (tc.dropP == 1) {
				t.Errorf("NotifyDrops = %d with NotifyDropP %v", drops, tc.dropP)
			}
		})
	}
}

func TestWorkerCrashDropsHandlersAndRestartRejoins(t *testing.T) {
	r := newRig(t, 101)
	r.addBroker("b1", nil)
	wk := r.addWorker("w1", func(c *sim.WorkerConfig) {
		c.Lease.Concurrency = 1
		c.Handler = sim.HandlerModel{Duration: [2]time.Duration{5 * time.Second, 5 * time.Second}}
	})
	r.producer("p1", nil, nil, nil)

	if !r.runUntil(time.Minute, func() bool { return len(wk.Holds()) == 1 }) {
		t.Fatalf("the worker never held a lease by %v", r.w.Now)
	}
	hold := wk.Holds()[0]
	if hold.Worker != "w1" || hold.Ref.Token != 1 || hold.Ref.Queue != testQueue {
		t.Errorf("hold = %+v, want worker w1, token 1, queue %q", hold, testQueue)
	}
	wk.Crash()
	if got := wk.Holds(); len(got) != 0 {
		t.Errorf("a crashed worker holds %v, want nothing", got)
	}
	r.runFor(time.Second)
	wk.Restart("w1-again", 2000)
	if wk.ID() != "w1-again" {
		t.Errorf("ID after restart = %q, want w1-again", wk.ID())
	}

	if !r.runUntil(5*time.Minute, func() bool { return r.completed == 1 }) {
		t.Fatalf("the job was not completed after the worker restarted, by %v", r.w.Now)
	}
	jobs := r.requireState(job.StateSucceeded)
	if len(jobs) != 1 || jobs[0].Epoch != 2 || jobs[0].LeasedBy != "" {
		t.Fatalf("rows = %+v, want one job redelivered once", jobs)
	}
	if cs := r.changesOf(memstore.OpComplete); len(cs) != 1 {
		t.Fatalf("%d completions", len(cs))
	}
	if got := r.changesOf(memstore.OpLease)[1].After.LeasedBy; got != "w1-again" {
		t.Errorf("redelivered to %q, want the restarted worker's new id", got)
	}
}

func TestHoldDeadlineIsInStoreTimeAndBeforeExpiry(t *testing.T) {
	r := newRig(t, 111)
	r.addBroker("b1", nil)
	wk := r.addWorker("w1", func(c *sim.WorkerConfig) {
		// A clock that starts hours away and runs slow: the deadline is
		// right only if it is converted back to virtual time.
		c.Clock = sim.Clock{Offset: 5 * time.Hour, Rate: 1 - 1e-4}
		c.Lease.Concurrency = 1
		c.Handler = sim.HandlerModel{Duration: [2]time.Duration{20 * time.Second, 20 * time.Second}}
	})
	r.producer("p1", nil, nil, nil)

	if !r.runUntil(time.Minute, func() bool { return len(wk.Holds()) == 1 }) {
		t.Fatalf("the worker never held a lease by %v", r.w.Now)
	}
	hold := wk.Holds()[0]
	expiry := r.changesOf(memstore.OpLease)[0].After.LeaseExpiresAt
	// The deadline anchors at the time the request was sent and keeps the
	// margin, so it sits before the expiry by the margin plus the message and
	// store delays. A slow clock stretches the 27s it counts by 100ppm, which
	// is under 3ms.
	const drift = 3 * time.Millisecond
	if hold.Deadline.After(expiry.Add(-testMargin).Add(drift)) {
		t.Errorf("deadline %v is after expiry-margin %v by more than the clock drift", hold.Deadline, expiry.Add(-testMargin))
	}
	if slack := expiry.Add(-testMargin).Sub(hold.Deadline); slack > 50*time.Millisecond {
		t.Errorf("deadline %v is %v earlier than expiry-margin, want only the message delays", hold.Deadline, slack)
	}
	if !hold.Deadline.After(sim.Epoch.Add(r.w.Now)) {
		t.Errorf("deadline %v has passed at store time %v", hold.Deadline, sim.Epoch.Add(r.w.Now))
	}
}

func TestProducerCancelsConfirmedJobs(t *testing.T) {
	r := newRig(t, 121)
	r.addBroker("b1", nil)
	// No worker: nothing finishes a job before its cancel arrives.
	cancelled := map[int64]bool{}
	const n = 10
	r.producer("p1", func(c *sim.ProducerConfig) { c.Jobs, c.CancelP = n, 1 }, nil,
		func(id int64) { cancelled[id] = true })

	if !r.runUntil(time.Minute, func() bool { return len(cancelled) == n }) {
		t.Fatalf("%d of %d cancels were confirmed by %v", len(cancelled), n, r.w.Now)
	}
	jobs := r.requireState(job.StateCancelled)
	if len(jobs) != n {
		t.Errorf("%d rows, want %d", len(jobs), n)
	}
}

func TestRetryingHandlerBacksOffUntilTheJobIsDead(t *testing.T) {
	r := newRig(t, 151)
	r.addBroker("b1", nil)
	r.addWorker("w1", func(c *sim.WorkerConfig) {
		c.Handler = sim.HandlerModel{Duration: [2]time.Duration{50 * time.Millisecond, 50 * time.Millisecond}, RetryP: 1}
	})
	const attempts = 4
	r.producer("p1", func(c *sim.ProducerConfig) { c.MaxAttempts = []int32{attempts} }, nil, nil)

	if !r.runUntil(5*time.Minute, func() bool { return len(r.changesOf(memstore.OpFail)) == attempts }) {
		t.Fatalf("%d of %d failures by %v", len(r.changesOf(memstore.OpFail)), attempts, r.w.Now)
	}
	jobs := r.requireState(job.StateDead)
	if len(jobs) != 1 || jobs[0].Attempts != attempts || jobs[0].LastError != "handler failed" {
		t.Fatalf("rows = %+v, want one dead job after %d attempts, last error %q", jobs, attempts, "handler failed")
	}
	waited := false
	for i, ch := range r.changesOf(memstore.OpFail)[:attempts-1] {
		delay := ch.After.RunAt.Sub(ch.Now)
		if ceiling := job.DefaultBackoff.Ceiling(int32(i + 1)); delay < 0 || delay > ceiling {
			t.Errorf("retry after attempt %d waits %v, want within the backoff ceiling %v", i+1, delay, ceiling)
		}
		waited = waited || delay > 0
	}
	if !waited {
		t.Error("no retry waited at all, so the default backoff was not applied")
	}
}

func TestCancelledJobsHandlerIsToldAtTheNextHeartbeat(t *testing.T) {
	r := newRig(t, 161)
	r.addBroker("b1", nil)
	wk := r.addWorker("w1", func(c *sim.WorkerConfig) {
		c.Lease.Concurrency = 1
		c.Handler = sim.HandlerModel{Duration: [2]time.Duration{20 * time.Second, 20 * time.Second}}
	})
	// The cancel lands while the handler runs, with 20s to go at most.
	cancelled := false
	r.producer("p1", func(c *sim.ProducerConfig) { c.CancelP = 1 }, nil, func(int64) { cancelled = true })

	if !r.runUntil(time.Minute, func() bool { return cancelled }) {
		t.Fatalf("the job was not cancelled by %v", r.w.Now)
	}
	if len(wk.Holds()) != 1 {
		t.Fatalf("the worker holds %d leases when the cancel lands, want its one running job", len(wk.Holds()))
	}
	// The next heartbeat is at most 7.5s away. It finds the lease gone, and
	// the worker cancels the handler, which then never writes to the sink.
	r.runFor(10 * time.Second)
	if got := wk.Holds(); len(got) != 0 {
		t.Errorf("the worker still holds %v 10s after the job was cancelled", got)
	}
	r.runFor(20 * time.Second)
	jobs := r.requireState(job.StateCancelled)
	if len(jobs) != 1 {
		t.Fatalf("%d rows, want 1", len(jobs))
	}
	if last, wrote := r.sink.Last(jobs[0].ID); wrote {
		t.Errorf("the cancelled job's handler wrote token %d to the sink, want it stopped before it finished", last)
	}
	if got := r.brokers[0].Stats().FencedAcks; got < 2 {
		t.Errorf("FencedAcks = %d, want the heartbeat and the handler's ack both fenced", got)
	}
}

func TestSinkAcceptsOnlyNonDecreasingTokens(t *testing.T) {
	s := sim.NewSink()
	if _, ok := s.Last(7); ok {
		t.Fatal("Last reports a write for a job never written")
	}
	steps := []struct {
		id, token int64
		accepted  bool
		last      int64
	}{
		{7, 2, true, 2},
		{7, 1, false, 2}, // below the highest seen
		{7, 2, true, 2},  // equal is accepted
		{7, 5, true, 5},
		{7, 4, false, 5},
		{8, 1, true, 1}, // jobs are independent
	}
	for i, st := range steps {
		if got := s.Write(st.id, st.token); got != st.accepted {
			t.Errorf("step %d: Write(%d, %d) = %v, want %v", i, st.id, st.token, got, st.accepted)
		}
		if last, ok := s.Last(st.id); !ok || last != st.last {
			t.Errorf("step %d: Last(%d) = %d, %v, want %d", i, st.id, last, ok, st.last)
		}
	}
	if got := s.Rejections(); got != 2 {
		t.Errorf("Rejections = %d, want 2", got)
	}
}

func TestFaultsFireAndTheClusterRecovers(t *testing.T) {
	r := newRigNet(t, 131, sim.NetConfig{
		DropP: 0.02, DupP: 0.02,
		Latency: [2]time.Duration{time.Millisecond, 4 * time.Millisecond},
		TailP:   0.01, Tail: [2]time.Duration{100 * time.Millisecond, 4 * time.Second},
	})
	// Every node's clock starts somewhere else and drifts at the edge of what
	// the checker tolerates, so that a wakeup computed on one clock lands
	// on the right instant of another.
	for i, id := range []string{"b1", "b2"} {
		r.addBroker(id, func(c *sim.BrokerConfig) {
			c.NotifyDropP = 0.2
			c.Clock = sim.Clock{Offset: time.Duration(i+1) * 3 * time.Hour, Rate: 1 - 1e-4*float64(i+1)}
		})
	}
	for i, id := range []string{"w1", "w2", "w3"} {
		r.addWorker(id, func(c *sim.WorkerConfig) {
			c.Clock = sim.Clock{Offset: time.Duration(i) * 17 * time.Minute, Rate: 1 + 1e-4*float64(i-1)}
			c.CooperativeP = 0.5
			c.Handler = sim.HandlerModel{
				Duration: [2]time.Duration{50 * time.Millisecond, 2 * time.Second},
				LongP:    0.05, Long: [2]time.Duration{testTTL + 5*time.Second, 2 * testTTL},
				RetryP: 0.1,
			}
		})
	}
	const n = 40
	confirmed := map[int64]bool{}
	r.producer("p1", func(c *sim.ProducerConfig) { c.Jobs, c.Over = n, 20*time.Second },
		func(_ string, id int64) { confirmed[id] = true }, nil)

	cfg := sim.FaultConfig{
		Phase:         40 * time.Second,
		WorkerCrashes: 3, WorkerStalls: 3, BrokerCrashes: 2, StoreOutages: 2, Partitions: 3,
		StallRange:     [2]time.Duration{5 * time.Second, 45 * time.Second},
		OutageRange:    [2]time.Duration{time.Second, 4 * time.Second},
		PartitionRange: [2]time.Duration{time.Second, 6 * time.Second},
		RestartDelay:   [2]time.Duration{100 * time.Millisecond, 3 * time.Second},
	}
	faults := sim.ScheduleFaults(r.w, r.net, r.st, r.brokers, r.workers, cfg)
	r.w.At(cfg.Phase, "heal", func() {
		r.net.Heal()
		r.st.SetAvailable(true)
	})

	done := func() bool {
		if r.w.Now < cfg.Phase+5*time.Second {
			return false
		}
		for _, j := range r.snapshot() {
			if j.State != job.StateSucceeded && j.State != job.StateDead {
				return false
			}
		}
		return len(confirmed) == n
	}
	// Checking the whole store each step would be slow; look once a second.
	finished := false
	var check func()
	check = func() {
		if finished = done(); !finished {
			r.w.After(time.Second, "check", check)
		}
	}
	r.w.At(cfg.Phase, "check", check)
	if !r.runUntil(10*time.Minute, func() bool { return finished }) {
		t.Fatalf("not every job was finished by %v (%d confirmed)", r.w.Now, len(confirmed))
	}

	got := faults.Counts()
	if got.StoreOutages != 2 || got.Partitions != 3 {
		t.Errorf("outages = %d and partitions = %d, want 2 and 3", got.StoreOutages, got.Partitions)
	}
	if got.WorkerCrashes == 0 || got.WorkerCrashes > 3 || got.WorkerStalls == 0 || got.WorkerStalls > 3 || got.BrokerCrashes == 0 || got.BrokerCrashes > 2 {
		t.Errorf("counts = %+v, want each kind fired, at most as often as configured", got)
	}
	if len(r.changesOf(memstore.OpReap)) == 0 {
		t.Error("no lease was reaped in a run with worker crashes and stalls longer than the TTL")
	}
	fenced, notifyDrops := 0, 0
	for _, b := range r.brokers {
		fenced += b.Stats().FencedAcks
		notifyDrops += b.Stats().NotifyDrops
	}
	t.Logf("faults %+v: %d leases, %d reaps, %d fenced acks, %d sink rejections, %d notifications dropped, network %+v",
		got, len(r.changesOf(memstore.OpLease)), len(r.changesOf(memstore.OpReap)), fenced, r.sink.Rejections(), notifyDrops, r.net.Stats())
}

func TestActorsSameSeedSameTrace(t *testing.T) {
	run := func(seed uint64) (hash uint64, completed int) {
		r := newRigNet(t, seed, sim.NetConfig{
			DropP: 0.02, DupP: 0.02,
			Latency: [2]time.Duration{time.Millisecond, 4 * time.Millisecond},
		})
		r.addBroker("b1", func(c *sim.BrokerConfig) { c.NotifyDropP = 0.3 })
		r.addBroker("b2", nil)
		r.addWorker("w1", func(c *sim.WorkerConfig) { c.Handler.RetryP = 0.2 })
		r.addWorker("w2", nil)
		r.producer("p1", func(c *sim.ProducerConfig) { c.Jobs, c.DelayP, c.CancelP = 30, 0.3, 0.1 }, nil, nil)
		sim.ScheduleFaults(r.w, r.net, r.st, r.brokers, r.workers, sim.FaultConfig{
			Phase:         20 * time.Second,
			WorkerCrashes: 1, WorkerStalls: 1, BrokerCrashes: 1, StoreOutages: 1, Partitions: 1,
			StallRange:     [2]time.Duration{time.Second, 5 * time.Second},
			OutageRange:    [2]time.Duration{time.Second, 2 * time.Second},
			PartitionRange: [2]time.Duration{time.Second, 3 * time.Second},
			RestartDelay:   [2]time.Duration{100 * time.Millisecond, time.Second},
		})
		r.runFor(90 * time.Second)
		return r.w.TraceHash(), r.completed
	}
	h1, c1 := run(141)
	h2, c2 := run(141)
	h3, _ := run(142)
	if h1 != h2 || c1 != c2 {
		t.Errorf("same seed gave trace %x with %d completions, then %x with %d", h1, c1, h2, c2)
	}
	if h1 == h3 {
		t.Errorf("seeds 141 and 142 gave the same trace %x", h1)
	}
	if c1 == 0 {
		t.Error("no job completed, so the trace says little")
	}
}
