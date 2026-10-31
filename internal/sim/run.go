package sim

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

const (
	// maxClockDriftPPM is how far a node's clock may run from real time, in
	// parts per million. The checker tolerates the difference of two clocks.
	maxClockDriftPPM = 100
	// runLongPoll is the long poll that simulated workers ask for. It is
	// shorter than the production default, so that a run sees more of them.
	runLongPoll = 10 * time.Second
	// runReapInterval is the period of every broker's reaper, as in
	// production.
	runReapInterval = time.Second
)

// Config describes one simulated run. DefaultConfig draws every field that
// has a range from the seed.
type Config struct {
	// Seed seeds the world, and so every random choice of the run.
	Seed uint64
	// Brokers and Workers are the node counts. Every queue needs a worker,
	// so Workers is at least len(Queues); worker i serves Queues[i % len].
	Brokers, Workers int
	// Concurrency is the number of handlers each worker runs at once.
	Concurrency int
	// Jobs is how many jobs the producer enqueues, over the first half of
	// the fault phase.
	Jobs int
	// Tenants and Queues are the names the jobs are spread over.
	Tenants, Queues []string
	// TTL and Margin are the lease length and the worker's safety margin.
	TTL, Margin time.Duration
	// Net is the network's fault model until the fault phase ends.
	Net NetConfig
	// Faults is the fault schedule; its Phase is the length of the fault
	// phase.
	Faults FaultConfig
	// NotifyDropP is the chance that a store notification never reaches a
	// broker.
	NotifyDropP float64
	// Heal is how long the run goes on after the fault phase, with a healthy
	// network and store, for the system to finish what it holds.
	Heal time.Duration
	// Retention is how long the pruner keeps finished jobs. It is short, so
	// that a run prunes.
	Retention store.Retention
}

// DefaultConfig returns the configuration of a seed: every drawn value comes
// from NewRand(seed), so a seed always means the same run.
func DefaultConfig(seed uint64) Config {
	r := NewRand(seed)
	between := func(lo, hi int) int { return lo + r.IntN(hi-lo+1) }
	names := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = prefix + strconv.Itoa(i+1)
		}
		return out
	}
	c := Config{
		Seed:        seed,
		Brokers:     between(1, 3),
		Workers:     between(2, 5),
		Concurrency: between(1, 8),
		Jobs:        between(50, 300),
		TTL:         30 * time.Second,
		Margin:      3 * time.Second,
		Heal:        30 * time.Minute,
		Retention:   store.Retention{Done: 2 * time.Minute, Dead: 5 * time.Minute},
	}
	c.Tenants = names("t", between(1, 4))
	c.Queues = names("q", between(1, 2))
	c.Net = NetConfig{
		DropP:   r.Float64() * 0.05,
		DupP:    r.Float64() * 0.02,
		Latency: [2]time.Duration{200 * time.Microsecond, 5 * time.Millisecond},
		TailP:   0.01,
		Tail:    [2]time.Duration{100 * time.Millisecond, 6 * time.Second},
	}
	c.Faults = FaultConfig{
		Phase:          10 * time.Minute,
		WorkerCrashes:  between(0, 3),
		WorkerStalls:   between(0, 3),
		BrokerCrashes:  between(0, 3),
		StoreOutages:   between(0, 3),
		Partitions:     between(0, 3),
		StallRange:     [2]time.Duration{5 * time.Second, 90 * time.Second},
		OutageRange:    [2]time.Duration{time.Second, 60 * time.Second},
		PartitionRange: [2]time.Duration{time.Second, 2 * time.Minute},
		RestartDelay:   [2]time.Duration{100 * time.Millisecond, 30 * time.Second},
	}
	c.NotifyDropP = r.Float64() * 0.3
	return c
}

// Stats says what a run did and how rough it was.
type Stats struct {
	// Events is the number of events the world fired.
	Events int
	// Grants counts the leases the store granted, and DuplicateDeliveries
	// the grants beyond the first of each job.
	Grants, DuplicateDeliveries int
	// FencedAcks counts store outcomes Fenced across Heartbeat, Complete,
	// Fail and Release.
	FencedAcks int
	// SinkRejections counts the writes that the sink refused as stale.
	SinkRejections int
	// Reaps and Prunes count the rows reaped and pruned.
	Reaps, Prunes int
	// The faults that took effect, and the notifications that were dropped.
	WorkerCrashes, WorkerStalls, BrokerCrashes, StoreOutages, Partitions, NotifyDrops int
	// TraceHash is World.TraceHash at the end of the run.
	TraceHash uint64
}

// Add adds the counts of o to s. TraceHash is left alone: a sum of hashes
// says nothing.
func (s *Stats) Add(o Stats) {
	s.Events += o.Events
	s.Grants += o.Grants
	s.DuplicateDeliveries += o.DuplicateDeliveries
	s.FencedAcks += o.FencedAcks
	s.SinkRejections += o.SinkRejections
	s.Reaps += o.Reaps
	s.Prunes += o.Prunes
	s.WorkerCrashes += o.WorkerCrashes
	s.WorkerStalls += o.WorkerStalls
	s.BrokerCrashes += o.BrokerCrashes
	s.StoreOutages += o.StoreOutages
	s.Partitions += o.Partitions
	s.NotifyDrops += o.NotifyDrops
}

// Run builds the cluster that cfg describes, runs it for the fault phase and
// the heal phase, and checks every invariant after every event. The first
// violation stops the run and comes back as an error that names the seed and
// wraps the *Violation. A panic from a core or a node, which means a protocol
// error between them, comes back as an error that names the seed too, and
// so does a config that cannot be built. The Stats cover what ran.
func Run(cfg Config) (stats Stats, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("seed %d: panic: %v\n%s", cfg.Seed, r, debug.Stack())
		}
	}()
	if err := cfg.validate(); err != nil {
		return Stats{}, fmt.Errorf("seed %d: %w", cfg.Seed, err)
	}
	stats, err = run(cfg)
	if err != nil {
		err = fmt.Errorf("seed %d: %w", cfg.Seed, err)
	}
	return stats, err
}

// validate rejects a config that the run cannot be built from.
func (c Config) validate() error {
	switch {
	case c.Brokers < 1:
		return fmt.Errorf("config needs a broker, has %d", c.Brokers)
	case len(c.Queues) < 1 || len(c.Tenants) < 1:
		return fmt.Errorf("config needs a queue and a tenant, has %d queues and %d tenants", len(c.Queues), len(c.Tenants))
	case c.Workers < len(c.Queues):
		return fmt.Errorf("config has %d workers for %d queues, so a queue has no worker", c.Workers, len(c.Queues))
	case c.Faults.Phase <= 0 || c.Heal < 0:
		return fmt.Errorf("config has a fault phase of %v and a heal phase of %v", c.Faults.Phase, c.Heal)
	}
	return nil
}

// simulation is a cluster built from a Config, with the checker wired in.
type simulation struct {
	cfg     Config
	w       *World
	net     *Network
	st      *StoreNode
	sink    *Sink
	checker *Checker
	brokers []*BrokerNode
	workers []*WorkerNode
	prod    *Producer
	faults  *Faults

	stats     Stats
	granted   map[int64]bool // the jobs that have been leased at least once
	violation error          // the first thing the checker reported
}

// fail records the first error that a check returns.
func (s *simulation) fail(err error) {
	if err != nil && s.violation == nil {
		s.violation = err
	}
}

// newSimulation builds the world, the nodes, the producer and the fault
// schedule of cfg, and wires every store change to the checker.
func newSimulation(cfg Config) *simulation {
	w := NewWorld(cfg.Seed)
	s := &simulation{cfg: cfg, w: w, net: NewNetwork(w, cfg.Net), sink: NewSink(), granted: map[int64]bool{}}
	s.checker = NewChecker(CheckerConfig{
		Margin: cfg.Margin, MaxTTL: cfg.TTL, MaxLongPoll: runLongPoll, MaxDriftPPM: maxClockDriftPPM,
		Retention: cfg.Retention, ReapInterval: runReapInterval,
	}, s.sink)
	s.st = NewStoreNode(w, s.onChange)

	clock := func() Clock {
		return Clock{
			Offset: w.Rand.Between(0, 24*time.Hour),
			Rate:   1 + (w.Rand.Float64()*2-1)*maxClockDriftPPM/1e6,
		}
	}
	var brokerIDs []NodeID
	for i := range cfg.Brokers {
		id := NodeID("b" + strconv.Itoa(i+1))
		brokerIDs = append(brokerIDs, id)
		s.brokers = append(s.brokers, NewBrokerNode(w, s.net, s.st, BrokerConfig{
			ID: id, Clock: clock(), Scheduler: "drr",
			PollInterval: time.Second, ReapInterval: runReapInterval,
			PruneInterval: time.Minute, ActivityInterval: time.Second,
			Retention: cfg.Retention, NotifyDropP: cfg.NotifyDropP,
		}))
	}
	for i := range cfg.Workers {
		s.workers = append(s.workers, NewWorkerNode(w, s.net, s.sink, WorkerConfig{
			ID:    NodeID("w" + strconv.Itoa(i+1)),
			Queue: cfg.Queues[i%len(cfg.Queues)],
			Clock: clock(),
			Lease: lease.Config{
				Concurrency: cfg.Concurrency, TTL: cfg.TTL, Margin: cfg.Margin, LongPoll: runLongPoll,
			},
			Brokers:      brokerIDs,
			CooperativeP: 0.5,
			Handler: HandlerModel{
				Duration: [2]time.Duration{20 * time.Millisecond, 2 * time.Second},
				LongP:    0.03, Long: [2]time.Duration{cfg.TTL + 2*time.Second, 3 * cfg.TTL},
				RetryP: 0.1, PermanentP: 0.02,
			},
		}, w.Rand.seed()))
	}
	s.prod = NewProducer(w, s.net, ProducerConfig{
		ID: "p1", Brokers: brokerIDs, Jobs: cfg.Jobs, Over: cfg.Faults.Phase / 2,
		Tenants: cfg.Tenants, Queues: cfg.Queues,
		DelayP: 0.15, MaxAttempts: []int32{2, 5, job.DefaultMaxAttempts}, CancelP: 0.05,
	}, func(key string, id int64) { s.fail(s.checker.Confirmed(key, id)) }, s.checker.Cancelled)

	s.faults = ScheduleFaults(w, s.net, s.st, s.brokers, s.workers, cfg.Faults)
	w.At(cfg.Faults.Phase, "heal", func() {
		s.net.Heal()
		s.st.SetAvailable(true)
	})
	return s
}

// onChange sees every change of the store: the checker checks it, and the
// stats count it.
func (s *simulation) onChange(ch memstore.Change) {
	s.fail(s.checker.Change(ch))
	switch ch.Op {
	case memstore.OpLease:
		s.stats.Grants++
		s.granted[ch.After.ID] = true
	case memstore.OpReap:
		s.stats.Reaps++
	case memstore.OpPrune:
		s.stats.Prunes++
	case memstore.OpEnqueue, memstore.OpHeartbeat, memstore.OpComplete, memstore.OpFail,
		memstore.OpRelease, memstore.OpCancel, memstore.OpRequeue, memstore.OpPurge:
	}
}

// step fires one event and checks what every worker believes it holds. It
// reports false when no event is left.
func (s *simulation) step() bool {
	if !s.w.Step() {
		return false
	}
	s.stats.Events++
	for _, wk := range s.workers {
		s.fail(s.checker.Holds(wk.Holds()))
	}
	return true
}

// finish totals the stats and runs the checks of the end of the run: the
// first violation of the run, then the final safety and liveness checks, then
// that the producer got every job confirmed.
func (s *simulation) finish() (Stats, error) {
	st := &s.stats
	st.DuplicateDeliveries = st.Grants - len(s.granted)
	st.SinkRejections = s.sink.Rejections()
	for _, b := range s.brokers {
		st.FencedAcks += b.Stats().FencedAcks
		st.NotifyDrops += b.Stats().NotifyDrops
	}
	counts := s.faults.Counts()
	st.WorkerCrashes, st.WorkerStalls, st.BrokerCrashes = counts.WorkerCrashes, counts.WorkerStalls, counts.BrokerCrashes
	st.StoreOutages, st.Partitions = counts.StoreOutages, counts.Partitions
	st.TraceHash = s.w.TraceHash()
	if s.violation != nil {
		return *st, s.violation
	}
	snapshot, err := s.st.Mem().Snapshot(context.Background())
	if err != nil {
		return *st, fmt.Errorf("final snapshot: %w", err)
	}
	if err := s.checker.Final(snapshot, s.w.StoreTime()); err != nil {
		return *st, err
	}
	if got := s.prod.Confirmed(); got != s.cfg.Jobs {
		return *st, fmt.Errorf("the producer got %d of its %d jobs confirmed by %v, after the network and the store healed",
			got, s.cfg.Jobs, s.cfg.Faults.Phase+s.cfg.Heal)
	}
	return *st, nil
}

// run is Run without the error handling around it.
func run(cfg Config) (Stats, error) {
	s := newSimulation(cfg)
	for end := cfg.Faults.Phase + cfg.Heal; s.violation == nil && s.w.Now < end && s.step(); {
	}
	return s.finish()
}
