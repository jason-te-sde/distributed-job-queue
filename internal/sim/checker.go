package sim

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// The invariants a Checker reports, named as in the design: S1 to S8 hold at
// every step, and L1 and L2 hold once the faults have stopped and the system
// has had time to heal.
const (
	invS1 = "S1" // no confirmed job is lost; only the pruner deletes, and only past retention
	invS2 = "S2" // finished jobs do not come back
	invS3 = "S3" // one holder at a time: epochs grow by one and are never reused
	invS4 = "S4" // fencing: accepted writes carry the current epoch
	invS5 = "S5" // a worker's local view of a lease is conservative
	invS6 = "S6" // attempts stay within the budget
	invS7 = "S7" // an idempotency key names one job while the row exists
	invS8 = "S8" // no lease before run_at
	invL1 = "L1" // every confirmed job that was not cancelled finishes
	invL2 = "L2" // no lease outlives its expiry by more than the reap interval
)

// CheckerConfig configures a Checker. It must describe the system under
// check: a tolerance computed from a longer TTL than the run uses would let a
// real violation through.
type CheckerConfig struct {
	// Margin is the worker's safety margin: how long before a lease's
	// expiry the worker gives up on it.
	Margin time.Duration
	// MaxTTL and MaxLongPoll are the longest lease and the longest long
	// poll in the run. A worker's deadline is measured over both, so they
	// scale the clock drift that S5 tolerates.
	MaxTTL, MaxLongPoll time.Duration
	// MaxDriftPPM is how far apart, in parts per million, two nodes' clocks
	// may run.
	MaxDriftPPM float64
	// Retention is the retention the pruner is given.
	Retention store.Retention
	// ReapInterval is the period of the reaper.
	ReapInterval time.Duration
}

// Violation says that an invariant was broken.
type Violation struct {
	// Invariant names the invariant: S1 to S8, L1 or L2.
	Invariant string
	// JobID is the job the violation is about.
	JobID int64
	// Detail says what was seen.
	Detail string
}

// Error formats the violation as "<Invariant> violated for job <JobID>:
// <Detail>".
func (v *Violation) Error() string {
	return fmt.Sprintf("%s violated for job %d: %s", v.Invariant, v.JobID, v.Detail)
}

// violate builds a Violation.
func violate(invariant string, id int64, format string, args ...any) *Violation {
	return &Violation{Invariant: invariant, JobID: id, Detail: fmt.Sprintf(format, args...)}
}

// leaseKey names one lease: a job and the epoch it was granted under.
type leaseKey struct{ id, epoch int64 }

// Checker is the oracle of a simulation. It is fed everything that happens at
// the store (Change), everything that clients are told (Confirmed and
// Cancelled) and every worker's view of its leases (Holds), and returns the
// first invariant that a call breaks. It is meant to be called after every
// event, so that a violation that heals itself is still seen.
//
// A Checker is not safe for concurrent use, and it never reads a clock: the
// times it compares are the store's.
type Checker struct {
	cfg  CheckerConfig
	sink *Sink

	keys      map[string]int64       // the id each confirmed key was answered with
	confirmed map[int64]bool         // the ids a broker confirmed
	cancelled map[int64]bool         // the ids the store cancelled
	maxEpoch  map[int64]int64        // the highest epoch granted for each job
	expiry    map[leaseKey]time.Time // the latest expiry the store gave each lease
	pruned    map[int64]bool         // the ids the pruner deleted
}

// NewChecker returns a checker for a run configured by cfg. The sink is the
// downstream that handlers write to, which S4 reads; nil skips that part of
// S4.
func NewChecker(cfg CheckerConfig, sink *Sink) *Checker {
	return &Checker{
		cfg: cfg, sink: sink,
		keys:      map[string]int64{},
		confirmed: map[int64]bool{},
		cancelled: map[int64]bool{},
		maxEpoch:  map[int64]int64{},
		expiry:    map[leaseKey]time.Time{},
		pruned:    map[int64]bool{},
	}
}

// Tolerance is how far past "expiry minus margin" a worker's deadline may
// lie before S5 calls it a violation: the drift of two clocks, 2 *
// MaxDriftPPM/1e6, over the longest time a deadline is measured across, a
// long poll plus a TTL, and 2µs for the rounding of a deadline to a whole
// microsecond at each end.
func (c *Checker) Tolerance() time.Duration {
	drift := 2 * c.cfg.MaxDriftPPM / 1e6 * float64(c.cfg.MaxLongPoll+c.cfg.MaxTTL)
	return time.Duration(math.Round(drift)) + 2*time.Microsecond
}

// Confirmed reports that a broker answered an enqueue with key by naming job
// id. It checks S7: a key answered before must get the same id again, unless
// the row of that id was pruned, which ends the key's life.
func (c *Checker) Confirmed(key string, id int64) error {
	if prev, seen := c.keys[key]; seen && prev != id && !c.pruned[prev] {
		return violate(invS7, id, "key %q was answered with job %d and is now answered with job %d, though job %d was never pruned", key, prev, id, prev)
	}
	c.keys[key] = id
	c.confirmed[id] = true
	return nil
}

// Cancelled reports that a client's cancel of job id took effect. Such a job
// is not owed a finish by L1.
func (c *Checker) Cancelled(id int64) {
	c.cancelled[id] = true
}

// Change checks one row change of the store, in the order the store made
// them, against S1 to S4, S6 and S8, and records the lease expiries that S5
// compares against. A change that the store reports as a cancel also counts as
// a cancel: the store decides what is cancelled, and the client may never
// have heard.
func (c *Checker) Change(ch memstore.Change) error {
	id := ch.After.ID
	if ch.Deleted {
		id = ch.Before.ID
	}

	// S1: a row is deleted only by the pruner, and only once it outlived its
	// retention.
	if ch.Deleted {
		if ch.Op != memstore.OpPrune {
			return violate(invS1, id, "%v deleted the row, and only prune may", ch.Op)
		}
		if !job.Prunable(ch.Before, ch.Now, c.cfg.Retention.Done, c.cfg.Retention.Dead) {
			return violate(invS1, id, "pruned at %v a %v row finished at %v, within its retention (done %v, dead %v)",
				ch.Now, ch.Before.State, ch.Before.FinishedAt, c.cfg.Retention.Done, c.cfg.Retention.Dead)
		}
		c.pruned[id] = true
		return nil
	}

	// S2: a finished job stays as it is until it is pruned; a dead one may
	// also be requeued.
	switch ch.Before.State {
	case job.StateSucceeded, job.StateCancelled:
		return violate(invS2, id, "%v changed a %v job to %v", ch.Op, ch.Before.State, ch.After.State)
	case job.StateDead:
		if ch.Op != memstore.OpRequeue || ch.After.State != job.StateReady {
			return violate(invS2, id, "%v changed a dead job to %v, and only a requeue to ready may", ch.Op, ch.After.State)
		}
	case job.StateReady, job.StateLeased:
	}

	switch ch.Op {
	case memstore.OpLease:
		// S3: a lease takes a ready row, moves the epoch up by one, and no
		// epoch of the job is granted twice.
		if ch.Before.State != job.StateReady {
			return violate(invS3, id, "leased a %v row", ch.Before.State)
		}
		if ch.After.Epoch != ch.Before.Epoch+1 {
			return violate(invS3, id, "lease moved the epoch from %d to %d, want %d", ch.Before.Epoch, ch.After.Epoch, ch.Before.Epoch+1)
		}
		if prev := c.maxEpoch[id]; ch.After.Epoch <= prev {
			return violate(invS3, id, "epoch %d granted, though epoch %d was granted before", ch.After.Epoch, prev)
		}
		c.maxEpoch[id] = ch.After.Epoch
		c.expiry[leaseKey{id, ch.After.Epoch}] = ch.After.LeaseExpiresAt
		// S8: not before the row's run_at.
		if ch.Before.RunAt.After(ch.Now) {
			return violate(invS8, id, "leased at %v, before run_at %v", ch.Now, ch.Before.RunAt)
		}
	case memstore.OpHeartbeat, memstore.OpComplete, memstore.OpFail, memstore.OpRelease:
		// S4: only the holder of the current lease is accepted.
		if ch.Before.State != job.StateLeased {
			return violate(invS4, id, "%v with token %d accepted on a %v row", ch.Op, ch.Token, ch.Before.State)
		}
		if ch.Token != ch.Before.Epoch {
			return violate(invS4, id, "%v with token %d accepted while the epoch is %d", ch.Op, ch.Token, ch.Before.Epoch)
		}
		if ch.Op == memstore.OpComplete && c.sink != nil {
			if last, wrote := c.sink.Last(id); !wrote || last != ch.Token {
				return violate(invS4, id, "complete with token %d accepted, but the last token the sink accepted is %d (written: %v)", ch.Token, last, wrote)
			}
		}
		if ch.Op == memstore.OpHeartbeat {
			key := leaseKey{id, ch.Token}
			if ch.After.LeaseExpiresAt.After(c.expiry[key]) {
				c.expiry[key] = ch.After.LeaseExpiresAt
			}
		}
	case memstore.OpCancel:
		c.cancelled[id] = true
	case memstore.OpEnqueue, memstore.OpReap, memstore.OpRequeue, memstore.OpPrune, memstore.OpPurge:
	}

	// S6: no attempt beyond the budget.
	if ch.After.Attempts > ch.After.MaxAttempts {
		return violate(invS6, id, "%v left %d attempts of a budget of %d", ch.Op, ch.After.Attempts, ch.After.MaxAttempts)
	}
	return nil
}

// Holds checks S5 for the leases that workers believe they hold: each must be
// one the store granted, and its deadline must not lie later than the latest
// expiry the store gave it, less the margin, plus Tolerance. The check is on
// the state, so it holds at every step whatever the network and the workers
// are doing.
func (c *Checker) Holds(holds []Hold) error {
	tolerance := c.Tolerance()
	for _, h := range holds {
		expiry, ok := c.expiry[leaseKey{h.Ref.ID, h.Ref.Token}]
		if !ok {
			return violate(invS5, h.Ref.ID, "worker %s holds token %d, which the store never granted", h.Worker, h.Ref.Token)
		}
		bound := expiry.Add(-c.cfg.Margin).Add(tolerance)
		if h.Deadline.After(bound) {
			return violate(invS5, h.Ref.ID, "worker %s holds token %d until %v, %v past the bound %v (expiry %v, margin %v, tolerance %v)",
				h.Worker, h.Ref.Token, h.Deadline, h.Deadline.Sub(bound), bound, expiry, c.cfg.Margin, tolerance)
		}
	}
	return nil
}

// Final checks the end of a run, once the faults have stopped and the system
// has had time to heal, against the snapshot of the store taken at store time
// now. S1: every confirmed job is in the store or was pruned. L1: every
// confirmed job that was not cancelled succeeded or died, or was pruned. L2: no
// job is leased past its expiry by more than the reap interval and 1ms.
func (c *Checker) Final(snapshot []job.Job, now time.Time) error {
	byID := make(map[int64]job.Job, len(snapshot))
	for _, j := range snapshot {
		byID[j.ID] = j
	}
	ids := slices.Sorted(maps.Keys(c.confirmed))
	for _, id := range ids {
		if _, present := byID[id]; !present && !c.pruned[id] {
			return violate(invS1, id, "a confirmed job is neither in the store nor pruned")
		}
	}
	for _, id := range ids {
		j, present := byID[id]
		if !present || c.cancelled[id] {
			continue
		}
		if j.State != job.StateSucceeded && j.State != job.StateDead {
			return violate(invL1, id, "a confirmed job is still %v at %v (epoch %d, attempts %d of %d, run_at %v)",
				j.State, now, j.Epoch, j.Attempts, j.MaxAttempts, j.RunAt)
		}
	}
	for _, j := range snapshot {
		if j.State == job.StateLeased && j.LeaseExpiresAt.Add(c.cfg.ReapInterval+time.Millisecond).Before(now) {
			return violate(invL2, j.ID, "leased to %q at %v, though the lease expired at %v and the reap interval is %v",
				j.LeasedBy, now, j.LeaseExpiresAt, c.cfg.ReapInterval)
		}
	}
	return nil
}
