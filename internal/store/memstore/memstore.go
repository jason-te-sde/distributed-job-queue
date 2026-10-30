// Package memstore is the in-memory reference implementation of store.Store.
//
// It is never used in production. Its job is to be obviously correct: it
// keeps the rows in a map, builds a sorted id slice for every call that needs
// an order, and finds rows by scanning, because a store that anyone can check
// by reading is the model the PostgreSQL store is held to (the differential
// test) and the store under the simulator. Speed is not a goal.
//
// Every row change goes through the transitions of package job, so the rules
// of a job live in one place. A call reads the Clock once, and uses that one
// instant for every row it touches, as the PostgreSQL store does with the
// clock of its transaction.
//
// The store copies at both of its boundaries. A Job shares its Payload and
// Result byte slices with whoever made it, so the store copies Spec.Payload
// and CompleteRequest.Result on the way in, and every Job it hands out (in
// grants, Get, Snapshot, ListDead and the observer's Changes) is a Clone. A
// caller that changes those bytes cannot change a stored row, which is also
// how the PostgreSQL store behaves.
package memstore

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// Op names the operation that changed a row.
type Op uint8

// The operations that change rows.
const (
	// OpEnqueue inserts a row.
	OpEnqueue Op = iota + 1
	// OpLease makes a ready row leased.
	OpLease
	// OpHeartbeat extends a lease.
	OpHeartbeat
	// OpComplete finishes a leased row.
	OpComplete
	// OpFail fails a leased row.
	OpFail
	// OpRelease gives a leased row back.
	OpRelease
	// OpCancel cancels an unfinished row.
	OpCancel
	// OpReap takes back an expired lease.
	OpReap
	// OpRequeue revives a dead row.
	OpRequeue
	// OpPrune deletes a row that outlived its retention.
	OpPrune
	// OpPurge deletes a finished row on request.
	OpPurge
)

// String returns the lower-case name of the operation, or "op(N)" for a
// value that is not one.
func (o Op) String() string {
	switch o {
	case OpEnqueue:
		return "enqueue"
	case OpLease:
		return "lease"
	case OpHeartbeat:
		return "heartbeat"
	case OpComplete:
		return "complete"
	case OpFail:
		return "fail"
	case OpRelease:
		return "release"
	case OpCancel:
		return "cancel"
	case OpReap:
		return "reap"
	case OpRequeue:
		return "requeue"
	case OpPrune:
		return "prune"
	case OpPurge:
		return "purge"
	}
	return "op(" + strconv.Itoa(int(o)) + ")"
}

// Change describes one row mutation.
type Change struct {
	// Op is the operation that made the change.
	Op Op
	// Token is the fencing token the caller presented, for Heartbeat,
	// Complete, Fail and Release; 0 for every other operation.
	Token int64
	// Before is the row before the change; the zero Job when Inserted.
	Before job.Job
	// After is the row after the change; the zero Job when Deleted.
	After job.Job
	// Inserted and Deleted say that the row was created or removed.
	Inserted, Deleted bool
	// Now is the store time of the call that made the change.
	Now time.Time
}

// Options configures a Store.
type Options struct {
	// Clock returns the store's time. It is required; its values pass
	// through job.NormalizeTime.
	Clock func() time.Time
	// Observer, if set, is called synchronously after each row mutation, in
	// order, while the store's lock is held. It must not call the store, and
	// may keep the Change, whose jobs are copies.
	Observer func(Change)
}

// dedupKey is the scope of an idempotency key.
type dedupKey struct{ tenant, queue, key string }

// Store is an in-memory store.Store and store.Snapshotter. It is safe for
// concurrent use.
type Store struct {
	opts Options

	mu     sync.Mutex
	jobs   map[int64]job.Job
	dedup  map[dedupKey]int64 // an entry lives exactly as long as its row
	lastID int64
}

var (
	_ store.Store       = (*Store)(nil)
	_ store.Snapshotter = (*Store)(nil)
)

// errNotImplemented stands in for the operations that a later change adds.
var errNotImplemented = errors.New("memstore: not implemented")

// New returns an empty Store. It panics if opts.Clock is nil.
func New(opts Options) *Store {
	if opts.Clock == nil {
		panic("memstore.New: Options.Clock is required")
	}
	return &Store{opts: opts, jobs: map[int64]job.Job{}, dedup: map[dedupKey]int64{}}
}

// now reads the clock. A call does so once, under the lock.
func (s *Store) now() time.Time { return job.NormalizeTime(s.opts.Clock()) }

// emit tells the observer about a change, handing it copies.
func (s *Store) emit(c Change) {
	if s.opts.Observer == nil {
		return
	}
	c.Before, c.After = c.Before.Clone(), c.After.Clone()
	s.opts.Observer(c)
}

// insert adds a new row.
func (s *Store) insert(op Op, j job.Job, now time.Time) {
	s.jobs[j.ID] = j
	if j.IdempotencyKey != "" {
		s.dedup[dedupKey{j.Tenant, j.Queue, j.IdempotencyKey}] = j.ID
	}
	s.emit(Change{Op: op, After: j, Inserted: true, Now: now})
}

// update replaces a row by its new version.
func (s *Store) update(op Op, token int64, before, after job.Job, now time.Time) {
	s.jobs[after.ID] = after
	s.emit(Change{Op: op, Token: token, Before: before, After: after, Now: now})
}

// sortedIDs returns the ids of all rows in ascending order.
func (s *Store) sortedIDs() []int64 { return slices.Sorted(maps.Keys(s.jobs)) }

// find returns the row that ref names, if it exists in ref's queue.
func (s *Store) find(ref store.Ref) (job.Job, bool) {
	j, ok := s.jobs[ref.ID]
	return j, ok && j.Queue == ref.Queue
}

// Enqueue implements store.Store.
func (s *Store) Enqueue(_ context.Context, specs []job.Spec) ([]store.EnqueueResult, error) {
	if err := job.ValidateBatch("specs", len(specs), job.MaxBatch); err != nil {
		return nil, err
	}
	for _, sp := range specs {
		if err := job.ValidateSpec(sp); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := make([]store.EnqueueResult, len(specs))
	for i, sp := range specs {
		s.lastID++ // taken by every item, so that a deduplicated one leaves a gap
		if sp.IdempotencyKey != "" {
			if id, dup := s.dedup[dedupKey{sp.Tenant, sp.Queue, sp.IdempotencyKey}]; dup {
				out[i] = store.EnqueueResult{ID: id, Deduplicated: true}
				continue
			}
		}
		sp.Payload = bytes.Clone(sp.Payload)
		j := job.New(sp, s.lastID, now)
		s.insert(OpEnqueue, j, now)
		out[i] = store.EnqueueResult{ID: j.ID}
	}
	return out, nil
}

// validateLease checks a lease request as a whole.
func validateLease(req store.LeaseRequest) error {
	if err := job.ValidateName("queue", req.Queue); err != nil {
		return err
	}
	if err := job.ValidateTTL(req.TTL); err != nil {
		return err
	}
	if err := job.ValidateWorkerID(req.Worker); err != nil {
		return err
	}
	if err := job.ValidateBatch("takes", len(req.Takes), job.MaxBatch); err != nil {
		return err
	}
	fifo := len(req.Takes) == 1 && req.Takes[0].Tenant == ""
	total := 0
	for _, tk := range req.Takes {
		if !fifo {
			if err := job.ValidateName("tenant", tk.Tenant); err != nil {
				return err
			}
		}
		// Each limit is checked on its own before the sum, so that a
		// negative limit cannot hide a large one.
		if err := job.ValidateBatch("limit", tk.Limit, job.MaxBatch); err != nil {
			return err
		}
		total += tk.Limit
	}
	return job.ValidateBatch("limit", total, job.MaxBatch)
}

// readyJobs returns the ready rows of a queue, ordered by (run_at, id). A
// tenant of "" selects every tenant. Rows that are not yet due are included:
// whether a row may be leased is for job.Lease to say.
func (s *Store) readyJobs(queue, tenant string) []job.Job {
	var out []job.Job
	for _, id := range s.sortedIDs() {
		j := s.jobs[id]
		if j.Queue == queue && j.State == job.StateReady && (tenant == "" || j.Tenant == tenant) {
			out = append(out, j)
		}
	}
	slices.SortStableFunc(out, byRunAtThenID)
	return out
}

// byRunAtThenID orders jobs the way a lease serves them.
func byRunAtThenID(a, b job.Job) int {
	return cmp.Or(a.RunAt.Compare(b.RunAt), cmp.Compare(a.ID, b.ID))
}

// Lease implements store.Store.
func (s *Store) Lease(_ context.Context, req store.LeaseRequest) ([]store.Grant, error) {
	if err := validateLease(req); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var grants []store.Grant
	for _, take := range req.Takes {
		n := 0
		for _, before := range s.readyJobs(req.Queue, take.Tenant) {
			if n == take.Limit {
				break
			}
			leased, out := job.Lease(before, now, req.TTL, req.Worker)
			if out != job.Applied {
				continue
			}
			s.update(OpLease, 0, before, leased, now)
			grants = append(grants, store.Grant{Job: leased.Clone(), Remaining: leased.LeaseExpiresAt.Sub(now)})
			n++
		}
	}
	slices.SortStableFunc(grants, func(a, b store.Grant) int { return byRunAtThenID(a.Job, b.Job) })
	return grants, nil
}

// validateRefs checks the refs of a Heartbeat or Release call.
func validateRefs(refs []store.Ref) error {
	if err := job.ValidateBatch("refs", len(refs), job.MaxAckBatch); err != nil {
		return err
	}
	for _, r := range refs {
		if err := job.ValidateName("queue", r.Queue); err != nil {
			return err
		}
	}
	return nil
}

// Heartbeat implements store.Store.
func (s *Store) Heartbeat(_ context.Context, refs []store.Ref, ttl time.Duration) ([]store.HeartbeatResult, error) {
	if err := validateRefs(refs); err != nil {
		return nil, err
	}
	if err := job.ValidateTTL(ttl); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := make([]store.HeartbeatResult, len(refs))
	for i, ref := range refs {
		j, ok := s.find(ref)
		if !ok {
			out[i].Outcome = job.NotFound
			continue
		}
		next, outcome := job.Heartbeat(j, ref.Token, now, ttl)
		out[i].Outcome = outcome
		if outcome == job.Applied {
			s.update(OpHeartbeat, ref.Token, j, next, now)
			out[i].Remaining = next.LeaseExpiresAt.Sub(now)
		}
	}
	return out, nil
}

// Complete implements store.Store.
func (s *Store) Complete(_ context.Context, reqs []store.CompleteRequest) ([]job.Outcome, error) {
	if err := job.ValidateBatch("completions", len(reqs), job.MaxAckBatch); err != nil {
		return nil, err
	}
	for _, r := range reqs {
		if err := job.ValidateName("queue", r.Ref.Queue); err != nil {
			return nil, err
		}
		if err := job.ValidateResult(r.Result); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := make([]job.Outcome, len(reqs))
	for i, r := range reqs {
		j, ok := s.find(r.Ref)
		if !ok {
			out[i] = job.NotFound
			continue
		}
		next, outcome := job.Complete(j, r.Ref.Token, now, bytes.Clone(r.Result))
		out[i] = outcome
		if outcome == job.Applied {
			s.update(OpComplete, r.Ref.Token, j, next, now)
		}
	}
	return out, nil
}

// Fail implements store.Store.
func (s *Store) Fail(_ context.Context, reqs []store.FailRequest) ([]job.Outcome, error) {
	if err := job.ValidateBatch("failures", len(reqs), job.MaxAckBatch); err != nil {
		return nil, err
	}
	for _, r := range reqs {
		if err := job.ValidateName("queue", r.Ref.Queue); err != nil {
			return nil, err
		}
		if err := job.ValidateDelay("delay", r.Delay); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := make([]job.Outcome, len(reqs))
	for i, r := range reqs {
		j, ok := s.find(r.Ref)
		if !ok {
			out[i] = job.NotFound
			continue
		}
		next, outcome := job.Fail(j, r.Ref.Token, now, r.Permanent, r.Delay, r.Error)
		out[i] = outcome
		if outcome == job.Applied {
			s.update(OpFail, r.Ref.Token, j, next, now)
		}
	}
	return out, nil
}

// Release implements store.Store.
func (s *Store) Release(_ context.Context, refs []store.Ref) ([]job.Outcome, error) {
	if err := validateRefs(refs); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := make([]job.Outcome, len(refs))
	for i, ref := range refs {
		j, ok := s.find(ref)
		if !ok {
			out[i] = job.NotFound
			continue
		}
		next, outcome := job.Release(j, ref.Token, now)
		out[i] = outcome
		if outcome == job.Applied {
			s.update(OpRelease, ref.Token, j, next, now)
		}
	}
	return out, nil
}

// Get implements store.Store.
func (s *Store) Get(_ context.Context, id int64, tenant string) (job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok || tenant != "" && j.Tenant != tenant {
		return job.Job{}, fmt.Errorf("memstore: job %d: %w", id, store.ErrNotFound)
	}
	return j.Clone(), nil
}

// Snapshot implements store.Snapshotter.
func (s *Store) Snapshot(_ context.Context) ([]job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]job.Job, 0, len(s.jobs))
	for _, id := range s.sortedIDs() {
		out = append(out, s.jobs[id].Clone())
	}
	return out, nil
}

// Cancel implements store.Store.
func (s *Store) Cancel(context.Context, int64, string) (job.Outcome, error) {
	return 0, errNotImplemented
}

// Requeue implements store.Store.
func (s *Store) Requeue(context.Context, int64) (job.Outcome, error) {
	return 0, errNotImplemented
}

// Reap implements store.Store.
func (s *Store) Reap(context.Context, int, store.BackoffFunc) (int, error) {
	return 0, errNotImplemented
}

// Prune implements store.Store.
func (s *Store) Prune(context.Context, store.Retention, int) (int, error) {
	return 0, errNotImplemented
}

// Purge implements store.Store.
func (s *Store) Purge(context.Context, store.PurgeRequest) (int, error) {
	return 0, errNotImplemented
}

// ListDead implements store.Store.
func (s *Store) ListDead(context.Context, string, int64, int) ([]job.Job, error) {
	return nil, errNotImplemented
}

// Stats implements store.Store.
func (s *Store) Stats(context.Context, string) ([]store.TenantStats, error) {
	return nil, errNotImplemented
}

// Activity implements store.Store.
func (s *Store) Activity(context.Context, string) (store.Activity, error) {
	return store.Activity{}, errNotImplemented
}
