package sim_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/core/lease"
	"github.com/jason-te-sde/distributed-job-queue/internal/sim"
	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// checkerConfig is the configuration that the hand-built histories assume:
// retention of 2m and 5m, and a tolerance of 2 * 100ppm * (20s + 30s) + 2µs,
// which is 10.002ms.
func checkerConfig() sim.CheckerConfig {
	return sim.CheckerConfig{
		Margin:       3 * time.Second,
		MaxTTL:       30 * time.Second,
		MaxLongPoll:  20 * time.Second,
		MaxDriftPPM:  100,
		Retention:    store.Retention{Done: 2 * time.Minute, Dead: 5 * time.Minute},
		ReapInterval: time.Second,
	}
}

// at is a store time d after virtual time zero.
func at(d time.Duration) time.Time { return sim.Epoch.Add(d) }

// row builds a job in a state at an epoch, with a budget of 5 attempts.
func row(id int64, state job.State, epoch int64) job.Job {
	return job.Job{ID: id, Queue: "q", Tenant: "t", State: state, Epoch: epoch, MaxAttempts: 5, RunAt: at(0)}
}

// leaseChange is a lease of row id that moves it from epoch to epoch+1 at
// now, for ttl.
func leaseChange(id, epoch int64, now, ttl time.Duration) memstore.Change {
	before, after := row(id, job.StateReady, epoch), row(id, job.StateLeased, epoch+1)
	after.Attempts = 1
	after.LeaseExpiresAt = at(now + ttl)
	return memstore.Change{Op: memstore.OpLease, Before: before, After: after, Now: at(now)}
}

// prune is the deletion of a finished row by the pruner at now.
func prune(before job.Job, now time.Duration) memstore.Change {
	return memstore.Change{Op: memstore.OpPrune, Before: before, Deleted: true, Now: at(now)}
}

// must fails the test if a call that should not alarm does.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("setup call alarmed: %v", err)
	}
}

func TestCheckerAlarms(t *testing.T) {
	cfg := checkerConfig()
	finished := func(state job.State, id int64, finishedAt time.Duration) job.Job {
		j := row(id, state, 1)
		j.FinishedAt = at(finishedAt)
		return j
	}
	for _, tc := range []struct {
		name string
		want string // the invariant
		id   int64  // the job the violation names
		run  func(t *testing.T, c *sim.Checker, sink *sim.Sink) error
	}{
		{"S1 a reap deletes a row", "S1", 1, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			return c.Change(memstore.Change{Op: memstore.OpReap, Before: row(1, job.StateLeased, 1), Deleted: true, Now: at(time.Hour)})
		}},
		{"S1 a prune before the retention is over", "S1", 2, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			// Finished 1m ago, with a retention of 2m.
			return c.Change(prune(finished(job.StateSucceeded, 2, 0), time.Minute))
		}},
		{"S1 a prune exactly at the retention keeps the row", "S1", 2, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			return c.Change(prune(finished(job.StateSucceeded, 2, 0), 2*time.Minute))
		}},
		{"S1 a purge deletes a row that is old enough to prune", "S1", 27, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			ch := prune(finished(job.StateSucceeded, 27, 0), time.Hour)
			ch.Op = memstore.OpPurge
			return c.Change(ch)
		}},
		{"S1 a dead row pruned between the two retentions", "S1", 29, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			// Done is 2m and Dead is 5m: a dead row finished 3m ago is not
			// old enough, though a succeeded one would be.
			return c.Change(prune(finished(job.StateDead, 29, 0), 3*time.Minute))
		}},
		{"S1 a confirmed job is gone and was not pruned", "S1", 3, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			must(t, c.Confirmed("k", 3))
			return c.Final(nil, at(time.Hour))
		}},
		{"S2 a succeeded job changes", "S2", 4, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			after := row(4, job.StateCancelled, 1)
			return c.Change(memstore.Change{Op: memstore.OpCancel, Before: row(4, job.StateSucceeded, 1), After: after, Now: at(time.Second)})
		}},
		{"S2 a cancelled job changes", "S2", 5, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			after := row(5, job.StateReady, 1)
			return c.Change(memstore.Change{Op: memstore.OpRequeue, Before: row(5, job.StateCancelled, 1), After: after, Now: at(time.Second)})
		}},
		{"S2 a dead job is cancelled", "S2", 6, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			after := row(6, job.StateCancelled, 1)
			return c.Change(memstore.Change{Op: memstore.OpCancel, Before: row(6, job.StateDead, 1), After: after, Now: at(time.Second)})
		}},
		{"S2 a dead job is requeued into another state", "S2", 7, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			after := row(7, job.StateLeased, 1)
			return c.Change(memstore.Change{Op: memstore.OpRequeue, Before: row(7, job.StateDead, 1), After: after, Now: at(time.Second)})
		}},
		{"S3 a lease does not advance the epoch", "S3", 8, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			ch := leaseChange(8, 4, time.Second, 30*time.Second)
			ch.After.Epoch = ch.Before.Epoch
			return c.Change(ch)
		}},
		{"S3 a lease skips an epoch", "S3", 28, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			ch := leaseChange(28, 1, time.Second, 30*time.Second)
			ch.After.Epoch = ch.Before.Epoch + 2
			return c.Change(ch)
		}},
		{"S3 an epoch is granted twice", "S3", 9, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			must(t, c.Change(leaseChange(9, 0, time.Second, 30*time.Second)))
			// The row goes back to ready with its epoch reset, so that the
			// next lease is Before+1 and yet an epoch granted before.
			return c.Change(leaseChange(9, 0, 40*time.Second, 30*time.Second))
		}},
		{"S3 a lease of a row that is not ready", "S3", 10, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			ch := leaseChange(10, 1, time.Second, 30*time.Second)
			ch.Before.State = job.StateLeased
			return c.Change(ch)
		}},
		{"S4 a complete carries an older token", "S4", 11, func(t *testing.T, c *sim.Checker, sink *sim.Sink) error {
			sink.Write(11, 2)
			return c.Change(memstore.Change{Op: memstore.OpComplete, Token: 1, Before: row(11, job.StateLeased, 2), After: row(11, job.StateSucceeded, 2), Now: at(time.Second)})
		}},
		{"S4 a heartbeat on a row that is not leased", "S4", 12, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			return c.Change(memstore.Change{Op: memstore.OpHeartbeat, Token: 1, Before: row(12, job.StateReady, 1), After: row(12, job.StateReady, 1), Now: at(time.Second)})
		}},
		{"S4 a fail carries a newer token", "S4", 13, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			return c.Change(memstore.Change{Op: memstore.OpFail, Token: 3, Before: row(13, job.StateLeased, 2), After: row(13, job.StateReady, 2), Now: at(time.Second)})
		}},
		{"S4 a release carries an older token", "S4", 14, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			return c.Change(memstore.Change{Op: memstore.OpRelease, Token: 1, Before: row(14, job.StateLeased, 2), After: row(14, job.StateReady, 2), Now: at(time.Second)})
		}},
		{"S4 a complete the sink never saw", "S4", 15, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			return c.Change(memstore.Change{Op: memstore.OpComplete, Token: 2, Before: row(15, job.StateLeased, 2), After: row(15, job.StateSucceeded, 2), Now: at(time.Second)})
		}},
		{"S4 a complete while the sink holds another token", "S4", 16, func(t *testing.T, c *sim.Checker, sink *sim.Sink) error {
			sink.Write(16, 3)
			return c.Change(memstore.Change{Op: memstore.OpComplete, Token: 2, Before: row(16, job.StateLeased, 2), After: row(16, job.StateSucceeded, 2), Now: at(time.Second)})
		}},
		{"S5 a hold 1µs beyond the tolerance", "S5", 17, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			must(t, c.Change(leaseChange(17, 0, 10*time.Second, 30*time.Second)))
			bound := at(40 * time.Second).Add(-cfg.Margin).Add(c.Tolerance())
			return c.Holds([]sim.Hold{{Worker: "w1", Ref: lease.Ref{ID: 17, Queue: "q", Token: 1}, Deadline: bound.Add(time.Microsecond)}})
		}},
		{"S5 a hold the store never granted", "S5", 18, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			must(t, c.Change(leaseChange(18, 0, 10*time.Second, 30*time.Second)))
			return c.Holds([]sim.Hold{{Worker: "w1", Ref: lease.Ref{ID: 18, Queue: "q", Token: 2}, Deadline: at(11 * time.Second)}})
		}},
		{"S5 a heartbeat that did not extend the lease", "S5", 19, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			must(t, c.Change(leaseChange(19, 0, 0, 30*time.Second)))
			// The worker believes a heartbeat extended the lease to 60s, but
			// the store only ever recorded 30s.
			return c.Holds([]sim.Hold{{Worker: "w1", Ref: lease.Ref{ID: 19, Queue: "q", Token: 1}, Deadline: at(57 * time.Second)}})
		}},
		{"S6 attempts above the budget", "S6", 20, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			before, after := row(20, job.StateLeased, 3), row(20, job.StateReady, 3)
			before.Attempts, after.Attempts = 5, 6
			return c.Change(memstore.Change{Op: memstore.OpReap, Before: before, After: after, Now: at(time.Hour)})
		}},
		{"S7 a key is answered with another id", "S7", 22, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			must(t, c.Confirmed("k", 21))
			return c.Confirmed("k", 22)
		}},
		{"S8 a lease 1µs before run_at", "S8", 23, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			ch := leaseChange(23, 0, time.Second, 30*time.Second)
			ch.Before.RunAt = at(time.Second + time.Microsecond)
			return c.Change(ch)
		}},
		{"L1 a confirmed job is still ready", "L1", 24, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			must(t, c.Confirmed("k", 24))
			return c.Final([]job.Job{row(24, job.StateReady, 0)}, at(time.Hour))
		}},
		{"L1 a confirmed job is still leased", "L1", 25, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			j := row(25, job.StateLeased, 1)
			j.LeaseExpiresAt = at(time.Hour + time.Minute)
			must(t, c.Confirmed("k", 25))
			return c.Final([]job.Job{j}, at(time.Hour))
		}},
		{"L2 a lease outlived the reap interval", "L2", 26, func(t *testing.T, c *sim.Checker, _ *sim.Sink) error {
			j := row(26, job.StateLeased, 1)
			j.LeaseExpiresAt = at(time.Minute)
			// The bound is expiry + reap interval (1s) + 1ms; this is 1µs over.
			return c.Final([]job.Job{j}, at(time.Minute+time.Second+time.Millisecond+time.Microsecond))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := sim.NewSink()
			err := tc.run(t, sim.NewChecker(cfg, sink), sink)
			var v *sim.Violation
			if !errors.As(err, &v) {
				t.Fatalf("err = %v, want a *sim.Violation for %s", err, tc.want)
			}
			if v.Invariant != tc.want || v.JobID != tc.id {
				t.Errorf("violation = %q for job %d (%s), want %s for job %d", v.Invariant, v.JobID, v.Detail, tc.want, tc.id)
			}
			if v.Detail == "" {
				t.Error("the violation has no detail")
			}
		})
	}
}

// TestCheckerStaysQuietAtTheBoundaries pairs the alarms of
// TestCheckerAlarms that sit on a boundary with the case one step inside it,
// which must not alarm.
func TestCheckerStaysQuietAtTheBoundaries(t *testing.T) {
	cfg := checkerConfig()
	done := row(1, job.StateSucceeded, 1)
	done.FinishedAt = at(0)
	leased := row(2, job.StateLeased, 1)
	leased.LeaseExpiresAt = at(time.Minute)

	for _, tc := range []struct {
		name string
		run  func(c *sim.Checker) error
	}{
		{"a prune 1µs after the retention", func(c *sim.Checker) error {
			return c.Change(prune(done, 2*time.Minute+time.Microsecond))
		}},
		{"a dead job is pruned only after its own retention", func(c *sim.Checker) error {
			dead := row(3, job.StateDead, 1)
			dead.FinishedAt = at(0)
			return c.Change(prune(dead, 5*time.Minute+time.Microsecond))
		}},
		{"a dead job is requeued to ready", func(c *sim.Checker) error {
			return c.Change(memstore.Change{Op: memstore.OpRequeue, Before: row(4, job.StateDead, 1), After: row(4, job.StateReady, 1), Now: at(time.Second)})
		}},
		{"a lease exactly at run_at", func(c *sim.Checker) error {
			ch := leaseChange(5, 0, time.Second, 30*time.Second)
			ch.Before.RunAt = at(time.Second)
			return c.Change(ch)
		}},
		{"attempts exactly at the budget", func(c *sim.Checker) error {
			before, after := row(6, job.StateLeased, 3), row(6, job.StateDead, 3)
			before.Attempts, after.Attempts = 5, 5
			return c.Change(memstore.Change{Op: memstore.OpFail, Token: 3, Before: before, After: after, Now: at(time.Second)})
		}},
		{"a lease exactly at the reap bound", func(c *sim.Checker) error {
			return c.Final([]job.Job{leased}, at(time.Minute+time.Second+time.Millisecond))
		}},
		{"a confirmed job that succeeded or died", func(c *sim.Checker) error {
			if err := c.Confirmed("a", 1); err != nil {
				return err
			}
			if err := c.Confirmed("b", 7); err != nil {
				return err
			}
			return c.Final([]job.Job{done, row(7, job.StateDead, 1)}, at(time.Hour))
		}},
		{"a confirmed job that was cancelled", func(c *sim.Checker) error {
			c.Cancelled(8)
			if err := c.Confirmed("c", 8); err != nil {
				return err
			}
			return c.Final([]job.Job{row(8, job.StateReady, 1)}, at(time.Hour))
		}},
		{"a confirmed job the store cancelled without the producer hearing", func(c *sim.Checker) error {
			if err := c.Confirmed("d", 9); err != nil {
				return err
			}
			cancelled := row(9, job.StateCancelled, 1)
			if err := c.Change(memstore.Change{Op: memstore.OpCancel, Before: row(9, job.StateReady, 1), After: cancelled, Now: at(time.Second)}); err != nil {
				return err
			}
			return c.Final([]job.Job{cancelled}, at(time.Hour))
		}},
		{"the same key confirmed again with the same id", func(c *sim.Checker) error {
			if err := c.Confirmed("e", 10); err != nil {
				return err
			}
			return c.Confirmed("e", 10)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(sim.NewChecker(cfg, sim.NewSink())); err != nil {
				t.Errorf("alarmed: %v", err)
			}
		})
	}
}

// TestCheckerAcceptsGoodHistory drives the checker with the changes of the
// real store through a whole life of one job, calling the other entry points
// where a run would.
func TestCheckerAcceptsGoodHistory(t *testing.T) {
	cfg := checkerConfig()
	sink := sim.NewSink()
	c := sim.NewChecker(cfg, sink)
	now := sim.Epoch
	changes := 0
	st := memstore.New(memstore.Options{
		Clock: func() time.Time { return now },
		Observer: func(ch memstore.Change) {
			changes++
			if err := c.Change(ch); err != nil {
				t.Errorf("change %d (%v) alarmed: %v", changes, ch.Op, err)
			}
		},
	})
	ctx := context.Background()
	const ttl = 30 * time.Second
	ref := func(id, token int64) store.Ref { return store.Ref{ID: id, Queue: "q", Token: token} }
	holds := func(id, token int64, deadline time.Time) {
		t.Helper()
		must(t, c.Holds([]sim.Hold{{Worker: "w1", Ref: lease.Ref{ID: id, Queue: "q", Token: token}, Deadline: deadline}}))
	}

	res, err := st.Enqueue(ctx, []job.Spec{{Queue: "q", Tenant: "t", IdempotencyKey: "k"}})
	if err != nil {
		t.Fatal(err)
	}
	id := res[0].ID
	must(t, c.Confirmed("k", id))

	now = now.Add(time.Second)
	if _, err := st.Lease(ctx, store.LeaseRequest{Queue: "q", Takes: []store.TenantTake{{Tenant: "t", Limit: 1}}, TTL: ttl, Worker: "w1"}); err != nil {
		t.Fatal(err)
	}
	holds(id, 1, now.Add(ttl-cfg.Margin)) // the deadline a worker computes on a lease taken at now

	now = now.Add(5 * time.Second)
	if _, err := st.Heartbeat(ctx, []store.Ref{ref(id, 1)}, ttl); err != nil {
		t.Fatal(err)
	}
	heartbeatDeadline := now.Add(ttl - cfg.Margin)
	holds(id, 1, heartbeatDeadline)

	now = now.Add(time.Second)
	if _, err := st.Fail(ctx, []store.FailRequest{{Ref: ref(id, 1), Delay: 2 * time.Second, Error: "boom"}}); err != nil {
		t.Fatal(err)
	}
	// The failed lease is dead at the store, but the worker may not know yet,
	// and its deadline is still before the last expiry the store recorded.
	holds(id, 1, heartbeatDeadline)

	now = now.Add(2 * time.Second) // run_at is exactly now
	if _, err := st.Lease(ctx, store.LeaseRequest{Queue: "q", Takes: []store.TenantTake{{Tenant: "t", Limit: 1}}, TTL: ttl, Worker: "w2"}); err != nil {
		t.Fatal(err)
	}
	holds(id, 2, now.Add(ttl-cfg.Margin))

	if !sink.Write(id, 2) {
		t.Fatal("the sink refused the holder's write")
	}
	now = now.Add(time.Second)
	if out, err := st.Complete(ctx, []store.CompleteRequest{{Ref: ref(id, 2)}}); err != nil || out[0] != job.Applied {
		t.Fatalf("complete = %v, %v", out, err)
	}
	snapshot, err := st.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	must(t, c.Final(snapshot, now))

	now = now.Add(cfg.Retention.Done + time.Second)
	if n, err := st.Prune(ctx, cfg.Retention, 10); err != nil || n != 1 {
		t.Fatalf("prune = %d, %v", n, err)
	}
	snapshot, err = st.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	must(t, c.Final(snapshot, now))
	if changes != 7 {
		t.Errorf("the store reported %d changes, want 7 (enqueue, lease, heartbeat, fail, lease, complete, prune)", changes)
	}
}

func TestCheckerTolerance(t *testing.T) {
	if got, want := sim.NewChecker(checkerConfig(), nil).Tolerance(), 10*time.Millisecond+2*time.Microsecond; got != want {
		t.Errorf("Tolerance = %v, want %v", got, want)
	}
	cfg := checkerConfig()
	cfg.MaxDriftPPM, cfg.MaxLongPoll, cfg.MaxTTL = 0, 0, time.Hour
	if got, want := sim.NewChecker(cfg, nil).Tolerance(), 2*time.Microsecond; got != want {
		t.Errorf("Tolerance without drift = %v, want %v", got, want)
	}
}

func TestS5ToleranceBoundary(t *testing.T) {
	c := sim.NewChecker(checkerConfig(), sim.NewSink())
	must(t, c.Change(leaseChange(1, 0, 0, 30*time.Second)))
	bound := at(30 * time.Second).Add(-3 * time.Second).Add(c.Tolerance())
	hold := func(deadline time.Time) error {
		return c.Holds([]sim.Hold{{Worker: "w1", Ref: lease.Ref{ID: 1, Queue: "q", Token: 1}, Deadline: deadline}})
	}
	if err := hold(bound); err != nil {
		t.Errorf("a hold exactly at the bound alarmed: %v", err)
	}
	var v *sim.Violation
	if err := hold(bound.Add(time.Microsecond)); !errors.As(err, &v) || v.Invariant != "S5" {
		t.Errorf("a hold 1µs over the bound gave %v, want S5", err)
	}
	if err := hold(bound.Add(-time.Hour)); err != nil {
		t.Errorf("a hold far below the bound alarmed: %v", err)
	}
}

func TestS5FollowsHeartbeatsAndLaterLeases(t *testing.T) {
	c := sim.NewChecker(checkerConfig(), sim.NewSink())
	must(t, c.Change(leaseChange(1, 0, 0, 30*time.Second)))
	hb := memstore.Change{
		Op: memstore.OpHeartbeat, Token: 1,
		Before: row(1, job.StateLeased, 1), After: row(1, job.StateLeased, 1), Now: at(10 * time.Second),
	}
	hb.After.LeaseExpiresAt = at(40 * time.Second)
	must(t, c.Change(hb))
	hold := func(token int64, deadline time.Duration) error {
		return c.Holds([]sim.Hold{{Worker: "w1", Ref: lease.Ref{ID: 1, Queue: "q", Token: token}, Deadline: at(deadline)}})
	}
	if err := hold(1, 37*time.Second); err != nil {
		t.Errorf("a deadline at the extended expiry minus the margin alarmed: %v", err)
	}
	var v *sim.Violation
	if err := hold(1, 38*time.Second); !errors.As(err, &v) || v.Invariant != "S5" {
		t.Errorf("a deadline past the extended expiry gave %v, want S5", err)
	}
	// Each epoch has its own expiry: the new lease does not widen the old.
	must(t, c.Change(leaseChange(1, 1, 50*time.Second, 30*time.Second)))
	if err := hold(1, 38*time.Second); !errors.As(err, &v) || v.Invariant != "S5" {
		t.Errorf("the second lease widened the first one's expiry: %v", err)
	}
	if err := hold(2, 77*time.Second); err != nil {
		t.Errorf("a deadline of the second lease alarmed: %v", err)
	}
}

func TestS7AfterPruneAllowsNewID(t *testing.T) {
	c := sim.NewChecker(checkerConfig(), sim.NewSink())
	must(t, c.Confirmed("k", 1))
	var v *sim.Violation
	if err := c.Confirmed("k", 2); !errors.As(err, &v) || v.Invariant != "S7" {
		t.Fatalf("a second id for a live key gave %v, want S7", err)
	}

	c = sim.NewChecker(checkerConfig(), sim.NewSink())
	must(t, c.Confirmed("k", 1))
	done := row(1, job.StateSucceeded, 1)
	done.FinishedAt = at(0)
	must(t, c.Change(prune(done, 3*time.Minute)))
	if err := c.Confirmed("k", 2); err != nil {
		t.Errorf("a new id for a pruned key alarmed: %v", err)
	}
	// The key now belongs to id 2, so a third id is a violation again.
	if err := c.Confirmed("k", 3); !errors.As(err, &v) || v.Invariant != "S7" {
		t.Errorf("a third id gave %v, want S7", err)
	}
}

func TestViolationError(t *testing.T) {
	v := &sim.Violation{Invariant: "S3", JobID: 17, Detail: "epoch 2 granted twice"}
	if got, want := v.Error(), "S3 violated for job 17: epoch 2 granted twice"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// checkedRun is a simulated cluster with the checker wired in, as the seed
// sweep wires it: every change goes to Change, every confirmation and cancel
// to the checker, every worker's holds to Holds after every event.
type checkedRun struct {
	*rig
	c       *sim.Checker
	err     error
	holds   int // holds examined, so that a run that never looked can be told
	changes int
}

func (cr *checkedRun) fail(err error) {
	if err != nil && cr.err == nil {
		cr.err = err
	}
}

func newCheckedRun(t *testing.T, seed uint64, net sim.NetConfig) *checkedRun {
	cr := &checkedRun{rig: newRigNet(t, seed, net)}
	cr.step = cr.stepChecked
	cr.c = sim.NewChecker(sim.CheckerConfig{
		Margin: testMargin, MaxTTL: testTTL, MaxLongPoll: 2 * time.Second, MaxDriftPPM: 100,
		Retention: store.Retention{Done: 24 * time.Hour, Dead: 336 * time.Hour}, ReapInterval: time.Second,
	}, cr.sink)
	cr.onChange(func(ch memstore.Change) {
		cr.changes++
		cr.fail(cr.c.Change(ch))
	})
	return cr
}

// finish fails the test if any call of the run alarmed, or if the final
// checks do, and returns the rows.
func (cr *checkedRun) finish() []job.Job {
	cr.t.Helper()
	if cr.err != nil {
		cr.t.Fatalf("at %v: %v", cr.w.Now, cr.err)
	}
	rows := cr.snapshot()
	if err := cr.c.Final(rows, cr.w.StoreTime()); err != nil {
		cr.t.Fatalf("final: %v", err)
	}
	return rows
}

// stepChecked fires one event and checks every worker's holds.
func (cr *checkedRun) stepChecked() bool {
	ok := cr.w.Step()
	for _, wk := range cr.workers {
		h := wk.Holds()
		cr.holds += len(h)
		cr.fail(cr.c.Holds(h))
	}
	return ok
}

func TestCheckerHoldsUpOnSimulatedRuns(t *testing.T) {
	for seed := uint64(201); seed < 205; seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			cr := newCheckedRun(t, seed, sim.NetConfig{
				DropP: 0.02, DupP: 0.02,
				Latency: [2]time.Duration{time.Millisecond, 4 * time.Millisecond},
				TailP:   0.01, Tail: [2]time.Duration{100 * time.Millisecond, 4 * time.Second},
			})
			for i, id := range []string{"b1", "b2"} {
				cr.addBroker(id, func(c *sim.BrokerConfig) {
					c.NotifyDropP = 0.2
					c.Clock = sim.Clock{Offset: time.Duration(i+1) * time.Hour, Rate: 1 + 1e-4*float64(1-2*i)}
				})
			}
			for i, id := range []string{"w1", "w2", "w3"} {
				cr.addWorker(id, func(c *sim.WorkerConfig) {
					c.Clock = sim.Clock{Offset: time.Duration(i) * 11 * time.Minute, Rate: 1 + 1e-4*float64(i-1)}
					c.CooperativeP = 0.5
					c.Handler = sim.HandlerModel{
						Duration: [2]time.Duration{50 * time.Millisecond, 2 * time.Second},
						LongP:    0.05, Long: [2]time.Duration{testTTL + 5*time.Second, 2 * testTTL},
						RetryP: 0.1, PermanentP: 0.02,
					}
				})
			}
			cr.producer("p1", func(c *sim.ProducerConfig) {
				c.Jobs, c.Over = 50, 20*time.Second
				c.Tenants = []string{"a", "b"}
				c.DelayP, c.CancelP = 0.2, 0.1
				c.MaxAttempts = []int32{3, 25}
			}, func(key string, id int64) { cr.fail(cr.c.Confirmed(key, id)) }, cr.c.Cancelled)

			faults := sim.FaultConfig{
				Phase:         40 * time.Second,
				WorkerCrashes: 3, WorkerStalls: 3, BrokerCrashes: 2, StoreOutages: 2, Partitions: 3,
				StallRange:     [2]time.Duration{5 * time.Second, 45 * time.Second},
				OutageRange:    [2]time.Duration{time.Second, 4 * time.Second},
				PartitionRange: [2]time.Duration{time.Second, 6 * time.Second},
				RestartDelay:   [2]time.Duration{100 * time.Millisecond, 3 * time.Second},
			}
			sim.ScheduleFaults(cr.w, cr.net, cr.st, cr.brokers, cr.workers, faults)
			cr.w.At(faults.Phase, "heal", func() {
				cr.net.Heal()
				cr.st.SetAvailable(true)
			})

			settled := false
			var look func()
			look = func() {
				settled = true
				for _, j := range cr.snapshot() {
					if j.State != job.StateSucceeded && j.State != job.StateDead && j.State != job.StateCancelled {
						settled = false
					}
				}
				if !settled {
					cr.w.After(time.Second, "look", look)
				}
			}
			cr.w.At(faults.Phase+5*time.Second, "look", look)
			for cr.err == nil && !settled && cr.w.Now < 10*time.Minute && cr.step() {
			}
			if cr.err != nil {
				t.Fatalf("at %v: %v", cr.w.Now, cr.err)
			}
			if !settled {
				t.Fatalf("not every job finished by %v", cr.w.Now)
			}
			if err := cr.c.Final(cr.snapshot(), cr.w.StoreTime()); err != nil {
				t.Fatalf("final: %v", err)
			}
			if cr.holds == 0 || cr.changes == 0 {
				t.Errorf("the checker saw %d holds and %d changes, want both", cr.holds, cr.changes)
			}
			t.Logf("%d changes and %d holds checked; %d reaps, %d sink rejections", cr.changes, cr.holds, len(cr.changesOf(memstore.OpReap)), cr.sink.Rejections())
		})
	}
}

// TestCheckedStalledWorkerIsFenced runs the stalled-worker scenario, in both
// orders of the late writes, with the checker looking after every event. It
// is the case that catches a store that completes on a stale token (S4).
func TestCheckedStalledWorkerIsFenced(t *testing.T) {
	for _, tc := range stalledHolderCases {
		t.Run(tc.name, func(t *testing.T) {
			cr := newCheckedRun(t, 21, sim.NetConfig{Latency: [2]time.Duration{time.Millisecond, 3 * time.Millisecond}})
			cr.runStalledHolder(tc.handler)
			rows := cr.finish()
			if len(rows) != 1 || rows[0].State != job.StateSucceeded {
				t.Errorf("rows = %+v, want the one job succeeded", rows)
			}
			if cr.holds == 0 {
				t.Error("the checker saw no hold, so S5 was not exercised")
			}
		})
	}
}

// TestCheckedWorkerCrashAtTheAttemptBudget crashes the only worker while it
// holds a job that has one attempt. The reaper must kill the job instead of
// giving it another lease (S6).
func TestCheckedWorkerCrashAtTheAttemptBudget(t *testing.T) {
	cr := newCheckedRun(t, 31, sim.NetConfig{Latency: [2]time.Duration{time.Millisecond, 3 * time.Millisecond}})
	cr.addBroker("b1", nil)
	wk := cr.addWorker("w1", func(c *sim.WorkerConfig) {
		c.Lease.Concurrency = 1
		c.Handler = sim.HandlerModel{Duration: [2]time.Duration{time.Minute, time.Minute}}
	})
	crashed := false
	cr.onChange(func(ch memstore.Change) {
		if ch.Op != memstore.OpLease || crashed {
			return
		}
		crashed = true
		cr.w.After(100*time.Millisecond, "crash worker", wk.Crash)
		cr.w.After(time.Second, "restart worker", func() { wk.Restart("w1-again", 7) })
	})
	cr.producer("p1", func(c *sim.ProducerConfig) { c.MaxAttempts = []int32{1} },
		func(key string, id int64) { cr.fail(cr.c.Confirmed(key, id)) }, nil)

	cr.runFor(2 * time.Minute)
	rows := cr.finish()
	if len(rows) != 1 || rows[0].State != job.StateDead || rows[0].Attempts != 1 {
		t.Errorf("rows = %+v, want the one job dead after its one attempt", rows)
	}
	if got := len(cr.changesOf(memstore.OpLease)); got != 1 {
		t.Errorf("%d leases, want 1: a job that used its budget is not leased again", got)
	}
}

// TestCheckedStaleHeartbeatIsFenced delivers a heartbeat of the first lease
// to the store just after the job was leased a second time, as a delayed
// message would. The store must fence it, and the checker, which sees every
// change, must stay quiet. A store that extended the lease on a stale token
// would make a change with Token 1 on a row of epoch 2 (S4).
func TestCheckedStaleHeartbeatIsFenced(t *testing.T) {
	cr := newCheckedRun(t, 41, sim.NetConfig{Latency: [2]time.Duration{time.Millisecond, 3 * time.Millisecond}})
	cr.addBroker("b1", nil)
	cr.addWorker("w1", func(c *sim.WorkerConfig) {
		c.Lease.Concurrency = 1
		c.Handler = sim.HandlerModel{Duration: [2]time.Duration{50 * time.Millisecond, 50 * time.Millisecond}, RetryP: 1}
	})
	var outcome job.Outcome
	injected := false
	cr.onChange(func(ch memstore.Change) {
		if ch.Op != memstore.OpLease || ch.After.Epoch != 2 || injected {
			return
		}
		injected = true
		stale := []store.Ref{{ID: ch.After.ID, Queue: ch.After.Queue, Token: 1}}
		cr.st.Call("stale heartbeat", func(s store.Store) error {
			res, err := s.Heartbeat(context.Background(), stale, testTTL)
			if err == nil {
				outcome = res[0].Outcome
			}
			return err
		}, nil)
	})
	cr.producer("p1", nil, nil, nil)

	cr.runFor(20 * time.Second)
	cr.finish()
	if !injected {
		t.Fatal("the job was never leased a second time")
	}
	if outcome != job.Fenced {
		t.Errorf("the stale heartbeat got %v, want %v", outcome, job.Fenced)
	}
}
