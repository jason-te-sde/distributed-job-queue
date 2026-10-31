package pgstore

import (
	"context"
	"sync/atomic"
)

// The connections that notifying transactions hold.
//
// Every transaction that runs pg_notify (Enqueue, Release and Requeue) takes,
// at its commit, the lock that PostgreSQL uses to serialize the writers of the
// notification queue (locktype object, classid 1262, object 0), and keeps it
// until the commit is over, the flush of the WAL included. Notifying
// transactions therefore commit one at a time. When commits are slow, or the
// load is high, they queue on that lock, and each holds a connection of the
// pool while it waits. Left alone, they take every connection, and the calls
// that never notify (Lease, Activity, Heartbeat, Complete, Fail, Reap and
// Prune) wait behind them for one: leasing stops while the backlog of
// enqueues drains, even though the database could serve the lease at once.
//
// A gate bounds how many connections notifying calls hold at once. A call takes
// a permit before it asks the pool for a connection, so one that waits for its
// turn holds nothing. It gives the permit back only after its connection is
// back in the pool, which is the order of the deferred calls at each site: a
// permit then never stands for less than the connection it guards, and the
// connections that notifying calls hold cannot exceed the bound. The permits
// are fewer than the connections, and what is left over is for everyone else.
//
// A named cost: a call takes a permit before it can know whether its statement
// will notify, and some calls that take one do not. An Enqueue of scheduled or
// delayed jobs only, a batch of nothing but duplicates, a Requeue of a job that
// is not dead and a Release that every fence refuses all commit without the
// notify lock. They are gated all the same, because splitting them out would
// add a second code path for the sake of a wait that the bound does not make
// measurable: at the default cap of 15 of 20 connections such a call waits only
// when 15 notifying calls are already in.

// defaultReserveFloor is the fewest connections that the default bound leaves
// to calls that do not notify, from a pool of 3 connections up. A pool of 2
// leaves one, because a bound of nothing would stop Enqueue, and a pool of 1
// leaves none.
const defaultReserveFloor = 2

// defaultMaxNotifying is the default bound on the connections that notifying
// calls hold, for a pool of poolMax connections: all but a reserve of
// max(2, poolMax/4), and at least 1, because a bound of nothing would stop
// Enqueue altogether. From a pool of 3 up that leaves at least 2 connections
// to the other calls, a pool of 2 leaves 1, and a pool of 1 leaves none.
//
// Why a reserve of that size. A broker has up to MaxInFlight Lease calls (4 by
// default) and one Activity read in flight for a queue, and heartbeats,
// acknowledgements and the reaper add a few short calls, so a queue's dispatch
// needs about five connections at the busiest instant. A quarter of the default
// pool of 20 is exactly 5, and the floor of 2 keeps a small pool from being left
// with less. Why not a larger reserve: the notify lock lets one commit through
// at a time, so notifying calls beyond the first few only wait their turn, and
// a bound of three quarters of the pool costs no enqueue throughput as long as
// that lock is what limits it.
func defaultMaxNotifying(poolMax int) int {
	reserve := max(defaultReserveFloor, poolMax/4)
	return max(1, poolMax-reserve)
}

// notifyingCeiling is the largest bound that WithMaxNotifying and the broker
// allow for a pool of poolMax connections: all but one, so that the bound can
// never be as large as the pool and so turn itself off. A pool of 1 cannot
// spare one, and its bound is the one connection.
func notifyingCeiling(poolMax int) int { return max(1, poolMax-1) }

// gate is a counting semaphore of fixed size. Waiting for a permit respects
// the caller's context.
type gate struct{ slots chan struct{} }

func newGate(n int) *gate { return &gate{slots: make(chan struct{}, max(1, n))} }

// permit is one acquired slot of a gate. Releasing it twice is a bug of the
// caller, which panics instead of handing back a slot that another holder owns.
type permit struct {
	g        *gate
	released atomic.Bool
}

// acquire takes a permit, waiting for one while all are taken. It returns the
// error of ctx, unwrapped, if ctx ends first, and takes nothing then; a context
// that has ended already is refused even when a permit is free, as the pool
// refuses it.
func (g *gate) acquire(ctx context.Context) (*permit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case g.slots <- struct{}{}:
		return &permit{g: g}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// release gives the permit back. It panics if the permit was already released.
func (p *permit) release() {
	if p.released.Swap(true) {
		panic("pgstore: a gate permit was released twice")
	}
	p.g.give()
}

// give frees one slot. It panics when none is taken, which only a permit
// released against the wrong gate or a corrupted count can cause: waiting
// there would hang the caller for good.
func (g *gate) give() {
	select {
	case <-g.slots:
	default:
		panic("pgstore: no permit to give back: the gate holds none")
	}
}

// capacity is the number of permits, and held the number in use.
func (g *gate) capacity() int { return cap(g.slots) }
func (g *gate) held() int     { return len(g.slots) }
