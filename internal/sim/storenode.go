package sim

import (
	"fmt"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/store"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/memstore"
)

// Latency of one hop to or from the store: each call pays it once on the way
// in and once on the way out.
const (
	storeLatencyMin = 100 * time.Microsecond
	storeLatencyMax = 2 * time.Millisecond
)

// StoreNode is the simulated database: a memstore.Store on the world's clock,
// reached through calls that take virtual time and fail while the database is
// unavailable. Every row change it makes is handed to the observers.
type StoreNode struct {
	w         *World
	mem       *memstore.Store
	observers []func(memstore.Change)
	available bool

	// pending holds the changes of the call in progress. The store reports a
	// change while it holds its lock, and an observer that read the store
	// then would deadlock, so the callback only queues and Call delivers
	// once the store call has returned.
	pending []memstore.Change
}

// NewStoreNode returns an available, empty store whose clock is
// World.StoreTime. The observers see every row change, in order, in the world
// event of the call that made it, after the store has finished that call:
// they may read the store, but must not write to it.
func NewStoreNode(w *World, observers ...func(memstore.Change)) *StoreNode {
	s := &StoreNode{w: w, observers: observers, available: true}
	s.mem = memstore.New(memstore.Options{
		Clock:    w.StoreTime,
		Observer: func(ch memstore.Change) { s.pending = append(s.pending, ch) },
	})
	return s
}

// subscribe adds an observer that runs after the ones given to NewStoreNode.
func (s *StoreNode) subscribe(f func(memstore.Change)) {
	s.observers = append(s.observers, f)
}

// Mem returns the underlying store for reading, by the checker and for the
// final snapshot. A write through it skips the call delay and the fan-out of
// changes, so only reads belong here.
func (s *StoreNode) Mem() *memstore.Store {
	return s.mem
}

// SetAvailable starts or ends an outage. A call that finds the store
// unavailable when it reaches it fails with store.ErrUnavailable.
func (s *StoreNode) SetAvailable(ok bool) {
	s.available = ok
}

// Call runs fn against the store as a simulated round trip. fn runs in a
// world event Between(100µs, 2ms) from now, and done runs in another that
// long after fn, with the error fn returned. While the store is unavailable
// when fn would run, fn does not run and done gets an error that wraps
// store.ErrUnavailable. The label names both events in the trace. A nil done
// is allowed for a caller that does not care how the call ended.
func (s *StoreNode) Call(label string, fn func(st store.Store) error, done func(err error)) {
	s.w.After(s.w.Rand.Between(storeLatencyMin, storeLatencyMax), "store "+label, func() {
		var err error
		if s.available {
			err = fn(s.mem)
			s.deliver()
		} else {
			err = fmt.Errorf("sim store: %s: %w", label, store.ErrUnavailable)
		}
		if done != nil {
			s.w.After(s.w.Rand.Between(storeLatencyMin, storeLatencyMax), "store done "+label, func() { done(err) })
		}
	})
}

// deliver hands the queued changes to the observers.
func (s *StoreNode) deliver() {
	pending := s.pending
	s.pending = nil
	for _, ch := range pending {
		for _, o := range s.observers {
			o(ch)
		}
	}
}
