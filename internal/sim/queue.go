package sim

import (
	"container/heap"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"time"
)

// Epoch is the wall-clock time that virtual time zero stands for.
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// event is one scheduled call. seq is the order in which it was scheduled,
// which breaks ties between events at the same virtual time.
type event struct {
	at    time.Duration
	seq   uint64
	label string
	fn    func()
}

// eventHeap orders events by (at, seq), earliest first.
type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) { *h = append(*h, x.(*event)) }

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil // let the event, and what its closure holds, be collected
	*h = old[:n-1]
	return e
}

// World is the single-goroutine scheduler. All simulated activity is events
// in its queue, and nothing runs except inside Step, so a run needs no locks
// and cannot race. Create one with NewWorld.
type World struct {
	// Now is the virtual time since Epoch. Only Step advances it, and an
	// event sees the time it was scheduled for.
	Now time.Duration
	// Rand is the world's random source, seeded by NewWorld.
	Rand *Rand

	events eventHeap
	seq    uint64
	trace  hash.Hash64
}

// NewWorld returns an empty world at virtual time zero whose Rand is
// NewRand(seed).
func NewWorld(seed uint64) *World {
	return &World{Rand: NewRand(seed), trace: fnv.New64a()}
}

// At schedules fn to run when virtual time reaches at. Events at the same
// time run in the order they were scheduled. The label names the event in
// the trace hash. At panics if at is before Now: that is a bug in the
// scenario, and running the event late would hide it.
func (w *World) At(at time.Duration, label string, fn func()) {
	if at < w.Now {
		panic("sim: event scheduled in the past: " + label)
	}
	heap.Push(&w.events, &event{at: at, seq: w.seq, label: label, fn: fn})
	w.seq++
}

// After schedules fn to run d after Now. It panics if d is negative.
func (w *World) After(d time.Duration, label string, fn func()) {
	w.At(w.Now+d, label, fn)
}

// Step fires the earliest event, the lowest seq first among equals, after
// moving Now to its time. It reports false, and does nothing, when the queue
// is empty.
func (w *World) Step() bool {
	if len(w.events) == 0 {
		return false
	}
	e := heap.Pop(&w.events).(*event)
	w.Now = e.at
	// Length-prefixing the label keeps ("a", "bc") and ("ab", "c") apart.
	var buf [24]byte
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.at))
	binary.LittleEndian.PutUint64(buf[8:], e.seq)
	binary.LittleEndian.PutUint64(buf[16:], uint64(len(e.label)))
	w.trace.Write(buf[:])
	w.trace.Write([]byte(e.label))
	e.fn()
	return true
}

// StoreTime returns the current virtual time as a wall-clock time for a
// simulated store: Epoch plus Now, cut down to whole microseconds, which is
// the precision every store time has.
func (w *World) StoreTime() time.Time {
	return Epoch.Add(w.Now).Truncate(time.Microsecond)
}

// TraceHash returns the FNV-64a hash of the (time, seq, label) of every
// event fired so far, in firing order. Two runs of one scenario agree on it
// exactly when they fired the same events in the same order.
func (w *World) TraceHash() uint64 {
	return w.trace.Sum64()
}
