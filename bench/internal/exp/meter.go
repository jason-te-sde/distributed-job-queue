package exp

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
)

// StampBytes is the size of the stamp that every benchmark payload starts with:
// the intended time and the sequence number, 8 bytes each.
const StampBytes = 16

// noStart is the value of a tenant's last-start offset before any job started.
const noStart = math.MinInt64

// Meter measures enqueue-to-start latency for the jobs of an open-loop run.
//
// The payload of every job starts with its stamp: its intended time, as
// nanoseconds since the meter's epoch, which the producer and the handler read
// off the same monotonic clock because they run in one process, and a sequence
// number that the meter gives it when the payload is made. The handler gives the
// payload to ObservePayload when the job starts, and the meter records the time
// since the intended time, never the time since the send began.
//
// The meter counts jobs, not starts. A job can start more than once: a producer
// call that commits after its client deadline is retried under the same
// idempotency key, and when the first job has run and been pruned by then, which
// the store allows because it dedups only for as long as it keeps the job, the
// key matches nothing and a second job is made from the same payload; a
// redelivery after a lost lease starts the same job again. Only the first start
// of a job counts toward Started, Await and LastStart and is recorded in the
// latency histogram. A later start counts toward Duplicates alone, so that it
// cannot stand in for a job that has not started.
//
// A job is told from another by its sequence number, not by its intended time,
// because jobs can share an intended time: the flood of the noisy-neighbour
// experiment stamps each job with the time it is made, and two made within one
// tick of the clock get the same. Identity is per tenant.
//
// The sequence is meter-wide and dense, from 1, so each tenant keeps the jobs it
// has seen as a bitset indexed by sequence number, which grows to the highest
// number it has seen and at most to the meter's high-water mark. The memory is
// therefore at most k tenants times n bits for a meter that has issued n numbers:
// for example 1.1 MB per tenant for the 9 million jobs of 30 minutes at 5000 a
// second. The slice grows by about 1.25 times, so the old and the new array
// together peak at about 2.25 times that while it does. Observing a start costs
// one lock and one bit. A sequence number of 0, or above the highest issued, is
// refused, so no payload can grow a set past the high-water mark.
type Meter struct {
	epoch   time.Time
	seq     atomic.Uint64 // the last sequence number issued
	tenants map[string]*tenantMeter
}

type tenantMeter struct {
	hist       *hist.Hist
	started    atomic.Int64
	duplicates atomic.Int64
	last       atomic.Int64 // offset of the latest first start, or noStart

	mu   sync.Mutex
	seen []uint64 // bit s is set once the job of sequence number s has started
}

// Stamp is what a job carries so that the meter can tell when it was meant to
// be sent and which job it is.
type Stamp struct {
	// Intended is the intended time, in nanoseconds since the meter's epoch.
	Intended int64
	// Seq is the sequence number of the job, from 1.
	Seq uint64
}

// NewMeter returns a meter for jobs of the named tenants, whose epoch is now.
func NewMeter(tenants ...string) *Meter {
	m := &Meter{epoch: time.Now(), tenants: make(map[string]*tenantMeter, len(tenants))}
	for _, t := range tenants {
		tm := &tenantMeter{hist: hist.New()}
		tm.last.Store(noStart)
		m.tenants[t] = tm
	}
	return m
}

// Epoch is the instant that the stamps count from.
func (m *Meter) Epoch() time.Time { return m.epoch }

// NewStamp returns the stamp of a new job whose intended time is given, for a
// system that carries it in fields of its own instead of in the payload. No two
// stamps are equal, whatever their intended times.
func (m *Meter) NewStamp(intended time.Time) Stamp {
	return Stamp{Intended: int64(intended.Sub(m.epoch)), Seq: m.seq.Add(1)}
}

// Payload returns the payload of a new job, of size bytes, that carries its
// stamp. A size below the 16 bytes of the stamp gives those 16.
func (m *Meter) Payload(intended time.Time, size int) []byte {
	s := m.NewStamp(intended)
	p := make([]byte, max(size, StampBytes))
	binary.BigEndian.PutUint64(p, uint64(s.Intended))
	binary.BigEndian.PutUint64(p[8:], s.Seq)
	return p
}

// ObservePayload records the start of a job of the tenant whose payload
// Payload made. It fails for a tenant the meter was not told of, for a payload
// that is too short to hold a stamp, and for a sequence number the meter has not
// issued, which is 0 or above the highest it has issued. A stamp that another
// meter made with a number the meter has issued is not told from its own.
func (m *Meter) ObservePayload(tenant string, payload []byte) error {
	if len(payload) < StampBytes {
		return fmt.Errorf("meter: a payload of %d bytes holds no stamp, want at least %d", len(payload), StampBytes)
	}
	return m.Observe(tenant, Stamp{
		Intended: int64(binary.BigEndian.Uint64(payload)),
		Seq:      binary.BigEndian.Uint64(payload[8:]),
	})
}

// Observe records the start of the job of the tenant that has the stamp, at the
// current time. If the job has started before, it counts the start as a
// duplicate and records nothing else. It is safe to call from many goroutines.
func (m *Meter) Observe(tenant string, s Stamp) error {
	tm, ok := m.tenants[tenant]
	if !ok {
		return fmt.Errorf("meter: the job belongs to tenant %q, which the meter does not measure", tenant)
	}
	if s.Seq == 0 || s.Seq > m.seq.Load() {
		return fmt.Errorf("meter: the job has sequence number %d, which the meter did not issue: it is not one of the run's", s.Seq)
	}
	if !tm.first(s.Seq) {
		tm.duplicates.Add(1)
		return nil
	}
	now := int64(time.Since(m.epoch))
	tm.hist.Record(time.Duration(now - s.Intended))
	// The time of the start is stored before the job is counted, so that a reader
	// that sees it counted in Started and then reads LastStart, as Probe does,
	// never finds LastStart behind it.
	for {
		last := tm.last.Load()
		if now <= last || tm.last.CompareAndSwap(last, now) {
			break
		}
	}
	tm.started.Add(1)
	return nil
}

// first marks the job of the sequence number as started and reports whether it
// had not been. The set grows to hold the number, which Observe has checked was
// issued.
func (tm *tenantMeter) first(seq uint64) bool {
	word, bit := seq/64, uint64(1)<<(seq%64)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if word >= uint64(len(tm.seen)) {
		tm.seen = append(tm.seen, make([]uint64, word+1-uint64(len(tm.seen)))...)
	}
	if tm.seen[word]&bit != 0 {
		return false
	}
	tm.seen[word] |= bit
	return true
}

// Hist is the latency histogram of the tenant, or nil if the meter does not
// measure it.
func (m *Meter) Hist(tenant string) *hist.Hist {
	if tm, ok := m.tenants[tenant]; ok {
		return tm.hist
	}
	return nil
}

// Started is the number of jobs of the tenant that have started, each counted
// once.
func (m *Meter) Started(tenant string) int64 {
	if tm, ok := m.tenants[tenant]; ok {
		return tm.started.Load()
	}
	return 0
}

// StartedTotal is the number of jobs of every tenant that have started, each
// counted once.
func (m *Meter) StartedTotal() int64 {
	var n int64
	for _, tm := range m.tenants {
		n += tm.started.Load()
	}
	return n
}

// Duplicates is the number of starts of the tenant's jobs that were not the
// first start of their job.
func (m *Meter) Duplicates(tenant string) int64 {
	if tm, ok := m.tenants[tenant]; ok {
		return tm.duplicates.Load()
	}
	return 0
}

// DuplicatesTotal is Duplicates for every tenant together.
func (m *Meter) DuplicatesTotal() int64 {
	var n int64
	for _, tm := range m.tenants {
		n += tm.duplicates.Load()
	}
	return n
}

// LastStart is the time at which the latest job of any tenant first started, or
// the zero time if none has. A later start of a job that had started does not
// move it.
func (m *Meter) LastStart() time.Time { return m.lastStart(nil) }

// LastStartOf is LastStart for the tenant.
func (m *Meter) LastStartOf(tenant string) time.Time { return m.lastStart([]string{tenant}) }

func (m *Meter) lastStart(only []string) time.Time {
	latest := int64(noStart)
	for name, tm := range m.tenants {
		if len(only) > 0 && only[0] != name {
			continue
		}
		latest = max(latest, tm.last.Load())
	}
	if latest == noStart {
		return time.Time{}
	}
	return m.epoch.Add(time.Duration(latest))
}

// Await waits until n distinct jobs have started in all, for at most d, and
// reports whether they did. It returns false at once when ctx ends.
func (m *Meter) Await(ctx context.Context, n int64, d time.Duration) bool {
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return false
		}
		if m.StartedTotal() >= n {
			return true
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			return m.StartedTotal() >= n
		case <-ctx.Done():
			return false
		}
	}
}
