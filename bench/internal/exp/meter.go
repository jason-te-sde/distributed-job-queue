package exp

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/hist"
)

// stampBytes is the size of the stamp that every benchmark payload starts with.
const stampBytes = 8

// noStart is the value of a tenant's last-start offset before any job started.
const noStart = math.MinInt64

// Meter measures enqueue-to-start latency for the jobs of an open-loop run.
//
// The payload of every job carries its intended time, as nanoseconds since the
// meter's epoch, which the producer and the handler read off the same monotonic
// clock because they run in one process. The handler gives the payload to
// ObservePayload when the job starts, and the meter records the time since the
// intended time, never the time since the send began.
type Meter struct {
	epoch   time.Time
	tenants map[string]*tenantMeter
}

type tenantMeter struct {
	hist    *hist.Hist
	started atomic.Int64
	last    atomic.Int64 // offset of the latest start, or noStart
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

// Payload returns a payload of size bytes that carries the intended time. A size
// below the 8 bytes of the stamp gives those 8.
func (m *Meter) Payload(intended time.Time, size int) []byte {
	p := make([]byte, max(size, stampBytes))
	binary.BigEndian.PutUint64(p, uint64(intended.Sub(m.epoch)))
	return p
}

// Stamp is the intended time of a job as the meter counts it, for a system that
// carries it in a field of its own instead of in the payload.
func (m *Meter) Stamp(intended time.Time) int64 { return int64(intended.Sub(m.epoch)) }

// ObservePayload records the start of a job of the tenant whose payload
// Payload made. It fails for a tenant the meter was not told of and for a
// payload that is too short to hold a stamp, which means the job is not one of
// the run's.
func (m *Meter) ObservePayload(tenant string, payload []byte) error {
	if len(payload) < stampBytes {
		return fmt.Errorf("meter: a payload of %d bytes holds no stamp, want at least %d", len(payload), stampBytes)
	}
	return m.Observe(tenant, int64(binary.BigEndian.Uint64(payload)))
}

// Observe records the start of a job of the tenant, at the current time, given
// the stamp of its intended time.
func (m *Meter) Observe(tenant string, stamp int64) error {
	tm, ok := m.tenants[tenant]
	if !ok {
		return fmt.Errorf("meter: the job belongs to tenant %q, which the meter does not measure", tenant)
	}
	now := int64(time.Since(m.epoch))
	tm.hist.Record(time.Duration(now - stamp))
	tm.started.Add(1)
	for {
		last := tm.last.Load()
		if now <= last || tm.last.CompareAndSwap(last, now) {
			return nil
		}
	}
}

// Hist is the latency histogram of the tenant, or nil if the meter does not
// measure it.
func (m *Meter) Hist(tenant string) *hist.Hist {
	if tm, ok := m.tenants[tenant]; ok {
		return tm.hist
	}
	return nil
}

// Started is the number of jobs of the tenant that have started.
func (m *Meter) Started(tenant string) int64 {
	if tm, ok := m.tenants[tenant]; ok {
		return tm.started.Load()
	}
	return 0
}

// StartedTotal is the number of jobs of every tenant that have started.
func (m *Meter) StartedTotal() int64 {
	var n int64
	for _, tm := range m.tenants {
		n += tm.started.Load()
	}
	return n
}

// LastStart is the time at which the latest job of any tenant started, or the
// zero time if none has.
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

// Await waits until n jobs have started in all, for at most d, and reports
// whether they did. It returns false at once when ctx ends.
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
