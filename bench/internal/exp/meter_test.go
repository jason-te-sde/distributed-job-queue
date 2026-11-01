package exp

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMeterPayloadCarriesTheIntendedTime(t *testing.T) {
	m := NewMeter("t")
	intended := m.Epoch().Add(-300 * time.Millisecond) // a job whose time was 300ms before the meter began
	payload := m.Payload(intended, 64)
	if len(payload) != 64 {
		t.Fatalf("len(Payload(_, 64)) = %d, want 64", len(payload))
	}
	if err := m.ObservePayload("t", payload); err != nil {
		t.Fatalf("ObservePayload: %v", err)
	}
	// The latency is measured from the intended time, so it is at least the
	// 300ms the job was overdue before the meter even began.
	if got := m.Hist("t").Quantiles()["max"]; got < 300*time.Millisecond {
		t.Errorf("latency = %v, want at least 300ms: it is measured from the intended time", got)
	}
}

func TestMeterPayloadHasAFloor(t *testing.T) {
	m := NewMeter("t")
	for _, size := range []int{0, 1, 7} {
		if got := len(m.Payload(m.Epoch(), size)); got != 16 {
			t.Errorf("len(Payload(_, %d)) = %d, want the 16 bytes of the stamp", size, got)
		}
	}
}

func TestMeterRejectsWhatItDidNotStamp(t *testing.T) {
	m := NewMeter("t")
	if err := m.ObservePayload("t", []byte("short")); err == nil {
		t.Error("ObservePayload accepted a payload of 5 bytes")
	}
	if err := m.ObservePayload("other", m.Payload(m.Epoch(), 8)); err == nil {
		t.Error("ObservePayload accepted a tenant the meter was not told of")
	}
	if got := m.Started("t"); got != 0 {
		t.Errorf("Started = %d after two refused observations, want 0", got)
	}
}

func TestMeterCountsTenantsSeparately(t *testing.T) {
	m := NewMeter("a", "b")
	for range 3 {
		if err := m.ObservePayload("a", m.Payload(m.Epoch(), 8)); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.ObservePayload("b", m.Payload(m.Epoch(), 8)); err != nil {
		t.Fatal(err)
	}
	if a, b := m.Started("a"), m.Started("b"); a != 3 || b != 1 {
		t.Errorf("Started = a %d, b %d; want 3 and 1", a, b)
	}
	if a, b := m.Hist("a").Count(), m.Hist("b").Count(); a != 3 || b != 1 {
		t.Errorf("histogram counts = a %d, b %d; want 3 and 1", a, b)
	}
	if got := m.StartedTotal(); got != 4 {
		t.Errorf("StartedTotal = %d, want 4", got)
	}
}

func TestMeterLastStartIsTheLatest(t *testing.T) {
	m := NewMeter("t")
	if !m.LastStart().IsZero() {
		t.Errorf("LastStart = %v before any job started, want the zero time", m.LastStart())
	}
	if err := m.ObservePayload("t", m.Payload(m.Epoch(), 8)); err != nil {
		t.Fatal(err)
	}
	first := m.LastStart()
	time.Sleep(5 * time.Millisecond)
	if err := m.ObservePayload("t", m.Payload(m.Epoch(), 8)); err != nil {
		t.Fatal(err)
	}
	if second := m.LastStart(); !second.After(first) {
		t.Errorf("LastStart went from %v to %v, want it to advance", first, second)
	}
}

func TestMeterAwait(t *testing.T) {
	m := NewMeter("t")
	go func() {
		time.Sleep(20 * time.Millisecond)
		for range 5 {
			_ = m.ObservePayload("t", m.Payload(m.Epoch(), 8))
		}
	}()
	if !m.Await(t.Context(), 5, 10*time.Second) {
		t.Fatal("Await(5) = false, want true once five jobs started")
	}
	if m.Await(t.Context(), 6, 50*time.Millisecond) {
		t.Error("Await(6) = true although only five jobs started")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if m.Await(ctx, 6, 10*time.Second) {
		t.Error("Await returned true for a cancelled context")
	}
	if !m.Await(t.Context(), 0, 0) {
		t.Error("Await(0) = false, want true: nothing to wait for")
	}
}

func TestMeterRejectsAPayloadShorterThanTheStamp(t *testing.T) {
	m := NewMeter("t")
	// 8 bytes hold the intended time but no sequence number, and 15 is one byte
	// short of the stamp.
	for _, n := range []int{0, 8, 15} {
		if err := m.ObservePayload("t", make([]byte, n)); err == nil {
			t.Errorf("ObservePayload accepted a payload of %d bytes, which holds no whole stamp", n)
		}
	}
	if got := m.Started("t"); got != 0 {
		t.Errorf("Started = %d after refused observations, want 0", got)
	}
}

func TestMeterRejectsAJobItDidNotStamp(t *testing.T) {
	// A sequence number that the meter has not issued, which is 0 or above its
	// high-water mark, is refused rather than counted, and does not grow the set
	// of jobs seen. A stamp from another meter is told from the meter's own only
	// by that: the foreign payload here has a number above the mark, and one below
	// it would pass as the meter's own.
	m := NewMeter("t")
	other := NewMeter("t")
	for range 5 {
		other.Payload(other.Epoch(), 16)
	}
	huge := m.Payload(m.Epoch(), 16)
	binary.BigEndian.PutUint64(huge[8:], 1<<62)
	for name, payload := range map[string][]byte{
		"unstamped": make([]byte, 64),
		"foreign":   other.Payload(other.Epoch(), 16),
		"huge":      huge,
	} {
		if err := m.ObservePayload("t", payload); err == nil {
			t.Errorf("ObservePayload accepted the %s payload, whose sequence number the meter never issued", name)
		}
	}
	if err := m.ObservePayload("t", m.Payload(m.Epoch(), 16)); err != nil {
		t.Errorf("ObservePayload refused a payload that the meter made: %v", err)
	}
	if got := m.Started("t"); got != 1 {
		t.Errorf("Started = %d, want 1: only the payload the meter made started", got)
	}
}

func TestMeterCountsAJobOnceWhenItStartsTwice(t *testing.T) {
	m := NewMeter("a", "b")
	payload := m.Payload(m.Epoch(), 16)
	for range 2 {
		if err := m.ObservePayload("a", payload); err != nil {
			t.Fatal(err)
		}
	}
	if a, total := m.Started("a"), m.StartedTotal(); a != 1 || total != 1 {
		t.Errorf("Started = %d, StartedTotal = %d after one job started twice; want 1 and 1", a, total)
	}
	if a, b, total := m.Duplicates("a"), m.Duplicates("b"), m.DuplicatesTotal(); a != 1 || b != 0 || total != 1 {
		t.Errorf("Duplicates = a %d, b %d, total %d; want 1, 0 and 1", a, b, total)
	}
}

func TestMeterIdentityIsPerTenant(t *testing.T) {
	m := NewMeter("a", "b")
	payload := m.Payload(m.Epoch(), 16)
	for _, tenant := range []string{"a", "b"} {
		if err := m.ObservePayload(tenant, payload); err != nil {
			t.Fatal(err)
		}
	}
	if a, b := m.Started("a"), m.Started("b"); a != 1 || b != 1 {
		t.Errorf("Started = a %d, b %d; want 1 each: the same payload under two tenants is two jobs", a, b)
	}
	if got := m.DuplicatesTotal(); got != 0 {
		t.Errorf("DuplicatesTotal = %d, want 0", got)
	}
}

func TestMeterJobsOfOneInstantAreDistinct(t *testing.T) {
	m := NewMeter("t")
	at := m.Epoch().Add(time.Second)
	a, b := m.Payload(at, 16), m.Payload(at, 16)
	if bytes.Equal(a, b) {
		t.Fatal("two jobs with the same intended time have the same payload, so one cannot be told from a second start of the other")
	}
	for _, p := range [][]byte{a, b} {
		if err := m.ObservePayload("t", p); err != nil {
			t.Fatal(err)
		}
	}
	if started, dup := m.Started("t"), m.Duplicates("t"); started != 2 || dup != 0 {
		t.Errorf("Started = %d, Duplicates = %d for two jobs of one instant; want 2 and 0", started, dup)
	}
	if s1, s2 := m.NewStamp(at), m.NewStamp(at); s1 == s2 {
		t.Errorf("two stamps of one instant are equal: %+v", s1)
	}
}

func TestMeterObserveCountsAStampOnce(t *testing.T) {
	m := NewMeter("t")
	s := m.NewStamp(m.Epoch())
	for range 3 {
		if err := m.Observe("t", s); err != nil {
			t.Fatal(err)
		}
	}
	if started, dup := m.Started("t"), m.Duplicates("t"); started != 1 || dup != 2 {
		t.Errorf("Started = %d, Duplicates = %d after one stamp started three times; want 1 and 2", started, dup)
	}
}

func TestMeterAwaitDoesNotCountADuplicateAsAJob(t *testing.T) {
	m := NewMeter("t")
	a, b := m.Payload(m.Epoch(), 16), m.Payload(m.Epoch(), 16)
	for range 2 {
		if err := m.ObservePayload("t", a); err != nil {
			t.Fatal(err)
		}
	}
	if m.Await(t.Context(), 2, 50*time.Millisecond) {
		t.Error("Await(2) = true after one job started twice and another never started")
	}
	if err := m.ObservePayload("t", b); err != nil {
		t.Fatal(err)
	}
	if !m.Await(t.Context(), 2, 10*time.Second) {
		t.Error("Await(2) = false after two distinct jobs started")
	}
}

func TestMeterDuplicateStartRecordsNoLatency(t *testing.T) {
	m := NewMeter("t")
	payload := m.Payload(m.Epoch(), 16)
	if err := m.ObservePayload("t", payload); err != nil {
		t.Fatal(err)
	}
	before := m.Hist("t").Quantiles()["max"]
	time.Sleep(30 * time.Millisecond) // a second start now would show as a latency 30ms longer
	if err := m.ObservePayload("t", payload); err != nil {
		t.Fatal(err)
	}
	if got := m.Hist("t").Count(); got != 1 {
		t.Errorf("histogram holds %d samples after one job started twice, want 1", got)
	}
	if after := m.Hist("t").Quantiles()["max"]; after != before {
		t.Errorf("max latency went from %v to %v on a second start, want it unchanged", before, after)
	}
}

func TestMeterLastStartIsTheLastFirstStart(t *testing.T) {
	m := NewMeter("t", "u")
	a := m.Payload(m.Epoch(), 16)
	if err := m.ObservePayload("t", a); err != nil {
		t.Fatal(err)
	}
	first := m.LastStart()
	time.Sleep(5 * time.Millisecond)
	if err := m.ObservePayload("t", a); err != nil {
		t.Fatal(err)
	}
	if got := m.LastStart(); !got.Equal(first) {
		t.Errorf("LastStart moved from %v to %v on a second start of the same job, want it unchanged", first, got)
	}
	if got := m.LastStartOf("t"); !got.Equal(first) {
		t.Errorf("LastStartOf = %v, want %v", got, first)
	}
	time.Sleep(5 * time.Millisecond)
	if err := m.ObservePayload("t", m.Payload(m.Epoch(), 16)); err != nil {
		t.Fatal(err)
	}
	if got := m.LastStart(); !got.After(first) {
		t.Errorf("LastStart = %v after a new job started, want it after %v", got, first)
	}
}

func TestMeterObserveIsSafeToCallConcurrently(t *testing.T) {
	const jobs, starters = 500, 8
	m := NewMeter("t")
	payloads := make([][]byte, jobs)
	for i := range payloads {
		payloads[i] = m.Payload(m.Epoch(), 16)
	}
	var wg sync.WaitGroup
	for range starters {
		wg.Go(func() {
			for _, p := range payloads {
				if err := m.ObservePayload("t", p); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if got := m.Started("t"); got != jobs {
		t.Errorf("Started = %d, want %d: each job once however many handlers started it", got, jobs)
	}
	if got := m.Duplicates("t"); got != jobs*(starters-1) {
		t.Errorf("Duplicates = %d, want %d", got, jobs*(starters-1))
	}
	if got := m.Hist("t").Count(); got != jobs {
		t.Errorf("histogram holds %d samples, want %d", got, jobs)
	}
}

func TestMeterLastStartIsNeverBehindStarted(t *testing.T) {
	// Probe reads Started and then LastStart, so a job that is counted must
	// already be in LastStart. One goroutine starts jobs one after the other,
	// noting the time just before each start, and another reads: after it sees
	// job i counted, LastStart must not be earlier than that note.
	const jobs = 300_000
	m := NewMeter("t")
	before := make([]atomic.Int64, jobs+1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= jobs; i++ {
			before[i].Store(int64(time.Since(m.Epoch())))
			if err := m.Observe("t", m.NewStamp(m.Epoch())); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
		}
		started := m.Started("t")
		if started == 0 {
			continue
		}
		if last, noted := m.LastStart().Sub(m.Epoch()), time.Duration(before[started].Load()); last < noted {
			t.Errorf("job %d is counted in Started but LastStart is %v, before the %v at which that job began to start", started, last, noted)
			<-done
			return
		}
	}
}
