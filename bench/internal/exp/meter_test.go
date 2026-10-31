package exp

import (
	"context"
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
		if got := len(m.Payload(m.Epoch(), size)); got != 8 {
			t.Errorf("len(Payload(_, %d)) = %d, want the 8 bytes of the stamp", size, got)
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
