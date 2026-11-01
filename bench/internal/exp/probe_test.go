package exp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// instant is a send function for a system that starts every job after delay,
// and drops the jobs that drop selects.
func instant(m *Meter, delay time.Duration, drop func(k int) bool) func(context.Context, int, time.Time) error {
	return func(_ context.Context, k int, intended time.Time) error {
		if drop != nil && drop(k) {
			return nil
		}
		payload := m.Payload(intended, 16)
		time.AfterFunc(delay, func() { _ = m.ObservePayload("bench", payload) })
		return nil
	}
}

func TestProbeSustainedSystem(t *testing.T) {
	m := NewMeter("bench")
	out, err := Probe(t.Context(), ProbeConfig{Rate: 500, Duration: 200 * time.Millisecond, DrainWait: 2 * time.Second}, m, "bench",
		instant(m, time.Millisecond, nil))
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if out.Sent != 100 || out.Started != 100 || !out.Complete {
		t.Errorf("outcome = sent %d, started %d, complete %v; want 100, 100, true", out.Sent, out.Started, out.Complete)
	}
	if !out.Sustained() {
		t.Errorf("Sustained() = false for a system that starts every job 1ms after it is sent: %+v", out)
	}
	if out.Hist.Count() != 100 {
		t.Errorf("histogram holds %d samples, want 100", out.Hist.Count())
	}
	if out.Offered != 500 {
		t.Errorf("Offered = %v, want 500", out.Offered)
	}
	if out.ThroughputPerSec < 100 {
		t.Errorf("ThroughputPerSec = %v, want a rate of the order of the offered 500/s", out.ThroughputPerSec)
	}
}

func TestProbeBacklogNotFinishedIsNotSustained(t *testing.T) {
	m := NewMeter("bench")
	// Every tenth job is lost, so the backlog never finishes.
	out, err := Probe(t.Context(), ProbeConfig{Rate: 500, Duration: 100 * time.Millisecond, DrainWait: 100 * time.Millisecond}, m, "bench",
		instant(m, time.Millisecond, func(k int) bool { return k%10 == 0 }))
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if out.Complete || out.Started != 45 || out.Sent != 50 {
		t.Errorf("outcome = sent %d, started %d, complete %v; want 50, 45, false", out.Sent, out.Started, out.Complete)
	}
	if out.Sustained() {
		t.Error("Sustained() = true although the backlog was not finished within the drain wait")
	}
	if out.DrainSeconds < 0.09 {
		t.Errorf("DrainSeconds = %v, want the whole 0.1s drain wait when the backlog does not finish", out.DrainSeconds)
	}
}

func TestProbeSlowStartsAreNotSustained(t *testing.T) {
	m := NewMeter("bench")
	// Everything starts, but 1.1s after it was sent: p99 is over the 1s limit.
	out, err := Probe(t.Context(), ProbeConfig{Rate: 50, Duration: 100 * time.Millisecond, DrainWait: 3 * time.Second}, m, "bench",
		instant(m, 1100*time.Millisecond, nil))
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !out.Complete {
		t.Fatalf("the backlog should have finished within the drain wait: %+v", out)
	}
	if out.Sustained() {
		t.Errorf("Sustained() = true with p99 = %v, want false over %v", out.Hist.Quantiles()["p99"], SustainedP99)
	}
}

func TestProbeBacklogFinishedLateIsNotSustained(t *testing.T) {
	// A system that is quick for 95 of 100 jobs, and finishes the other five
	// 2.5s after the load ended, meets the p99 bar by a hair and must still
	// fail the backlog bar.
	m := NewMeter("bench")
	out, err := Probe(t.Context(), ProbeConfig{Rate: 1000, Duration: 100 * time.Millisecond, DrainWait: 5 * time.Second}, m, "bench",
		func(ctx context.Context, k int, intended time.Time) error {
			delay := time.Millisecond
			if k >= 95 {
				delay = 2600 * time.Millisecond
			}
			return instant(m, delay, nil)(ctx, k, intended)
		})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !out.Complete {
		t.Fatalf("the backlog should have finished within the drain wait of 5s: %+v", out)
	}
	if out.DrainSeconds < SustainedDrain.Seconds() {
		t.Errorf("DrainSeconds = %v, want over %v", out.DrainSeconds, SustainedDrain.Seconds())
	}
	if out.Sustained() {
		t.Error("Sustained() = true although the backlog finished more than 2s after the load ended")
	}
}

func TestProbeSendErrorIsNotSustained(t *testing.T) {
	m := NewMeter("bench")
	boom := errors.New("boom")
	var calls atomic.Int32
	out, err := Probe(t.Context(), ProbeConfig{Rate: 1000, Duration: 100 * time.Millisecond, DrainWait: 100 * time.Millisecond}, m, "bench",
		func(context.Context, int, time.Time) error {
			if calls.Add(1) == 20 {
				return boom
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Probe returned %v: a failed send is a result of the probe, not a failure of the harness", err)
	}
	if !errors.Is(out.SendErr, boom) {
		t.Errorf("SendErr = %v, want boom", out.SendErr)
	}
	if out.Sustained() {
		t.Error("Sustained() = true for a probe whose enqueue failed")
	}
}

func TestProbeStopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	m := NewMeter("bench")
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_, err := Probe(ctx, ProbeConfig{Rate: 10, Duration: time.Minute, DrainWait: time.Second}, m, "bench",
		func(context.Context, int, time.Time) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Probe error = %v, want context.Canceled: the user's interrupt is not a verdict on the rate", err)
	}
}

func TestProbeDuplicateStartDoesNotStandInForAJobThatNeverStarted(t *testing.T) {
	m := NewMeter("bench")
	// Job 0 is lost, and job 1 starts twice: the starts add up to the 50 jobs
	// sent, though only 49 of them ran.
	out, err := Probe(t.Context(), ProbeConfig{Rate: 500, Duration: 100 * time.Millisecond, DrainWait: 200 * time.Millisecond}, m, "bench",
		func(_ context.Context, k int, intended time.Time) error {
			if k == 0 {
				return nil
			}
			payload := m.Payload(intended, 16)
			start := func() { _ = m.ObservePayload("bench", payload) }
			time.AfterFunc(time.Millisecond, start)
			if k == 1 {
				time.AfterFunc(2*time.Millisecond, start)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if out.Sent != 50 || out.Started != 49 || out.Duplicates != 1 || out.Complete {
		t.Errorf("outcome = sent %d, started %d, duplicates %d, complete %v; want 50, 49, 1, false", out.Sent, out.Started, out.Duplicates, out.Complete)
	}
	if out.Hist.Count() != 49 {
		t.Errorf("histogram holds %d samples, want 49: one per job that started", out.Hist.Count())
	}
	if got := out.Metrics()["duplicate_starts"]; got != 1 {
		t.Errorf("Metrics()[duplicate_starts] = %v, want 1", got)
	}
}
