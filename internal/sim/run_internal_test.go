package sim

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestRunHealsTheNetworkAndTheStoreAtTheEndOfThePhase makes the faults last
// past the phase: a lossy network, and outages and partitions that start just
// before the end and run for minutes. Nothing may be lost after the phase.
func TestRunHealsTheNetworkAndTheStoreAtTheEndOfThePhase(t *testing.T) {
	cfg := DefaultConfig(12)
	cfg.Net.DropP, cfg.Net.DupP = 0.05, 0.02
	cfg.Faults.StoreOutages, cfg.Faults.Partitions = 3, 3
	cfg.Faults.OutageRange = [2]time.Duration{10 * time.Minute, 10 * time.Minute}
	cfg.Faults.PartitionRange = [2]time.Duration{10 * time.Minute, 10 * time.Minute}
	s := newSimulation(cfg)
	phase := cfg.Faults.Phase

	for s.violation == nil && s.w.Now < phase+time.Second && s.step() {
	}
	if s.violation != nil {
		t.Fatalf("violation before the phase ended: %v", s.violation)
	}
	before := s.net.Stats()
	if before.Dropped == 0 || before.Partitioned == 0 {
		t.Fatalf("network stats %+v at the end of the phase: want drops and partitioned messages, or the test shows nothing", before)
	}
	if !s.st.available {
		t.Error("the store is still unavailable just after the phase")
	}
	for s.violation == nil && s.w.Now < phase+5*time.Minute && s.step() {
	}
	after := s.net.Stats()
	if after.Dropped != before.Dropped || after.Duplicated != before.Duplicated || after.Partitioned != before.Partitioned {
		t.Errorf("after the phase the network went on losing messages: %+v then %+v", before, after)
	}
	if after.Sent <= before.Sent {
		t.Errorf("no message was sent in 5 minutes after the phase: %+v then %+v", before, after)
	}
}

// TestRunFailsWhenTheProducerCannotConfirmItsJobs drops every message until
// the phase ends and gives no time to heal, so that no job is confirmed. The
// run must say so rather than pass with nothing to check.
func TestRunFailsWhenTheProducerCannotConfirmItsJobs(t *testing.T) {
	cfg := DefaultConfig(13)
	cfg.Net.DropP = 1
	cfg.Heal = 0
	_, err := Run(cfg)
	if err == nil || !strings.Contains(err.Error(), "seed 13") || !strings.Contains(err.Error(), "producer got") {
		t.Errorf("err = %v, want an error that names the seed and the producer's confirmations", err)
	}
}

// overstrictSimulation is a simulation whose checker holds the workers to
// twice their margin, so that the first lease a worker holds breaks S5, and
// nothing else in the run does.
func overstrictSimulation(cfg Config) *simulation {
	s := newSimulation(cfg)
	s.checker = NewChecker(CheckerConfig{
		Margin: 2 * cfg.Margin, MaxTTL: cfg.TTL, MaxLongPoll: runLongPoll, MaxDriftPPM: maxClockDriftPPM,
		Retention: cfg.Retention, ReapInterval: runReapInterval,
	}, s.sink)
	return s
}

// TestRunChecksWhatWorkersHoldAfterEveryEvent steps a simulation, event by
// event, and needs the hold check inside step to raise S5. It stops at the
// end of the fault phase, so that a step that checks nothing fails the test
// rather than running forever.
func TestRunChecksWhatWorkersHoldAfterEveryEvent(t *testing.T) {
	cfg := DefaultConfig(3)
	s := overstrictSimulation(cfg)
	for s.violation == nil && s.w.Now < cfg.Faults.Phase && s.step() {
	}
	var v *Violation
	if !errors.As(s.violation, &v) || v.Invariant != "S5" {
		t.Fatalf("violation = %v at %v, want S5 from the hold check of step", s.violation, s.w.Now)
	}
}

// TestRunStopsAtTheFirstViolation runs the same simulation twice. Stepping it
// by hand counts the events up to and including the one that breaks S5;
// execute must fire exactly those and no more, and return the violation.
func TestRunStopsAtTheFirstViolation(t *testing.T) {
	cfg := DefaultConfig(3)
	counted := overstrictSimulation(cfg)
	events := 0
	for counted.violation == nil && counted.w.Now < cfg.Faults.Phase && counted.step() {
		events++
	}
	if counted.violation == nil {
		t.Fatal("the counting run found no violation")
	}

	stats, err := overstrictSimulation(cfg).execute()
	var v *Violation
	if !errors.As(err, &v) || v.Invariant != "S5" {
		t.Fatalf("execute returned %v, want the S5 violation", err)
	}
	if stats.Events != events {
		t.Errorf("execute fired %d events, want the %d up to the violation: a violation must stop the run", stats.Events, events)
	}
}
