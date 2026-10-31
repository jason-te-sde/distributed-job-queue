package sim

import (
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
