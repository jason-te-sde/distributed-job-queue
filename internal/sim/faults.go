package sim

import (
	"fmt"
	"time"
)

// FaultConfig says how many faults of each kind to inject, and how long they
// last. Every fault starts at a random time in the first Phase of the run.
type FaultConfig struct {
	// Phase is the length of the fault phase.
	Phase time.Duration
	// WorkerCrashes, WorkerStalls, BrokerCrashes, StoreOutages and
	// Partitions are how many faults of each kind to inject.
	WorkerCrashes, WorkerStalls, BrokerCrashes, StoreOutages, Partitions int
	// StallRange, OutageRange and PartitionRange are the ranges of a
	// stall's, an outage's and a partition's length, and RestartDelay is
	// the range of the time a crashed node stays down.
	StallRange, OutageRange, PartitionRange, RestartDelay [2]time.Duration
}

// FaultCounts says how many faults of each kind took effect. A crash or a
// stall aimed at a node that is already down does nothing and is not counted.
type FaultCounts struct {
	WorkerCrashes, WorkerStalls, BrokerCrashes, StoreOutages, Partitions int
}

// Faults is the schedule of injected faults.
type Faults struct {
	w       *World
	st      *StoreNode
	brokers []*BrokerNode
	workers []*WorkerNode

	counts  FaultCounts
	outages int         // outages in progress; the store returns when none is
	revived map[int]int // restarts so far of each worker, for its new ids
	base    []NodeID    // each worker's original id
}

// ScheduleFaults schedules every fault of cfg in the world, drawing every
// choice (time, target, length) from the world's Rand. A crashed node comes
// back after a delay drawn from RestartDelay, a worker under a new id.
// Overlapping outages keep the store down until the last one ends.
func ScheduleFaults(w *World, net *Network, st *StoreNode, brokers []*BrokerNode, workers []*WorkerNode, cfg FaultConfig) *Faults {
	f := &Faults{w: w, st: st, brokers: brokers, workers: workers, revived: map[int]int{}}
	for _, wk := range workers {
		f.base = append(f.base, wk.ID())
	}
	at := func() time.Duration { return w.Rand.Between(0, cfg.Phase) }
	for range cfg.WorkerCrashes {
		i, when, down := w.Rand.IntN(len(workers)), at(), w.Rand.Between(cfg.RestartDelay[0], cfg.RestartDelay[1])
		w.At(when, "fault worker crash", func() { f.crashWorker(i, down) })
	}
	for range cfg.WorkerStalls {
		i, when, d := w.Rand.IntN(len(workers)), at(), w.Rand.Between(cfg.StallRange[0], cfg.StallRange[1])
		w.At(when, "fault worker stall", func() {
			if workers[i].up {
				workers[i].Stall(d)
				f.counts.WorkerStalls++
			}
		})
	}
	for range cfg.BrokerCrashes {
		i, when, down := w.Rand.IntN(len(brokers)), at(), w.Rand.Between(cfg.RestartDelay[0], cfg.RestartDelay[1])
		w.At(when, "fault broker crash", func() { f.crashBroker(i, down) })
	}
	for range cfg.StoreOutages {
		when, d := at(), w.Rand.Between(cfg.OutageRange[0], cfg.OutageRange[1])
		w.At(when, "fault store outage", func() { f.outage(d) })
	}
	for range cfg.Partitions {
		i, j, when, d := w.Rand.IntN(len(workers)), w.Rand.IntN(len(brokers)), at(), w.Rand.Between(cfg.PartitionRange[0], cfg.PartitionRange[1])
		w.At(when, "fault partition", func() {
			net.Partition(workers[i].ID(), brokers[j].cfg.ID, w.Now+d)
			f.counts.Partitions++
		})
	}
	return f
}

// Counts returns how many faults of each kind have taken effect so far.
func (f *Faults) Counts() FaultCounts {
	return f.counts
}

// crashWorker crashes worker i if it is running and restarts it, under a new
// id, after down.
func (f *Faults) crashWorker(i int, down time.Duration) {
	wk := f.workers[i]
	if !wk.up {
		return
	}
	wk.Crash()
	f.counts.WorkerCrashes++
	f.w.After(down, "fault worker restart", func() {
		f.revived[i]++
		wk.Restart(NodeID(fmt.Sprintf("%s.%d", f.base[i], f.revived[i])), f.w.Rand.seed())
	})
}

// crashBroker crashes broker i if it is running and restarts it after down.
func (f *Faults) crashBroker(i int, down time.Duration) {
	b := f.brokers[i]
	if !b.up {
		return
	}
	b.Crash()
	f.counts.BrokerCrashes++
	f.w.After(down, "fault broker restart", b.Restart)
}

// outage makes the store unavailable for d.
func (f *Faults) outage(d time.Duration) {
	f.outages++
	f.counts.StoreOutages++
	f.st.SetAvailable(false)
	f.w.After(d, "fault store recovery", func() {
		if f.outages--; f.outages == 0 {
			f.st.SetAvailable(true)
		}
	})
}

// seed draws a seed for a node that is restarting.
func (r *Rand) seed() uint64 {
	return r.r.Uint64()
}
