// Package sim is the deterministic simulation kernel: a single-goroutine
// scheduler over virtual time, seeded randomness, per-node clocks that
// drift, and a network that drops, duplicates, delays and partitions
// messages.
//
// A run is a function of its seed. Nothing here reads the wall clock, starts
// a goroutine or draws from a global random source: virtual time moves only
// when World.Step fires the next event, and every random choice comes from a
// World's Rand. Running the same scenario twice with the same seed therefore
// fires the same events in the same order, which World.TraceHash makes
// checkable, and a failing seed replays exactly.
//
// internal/archtest enforces the parts of this that a compiler cannot: the
// sim-determinism rule rejects a go statement and the time package's clock
// functions anywhere under internal/sim.
//
// On top of the kernel sit the actors of a simulated cluster: StoreNode, a
// memstore behind calls that take virtual time and can fail; BrokerNode and
// WorkerNode, which run the real dispatch, fair and lease cores and carry out
// the effects they return; Producer, a client that retries an enqueue by its
// idempotency key; Sink, the fenced downstream that a handler writes to; and
// ScheduleFaults, which crashes, stalls and partitions them. Nodes talk only
// through Network.Send, and each measures time on its own Clock.
//
// Checker is the oracle. A run feeds it every row change of the store, every
// confirmation and cancel a client hears, and every worker's view of its
// leases after each event; it names the first invariant that breaks, S1 to S8
// while the faults run and L1 and L2 once the system has healed.
//
// Epoch is the wall-clock instant that virtual time zero stands for when a
// simulated database needs a timestamp; see World.StoreTime.
package sim
