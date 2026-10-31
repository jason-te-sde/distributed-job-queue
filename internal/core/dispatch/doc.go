// Package dispatch decides when a broker fetches jobs for its long-polling
// workers, and what each worker is told.
//
// A worker that asks for jobs becomes a waiter. The core keeps the waiters of
// each queue in arrival order and starts a fetch for the oldest one only
// when there is reason to believe the queue holds work: a notification, a
// due job, a fallback poll, or a previous fetch that may have left jobs
// behind. Every such event starts at most one fetch. A full batch chains the
// next waiter and a partial one stops, so the number of fetches follows the
// amount of work, and at most MaxInFlight fetches per queue run at once. The
// stop needs care under fair sharing: a fetch that took from named tenants and
// came back short found only that those tenants ran dry, so it chains like a
// full one, and only a fetch across all tenants can find the queue empty. The
// shell says which kind of fetch it ran when it reports the result.
//
// A waiter is never told "nothing" before something has looked for it: Tick
// times out only a waiter that a fetch has served or failed to serve, or one
// whose queue is known to be empty. Any other waiter waits for its turn at a
// fetch slot. Knowledge that a queue is empty goes stale, and Tick runs only
// while a waiter is parked, so an arriving waiter first applies the poll and
// due-time rules that Tick would.
//
// The core is pure. It holds no clock, no lock and no store. Every method
// takes the current monotonic instant and returns effects for the shell to
// carry out: fetch from the store, answer a waiter, release the jobs fetched
// for a waiter that went away, and arm the shell's single timer. The shell
// reports fetch results back through FetchDone, and calls Tick when the
// armed instant arrives.
//
// A Core is not safe for concurrent use: the shell serializes all calls.
package dispatch
