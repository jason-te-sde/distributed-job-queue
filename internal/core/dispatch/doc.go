// Package dispatch decides when a broker fetches jobs for its long-polling
// workers, and what each worker is told.
//
// A worker that asks for jobs becomes a waiter. The core keeps the waiters of
// each queue in arrival order and starts a fetch for the oldest one only
// when there is reason to believe the queue holds work: a notification, a
// due job, a fallback poll, or a previous fetch that came back full. A full
// batch chains the next waiter and a partial one stops, so the number of
// fetches follows the amount of work, and at most MaxInFlight fetches per
// queue run at once.
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
