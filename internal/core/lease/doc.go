// Package lease tracks the leases that one worker holds, and decides when to
// ask for more work, when to keep leases alive, when to give up on a lease,
// and when to report a finished job.
//
// The tracker is the worker side of the lease protocol. Its safety rule is
// that a worker must stop acting on a lease before the broker can hand the
// job to someone else. The tracker therefore never trusts its own clock to
// say how long a lease lasts. It anchors each local deadline at the instant
// the request that obtained or extended the lease was sent, never at the
// instant the answer arrived, because the answer may have waited in the
// network for any time. The deadline is also a margin earlier than the
// lease's expiry, so that the handler is cancelled before the lease can be
// taken over. Heartbeats go out every quarter of the TTL, which lets two
// consecutive heartbeats be lost while the lease still survives.
//
// The tracker is pure. It holds no clock, no timer, no goroutine and no
// connection. Every method takes the current monotonic instant and returns
// the effects that its caller, the shell, must carry out: send a request,
// start or cancel a handler, set the one timer. The shell reports each
// outcome by calling the matching method. The same calls at the same
// instants therefore always give the same effects, which is what lets the
// simulator and the property test drive it. The only randomness is the
// jitter of the lease-request backoff, drawn from a generator that the
// caller seeds.
//
// A Tracker is not safe for concurrent use: the shell serializes all calls
// and passes instants that never go backwards.
package lease
