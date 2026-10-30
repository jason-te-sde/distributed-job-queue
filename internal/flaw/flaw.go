// Package flaw is the switch behind the deliberately planted flaws that the
// test suite must catch.
//
// Each flaw is a small, named defect at one site in the code, such as a
// completion that ignores the lease token. A site asks On(id) whether its
// defect is active and, if so, misbehaves. In a normal build On is a constant
// false and the compiler removes the site. Built with the jobqueue_flaws tag,
// On reads the JOBQUEUE_FLAW environment variable once at start-up, and the
// flaw suite uses it to prove that the tests detect every defect.
package flaw

// ID names one planted flaw. The values are what JOBQUEUE_FLAW accepts.
type ID string

// The planted flaws. A "-mem" or "-pg" suffix names the store the defect is
// planted in: the in-memory store or the PostgreSQL store.
const (
	// F1Mem and F1PG make Complete ignore the lease token.
	F1Mem ID = "F1-mem"
	F1PG  ID = "F1-pg"
	// F2Mem and F2PG make Lease skip the epoch increment.
	F2Mem ID = "F2-mem"
	F2PG  ID = "F2-pg"
	// F3Mem and F3PG make Reap ignore max_attempts.
	F3Mem ID = "F3-mem"
	F3PG  ID = "F3-pg"
	// F4Mem and F4PG make Heartbeat skip the epoch comparison.
	F4Mem ID = "F4-mem"
	F4PG  ID = "F4-pg"
	// F5 makes the lease tracker anchor the deadline at response receipt.
	F5 ID = "F5"
	// F6 makes the pgstore idempotency check a check-then-insert.
	F6 ID = "F6"
	// F7 makes the pgstore lease select without FOR UPDATE SKIP LOCKED.
	F7 ID = "F7"
	// F8 makes the deficit round robin drop the deficit when a batch cuts a
	// turn.
	F8 ID = "F8"
	// F9Mem and F9PG make Lease ignore run_at.
	F9Mem ID = "F9-mem"
	F9PG  ID = "F9-pg"
)

// All returns every flaw ID in a fixed order. The caller may modify the
// returned slice.
func All() []ID {
	return []ID{
		F1Mem, F1PG, F2Mem, F2PG, F3Mem, F3PG, F4Mem, F4PG,
		F5, F6, F7, F8, F9Mem, F9PG,
	}
}
