//go:build !race

package jobqueue

// raceEnabled says that this is not a race build: the lease tracker is not
// checked for entry from a second goroutine, which costs a stack read.
const raceEnabled = false

// goroutineID is never called outside a race build.
func goroutineID() uint64 { return 0 }
