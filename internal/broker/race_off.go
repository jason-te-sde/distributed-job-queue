//go:build !race

package broker

// raceEnabled says that this is not a race build: the dispatch core is not
// checked for entry from a second goroutine, which costs a stack read.
const raceEnabled = false

// goroutineID is never called outside a race build.
func goroutineID() uint64 { return 0 }
