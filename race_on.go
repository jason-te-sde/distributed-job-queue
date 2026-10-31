//go:build race

package jobqueue

import (
	"bytes"
	"runtime"
	"strconv"
)

// raceEnabled says that this is a race build, which also checks that the lease
// tracker is entered from one goroutine only. The check needs the id of the
// calling goroutine, which Go does not expose; it is read from the first line
// of the stack, "goroutine N [running]:".
const raceEnabled = true

// goroutineID returns the id of the calling goroutine.
func goroutineID() uint64 {
	var buf [64]byte
	line := buf[:runtime.Stack(buf[:], false)]
	line = bytes.TrimPrefix(line, []byte("goroutine "))
	if i := bytes.IndexByte(line, ' '); i >= 0 {
		line = line[:i]
	}
	id, err := strconv.ParseUint(string(line), 10, 64)
	if err != nil {
		panic("jobqueue: cannot read the goroutine id from the stack: " + err.Error())
	}
	return id
}
