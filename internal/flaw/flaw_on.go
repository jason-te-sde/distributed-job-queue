//go:build jobqueue_flaws

package flaw

import (
	"os"
	"slices"
)

// active is the flaw selected by JOBQUEUE_FLAW, or empty when none is.
var active ID

// init reads JOBQUEUE_FLAW once. An empty value activates nothing. A value
// that is not an ID from All is a typo that would silently test nothing, so
// it stops the process instead.
func init() {
	v := os.Getenv("JOBQUEUE_FLAW")
	if v == "" {
		return
	}
	if !slices.Contains(All(), ID(v)) {
		panic("unknown JOBQUEUE_FLAW " + v)
	}
	active = ID(v)
}

// On reports whether the flaw id is the one selected by JOBQUEUE_FLAW.
func On(id ID) bool { return id != "" && id == active }
