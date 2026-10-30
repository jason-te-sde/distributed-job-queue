//go:build !jobqueue_flaws

package flaw

// On reports whether the flaw id is active. Without the jobqueue_flaws build
// tag no flaw is ever active.
func On(ID) bool { return false }
