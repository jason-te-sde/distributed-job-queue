package flaw

import (
	"slices"
	"testing"
)

// TestAllListsEveryFlawOnce pins the spelling of every ID. The strings are
// an interface: they are the values JOBQUEUE_FLAW accepts and the names the
// flaw scripts and CI jobs refer to.
func TestAllListsEveryFlawOnce(t *testing.T) {
	want := []ID{
		"F1-mem", "F1-pg", "F2-mem", "F2-pg", "F3-mem", "F3-pg", "F4-mem", "F4-pg",
		"F5", "F6", "F7", "F8", "F9-mem", "F9-pg",
	}
	if got := All(); !slices.Equal(got, want) {
		t.Fatalf("All() = %v, want %v", got, want)
	}
}
