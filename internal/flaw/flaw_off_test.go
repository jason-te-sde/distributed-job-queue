//go:build !jobqueue_flaws

package flaw

import "testing"

// TestOffNeverActive pins the production guarantee: a build without the
// jobqueue_flaws tag has no flaw site switched on.
func TestOffNeverActive(t *testing.T) {
	for _, id := range append(All(), "not-a-flaw") {
		if On(id) {
			t.Errorf("On(%q) = true in a build without the jobqueue_flaws tag", id)
		}
	}
}
