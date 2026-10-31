package flaw

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEveryFlawHasARow holds the table of scripts/flaws.tsv to All. The flaws
// job runs the rows of the table and nothing else, so a flaw without a row is
// planted and never checked. scripts/check-flaws.sh also looks for the ids that
// flaw.go quotes, but it finds them with a pattern for the shapes of the ids
// that exist today; this test asks the list itself, so an id of another shape
// cannot slip past it. The test has no build tag, so it runs in every build.
func TestEveryFlawHasARow(t *testing.T) {
	path := filepath.Join("..", "..", "scripts", "flaws.tsv")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read the table of flaws: %v", err)
	}
	rows := map[string]bool{}
	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		id, _, _ := strings.Cut(line, "\t")
		rows[id] = true
	}
	if len(rows) == 0 {
		t.Fatalf("%s has no rows", path)
	}
	for _, id := range All() {
		if !rows[string(id)] {
			t.Errorf("flaw %q has no row in scripts/flaws.tsv, so no test is known to catch it", id)
		}
	}
}
