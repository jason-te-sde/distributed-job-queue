package migrate_test

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

func TestMigrationsAreContiguous(t *testing.T) {
	files := migrate.Files()
	if len(files) == 0 {
		t.Fatal("no embedded migrations")
	}
	name := regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)
	for i, f := range files {
		m := name.FindStringSubmatch(f)
		if m == nil {
			t.Fatalf("migration %d is %q, which does not match %s", i+1, f, name)
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("migration %q: %v", f, err)
		}
		if n != i+1 {
			t.Fatalf("migration at position %d is %q; versions must run 0001..N without gaps or reordering", i+1, f)
		}
	}
	if len(files) != migrate.Version {
		t.Fatalf("%d embedded migrations %v, but Version is %d", len(files), files, migrate.Version)
	}
}

func TestSchemaMismatchErrorNamesBothVersionsAndTheRemedy(t *testing.T) {
	err := fmt.Errorf("startup: %w", &migrate.SchemaMismatchError{Have: 2, Want: 5})
	var mismatch *migrate.SchemaMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("errors.As did not find a *SchemaMismatchError in %v", err)
	}
	const want = "database schema version 2, this binary needs 5: run jobqueuectl migrate"
	if got := mismatch.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestLockKeySpellsTheProjectName(t *testing.T) {
	var spelled int64
	for _, b := range []byte("jobqueue") {
		spelled = spelled<<8 | int64(b)
	}
	if migrate.LockKey != spelled {
		t.Fatalf("LockKey = %#x, want %#x (the ASCII bytes of \"jobqueue\")", migrate.LockKey, spelled)
	}
}
