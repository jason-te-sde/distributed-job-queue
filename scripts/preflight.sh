#!/usr/bin/env bash
# Runs every check CI runs that can run on a developer machine.
# usage: scripts/preflight.sh   (or: make preflight)
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

unformatted="$(gofmt -l .)"
if [ -n "$unformatted" ]; then
  printf 'gofmt: these files are not formatted:\n%s\n' "$unformatted" >&2
  exit 1
fi

# The generated code must match the proto files. Fingerprint internal/gen
# around `make gen` instead of asking git whether it changed: a tree that is
# already dirty must not fail by itself, and a file that was modified before
# and differs after is still drift, which a before/after `git status` cannot
# tell apart.
gen_fingerprint() {
  find internal/gen -type f | LC_ALL=C sort | while IFS= read -r file; do
    printf '%s  %s\n' "$(git hash-object "$file")" "$file"
  done
}
gen_before="$(gen_fingerprint)"
make gen
if [ "$gen_before" != "$(gen_fingerprint)" ]; then
  echo "gen: internal/gen was out of date and make gen has rewritten it; review the changes and commit them" >&2
  exit 1
fi

make lint
make hygiene
make helm-check
make test-scripts
make test

# The pg job of CI vets the Postgres tests first. Vetting needs no database, so
# this runs even when JOBQUEUE_TEST_DSN is unset and the pg tests are skipped
# below: the files behind the tag still have to compile.
go vet -tags pg ./...

if [ -n "${JOBQUEUE_TEST_DSN:-}" ]; then
  make test-pg
else
  echo "SKIPPED test-pg: JOBQUEUE_TEST_DSN unset"
fi

# The flaws job of CI. Without a database it skips the pg rows and says how
# many it skipped.
make flaws
