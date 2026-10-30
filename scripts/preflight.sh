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

make lint
make hygiene
make test-scripts
make test

if [ -n "${JOBQUEUE_TEST_DSN:-}" ]; then
  make test-pg
else
  echo "SKIPPED test-pg: JOBQUEUE_TEST_DSN unset"
fi
