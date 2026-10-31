#!/usr/bin/env bash
# Tests for scripts/check-flaws.sh. Every case builds a throwaway Go module
# whose tests fail only under a named flaw, points the script at it with
# FLAWS_ROOT and FLAWS_TSV, and prints "PASS <case>"; the first failing case
# aborts the run.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="$(cd "$here/.." && pwd)/check-flaws.sh"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

# The throwaway modules build offline, with the toolchain that is installed,
# whatever the caller's environment says about workspaces and flags.
export GOTOOLCHAIN=local GOFLAGS='' GOWORK=off

root="$workdir/mod"
tsv="$workdir/flaws.tsv"
out=""
status=0

TAB=$'\t'

# write <path> <text>: create a file under the module root.
write() {
  mkdir -p "$(dirname "$root/$1")"
  printf '%s\n' "$2" >"$root/$1"
}

# flaw_source <id>...: write internal/flaw/flaw.go quoting these ids, the way
# the real file quotes its own.
flaw_source() {
  local body id n=0
  body='package flaw

// ID names a flaw.
type ID string

// The flaws, as the real file quotes them.
const ('
  for id in "$@"; do
    n=$((n + 1))
    body="$body
	C$n ID = \"$id\""
  done
  write internal/flaw/flaw.go "$body
)"
}

# new_module: a module with these packages.
#   ./p       TestFailsUnderF1 fails under flaw F1 and says "marker-alpha";
#             TestFailsUnderF2 fails under flaw F2 and says "marker-beta";
#             TestIgnoresFlaw never fails.
#   ./q       TestQuiet passes, but the package panics in init under flaw F3.
#   ./r       imports a stand-in for rapid; TestSawNoFailFile fails under flaw
#             F4 when the test binary was started with -rapid.nofailfile.
#   ./pgp     TestPGUnderF5 fails under flaw F5 when built with the pg tag.
#   internal/sim  TestLeavesAFailureFile writes a failure file under flaw F6
#             and fails, saying whether an older file was still there.
new_module() {
  rm -rf "$root"
  mkdir -p "$root"
  write go.mod 'module example.com/flawmod

go 1.21

require pgregory.net/rapid v0.0.0

replace pgregory.net/rapid => ./fakerapid'
  write fakerapid/go.mod 'module pgregory.net/rapid

go 1.21'
  write fakerapid/rapid.go 'package rapid

import (
	"flag"
	"time"
)

// The flags the real package registers in init, which the script passes.
var (
	noFailFile = flag.Bool("rapid.nofailfile", false, "rapid: do not write fail files on test failures")
	_          = flag.Int("rapid.checks", 100, "rapid: number of checks to perform")
	_          = flag.Duration("rapid.shrinktime", 30*time.Second, "rapid: maximum time to spend on test case minimization")
)

// NoFailFile reports the first flag.
func NoFailFile() bool { return *noFailFile }'
  write p/p_test.go 'package p

import (
	"os"
	"testing"
)

func TestFailsUnderF1(t *testing.T) {
	if os.Getenv("JOBQUEUE_FLAW") == "F1" {
		t.Fatal("flaw F1 is on: marker-alpha")
	}
}

func TestFailsUnderF2(t *testing.T) {
	if os.Getenv("JOBQUEUE_FLAW") == "F2" {
		t.Fatal("flaw F2 is on: marker-beta")
	}
}

func TestIgnoresFlaw(t *testing.T) {}'
  write q/q.go 'package q

import "os"

func init() {
	if os.Getenv("JOBQUEUE_FLAW") == "F3" {
		panic("flaw clause drifted")
	}
}'
  write q/q_test.go 'package q

import "testing"

func TestQuiet(t *testing.T) {}'
  write r/r_test.go 'package r

import (
	"os"
	"testing"

	"pgregory.net/rapid"
)

func TestSawNoFailFile(t *testing.T) {
	if os.Getenv("JOBQUEUE_FLAW") == "F4" && rapid.NoFailFile() {
		t.Fatal("flaw F4 is on and rapid was told not to write fail files")
	}
}'
  write pgp/pgp.go 'package pgp'
  write pgp/pgp_test.go '//go:build pg

package pgp

import (
	"os"
	"testing"
)

func TestPGUnderF5(t *testing.T) {
	if os.Getenv("JOBQUEUE_TEST_DSN") == "" {
		t.Fatal("JOBQUEUE_TEST_DSN is not set")
	}
	if os.Getenv("JOBQUEUE_FLAW") == "F5" {
		t.Fatal("flaw F5 is on: marker-gamma")
	}
}'
  write internal/sim/sim.go 'package sim'
  write internal/sim/sim_test.go 'package sim

import (
	"os"
	"testing"
)

func TestLeavesAFailureFile(t *testing.T) {
	if os.Getenv("JOBQUEUE_FLAW") != "F6" {
		return
	}
	_, err := os.Stat("testdata/failures/seed-7.txt")
	stale := err == nil
	if err := os.MkdirAll("testdata/failures", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("testdata/failures/seed-8.txt", []byte("failure"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("flaw F6 is on: older file still there = %v", stale)
}'
  flaw_source F1 F2
}

# rows <line>...: write the TSV, one argument per line, with "|" standing for
# a tab so that the cases stay readable.
rows() {
  : >"$tsv"
  local line
  for line in "$@"; do
    printf '%s\n' "${line//|/$TAB}" >>"$tsv"
  done
}

# run_check [args]: run the script against the module and the TSV, with a
# database address set so that pg rows have what they need.
run_check() {
  status=0
  out="$(JOBQUEUE_TEST_DSN=dsn FLAWS_ROOT="$root" FLAWS_TSV="$tsv" bash "$check" "$@" 2>&1)" || status=$?
}

expect_status() { # expect_status <case> <want>
  if [ "$status" -ne "$2" ]; then
    printf 'FAIL %s: exit status %s, want %s\n%s\n' "$1" "$status" "$2" "$out" >&2
    exit 1
  fi
}

expect_output() { # expect_output <case> <substring>
  case "$out" in
    *"$2"*) ;;
    *)
      printf 'FAIL %s: output does not contain %q\n%s\n' "$1" "$2" "$out" >&2
      exit 1
      ;;
  esac
}

expect_no_output() { # expect_no_output <case> <substring>
  case "$out" in
    *"$2"*)
      printf 'FAIL %s: output contains %q\n%s\n' "$1" "$2" "$out" >&2
      exit 1
      ;;
  esac
}

pass() { printf 'PASS %s\n' "$1"; }

# The rows that cover both ids the module's flaw.go quotes at the start.
base_rows=(
  "F1|./p|^TestFailsUnderF1\$|-|-"
  "F2|./p|^TestFailsUnderF2\$|-|-"
)

# (a) A test that fails under its flaw is caught, and the run succeeds.
new_module
rows "${base_rows[@]}"
run_check
expect_status caught-rows-pass 0
expect_output caught-rows-pass 'flaw F1 caught by ^TestFailsUnderF1$'
expect_output caught-rows-pass 'flaw F2 caught by ^TestFailsUnderF2$'
expect_output caught-rows-pass 'inert build passed'
expect_no_output caught-rows-pass 'NOT caught'
pass caught-rows-pass

# (b) A test that passes under its flaw is not caught: the run fails, says so,
# and still reports the rows after it.
rows "F1|./p|^TestIgnoresFlaw\$|-|-" "F2|./p|^TestFailsUnderF2\$|-|-"
run_check
expect_status passing-test-is-not-caught 1
expect_output passing-test-is-not-caught 'flaw F1 NOT caught by ^TestIgnoresFlaw$'
expect_output passing-test-is-not-caught 'flaw F2 caught by ^TestFailsUnderF2$'
pass passing-test-is-not-caught

# (c) A test that fails without the expected text is not caught, and one that
# fails with it is.
rows "F1|./p|^TestFailsUnderF1\$|-|marker-beta" "F2|./p|^TestFailsUnderF2\$|-|marker-beta"
run_check
expect_status expect-text-is-required 1
expect_output expect-text-is-required 'flaw F1 NOT caught by ^TestFailsUnderF1$'
expect_output expect-text-is-required 'flaw F2 caught by ^TestFailsUnderF2$'
pass expect-text-is-required

# (d) A package that panics in init fails every test in it, but the failure is
# not the flaw's: no test went red, so the row is not caught, with or without
# expect text. This is what a flaw clause that has drifted from its statement
# does to the whole package.
rows "${base_rows[@]}" "F3|./q|^TestQuiet\$|-|-" "F3|./q|^TestQuiet\$|-|flaw clause drifted"
run_check
expect_status init-panic-is-not-caught 1
expect_output init-panic-is-not-caught 'flaw F3 NOT caught by ^TestQuiet$'
expect_no_output init-panic-is-not-caught 'flaw F3 caught'
pass init-panic-is-not-caught

# (e) A regex that matches no test is not caught.
rows "F1|./p|^TestNoSuchTest\$|-|-" "F2|./p|^TestFailsUnderF2\$|-|-"
run_check
expect_status unmatched-regex-is-not-caught 1
expect_output unmatched-regex-is-not-caught 'flaw F1 NOT caught by ^TestNoSuchTest$'
pass unmatched-regex-is-not-caught

# (f) The build without a flaw must be green. A test that fails whenever
# JOBQUEUE_FLAW is empty makes the inert run red, whatever the rows say; and a
# JOBQUEUE_FLAW left in the caller's environment must not leak into that run.
write inert/inert_test.go 'package inert

import (
	"os"
	"testing"
)

func TestOnlyUnderAFlaw(t *testing.T) {
	if os.Getenv("JOBQUEUE_FLAW") == "" {
		t.Fatal("no flaw is selected")
	}
}'
rows "${base_rows[@]}"
run_check
expect_status inert-build-must-pass 1
expect_output inert-build-must-pass 'inert build FAILED'
expect_output inert-build-must-pass 'flaw F1 caught by'
pass inert-build-must-pass

rm -rf "$root/inert"
status=0
out="$(JOBQUEUE_FLAW=F1 JOBQUEUE_TEST_DSN=dsn FLAWS_ROOT="$root" FLAWS_TSV="$tsv" bash "$check" 2>&1)" || status=$?
expect_status inert-run-ignores-callers-flaw 0
expect_output inert-run-ignores-callers-flaw 'inert build passed'
pass inert-run-ignores-callers-flaw

# (g) Completeness: every id that flaw.go quotes needs a row.
flaw_source F1 F2 F7 F8-mem
rows "${base_rows[@]}"
run_check
expect_status missing-row-fails 1
expect_output missing-row-fails 'F7, which has no row'
expect_output missing-row-fails 'F8-mem, which has no row'
expect_no_output missing-row-fails 'flaw F1'
pass missing-row-fails

# An id counts only as a whole name in the first column: a row for F1-mem does
# not cover F1, and an id that appears in a comment or in another column does
# not count.
flaw_source F1 F1-mem
rows "F1-mem|./p|^TestFailsUnderF1\$|-|-" "# F1 is only mentioned in a comment" "F1-pg|./p|^TestFailsUnderF1\$|-|F1"
run_check
expect_status id-must-match-whole-name 1
expect_output id-must-match-whole-name 'F1, which has no row'
expect_no_output id-must-match-whole-name 'F1-mem, which'
pass id-must-match-whole-name

# An id with several rows is covered. The rows run under flaws that no test of
# the module knows, so they are not caught, but the table is complete.
rows "F1|./p|^TestFailsUnderF1\$|-|-" "F1-mem|./p|^TestFailsUnderF1\$|-|-" "F1-mem|./p|^TestFailsUnderF2\$|-|-"
run_check
expect_status second-row-for-an-id-is-fine 1
expect_no_output second-row-for-an-id-is-fine 'no row'
expect_output second-row-for-an-id-is-fine 'flaw F1-mem NOT caught by ^TestFailsUnderF2$'
pass second-row-for-an-id-is-fine

# A flaw.go that quotes no ids, or is missing, is a configuration error and
# not a vacuous pass.
write internal/flaw/flaw.go 'package flaw'
rows "${base_rows[@]}"
run_check
expect_status flaw-source-without-ids 2
expect_output flaw-source-without-ids 'no flaw ids'
pass flaw-source-without-ids

rm "$root/internal/flaw/flaw.go"
run_check
expect_status flaw-source-missing 2
expect_output flaw-source-missing 'flaw.go'
pass flaw-source-missing

# (h) The TSV: comments and blank lines are ignored; a malformed table is a
# configuration error that names the line.
new_module
rows "# a comment" "" "${base_rows[0]}" "   " "${base_rows[1]}" "# another comment"
run_check
expect_status comments-and-blank-lines-are-ignored 0
pass comments-and-blank-lines-are-ignored

rows "${base_rows[@]}" "F1|./p|^TestFailsUnderF1\$|-"
run_check
expect_status short-row-is-an-error 2
expect_output short-row-is-an-error 'line 3'
pass short-row-is-an-error

rows "${base_rows[@]}" "F1|./p|^TestFailsUnderF1\$|-|-|surplus"
run_check
expect_status long-row-is-an-error 2
expect_output long-row-is-an-error 'line 3'
pass long-row-is-an-error

rows "${base_rows[@]}" "F1|./p|^TestFailsUnderF1\$|maybe|-"
run_check
expect_status unknown-tags-value-is-an-error 2
expect_output unknown-tags-value-is-an-error 'maybe'
pass unknown-tags-value-is-an-error

rows "${base_rows[@]}" "F1||^TestFailsUnderF1\$|-|-"
run_check
expect_status empty-field-is-an-error 2
expect_output empty-field-is-an-error 'line 3'
pass empty-field-is-an-error

rows "# nothing but a comment"
run_check
expect_status empty-table-is-an-error 2
expect_output empty-table-is-an-error 'no rows'
pass empty-table-is-an-error

status=0
out="$(FLAWS_ROOT="$root" FLAWS_TSV="$workdir/absent.tsv" bash "$check" 2>&1)" || status=$?
expect_status missing-table-is-an-error 2
expect_output missing-table-is-an-error 'absent.tsv'
pass missing-table-is-an-error

rows "${base_rows[@]}"
run_check --bogus
expect_status unknown-flag-is-a-usage-error 2
expect_output unknown-flag-is-a-usage-error 'usage'
pass unknown-flag-is-a-usage-error

# A package that does not exist is a configuration error, not a row that
# happens to fail.
rows "F1|./nope|^TestFailsUnderF1\$|-|-" "F2|./p|^TestFailsUnderF2\$|-|-"
run_check
expect_status unknown-package-is-an-error 2
expect_output unknown-package-is-an-error './nope'
pass unknown-package-is-an-error

# (i) pg rows. They need a database, and the tag reaches the build.
flaw_source F1 F2 F5
rows "${base_rows[@]}" "F5|./pgp|^TestPGUnderF5\$|pg|marker-gamma"
status=0
out="$(env -u JOBQUEUE_TEST_DSN FLAWS_ROOT="$root" FLAWS_TSV="$tsv" bash "$check" 2>&1)" || status=$?
expect_status pg-rows-need-a-dsn 2
expect_output pg-rows-need-a-dsn 'JOBQUEUE_TEST_DSN'
expect_output pg-rows-need-a-dsn '--no-pg'
pass pg-rows-need-a-dsn

run_check
expect_status pg-row-is-caught 0
expect_output pg-row-is-caught 'flaw F5 caught by ^TestPGUnderF5$'
pass pg-row-is-caught

status=0
out="$(env -u JOBQUEUE_TEST_DSN FLAWS_ROOT="$root" FLAWS_TSV="$tsv" bash "$check" --no-pg 2>&1)" || status=$?
expect_status no-pg-skips-pg-rows 0
expect_output no-pg-skips-pg-rows 'SKIPPED 1 pg rows'
expect_no_output no-pg-skips-pg-rows 'flaw F5'
expect_output no-pg-skips-pg-rows 'flaw F1 caught by'
pass no-pg-skips-pg-rows

# --no-pg skips only the run: a pg row still counts for the completeness check.
rows "${base_rows[@]}"
run_check --no-pg
expect_status no-pg-still-checks-completeness 1
expect_output no-pg-still-checks-completeness 'F5, which has no row'
pass no-pg-still-checks-completeness

# (j) rapid. A package whose test binary links rapid is run with
# -rapid.nofailfile, so that a flaw run leaves no failure files behind; a
# package that does not link it must not get the flag, because its test binary
# would refuse to start, and a row that fails for that reason is not a catch.
# The F1 and F2 rows are in a package without rapid, the F4 row in one with it.
flaw_source F1 F2 F4
rows "${base_rows[@]}" "F4|./r|^TestSawNoFailFile\$|-|rapid was told"
run_check
expect_status rapid-flag-reaches-only-rapid-packages 0
expect_output rapid-flag-reaches-only-rapid-packages 'flaw F4 caught by ^TestSawNoFailFile$'
expect_output rapid-flag-reaches-only-rapid-packages 'flaw F1 caught by ^TestFailsUnderF1$'
pass rapid-flag-reaches-only-rapid-packages

# (k) The simulator writes a failure file for every failing seed under a
# flaw. The directory is emptied before the rows run, so that an old file
# cannot be mistaken for the flaw's, and after them, so that a flaw run leaves
# nothing in the tree.
flaw_source F1 F2 F6
mkdir -p "$root/internal/sim/testdata/failures"
printf 'old\n' >"$root/internal/sim/testdata/failures/seed-7.txt"
rows "${base_rows[@]}" "F6|./internal/sim|^TestLeavesAFailureFile\$|-|older file still there = false"
run_check
expect_status failure-files-are-cleared 0
expect_output failure-files-are-cleared 'flaw F6 caught by ^TestLeavesAFailureFile$'
if [ -e "$root/internal/sim/testdata/failures/seed-7.txt" ] || [ -e "$root/internal/sim/testdata/failures/seed-8.txt" ]; then
  printf 'FAIL failure-files-are-cleared: failure files are left in the tree\n' >&2
  exit 1
fi
pass failure-files-are-cleared

# (l) The table of this repository. It names only packages that exist, and
# every id that internal/flaw/flaw.go quotes has a row; the script's own
# completeness check says which one is missing.
real_root="$(cd "$here/../.." && pwd)"
real_ids="$(grep -oE '"F[0-9]+(-mem|-pg)?"' "$real_root/internal/flaw/flaw.go" | tr -d '"' | sort -u)"
[ -n "$real_ids" ] || {
  printf 'FAIL flaws-table-is-complete: no flaw ids in internal/flaw/flaw.go\n' >&2
  exit 1
}
for id in $real_ids; do
  if ! awk -F'\t' -v id="$id" '$1 == id { found = 1 } END { exit !found }' "$real_root/scripts/flaws.tsv"; then
    printf 'FAIL flaws-table-is-complete: scripts/flaws.tsv has no row for %s\n' "$id" >&2
    exit 1
  fi
done
pass flaws-table-is-complete
