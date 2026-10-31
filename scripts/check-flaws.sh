#!/usr/bin/env bash
# Asserts that every planted flaw turns its named tests red, and that the build
# with the flaws compiled in is otherwise inert.
#
# usage: check-flaws.sh [--no-pg]
#
# scripts/flaws.tsv holds one row per (flaw, test) pair, tab-separated:
#
#   id      the flaw, as JOBQUEUE_FLAW takes it (see internal/flaw/flaw.go)
#   package the package whose tests must go red, as go test names it
#   regex   the test that must go red, as go test -run takes it
#   tags    - or pg; pg adds the pg build tag and needs a database
#   expect  - or a substring that the failing output must contain
#
# A line that is blank or starts with # is ignored. A flaw may have several
# rows, and every one of them must be caught.
#
# The script does four things:
#   1. Fails if internal/flaw/flaw.go quotes an id that no row names, so that a
#      new flaw cannot ship without a test that is known to catch it.
#   2. For each row, runs JOBQUEUE_FLAW=<id> go test -count=1 with the
#      jobqueue_flaws tag, and requires a test to have failed (a line saying
#      "--- FAIL"), and the expect text, if the row has any, in the output. A
#      run that fails without any test failing is not a catch: a flaw clause
#      that has drifted from its statement panics every test of the package in
#      init, a package can fail to build, and a test can time out, and none of
#      these says that a test notices the flaw. The expect text is a second
#      guard for tests that fail for another reason as well, such as a missing
#      database. It prints "flaw <id> caught by <regex>" or "flaw <id> NOT
#      caught by <regex>".
#   3. With JOBQUEUE_FLAW unset, runs go test -count=1 -tags jobqueue_flaws ./...
#      and requires it to pass: the flaw sites must do nothing until one is
#      selected.
#   4. Exits 1 if any row was not caught or the inert run failed.
#
# --no-pg skips the pg rows, and builds the inert run without the pg tag, for a
# machine without a database; it prints how many rows it skipped. CI never
# passes it. Without the flag the pg rows need JOBQUEUE_TEST_DSN.
#
# A test binary that links pgregory.net/rapid is also started with
# -rapid.nofailfile, so that a failing property test leaves no file under
# testdata/rapid, and with -rapid.checks and -rapid.shrinktime (see below). A
# test binary that does not link rapid refuses these flags and exits without
# running a test, so they go only to the packages that do.
#
# The simulator writes internal/sim/testdata/failures/seed-<n>.txt for each
# seed that fails, which under a flaw is most of them. The directory is emptied
# before the first row and after the last.
#
# FLAWS_ROOT and FLAWS_TSV replace the repository root and the table, which is
# how scripts/tests/flaws_test.sh points the script at a throwaway module.
# Exit status: 0 every row caught and the inert run passed, 1 otherwise, 2 a
# usage or configuration error.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="${FLAWS_ROOT:-$(cd "$script_dir/.." && pwd)}"
tsv="${FLAWS_TSV:-$script_dir/flaws.tsv}"
flaw_source="$root/internal/flaw/flaw.go"
failures_dir="$root/internal/sim/testdata/failures"

# How many property-test cases a rapid package runs under a flaw. A property
# test goes red at the first case that finds the defect, and stops, so a large
# number costs nothing when the flaw is found early and only buys certainty
# when it is not. Measured against TestPGDifferential, whose rows are the
# slowest to catch, one case in a run finds F1-pg 45% of the time (54 of 120
# runs), F2-pg 85% (34 of 40), F3-pg 19% (58 of 300), F4-pg 27% (33 of 120) and
# F9-pg 57% (23 of 40). rapid's own default is 100 cases, which would miss the
# rarest of them about once in 10^7 runs if the rate is 15%; 300 makes it
# vanish. rapid also shrinks a failure for up to 30 seconds by default, which
# says nothing here, so the time allowed for it is cut to a millisecond.
rapid_checks="${FLAWS_RAPID_CHECKS:-300}"

TAB=$'\t'
US=$'\x1f'

usage() {
  printf 'usage: check-flaws.sh [--no-pg]\n' >&2
  exit 2
}

die() {
  printf 'check-flaws: %s\n' "$*" >&2
  exit 2
}

no_pg=0
case "$#" in
  0) ;;
  1) if [ "$1" = "--no-pg" ]; then no_pg=1; else usage; fi ;;
  *) usage ;;
esac

[ -f "$tsv" ] && [ -r "$tsv" ] || die "cannot read the table of flaws: $tsv"
[ -f "$flaw_source" ] && [ -r "$flaw_source" ] || die "cannot read the list of flaws: $flaw_source"
[ -d "$root" ] || die "no such directory: $root"
command -v go >/dev/null 2>&1 || die "go is not installed"

# --- the table ---------------------------------------------------------------

row_id=()
row_pkg=()
row_regex=()
row_tags=()
row_expect=()
lineno=0
while IFS= read -r line || [ -n "$line" ]; do
  lineno=$((lineno + 1))
  trimmed="${line#"${line%%[![:space:]]*}"}"
  case "$trimmed" in '' | '#'*) continue ;; esac
  # A tab is white space to read, which merges neighbouring tabs and would
  # shift the fields of a row with an empty one, so the tabs become a
  # separator that is not.
  IFS="$US" read -r id pkg regex tags expect surplus <<<"${line//$TAB/$US}"
  if [ -z "${id:-}" ] || [ -z "${pkg:-}" ] || [ -z "${regex:-}" ] || [ -z "${tags:-}" ] || [ -z "${expect:-}" ]; then
    die "$tsv line $lineno: want 5 tab-separated fields, none empty (id, package, test regex, tags, expect; - for none)"
  fi
  [ -z "${surplus:-}" ] || die "$tsv line $lineno: more than 5 fields"
  case "$tags" in
    - | pg) ;;
    *) die "$tsv line $lineno: tags is '$tags', want - or pg" ;;
  esac
  row_id+=("$id")
  row_pkg+=("$pkg")
  row_regex+=("$regex")
  row_tags+=("$tags")
  row_expect+=("$expect")
done <"$tsv"
rows=${#row_id[@]}
[ "$rows" -gt 0 ] || die "$tsv has no rows"

# --- completeness ------------------------------------------------------------

# The ids that flaw.go quotes. Each is a whole name: F1 is not F1-mem.
quoted="$(grep -oE '"F[0-9]+(-mem|-pg)?"' -- "$flaw_source" | tr -d '"' | sort -u || true)"
[ -n "$quoted" ] || die "no flaw ids found in $flaw_source"
missing=0
for id in $quoted; do
  found=0
  for named in "${row_id[@]}"; do
    if [ "$named" = "$id" ]; then
      found=1
      break
    fi
  done
  if [ "$found" -eq 0 ]; then
    printf 'flaws: %s quotes %s, which has no row in %s\n' "internal/flaw/flaw.go" "$id" "$tsv" >&2
    missing=1
  fi
done
if [ "$missing" -ne 0 ]; then
  printf 'flaws: every flaw needs a row naming the test that catches it\n' >&2
  exit 1
fi

# --- what the rows need ------------------------------------------------------

pg_rows=0
for tags in "${row_tags[@]}"; do
  if [ "$tags" = pg ]; then pg_rows=$((pg_rows + 1)); fi
done
if [ "$pg_rows" -gt 0 ] && [ "$no_pg" -eq 0 ] && [ -z "${JOBQUEUE_TEST_DSN:-}" ]; then
  die "the pg rows need JOBQUEUE_TEST_DSN, for example postgres://localhost:5432/postgres?sslmode=disable; set it, or pass --no-pg to skip them"
fi

cd "$root"

# links_rapid <package> <tags>: whether the package's test binary links rapid,
# which is what registers the -rapid.* flags.
links_rapid() {
  local deps
  deps="$(go list -test -deps -tags "$2" -f '{{.ImportPath}}' "$1" 2>&1)" || die "cannot list $1: $deps"
  case $'\n'"$deps"$'\n' in
    *$'\n'pgregory.net/rapid$'\n'*) return 0 ;;
  esac
  return 1
}

# clear_failures empties the simulator's failure directory and removes it, so
# that the tree is as it was. It runs before the first row, and again on the way
# out, however the script ends.
clear_failures() {
  rm -f "$failures_dir"/seed-*.txt
  rmdir "$failures_dir" 2>/dev/null || true
}
trap clear_failures EXIT

# show <output>: the end of a run's output, indented.
show() {
  printf '%s\n' "$1" | tail -n 40 | sed 's/^/    /'
}

# --- the rows ----------------------------------------------------------------

# Which tags each row builds with, and whether its test binary links rapid.
# Every package is looked up before the first test runs, so that a package
# that does not exist stops the run at once and not after the slow rows.
row_gotags=()
row_rapid=()
for ((i = 0; i < rows; i++)); do
  tags=jobqueue_flaws
  if [ "${row_tags[$i]}" = pg ]; then tags=jobqueue_flaws,pg; fi
  row_gotags+=("$tags")
  if [ "${row_tags[$i]}" = pg ] && [ "$no_pg" -eq 1 ]; then
    row_rapid+=(0)
  elif links_rapid "${row_pkg[$i]}" "$tags"; then
    row_rapid+=(1)
  else
    row_rapid+=(0)
  fi
done

clear_failures
caught=0
not_caught=0
skipped=0
for ((i = 0; i < rows; i++)); do
  id="${row_id[$i]}" pkg="${row_pkg[$i]}" regex="${row_regex[$i]}" expect="${row_expect[$i]}"
  if [ "${row_tags[$i]}" = pg ] && [ "$no_pg" -eq 1 ]; then
    skipped=$((skipped + 1))
    continue
  fi
  flags=()
  if [ "${row_rapid[$i]}" -eq 1 ]; then
    flags=(-rapid.nofailfile -rapid.shrinktime=1ms "-rapid.checks=$rapid_checks")
  fi

  status=0
  out="$(JOBQUEUE_FLAW="$id" go test -count=1 -tags "${row_gotags[$i]}" -run "$regex" "$pkg" ${flags[@]+"${flags[@]}"} 2>&1)" || status=$?

  reason=""
  if [ "$status" -eq 0 ]; then
    reason="the tests passed with the flaw on"
  else
    case "$out" in
      *'--- FAIL'*) ;;
      *) reason="the run failed (exit status $status) but no test did: a build error, a panic in init or a timeout, not a test noticing the flaw" ;;
    esac
  fi
  if [ -z "$reason" ] && [ "$expect" != "-" ]; then
    case "$out" in
      *"$expect"*) ;;
      *) reason="a test failed, but nothing it printed contains: $expect" ;;
    esac
  fi

  if [ -z "$reason" ]; then
    printf 'flaw %s caught by %s\n' "$id" "$regex"
    caught=$((caught + 1))
  else
    printf 'flaw %s NOT caught by %s\n' "$id" "$regex"
    printf '  %s\n' "$reason"
    show "$out"
    not_caught=$((not_caught + 1))
  fi
done

# --- the inert build ---------------------------------------------------------

inert_tags=jobqueue_flaws
if [ "$no_pg" -eq 0 ]; then
  inert_tags=jobqueue_flaws,pg
fi
status=0
out="$(env -u JOBQUEUE_FLAW go test -count=1 -tags "$inert_tags" ./... 2>&1)" || status=$?
inert=ok
if [ "$status" -eq 0 ]; then
  printf 'inert build passed: go test -tags %s ./... with no flaw selected\n' "$inert_tags"
else
  inert=failed
  printf 'inert build FAILED: go test -tags %s ./... with no flaw selected exited with status %s\n' "$inert_tags" "$status"
  show "$out"
fi

# --- the summary -------------------------------------------------------------

if [ "$skipped" -gt 0 ]; then
  printf 'SKIPPED %d pg rows\n' "$skipped"
fi
printf 'flaws: %d rows of %d run, %d caught, %d missed, inert build %s, %ds\n' \
  "$((rows - skipped))" "$rows" "$caught" "$not_caught" "$inert" "$SECONDS"
if [ "$not_caught" -ne 0 ] || [ "$inert" != ok ]; then
  exit 1
fi
