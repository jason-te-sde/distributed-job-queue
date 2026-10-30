#!/usr/bin/env bash
# Tests for scripts/check-pg-count.sh. Every case writes fixture `go test -json`
# files into a throwaway directory and prints "PASS <case>"; the first failing
# case aborts the run.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="$(cd "$here/.." && pwd)/check-pg-count.sh"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

out=""
status=0

# event <action> [test]: print one go test -json event for package p.
event() {
  if [ -n "${2:-}" ]; then
    printf '{"Time":"2026-10-29T10:00:00Z","Action":"%s","Package":"example.com/p","Test":"%s","Elapsed":0.01}\n' "$1" "$2"
  else
    printf '{"Time":"2026-10-29T10:00:00Z","Action":"%s","Package":"example.com/p","Elapsed":0.01}\n' "$1"
  fi
}

# output_event <test> <json-escaped text>: print one output event for package
# p, as go test -json writes the text of a test, with its escapes.
output_event() {
  printf '{"Time":"2026-10-29T10:00:00Z","Action":"output","Package":"example.com/p","Test":"%s","Output":"%s","OutputType":"frame"}\n' "$1" "$2"
}

# three_passes: the fixture the minimum is compared with. Subtests, tests
# outside the TestPG prefix, package events and non-pass events must not
# change the count of 3.
three_passes() {
  event run TestPGOne
  event pass TestPGOne
  event run TestPGTwo
  event run TestPGTwo/subtest
  event pass TestPGTwo/subtest
  event pass TestPGTwo
  event pass TestPGThree
  event pass TestUnit
  event pass TestPGLowercase/Deep
  event skip TestPGSkipped
  event pass
}

# run_check <json file> <minimum, or "-" for a minimum file that does not exist>
run_check() {
  local json="$1" min="$2" min_file="$workdir/min.txt"
  if [ "$min" = "-" ]; then
    min_file="$workdir/does-not-exist.txt"
  else
    printf '%s\n' "$min" >"$min_file"
  fi
  status=0
  out="$(PG_MIN_TESTS_FILE="$min_file" bash "$check" "$json" 2>&1)" || status=$?
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

# (a) Exactly the minimum passes, and the report says so.
three_passes >"$workdir/ok.json"
run_check "$workdir/ok.json" 3
expect_status minimum-met 0
expect_output minimum-met "pg tests: 3 passed (minimum 3)"
pass minimum-met

# (b) Below the minimum fails and names the shortfall.
run_check "$workdir/ok.json" 4
expect_status below-minimum-fails 1
expect_output below-minimum-fails "pg tests: 3 passed, minimum is 4 (short by 1)"
pass below-minimum-fails

# (c) Above the minimum passes: the minimum is a floor.
run_check "$workdir/ok.json" 2
expect_status above-minimum-passes 0
expect_output above-minimum-passes "pg tests: 3 passed (minimum 2)"
pass above-minimum-passes

# (d) One failed test fails the run even when the count is met, and the
# failing test is named.
{
  three_passes
  event run TestPGBroken
  event fail TestPGBroken
  event fail
} >"$workdir/fail.json"
run_check "$workdir/fail.json" 3
expect_status failed-test-fails 1
expect_output failed-test-fails "TestPGBroken"
expect_no_output failed-test-fails "passed (minimum"
pass failed-test-fails

# (e) A package that failed without a test event (a build failure) fails too.
{
  three_passes
  event fail
} >"$workdir/pkgfail.json"
run_check "$workdir/pkgfail.json" 3
expect_status failed-package-fails 1
expect_output failed-package-fails "example.com/p"
pass failed-package-fails

# (f) A missing results file is a usage error, distinct from a failure.
run_check "$workdir/absent.json" 3
expect_status missing-results-file 2
expect_output missing-results-file "absent.json"
pass missing-results-file

# (g) No argument is a usage error.
status=0
out="$(PG_MIN_TESTS_FILE="$workdir/min.txt" bash "$check" 2>&1)" || status=$?
expect_status no-argument 2
expect_output no-argument "usage"
pass no-argument

# (h) A missing minimum file is a configuration error.
run_check "$workdir/ok.json" -
expect_status missing-minimum-file 2
expect_output missing-minimum-file "does-not-exist.txt"
pass missing-minimum-file

# (i) A minimum that is not a whole number is a configuration error.
run_check "$workdir/ok.json" "three"
expect_status malformed-minimum 2
expect_output malformed-minimum "three"
pass malformed-minimum

# (j) An empty results file counts zero passes and fails against any minimum.
: >"$workdir/empty.json"
run_check "$workdir/empty.json" 1
expect_status empty-results-fails 1
expect_output empty-results-fails "pg tests: 0 passed, minimum is 1 (short by 1)"
pass empty-results-fails

# (l) The text a failing test wrote is shown with its JSON escapes decoded,
# and the text of a passing test is not.
{
  three_passes
  output_event TestPGOne 'noise from a passing test\n'
  event run TestPGBroken
  output_event TestPGBroken '    x_test.go:6: want \"a\"\tgot \"b\"\n'
  output_event TestPGBroken 'path C:\\tmp\n'
  event fail TestPGBroken
} >"$workdir/output.json"
run_check "$workdir/output.json" 3
expect_status failing-output-is-shown 1
expect_output failing-output-is-shown "x_test.go:6: want \"a\"$(printf '\t')got \"b\""
expect_output failing-output-is-shown 'path C:\tmp'
expect_no_output failing-output-is-shown "noise from a passing test"
pass failing-output-is-shown

# (m) A package that did not build shows the compiler's complaint, which go
# test -json reports in build-output events.
{
  three_passes
  printf '%s\n' '{"ImportPath":"example.com/p [example.com/p.test]","Action":"build-output","Output":"p_test.go:5:34: undefined: missing\n"}'
  event fail
} >"$workdir/build.json"
run_check "$workdir/build.json" 3
expect_status build-failure-shows-compiler-output 1
expect_output build-failure-shows-compiler-output "p_test.go:5:34: undefined: missing"
pass build-failure-shows-compiler-output

# (k) The committed minimum is a positive whole number, so the real run
# cannot be configured away by an empty or zero file.
committed="$(tr -d '[:space:]' <"$here/../pg-min-tests.txt")"
case "$committed" in
  '' | *[!0-9]* | 0)
    printf 'FAIL committed-minimum: scripts/pg-min-tests.txt holds %q, want a positive whole number\n' "$committed" >&2
    exit 1
    ;;
esac
pass committed-minimum
