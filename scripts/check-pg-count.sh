#!/usr/bin/env bash
# Checks the results of a `go test -json` run of the Postgres tests.
#
# usage: check-pg-count.sh <json-file>
#
# Fails when any event in the file has "Action":"fail", and when fewer
# top-level tests named TestPG* passed than the minimum in pg-min-tests.txt
# next to this script. The count is what makes the run trustworthy: a test that
# is skipped, excluded by a build tag or lost in a refactor leaves the build
# green but lowers it, and the minimum, raised in the commit that adds tests,
# turns that into a failure. The environment variable PG_MIN_TESTS_FILE names
# a different minimum file.
#
# Output: "pg tests: N passed (minimum M)", or the failures with the output
# the failing tests produced, or the shortfall. The run's own text goes into
# the JSON file and not to the terminal, so a failure has to be reported from
# there.
# Exit status: 0 ok, 1 a failure or too few passes, 2 usage or configuration
# error.
set -euo pipefail

# Everything below works on bytes, whatever the caller's locale says.
export LC_ALL=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
min_file="${PG_MIN_TESTS_FILE:-$script_dir/pg-min-tests.txt}"

die() {
  printf 'pg tests: %s\n' "$*" >&2
  exit 2
}

[ "$#" -eq 1 ] || die "usage: check-pg-count.sh <go test -json file>"
results="$1"
[ -f "$results" ] && [ -r "$results" ] || die "cannot read the results file: $results"
[ -f "$min_file" ] && [ -r "$min_file" ] || die "cannot read the minimum file: $min_file"
minimum="$(tr -d '[:space:]' <"$min_file")"
case "$minimum" in
  '' | *[!0-9]*) die "the minimum file $min_file must hold a whole number, found: $minimum" ;;
esac

# json_field <name> <line>: print the string value of a top-level field.
json_field() {
  printf '%s\n' "$2" | sed -n "s/.*\"$1\":\"\\([^\"]*\\)\".*/\\1/p"
}

# output_text: read go test -json events on stdin and print their Output
# fields with the JSON string escapes decoded. A literal backslash is parked
# in a control character, which JSON never carries unescaped, while the other
# escapes are decoded. Each Output ends in a newline of its own, which sed's
# line break replaces.
output_text() {
  local park=$'\001'
  sed -nE 's/.*"Output":"(([^"\\]|\\.)*)".*/\1/p' |
    sed -e "s/\\\\\\\\/$park/g" -e 's/\\n$//' -e 's/\\n/\
/g' -e 's/\\t/	/g' -e 's/\\"/"/g' \
      -e 's/\\u003c/</g' -e 's/\\u003e/>/g' -e 's/\\u0026/\&/g' -e "s/$park/\\\\/g"
}

# show_output <package> [<test>]: print what a test, or a package outside any
# test, wrote while running.
show_output() {
  local events
  events="$(grep -F '"Action":"output"' -- "$results" | grep -F "\"Package\":\"$1\"" || true)"
  if [ -n "${2:-}" ]; then
    printf '%s\n' "$events" | { grep -F "\"Test\":\"$2\"" || true; } | output_text >&2
  else
    printf '%s\n' "$events" | { grep -vF '"Test":"' || true; } | output_text >&2
  fi
}

failed=0
while IFS= read -r line; do
  failed=1
  package="$(json_field Package "$line")"
  test_name="$(json_field Test "$line")"
  if [ -n "$test_name" ]; then
    printf 'FAIL %s %s\n' "$package" "$test_name" >&2
    show_output "$package" "$test_name"
  else
    printf 'FAIL package %s\n' "${package:-unknown}" >&2
    show_output "$package"
  fi
done < <(grep -E '"Action":"fail"' -- "$results" || true)
if [ "$failed" -ne 0 ]; then
  # A package that did not build reports its compiler errors in separate events.
  { grep -F '"Action":"build-output"' -- "$results" || true; } | output_text >&2
  printf 'pg tests: failures found in %s\n' "$results" >&2
  exit 1
fi

# Passing top-level tests only: a subtest name contains a slash, and a package
# event has no Test field.
passed="$({ grep -E '"Action":"pass"' -- "$results" || true; } | { grep -cE '"Test":"TestPG[^"/]*"' || true; })"

if [ "$passed" -lt "$minimum" ]; then
  printf 'pg tests: %d passed, minimum is %d (short by %d)\n' "$passed" "$minimum" "$((minimum - passed))" >&2
  exit 1
fi
printf 'pg tests: %d passed (minimum %d)\n' "$passed" "$minimum"
