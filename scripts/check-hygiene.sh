#!/usr/bin/env bash
# Repository hygiene check.
#
# usage: check-hygiene.sh [root]
#
# Fails when a tracked file or a commit message on the current branch
# contains CJK text or an email address, or matches an attribution pattern.
# The author and committer addresses of a commit are metadata, not message,
# and are not scanned.
#
# The patterns live in attribution-patterns.txt next to this script: one
# extended regular expression per line, matched case-insensitively. The
# environment variable HYGIENE_PATTERNS names a different file. Inside a
# pattern, \xHH stands for the raw byte HH, which keeps non-ASCII byte
# sequences readable. The patterns file is never scanned for patterns.
#
# Output: "hygiene: ok", or one "path:line: rule" line per violation.
# Exit status: 0 clean, 1 violations found, 2 configuration or usage error,
# which includes any failure of grep or git themselves: a scan that could
# not run is never reported as clean.
set -euo pipefail

# Everything below works on bytes, whatever the caller's locale says.
export LC_ALL=C

root="${1:-.}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
patterns_file="${HYGIENE_PATTERNS:-$script_dir/attribution-patterns.txt}"
min_patterns=5

die() {
  printf 'hygiene: %s\n' "$*" >&2
  exit 2
}

[ -f "$patterns_file" ] && [ -r "$patterns_file" ] || die "cannot read patterns file: $patterns_file"
git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git work tree: $root"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# expand_escapes <text>: print text with every \xHH replaced by the byte HH.
expand_escapes() {
  local rest="$1" done_part="" match hex byte
  local escape_re='\\x([0-9A-Fa-f]{2})'
  while [[ $rest =~ $escape_re ]]; do
    match="${BASH_REMATCH[0]}"
    hex="${BASH_REMATCH[1]}"
    printf -v byte "\\x$hex"
    done_part="$done_part${rest%%"$match"*}$byte"
    rest="${rest#*"$match"}"
  done
  printf '%s%s' "$done_part" "$rest"
}

expanded="$tmp/patterns"
: >"$expanded"
pattern_count=0
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    *[![:space:]]*) ;;
    *) continue ;;
  esac
  expand_escapes "$line" >>"$expanded"
  printf '\n' >>"$expanded"
  pattern_count=$((pattern_count + 1))
done <"$patterns_file"
[ "$pattern_count" -ge "$min_patterns" ] || die "patterns file has fewer than $min_patterns lines: $patterns_file"

# run_grep <grep arguments>...: leave the matching lines in $tmp/hits. Status 1
# means no match; a higher status is grep itself failing (an unreadable file,
# a bad expression) and ends the run, so it cannot read as "no matches".
run_grep() {
  local status=0
  grep "$@" >"$tmp/hits" 2>"$tmp/grep-error" || status=$?
  if [ "$status" -gt 1 ]; then
    die "grep failed (status $status): $(head -n 1 "$tmp/grep-error")"
  fi
}

# Compile every pattern once, against no input, before any scan relies on them.
status=0
grep -E -f "$expanded" /dev/null 2>"$tmp/grep-error" || status=$?
[ "$status" -le 1 ] || die "patterns file has an invalid pattern: $(head -n 1 "$tmp/grep-error")"

# CJK Unified Ideographs (U+4E00 to U+9FFF) as UTF-8 byte sequences.
cjk_regex=$'\xe4[\xb8-\xbf][\x80-\xbf]|[\xe5-\xe8][\x80-\xbf][\x80-\xbf]|\xe9[\x80-\xbf][\x80-\xbf]'
# An email address: a local part, an @, and a domain with at least one dot. A
# GitHub handle (@name), a scoped package (@scope/name) and user@host are not.
email_regex='[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}'

root_abs="$(cd "$root" && pwd -P)"
patterns_abs="$(cd "$(dirname "$patterns_file")" && pwd -P)/$(basename "$patterns_file")"

violations=0
report() {
  printf '%s\n' "$1"
  violations=$((violations + 1))
}

# report_hits <where> <rule>: report one violation per line in $tmp/hits.
report_hits() {
  local hit
  while IFS= read -r hit; do
    report "$1:${hit%%:*}: $2"
  done <"$tmp/hits"
}

# scan_file <path relative to root>: report every hit in one tracked file.
scan_file() {
  local rel="$1" abs="$root_abs/$1"
  [ -f "$abs" ] || return 0
  run_grep -InE -e "$cjk_regex" -- "$abs"
  report_hits "$rel" cjk-text
  run_grep -InE -e "$email_regex" -- "$abs"
  report_hits "$rel" email-address
  [ "$abs" != "$patterns_abs" ] || return 0
  run_grep -InEi -f "$expanded" -- "$abs"
  report_hits "$rel" attribution
}

git -C "$root" ls-files -z >"$tmp/files" || die "git ls-files failed in $root"
while IFS= read -r -d '' rel; do
  scan_file "$rel"
done <"$tmp/files"

# A repository without commits has no history to scan.
: >"$tmp/commits"
if git -C "$root" rev-parse --verify --quiet HEAD >/dev/null; then
  git -C "$root" log --format=%H >"$tmp/commits" || die "git log failed in $root"
fi
while IFS= read -r sha; do
  git -C "$root" show -s --format=%B "$sha" >"$tmp/message" || die "git show failed for $sha in $root"
  run_grep -nE -e "$cjk_regex" -- "$tmp/message"
  report_hits "commit $sha" cjk-text
  run_grep -nE -e "$email_regex" -- "$tmp/message"
  report_hits "commit $sha" email-address
  run_grep -nEi -f "$expanded" -- "$tmp/message"
  report_hits "commit $sha" attribution
done <"$tmp/commits"

if [ "$violations" -gt 0 ]; then
  printf 'hygiene: %d violation(s)\n' "$violations" >&2
  exit 1
fi
printf 'hygiene: ok\n'
