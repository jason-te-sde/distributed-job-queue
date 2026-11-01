#!/usr/bin/env bash
# Tests for scripts/check-hygiene.sh. Every case builds a throwaway git
# repository and prints "PASS <case>"; the first failing case aborts the run.
#
# Strings that the hygiene check must reject are assembled at runtime from
# pieces, so the offending literals never appear in this repository.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="$(cd "$here/.." && pwd)/check-hygiene.sh"

# Keep the developer's global git configuration (hooks, templates, signing)
# out of the throwaway repositories.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

out=""
status=0

new_repo() {
  local dir
  dir="$(mktemp -d "$workdir/repo.XXXXXX")"
  git -C "$dir" init -q
  git -C "$dir" config user.name "Hygiene Test"
  local local_part=hygiene domain=invalid
  git -C "$dir" config user.email "${local_part}@${domain}"
  git -C "$dir" config commit.gpgsign false
  printf '%s' "$dir"
}

commit_all() { # commit_all <repo> <message>...
  local dir="$1"
  shift
  local args=()
  local msg
  for msg in "$@"; do
    args+=(-m "$msg")
  done
  git -C "$dir" add -A
  git -C "$dir" commit -q "${args[@]}"
}

run_check() { # run_check <repo> [env assignment]...
  local dir="$1"
  shift
  status=0
  out="$(env "$@" bash "$check" "$dir" 2>&1)" || status=$?
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

pass() { printf 'PASS %s\n' "$1"; }

# (a) A clean tracked file passes.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
commit_all "$repo" "feat: add a"
run_check "$repo"
expect_status clean-repo-passes 0
expect_output clean-repo-passes "hygiene: ok"
pass clean-repo-passes

# (b) A tracked file with CJK text fails and is named.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
printf '\xe4\xb8\xad' >"$repo/zh.go"
commit_all "$repo" "feat: add files"
run_check "$repo"
expect_status cjk-file-fails 1
expect_output cjk-file-fails "zh.go"
pass cjk-file-fails

# (c) A commit message carrying an attribution trailer fails.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
trailer="Co-Authored""-By: Cl""aude <noreply@anthr""opic.com>"
commit_all "$repo" "feat: add a" "$trailer"
run_check "$repo"
expect_status attribution-trailer-fails 1
expect_output attribution-trailer-fails "attribution"
pass attribution-trailer-fails

# (d) A patterns file with fewer than five lines is a configuration error.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
commit_all "$repo" "feat: add a"
empty="$workdir/empty-patterns.txt"
: >"$empty"
run_check "$repo" "HYGIENE_PATTERNS=$empty"
expect_status empty-patterns-is-config-error 2
expect_output empty-patterns-is-config-error "patterns file has fewer than 5 lines"
pass empty-patterns-is-config-error

# (e) A tracked file carrying a generation notice fails and is named.
repo="$(new_repo)"
printf '// Generated wi''th Cl''aude.\npackage a\n' >"$repo/notice.go"
commit_all "$repo" "feat: add notice"
run_check "$repo"
expect_status attribution-in-file-fails 1
expect_output attribution-in-file-fails "notice.go:1"
pass attribution-in-file-fails

# (f) The robot emoji, written in the patterns file as an escaped byte
# sequence, is matched against raw bytes in tracked files.
repo="$(new_repo)"
printf '// note \xf0\x9f\xa4\x96\npackage a\n' >"$repo/emoji.go"
commit_all "$repo" "feat: add emoji"
run_check "$repo"
expect_status robot-emoji-fails 1
expect_output robot-emoji-fails "emoji.go:1"
pass robot-emoji-fails

# (g) The patterns file itself is exempt when it is tracked in the repository.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
cp "$here/../attribution-patterns.txt" "$repo/patterns.txt"
commit_all "$repo" "feat: add a and its patterns"
run_check "$repo" "HYGIENE_PATTERNS=$repo/patterns.txt"
expect_status patterns-file-is-exempt 0
pass patterns-file-is-exempt

# (h) A missing patterns file is a configuration error.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
commit_all "$repo" "feat: add a"
run_check "$repo" "HYGIENE_PATTERNS=$workdir/does-not-exist.txt"
expect_status missing-patterns-is-config-error 2
expect_output missing-patterns-is-config-error "cannot read patterns file"
pass missing-patterns-is-config-error

# (i) A pattern that is not a valid regular expression is a configuration
# error, not "no matches".
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
commit_all "$repo" "feat: add a"
malformed="$workdir/malformed-patterns.txt"
{
  printf '(unclosed\n'
  tail -n +2 "$here/../attribution-patterns.txt"
} >"$malformed"
run_check "$repo" "HYGIENE_PATTERNS=$malformed"
expect_status malformed-pattern-is-config-error 2
expect_output malformed-pattern-is-config-error "invalid pattern"
pass malformed-pattern-is-config-error

# (j) CJK text in a commit message fails, like CJK text in a file.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
commit_all "$repo" "feat: add a" "$(printf '\xe4\xb8\xad')"
run_check "$repo"
expect_status cjk-commit-message-fails 1
expect_output cjk-commit-message-fails "cjk-text"
expect_output cjk-commit-message-fails "commit "
pass cjk-commit-message-fails

# (k) A scan that cannot read a tracked file is an error, not a clean run.
# Root can read any file, so the case cannot run there.
if [ "$(id -u)" -eq 0 ]; then
  printf 'SKIP unreadable-file-is-error: running as root\n'
else
  repo="$(new_repo)"
  printf 'package a\n' >"$repo/a.go"
  commit_all "$repo" "feat: add a"
  chmod 000 "$repo/a.go"
  run_check "$repo"
  expect_status unreadable-file-is-error 2
  expect_output unreadable-file-is-error "grep failed"
  pass unreadable-file-is-error
fi

# (l) The "an AI" pattern ends at a word boundary, so a longer word is fine.
repo="$(new_repo)"
printf '// created by an %s\npackage a\n' "aide" >"$repo/aide.go"
commit_all "$repo" "feat: add aide"
run_check "$repo"
expect_status attribution-needs-word-boundary 0
pass attribution-needs-word-boundary

# (m) The same pattern still matches at the end of a line.
repo="$(new_repo)"
printf '// created by an A''I\npackage a\n' >"$repo/ai.go"
commit_all "$repo" "feat: add ai"
run_check "$repo"
expect_status attribution-at-end-of-line-fails 1
expect_output attribution-at-end-of-line-fails "ai.go:1"
pass attribution-at-end-of-line-fails

# (n) History that git cannot read is an error, not an empty history.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
commit_all "$repo" "feat: add a"
head_sha="$(git -C "$repo" rev-parse HEAD)"
rm -f "$repo/.git/objects/${head_sha%"${head_sha#??}"}/${head_sha#??}"
run_check "$repo"
expect_status unreadable-history-is-error 2
expect_output unreadable-history-is-error "git log failed"
pass unreadable-history-is-error

# (o) A tracked file carrying an email address fails and is named.
repo="$(new_repo)"
address="jane.doe""@""example.org"
printf '// Contact %s.\npackage a\n' "$address" >"$repo/contact.go"
commit_all "$repo" "feat: add contact"
run_check "$repo"
expect_status email-in-file-fails 1
expect_output email-in-file-fails "contact.go:1: email-address"
pass email-in-file-fails

# (p) A commit message carrying an email address fails.
repo="$(new_repo)"
printf 'package a\n' >"$repo/a.go"
commit_all "$repo" "feat: add a" "Reported by ${address}."
run_check "$repo"
expect_status email-in-commit-message-fails 1
expect_output email-in-commit-message-fails "email-address"
expect_output email-in-commit-message-fails "commit "
pass email-in-commit-message-fails

# (q) A mention of a GitHub handle, a scoped package or an address with no
# domain is not an email address. The author's address in the commit metadata
# is not scanned (new_repo sets one).
repo="$(new_repo)"
printf '// Thanks to @octocat; see @types/node and user%shost.\npackage a\n' "@" >"$repo/a.go"
commit_all "$repo" "feat: add a"
run_check "$repo"
expect_status handles-are-not-email 0
pass handles-are-not-email
