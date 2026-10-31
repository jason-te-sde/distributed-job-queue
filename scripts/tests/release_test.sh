#!/usr/bin/env bash
# Tests for scripts/check-release.sh. Every case builds a throwaway git
# repository with a CHANGELOG and a chart and prints "PASS <case>"; the first
# failing case aborts the run.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="$(cd "$here/.." && pwd)/check-release.sh"

# Keep the developer's global git configuration (hooks, templates, signing)
# out of the throwaway repositories.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

# The check compares the commit with this ref, which is origin/main in CI. The
# throwaway repositories have no remote, so their main branch stands in for it.
# The case that needs the default unsets it.
export RELEASE_MAIN_REF=main

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

out=""
status=0

# new_repo: an empty repository whose branch is main.
new_repo() {
  local dir
  dir="$(mktemp -d "$workdir/repo.XXXXXX")"
  git -C "$dir" init -q -b main
  git -C "$dir" config user.name "Release Test"
  local local_part=release domain=invalid
  git -C "$dir" config user.email "${local_part}@${domain}"
  git -C "$dir" config commit.gpgsign false
  printf '%s' "$dir"
}

# write_changelog <repo> <version>...: a changelog that lists the versions in
# the order given, after an Unreleased section.
write_changelog() {
  local dir="$1" version
  shift
  {
    printf '# Changelog\n\n## [Unreleased]\n\n'
    for version in "$@"; do
      printf '## [%s] - 2026-10-31\n\n- A change in %s.\n\n' "$version" "$version"
    done
  } >"$dir/CHANGELOG.md"
}

# write_chart <repo> <version line> <appVersion line>: a Chart.yaml with these
# two lines as they are, so that a case can quote or comment them. The chart's
# dependencies carry a version of their own, which is not the chart's.
write_chart() {
  mkdir -p "$1/deploy/helm/jobqueue"
  {
    printf 'apiVersion: v2\nname: jobqueue\n# version: 9.9.9\n'
    printf '%s\n%s\n' "$2" "$3"
    printf 'dependencies:\n  - name: other\n    version: 7.7.7\n'
  } >"$1/deploy/helm/jobqueue/Chart.yaml"
}

commit_all() { # commit_all <repo> <message>
  git -C "$1" add -A
  git -C "$1" commit -q -m "$2"
}

# release_repo: a repository on main whose changelog and chart both say 0.1.0.
release_repo() {
  local dir
  dir="$(new_repo)"
  write_changelog "$dir" 0.1.0 0.0.9
  write_chart "$dir" 'version: 0.1.0' 'appVersion: 0.1.0'
  commit_all "$dir" "chore: release 0.1.0"
  printf '%s' "$dir"
}

# run_check <repo> <tag>: run the check on the repository.
run_check() {
  status=0
  out="$(bash "$check" "$2" "$1" 2>&1)" || status=$?
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

# (a) A tag that matches the changelog, the chart and main passes.
repo="$(release_repo)"
run_check "$repo" v0.1.0
expect_status matching-tag-passes 0
expect_output matching-tag-passes "release: ok"
pass matching-tag-passes

# (b) A changelog whose newest release is 0.0.9 fails and names the file, what
# it says and what the tag wants.
repo="$(new_repo)"
write_changelog "$repo" 0.0.9
write_chart "$repo" 'version: 0.1.0' 'appVersion: 0.1.0'
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status stale-changelog-fails 1
expect_output stale-changelog-fails "CHANGELOG"
expect_output stale-changelog-fails "0.0.9"
expect_output stale-changelog-fails "0.1.0"
pass stale-changelog-fails

# (c) A chart whose appVersion differs fails and names appVersion.
repo="$(new_repo)"
write_changelog "$repo" 0.1.0
write_chart "$repo" 'version: 0.1.0' 'appVersion: 0.1.1'
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status app-version-mismatch-fails 1
expect_output app-version-mismatch-fails "appVersion"
expect_output app-version-mismatch-fails "0.1.1"
pass app-version-mismatch-fails

# (d) A chart whose version differs fails and names version, not appVersion.
repo="$(new_repo)"
write_changelog "$repo" 0.1.0
write_chart "$repo" 'version: 0.2.0' 'appVersion: 0.1.0'
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status chart-version-mismatch-fails 1
expect_output chart-version-mismatch-fails "Chart.yaml"
expect_output chart-version-mismatch-fails "version is 0.2.0"
expect_no_output chart-version-mismatch-fails "appVersion"
pass chart-version-mismatch-fails

# (e) A tag that is not vMAJOR.MINOR.PATCH fails, whatever the files say.
repo="$(release_repo)"
for tag in 0.1 0.1.0 v0.1 v0.1.0.1 v0.1.0-rc1 v0.1.x vv0.1.0 v1.2.3+meta " v0.1.0"; do
  run_check "$repo" "$tag"
  expect_status "malformed-tag-fails ($tag)" 1
  expect_output "malformed-tag-fails ($tag)" "is not a release tag"
done
pass malformed-tag-fails

# (f) Every mismatch is reported in one run, not only the first.
repo="$(new_repo)"
write_changelog "$repo" 0.0.9
write_chart "$repo" 'version: 0.2.0' 'appVersion: 0.3.0'
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status all-mismatches-are-reported 1
expect_output all-mismatches-are-reported "CHANGELOG"
expect_output all-mismatches-are-reported "version is 0.2.0"
expect_output all-mismatches-are-reported "appVersion is 0.3.0"
pass all-mismatches-are-reported

# (g) A commit that is not on main fails and names main.
repo="$(release_repo)"
git -C "$repo" checkout -q -b side
printf 'x\n' >"$repo/x.txt"
commit_all "$repo" "feat: work that is not on main"
run_check "$repo" v0.1.0
expect_status commit-off-main-fails 1
expect_output commit-off-main-fails "main"
pass commit-off-main-fails

# (h) Where CI stands, a detached HEAD at a commit that main contains, passes,
# also when main has moved on since.
repo="$(release_repo)"
tagged="$(git -C "$repo" rev-parse HEAD)"
printf 'x\n' >"$repo/x.txt"
commit_all "$repo" "feat: work after the release commit"
git -C "$repo" checkout -q --detach "$tagged"
run_check "$repo" v0.1.0
expect_status detached-head-on-main-passes 0
pass detached-head-on-main-passes

# (i) The newest release heading counts, and Unreleased is not one. The older
# 0.0.9 in the same file does not satisfy a v0.0.9 tag.
repo="$(release_repo)"
run_check "$repo" v0.0.9
expect_status older-heading-does-not-count 1
expect_output older-heading-does-not-count "CHANGELOG"
pass older-heading-does-not-count

# (j) A changelog with no release heading fails.
repo="$(new_repo)"
printf '# Changelog\n\n## [Unreleased]\n\n- Everything.\n' >"$repo/CHANGELOG.md"
write_chart "$repo" 'version: 0.1.0' 'appVersion: 0.1.0'
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status changelog-without-release-fails 1
expect_output changelog-without-release-fails "CHANGELOG"
pass changelog-without-release-fails

# (k) A missing changelog and a missing chart each fail and are named.
repo="$(new_repo)"
write_chart "$repo" 'version: 0.1.0' 'appVersion: 0.1.0'
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status missing-changelog-fails 1
expect_output missing-changelog-fails "CHANGELOG"
repo="$(new_repo)"
write_changelog "$repo" 0.1.0
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status missing-chart-fails 1
expect_output missing-chart-fails "Chart.yaml"
pass missing-files-fail

# (l) Quoted values and trailing comments in Chart.yaml are read as the value;
# a commented-out or indented version is not the chart's.
repo="$(new_repo)"
write_changelog "$repo" 0.1.0
write_chart "$repo" 'version: "0.1.0" # the chart' "appVersion: '0.1.0'"
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status quoted-chart-values-pass 0
pass quoted-chart-values-pass

# (m) A chart without appVersion fails and says it is missing.
repo="$(new_repo)"
write_changelog "$repo" 0.1.0
write_chart "$repo" 'version: 0.1.0' '# appVersion: 0.1.0'
commit_all "$repo" "chore: release"
run_check "$repo" v0.1.0
expect_status missing-app-version-fails 1
expect_output missing-app-version-fails "appVersion is missing"
pass missing-app-version-fails

# (n) Without the ref of main the check cannot answer, which is an error and
# not a pass: here the default, origin/main, does not exist.
repo="$(release_repo)"
status=0
out="$(env -u RELEASE_MAIN_REF bash "$check" v0.1.0 "$repo" 2>&1)" || status=$?
expect_status missing-main-ref-is-an-error 2
expect_output missing-main-ref-is-an-error "cannot find origin/main"
pass missing-main-ref-is-an-error

# (o) A shallow clone cannot say whether a commit is on main, so it is an error.
repo="$(release_repo)"
printf 'x\n' >"$repo/x.txt"
commit_all "$repo" "feat: a second commit"
shallow="$workdir/shallow"
git clone -q --depth 1 "file://$repo" "$shallow"
run_check "$shallow" v0.1.0
expect_status shallow-clone-is-an-error 2
expect_output shallow-clone-is-an-error "shallow"
pass shallow-clone-is-an-error

# (p) A call without a tag, or with a path that is no repository, is a usage
# error.
status=0
out="$(bash "$check" 2>&1)" || status=$?
expect_status no-tag-is-a-usage-error 2
expect_output no-tag-is-a-usage-error "usage"
run_check "$workdir/absent" v0.1.0
expect_status no-repository-is-an-error 2
expect_output no-repository-is-an-error "absent"
pass usage-errors

# (q) The repository defaults to the one that holds the script, wherever the
# command is run from.
repo="$(release_repo)"
mkdir -p "$repo/scripts"
cp "$check" "$repo/scripts/check-release.sh"
status=0
out="$(cd / && bash "$repo/scripts/check-release.sh" v0.1.0 2>&1)" || status=$?
expect_status default-repository-is-the-scripts-own 0
pass default-repository-is-the-scripts-own
