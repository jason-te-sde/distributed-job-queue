#!/usr/bin/env bash
# Tests for scripts/check-release.sh and scripts/is-latest-release.sh. Every
# case builds a throwaway git repository, with a CHANGELOG and a chart where the
# case needs them, and prints "PASS <case>"; the first failing case aborts the
# run.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="$(cd "$here/.." && pwd)/check-release.sh"
latest="$(cd "$here/.." && pwd)/is-latest-release.sh"

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

# (g) A commit that is not on main fails and says so.
repo="$(release_repo)"
git -C "$repo" checkout -q -b side
printf 'x\n' >"$repo/x.txt"
commit_all "$repo" "feat: work that is not on main"
run_check "$repo" v0.1.0
expect_status commit-off-main-fails 1
expect_output commit-off-main-fails "is not on main"
pass commit-off-main-fails

# (h) The commit must be on main, and proto/ must be what main has. The lint of
# the preflight compares proto/ with main using buf breaking, so a proto change
# that landed on main after the tagged commit would fail it. A commit behind
# the tip of main passes when nothing in proto/ has changed since, and fails,
# naming proto/ and buf breaking, when something has. Releases are cut from the
# tip, which always passes; the tip itself is not required, because main may
# move while the release runs.
repo="$(release_repo)"
mkdir -p "$repo/proto"
printf 'syntax = "proto3";\n' >"$repo/proto/a.proto"
commit_all "$repo" "feat: add a proto file"
tagged="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" checkout -q --detach "$tagged"
run_check "$repo" v0.1.0
expect_status detached-head-at-the-tip-passes 0
git -C "$repo" checkout -q main
printf 'x\n' >"$repo/x.txt"
commit_all "$repo" "feat: work that does not touch proto"
git -C "$repo" checkout -q --detach "$tagged"
run_check "$repo" v0.1.0
expect_status behind-the-tip-without-a-proto-change-passes 0
expect_output behind-the-tip-without-a-proto-change-passes "release: ok"
git -C "$repo" checkout -q main
printf 'syntax = "proto3";\nmessage M {}\n' >"$repo/proto/a.proto"
commit_all "$repo" "feat: change a proto file"
git -C "$repo" checkout -q --detach "$tagged"
run_check "$repo" v0.1.0
expect_status behind-the-tip-with-a-proto-change-fails 1
expect_output behind-the-tip-with-a-proto-change-fails "proto/"
expect_output behind-the-tip-with-a-proto-change-fails "buf breaking"
expect_no_output behind-the-tip-with-a-proto-change-fails "is not on main"
git -C "$repo" checkout -q main
run_check "$repo" v0.1.0
expect_status tip-with-a-proto-change-passes 0
pass commit-on-main-and-proto

# A proto/ that main has gained since the commit counts as a change too, and a
# change that main made and then undid does not.
repo="$(release_repo)"
tagged="$(git -C "$repo" rev-parse HEAD)"
mkdir -p "$repo/proto"
printf 'syntax = "proto3";\n' >"$repo/proto/a.proto"
commit_all "$repo" "feat: add proto"
git -C "$repo" checkout -q --detach "$tagged"
run_check "$repo" v0.1.0
expect_status proto-added-on-main-fails 1
expect_output proto-added-on-main-fails "proto/"
git -C "$repo" checkout -q main
git -C "$repo" rm -q -r proto
commit_all "$repo" "revert: remove proto again"
git -C "$repo" checkout -q --detach "$tagged"
run_check "$repo" v0.1.0
expect_status proto-added-and-removed-passes 0
pass proto-added-on-main

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

# (o) A shallow clone cannot say whether a commit is on main, so it is an
# error, also at the tip.
repo="$(release_repo)"
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

# (r) The shape of CI: a full clone with a real origin, a detached HEAD at the
# tag's commit, and the default ref, origin/main. The check passes at the tip of
# origin/main, and after origin/main has moved on, as long as the move did not
# touch proto/; a move that did fails it.
repo="$(release_repo)"
mkdir -p "$repo/proto"
printf 'syntax = "proto3";\n' >"$repo/proto/a.proto"
commit_all "$repo" "feat: add a proto file"
upstream="$workdir/upstream.git"
git clone -q --bare "$repo" "$upstream"
full="$workdir/full"
git clone -q "file://$upstream" "$full"
git -C "$full" checkout -q --detach origin/main
status=0
out="$(env -u RELEASE_MAIN_REF bash "$check" v0.1.0 "$full" 2>&1)" || status=$?
expect_status ci-shape-at-the-tip-passes 0
expect_output ci-shape-at-the-tip-passes "release: ok"
printf 'x\n' >"$repo/x.txt"
commit_all "$repo" "feat: work that does not touch proto"
git -C "$repo" push -q "$upstream" main
git -C "$full" fetch -q origin
status=0
out="$(env -u RELEASE_MAIN_REF bash "$check" v0.1.0 "$full" 2>&1)" || status=$?
expect_status ci-shape-behind-origin-main-passes 0
printf 'syntax = "proto3";\nmessage M {}\n' >"$repo/proto/a.proto"
commit_all "$repo" "feat: change a proto file"
git -C "$repo" push -q "$upstream" main
git -C "$full" fetch -q origin
status=0
out="$(env -u RELEASE_MAIN_REF bash "$check" v0.1.0 "$full" 2>&1)" || status=$?
expect_status ci-shape-proto-moved-on-fails 1
expect_output ci-shape-proto-moved-on-fails "proto/"
expect_output ci-shape-proto-moved-on-fails "origin/main"
pass ci-shape

# run_latest <repo> <tag>: run is-latest-release.sh on the repository.
run_latest() {
  status=0
  out="$(bash "$latest" "$2" "$1" 2>&1)" || status=$?
}

# tag_repo <tag>...: a repository with these tags.
tag_repo() {
  local dir tag
  dir="$(new_repo)"
  printf 'x\n' >"$dir/x.txt"
  commit_all "$dir" "chore: start"
  for tag in "$@"; do
    git -C "$dir" tag "$tag"
  done
  printf '%s' "$dir"
}

# (s) The highest release tag is the latest, compared as numbers: 10.0.0 is
# above 9.0.0, 0.10.0 above 0.9.0 and 1.2.10 above 1.2.9, which a text sort of
# any of the three parts gets wrong.
repo="$(tag_repo v0.2.0 v0.9.0 v0.10.0 v1.2.9 v1.2.10 v0.1.0)"
run_latest "$repo" v1.2.10
expect_status highest-tag-is-latest 0
[ "$out" = true ] || { printf 'FAIL highest-tag-is-latest: got %q\n' "$out" >&2; exit 1; }
for tag in v1.2.9 v0.10.0 v0.9.0 v0.1.0; do
  run_latest "$repo" "$tag"
  expect_status "lower-tag-is-not-latest ($tag)" 0
  [ "$out" = false ] || { printf 'FAIL lower-tag-is-not-latest (%s): got %q\n' "$tag" "$out" >&2; exit 1; }
done
repo="$(tag_repo v0.2.0 v0.9.0 v0.10.0)"
run_latest "$repo" v0.10.0
[ "$out" = true ] || { printf 'FAIL numeric-order: v0.10.0 got %q\n' "$out" >&2; exit 1; }
run_latest "$repo" v0.9.0
[ "$out" = false ] || { printf 'FAIL numeric-order: v0.9.0 got %q\n' "$out" >&2; exit 1; }
repo="$(tag_repo v9.0.0 v10.0.0)"
run_latest "$repo" v10.0.0
[ "$out" = true ] || { printf 'FAIL numeric-order: v10.0.0 got %q\n' "$out" >&2; exit 1; }
run_latest "$repo" v9.0.0
[ "$out" = false ] || { printf 'FAIL numeric-order: v9.0.0 got %q\n' "$out" >&2; exit 1; }
pass latest-is-the-highest-tag-by-number

# (t) Tags that are not releases do not count: a higher pre-release tag, a bare
# major, and names that are no version at all must not keep a release from
# being the latest.
repo="$(tag_repo v0.1.0 v1.0.0-rc1 v2 v1.0 latest vfoo nightly-2026-10-31)"
run_latest "$repo" v0.1.0
expect_status non-release-tags-are-ignored 0
[ "$out" = true ] || { printf 'FAIL non-release-tags-are-ignored: got %q\n' "$out" >&2; exit 1; }
pass non-release-tags-are-ignored

# (u) The tag being released counts even when it does not exist yet: a version
# above every tag is the latest, a version below one is not, and with no tags
# at all the first release is.
repo="$(tag_repo v0.1.0)"
run_latest "$repo" v0.2.0
[ "$out" = true ] || { printf 'FAIL untagged-higher-version: got %q\n' "$out" >&2; exit 1; }
run_latest "$repo" v0.0.9
[ "$out" = false ] || { printf 'FAIL untagged-lower-version: got %q\n' "$out" >&2; exit 1; }
repo="$(tag_repo)"
run_latest "$repo" v0.1.0
[ "$out" = true ] || { printf 'FAIL first-release: got %q\n' "$out" >&2; exit 1; }
pass untagged-version-counts

# (v) A tag that is not a release tag, no tag, or a path that is no repository
# is an error, not an answer.
repo="$(tag_repo v0.1.0)"
for tag in 0.1.0 v0.1 v0.1.0-rc1 ""; do
  run_latest "$repo" "$tag"
  expect_status "latest-malformed-tag-is-an-error ($tag)" 2
  expect_output "latest-malformed-tag-is-an-error ($tag)" "is-latest-release"
done
status=0
out="$(bash "$latest" 2>&1)" || status=$?
expect_status latest-no-tag-is-a-usage-error 2
expect_output latest-no-tag-is-a-usage-error "usage"
run_latest "$workdir/absent" v0.1.0
expect_status latest-no-repository-is-an-error 2
expect_output latest-no-repository-is-an-error "absent"
pass latest-usage-errors
