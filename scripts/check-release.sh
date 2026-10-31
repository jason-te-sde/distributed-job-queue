#!/usr/bin/env bash
# Checks that a release tag agrees with the files it releases.
#
# usage: check-release.sh <tag> [root]
#
# The tag is a name such as v0.1.0. root is the repository to check; it
# defaults to the repository that holds this script. The check passes when
# all of these hold, and it reports every one that does not:
#
#   - the tag is vMAJOR.MINOR.PATCH, with nothing before or after;
#   - the first "## [x.y.z]" heading of CHANGELOG.md is the tag without the v;
#     a heading that is not a version, such as "## [Unreleased]", is skipped;
#   - deploy/helm/jobqueue/Chart.yaml has that version as its version and as
#     its appVersion, which is the tag of the image the chart deploys;
#   - the checked-out commit is the tip of main.
#
# Main is the ref in RELEASE_MAIN_REF, which is origin/main unless the variable
# says otherwise. The tip is required, not just a place on main: a release is
# cut from main's tip, and the lint that the release build runs compares the
# proto files with the tip of main, so an older commit that was tagged after a
# proto change would fail it for a reason that has nothing to do with the
# release. The check compares two commit ids and needs no history.
#
# Output: "release: ok", or one "release: FAIL: ..." line per problem.
# Exit status: 0 ok, 1 a requirement is not met, 2 usage or environment error,
# which includes a main ref that does not exist: a check that could not run is
# never reported as passed.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
main_ref="${RELEASE_MAIN_REF:-origin/main}"
changelog_name=CHANGELOG.md
chart_name=deploy/helm/jobqueue/Chart.yaml

die() {
  printf 'release: %s\n' "$*" >&2
  exit 2
}

failures=0
fail() {
  printf 'release: FAIL: %s\n' "$*" >&2
  failures=$((failures + 1))
}

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || die "usage: check-release.sh <tag> [root]"
tag="$1"
root="${2:-$script_dir/..}"
[ -d "$root" ] || die "not a directory: $root"
root="$(cd "$root" && pwd)"
git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git work tree: $root"

# The tag must be a version before it can be compared with anything.
tag_pattern='^v[0-9]+\.[0-9]+\.[0-9]+$'
if ! [[ $tag =~ $tag_pattern ]]; then
  fail "the tag \"$tag\" is not a release tag: want vMAJOR.MINOR.PATCH, such as v0.1.0"
  exit 1
fi
want="${tag#v}"

# The first "## [x.y.z]" heading of the changelog.
changelog="$root/$changelog_name"
if [ ! -f "$changelog" ]; then
  fail "$changelog_name: not found, so there are no release notes for $want"
else
  found="$(sed -n -E '/^## \[[0-9]+\.[0-9]+\.[0-9]+\]/{s/^## \[([^]]*)\].*/\1/;p;q;}' "$changelog")"
  if [ -z "$found" ]; then
    fail "$changelog_name: has no \"## [x.y.z]\" heading, want one for $want"
  elif [ "$found" != "$want" ]; then
    fail "$changelog_name: the first release heading is $found, want $want (the tag without the v)"
  fi
fi

# chart_field <name>: the value of a top-level key of Chart.yaml, without its
# quotes or a trailing comment. Only a key at the start of a line counts, so a
# commented-out line or a dependency's version is not mistaken for the chart's.
chart="$root/$chart_name"
chart_field() {
  sed -n -E "/^$1:/{s/^$1:[[:space:]]*(\"([^\"]*)\"|'([^']*)'|([^[:space:]#\"']+))[[:space:]]*(#.*)?\$/\\2\\3\\4/;p;q;}" "$chart"
}

if [ ! -f "$chart" ]; then
  fail "$chart_name: not found"
else
  for key in version appVersion; do
    value="$(chart_field "$key")"
    if [ -z "$value" ]; then
      fail "$chart_name: $key is missing, want $want"
    elif [ "$value" != "$want" ]; then
      fail "$chart_name: $key is $value, want $want"
    fi
  done
fi

# The commit is the tip of main. When it is not, ancestry tells the two ways of
# missing apart: a commit off main, such as one on another branch, and a commit
# that is on main but behind its tip.
git -C "$root" rev-parse --verify --quiet "$main_ref^{commit}" >/dev/null ||
  die "cannot find $main_ref, so it cannot tell whether the commit is the tip of main; fetch it, or name the ref in RELEASE_MAIN_REF"
tip="$(git -C "$root" rev-parse "$main_ref^{commit}")"
head="$(git -C "$root" rev-parse HEAD)" || die "the repository has no commit"
if [ "$head" != "$tip" ]; then
  ancestor=0
  git -C "$root" merge-base --is-ancestor "$head" "$tip" || ancestor=$?
  case "$ancestor" in
    0) fail "the commit $head is on main but is not its tip: $main_ref is at $tip, and a release is cut from the tip of main" ;;
    1) fail "the commit $head is not on main ($main_ref is at $tip): a release is cut from the tip of main" ;;
    *) die "git merge-base failed (status $ancestor) for $main_ref" ;;
  esac
fi

[ "$failures" -eq 0 ] || exit 1
printf 'release: ok\n'
