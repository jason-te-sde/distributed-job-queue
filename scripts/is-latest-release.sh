#!/usr/bin/env bash
# Says whether a release tag is the latest release.
#
# usage: is-latest-release.sh <tag> [root]
#
# Prints "true" when no release tag of the repository is higher than the tag,
# and "false" otherwise. A release tag is vMAJOR.MINOR.PATCH, and tags are
# compared part by part as numbers, so v0.10.0 is above v0.9.0. Every other
# tag is ignored: a pre-release such as v1.0.0-rc1 or a name that is no version
# must not keep a release from being the latest. The tag counts whether or not
# it exists yet. root defaults to the repository that holds this script.
#
# The release workflow tags the image :latest only when this says true, so that
# a release of an older line does not take the name from a newer one.
#
# Output: "true" or "false". Exit status: 0 an answer, 2 usage or environment
# error, which includes a tag that is not a release tag.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
  printf 'is-latest-release: %s\n' "$*" >&2
  exit 2
}

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || die "usage: is-latest-release.sh <tag> [root]"
tag="$1"
root="${2:-$script_dir/..}"
tag_pattern='^v[0-9]+\.[0-9]+\.[0-9]+$'
[[ $tag =~ $tag_pattern ]] || die "the tag \"$tag\" is not a release tag: want vMAJOR.MINOR.PATCH, such as v0.1.0"
[ -d "$root" ] || die "not a directory: $root"
root="$(cd "$root" && pwd)"
git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git work tree: $root"

# The release tags and the tag itself, in ascending order; the last is the
# highest. -t. cuts "vMAJOR.MINOR.PATCH" into three keys, and the first key
# starts after the v. awk only filters, and tail reads to the end, so no stage
# of the pipeline is cut short by a closed pipe.
highest="$({ git -C "$root" tag --list 'v*' && printf '%s\n' "$tag"; } |
  awk '/^v[0-9]+\.[0-9]+\.[0-9]+$/' |
  LC_ALL=C sort -t. -k1.2,1n -k2,2n -k3,3n |
  tail -n 1)"
if [ "$highest" = "$tag" ]; then
  echo true
else
  echo false
fi
