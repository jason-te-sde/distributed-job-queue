# Releasing

A release is a tag on `main`. Pushing a tag that matches `v*` starts the release workflow (`.github/workflows/release.yml`), which checks the repository against the tag, builds the binaries and the image, packages the chart and creates the GitHub release. **Nothing is built or published from a laptop.** The workflow refuses to release when the repository does not agree with the tag, so the steps below are the ones it checks.

Versions follow [Semantic Versioning](https://semver.org). The tag is `vX.Y.Z`; the version without the `v` is the version in the changelog, in the chart and in the image tag.

## Before the tag

1. **Everything is merged and green.** All work for the release is on `main`, and the CI run on the merge commit is green: lint, the test matrix, the pg jobs on PostgreSQL 15 and 18, the planted flaws, the chaos test, the Kubernetes end-to-end test, the image, the clean clone, the benchmark smoke test and the hygiene check. The release workflow runs the preflight again, with a PostgreSQL 18 service, so the PostgreSQL tests and the planted flaws run in it too. What it does not run, and what you checked on `main`, is the chaos test, the Kubernetes end-to-end test and the pg job on PostgreSQL 15.
2. **Write the changelog.** In [CHANGELOG.md](CHANGELOG.md), put the release at the top as `## [X.Y.Z] - YYYY-MM-DD`, in the Keep a Changelog format, with the date of the release. The release check reads the **first** heading of the form `## [x.y.z]` and requires it to be the tag without the `v`; a heading such as `## [Unreleased]` is skipped. The text under it is the release page's notes, and the workflow fails if there is none. Note any migration that an older broker cannot run against, and add the link reference at the bottom.
3. **Set the chart version.** In `deploy/helm/jobqueue/Chart.yaml`, `version` and `appVersion` are both `X.Y.Z`. `appVersion` is the tag of the image that the chart deploys.
4. **Run `make preflight`** on the commit you mean to tag (with `JOBQUEUE_TEST_DSN` set, so that the PostgreSQL tests and the flaw rows that need a database run too).
5. **Check that the measured numbers are current.** Every figure in the README comes from [docs/benchmarks.md](docs/benchmarks.md); if a change since the last run could move them, run the benchmarks again.

Merge these changes to `main` in a pull request like any other.

## Tag and push

```sh
git switch main && git pull --ff-only
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

The tag must point at a commit that is on `main`.

## What the workflow does

The steps that only build come before the first one that publishes, so a failure while building leaves nothing behind, and the image is published before the release page, because the release page is what announces it.

1. **Checks the repository against the tag** with `scripts/check-release.sh "$GITHUB_REF_NAME"`, and fails with every mismatch if any of these is untrue: the tag matches `^v[0-9]+\.[0-9]+\.[0-9]+$` (nothing before or after); the first `## [x.y.z]` heading of `CHANGELOG.md` is the tag without the `v`; `version` and `appVersion` in `Chart.yaml` are both that version; the tagged commit is reachable from `origin/main`; and `proto/` has not changed on `origin/main` since the tagged commit (the lint of step 2 compares `proto/` with `origin/main` using `buf breaking`, so a protocol change that landed on main after the tag would fail it). The check needs the whole history (the checkout uses `fetch-depth: 0`) and refuses to answer from a shallow clone.
2. **Runs `make preflight`:** formatting, the generated code, lint (`golangci-lint` and `buf`, installed at the versions of the Makefile), the hygiene check, the Helm checks, the tests of the scripts, `go test -race ./...`, the PostgreSQL tests and the planted-flaw check. The job starts a PostgreSQL 18 service for the last two and gives the preflight `JOBQUEUE_TEST_DSN`.
3. **Builds `jobqueue-server` and `jobqueuectl`** for linux and darwin on amd64 and arm64, statically, with `-X main.version=<tag>` (the tag keeps its `v`). Each platform is one archive, `jobqueue_<version>_<os>_<arch>.tar.gz`, holding both binaries and the `LICENSE`.
4. **Checks that the binaries report the tag:** it unpacks the linux/amd64 archive and requires `jobqueue-server --version` and `jobqueuectl version` to print exactly the tag. An `-X` flag that names a variable that does not exist is ignored silently, which is why it asks.
5. **Packages the Helm chart** with `helm package`, giving `jobqueue-<version>.tgz`.
6. **Writes `SHA256SUMS`** over the archives and the chart, and verifies it.
7. **Extracts the release notes:** the section of the changelog under the heading of this version, without the heading.
8. **Builds and pushes the multi-architecture image** (linux/amd64 and linux/arm64) to `ghcr.io/jason-te-sde/distributed-job-queue`, tagged `<version>` (without the `v`), and also `latest` when the tag is the highest release tag (`scripts/is-latest-release.sh` decides, so that a release of an older line does not take the name), with labels that name the source repository, the commit, the version and the licence.
9. **Creates the GitHub release** for the tag as a draft (`gh release create --verify-tag --draft`), titled with the tag, with the notes of step 7 and the archives, the chart and `SHA256SUMS` attached, and publishes it only then, so that a failed upload never leaves a public release with files missing. The `publish` job, which runs steps 8 and 9, begins by refusing to run if a release or a draft for the tag already exists, and by verifying `SHA256SUMS` again.

## After the release

**Check that the image can be pulled without a login.** The workflow pushes the image with `GITHUB_TOKEN` from this public repository, and the image's `org.opencontainers.image.source` label names it, so the package takes the repository's visibility: v0.1.0 was public from its first publish. Check it with a pull that uses no login, by giving Docker an empty configuration directory:

```sh
DOCKER_CONFIG="$(mktemp -d)" docker pull ghcr.io/jason-te-sde/distributed-job-queue:X.Y.Z
```

If that fails with a 401, the package is private, for example because its visibility was changed in its settings, and `helm install` with the chart's default image fails with `ImagePullBackOff` for everyone but the owner. On GitHub, open the package `ghcr.io/jason-te-sde/distributed-job-queue`, go to Package settings and set its visibility to Public. A package that is public cannot be made private again.

The chart needs no such step: it is a `.tgz` attached to the GitHub release, not a package in a registry.

Check that the release page lists the four archives, the chart and `SHA256SUMS`; that `sha256sum -c SHA256SUMS` passes in a directory with the downloads; that a downloaded `jobqueue-server --version` and `jobqueuectl version` print the tag; and that `docker pull ghcr.io/jason-te-sde/distributed-job-queue:X.Y.Z` works and `docker run --rm --entrypoint /jobqueuectl ghcr.io/jason-te-sde/distributed-job-queue:X.Y.Z version` prints the tag. Install the chart on a test cluster once.

## If a release is wrong

Do not move or delete a tag, and do not overwrite an image tag that people may have pulled. The repository's `release tags` ruleset (Settings → Rules → Rulesets) refuses to move or delete any `v*` tag, for everyone; it has no bypass list. Fix the problem on `main` and release the next patch version, and say in its changelog entry what was wrong with the previous one. If a release is dangerous, publish a GitHub security advisory for it (see [SECURITY.md](SECURITY.md)) and mark its release page.

## If the workflow fails

Read where it stopped. A failure in steps 1 to 7 has published nothing: fix the cause on `main`, delete the tag and push it again on the new commit (the check requires that commit to be on `main`). The `release tags` ruleset refuses the deletion, so set its enforcement to Disabled for it and back to Active as soon as the tag is gone; pushing the tag again creates it, which the ruleset allows. A failure in step 8 or 9 may have published the image tags, and a tag that has published anything is not touched: rerun the workflow for the same tag if the cause was the environment (a registry outage, a runner), and release the next patch version if it was in the repository. A failed run can leave the draft release behind, and the `publish` job refuses to run while a release or a draft exists for the tag, so delete it first (`gh release delete vX.Y.Z`, or the draft on the Releases page; this removes the release page and not the tag).
