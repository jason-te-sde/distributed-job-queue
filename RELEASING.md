# Releasing

A release is a tag on `main`. The release workflow builds and publishes everything from that tag, and **nothing is built or published from a laptop**. The workflow refuses to release when the repository does not agree with the tag, so the steps below are the ones it checks.

Versions follow [Semantic Versioning](https://semver.org). The tag is `vX.Y.Z`; the version without the `v` is the version in the changelog and in the chart.

## Before the tag

1. **Everything is merged and green.** All work for the release is on `main`, and the CI run on the merge commit is green: lint, the test matrix, the pg jobs on PostgreSQL 15 and 18, the planted flaws, the chaos test, the Kubernetes end-to-end test, the image, the clean clone and the hygiene check.
2. **Write the changelog.** In [CHANGELOG.md](CHANGELOG.md), put the release at the top as `## [X.Y.Z] - YYYY-MM-DD`, in the Keep a Changelog format, with the date of the release. Note any migration that an older broker cannot run against. Add the link reference at the bottom. The release page takes its notes from this section.
3. **Set the chart version.** In `deploy/helm/jobqueue/Chart.yaml`, `version` and `appVersion` are both `X.Y.Z`.
4. **Run `make preflight`** on the commit you mean to tag.
5. **Check that the measured numbers are current.** Every figure in the README comes from [docs/benchmarks.md](docs/benchmarks.md); if a change since the last run could move them, run the benchmarks again.

Merge these changes to `main` in a pull request like any other.

## Tag and push

```sh
git switch main && git pull --ff-only
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

Pushing a tag that matches `v*` starts the release workflow.

## What the workflow does

1. **Checks the repository against the tag** with `scripts/check-release.sh vX.Y.Z`, and fails with the mismatch if any of these is untrue: the tag matches `^v\d+\.\d+\.\d+$`; the first `## [x.y.z]` heading of `CHANGELOG.md` is the tag without the `v`; `version` and `appVersion` in `Chart.yaml` are that version; and the tagged commit is on `main`.
2. **Runs `make preflight`** without the PostgreSQL tests (the pg job of CI has run them).
3. **Builds `jobqueue-server` and `jobqueuectl`** for linux and darwin on amd64 and arm64, with `-X main.version=<tag>`, packs each platform into a tarball and writes `SHA256SUMS`.
4. **Builds and pushes the multi-architecture image** to `ghcr.io/jason-te-sde/distributed-job-queue`, tagged `<version>` and `latest`.
5. **Packages the Helm chart** with `helm package`.
6. **Creates the GitHub release,** with the changelog section as its notes, and attaches the tarballs, `SHA256SUMS` and the chart.

## After the release

Check that the release page lists the tarballs, the checksums and the chart; that `docker pull ghcr.io/jason-te-sde/distributed-job-queue:X.Y.Z` works and `docker run --rm --entrypoint /jobqueuectl <image> help` runs; that a downloaded `jobqueue-server --version` prints the tag; and that `sha256sum -c SHA256SUMS` passes. Install the chart on a test cluster once.

## If a release is wrong

Do not move or delete a tag, and do not overwrite an image tag that people may have pulled. Fix the problem on `main` and release the next patch version, and say in its changelog entry what was wrong with the previous one. If a release is dangerous, publish a GitHub security advisory for it (see [SECURITY.md](SECURITY.md)) and mark its release page.

## If the workflow fails

The workflow can be rerun for the same tag when the failure was the environment (a registry outage, a runner). If the failure was in the repository, a tag that has not published anything may be deleted and made again after the fix; a tag that has published anything is not touched, and the fix is the next patch version.
