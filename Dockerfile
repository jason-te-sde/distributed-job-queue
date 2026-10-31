# syntax=docker/dockerfile:1

# The image of the broker, its admin CLI and the example worker.
#
#   docker build -t jq:dev .
#   docker buildx build --platform linux/amd64,linux/arm64 .
#
# The entrypoint is the broker, so a container takes the broker's flags as its
# arguments. The CLI is there for the Helm chart: the migration Job runs
# /jobqueuectl migrate, and the preStop hook runs /jobqueuectl sleep, because
# the final image has no shell. The example worker is /thumbnail, which the chart
# runs when example.enabled is set.

# The build stage runs on the machine that runs the build and cross-compiles for
# each target platform, so a multi-arch build does not run a compiler under
# emulation. With CGO_ENABLED=0 the binaries are static, which is what lets the
# final stage be distroless.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build

ARG TARGETOS
ARG TARGETARCH
# What `jobqueue-server --version` prints.
ARG VERSION=dev

WORKDIR /src

# The module files alone, then a download. The modules go into a cache mount, and
# a cache mount is not part of any layer: a cache hit on this step restores
# nothing. The build below finds the modules because it mounts the same cache on
# the same builder, and it downloads them again when that cache is gone, for
# example on a fresh CI builder. The step is here to fail early, before the
# source is copied, on an unreachable module or a go.sum mismatch, and to warm
# the cache. The two cache mounts are the Go module cache and the Go build
# cache; neither ends up in the image.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# To ship one more binary, add its package here and one COPY line below.
# -X main.version has no effect on a command that declares no version variable.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o /out/ \
        ./cmd/jobqueue-server \
        ./cmd/jobqueuectl \
        ./examples/thumbnail

# Distroless static on Debian 13 (trixie), checked on 2026-10-30: it is the
# newest Debian release that distroless publishes (static-debian14 does not
# exist, and the distroless README lists debian13 as the current one). The
# image has CA certificates, tzdata and a nonroot user, and nothing else: no
# shell and no package manager.
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /out/jobqueue-server /jobqueue-server
COPY --from=build /out/jobqueuectl /jobqueuectl
COPY --from=build /out/thumbnail /thumbnail

# gRPC, health and metrics, and the autoscaler.
EXPOSE 7070 7071 7072

# The Helm chart pins runAsUser and runAsGroup to 65532 as well. The base image
# already runs as this user; it is repeated so that the contract is visible
# here.
USER 65532:65532

ENTRYPOINT ["/jobqueue-server"]
