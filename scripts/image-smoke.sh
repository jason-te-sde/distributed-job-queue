#!/usr/bin/env bash
# Smoke test of the container image.
#
# usage: image-smoke.sh <image>
#
# Checks what the image declares (the user, the entrypoint and the exposed
# ports), then starts PostgreSQL and the image's broker on a private Docker
# network and drives them with the image's own jobqueuectl: apply the schema,
# enqueue a job through the broker and read it back. Nothing on the host is
# used but Docker; no port is published, so concurrent runs cannot collide.
#
# Every container and the network carry a name that is unique to the run, and
# they are removed when the script ends, however it ends. When a run fails, the
# logs of the broker, of the migration and of PostgreSQL are printed first.
#
# Output: "smoke: ok", or "smoke: FAIL: <what went wrong>".
# Exit status: 0 ok, 1 a check failed, 2 usage or environment error.
set -euo pipefail

# The alpine variant is used because its busybox wget probes /readyz from inside
# the network: the image under test has no shell and no curl.
postgres_image=postgres:17-alpine
ready_timeout=30

# The port numbers and the user are what the Dockerfile promises and what the
# Helm chart relies on (runAsUser 65532, the probes, the scaler port).
want_ports="7070/tcp 7071/tcp 7072/tcp"
want_uid=65532

die() {
  printf 'smoke: %s\n' "$*" >&2
  exit 2
}

fail() {
  printf 'smoke: FAIL: %s\n' "$*" >&2
  exit 1
}

[ "$#" -eq 1 ] && [ -n "$1" ] || die "usage: image-smoke.sh <image>"
image="$1"
command -v docker >/dev/null 2>&1 || die "docker is not installed"
docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable"
docker image inspect "$image" >/dev/null 2>&1 || die "no such image: $image (build it first: docker build -t $image .)"

# The database is a throwaway on a private network, so its password is no secret.
run_id="jq-smoke-$$-$RANDOM"
network="$run_id"
postgres="$run_id-postgres"
broker="$run_id-broker"
# The migration container is kept (no --rm) until the teardown, so that its log
# can be printed when a later step fails.
migrate="$run_id-migrate"
database_url="postgres://postgres:smoke@db:5432/postgres?sslmode=disable"

# show_log <container> <label>: print the log of a container, if it exists.
show_log() {
  docker inspect "$1" >/dev/null 2>&1 || return 0
  printf 'smoke: the %s log:\n' "$2" >&2
  docker logs "$1" >&2 2>&1 || true
}

# cleanup runs on every exit. It keeps the exit status of the script, prints the
# logs when the run failed, and never fails itself: a container that was never
# started is not an error.
cleanup() {
  local status=$?
  trap - EXIT INT TERM HUP
  if [ "$status" -ne 0 ]; then
    show_log "$broker" broker
    show_log "$migrate" migrate
    show_log "$postgres" PostgreSQL
  fi
  docker rm -f "$broker" "$migrate" "$postgres" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
# A signal ends the script through the EXIT trap, so the teardown still runs.
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# expect <what> <extended regex> <text>: fail unless a line of the text matches.
expect() {
  printf '%s\n' "$3" | grep -Eq -- "$2" || fail "$1: expected a line matching '$2', got: $3"
}

# ctl <jobqueuectl argument>...: the image's jobqueuectl, on the private network.
ctl() {
  docker run --rm --network "$network" --entrypoint /jobqueuectl "$image" "$@"
}

# What the image declares.
user="$(docker image inspect --format '{{.Config.User}}' "$image")"
[ "${user%%:*}" = "$want_uid" ] || fail "the image runs as user '$user', want uid $want_uid"
entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "$image")"
[ "$entrypoint" = '["/jobqueue-server"]' ] || fail "the entrypoint is $entrypoint, want [\"/jobqueue-server\"]"
# Go templates visit the keys of a map in sorted order.
ports="$(docker image inspect --format '{{range $port, $_ := .Config.ExposedPorts}}{{$port}} {{end}}' "$image")"
[ "${ports% }" = "$want_ports" ] || fail "the image exposes '${ports% }', want '$want_ports'"

# Pull quietly and only when needed: the progress output of a pull would bury
# the result.
docker image inspect "$postgres_image" >/dev/null 2>&1 || docker pull -q "$postgres_image" >/dev/null

docker network create "$network" >/dev/null

docker run -d --name "$postgres" --network "$network" --network-alias db \
  -e POSTGRES_PASSWORD=smoke "$postgres_image" >/dev/null
# Over TCP: while the entrypoint script initializes the cluster, the temporary
# server listens on the Unix socket only, and the final one follows.
deadline=$((SECONDS + ready_timeout))
until docker exec "$postgres" pg_isready -q -h 127.0.0.1 -U postgres; do
  [ "$SECONDS" -lt "$deadline" ] || fail "PostgreSQL was not ready within ${ready_timeout}s"
  sleep 1
done

# The database is fresh, so every migration is applied and the schema version
# is the number applied. Neither is a fixed number: a later migration file must
# not break this check.
out="$(docker run --name "$migrate" --network "$network" --entrypoint /jobqueuectl "$image" \
  migrate --database-url "$database_url" 2>&1)" || fail "migrate exited with an error: $out"
line="$(printf '%s\n' "$out" | grep -E '^applied=[0-9]+ version=[0-9]+$' || true)"
[ -n "$line" ] || fail "migrate: expected a line 'applied=N version=N', got: $out"
applied="${line#applied=}"
applied="${applied%% *}"
[ "$applied" -ge 1 ] && [ "${line##*version=}" = "$applied" ] ||
  fail "migrate on a fresh database: want applied >= 1 and version equal to applied, got: $line"

docker run -d --name "$broker" --network "$network" --network-alias jq "$image" \
  --insecure --grpc-addr :7070 --http-addr :7071 --database-url "$database_url" >/dev/null
deadline=$((SECONDS + ready_timeout))
until docker exec "$postgres" wget -q -O /dev/null http://jq:7071/readyz 2>/dev/null; do
  [ "$(docker inspect --format '{{.State.Running}}' "$broker")" = true ] || fail "the broker exited before /readyz answered 200"
  [ "$SECONDS" -lt "$deadline" ] || fail "/readyz did not answer 200 within ${ready_timeout}s"
  sleep 1
done

out="$(ctl --insecure --addr dns:///jq:7070 enqueue --queue smoke --payload-string hi 2>&1)" || fail "enqueue exited with an error: $out"
expect "enqueue" '^id=1( |$)' "$out"

out="$(ctl --insecure --addr dns:///jq:7070 get 1 2>&1)" || fail "get exited with an error: $out"
expect "get 1" '^state:[[:space:]]+ready$' "$out"

printf 'smoke: ok\n'
