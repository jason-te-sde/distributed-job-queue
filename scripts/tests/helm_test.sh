#!/usr/bin/env bash
# Tests for scripts/check-helm.sh. The first case runs the check against the
# real chart. Every other case breaks one thing in a throwaway copy of the
# chart and expects the check to fail and name what is missing, which proves
# that each assertion in the check can fail. Each case prints "PASS <case>";
# the first failing case aborts the run.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scripts_dir="$(cd "$here/.." && pwd)"
chart="$(cd "$scripts_dir/.." && pwd)/deploy/helm/jobqueue"
check="$scripts_dir/check-helm.sh"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

out=""
status=0

# copy_chart: print the path of a fresh copy of the chart. The directory keeps
# the chart name, which helm lint requires.
copy_chart() {
  local dir
  dir="$(mktemp -d "$workdir/copy.XXXXXX")"
  cp -R "$chart" "$dir/jobqueue"
  printf '%s' "$dir/jobqueue"
}

run_check() { # run_check <chart dir>
  status=0
  out="$(bash "$check" "$1" 2>&1)" || status=$?
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

# sub <file> <sed expression>: edit a file in place, leaving no backup behind
# (a stray file in templates/ would be rendered as a template).
sub() {
  sed -e "$2" "$1" >"$1.new"
  mv "$1.new" "$1"
}

# broken_case <case> <expected output> <mutation>...: apply the mutation
# (a shell command run inside the chart copy) and expect the check to fail
# with the expected text in its output. A mutation that changes nothing leaves
# a passing chart, so the exit status check catches it.
broken_case() {
  local name="$1" want="$2" copy
  shift 2
  copy="$(copy_chart)"
  (cd "$copy" && "$@")
  run_check "$copy"
  expect_status "$name" 1
  expect_output "$name" "$want"
  pass "$name"
}

# (a) The real chart passes every assertion.
run_check "$chart"
expect_status real-chart-passes 0
expect_output real-chart-passes "helm: ok"
pass real-chart-passes

# (b) A chart without a PodDisruptionBudget fails.
broken_case missing-pdb-fails "PodDisruptionBudget" rm templates/pdb.yaml

# (c) A chart without the headless Service fails.
broken_case missing-headless-service-fails "clusterIP: None" rm templates/service-headless.yaml

# (d) A chart without the migration Job fails.
broken_case missing-migration-hook-fails "helm.sh/hook" rm templates/migrate-job.yaml

# (e) A chart without the ScaledObject fails when the example and KEDA are on.
broken_case missing-scaledobject-fails "kind: ScaledObject" rm templates/scaledobject.yaml

# (f) A ScaledObject without the scalerAddress metadata key fails.
broken_case missing-scaler-address-fails "scalerAddress" \
  sub templates/scaledobject.yaml 's/scalerAddress/scalerAddr/'

# (g) A Deployment without a preStop hook fails.
broken_case missing-prestop-fails "preStop" \
  sub templates/deployment.yaml 's/preStop:/postStart:/'

# (h) A readiness probe on the wrong path fails.
broken_case wrong-readiness-path-fails "path: /readyz" \
  sub templates/deployment.yaml 's#/readyz#/healthz#'

# (i) A default replica count other than 2 fails.
broken_case wrong-default-replicas-fails "replicas: 2" \
  sub values.yaml 's/^replicas: 2$/replicas: 3/'

# (j) A chart that is insecure by default fails.
broken_case insecure-by-default-fails "--insecure" \
  sub values.yaml 's/^insecure: false$/insecure: true/'

# (k) A chart that never renders the insecure flag fails when insecure is set.
broken_case insecure-flag-never-rendered-fails "--insecure" \
  sub templates/deployment.yaml 's/--insecure/--plaintext/'

# (l) A NetworkPolicy that does not admit gRPC clients fails. A policy that
# selects the broker pods and lists only the scaler port would cut them off.
broken_case network-policy-blocks-grpc-fails "port 7070" \
  sub templates/networkpolicy.yaml '/jobqueue.grpcPort/d'

# (m) The scaler flag must follow scaler.enabled.
broken_case scaler-flag-ignores-disabled-fails "--scaler-addr" \
  sub templates/deployment.yaml 's/{{- if .Values.scaler.enabled }}$/{{- if true }}/'
