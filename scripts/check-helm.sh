#!/usr/bin/env bash
# Lints and renders the Helm chart, then asserts what the rendered manifests
# must contain.
#
# usage: check-helm.sh [chart-dir]
#
# chart-dir defaults to deploy/helm/jobqueue in this repository.
#
# The chart is linted with helm lint --strict twice: with the default values,
# and with the example worker and KEDA enabled. It is then rendered with
# helm template under several value sets, and each assertion greps the
# rendering. Comment lines are removed first, so a comment can never satisfy an
# assertion. Every failed assertion prints what was expected and the check
# carries on, so one run reports every problem.
#
# Output: "helm: ok", or one "FAIL" line per failed expectation.
# Exit status: 0 clean, 1 a check failed, 2 usage or environment error.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chart="${1:-$script_dir/../deploy/helm/jobqueue}"

die() {
  printf 'helm: %s\n' "$*" >&2
  exit 2
}

command -v helm >/dev/null 2>&1 || die "helm is not installed"
[ -f "$chart/Chart.yaml" ] || die "not a chart directory: $chart"

release=jq
namespace=jobqueue
# The Deployment is named <release>-<chart> when the release name does not
# already contain the chart name.
fullname="$release-jobqueue"

failures=0
fail() {
  printf 'FAIL %s\n' "$1" >&2
  failures=$((failures + 1))
}

# lint <label> [helm lint argument]...
lint() {
  local label="$1" output
  shift
  if ! output="$(helm lint --strict "$chart" "$@" 2>&1)"; then
    fail "helm lint --strict failed ($label):"
    printf '%s\n' "$output" >&2
  fi
}

rendered=""
context=""

# render <label> [helm template argument]...
# Sets $rendered to the manifests without comment lines and $context to the
# label. Returns 1, after recording a failure, when helm template fails.
render() {
  local raw
  context="$1"
  shift
  rendered=""
  if ! raw="$(helm template "$release" "$chart" --namespace "$namespace" "$@" 2>&1)"; then
    fail "[$context] helm template failed:"
    printf '%s\n' "$raw" >&2
    return 1
  fi
  rendered="$(grep -vE '^[[:space:]]*#' <<<"$raw" || true)"
}

# expect_text <what> <text>: the rendering contains the text.
expect_text() {
  grep -Fq -- "$2" <<<"$rendered" ||
    fail "[$context] expected $1: no line contains '$2'"
  return 0
}

# expect_pattern <what> <extended regex>: a line of the rendering matches.
expect_pattern() {
  grep -Eq -- "$2" <<<"$rendered" ||
    fail "[$context] expected $1: no line matches '$2'"
  return 0
}

# expect_no_text <what> <text>: the rendering does not contain the text.
expect_no_text() {
  if grep -Fq -- "$2" <<<"$rendered"; then
    fail "[$context] expected $1, but a line contains '$2'"
  fi
  return 0
}

# expect_render_error <label> <message> [helm template argument]...
# helm template must fail and its output must contain the message.
expect_render_error() {
  local label="$1" message="$2" output
  shift 2
  if output="$(helm template "$release" "$chart" --namespace "$namespace" "$@" 2>&1)"; then
    fail "[$label] expected helm template to fail with '$message', but it succeeded"
  else
    case "$output" in
      *"$message"*) ;;
      *) fail "[$label] expected the error to contain '$message', got: $output" ;;
    esac
  fi
}

lint "default values"
lint "example worker and KEDA enabled" --set example.enabled=true,example.keda.enabled=true

if render "defaults"; then
  expect_text "a PodDisruptionBudget" "kind: PodDisruptionBudget"
  expect_text "a headless Service for gRPC" "clusterIP: None"
  expect_text "a readiness probe on /readyz" "path: /readyz"
  expect_text "a liveness probe on /healthz" "path: /healthz"
  expect_text "a preStop hook" "preStop"
  expect_text "the migration hook annotation" '"helm.sh/hook": pre-install,pre-upgrade'
  expect_pattern "two broker replicas" '^ *replicas: 2$'
  expect_no_text "no insecure flag by default" "--insecure"
  expect_text "the scaler flag, as the scaler is on by default" "--scaler-addr=:7072"
  expect_text "a NetworkPolicy, as the scaler is on by default" "kind: NetworkPolicy"
  expect_no_text "no tokens flag without a tokens Secret" "--tokens-file"
  expect_no_text "no TLS flags without a TLS Secret" "--tls-cert"
fi

# The NetworkPolicy selects the broker pods, so it must list every port that
# clients use. A policy that names only the scaler port cuts them all off.
if render "defaults, NetworkPolicy" --show-only templates/networkpolicy.yaml; then
  expect_text "the NetworkPolicy to admit gRPC clients on port 7070" "port: 7070"
  expect_text "the NetworkPolicy to admit HTTP clients on port 7071" "port: 7071"
  expect_text "the NetworkPolicy to admit the scaler on port 7072" "port: 7072"
  expect_text "the scaler port limited to the KEDA namespace" "kubernetes.io/metadata.name: keda"
fi

if render "insecure=true" --set insecure=true; then
  expect_text "the insecure flag" "--insecure"
fi

if render "example worker and KEDA" --set example.enabled=true,example.keda.enabled=true; then
  expect_text "a ScaledObject" "kind: ScaledObject"
  expect_text "the scalerAddress trigger metadata" "scalerAddress"
  expect_text "the scaler Service address in the namespace" "$fullname-scaler.$namespace.svc.cluster.local:7072"
  expect_text "the example worker command" '"/thumbnail"'
  expect_text "the example worker to dial the headless Service" "dns:///$fullname-grpc:7070"
  expect_pattern "the example worker to start at zero replicas" '^ *replicas: 0$'
fi

if render "example worker without KEDA" --set example.enabled=true; then
  expect_pattern "one example worker replica" '^ *replicas: 1$'
  expect_no_text "no ScaledObject without KEDA" "kind: ScaledObject"
fi

if render "scaler disabled" --set scaler.enabled=false; then
  expect_no_text "no scaler flag" "--scaler-addr"
  expect_no_text "no NetworkPolicy" "kind: NetworkPolicy"
  expect_no_text "no scaler Service" "$fullname-scaler"
fi

if render "tokens and TLS Secrets" --set tokens.secretName=tokens-secret,tls.secretName=tls-secret; then
  expect_text "the tokens file flag" "--tokens-file=/etc/jobqueue/tokens/tokens"
  expect_text "the TLS certificate flag" "--tls-cert=/etc/jobqueue/tls/tls.crt"
  expect_text "the TLS key flag" "--tls-key=/etc/jobqueue/tls/tls.key"
  expect_text "the tokens Secret mounted" "secretName: tokens-secret"
  expect_text "the TLS Secret mounted" "secretName: tls-secret"
fi

if render "tenant weights" --set-string 'tenantWeights=a=4\,b=1'; then
  expect_text "the tenant weights flag" "--tenant-weights=a=4,b=1"
fi

expect_render_error "KEDA without the example worker" "example.keda.enabled requires example.enabled" \
  --set example.keda.enabled=true
expect_render_error "KEDA without the scaler" "example.keda.enabled requires scaler.enabled" \
  --set example.enabled=true,example.keda.enabled=true,scaler.enabled=false

if [ "$failures" -gt 0 ]; then
  printf 'helm: %d check(s) failed\n' "$failures" >&2
  exit 1
fi
printf 'helm: ok\n'
