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
# rendering or compares one block of it. Comment lines are removed first, so a
# comment can never satisfy an assertion. Every failed assertion prints what was expected and the check
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

default_release=jq
release="$default_release"
namespace=jobqueue
# The Deployment is named <release>-<chart> when the release name does not
# already contain the chart name.
fullname="$default_release-jobqueue"

# The labels that select the broker pods, and those that select the example
# worker pods. Selectors never carry the version or chart labels: a Deployment
# selector cannot change once it exists.
broker_selector="app.kubernetes.io/name: jobqueue
app.kubernetes.io/instance: $release
app.kubernetes.io/component: broker"
worker_selector="app.kubernetes.io/name: jobqueue
app.kubernetes.io/instance: $release
app.kubernetes.io/component: example-worker"

# The example worker needs a token unless the chart is insecure, so every
# rendering that enables the worker passes one.
example_token="example.tokenSecret=example-token,example.tokenKey=worker-token"

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

# under <key>...: print the lines nested under the first line whose text,
# ignoring leading spaces, is the first key; with more keys, repeat inside the
# lines found. Reads $rendered.
under() {
  local text="$rendered" key
  for key in "$@"; do
    text="$(awk -v key="$key" '
      function indent(s) { match(s, /^ */); return RLENGTH }
      found && NF && indent($0) <= base { exit }
      found { print; next }
      { t = $0; sub(/^ +/, "", t); if (t == key) { found = 1; base = indent($0) } }
    ' <<<"$text")"
  done
  printf '%s\n' "$text"
}

# expect_block <what> <expected lines> <key>...: the lines under the keys, with
# their indentation removed, are exactly the expected lines.
expect_block() {
  local what="$1" want="$2" got
  shift 2
  got="$(under "$@" | sed 's/^ *//')"
  if [ "$got" != "$want" ]; then
    fail "[$context] expected $what: the lines under '$*' should be
$want
but are
$got"
  fi
  return 0
}

# expect_block_has <what> <expected lines> <key>...: every expected line is one
# of the lines under the keys.
expect_block_has() {
  local what="$1" want="$2" got line
  shift 2
  got="$(under "$@" | sed 's/^ *//')"
  while IFS= read -r line; do
    grep -Fxq -- "$line" <<<"$got" ||
      fail "[$context] expected $what: no line under '$*' is '$line'"
  done <<<"$want"
  return 0
}

# names: print the metadata.name of every object in $rendered.
names() {
  awk '
    /^---/ { inmeta = 0 }
    /^metadata:/ { inmeta = 1; next }
    inmeta && /^  name: / { print $2; inmeta = 0 }
  ' <<<"$rendered"
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
lint "example worker and KEDA enabled" --set "example.enabled=true,example.keda.enabled=true,$example_token"

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

if render "example worker and KEDA" --set "example.enabled=true,example.keda.enabled=true,$example_token"; then
  expect_text "a ScaledObject" "kind: ScaledObject"
  expect_text "the scalerAddress trigger metadata" "scalerAddress"
  expect_text "the scaler Service address in the namespace" "$fullname-scaler.$namespace.svc.cluster.local:7072"
  expect_text "the example worker command" '"/thumbnail"'
  expect_text "the example worker to dial the headless Service" "dns:///$fullname-grpc:7070"
fi

if render "example worker without KEDA" --set "example.enabled=true,$example_token"; then
  expect_no_text "no ScaledObject without KEDA" "kind: ScaledObject"
fi

# Without KEDA nothing else scales the worker, so the chart sets its replicas.
if render "example worker Deployment without KEDA" --set "example.enabled=true,$example_token" --show-only templates/example-worker.yaml; then
  expect_block_has "one example worker replica" "replicas: 1" spec:
fi

# The example worker reaches the broker the way the broker is served, and sends
# the token the chart was given. Each rendering is limited to the worker's own
# template: the broker's Deployment has flags that look the same. The blocks are
# compared whole, so a flag that is missing, doubled or in the wrong place fails.
worker_template=templates/example-worker.yaml
worker_args_common='- "work"
- "--addr"
- "dns:///'"$fullname"'-grpc:7070"
- "--concurrency"
- "4"'
database_env='- name: JOBQUEUE_DATABASE_URL
valueFrom:
secretKeyRef:
name: jobqueue-database
key: url'
token_env='- name: JOBQUEUE_TOKEN
valueFrom:
secretKeyRef:
name: example-token
key: worker-token'
ca_mount='- name: tls-ca
mountPath: /etc/jobqueue/tls
readOnly: true'
ca_volume='- name: tls-ca
secret:
secretName: tls-secret
items:
- key: ca.crt
path: ca.crt'

# With insecure and no tokens file the broker takes every caller, so the worker
# needs no token and the chart renders it without one.
if render "example worker, insecure, no tokens file, no worker token" --set example.enabled=true,insecure=true --show-only "$worker_template"; then
  expect_block "the example worker to connect without TLS" "$worker_args_common
- \"--insecure\"" args:
  expect_block "the example worker to have the database URL only" "$database_env" env:
  expect_no_text "no volumes without a TLS Secret" "volumes:"
  expect_no_text "no volume mounts without a TLS Secret" "volumeMounts:"
fi

# A tokens file makes the broker require a token under insecure as well.
if render "example worker, insecure with a tokens file and a token" --set "example.enabled=true,insecure=true,tokens.secretName=tokens-secret,$example_token" --show-only "$worker_template"; then
  expect_block "the example worker to connect without TLS" "$worker_args_common
- \"--insecure\"" args:
  expect_block "the example worker to take its token from the example token Secret" "$database_env
$token_env" env:
fi

if render "example worker, TLS" --set "example.enabled=true,tls.secretName=tls-secret,tokens.secretName=tokens-secret,$example_token" --show-only "$worker_template"; then
  expect_block "the example worker to trust the CA of the TLS Secret" "$worker_args_common
- \"--tls-ca\"
- \"/etc/jobqueue/tls/ca.crt\"" args:
  expect_block "the example worker to take its token from the example token Secret" "$database_env
$token_env" env:
  expect_block "the CA to be mounted read-only where --tls-ca looks" "$ca_mount" volumeMounts:
  expect_block "only ca.crt of the TLS Secret to be projected, not its key" "$ca_volume" volumes:
fi

# The broker serves TLS whenever it has a certificate, and --insecure only lifts
# its refusal to serve without one. A worker that sent plaintext to it would
# never connect, so the TLS Secret wins over insecure.
if render "example worker, insecure and TLS" --set example.enabled=true,insecure=true,tls.secretName=tls-secret --show-only "$worker_template"; then
  expect_block "the example worker to use TLS when the broker does, even if insecure is set" "$worker_args_common
- \"--tls-ca\"
- \"/etc/jobqueue/tls/ca.crt\"" args:
fi

# The broker requires a token from every caller whenever it has a tokens file,
# insecure or not, and without insecure it must have one. Either way a worker
# without a token would only get Unauthenticated.
token_guard="example.enabled requires example.tokenSecret when tokens.secretName is set or insecure is false"
expect_render_error "the example worker without a token, not insecure" "$token_guard" \
  --set example.enabled=true
expect_render_error "the example worker without a token, insecure but with a tokens file" "$token_guard" \
  --set example.enabled=true,insecure=true,tokens.secretName=tokens-secret
expect_render_error "a token Secret without a key" \
  "example.tokenSecret requires example.tokenKey" --set example.enabled=true,example.tokenSecret=example-token

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
  --set "example.enabled=true,example.keda.enabled=true,scaler.enabled=false,$example_token"

# Everything that selects the broker pods must select exactly them: no other
# pods, and none of the broker pods missed.
for entry in "service:the HTTP" "service-headless:the gRPC" "service-scaler:the scaler"; do
  template="${entry%%:*}"
  if render "selector of $template" --show-only "templates/$template.yaml"; then
    expect_block "${entry#*:} Service to select exactly the broker pods" "$broker_selector" selector:
  fi
done
if render "selector of the PDB" --show-only templates/pdb.yaml; then
  expect_block "the PodDisruptionBudget to select exactly the broker pods" "matchLabels:
$broker_selector" selector:
fi
if render "selector of the NetworkPolicy" --show-only templates/networkpolicy.yaml; then
  expect_block "the NetworkPolicy to select exactly the broker pods" "matchLabels:
$broker_selector" podSelector:
fi

if render "broker Deployment" --show-only templates/deployment.yaml; then
  expect_block "the broker Deployment to select exactly the broker pods" "matchLabels:
$broker_selector" selector:
  expect_block_has "the broker pod template to carry the selector labels" "$broker_selector" template: labels:
  expect_block "the readiness probe to request /readyz on the http port" "path: /readyz
port: http" readinessProbe: httpGet:
  expect_block "the liveness probe to request /healthz on the http port" "path: /healthz
port: http" livenessProbe: httpGet:
  expect_block "the preStop hook to run the jobqueuectl sleep subcommand" 'command: ["/jobqueuectl", "sleep", "5s"]' lifecycle: preStop: exec:
  expect_block_has "the broker to be told to drain for 25 seconds" '- "--shutdown-grace=25s"' args:
fi

if render "preStopSleepSeconds=7" --set preStopSleepSeconds=7 --show-only templates/deployment.yaml; then
  expect_block "the preStop hook to sleep for preStopSleepSeconds" 'command: ["/jobqueuectl", "sleep", "7s"]' lifecycle: preStop: exec:
fi

# The pod is killed when terminationGracePeriodSeconds runs out, so the preStop
# sleep plus the broker's drain (25 seconds by default) must be strictly
# smaller. 14 + 25 is 39, which fits in 40; 15 + 25 is 40, which does not.
render "preStopSleepSeconds=14, the largest value that fits" --set preStopSleepSeconds=14 >/dev/null || true
expect_render_error "preStopSleepSeconds=15, no room left to drain" \
  "must be less than terminationGracePeriodSeconds" --set preStopSleepSeconds=15
expect_render_error "terminationGracePeriodSeconds=30" \
  "must be less than terminationGracePeriodSeconds" --set terminationGracePeriodSeconds=30

# The drain is the broker's --shutdown-grace, and the chart passes the same
# value that the guard above counts, so a change to one cannot leave the other
# behind. With the preStop sleep at 5 and the termination grace at 40, a drain
# of 34 seconds fits and is passed on as 34s, and 35 does not fit.
if render "shutdownGraceSeconds=10" --set shutdownGraceSeconds=10 --show-only templates/deployment.yaml; then
  expect_block_has "the broker to be told to drain for shutdownGraceSeconds" '- "--shutdown-grace=10s"' args:
fi
if render "shutdownGraceSeconds=34, the largest value that fits" --set shutdownGraceSeconds=34 --show-only templates/deployment.yaml; then
  expect_block_has "the broker to be told to drain for shutdownGraceSeconds" '- "--shutdown-grace=34s"' args:
fi
expect_render_error "shutdownGraceSeconds=35, no room left to drain" \
  "must be less than terminationGracePeriodSeconds" --set shutdownGraceSeconds=35
expect_render_error "shutdownGraceSeconds=0, which the broker refuses" \
  "shutdownGraceSeconds must be at least 1" --set shutdownGraceSeconds=0

if render "migration Job" --show-only templates/migrate-job.yaml; then
  expect_text "the migration Job to run jobqueuectl" 'command: ["/jobqueuectl"]'
  expect_text "the migration Job to run the migrate subcommand with a timeout of 4m" 'args: ["migrate", "--timeout=4m"]'
  expect_text "the migration Job to fail rather than restart in place" "restartPolicy: Never"
fi

# jobqueuectl gives up after 10 seconds unless it is told otherwise, which a
# migration that builds an index on a large table does not meet. The hook passes
# its own, longer timeout, and the timeout is a value of the chart.
if render "migrations.timeout=45m" --set migrations.timeout=45m --show-only templates/migrate-job.yaml; then
  expect_text "the migration Job to run the migrate subcommand with the timeout of migrations.timeout" 'args: ["migrate", "--timeout=45m"]'
fi

# The ScaledObject must scale the Deployment the chart renders for the example
# worker, and that Deployment must select its own pods only.
if render "example worker Deployment" --set "example.enabled=true,example.keda.enabled=true,$example_token" --show-only templates/example-worker.yaml; then
  worker_name="$(under metadata: | sed -n 's/^ *name: //p')"
  expect_block "the example worker Deployment to select exactly the example worker pods" "matchLabels:
$worker_selector" selector:
  # KEDA's autoscaler owns spec.replicas of this Deployment. A chart that also
  # renders a value resets the scale on every helm upgrade, so it renders none.
  expect_no_text "the example worker Deployment to leave spec.replicas to KEDA" "replicas:"
  if [ "$worker_name" != "$fullname-example-worker" ]; then
    fail "[$context] expected the example worker Deployment to be named $fullname-example-worker, got '$worker_name'"
  fi
  if render "ScaledObject target" --set "example.enabled=true,example.keda.enabled=true,$example_token" --show-only templates/scaledobject.yaml; then
    expect_block "the ScaledObject to target the example worker Deployment" "name: $worker_name" scaleTargetRef:
    expect_block_has "the KEDA trigger to name the queue the example worker drains" 'queue: "thumbnails"' triggers: metadata:
  fi
fi

# Services, Jobs and the rest are DNS labels of at most 63 characters, and the
# templates add suffixes to the release name. Helm allows 53 characters in a
# release name, so check the longest one.
release="$(printf '%053d' 0 | tr 0 r)"
if render "a 53-character release name" --set "example.enabled=true,example.keda.enabled=true,$example_token"; then
  object_count=0
  while IFS= read -r name; do
    object_count=$((object_count + 1))
    if [ "${#name}" -gt 63 ]; then
      fail "[$context] expected every metadata.name to be at most 63 characters: '$name' has ${#name}"
    fi
  done < <(names)
  if [ "$object_count" -lt 9 ]; then
    fail "[$context] expected at least 9 named objects in the rendering, found $object_count"
  fi
fi
release="$default_release"

if [ "$failures" -gt 0 ]; then
  printf 'helm: %d check(s) failed\n' "$failures" >&2
  exit 1
fi
printf 'helm: ok\n'
