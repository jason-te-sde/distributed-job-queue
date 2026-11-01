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

# insert_after <file> <text> <line>: add the line after every line of the file
# that contains the text. A mutation that adds a line cannot use sed, whose
# append command and newline escapes differ between GNU and BSD.
insert_after() {
  awk -v pat="$2" -v add="$3" '{ print } index($0, pat) { print add }' "$1" >"$1.new"
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

# (n) A Service, PodDisruptionBudget or NetworkPolicy whose selector does not
# match the broker pods fails. The headless Service, the PDB and the policy
# each take their selector from the same helper, so each is broken on its own.
broker_selector='{{- include "jobqueue.brokerSelectorLabels" . | nindent'
broken_case headless-service-selector-fails "the gRPC Service to select exactly the broker pods" \
  sub templates/service-headless.yaml "s#$broker_selector 4 }}#app.kubernetes.io/component: other#"
broken_case pdb-selector-fails "the PodDisruptionBudget to select exactly the broker pods" \
  sub templates/pdb.yaml "s#$broker_selector 6 }}#app.kubernetes.io/component: other#"
broken_case network-policy-selector-fails "the NetworkPolicy to select exactly the broker pods" \
  sub templates/networkpolicy.yaml "s#$broker_selector 6 }}#app.kubernetes.io/component: other#"

# (o) A preStop command that is not the jobqueuectl sleep subcommand fails.
broken_case wrong-prestop-command-fails "the preStop hook to run the jobqueuectl sleep subcommand" \
  sub templates/deployment.yaml 's/"sleep"/"sleeep"/'

# (p) Swapped probe paths fail: each assertion names the probe it checks.
broken_case swapped-probe-paths-fails "the readiness probe to request /readyz on the http port" \
  sub templates/deployment.yaml 's#/readyz#/TMP#; s#/healthz#/readyz#; s#/TMP#/healthz#'

# (q) A migration Job that runs another command, other arguments or restarts
# the pod in place fails.
broken_case wrong-migrate-command-fails 'command: ["/jobqueuectl"]' \
  sub templates/migrate-job.yaml 's#command: \["/jobqueuectl"\]#command: ["/jobqueue-server"]#'
broken_case wrong-migrate-args-fails 'args: ["migrate", "--timeout=10m"]' \
  sub templates/migrate-job.yaml 's#args: \["migrate"#args: ["migrat"#'
broken_case wrong-migrate-restart-policy-fails "restartPolicy: Never" \
  sub templates/migrate-job.yaml 's/restartPolicy: Never/restartPolicy: OnFailure/'

# (r) A ScaledObject that targets something other than the example worker
# Deployment fails.
broken_case scaledobject-wrong-target-fails "the ScaledObject to target the example worker Deployment" \
  sub templates/scaledobject.yaml 's/^    name: .*/    name: other/'

# (s) A pre-stop sleep that leaves no room for the broker's shutdown grace must
# not render. The guard compares with ge, so a guard that lets the boundary
# value through (15 + 25 == 40) is caught.
broken_case prestop-guard-off-by-one-fails "preStopSleepSeconds" \
  sub templates/deployment.yaml 's/{{- if ge /{{- if gt /'

# (t) Resource names that outgrow the 63 characters of a DNS label fail.
broken_case long-names-not-truncated-fails "at most 63 characters" \
  sub templates/_helpers.tpl 's/trunc 48/trunc 63/'

# (u) The example worker reaches the broker the way the broker is served and
# sends its token. Each case breaks one part of the worker's template.
broken_case example-insecure-flag-never-rendered-fails "the example worker to connect without TLS" \
  sub templates/example-worker.yaml 's/"--insecure"/"--plaintext"/'
broken_case example-tls-ca-path-fails "the example worker to trust the CA of the TLS Secret" \
  sub templates/example-worker.yaml 's#/etc/jobqueue/tls/ca.crt#/etc/jobqueue/tls/tls.crt#'
# The broker serves TLS whenever it has a certificate, so insecure must not
# turn a worker that has the CA into a plaintext one.
broken_case example-insecure-beats-tls-fails "the example worker to use TLS when the broker does, even if insecure is set" \
  sub templates/example-worker.yaml 's/{{- \$trustCA := .Values.tls.secretName }}/{{- $trustCA := and .Values.tls.secretName (not .Values.insecure) }}/'
broken_case example-ca-mount-writable-fails "the CA to be mounted read-only where --tls-ca looks" \
  sub templates/example-worker.yaml 's/readOnly: true/readOnly: false/'
broken_case example-projects-the-private-key-fails "only ca.crt of the TLS Secret to be projected, not its key" \
  sub templates/example-worker.yaml 's/- key: ca.crt/- key: tls.key/'
broken_case example-token-never-passed-fails "the example worker to take its token from the example token Secret" \
  sub templates/example-worker.yaml 's/name: JOBQUEUE_TOKEN/name: JOBQUEUE_TOKEN_UNUSED/'
broken_case example-token-wrong-key-fails "the example worker to take its token from the example token Secret" \
  sub templates/example-worker.yaml 's/key: {{ .Values.example.tokenKey }}/key: token/'
broken_case example-token-wrong-secret-fails "the example worker to take its token from the example token Secret" \
  sub templates/example-worker.yaml 's/name: {{ .Values.example.tokenSecret }}/name: {{ .Values.database.secretName }}/'
broken_case example-database-url-dropped-fails "the example worker to have the database URL only" \
  sub templates/example-worker.yaml 's/{{- include "jobqueue.databaseEnv" . | nindent 12 }}//'

# (v) The guards that refuse a worker that could not authenticate. The token
# is required when the broker has a tokens file, insecure or not, and when
# insecure is off; it is not required with insecure and no tokens file. Each
# half of the rule is broken on its own, in both directions.
token_guard='{{- if and (not .Values.example.tokenSecret) (or .Values.tokens.secretName (not .Values.insecure)) }}'
broken_case example-without-token-guard-fails "example.enabled requires example.tokenSecret when tokens.secretName is set or insecure is false" \
  sub templates/example-worker.yaml "s/$token_guard/{{- if false }}/"
broken_case example-guard-ignores-tokens-file-fails "example.enabled requires example.tokenSecret when tokens.secretName is set or insecure is false" \
  sub templates/example-worker.yaml 's/(or .Values.tokens.secretName (not .Values.insecure))/(not .Values.insecure)/'
broken_case example-guard-ignores-insecure-off-fails "example.enabled requires example.tokenSecret when tokens.secretName is set or insecure is false" \
  sub templates/example-worker.yaml 's/(or .Values.tokens.secretName (not .Values.insecure))/.Values.tokens.secretName/'
broken_case example-guard-always-wants-a-token-fails "[example worker, insecure, no tokens file, no worker token] helm template failed" \
  sub templates/example-worker.yaml "s/$token_guard/{{- if not .Values.example.tokenSecret }}/"
broken_case example-token-key-guard-fails "example.tokenSecret requires example.tokenKey" \
  sub templates/example-worker.yaml 's/{{- if and .Values.example.tokenSecret (not .Values.example.tokenKey) }}/{{- if false }}/'

# (w) Mutations that add something. A check that only looked for the expected
# lines among the rendered ones would pass these, so they pin that the blocks
# are compared whole: a worker that gets both transport flags, and one that
# gets an environment variable nobody asked for.
broken_case example-both-transport-flags-fails "the example worker to use TLS when the broker does, even if insecure is set" \
  sub templates/example-worker.yaml 's/{{- else if .Values.insecure }}/{{- end }}{{- if .Values.insecure }}/'
broken_case example-extra-env-entry-fails "the example worker to have the database URL only" \
  insert_after templates/example-worker.yaml '{{- include "jobqueue.databaseEnv" . | nindent 12 }}' '            - {name: EXTRA, value: "1"}'

# (x) The ScaledObject scales on the queue that the worker drains, which is a
# constant of the worker and not a value of the chart.
broken_case scaledobject-queue-wrong-fails "the KEDA trigger to name the queue the example worker drains" \
  sub templates/scaledobject.yaml 's/queue: "thumbnails"/queue: "other"/'
broken_case scaledobject-queue-from-values-fails "the KEDA trigger to name the queue the example worker drains" \
  sub templates/scaledobject.yaml 's/queue: "thumbnails"/queue: {{ .Values.example.queue | quote }}/'

# (y) The example worker's replicas belong to KEDA when it is enabled, and to
# the chart otherwise. A template that renders a count under KEDA resets the
# scale on every helm upgrade, and one that renders none without KEDA leaves
# the worker at the Kubernetes default of one by accident.
broken_case example-renders-replicas-under-keda-fails "the example worker Deployment to leave spec.replicas to KEDA" \
  sub templates/example-worker.yaml 's/{{- if not .Values.example.keda.enabled }}/{{- if true }}/'
broken_case example-drops-replicas-without-keda-fails "one example worker replica" \
  sub templates/example-worker.yaml 's/{{- if not .Values.example.keda.enabled }}/{{- if false }}/'

# (z) The broker is told how long to drain, from the value that the guard on
# terminationGracePeriodSeconds counts. Each case breaks one link: the flag
# missing, the flag not following the value, the guard not following the value,
# the default drifting from the one the guard and the docs state, and a value
# that the broker would refuse at startup.
broken_case shutdown-grace-flag-never-passed-fails "the broker to be told to drain for 25 seconds" \
  sub templates/deployment.yaml 's/--shutdown-grace=/--shutdown-wait=/'
broken_case shutdown-grace-flag-ignores-value-fails "the broker to be told to drain for shutdownGraceSeconds" \
  sub templates/deployment.yaml 's/printf "--shutdown-grace=%ss" (include "jobqueue.shutdownGraceSeconds" .)/printf "--shutdown-grace=%ss" "25"/'
broken_case shutdown-guard-ignores-value-fails "shutdownGraceSeconds=35, no room left to drain" \
  sub templates/deployment.yaml 's/(int (include "jobqueue.shutdownGraceSeconds" .))/25/'
broken_case shutdown-grace-default-drifts-fails "the broker to be told to drain for 25 seconds" \
  sub values.yaml 's/^shutdownGraceSeconds: 25$/shutdownGraceSeconds: 20/'
broken_case shutdown-grace-zero-allowed-fails "shutdownGraceSeconds must be at least 1" \
  sub templates/deployment.yaml 's/{{- if lt (int .Values.shutdownGraceSeconds) 1 }}/{{- if false }}/'

# (aa) The migration hook runs with a timeout of its own, taken from the values,
# because jobqueuectl's default of 10 seconds cuts a long migration off. Each
# case breaks one link: the flag missing, the flag not following the value, and
# a default that drifts back to something short.
broken_case migrate-timeout-flag-never-passed-fails 'args: ["migrate", "--timeout=10m"]' \
  sub templates/migrate-job.yaml 's/--timeout=%s/--deadline=%s/'
broken_case migrate-timeout-ignores-value-fails "the timeout of migrations.timeout" \
  sub templates/migrate-job.yaml 's/printf "--timeout=%s" .Values.migrations.timeout/printf "--timeout=%s" "10m"/'
broken_case migrate-timeout-default-drifts-fails "with a timeout of 10m" \
  sub values.yaml 's/^  timeout: 10m$/  timeout: 10s/'
