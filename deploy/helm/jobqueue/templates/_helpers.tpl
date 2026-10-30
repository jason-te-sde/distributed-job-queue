{{/*
Chart name, truncated to the 63 characters a DNS label allows.
*/}}
{{- define "jobqueue.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified name: the release name, with the chart name appended unless the
release name already contains it.
*/}}
{{- define "jobqueue.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Selector labels. The argument is a dict with the root context under "ctx" and
the component name under "component". Selectors are immutable on a Deployment,
so this set never changes between chart versions.
*/}}
{{- define "jobqueue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jobqueue.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Common labels. The argument is the same dict as for the selector labels.
*/}}
{{- define "jobqueue.labels" -}}
{{ include "jobqueue.selectorLabels" . }}
app.kubernetes.io/version: {{ .ctx.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .ctx.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .ctx.Chart.Name .ctx.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/* Container image: the repository with the tag, or the appVersion when the tag is empty. */}}
{{- define "jobqueue.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{/* Container ports. They match the flags in deployment.yaml and the image's EXPOSE. */}}
{{- define "jobqueue.grpcPort" -}}7070{{- end -}}
{{- define "jobqueue.httpPort" -}}7071{{- end -}}

{{/* The database URL, read from the Secret. */}}
{{- define "jobqueue.databaseEnv" -}}
- name: JOBQUEUE_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.database.secretName }}
      key: {{ .Values.database.secretKey }}
{{- end -}}

{{/*
Pod security context. The numeric uid is the image's nonroot user; runAsNonRoot
alone fails when an image declares its user by name.
*/}}
{{- define "jobqueue.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/* Container security context. The images have no shell and write nothing to disk. */}}
{{- define "jobqueue.containerSecurityContext" -}}
readOnlyRootFilesystem: true
allowPrivilegeEscalation: false
capabilities:
  drop:
    - ALL
{{- end -}}
