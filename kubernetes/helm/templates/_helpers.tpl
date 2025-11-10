{{/*
Expand the name of the chart.
*/}}
{{- define "jdy-data-sync.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "jdy-data-sync.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "jdy-data-sync.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "jdy-data-sync.labels" -}}
helm.sh/chart: {{ include "jdy-data-sync.chart" . }}
{{ include "jdy-data-sync.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels for app
*/}}
{{- define "jdy-data-sync.app.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jdy-data-sync.name" . }}-app
app.kubernetes.io/instance: {{ .Release.Name }}-app
{{- end -}}

{{/*
Selector labels for mysql
*/}}
{{- define "jdy-data-sync.mysql.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jdy-data-sync.name" . }}-mysql
app.kubernetes.io/instance: {{ .Release.Name }}-mysql
{{- end -}}