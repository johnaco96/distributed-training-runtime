{{/* Standard name/label helpers. */}}
{{- define "dtruntime.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dtruntime.fullname" -}}
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

{{- define "dtruntime.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dtruntime.labels" -}}
helm.sh/chart: {{ include "dtruntime.chart" . }}
app.kubernetes.io/name: {{ include "dtruntime.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: dtruntime
{{- end -}}

{{- define "dtruntime.coordinator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dtruntime.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: coordinator
{{- end -}}

{{- define "dtruntime.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "dtruntime.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "dtruntime.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "dtruntime.operator.image" -}}
{{- $tag := .Values.operator.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.operator.image.repository $tag -}}
{{- end -}}
