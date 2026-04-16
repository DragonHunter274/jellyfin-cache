{{/*
Expand the name of the chart.
*/}}
{{- define "jellyfin-cache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "jellyfin-cache.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "jellyfin-cache.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{ include "jellyfin-cache.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "jellyfin-cache.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jellyfin-cache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
The image tag to use — falls back to .Chart.AppVersion.
*/}}
{{- define "jellyfin-cache.imageTag" -}}
{{- .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Name of the rclone Secret to mount.
When existingSecret is set, use that; otherwise use the release-scoped secret
created by this chart.
*/}}
{{- define "jellyfin-cache.rcloneSecretName" -}}
{{- if .Values.rclone.existingSecret }}
{{- .Values.rclone.existingSecret }}
{{- else }}
{{- printf "%s-rclone" (include "jellyfin-cache.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Name of the PVC to use.
*/}}
{{- define "jellyfin-cache.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- include "jellyfin-cache.fullname" . }}
{{- end }}
{{- end }}
