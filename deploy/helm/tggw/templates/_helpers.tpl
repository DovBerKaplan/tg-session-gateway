{{/* Common labels */}}
{{- define "tggw.labels" -}}
app.kubernetes.io/name: tggw
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end }}

{{/* Plane labels: tggw/component in {botapi, mtproto} */}}
{{- define "tggw.componentLabels" -}}
{{ include "tggw.labels" . }}
app.kubernetes.io/component: {{ .component }}
tggw/component: {{ .component }}
{{- end }}

{{/* Image reference */}}
{{- define "tggw.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/* The single shared Secret name */}}
{{- define "tggw.secretName" -}}
{{- if .Values.existingSecret -}}
{{ .Values.existingSecret }}
{{- else -}}
{{ .Release.Name }}-secrets
{{- end -}}
{{- end }}

{{/* Generate-once admin secret: lookup reuses the value across upgrades */}}
{{- define "tggw.adminSecret" -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "tggw.secretName" .) -}}
{{- if $existing -}}
{{ index $existing.data "GW_ADMIN_SECRET" | required "existing secret lost GW_ADMIN_SECRET" }}
{{- else if .Values.secrets.adminSecret -}}
{{ .Values.secrets.adminSecret | b64enc }}
{{- else -}}
{{ randAlphaNum 32 | b64enc }}
{{- end -}}
{{- end }}

{{- define "tggw.appSecret" -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "tggw.secretName" .) -}}
{{- if $existing -}}
{{ index $existing.data "GW_APP_SECRET" | required "existing secret lost GW_APP_SECRET" }}
{{- else if .Values.secrets.appSecret -}}
{{ .Values.secrets.appSecret | b64enc }}
{{- else -}}
{{ randAlphaNum 32 | b64enc }}
{{- end -}}
{{- end }}

{{/* Storage class annotation: empty string → cluster default */}}
{{- define "tggw.storageClass" -}}
storageClassName: {{ .storageClass | quote }}
{{- end }}
