{{- define "servicegraph-flink.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "servicegraph-flink.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "servicegraph-flink.name" .) | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "servicegraph-flink.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "servicegraph-flink.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: extended-otel-semconv
{{- end -}}

{{- define "servicegraph-flink.selectorLabels" -}}
app.kubernetes.io/name: {{ include "servicegraph-flink.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "servicegraph-flink.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "servicegraph-flink.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "servicegraph-flink.restServiceName" -}}
{{- printf "%s-rest" .Values.application.clusterId | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "servicegraph-flink.claimName" -}}
{{- if .Values.storage.existingClaim -}}
{{- .Values.storage.existingClaim -}}
{{- else -}}
{{- printf "%s-state" (include "servicegraph-flink.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "servicegraph-flink.kafkaEnvironment" -}}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ join "," .Values.streamContract.kafka.brokers | quote }}
- name: KAFKA_SECURITY_PROTOCOL
  value: {{ .Values.streamContract.kafka.security.protocol | quote }}
{{- if ne .Values.streamContract.kafka.security.protocol "PLAINTEXT" }}
- name: KAFKA_SASL_MECHANISM
  value: {{ .Values.streamContract.kafka.security.saslMechanism | quote }}
- name: KAFKA_SASL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ required "streamContract.kafka.security.existingSecret is required for Kafka SASL" .Values.streamContract.kafka.security.existingSecret }}
      key: {{ .Values.streamContract.kafka.security.usernameKey }}
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.streamContract.kafka.security.existingSecret }}
      key: {{ .Values.streamContract.kafka.security.passwordKey }}
{{- end }}
- name: INTERACTION_DIFF_INPUT_TOPIC
  value: {{ .Values.streamContract.topics.servicegraphMetrics | quote }}
- name: INTERACTION_DIFF_OUTPUT_TOPIC
  value: {{ .Values.streamContract.topics.interactionEvents | quote }}
- name: INTERACTION_DIFF_GROUP_ID
  value: {{ .Values.job.groupId | quote }}
{{- if .Values.entityEvents.enabled }}
- name: ENTITY_EVENTS_INPUT_TOPIC
  value: {{ required "streamContract.topics.entityEvents is required when entityEvents.enabled=true" .Values.streamContract.topics.entityEvents | quote }}
- name: ENTITY_EVENTS_GROUP_ID
  value: {{ .Values.entityEvents.groupId | quote }}
- name: ENTITY_EVENTS_REPORT_INTERVAL_GRACE_SECONDS
  value: {{ .Values.entityEvents.reportIntervalGraceSeconds | quote }}
{{- end }}
- name: INTERACTION_DIFF_TTL_SECONDS
  value: {{ .Values.job.interactionTtlSeconds | quote }}
- name: INTERACTION_DIFF_ALLOWED_LATENESS_SECONDS
  value: {{ .Values.job.allowedLatenessSeconds | quote }}
- name: FLINK_CHECKPOINT_INTERVAL_MS
  value: {{ .Values.job.checkpointIntervalMs | quote }}
- name: FLINK_PARALLELISM
  value: {{ .Values.application.parallelism | quote }}
- name: FLINK_RESTART_ATTEMPTS
  value: {{ .Values.job.restartAttempts | quote }}
- name: FLINK_RESTART_DELAY_SECONDS
  value: {{ .Values.job.restartDelaySeconds | quote }}
{{- end -}}
