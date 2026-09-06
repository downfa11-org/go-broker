{{- define "cursus.validate" -}}
{{- if not (has .Values.mode (list "standalone" "cluster")) }}
{{- fail "mode must be standalone or cluster" }}
{{- end }}
{{- if and (eq .Values.mode "standalone") (ne (int .Values.replicaCount) 1) }}
{{- fail "standalone mode requires exactly one replica" }}
{{- end }}
{{- if .Release.IsUpgrade }}
{{- $existing := lookup "apps/v1" "StatefulSet" .Release.Namespace (include "cursus.fullname" .) }}
{{- if $existing }}
{{- if or (ne .Values.mode "cluster") (ne (int $existing.spec.replicas) (int .Values.replicaCount)) }}{{ fail "existing cluster mode and voter count cannot be changed by Helm upgrade" }}{{ end }}
{{- end }}
{{- end }}
{{- if eq .Values.mode "cluster" }}
{{- if ne .Values.service.type "ClusterIP" }}{{ fail "cluster chart advertises in-cluster DNS and requires service.type=ClusterIP" }}{{ end }}
{{- if and .Release.IsUpgrade (lookup "apps/v1" "Deployment" .Release.Namespace (include "cursus.fullname" .)) }}{{ fail "do not convert a standalone release into a cluster; use a separate release and explicit migration" }}{{ end }}
{{- if not (has (int .Values.replicaCount) (list 3 5)) }}
{{- fail "cluster mode requires 3 or 5 stable replicas; changing membership needs an operator-managed migration" }}
{{- end }}
{{- if not .Values.persistence.enabled }}{{ fail "cluster mode requires persistent volumes" }}{{ end }}
{{- if not .Values.cluster.internalSecretName }}{{ fail "cluster.internalSecretName is required" }}{{ end }}
{{- if not .Values.cluster.internalTLSSecretName }}{{ fail "cluster.internalTLSSecretName is required" }}{{ end }}
{{- if or (lt (int .Values.cluster.minInSyncReplicas) 2) (gt (int .Values.cluster.minInSyncReplicas) (int .Values.replicaCount)) }}{{ fail "cluster minInSyncReplicas must be between 2 and replicaCount" }}{{ end }}
{{- if gt (len (include "cursus.fullname" .)) 50 }}{{ fail "cluster release fullname must not exceed 50 characters" }}{{ end }}
{{- end }}
{{- if .Values.authentication.enabled }}
{{- if not .Values.authentication.secretName }}{{ fail "authentication.secretName is required" }}{{ end }}
{{- if not .Values.tls.enabled }}{{ fail "client authentication requires TLS" }}{{ end }}
{{- end }}
{{- if .Values.production }}
{{- if not (and .Values.persistence.enabled .Values.tls.enabled .Values.authentication.enabled .Values.monitoring.enabled .Values.networkPolicy.enabled) }}
{{- fail "production requires persistence, TLS, authentication, monitoring and network policy" }}
{{- end }}
{{- if not (and .Values.resources.requests.cpu .Values.resources.requests.memory .Values.resources.limits.cpu .Values.resources.limits.memory) }}
{{- fail "production requires CPU and memory requests and limits" }}
{{- end }}
{{- end }}
{{- $ports := dict }}
{{- $allPorts := list .Values.service.port .Values.service.healthPort .Values.service.metricsPort }}
{{- if eq .Values.mode "cluster" }}{{ $allPorts = concat $allPorts (list .Values.cluster.raftPort .Values.cluster.discoveryPort .Values.cluster.internalPort) }}{{ end }}
{{- range $allPorts }}
{{- $port := int . }}
{{- $key := toString $port }}
{{- if or (lt $port 1) (gt $port 65535) (hasKey $ports $key) }}{{ fail "listener ports must be unique and between 1 and 65535" }}{{ end }}
{{- $_ := set $ports $key true }}
{{- end }}
{{- if or (lt (int .Values.terminationGracePeriodSeconds) 60) (lt (int .Values.startupProbeFailureThreshold) 30) }}{{ fail "shutdown grace must be at least 60 seconds and startup threshold at least 30" }}{{ end }}
{{- if or (lt (int .Values.shutdownTimeoutMS) 1) (gt (int .Values.shutdownTimeoutMS) 600000) }}{{ fail "shutdownTimeoutMS must be between 1 and 600000" }}{{ end }}
{{- if lt (mul (int .Values.terminationGracePeriodSeconds) 1000) (add (int .Values.shutdownTimeoutMS) 5000) }}{{ fail "termination grace must exceed the broker shutdown timeout by at least 5 seconds" }}{{ end }}
{{- if or (lt (int .Values.limits.maxClientConnections) 1) (lt (int .Values.limits.maxStreamConnections) 1) (lt (int .Values.limits.clientIdleTimeoutMS) 1) }}{{ fail "connection limits and idle timeout must be positive" }}{{ end }}
{{- range list .Values.limits.maxInFlightRequests .Values.limits.maxRequestBytes .Values.limits.maxInternalInFlightRequests .Values.limits.maxInternalRequestBytes }}
{{- if lt (int .) 1 }}{{ fail "request admission limits must be positive" }}{{ end }}
{{- end }}
{{- end -}}
