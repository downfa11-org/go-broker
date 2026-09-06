package helm_test

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func render(t *testing.T, overrides ...string) ([]map[string]any, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is required; deployment CI installs it")
	}
	args := []string{"template", "cursus", "../../manifests/helm", "--namespace", "brokers"}
	for _, value := range overrides {
		args = append(args, "--set", value)
	}
	output, err := exec.Command(helm, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("helm: %w: %s", err, output)
	}
	var documents []map[string]any
	decoder := yaml.NewDecoder(bytes.NewReader(output))
	for {
		var doc map[string]any
		err := decoder.Decode(&doc)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if doc != nil {
			documents = append(documents, doc)
		}
	}
	return documents, nil
}

func findKind(t *testing.T, documents []map[string]any, kind string) map[string]any {
	t.Helper()
	for _, doc := range documents {
		if doc["kind"] == kind {
			return doc
		}
	}
	t.Fatalf("missing %s", kind)
	return nil
}

func TestHelmStandaloneAndClusterContracts(t *testing.T) {
	standalone, err := render(t)
	require.NoError(t, err)
	spec := findKind(t, standalone, "Deployment")["spec"].(map[string]any)
	require.Equal(t, "Recreate", spec["strategy"].(map[string]any)["type"])
	require.Equal(t, 1, spec["replicas"])
	containers := spec["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	env := containers[0].(map[string]any)["env"].([]any)
	require.Contains(t, env, map[string]any{"name": "ADVERTISED_CLIENT_HOST", "value": "cursus.brokers.svc.cluster.local"})
	pvc := findKind(t, standalone, "PersistentVolumeClaim")["spec"].(map[string]any)
	require.NotContains(t, pvc, "storageClassName")
	cluster, err := render(t, "mode=cluster", "replicaCount=3", "cluster.internalSecretName=internal-auth", "cluster.internalTLSSecretName=internal-tls", "monitoring.enabled=true", "monitoring.serviceMonitor.enabled=false", "networkPolicy.enabled=true")
	require.NoError(t, err)
	spec = findKind(t, cluster, "StatefulSet")["spec"].(map[string]any)
	require.Equal(t, "Parallel", spec["podManagementPolicy"])
	require.Equal(t, "OnDelete", spec["updateStrategy"].(map[string]any)["type"])
	require.Len(t, spec["volumeClaimTemplates"], 1)
	require.Equal(t, 2, findKind(t, cluster, "PodDisruptionBudget")["spec"].(map[string]any)["minAvailable"])
	findKind(t, cluster, "NetworkPolicy")
	for _, doc := range cluster {
		require.NotEqual(t, "PersistentVolumeClaim", doc["kind"])
		require.NotEqual(t, "ServiceMonitor", doc["kind"])
	}
	data := findKind(t, cluster, "ConfigMap")["data"].(map[string]any)["config.yaml"].(string)
	cfg := config.DefaultConfig()
	require.NoError(t, yaml.Unmarshal([]byte(data), cfg))
	require.True(t, cfg.EnabledDistribution)
	require.True(t, cfg.InternalUseTLS)
	require.True(t, cfg.EnableExporter)
	require.Equal(t, 30000, cfg.ShutdownTimeoutMS)
	require.Equal(t, 16, cfg.MaxInFlightRequests)
	require.Equal(t, 64<<20, cfg.MaxRequestBytes)
	require.Equal(t, 32, cfg.MaxInternalInFlightRequests)
	require.Equal(t, 128<<20, cfg.MaxInternalRequestBytes)
	require.Equal(t, 3, cfg.DefaultReplicationFactor)
	require.Len(t, cfg.StaticClusterMembers, 3)
	for i, member := range cfg.StaticClusterMembers {
		host := fmt.Sprintf("cursus-%d.cursus-headless.brokers.svc.cluster.local", i)
		require.Equal(t, host+"-9000@"+host+":9001", member)
	}
	require.NotContains(t, data, "internal_auth_token")
}

func TestHelmRejectsUnsafeOverrides(t *testing.T) {
	for _, overrides := range [][]string{
		{"replicaCount=2"}, {"mode=unknown"}, {"mode=cluster", "replicaCount=1"},
		{"shutdownTimeoutMS=0"}, {"shutdownTimeoutMS=600001"},
		{"terminationGracePeriodSeconds=60", "shutdownTimeoutMS=56000"},
		{"mode=cluster", "replicaCount=3"}, {"production=true"},
		{"authentication.enabled=true", "authentication.secretName=users"},
		{"limits.maxClientConnections=0"}, {"service.port=9080"},
		{"limits.maxInFlightRequests=0"}, {"limits.maxRequestBytes=-1"},
		{"limits.maxInternalInFlightRequests=0"}, {"limits.maxInternalRequestBytes=-1"},
		{"tls.enabled=true", "tls.certPath=/tmp/cert.pem"},
		{"mode=cluster", "replicaCount=3", "service.type=LoadBalancer"},
	} {
		t.Run(strings.Join(overrides, "_"), func(t *testing.T) { _, err := render(t, overrides...); require.Error(t, err) })
	}
}

func TestHelmShutdownBudget(t *testing.T) {
	documents, err := render(t, "terminationGracePeriodSeconds=120", "shutdownTimeoutMS=90000")
	require.NoError(t, err)
	spec := findKind(t, documents, "Deployment")["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	require.Equal(t, 120, spec["terminationGracePeriodSeconds"])
	data := findKind(t, documents, "ConfigMap")["data"].(map[string]any)["config.yaml"].(string)
	cfg := config.DefaultConfig()
	require.NoError(t, yaml.Unmarshal([]byte(data), cfg))
	require.Equal(t, 90000, cfg.ShutdownTimeoutMS)
}

func TestHelmProductionUsesOnlySecretReferences(t *testing.T) {
	documents, err := render(t, "production=true", "tls.enabled=true", "authentication.enabled=true", "authentication.secretName=client-auth", "monitoring.enabled=true", "networkPolicy.enabled=true", "monitoring.rules.enabled=true")
	require.NoError(t, err)
	findKind(t, documents, "PrometheusRule")
	for _, doc := range documents {
		require.NotEqual(t, "Secret", doc["kind"])
	}
	spec := findKind(t, documents, "Deployment")["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	require.Equal(t, false, spec["automountServiceAccountToken"])
	container := spec["containers"].([]any)[0].(map[string]any)
	found := false
	for _, raw := range container["env"].([]any) {
		env := raw.(map[string]any)
		if env["name"] == "SASL_USERS" {
			found = true
			require.Contains(t, env, "valueFrom")
			require.NotContains(t, env, "value")
		}
	}
	require.True(t, found)
}
