package config_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg.BrokerPort != 9000 {
		t.Errorf("Expected default BrokerPort 9000, got %d", cfg.BrokerPort)
	}
	if cfg.LogLevel != util.LogLevelInfo {
		t.Errorf("Expected default LogLevel Info, got %v", cfg.LogLevel)
	}
	if cfg.RaftSnapshotIntervalMS != 120000 {
		t.Errorf("Expected default RaftSnapshotIntervalMS 120000, got %d", cfg.RaftSnapshotIntervalMS)
	}
	if cfg.RaftSnapshotThreshold != 8192 {
		t.Errorf("Expected default RaftSnapshotThreshold 8192, got %d", cfg.RaftSnapshotThreshold)
	}
	if cfg.RaftTrailingLogs != 10240 {
		t.Errorf("Expected default RaftTrailingLogs 10240, got %d", cfg.RaftTrailingLogs)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("BROKER_PORT", "9999")
	t.Setenv("SHUTDOWN_TIMEOUT_MS", "12345")
	t.Setenv("LOG_RETENTION_HOURS", "24")
	t.Setenv("RAFT_SNAPSHOT_INTERVAL_MS", "250")
	t.Setenv("RAFT_SNAPSHOT_THRESHOLD", "16")
	t.Setenv("RAFT_TRAILING_LOGS", "0")
	t.Setenv("MAX_INFLIGHT_REQUESTS", "3")
	t.Setenv("MAX_REQUEST_BYTES", "4096")
	t.Setenv("MAX_INTERNAL_INFLIGHT_REQUESTS", "5")
	t.Setenv("MAX_INTERNAL_REQUEST_BYTES", "8192")

	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.BrokerPort != 9999 {
		t.Errorf("Expected BrokerPort 9999 from env, got %d", cfg.BrokerPort)
	}
	require.Equal(t, 3, cfg.MaxInFlightRequests)
	require.Equal(t, 12345, cfg.ShutdownTimeoutMS)
	require.Equal(t, 4096, cfg.MaxRequestBytes)
	require.Equal(t, 5, cfg.MaxInternalInFlightRequests)
	require.Equal(t, 8192, cfg.MaxInternalRequestBytes)
	if cfg.RetentionHours != 24 {
		t.Errorf("Expected RetentionHours 24 from env, got %d", cfg.RetentionHours)
	}
	if cfg.RaftSnapshotIntervalMS != 250 {
		t.Errorf("Expected RaftSnapshotIntervalMS 250 from env, got %d", cfg.RaftSnapshotIntervalMS)
	}
	if cfg.RaftSnapshotThreshold != 16 {
		t.Errorf("Expected RaftSnapshotThreshold 16 from env, got %d", cfg.RaftSnapshotThreshold)
	}
	if cfg.RaftTrailingLogs != 0 {
		t.Errorf("Expected RaftTrailingLogs 0 from env, got %d", cfg.RaftTrailingLogs)
	}
}

func TestConfigNormalizeCleanupPolicies(t *testing.T) {
	for input, expected := range map[string]string{
		"compact":        config.CleanupPolicyCompact,
		"delete":         config.CleanupPolicyDelete,
		"compact,delete": config.CleanupPolicyDeleteCompact,
		"delete,compact": config.CleanupPolicyDeleteCompact,
	} {
		cfg := config.DefaultConfig()
		cfg.CleanupPolicy = input
		cfg.Normalize()
		if cfg.CleanupPolicy != expected {
			t.Fatalf("cleanup policy %q normalized to %q, want %q", input, cfg.CleanupPolicy, expected)
		}
	}

	cfg := config.DefaultConfig()
	cfg.CleanupPolicy = "unknown"
	cfg.Normalize()
	if cfg.CleanupPolicy != config.CleanupPolicyDelete {
		t.Fatalf("invalid cleanup policy normalized to %q", cfg.CleanupPolicy)
	}
}

func TestConfig_Normalize(t *testing.T) {
	cfg := &config.Config{}
	cfg.BrokerPort = -1
	cfg.Normalize()
	if cfg.BrokerPort != 9000 {
		t.Errorf("Normalize should have reset BrokerPort to 9000, got %d", cfg.BrokerPort)
	}
}

func TestShutdownTimeoutNormalization(t *testing.T) {
	for _, value := range []int{-1, 0, 1, 30000, 600000, 600001} {
		cfg := config.DefaultConfig()
		cfg.ShutdownTimeoutMS = value
		cfg.Normalize()
		want := value
		if value < 1 || value > config.MaxShutdownTimeoutMS {
			want = config.DefaultShutdownTimeoutMS
		}
		require.Equal(t, want, cfg.ShutdownTimeoutMS)
	}
}
