package config_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestRequestBudgetDefaultsAndOverrides(t *testing.T) {
	for _, cfg := range []*config.Config{config.DefaultConfig(), {}} {
		cfg.Normalize()
		require.Equal(t, config.DefaultMaxInFlightRequests, cfg.MaxInFlightRequests)
		require.Equal(t, config.DefaultMaxRequestBytes, cfg.MaxRequestBytes)
		require.Equal(t, config.DefaultMaxInFlightRequests, cfg.MaxInternalInFlightRequests)
		require.Equal(t, config.DefaultMaxRequestBytes, cfg.MaxInternalRequestBytes)
	}
	var cfg config.Config
	require.NoError(t, yaml.Unmarshal([]byte("max_inflight_requests: 3\nmax_request_bytes: 4096\nmax_internal_inflight_requests: 5\nmax_internal_request_bytes: 8192\n"), &cfg))
	cfg.Normalize()
	require.Equal(t, 3, cfg.MaxInFlightRequests)
	require.Equal(t, 4096, cfg.MaxRequestBytes)
	require.Equal(t, 5, cfg.MaxInternalInFlightRequests)
	require.Equal(t, 8192, cfg.MaxInternalRequestBytes)
}
