package observability

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRequestBudgetMetrics(t *testing.T) {
	client := wire.NewFrameBudget(1, 4096)
	internal := wire.NewFrameBudget(2, 8192)
	release, err := client.Reserve(wire.Frame{}, 10, 20)
	require.NoError(t, err)
	defer release()
	_, err = client.Reserve(wire.Frame{}, 10, 20)
	require.ErrorIs(t, err, wire.ErrAdmissionLimit)
	collector := NewCollector(nil, nil, nil, nil, nil, nil).WithRequestBudgets(client, internal)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	require.NoError(t, err)
	assertGauge(t, families, "cursus_request_inflight", map[string]string{"listener": "client"}, 1)
	assertGauge(t, families, "cursus_request_reserved_bytes", map[string]string{"listener": "client"}, wire.HeaderSize+30)
	assertGauge(t, families, "cursus_request_byte_limit", map[string]string{"listener": "internal"}, 8192)
	assertGauge(t, families, "cursus_request_inflight_limit", map[string]string{"listener": "internal"}, 2)
	for _, family := range families {
		if family.GetName() == "cursus_request_rejected_total" {
			var total float64
			for _, metric := range family.Metric {
				total += metric.GetCounter().GetValue()
			}
			require.Equal(t, float64(1), total)
			return
		}
	}
	t.Fatal("request rejection counter missing")
}
