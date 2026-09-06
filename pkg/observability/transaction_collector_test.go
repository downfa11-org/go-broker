package observability

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/prometheus/client_golang/prometheus"
)

type fixedTransactions struct{ snapshot transaction.RuntimeSnapshot }

func (source fixedTransactions) RuntimeSnapshot() transaction.RuntimeSnapshot { return source.snapshot }

func TestCollectorExportsTransactionState(t *testing.T) {
	collector := NewCollector(nil, nil, nil, nil, nil, nil, fixedTransactions{snapshot: transaction.RuntimeSnapshot{
		ByState: map[transaction.State]int{
			transaction.StateOpen:       2,
			transaction.StateCommitting: 1,
		},
		Expired:                3,
		OldestActiveAgeSeconds: 42,
		RecoveryReady:          true,
		RetainedBytes:          4096,
	}})
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	assertGauge(t, families, "cursus_transaction_recovery_ready", nil, 1)
	assertGauge(t, families, "cursus_transactions", map[string]string{"state": "open"}, 2)
	assertGauge(t, families, "cursus_transactions", map[string]string{"state": "committing"}, 1)
	assertGauge(t, families, "cursus_transactions_expired", nil, 3)
	assertGauge(t, families, "cursus_transaction_oldest_active_seconds", nil, 42)
	assertGauge(t, families, "cursus_transaction_retained_bytes", nil, 4096)
}
