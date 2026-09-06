package transaction

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func assertTransactionAccounting(t *testing.T, m *Manager) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	for id, tx := range m.txns {
		size := transactionCharge(tx)
		require.Equal(t, size, m.sizes[id], id)
		total += size
	}
	require.Len(t, m.sizes, len(m.txns))
	require.Equal(t, total, m.totalBytes)
}

func TestTransactionAdmissionRejectsWithoutMutation(t *testing.T) {
	limits := DefaultLimits()
	limits.Transactions, limits.Messages, limits.Offsets = 1, 1, 1
	m, err := NewManagerWithLimits(time.Hour, limits)
	require.NoError(t, err)
	producer, epoch := beginInitialized(t, m, "one")
	_, _, err = m.InitProducer("two")
	require.ErrorIs(t, err, ErrLimitExceeded)
	op := MessageOperation{Topic: "orders", Message: types.Message{Payload: "value", ControlBatchKey: []byte{1}}}
	require.NoError(t, m.AddMessage("one", producer, epoch, op))
	op.Message.ControlBatchKey[0] = 9
	before := m.ExportState()
	require.Equal(t, byte(1), before["one"].Messages[0].Message.ControlBatchKey[0])
	require.ErrorIs(t, m.AddMessage("one", producer, epoch, op), ErrLimitExceeded)
	require.Equal(t, before, m.ExportState())
	offset := OffsetOperation{Topic: "orders", Group: "g", Member: "m", Offset: 1}
	require.NoError(t, m.AddOffsets("one", producer, epoch, []OffsetOperation{offset}))
	before = m.ExportState()
	offset.Partition = 1
	require.ErrorIs(t, m.AddOffsets("one", producer, epoch, []OffsetOperation{offset}), ErrLimitExceeded)
	require.Equal(t, before, m.ExportState())
	_, _, err = m.InitProducer(strings.Repeat("x", 1025))
	require.ErrorIs(t, err, ErrLimitExceeded)
	_, nextEpoch, err := m.InitProducer("one")
	require.NoError(t, err)
	require.Equal(t, epoch+1, nextEpoch)
	assertTransactionAccounting(t, m)
}

func TestTransactionByteBoundaryAndGlobalBudgetRelease(t *testing.T) {
	m := NewManager()
	first, firstEpoch := beginInitialized(t, m, "one")
	second, secondEpoch := beginInitialized(t, m, "two")
	op := MessageOperation{Message: types.Message{Payload: strings.Repeat("x", 1024)}}
	m.limits.TransactionBytes = m.sizes["one"] + messageCharge(op)
	m.limits.TotalBytes = m.totalBytes + messageCharge(op)
	require.NoError(t, m.AddMessage("one", first, firstEpoch, op))
	require.ErrorIs(t, m.AddMessage("one", first, firstEpoch, op), ErrLimitExceeded)
	before := m.ExportState()
	require.ErrorIs(t, m.AddMessage("two", second, secondEpoch, op), ErrLimitExceeded)
	require.Equal(t, before, m.ExportState())
	require.NoError(t, m.Abort("one", first, firstEpoch))
	require.NoError(t, m.AddMessage("two", second, secondEpoch, op))
	assertTransactionAccounting(t, m)
	m.Delete("two")
	assertTransactionAccounting(t, m)
}

func TestOffsetRegressionDoesNotPartiallyUpdateTransaction(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "one")
	offsets := []OffsetOperation{{Topic: "orders", Group: "g", Member: "m", Partition: 0, Offset: 10},
		{Topic: "orders", Group: "g", Member: "m", Partition: 1, Offset: 10}}
	require.NoError(t, m.AddOffsets("one", producer, epoch, offsets))
	before := m.ExportState()
	offsets[0].Offset, offsets[1].Offset = 11, 9
	require.ErrorContains(t, m.AddOffsets("one", producer, epoch, offsets), "regression")
	require.Equal(t, before, m.ExportState())
	offsets[1].Offset = 11
	require.NoError(t, m.AddOffsets("one", producer, epoch, offsets))
	assertTransactionAccounting(t, m)
}

func TestRecoveredTransactionsCanFinishAboveAdmissionBudget(t *testing.T) {
	m := NewManagerWithExpiration(time.Hour)
	producer, epoch := beginInitialized(t, m, "one")
	require.NoError(t, m.AddMessage("one", producer, epoch, MessageOperation{Topic: "orders", Message: types.Message{Payload: "value"}}))
	prepared, err := m.BuildPreparedSnapshot("one", producer, epoch)
	require.NoError(t, err)
	limits := DefaultLimits()
	limits.TotalBytes, limits.TransactionBytes = 1, 1
	recovered, err := NewManagerWithLimits(time.Hour, limits)
	require.NoError(t, err)
	require.NoError(t, recovered.ImportState(map[string]*Snapshot{"one": prepared}))
	assertTransactionAccounting(t, recovered)
	_, _, err = recovered.InitProducer("two")
	require.ErrorIs(t, err, ErrLimitExceeded)
	committed, err := recovered.BuildCommittedSnapshot("one")
	require.NoError(t, err)
	require.NoError(t, recovered.ApplyReplicatedSnapshot(committed))
	require.NoError(t, recovered.ApplyReplicatedSnapshot(prepared))
	assertTransactionAccounting(t, recovered)
	affected, err := recovered.PruneTopicReferences("orders")
	require.NoError(t, err)
	require.Equal(t, []string{"one"}, affected)
	assertTransactionAccounting(t, recovered)
	recovered.ApplySnapshot(committed)
	assertTransactionAccounting(t, recovered)
	require.Equal(t, 1, recovered.PruneExpired(time.Now().Add(2*time.Hour)))
	assertTransactionAccounting(t, recovered)
	require.Equal(t, 1, recovered.PruneExpired(time.Now().Add(4*time.Hour)))
	require.Zero(t, recovered.RuntimeSnapshot().RetainedBytes)
	assertTransactionAccounting(t, recovered)
}

func TestConcurrentTransactionAdmissionsRespectBudget(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "one")
	m.limits.Messages = 5
	var accepted atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 50; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := m.AddMessage("one", producer, epoch, MessageOperation{}); err == nil {
				accepted.Add(1)
			} else if !strings.Contains(err.Error(), ErrLimitExceeded.Error()) {
				t.Errorf("unexpected admission error: %v", err)
			}
		}()
	}
	workers.Wait()
	require.Equal(t, int32(5), accepted.Load())
	assertTransactionAccounting(t, m)
}

func TestTransactionChargeBoundsEscapedJournalRecord(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "one")
	value := strings.Repeat("\x00<>&\xff", 100)
	require.NoError(t, m.AddMessage("one", producer, epoch, MessageOperation{Topic: value, Message: types.Message{
		Payload: value, Metadata: value, Key: value, ControlBatchKey: []byte(value), ControlBatchValue: []byte(value)}}))
	snap, _ := m.SnapshotByID("one")
	encoded, err := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
	require.NoError(t, err)
	require.Greater(t, m.totalBytes, int64(len(encoded)))
}

func TestInvalidTransactionLimits(t *testing.T) {
	for _, limits := range []Limits{{}, {Transactions: 1, Messages: 1, Offsets: 1, TransactionBytes: maxJournalRecordBytes, TotalBytes: 1}} {
		_, err := NewManagerWithLimits(time.Hour, limits)
		require.Error(t, err)
	}
}
