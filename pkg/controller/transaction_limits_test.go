package controller

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestTransactionLimitRejectionDoesNotPersistOrMutate(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	limits := transaction.DefaultLimits()
	limits.Transactions, limits.Messages = 1, 1
	manager, err := transaction.NewManagerWithLimits(time.Hour, limits)
	require.NoError(t, err)
	ch.TxnManager = manager
	require.NoError(t, tm.CreateTopic("orders", 1, false, false))
	producer, epoch, err := manager.InitProducer("one")
	require.NoError(t, err)
	require.NoError(t, manager.Begin("one", producer, epoch))
	writes := 0
	ch.transactionStateSyncHook = func(string) error { writes++; return nil }
	command := fmt.Sprintf("TXN_PUBLISH transactional_id=one topic=orders partition=0 producerId=%s epoch=%d seqNum=1 message=value", producer, epoch)
	require.Contains(t, ch.handleTxnPublish(command), "OK")
	before := manager.ExportState()
	bytesBefore := manager.RuntimeSnapshot().RetainedBytes
	response := ch.handleTxnPublish(command)
	parsed, ok := protocol.ParseErrorResponse(response)
	require.True(t, ok)
	require.Equal(t, "transaction_limit_exceeded", parsed.Code)
	require.Equal(t, protocol.ErrorClassValidation, parsed.Class)
	require.False(t, parsed.Retryable)
	require.Equal(t, before, manager.ExportState())
	require.Equal(t, bytesBefore, manager.RuntimeSnapshot().RetainedBytes)
	require.Equal(t, 1, writes)
	require.Contains(t, ch.handleInitProducerID("INIT_PRODUCER_ID transactional_id=two"), "ERROR: transaction_limit_exceeded")
	require.Equal(t, 1, writes)
}

func TestTransactionSyncRollbackRestoresAdmissionAccounting(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("orders", 1, false, false))
	producer, epoch, err := ch.TxnManager.InitProducer("one")
	require.NoError(t, err)
	require.NoError(t, ch.TxnManager.Begin("one", producer, epoch))
	before := ch.TxnManager.ExportState()
	bytesBefore := ch.TxnManager.RuntimeSnapshot().RetainedBytes
	ch.transactionStateSyncHook = func(string) error { return errors.New("injected sync failure") }
	command := fmt.Sprintf("TXN_PUBLISH transactional_id=one topic=orders partition=0 producerId=%s epoch=%d seqNum=1 message=value", producer, epoch)
	require.Contains(t, ch.handleTxnPublish(command), "ERROR: transaction_sync_failed")
	require.Equal(t, before, ch.TxnManager.ExportState())
	require.Equal(t, bytesBefore, ch.TxnManager.RuntimeSnapshot().RetainedBytes)
	require.Contains(t, ch.handleInitProducerID("INIT_PRODUCER_ID transactional_id=two"), "ERROR: transaction_sync_failed")
	require.Equal(t, before, ch.TxnManager.ExportState())
	require.Equal(t, bytesBefore, ch.TxnManager.RuntimeSnapshot().RetainedBytes)
}
