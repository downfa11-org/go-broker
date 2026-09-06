package controller

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func newDiskBackedTransactionHandler(t *testing.T) (*CommandHandler, *topic.TopicManager, *coordinator.Coordinator, *disk.DiskManager) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.IndexSize = 1024
	cfg.DiskFlushIntervalMS = 1

	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	coord := coordinator.NewCoordinator(context.Background(), cfg, tm)
	ch := NewCommandHandler(tm, cfg, coord, nil, nil)

	t.Cleanup(func() {
		_ = ch.Close()
		coord.Stop()
		tm.Stop()
		dm.CloseAllHandlers()
	})

	return ch, tm, coord, dm
}

func prepareTransactionGroup(t *testing.T, tm *topic.TopicManager, coord *coordinator.Coordinator, topicName, groupName, memberID string) int {
	t.Helper()

	require.NoError(t, tm.CreateTopic(topicName, 1, false, false))
	require.NoError(t, coord.RegisterGroup(topicName, groupName, 1))
	_, err := coord.AddConsumer(groupName, memberID)
	require.NoError(t, err)
	return coord.GetGeneration(groupName)
}

func initAndStageTransaction(t *testing.T, ch *CommandHandler, txnID, topicName, groupName, memberID string, generation int, nextOffset uint64) (string, int64) {
	t.Helper()

	producerID, epoch, err := ch.TxnManager.InitProducer(txnID)
	require.NoError(t, err)
	require.NoError(t, ch.TxnManager.Begin(txnID, producerID, epoch))
	require.NoError(t, ch.TxnManager.AddMessage(txnID, producerID, epoch, transaction.MessageOperation{
		Topic:     topicName,
		Partition: 0,
		Message: types.Message{
			Payload:          fmt.Sprintf("payload-%s", txnID),
			ProducerID:       producerID,
			SeqNum:           1,
			Epoch:            epoch,
			TransactionalID:  txnID,
			TransactionState: types.TransactionStateOpen,
		},
	}))
	require.NoError(t, ch.TxnManager.AddOffsets(txnID, producerID, epoch, []transaction.OffsetOperation{{
		Topic:      topicName,
		Group:      groupName,
		Member:     memberID,
		Generation: generation,
		Partition:  0,
		Offset:     nextOffset,
	}}))
	return producerID, epoch
}

func readCommittedPayloads(t *testing.T, tm *topic.TopicManager, topicName string) []string {
	t.Helper()

	p, err := tm.GetTopic(topicName).GetPartition(0)
	require.NoError(t, err)
	p.FlushDisk()
	messages, err := p.ReadCommitted(0, 100)
	require.NoError(t, err)

	payloads := make([]string, 0, len(messages))
	for _, msg := range messages {
		payloads = append(payloads, msg.Payload)
	}
	return payloads
}

func TestRecoverPreparedTransactionsLeavesOpenTransactionsInvisible(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-open-recovery-topic"
	groupName := "txn-open-recovery-group"
	memberID := "txn-open-recovery-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	initAndStageTransaction(t, ch, "tx-open-recovery", topicName, groupName, memberID, generation, 7)

	require.NoError(t, ch.RecoverPreparedTransactions())

	tx, err := ch.TxnManager.Status("tx-open-recovery")
	require.NoError(t, err)
	require.Equal(t, transaction.StateOpen, tx.State)
	require.Empty(t, readCommittedPayloads(t, tm, topicName))
	_, ok := coord.GetOffset(groupName, topicName, 0)
	require.False(t, ok)
}

func TestPreparedTransactionRemainsInvisibleUntilDurableDecision(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-decision-gate-topic"
	groupName := "txn-decision-gate-group"
	memberID := "txn-decision-gate-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	producerID, epoch := initAndStageTransaction(t, ch, "tx-decision-gate", topicName, groupName, memberID, generation, 13)
	tx, err := ch.TxnManager.PrepareCommit("tx-decision-gate", producerID, epoch)
	require.NoError(t, err)

	require.NoError(t, ch.applyTransaction(tx))

	require.Empty(t, readCommittedPayloads(t, tm, topicName))
	offset, ok := coord.GetOffset(groupName, topicName, 0)
	require.True(t, ok)
	require.Equal(t, uint64(13), offset)

	abortResp := ch.HandleCommand(
		fmt.Sprintf("END_TXN transactional_id=tx-decision-gate producerId=%s epoch=%d result=abort", producerID, epoch),
		NewClientContext("", 0),
	)
	require.Contains(t, abortResp, "ERROR: transaction_not_abortable")
	require.Empty(t, readCommittedPayloads(t, tm, topicName))

	require.NoError(t, ch.commitTransactionDecision("tx-decision-gate"))
	require.Equal(t, []string{"payload-tx-decision-gate"}, readCommittedPayloads(t, tm, topicName))
}

func TestStandaloneJournalRecoversPreparedTransactionAfterRestart(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	journalPath := filepath.Join(t.TempDir(), "transactions.journal")
	require.NoError(t, ch.ConfigureTransactionJournal(journalPath))

	topicName := "txn-journal-recovery-topic"
	groupName := "txn-journal-recovery-group"
	memberID := "txn-journal-recovery-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	producerID, epoch := initAndStageTransaction(t, ch, "tx-journal-recovery", topicName, groupName, memberID, generation, 17)
	require.NoError(t, ch.syncTransactionState("tx-journal-recovery"))
	require.NoError(t, ch.TxnManager.AddMessage("tx-journal-recovery", producerID, epoch, transaction.MessageOperation{
		Topic: topicName, Partition: 0,
		Message: types.Message{Payload: "second-output", ProducerID: producerID, SeqNum: 2, Epoch: epoch,
			TransactionalID: "tx-journal-recovery", TransactionState: types.TransactionStateOpen},
	}))
	require.NoError(t, ch.syncTransactionState("tx-journal-recovery"))
	tx, err := ch.TxnManager.PrepareCommit("tx-journal-recovery", producerID, epoch)
	require.NoError(t, err)
	require.NoError(t, ch.syncTransactionState("tx-journal-recovery"))
	require.NoError(t, ch.applyTransaction(tx))
	require.Empty(t, readCommittedPayloads(t, tm, topicName))

	restarted := NewCommandHandler(tm, ch.Config, coord, nil, nil)
	require.NoError(t, restarted.ConfigureTransactionJournal(journalPath))
	status, err := restarted.TxnManager.Status("tx-journal-recovery")
	require.NoError(t, err)
	require.Equal(t, transaction.StateCommitting, status.State)
	require.Len(t, status.Messages, 2)

	require.NoError(t, restarted.RecoverPreparedTransactions())
	require.Equal(t, []string{"payload-tx-journal-recovery", "second-output"}, readCommittedPayloads(t, tm, topicName))
	offset, ok := coord.GetOffset(groupName, topicName, 0)
	require.True(t, ok)
	require.Equal(t, uint64(17), offset)

	reloaded := NewCommandHandler(tm, ch.Config, coord, nil, nil)
	require.NoError(t, reloaded.ConfigureTransactionJournal(journalPath))
	finalStatus, err := reloaded.TxnManager.Status("tx-journal-recovery")
	require.NoError(t, err)
	require.Equal(t, transaction.StateCommitted, finalStatus.State)
	require.NoError(t, reloaded.RecoverPreparedTransactions())
	require.Equal(t, []string{"payload-tx-journal-recovery", "second-output"}, readCommittedPayloads(t, tm, topicName))
}
func TestRecoverPreparedTransactionsIsIdempotentAfterCommitWindow(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-commit-recovery-topic"
	groupName := "txn-commit-recovery-group"
	memberID := "txn-commit-recovery-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	producerID, epoch := initAndStageTransaction(t, ch, "tx-commit-recovery", topicName, groupName, memberID, generation, 9)
	_, err := ch.TxnManager.PrepareCommit("tx-commit-recovery", producerID, epoch)
	require.NoError(t, err)

	require.NoError(t, ch.RecoverPreparedTransactions())

	tx, err := ch.TxnManager.Status("tx-commit-recovery")
	require.NoError(t, err)
	require.Equal(t, transaction.StateCommitted, tx.State)
	require.Equal(t, []string{"payload-tx-commit-recovery"}, readCommittedPayloads(t, tm, topicName))
	offset, ok := coord.GetOffset(groupName, topicName, 0)
	require.True(t, ok)
	require.Equal(t, uint64(9), offset)

	p, err := tm.GetTopic(topicName).GetPartition(0)
	require.NoError(t, err)
	nextOffsetAfterRecovery := p.NextOffset()

	require.NoError(t, ch.RecoverPreparedTransactions())
	resp := ch.HandleCommand("END_TXN transactional_id=tx-commit-recovery producerId="+producerID+" epoch="+strconv.FormatInt(epoch, 10)+" result=commit", NewClientContext("", 0))
	require.True(t, strings.HasPrefix(resp, "OK "), resp)
	require.Equal(t, nextOffsetAfterRecovery, p.NextOffset())
	require.Equal(t, []string{"payload-tx-commit-recovery"}, readCommittedPayloads(t, tm, topicName))
	offset, ok = coord.GetOffset(groupName, topicName, 0)
	require.True(t, ok)
	require.Equal(t, uint64(9), offset)
}

func TestTransactionAbortRetryDoesNotMoveOffsetsOrAppendAgain(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-abort-retry-topic"
	groupName := "txn-abort-retry-group"
	memberID := "txn-abort-retry-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	producerID, epoch := initAndStageTransaction(t, ch, "tx-abort-retry", topicName, groupName, memberID, generation, 11)
	ctx := NewClientContext("", 0)
	cmd := "END_TXN transactional_id=tx-abort-retry producerId=" + producerID + " epoch=" + strconv.FormatInt(epoch, 10) + " result=abort"

	resp := ch.HandleCommand(cmd, ctx)
	require.Contains(t, resp, "state=aborted")
	require.Empty(t, readCommittedPayloads(t, tm, topicName))
	_, ok := coord.GetOffset(groupName, topicName, 0)
	require.False(t, ok)

	p, err := tm.GetTopic(topicName).GetPartition(0)
	require.NoError(t, err)
	nextOffsetAfterAbort := p.NextOffset()
	require.Equal(t, uint64(0), nextOffsetAfterAbort)

	resp = ch.HandleCommand(cmd, ctx)
	require.Contains(t, resp, "state=aborted")
	require.Equal(t, nextOffsetAfterAbort, p.NextOffset())
	require.Empty(t, readCommittedPayloads(t, tm, topicName))
	_, ok = coord.GetOffset(groupName, topicName, 0)
	require.False(t, ok)
}

func TestCommitTransactionOffsetsFailsClosedWithoutRouter(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	ops := []transaction.OffsetOperation{{
		Topic:      "orders",
		Group:      "workers",
		Member:     "member-1",
		Generation: 1,
		Partition:  0,
		Offset:     7,
	}}

	for name, cluster := range map[string]*clusterController.ClusterController{
		"nil cluster":      nil,
		"nil raft manager": {},
		"nil router":       {RaftManager: &replication.RaftReplicationManager{}},
	} {
		t.Run(name, func(t *testing.T) {
			ch := NewCommandHandler(nil, cfg, nil, nil, cluster)
			t.Cleanup(func() { require.NoError(t, ch.Close()) })
			err := ch.commitTransactionOffsets(ops)
			require.ErrorContains(t, err, "requires cluster coordinator router")
		})
	}
}

func TestEndTxnRetriesDurableCommittingSyncBeforeApply(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-sync-retry-topic"
	groupName := "txn-sync-retry-group"
	memberID := "txn-sync-retry-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)
	producerID, epoch := initAndStageTransaction(t, ch, "tx-sync-retry", topicName, groupName, memberID, generation, 19)

	syncCalls := 0
	ch.transactionStateSyncHook = func(txnID string) error {
		syncCalls++
		tx, err := ch.TxnManager.Status(txnID)
		require.NoError(t, err)
		require.Equal(t, transaction.StateOpen, tx.State)
		if syncCalls == 1 {
			return errors.New("injected durable sync failure")
		}
		return nil
	}

	cmd := fmt.Sprintf("END_TXN transactional_id=tx-sync-retry producerId=%s epoch=%d result=commit", producerID, epoch)
	first := ch.HandleCommand(cmd, NewClientContext("", 0))
	require.Contains(t, first, "transaction_sync_failed")
	require.Empty(t, readCommittedPayloads(t, tm, topicName))

	second := ch.HandleCommand(cmd, NewClientContext("", 0))
	require.True(t, strings.HasPrefix(second, "OK "), second)
	require.Equal(t, 2, syncCalls)
	require.Equal(t, []string{"payload-tx-sync-retry"}, readCommittedPayloads(t, tm, topicName))
}
