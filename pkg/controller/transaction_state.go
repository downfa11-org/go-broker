package controller

import (
	"errors"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/transaction"
)

func transactionMutationError(code string, err error) string {
	if errors.Is(err, transaction.ErrLimitExceeded) {
		return fmt.Sprintf("ERROR: transaction_limit_exceeded reason=%q", err.Error())
	}
	return fmt.Sprintf("ERROR: %s reason=%q", code, err.Error())
}

func (ch *CommandHandler) snapshotTransaction(txnID string) (*transaction.Snapshot, bool) {
	return ch.TxnManager.SnapshotByID(txnID)
}

func (ch *CommandHandler) restoreTransaction(txnID string, snapshot *transaction.Snapshot, hadPrevious bool) {
	if hadPrevious {
		ch.TxnManager.ApplySnapshot(snapshot)
		return
	}
	ch.TxnManager.Delete(txnID)
}

func (ch *CommandHandler) ConfigureTransactionJournal(path string) error {
	if ch.isDistributed() {
		return fmt.Errorf("standalone transaction journal cannot be enabled in distributed mode")
	}
	journal, err := transaction.OpenJournal(path)
	if err != nil {
		return err
	}
	state, err := journal.Load()
	if err != nil {
		return err
	}
	if err := ch.TxnManager.ImportState(state); err != nil {
		return fmt.Errorf("validate recovered transaction journal: %w", err)
	}
	if ch.TxnManager.PruneExpired(time.Now()) > 0 {
		if err := journal.Rewrite(ch.TxnManager.ExportState()); err != nil {
			return fmt.Errorf("prune recovered transaction journal: %w", err)
		}
	}
	ch.txnJournal = journal
	if err := ch.RecoverPendingTruncations(); err != nil {
		return fmt.Errorf("recover pending topic truncation: %w", err)
	}
	return nil
}

func (ch *CommandHandler) syncTransactionState(txnID string) error {
	if ch.transactionStateSyncHook != nil {
		return ch.transactionStateSyncHook(txnID)
	}
	snapshot, ok := ch.TxnManager.SnapshotByID(txnID)
	if !ok {
		return fmt.Errorf("transaction %s not found", txnID)
	}
	if !ch.isDistributed() {
		if ch.txnJournal == nil {
			return nil
		}
		if err := ch.txnJournal.Append(snapshot); err != nil {
			return err
		}
		if ch.TxnManager.PruneExpired(time.Now()) > 0 {
			return ch.txnJournal.Rewrite(ch.TxnManager.ExportState())
		}
		return nil
	}
	_, err := ch.applyViaLeader("TXN_SYNC", map[string]interface{}{"transaction": snapshot})
	return err
}

func (ch *CommandHandler) commitTransactionDecision(txnID string) error {
	snapshot, err := ch.TxnManager.BuildCommittedSnapshot(txnID)
	if err != nil {
		return err
	}
	return ch.persistFinalTransactionDecision(snapshot)
}

func (ch *CommandHandler) abortTransactionDecision(txnID, producerID string, epoch int64) error {
	snapshot, err := ch.TxnManager.BuildAbortedSnapshot(txnID, producerID, epoch)
	if err != nil {
		return err
	}
	return ch.persistFinalTransactionDecision(snapshot)
}

func (ch *CommandHandler) persistFinalTransactionDecision(snapshot *transaction.Snapshot) error {
	if ch.isDistributed() {
		_, err := ch.applyViaLeader("TXN_SYNC", map[string]interface{}{"transaction": snapshot})
		return err
	}
	if ch.txnJournal != nil {
		if err := ch.txnJournal.Append(snapshot); err != nil {
			return err
		}
	}
	return ch.TxnManager.ApplyReplicatedSnapshot(snapshot)
}
