package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
)

func (ch *CommandHandler) prepareTransaction(current *transaction.Transaction) (*transaction.Transaction, error) {
	if ch.isDistributed() && len(current.Offsets) != 0 {
		if ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
			return nil, fmt.Errorf("transaction prepare requires cluster metadata")
		}
		if err := ch.Cluster.RaftManager.GetFSM().PreparedTransactionRecoveryReady(); err != nil {
			return nil, err
		}
	}
	snap, err := ch.TxnManager.BuildPreparedSnapshot(current.ID, current.Producer, current.Epoch)
	if err != nil {
		return nil, err
	}
	persist := func() error {
		if ch.transactionStateSyncHook != nil {
			if err := ch.transactionStateSyncHook(snap.ID); err != nil {
				return err
			}
		} else if ch.isDistributed() {
			_, err := ch.applyViaLeader("TXN_SYNC", map[string]interface{}{"transaction": snap})
			return err
		} else if ch.txnJournal != nil {
			if err := ch.txnJournal.Append(snap); err != nil {
				return err
			}
		}
		return ch.TxnManager.ApplyReplicatedSnapshot(snap)
	}
	if len(snap.Offsets) == 0 {
		err = persist()
	} else {
		if ch.Coordinator == nil {
			return nil, fmt.Errorf("transaction prepare requires coordinator")
		}
		scope := snap.Offsets[0]
		items := make([]coordinator.OffsetItem, 0, len(snap.Offsets))
		for _, op := range snap.Offsets {
			if op.Group != scope.Group || op.Topic != scope.Topic || op.Member != scope.Member || op.Generation != scope.Generation {
				return nil, fmt.Errorf("transaction offset scope mismatch")
			}
			items = append(items, coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
		}
		err = ch.Coordinator.WithTransactionPrepareFence(scope.Group, scope.Topic, scope.Member, scope.Generation, items, func(epoch uint64) error {
			for i := range snap.Offsets {
				snap.Offsets[i].RegistrationEpoch = epoch
			}
			if ch.isDistributed() {
				return nil
			}
			return persist()
		})
		if err == nil && ch.isDistributed() {
			err = persist()
		}
	}
	if err != nil {
		return nil, err
	}
	return ch.waitForPreparedTransaction(snap)
}

func (ch *CommandHandler) waitForPreparedTransaction(snap *transaction.Snapshot) (*transaction.Transaction, error) {
	timer := time.NewTimer(DefaultFSMApplyTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := ch.TxnManager.Status(snap.ID)
		if err != nil {
			return nil, err
		}
		if current.Producer != snap.Producer || current.Epoch != snap.Epoch {
			return nil, fmt.Errorf("prepared transaction producer fenced")
		}
		if current.State == transaction.StateCommitting && current.Revision == snap.Revision &&
			reflect.DeepEqual(current.Messages, snap.Messages) && reflect.DeepEqual(current.Offsets, snap.Offsets) {
			return current, nil
		}
		select {
		case <-timer.C:
			return nil, fmt.Errorf("timeout waiting for local transaction prepare application")
		case <-ticker.C:
		}
	}
}

func (ch *CommandHandler) commitPreparedTransactionOffsets(tx *transaction.Transaction) error {
	if len(tx.Offsets) == 0 {
		return nil
	}
	scope := tx.Offsets[0]
	if scope.RegistrationEpoch == 0 {
		return ch.commitTransactionOffsets(tx.Offsets)
	}
	if ch.isDistributed() {
		_, err := ch.applyViaLeader("TXN_OFFSETS", map[string]interface{}{
			"transactional_id": tx.ID, "producer": tx.Producer, "epoch": tx.Epoch, "revision": tx.Revision,
		})
		return err
	}
	if ch.Coordinator == nil {
		return fmt.Errorf("transaction offset recovery requires coordinator")
	}
	items := make([]coordinator.OffsetItem, 0, len(tx.Offsets))
	for _, op := range tx.Offsets {
		if op.Group != scope.Group || op.Topic != scope.Topic || op.RegistrationEpoch != scope.RegistrationEpoch {
			return fmt.Errorf("prepared transaction offset scope mismatch")
		}
		items = append(items, coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
	}
	return ch.Coordinator.CommitPreparedOffsets(scope.Group, scope.Topic, scope.RegistrationEpoch, items)
}
