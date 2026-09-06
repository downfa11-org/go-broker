package fsm

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

func (f *BrokerFSM) applyTransactionSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Transaction *transaction.Snapshot `json:"transaction"`
	}
	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("FSM: Failed to unmarshal TXN_SYNC: %v", err)
		return err
	}
	if cmd.Transaction == nil || cmd.Transaction.ID == "" {
		return fmt.Errorf("invalid transaction sync payload")
	}
	if cmd.Transaction.State == transaction.StateCommitting && len(cmd.Transaction.Offsets) != 0 && cmd.Transaction.Offsets[0].RegistrationEpoch != 0 {
		if err := f.PreparedTransactionRecoveryReady(); err != nil {
			return err
		}
	}
	f.mu.RLock()
	txn := f.txn
	cd := f.cd
	for _, operation := range cmd.Transaction.Messages {
		if f.topicState[operation.Topic] == nil {
			f.mu.RUnlock()
			return fmt.Errorf("transaction topic %q is not present in cluster state", operation.Topic)
		}
	}
	for _, operation := range cmd.Transaction.Offsets {
		if f.topicState[operation.Topic] == nil {
			f.mu.RUnlock()
			return fmt.Errorf("transaction offset topic %q is not present in cluster state", operation.Topic)
		}
	}
	f.mu.RUnlock()
	if txn == nil {
		return fmt.Errorf("transaction manager not available")
	}
	snap := cmd.Transaction
	if snap.State == transaction.StateCommitting && len(snap.Offsets) != 0 {
		current, _ := txn.Status(snap.ID)
		prepared := current != nil && current.Producer == snap.Producer && current.Epoch == snap.Epoch &&
			reflect.DeepEqual(current.Messages, snap.Messages) && reflect.DeepEqual(current.Offsets, snap.Offsets) &&
			((current.State == transaction.StateCommitting && current.Revision == snap.Revision) ||
				(current.State == transaction.StateCommitted && current.Revision == snap.Revision+1))
		if !prepared {
			if cd == nil {
				return fmt.Errorf("transaction prepare requires coordinator")
			}
			scope := snap.Offsets[0]
			items := make([]coordinator.OffsetItem, 0, len(snap.Offsets))
			for _, op := range snap.Offsets {
				if op.Group != scope.Group || op.Topic != scope.Topic || op.Member != scope.Member || op.Generation != scope.Generation || op.RegistrationEpoch != scope.RegistrationEpoch {
					return fmt.Errorf("transaction offset scope mismatch")
				}
				items = append(items, coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
			}
			return cd.WithTransactionPrepareFence(scope.Group, scope.Topic, scope.Member, scope.Generation, items, func(epoch uint64) error {
				if scope.RegistrationEpoch != 0 && scope.RegistrationEpoch != epoch {
					return fmt.Errorf("transaction prepare group incarnation mismatch")
				}
				return txn.ApplyReplicatedSnapshot(snap)
			})
		}
	}
	return txn.ApplyReplicatedSnapshot(cmd.Transaction)
}

func (f *BrokerFSM) applyPreparedTransactionOffsetsCommand(jsonData string) interface{} {
	if err := f.PreparedTransactionRecoveryReady(); err != nil {
		return err
	}
	var cmd struct {
		ID       string `json:"transactional_id"`
		Producer string `json:"producer"`
		Epoch    int64  `json:"epoch"`
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		return err
	}
	f.mu.RLock()
	txn, cd := f.txn, f.cd
	f.mu.RUnlock()
	if txn == nil || cd == nil {
		return fmt.Errorf("transaction offset recovery requires transaction manager and coordinator")
	}
	tx, err := txn.Status(cmd.ID)
	if err != nil {
		return err
	}
	if tx.Producer != cmd.Producer || tx.Epoch != cmd.Epoch {
		return fmt.Errorf("prepared transaction producer fenced")
	}
	if tx.State == transaction.StateCommitted && tx.Revision == cmd.Revision+1 {
		return nil
	}
	if tx.State != transaction.StateCommitting || tx.Revision != cmd.Revision {
		return fmt.Errorf("transaction offset recovery requires matching durable prepare")
	}
	if len(tx.Offsets) == 0 {
		return nil
	}
	scope := tx.Offsets[0]
	items := make([]coordinator.OffsetItem, 0, len(tx.Offsets))
	for _, op := range tx.Offsets {
		if op.Group != scope.Group || op.Topic != scope.Topic || op.RegistrationEpoch != scope.RegistrationEpoch {
			return fmt.Errorf("prepared transaction offset scope mismatch")
		}
		items = append(items, coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
	}
	return cd.CommitPreparedOffsets(scope.Group, scope.Topic, scope.RegistrationEpoch, items)
}

func (f *BrokerFSM) PreparedTransactionRecoveryReady() error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.brokers) == 0 {
		return fmt.Errorf("prepared transaction recovery requires a registered broker topology")
	}
	for _, broker := range f.brokers {
		if broker.LifecycleProtocol < PreparedTransactionProtocolVersion {
			return fmt.Errorf("prepared transaction recovery requires broker protocol %d: broker=%s protocol=%d", PreparedTransactionProtocolVersion, broker.ID, broker.LifecycleProtocol)
		}
	}
	return nil
}
