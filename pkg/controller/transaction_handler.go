package controller

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

const transactionControlMarkerPayload = "__cursus_txn_control_marker__"

func (ch *CommandHandler) handleInitProducerID(cmd string) string {
	args := parseKeyValueArgs(cmd[len("INIT_PRODUCER_ID "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=INIT_PRODUCER_ID"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

	previousSnap, hadPrevious := ch.snapshotTransaction(txnID)
	producerID, epoch, err := ch.TxnManager.InitProducer(txnID)
	if err != nil {
		return transactionMutationError("init_producer_failed", err)
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		if hadPrevious {
			ch.TxnManager.ApplySnapshot(previousSnap)
		} else {
			ch.TxnManager.Delete(txnID)
		}
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s producerId=%s epoch=%d", txnID, producerID, epoch)
}

func (ch *CommandHandler) handleBeginTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("BEGIN_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=BEGIN_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

	producerID := firstNonEmpty(args["producerId"], args["producer_id"])
	if producerID == "" {
		return "ERROR: missing_producer_id command=BEGIN_TXN"
	}
	epoch, err := parseOptionalInt64(args["epoch"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_epoch reason=%q", err.Error())
	}
	previousSnap, hadPrevious := ch.snapshotTransaction(txnID)
	if err := ch.TxnManager.Begin(txnID, producerID, epoch); err != nil {
		if errors.Is(err, transaction.ErrProducerReinitializationRequired) {
			return fmt.Sprintf("ERROR: producer_reinitialization_required transactional_id=%s epoch=%d", txnID, epoch)
		}
		return transactionMutationError("transaction_begin_failed", err)
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		ch.restoreTransaction(txnID, previousSnap, hadPrevious)
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=open producerId=%s epoch=%d", txnID, producerID, epoch)
}

func (ch *CommandHandler) handleTxnPublish(cmd string, ctx ...*ClientContext) string {
	var clientCtx *ClientContext
	if len(ctx) > 0 {
		clientCtx = ctx[0]
	}
	args := parseKeyValueArgs(cmd[len("TXN_PUBLISH "):])
	if authResp := ch.authenticateInline(args, clientCtx); authResp != "" {
		return authResp
	}
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=TXN_PUBLISH"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=TXN_PUBLISH"
	}
	message := args["message"]
	if message == "" {
		return "ERROR: missing_message command=TXN_PUBLISH"
	}
	producerID, epoch, errResp := parseTxnProducerEpoch(args, "TXN_PUBLISH")
	if errResp != "" {
		return errResp
	}
	partition := -1
	if partitionStr := args["partition"]; partitionStr != "" {
		parsed, err := strconv.Atoi(partitionStr)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid_partition reason=%q", err.Error())
		}
		partition = parsed
	}
	seqNum, err := parseRequiredPositiveUint64(args["seqNum"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_seq_num command=TXN_PUBLISH reason=%q", err.Error())
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if authResp := ch.authorizeTopicWrite(t.PolicySnapshot(), clientCtx); authResp != "" {
		return fmt.Sprintf("%s topic=%s", authResp, topicName)
	}
	msg := types.Message{Payload: message, ProducerID: producerID, SeqNum: seqNum, Epoch: epoch, Key: args["key"], TransactionalID: txnID, TransactionState: types.TransactionStateOpen}
	if partition < 0 {
		partition = t.GetPartitionForMessage(msg)
	}
	if _, err := t.GetPartition(partition); err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
	}
	previousSnap, hadPrevious := ch.snapshotTransaction(txnID)
	if err := ch.TxnManager.AddMessage(txnID, producerID, epoch, transaction.MessageOperation{Topic: topicName, Partition: partition, Message: msg}); err != nil {
		return transactionMutationError("transaction_publish_failed", err)
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		ch.restoreTransaction(txnID, previousSnap, hadPrevious)
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_messages=1 topic=%s partition=%d", txnID, topicName, partition)
}

func (ch *CommandHandler) handleSendOffsetsToTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("SEND_OFFSETS_TO_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=SEND_OFFSETS_TO_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=SEND_OFFSETS_TO_TXN"
	}
	groupID := args["group"]
	if groupID == "" {
		return "ERROR: missing_group command=SEND_OFFSETS_TO_TXN"
	}
	producerID, epoch, errResp := parseTxnProducerEpoch(args, "SEND_OFFSETS_TO_TXN")
	if errResp != "" {
		return errResp
	}
	memberID := args["member"]
	if memberID == "" {
		return "ERROR: missing_member command=SEND_OFFSETS_TO_TXN"
	}
	generation, genErr := strconv.Atoi(args["generation"])
	if genErr != nil {
		return "ERROR: invalid_generation command=SEND_OFFSETS_TO_TXN"
	}
	if ch.Coordinator == nil {
		return "ERROR: offset_manager_not_available"
	}
	offsetTopic, offsetTopicErr := ch.resolveGroupOffsetTopic(groupID, topicName)
	if offsetTopicErr != "" {
		return offsetTopicErr
	}
	offsetPairs, err := wire.DecodeOffsetPairs(args["offsets"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_txn_offsets reason=%q", err.Error())
	}
	ops := make([]transaction.OffsetOperation, 0, len(offsetPairs))
	for _, pair := range offsetPairs {
		op := transaction.OffsetOperation{Topic: offsetTopic, Group: groupID, Member: memberID, Generation: generation, Partition: pair.Partition, Offset: pair.Offset}
		if err := ch.validateTransactionOffset(op, false); err != nil {
			return err.Error()
		}
		ops = append(ops, op)
	}
	previousSnap, hadPrevious := ch.snapshotTransaction(txnID)
	if err := ch.TxnManager.AddOffsets(txnID, producerID, epoch, ops); err != nil {
		return transactionMutationError("transaction_offsets_failed", err)
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		ch.restoreTransaction(txnID, previousSnap, hadPrevious)
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_offsets=%d", txnID, len(ops))
}

func (ch *CommandHandler) handleEndTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("END_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=END_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

	producerID, epoch, errResp := parseTxnProducerEpoch(args, "END_TXN")
	if errResp != "" {
		return errResp
	}
	result := strings.ToLower(firstNonEmpty(args["result"], args["action"], args["state"]))
	if result == "" {
		result = "commit"
	}
	current, statusErr := ch.TxnManager.ValidateOwner(txnID, producerID, epoch)
	if statusErr != nil {
		return fmt.Sprintf("ERROR: transaction_not_found reason=%q", statusErr.Error())
	}
	if result == "abort" {
		if current.State == transaction.StateCommitted {
			return fmt.Sprintf("ERROR: transaction_already_committed transactional_id=%s", txnID)
		}
		if current.State == transaction.StateAborted {
			return fmt.Sprintf("OK transactional_id=%s state=aborted", txnID)
		}
		if current.State == transaction.StateCommitting {
			return fmt.Sprintf("ERROR: transaction_not_abortable transactional_id=%s state=%s", txnID, current.State)
		}
		if err := ch.abortTransactionDecision(txnID, producerID, epoch); err != nil {
			return fmt.Sprintf("ERROR: transaction_abort_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK transactional_id=%s state=aborted", txnID)
	}
	if result != "commit" {
		return fmt.Sprintf("ERROR: invalid_transaction_result value=%s", result)
	}

	if current.State == transaction.StateCommitted {
		return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(current.Messages), len(current.Offsets))
	}
	if current.State == transaction.StateAborted {
		return fmt.Sprintf("ERROR: transaction_aborted transactional_id=%s", txnID)
	}
	if current.State == transaction.StateOpen {
		if err := ch.validateTransaction(current); err != nil {
			return fmt.Sprintf("ERROR: transaction_commit_failed state=open reason=%q", err.Error())
		}
	}

	tx := current
	if current.State == transaction.StateOpen {
		var err error
		tx, err = ch.prepareTransaction(current)
		if err != nil {
			return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
		}
	} else if tx.State == transaction.StateCommitting {
		if err := ch.syncTransactionState(txnID); err != nil {
			return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
		}
	}
	if err := ch.applyTransaction(tx); err != nil {
		if syncErr := ch.syncTransactionState(txnID); syncErr != nil {
			return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q sync_reason=%q", err.Error(), syncErr.Error())
		}
		return fmt.Sprintf("ERROR: transaction_commit_failed state=committing reason=%q", err.Error())
	}
	if err := ch.commitTransactionDecision(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(tx.Messages), len(tx.Offsets))
}

func (ch *CommandHandler) RecoverPreparedTransactions() error {
	if ch.TxnManager == nil {
		return nil
	}
	pending := ch.TxnManager.TransactionsByState(transaction.StateCommitting)
	if len(pending) == 0 {
		return nil
	}
	for _, pendingTx := range pending {
		if pendingTx == nil {
			continue
		}
		stateLock := ch.transactionStateLock(pendingTx.ID)
		stateLock.Lock()

		tx, err := ch.TxnManager.Status(pendingTx.ID)
		if err != nil {
			stateLock.Unlock()
			return fmt.Errorf("reload transaction %s for recovery: %w", pendingTx.ID, err)
		}
		if tx.State != transaction.StateCommitting {
			stateLock.Unlock()
			continue
		}
		if resp := ch.ensureTransactionCoordinator(tx.ID); resp != "" {
			stateLock.Unlock()
			util.Debug("Skipping transaction recovery for %s on non-coordinator: %s", tx.ID, resp)
			continue
		}
		if err := ch.applyTransaction(tx); err != nil {
			stateLock.Unlock()
			return fmt.Errorf("recover transaction %s: %w", tx.ID, err)
		}
		if err := ch.commitTransactionDecision(tx.ID); err != nil {
			stateLock.Unlock()
			return fmt.Errorf("mark recovered transaction %s committed: %w", tx.ID, err)
		}
		stateLock.Unlock()
		util.Info("Recovered prepared transaction %s", tx.ID)
	}
	return nil
}
func (ch *CommandHandler) handleTxnStatus(cmd string) string {
	args := parseKeyValueArgs(cmd[len("TXN_STATUS "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=TXN_STATUS"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	tx, err := ch.TxnManager.Status(txnID)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_not_found reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=%s messages=%d offsets=%d", tx.ID, tx.State, len(tx.Messages), len(tx.Offsets))
}

func (ch *CommandHandler) applyTransaction(tx *transaction.Transaction) error {
	ch.txnApplyMu.Lock()
	defer ch.txnApplyMu.Unlock()

	if err := ch.validateTransaction(tx); err != nil {
		return err
	}
	apply := func() error {
		for _, op := range tx.Messages {
			if err := ch.publishCommittedTransactionMessage(op); err != nil {
				return err
			}
		}
		if err := ch.appendTransactionMarkers(tx, types.TransactionMarkerCommit); err != nil {
			return err
		}
		if err := ch.commitPreparedTransactionOffsets(tx); err != nil {
			return err
		}
		return nil
	}
	return apply()
}

func (ch *CommandHandler) validateTransaction(tx *transaction.Transaction) error {
	for _, op := range tx.Messages {
		t := ch.TopicManager.GetTopic(op.Topic)
		if t == nil {
			return fmt.Errorf("topic %s not found", op.Topic)
		}
		if !t.PolicySnapshot().CanWrite() {
			return fmt.Errorf("NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", op.Topic)
		}
		if _, err := t.GetPartition(op.Partition); err != nil {
			return err
		}
	}
	for _, op := range tx.Offsets {
		if tx.State == transaction.StateCommitting && op.RegistrationEpoch != 0 {
			continue
		}
		if err := ch.validateTransactionOffset(op, true); err != nil {
			return err
		}
	}
	return nil
}

func (ch *CommandHandler) validateTransactionOffset(op transaction.OffsetOperation, checkRegression bool) error {
	if ch.Coordinator == nil {
		return fmt.Errorf("ERROR: offset_manager_not_available")
	}
	if ch.isDistributed() && ch.Cluster != nil && ch.Cluster.Router != nil {
		cmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d member=%s generation=%d validate_only=true", op.Topic, op.Partition, op.Group, op.Offset, op.Member, op.Generation)
		if !checkRegression {
			cmd += " ownership_only=true"
		}
		resp, err := ch.Cluster.Router.ForwardToCoordinator(op.Group, cmd)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(resp, "OK") {
			return fmt.Errorf("%s", resp)
		}
		return nil
	}
	if errResp := ch.Coordinator.ValidateOwnershipFailure(op.Group, op.Member, op.Generation, op.Partition); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}
	if checkRegression {
		if current, ok := ch.Coordinator.GetOffset(op.Group, op.Topic, op.Partition); ok && op.Offset < current {
			return fmt.Errorf("offset regression group=%s topic=%s partition=%d current=%d got=%d", op.Group, op.Topic, op.Partition, current, op.Offset)
		}
	}
	return nil
}
func (ch *CommandHandler) appendTransactionMarkers(tx *transaction.Transaction, marker string) error {
	if tx == nil || len(tx.Messages) == 0 {
		return nil
	}
	state := types.TransactionStateCommitted
	if marker == types.TransactionMarkerAbort {
		state = types.TransactionStateAborted
	}

	partitions := touchedTransactionPartitions(tx)
	for _, partition := range partitions {
		controlKey, controlValue, err := transactionMarkerControlBytes(marker, tx.Epoch)
		if err != nil {
			return err
		}
		msg := types.Message{
			Payload:                      transactionControlMarkerPayload,
			ProducerID:                   transactionMarkerProducerID(tx, marker),
			SeqNum:                       1,
			Epoch:                        tx.Epoch,
			TransactionalID:              tx.ID,
			TransactionState:             state,
			TransactionMarker:            marker,
			ControlBatchType:             types.ControlBatchTransaction,
			ControlBatchVersion:          types.ControlBatchVersionCursusV2,
			ControlBatchCoordinatorEpoch: tx.Epoch,
			ControlBatchKey:              controlKey,
			ControlBatchValue:            controlValue,
		}
		if err := ch.publishTransactionMarker(partition.Topic, partition.Partition, msg); err != nil {
			return err
		}
	}
	return nil
}

type transactionPartition struct {
	Topic     string
	Partition int
}

func transactionMarkerProducerID(tx *transaction.Transaction, marker string) string {
	if tx == nil {
		return "txn-marker:unknown"
	}
	return fmt.Sprintf("txn-marker:%s:%s", tx.ID, marker)
}
func touchedTransactionPartitions(tx *transaction.Transaction) []transactionPartition {
	seen := make(map[transactionPartition]struct{})
	for _, op := range tx.Messages {
		seen[transactionPartition{Topic: op.Topic, Partition: op.Partition}] = struct{}{}
	}
	out := make([]transactionPartition, 0, len(seen))
	for partition := range seen {
		out = append(out, partition)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic == out[j].Topic {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Topic < out[j].Topic
	})
	return out
}

func (ch *CommandHandler) publishTransactionMarker(topicName string, partition int, msg types.Message) error {
	if ch.isDistributed() {
		cmd := fmt.Sprintf("PUBLISH topic=%s acks=all producerId=%s partition=%d seqNum=%d epoch=%d isIdempotent=true transactional_id=%s transaction_state=%s transaction_marker=%s control_batch_type=%s control_batch_version=%d control_batch_coordinator_epoch=%d control_batch_key=%s control_batch_value=%s internal_txn_publish=true message=%s", topicName, msg.ProducerID, partition, msg.SeqNum, msg.Epoch, msg.TransactionalID, msg.TransactionState, msg.TransactionMarker, msg.ControlBatchType, msg.ControlBatchVersion, msg.ControlBatchCoordinatorEpoch, base64.StdEncoding.EncodeToString(msg.ControlBatchKey), base64.StdEncoding.EncodeToString(msg.ControlBatchValue), msg.Payload)
		return ch.publishInternalTransactionCommand(cmd)
	}
	return ch.TopicManager.PublishToPartitionWithAckIdempotent(topicName, partition, &msg)
}
func (ch *CommandHandler) publishCommittedTransactionMessage(op transaction.MessageOperation) error {
	msg := op.Message
	msg.TransactionState = types.TransactionStateOpen
	msg.TransactionMarker = types.TransactionMarkerNone
	if ch.isDistributed() {
		cmd := fmt.Sprintf("PUBLISH topic=%s acks=all producerId=%s partition=%d seqNum=%d epoch=%d isIdempotent=true internal_txn_publish=true", op.Topic, msg.ProducerID, op.Partition, msg.SeqNum, msg.Epoch)
		if msg.Key != "" {
			cmd += fmt.Sprintf(" key=%s", msg.Key)
		}
		if msg.TransactionalID != "" {
			cmd += fmt.Sprintf(" transactional_id=%s transaction_state=%s", msg.TransactionalID, msg.TransactionState)
		}
		cmd += " message=" + msg.Payload
		return ch.publishInternalTransactionCommand(cmd)
	}
	return ch.TopicManager.PublishToPartitionWithAckIdempotent(op.Topic, op.Partition, &msg)
}

func (ch *CommandHandler) publishInternalTransactionCommand(cmd string) error {
	deadline := time.Now().Add(DefaultFSMApplyTimeout)
	var lastResp string
	for {
		lastResp = ch.handlePublish(cmd, NewInternalClientContext("default-group", 0))
		if strings.HasPrefix(lastResp, "OK") || strings.HasPrefix(lastResp, "{") {
			return nil
		}
		if !isRetryableTransactionStateLag(lastResp) || !time.Now().Before(deadline) {
			return fmt.Errorf("%s", lastResp)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func isRetryableTransactionStateLag(resp string) bool {
	normalized := strings.ToLower(strings.TrimSpace(resp))
	for _, code := range []string{
		"transaction_not_found",
		"transaction_not_committing",
		"transaction_record_not_staged",
		"transaction_marker_partition_not_touched",
		"transaction_not_abortable",
		"producer_fenced",
	} {
		marker := "error: " + code
		if strings.HasPrefix(normalized, marker+" ") || strings.Contains(normalized, " "+marker+" ") {
			return true
		}
	}
	return false
}
func (ch *CommandHandler) validateTransactionPublishMetadata(args map[string]string, topicName string, partition int, msg *types.Message) string {
	if msg == nil {
		return ""
	}
	hasTxnMetadata := msg.TransactionalID != "" || msg.TransactionState != "" || msg.TransactionMarker != ""
	if !hasTxnMetadata {
		return ""
	}
	if !strings.EqualFold(args["internal_txn_publish"], "true") {
		return "ERROR: transaction_metadata_forbidden command=PUBLISH"
	}
	if msg.TransactionalID == "" {
		return "ERROR: missing_transactional_id command=PUBLISH"
	}
	if ch.TxnManager == nil {
		return "ERROR: transaction_manager_not_available command=PUBLISH"
	}
	tx, err := ch.TxnManager.Status(msg.TransactionalID)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_not_found transactional_id=%s", msg.TransactionalID)
	}
	if tx.Epoch != msg.Epoch {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	if msg.TransactionMarker != types.TransactionMarkerNone {
		return ch.validateTransactionMarkerPublish(tx, topicName, partition, msg)
	}
	if tx.Producer != msg.ProducerID {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	return ch.validateTransactionRecordPublish(tx, topicName, partition, msg)
}

func (ch *CommandHandler) validateTransactionRecordPublish(tx *transaction.Transaction, topicName string, partition int, msg *types.Message) string {
	if tx.State != transaction.StateCommitting && tx.State != transaction.StateCommitted {
		return fmt.Sprintf("ERROR: transaction_not_committing transactional_id=%s state=%s", tx.ID, tx.State)
	}
	if msg.TransactionState != types.TransactionStateOpen {
		return fmt.Sprintf("ERROR: invalid_transaction_state state=%s", msg.TransactionState)
	}
	for _, op := range tx.Messages {
		if op.Topic == topicName && op.Partition == partition && op.Message.ProducerID == msg.ProducerID && op.Message.SeqNum == msg.SeqNum && op.Message.Epoch == msg.Epoch && op.Message.Payload == msg.Payload && op.Message.Key == msg.Key {
			return ""
		}
	}
	return fmt.Sprintf("ERROR: transaction_record_not_staged transactional_id=%s", tx.ID)
}

func (ch *CommandHandler) validateTransactionMarkerPublish(tx *transaction.Transaction, topicName string, partition int, msg *types.Message) string {
	if msg.ControlBatchType != types.ControlBatchTransaction || msg.ControlBatchVersion != types.ControlBatchVersionCursusV2 {
		return fmt.Sprintf("ERROR: invalid_transaction_control_batch transactional_id=%s type=%s version=%d", tx.ID, msg.ControlBatchType, msg.ControlBatchVersion)
	}
	if msg.ControlBatchCoordinatorEpoch != tx.Epoch {
		return fmt.Sprintf("ERROR: invalid_transaction_control_epoch transactional_id=%s current_epoch=%d control_epoch=%d", tx.ID, tx.Epoch, msg.ControlBatchCoordinatorEpoch)
	}
	expectedKey, expectedValue, err := transactionMarkerControlBytes(msg.TransactionMarker, tx.Epoch)
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_transaction_control_epoch transactional_id=%s reason=%q", tx.ID, err.Error())
	}
	if !bytes.Equal(msg.ControlBatchKey, expectedKey) || !bytes.Equal(msg.ControlBatchValue, expectedValue) {
		return fmt.Sprintf("ERROR: invalid_transaction_control_record transactional_id=%s", tx.ID)
	}
	if msg.TransactionMarker != types.TransactionMarkerCommit && msg.TransactionMarker != types.TransactionMarkerAbort {
		return fmt.Sprintf("ERROR: invalid_transaction_marker marker=%s", msg.TransactionMarker)
	}
	if msg.ProducerID != transactionMarkerProducerID(tx, msg.TransactionMarker) || msg.SeqNum != 1 {
		return fmt.Sprintf("ERROR: invalid_transaction_marker_producer transactional_id=%s", tx.ID)
	}
	if msg.TransactionMarker == types.TransactionMarkerCommit && tx.State != transaction.StateCommitting && tx.State != transaction.StateCommitted {
		return fmt.Sprintf("ERROR: transaction_not_committing transactional_id=%s state=%s", tx.ID, tx.State)
	}
	if msg.TransactionMarker == types.TransactionMarkerAbort && tx.State != transaction.StateOpen && tx.State != transaction.StateAborted {
		return fmt.Sprintf("ERROR: transaction_not_abortable transactional_id=%s state=%s", tx.ID, tx.State)
	}
	for _, touched := range touchedTransactionPartitions(tx) {
		if touched.Topic == topicName && touched.Partition == partition {
			return ""
		}
	}
	return fmt.Sprintf("ERROR: transaction_marker_partition_not_touched transactional_id=%s topic=%s partition=%d", tx.ID, topicName, partition)
}
func (ch *CommandHandler) validateReplicatedTransactionMessage(topicName string, partition int, msg *types.Message) string {
	if msg == nil {
		return ""
	}
	hasTxnMetadata := msg.TransactionalID != "" || msg.TransactionState != "" || msg.TransactionMarker != ""
	if !hasTxnMetadata {
		return ""
	}
	if msg.TransactionalID == "" {
		return "ERROR: missing_transactional_id command=REPLICATE_MESSAGE"
	}
	if ch.TxnManager == nil {
		return "ERROR: transaction_manager_not_available command=REPLICATE_MESSAGE"
	}
	tx, err := ch.TxnManager.Status(msg.TransactionalID)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_not_found transactional_id=%s", msg.TransactionalID)
	}
	if tx.Epoch != msg.Epoch {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	if msg.TransactionMarker != types.TransactionMarkerNone {
		return ch.validateTransactionMarkerPublish(tx, topicName, partition, msg)
	}
	if tx.Producer != msg.ProducerID {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	return ch.validateTransactionRecordPublish(tx, topicName, partition, msg)
}
func (ch *CommandHandler) commitTransactionOffsets(ops []transaction.OffsetOperation) error {
	if len(ops) == 0 {
		return nil
	}

	ordered := append([]transaction.OffsetOperation(nil), ops...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Partition < ordered[j].Partition
	})
	scope := ordered[0]
	items := make([]coordinator.OffsetItem, 0, len(ordered))
	pairs := make([]wire.OffsetPair, 0, len(ordered))
	for _, op := range ordered {
		if op.Topic != scope.Topic || op.Group != scope.Group || op.Member != scope.Member || op.Generation != scope.Generation {
			return fmt.Errorf(
				"transaction offset scope mismatch: expected topic=%s group=%s member=%s generation=%d",
				scope.Topic, scope.Group, scope.Member, scope.Generation,
			)
		}
		items = append(items, coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
		pairs = append(pairs, wire.OffsetPair{Partition: op.Partition, Offset: op.Offset})
	}

	if ch.Config != nil && ch.Config.EnabledDistribution {
		if ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.Cluster.Router == nil {
			return fmt.Errorf("distributed transaction offset commit requires cluster coordinator router")
		}
		encodedOffsets, err := wire.EncodeOffsetPairs(pairs)
		if err != nil {
			return err
		}
		cmd := fmt.Sprintf(
			"BATCH_COMMIT topic=%s group=%s member=%s generation=%d offsets=%s",
			scope.Topic, scope.Group, scope.Member, scope.Generation, encodedOffsets,
		)
		resp, err := ch.Cluster.Router.ForwardToCoordinator(scope.Group, cmd)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(resp, "OK") {
			return fmt.Errorf("%s", resp)
		}
		return nil
	}
	if ch.Coordinator == nil {
		return fmt.Errorf("transaction offset commit requires coordinator")
	}
	return ch.Coordinator.ValidateAndCommitOffsetsBulk(
		scope.Group, scope.Topic, scope.Member, scope.Generation, items,
	)
}

func (ch *CommandHandler) ensureTransactionCoordinator(txnID string) string {
	if !ch.isDistributed() {
		return ""
	}
	coordAddr, isCoord, coordErr := ch.checkTransactionCoordinator(txnID)
	if coordErr != nil {
		return coordinatorUnavailableResponse
	}
	if !isCoord {
		return notCoordinatorResponse(coordAddr)
	}
	return ""
}
