package topic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// producerEntry tracks the last producer epoch, sequence number, and activity time for a producer.
type producerEntry struct {
	lastEpoch int64
	lastSeq   uint64
	lastSeen  time.Time
}

type stagedProducerEntry struct {
	lastEpoch int64
	lastSeq   uint64
}

// PartitionOffsetRange describes the retained and committed offsets for a partition.
// Latest is the next readable committed offset, capped by the flushed disk tail.
type PartitionOffsetRange struct {
	Earliest uint64
	Latest   uint64
	LEO      uint64
	HWM      uint64
}

type TransactionDecisionResolver interface {
	TransactionDecision(transactionalID string, epoch int64) (state string, known bool)
}

// Partition handles messages for one shard of a topic.
type Partition struct {
	id                    int
	topic                 string
	messageNotifyMu       sync.Mutex
	messageGeneration     uint64
	messageNotifyCh       chan struct{}
	LEO                   atomic.Uint64
	HWM                   uint64
	mu                    sync.RWMutex
	snapshotRecovery      bool
	recoveryCheckpointHWM uint64
	recoverySnapshotHWM   uint64
	dh                    types.StorageHandler
	closed                bool
	streamManager         StreamManager
	hwmCheckpointPath     string
	hwmCheckpointCh       chan struct{}
	hwmCheckpointMu       sync.Mutex
	hwmCheckpointWG       sync.WaitGroup
	producerStatePath     string
	producerStateCh       chan struct{}
	producerStateMu       sync.Mutex
	producerStateWG       sync.WaitGroup
	producerState         sync.Map // map[string]*producerEntry
	isIdempotent          bool
	producerStateTTL      time.Duration
	txnMarkerMu           sync.RWMutex
	txnResolver           TransactionDecisionResolver
	txnMarkers            map[transactionMarkerKey]transactionMarkerInfo
	txnOpenOffsets        map[transactionMarkerKey]uint64
	txnRetentionFloor     uint64
	closeCh               chan struct{}
}

func (p *Partition) SetTransactionDecisionResolver(resolver TransactionDecisionResolver) {
	p.txnMarkerMu.Lock()
	p.txnResolver = resolver
	p.txnMarkerMu.Unlock()
}

func (p *Partition) SetDistributedCompactionGate(gate func(topic string, partition int) (bool, string)) {
	setter, ok := p.dh.(interface {
		SetDistributedCompactionGate(func() (bool, string))
	})
	if !ok {
		return
	}
	setter.SetDistributedCompactionGate(func() (bool, string) {
		p.mu.RLock()
		closed := p.closed
		hwm := p.HWM
		p.mu.RUnlock()
		if closed {
			return false, "partition_closed"
		}
		if leo := p.LEO.Load(); leo != hwm {
			return false, "local_uncommitted_tail"
		}
		if gate == nil {
			return false, "cluster_gate_unavailable"
		}
		return gate(p.topic, p.id)
	})
}

// NewPartition creates a partition instance.
func NewPartition(id int, topic string, dh types.StorageHandler, sm StreamManager, cfg *config.Config) *Partition {
	// Storage exposes the next assignable offset, not the last record offset.
	initialOffset := dh.GetLatestOffset()

	p := &Partition{
		id:               id,
		topic:            topic,
		dh:               dh,
		streamManager:    sm,
		messageNotifyCh:  make(chan struct{}),
		closeCh:          make(chan struct{}),
		txnMarkers:       make(map[transactionMarkerKey]transactionMarkerInfo),
		txnOpenOffsets:   make(map[transactionMarkerKey]uint64),
		producerStateTTL: producerStateTTLFromConfig(cfg),
	}

	p.LEO.Store(initialOffset)
	p.HWM = initialOffset

	if handler, ok := dh.(*disk.DiskHandler); ok {
		p.hwmCheckpointPath = hwmCheckpointPath(handler, id)
		p.hwmCheckpointCh = make(chan struct{}, 1)
		p.producerStatePath = producerStateCheckpointPath(handler, id)
		p.producerStateCh = make(chan struct{}, 1)
		durableTail := handler.GetAbsoluteOffset()
		if persistedHWM, ok := loadHWMCheckpoint(p.hwmCheckpointPath); ok {
			if persistedHWM > durableTail {
				util.Warn("clamping HWM checkpoint %s from %d to durable tail %d", p.hwmCheckpointPath, persistedHWM, durableTail)
				p.HWM = durableTail
			} else {
				p.HWM = persistedHWM
			}
		} else {
			p.HWM = durableTail
		}
		handler.SetOnSync(func(uint64) { p.NotifyNewMessage() })
	}

	if p.hwmCheckpointCh != nil {
		p.hwmCheckpointWG.Add(1)
		go p.runHWMCheckpointLoop()
	}
	if p.producerStateCh != nil {
		p.loadProducerStateCheckpoint()
	}
	if _, ok := dh.(*disk.DiskHandler); ok {
		p.rebuildTransactionMarkerIndex()
	}

	return p
}

func (p *Partition) validateProducerMessage(msg *types.Message) (bool, error) {
	return p.validateProducerMessageWithStage(msg, nil, false)
}

func (p *Partition) validateProducerMessageWithStage(msg *types.Message, staged map[string]stagedProducerEntry, force bool) (bool, error) {
	if !p.isIdempotent && !force {
		return false, nil
	}
	if msg.ProducerID == "" {
		return false, nil
	}
	if msg.SeqNum == 0 {
		if force {
			return false, fmt.Errorf("idempotency error: producer %s must set seqNum > 0", msg.ProducerID)
		}
		// SeqNum == 0 means a non-transactional producer did not explicitly set a sequence number;
		// skip dedup to avoid incorrectly rejecting every message after the first.
		return false, nil
	}

	if staged != nil {
		if entry, ok := staged[msg.ProducerID]; ok {
			return p.validateAgainstProducerState(msg, entry.lastEpoch, entry.lastSeq, true)
		}
	}

	if val, ok := p.producerState.Load(msg.ProducerID); ok {
		entry := val.(*producerEntry)
		return p.validateAgainstProducerState(msg, entry.lastEpoch, entry.lastSeq, true)
	}

	if msg.SeqNum != 1 {
		return false, fmt.Errorf("idempotency error: first message for producer %s must have seqNum 1, got %d", msg.ProducerID, msg.SeqNum)
	}

	return false, nil
}
func (p *Partition) validateAgainstProducerState(msg *types.Message, lastEpoch int64, lastSeq uint64, allowDuplicate bool) (bool, error) {
	if msg.Epoch < lastEpoch {
		return false, fmt.Errorf("stale_producer_epoch producer=%s current=%d got=%d", msg.ProducerID, lastEpoch, msg.Epoch)
	}
	if msg.Epoch == lastEpoch {
		if allowDuplicate && msg.SeqNum <= lastSeq {
			metrics.SeqNumDuplicateTotal.WithLabelValues(p.topic, fmt.Sprintf("%d", p.id)).Inc()
			return true, nil
		}
		if msg.SeqNum != lastSeq+1 {
			return false, fmt.Errorf("idempotency gap for producer %s: expected %d, got %d", msg.ProducerID, lastSeq+1, msg.SeqNum)
		}
		return false, nil
	}
	if msg.SeqNum != 1 {
		return false, fmt.Errorf("idempotency error: first message in new producer epoch for producer %s must have seqNum 1, got %d", msg.ProducerID, msg.SeqNum)
	}
	return false, nil
}

func (p *Partition) updateProducerState(msg *types.Message) {
	p.updateProducerStateWithMode(msg, false)
}

func (p *Partition) updateProducerStateWithMode(msg *types.Message, force bool) {
	if (!p.isIdempotent && !force) || msg.ProducerID == "" {
		return
	}
	if msg.SeqNum > 0 {
		if val, ok := p.producerState.Load(msg.ProducerID); ok {
			entry := val.(*producerEntry)
			if msg.Epoch == entry.lastEpoch && msg.SeqNum > entry.lastSeq+1 {
				gap := msg.SeqNum - entry.lastSeq - 1
				metrics.SeqNumGapTotal.WithLabelValues(p.topic, fmt.Sprintf("%d", p.id), msg.ProducerID).Add(float64(gap))
				util.Warn("Partition %d: seqNum gap detected for producer %s: expected %d, got %d (gap=%d)",
					p.id, msg.ProducerID, entry.lastSeq+1, msg.SeqNum, gap)
			}
		}
	}
	p.producerState.Store(msg.ProducerID, &producerEntry{
		lastEpoch: msg.Epoch,
		lastSeq:   msg.SeqNum,
		lastSeen:  time.Now(),
	})
	p.signalProducerStateCheckpoint()
}

// Enqueue pushes a message into the partition queue.
func (p *Partition) Enqueue(msg types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		util.Warn("⚠️ Partition closed, dropping message [partition-%d]", p.id)
		return fmt.Errorf("partition %d is closed", p.id)
	}

	duplicate, err := p.validateProducerMessage(&msg)
	if err != nil {
		util.Warn("Partition %d: rejecting message from producer %s: %v", p.id, msg.ProducerID, err)
		return err
	}
	if duplicate {
		util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d)", p.id, msg.ProducerID, msg.Epoch, msg.SeqNum)
		return nil
	}

	offset, err := p.dh.AppendMessage(p.topic, p.id, &msg)
	if err != nil {
		util.Error("❌ Failed to enqueue message to disk [partition-%d]: %v", p.id, err)
		return err
	}

	p.updateProducerState(&msg)
	msg.Offset = offset
	p.indexTransactionMessage(msg)
	p.LEO.Store(offset + 1)
	p.setHWMLocked(offset + 1)

	p.NotifyNewMessage()
	return nil
}

func (p *Partition) EnqueueSync(msg types.Message) error {
	return p.enqueueSync(msg, false)
}

func (p *Partition) EnqueueSyncIdempotent(msg types.Message) error {
	return p.enqueueSync(msg, true)
}

func (p *Partition) enqueueSync(msg types.Message, forceIdempotent bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	duplicate, err := p.validateProducerMessageWithStage(&msg, nil, forceIdempotent)
	if err != nil {
		return err
	}
	if duplicate {
		util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d)", p.id, msg.ProducerID, msg.Epoch, msg.SeqNum)
		return nil
	}

	offset, err := p.dh.AppendMessageSync(p.topic, p.id, &msg)
	if err != nil {
		return fmt.Errorf("disk write failed: %w", err)
	}

	p.updateProducerStateWithMode(&msg, forceIdempotent)
	msg.Offset = offset
	p.indexTransactionMessage(msg)
	p.LEO.Store(offset + 1)
	p.setHWMLocked(offset + 1)

	p.NotifyNewMessage()
	return nil
}
func batchHasTransactionalMessages(msgs []types.Message) bool {
	for _, msg := range msgs {
		if msg.TransactionalID != "" {
			return true
		}
	}
	return false
}

// EnqueueBatchSync pushes multiple messages into the partition queue synchronously.
func (p *Partition) EnqueueBatchSync(msgs []types.Message) error {
	return p.EnqueueBatchSyncWithMode(msgs, false)
}

// EnqueueBatchSyncWithMode durably appends a standalone batch and can enforce
// producer sequencing even when the topic itself is not idempotent.
func (p *Partition) EnqueueBatchSyncWithMode(msgs []types.Message, forceIdempotent bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}
	if storage, ok := p.dh.(types.AllocatingBatchStorage); ok {
		return p.appendStandaloneBatch(storage, msgs, forceIdempotent)
	}

	for i := range msgs {
		duplicate, err := p.validateProducerMessageWithStage(&msgs[i], nil, forceIdempotent)
		if err != nil {
			return err
		}
		if duplicate {
			util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d) in batch sync", p.id, msgs[i].ProducerID, msgs[i].Epoch, msgs[i].SeqNum)
			continue
		}

		offset, err := p.dh.AppendMessageSync(p.topic, p.id, &msgs[i])
		if err != nil {
			p.NotifyNewMessage()
			return fmt.Errorf("disk write failed for partition %d: %w", p.id, err)
		}

		p.updateProducerStateWithMode(&msgs[i], forceIdempotent)
		msgs[i].Offset = offset
		p.indexTransactionMessage(msgs[i])
		p.LEO.Store(offset + 1)
		p.setHWMLocked(offset + 1)
	}
	p.NotifyNewMessage()
	return nil
}

// EnqueueBatch pushes multiple messages into the partition queue asynchronously.
func (p *Partition) EnqueueBatch(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	for i := range msgs {
		duplicate, err := p.validateProducerMessage(&msgs[i])
		if err != nil {
			return err
		}
		if duplicate {
			util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d) in batch", p.id, msgs[i].ProducerID, msgs[i].Epoch, msgs[i].SeqNum)
			continue
		}

		offset, err := p.dh.AppendMessage(p.topic, p.id, &msgs[i])
		if err != nil {
			p.NotifyNewMessage()
			return fmt.Errorf("batch enqueue failed at index %d: %w", i, err)
		}

		p.updateProducerState(&msgs[i])
		msgs[i].Offset = offset
		p.indexTransactionMessage(msgs[i])
		p.LEO.Store(offset + 1)
		p.setHWMLocked(offset + 1)
	}
	p.NotifyNewMessage()
	return nil
}

// EnqueueBatchLeader appends messages and updates LEO, but does NOT update HWM.
// Used by the partition leader in cluster mode. HWM is updated separately after
// successful replication, ensuring consumers never read unreplicated messages.
func (p *Partition) EnqueueBatchLeader(msgs []types.Message) error {
	return p.EnqueueBatchLeaderWithMode(msgs, false)
}

// EnqueueBatchLeaderWithMode appends leader records without advancing HWM and
// can enforce producer sequencing for request-level idempotence.
func (p *Partition) EnqueueBatchLeaderWithMode(msgs []types.Message, forceIdempotent bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}
	if p.id > math.MaxInt32 {
		return fmt.Errorf("partition ID %d exceeds int32 range", p.id)
	}
	partitionID := int32(p.id) // #nosec G115 -- p.id is validated against math.MaxInt32 before narrowing.

	type pendingLeaderMessage struct {
		index int
	}

	nextOffset := p.LEO.Load()
	pending := make([]pendingLeaderMessage, 0, len(msgs))
	diskBatch := make([]types.DiskMessage, 0, len(msgs))
	staged := make(map[string]stagedProducerEntry)
	forceIdempotent = forceIdempotent || batchHasTransactionalMessages(msgs)

	for i := range msgs {
		duplicate, err := p.validateProducerMessageWithStage(&msgs[i], staged, forceIdempotent)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}
		if (p.isIdempotent || forceIdempotent) && msgs[i].ProducerID != "" && msgs[i].SeqNum > 0 {
			staged[msgs[i].ProducerID] = stagedProducerEntry{
				lastEpoch: msgs[i].Epoch,
				lastSeq:   msgs[i].SeqNum,
			}
		}

		offset := nextOffset
		nextOffset++
		msgs[i].Offset = offset
		pending = append(pending, pendingLeaderMessage{
			index: i,
		})
		diskBatch = append(diskBatch, types.DiskMessage{
			Topic:                        p.topic,
			Partition:                    partitionID,
			Offset:                       offset,
			ProducerID:                   msgs[i].ProducerID,
			SeqNum:                       msgs[i].SeqNum,
			Epoch:                        msgs[i].Epoch,
			Payload:                      msgs[i].Payload,
			Key:                          msgs[i].Key,
			EventType:                    msgs[i].EventType,
			SchemaVersion:                msgs[i].SchemaVersion,
			AggregateVersion:             msgs[i].AggregateVersion,
			Metadata:                     msgs[i].Metadata,
			TransactionalID:              msgs[i].TransactionalID,
			TransactionState:             msgs[i].TransactionState,
			TransactionMarker:            msgs[i].TransactionMarker,
			ControlBatchType:             msgs[i].ControlBatchType,
			ControlBatchVersion:          msgs[i].ControlBatchVersion,
			ControlBatchCoordinatorEpoch: msgs[i].ControlBatchCoordinatorEpoch,
			ControlBatchKey:              msgs[i].ControlBatchKey,
			ControlBatchValue:            msgs[i].ControlBatchValue,
		})
	}

	if len(diskBatch) == 0 {
		p.NotifyNewMessage()
		return nil
	}

	if err := writeBatchDurably(p.dh, diskBatch); err != nil {
		for _, msg := range pending {
			msgs[msg.index].Offset = 0
		}
		p.NotifyNewMessage()
		return fmt.Errorf("leader batch write failed: %w", err)
	}

	for _, msg := range pending {
		p.updateProducerStateWithMode(&msgs[msg.index], forceIdempotent)
		p.indexTransactionMessage(msgs[msg.index])
	}
	p.LEO.Store(nextOffset)
	p.NotifyNewMessage()
	return nil
}

// AdvanceHWM sets HWM to the current LEO. Called after successful replication.
func (p *Partition) AdvanceHWM() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setHWMLocked(p.LEO.Load())
}

// ReplicaAppend writes messages with pre-assigned offsets from the leader
// (follower replication). It preserves the leader's offset assignments and
// updates LEO; committed HWM advances separately.
func (p *Partition) ReplicaAppend(msgs []types.Message) error {
	return p.ReplicaAppendWithMode(msgs, false)
}

// ReplicaAppendWithMode preserves request-level producer sequencing on a
// follower so promotion does not forget idempotent records from a
// non-idempotent topic.
func (p *Partition) ReplicaAppendWithMode(msgs []types.Message, forceIdempotent bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}
	if p.id > math.MaxInt32 {
		return fmt.Errorf("partition ID %d exceeds int32 range", p.id)
	}

	partitionID := int32(p.id) // #nosec G115 -- p.id is validated before narrowing.
	initialLEO := p.LEO.Load()
	nextOffset := initialLEO
	pending := make([]int, 0, len(msgs))
	diskBatch := make([]types.DiskMessage, 0, len(msgs))
	stagedByOffset := make(map[uint64]types.Message)

	for i := range msgs {
		switch {
		case msgs[i].Offset < initialLEO:
			existing, err := p.dh.ReadMessages(msgs[i].Offset, 1)
			if err != nil || len(existing) != 1 || !sameReplicatedMessage(existing[0], msgs[i]) {
				return fmt.Errorf("replica offset conflict at offset %d", msgs[i].Offset)
			}
			continue
		case msgs[i].Offset < nextOffset:
			existing, ok := stagedByOffset[msgs[i].Offset]
			if !ok || !sameReplicatedMessage(existing, msgs[i]) {
				return fmt.Errorf("replica offset conflict at offset %d", msgs[i].Offset)
			}
			continue
		case msgs[i].Offset > nextOffset:
			return fmt.Errorf("replica offset gap: expected %d, got %d", nextOffset, msgs[i].Offset)
		}

		pending = append(pending, i)
		diskBatch = append(diskBatch, diskMessageFromMessage(p.topic, partitionID, msgs[i]))
		stagedByOffset[msgs[i].Offset] = msgs[i]
		nextOffset++
	}

	if err := writeBatchDurably(p.dh, diskBatch); err != nil {
		return fmt.Errorf("replica batch append failed: %w", err)
	}
	for _, i := range pending {
		p.updateProducerStateWithMode(&msgs[i], forceIdempotent || msgs[i].TransactionalID != "")
		p.indexTransactionMessage(msgs[i])
	}
	if len(pending) > 0 {
		p.LEO.Store(nextOffset)
		p.NotifyNewMessage()
	}
	return nil
}

// ReplicaAppendCompactedRange installs a leader-provided logical range while
// preserving holes created by compaction and advancing only LEO. HWM remains a
// separate authoritative transition.
func (p *Partition) ReplicaAppendCompactedRange(msgs []types.Message, endOffset uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}
	startOffset := p.LEO.Load()
	if endOffset <= startOffset {
		return fmt.Errorf("invalid compacted replica range [%d,%d)", startOffset, endOffset)
	}
	storage, ok := p.dh.(types.CompactedRangeStorage)
	if !ok {
		return fmt.Errorf("storage does not support compacted replica ranges")
	}
	if p.id > math.MaxInt32 {
		return fmt.Errorf("partition ID %d exceeds int32 range", p.id)
	}
	partitionID := int32(p.id) // #nosec G115 -- p.id is validated before narrowing.
	diskBatch := make([]types.DiskMessage, 0, len(msgs))
	previous := startOffset
	for i := range msgs {
		if msgs[i].Offset < startOffset || msgs[i].Offset >= endOffset || (i > 0 && msgs[i].Offset <= previous) {
			return fmt.Errorf("invalid compacted replica offset %d for range [%d,%d)", msgs[i].Offset, startOffset, endOffset)
		}
		previous = msgs[i].Offset
		diskBatch = append(diskBatch, diskMessageFromMessage(p.topic, partitionID, msgs[i]))
	}
	if err := storage.WriteCompactedReplicaRange(startOffset, endOffset, diskBatch); err != nil {
		return fmt.Errorf("compacted replica range append failed: %w", err)
	}
	for i := range msgs {
		p.updateProducerStateWithMode(&msgs[i], true)
		p.indexTransactionMessage(msgs[i])
	}
	p.LEO.Store(endOffset)
	p.NotifyNewMessage()
	return nil
}

func writeBatchDurably(storage types.StorageHandler, batch []types.DiskMessage) error {
	if len(batch) == 0 {
		return nil
	}
	if durable, ok := storage.(types.DurableBatchStorage); ok {
		return durable.WriteBatchSync(batch)
	}
	return storage.WriteBatch(batch)
}

func diskMessageFromMessage(topic string, partition int32, msg types.Message) types.DiskMessage {
	return types.DiskMessage{
		Topic:                        topic,
		Partition:                    partition,
		Offset:                       msg.Offset,
		ProducerID:                   msg.ProducerID,
		SeqNum:                       msg.SeqNum,
		Epoch:                        msg.Epoch,
		Payload:                      msg.Payload,
		Key:                          msg.Key,
		EventType:                    msg.EventType,
		SchemaVersion:                msg.SchemaVersion,
		AggregateVersion:             msg.AggregateVersion,
		Metadata:                     msg.Metadata,
		TransactionalID:              msg.TransactionalID,
		TransactionState:             msg.TransactionState,
		TransactionMarker:            msg.TransactionMarker,
		ControlBatchType:             msg.ControlBatchType,
		ControlBatchVersion:          msg.ControlBatchVersion,
		ControlBatchCoordinatorEpoch: msg.ControlBatchCoordinatorEpoch,
		ControlBatchKey:              msg.ControlBatchKey,
		ControlBatchValue:            msg.ControlBatchValue,
	}
}

func sameReplicatedMessage(a, b types.Message) bool {
	return a.Offset == b.Offset && a.ProducerID == b.ProducerID && a.SeqNum == b.SeqNum &&
		a.Payload == b.Payload && a.Key == b.Key && a.Epoch == b.Epoch &&
		a.EventType == b.EventType && a.SchemaVersion == b.SchemaVersion &&
		a.AggregateVersion == b.AggregateVersion && a.Metadata == b.Metadata &&
		a.TransactionalID == b.TransactionalID && a.TransactionState == b.TransactionState &&
		a.TransactionMarker == b.TransactionMarker && a.ControlBatchType == b.ControlBatchType &&
		a.ControlBatchVersion == b.ControlBatchVersion &&
		a.ControlBatchCoordinatorEpoch == b.ControlBatchCoordinatorEpoch &&
		bytes.Equal(a.ControlBatchKey, b.ControlBatchKey) && bytes.Equal(a.ControlBatchValue, b.ControlBatchValue)
}
func (p *Partition) NotifyNewMessage() {
	p.messageNotifyMu.Lock()
	previous := p.messageNotifyCh
	p.messageGeneration++
	p.messageNotifyCh = make(chan struct{})
	close(previous)
	p.messageNotifyMu.Unlock()
}

// MessageNotification returns a monotonic generation and a channel closed by
// the next append/commit event. Every waiter on the same generation wakes.
func (p *Partition) MessageNotification() (uint64, <-chan struct{}) {
	p.messageNotifyMu.Lock()
	defer p.messageNotifyMu.Unlock()
	return p.messageGeneration, p.messageNotifyCh
}

func (p *Partition) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	return p.dh.ReadMessages(offset, max)
}

// ProducerSequenceOffset resolves the durable offset originally assigned to an
// idempotent producer sequence. It is used only on duplicate acknowledgements,
// where returning the current log tail would acknowledge the wrong record.
func (p *Partition) ProducerSequenceOffset(producerID string, epoch int64, seqNum uint64) (uint64, bool, error) {
	if producerID == "" || seqNum == 0 {
		return 0, false, nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	first := p.dh.GetFirstOffset()
	limit := p.LEO.Load()
	offset := first
	const batchSize = 1024
	for offset < limit {
		messages, err := p.dh.ReadMessages(offset, batchSize)
		if err != nil {
			return 0, false, err
		}
		if len(messages) == 0 {
			break
		}
		for _, message := range messages {
			if message.Offset >= limit {
				return 0, false, nil
			}
			if message.ProducerID == producerID && message.Epoch == epoch && message.SeqNum == seqNum {
				return message.Offset, true, nil
			}
			next := message.Offset + 1
			if next <= offset {
				next = offset + 1
			}
			offset = next
		}
		if len(messages) < batchSize {
			break
		}
	}
	return 0, false, nil
}

// LastStableOffset returns the first offset that may still be blocked by an
// unresolved transaction, capped by the committed and flushed partition tail.
func (p *Partition) LastStableOffset() uint64 {
	p.mu.RLock()
	hwm := p.HWM
	p.mu.RUnlock()

	flushed := p.dh.GetFlushedOffset()
	if flushed < hwm {
		hwm = flushed
	}
	p.pruneTransactionIndex(p.dh.GetFirstOffset())
	p.txnMarkerMu.RLock()
	defer p.txnMarkerMu.RUnlock()
	return firstUnresolvedOpenOffset(hwm, p.txnRetentionFloor, p.txnOpenOffsets, p.txnMarkers, p.txnResolver)
}
func (p *Partition) ReadCommitted(offset uint64, max int) ([]types.Message, error) {
	p.mu.RLock()
	hwm := p.HWM
	p.mu.RUnlock()

	// Cap at flushed offset to avoid reading data not yet on disk.
	flushed := p.dh.GetFlushedOffset()
	if flushed < hwm {
		hwm = flushed
	}

	earliest := p.dh.GetFirstOffset()
	if offset < earliest {
		return nil, &types.OffsetOutOfRangeError{
			Requested: offset,
			Earliest:  earliest,
			Latest:    hwm,
		}
	}

	if offset >= hwm {
		return nil, nil
	}
	p.pruneTransactionIndex(earliest)

	canRead := hwm - offset
	if canRead <= math.MaxInt && max > int(canRead) { // #nosec G115 -- canRead is bounded by math.MaxInt before narrowing.
		max = int(canRead) // #nosec G115 -- canRead is bounded by math.MaxInt before narrowing.
	}

	return p.readVisibleCommitted(offset, max, hwm)
}

func (p *Partition) readVisibleCommitted(offset uint64, max int, hwm uint64) ([]types.Message, error) {
	if max <= 0 {
		return nil, nil
	}

	p.txnMarkerMu.RLock()
	defer p.txnMarkerMu.RUnlock()
	resolver := p.txnResolver
	lso := firstUnresolvedOpenOffset(hwm, p.txnRetentionFloor, p.txnOpenOffsets, p.txnMarkers, resolver)
	scanLimit := hwm
	if lso < scanLimit {
		scanLimit = lso
	}
	return p.readCommittedScanRange(offset, scanLimit, hwm, max, p.txnMarkers, resolver)
}

func (p *Partition) readCommittedScanRange(offset, scanLimit, committedHWM uint64, maxVisible int, markers map[transactionMarkerKey]transactionMarkerInfo, resolver TransactionDecisionResolver) ([]types.Message, error) {
	if offset >= scanLimit {
		return nil, nil
	}

	visible := make([]types.Message, 0, maxVisible)
	current := offset
	const scanBatchSize = 1024
	for current < scanLimit && len(visible) < maxVisible {
		remaining := scanLimit - current
		readMax := scanBatchSize
		if needed := maxVisible - len(visible); needed < readMax {
			readMax = needed
		}
		if remaining <= math.MaxInt && readMax > int(remaining) { // #nosec G115 -- remaining is bounded by math.MaxInt before narrowing.
			readMax = int(remaining) // #nosec G115 -- remaining is bounded by math.MaxInt before narrowing.
		}
		if readMax <= 0 {
			break
		}

		batch, err := p.ReadMessages(current, readMax)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		for _, msg := range batch {
			if msg.Offset >= scanLimit {
				break
			}
			next := msg.Offset + 1
			if next <= current {
				next = current + 1
			}
			current = next
			if isReadCommittedVisible(msg, committedHWM, markers, resolver) {
				visible = append(visible, msg)
				if len(visible) == maxVisible {
					break
				}
			}
		}
		if len(batch) < readMax {
			break
		}
	}
	return visible, nil
}

func (p *Partition) indexTransactionMessage(msg types.Message) {
	if msg.TransactionalID == "" {
		return
	}
	key := messageTransactionMarkerKey(msg)
	p.txnMarkerMu.Lock()
	defer p.txnMarkerMu.Unlock()
	if p.txnMarkers == nil {
		p.txnMarkers = make(map[transactionMarkerKey]transactionMarkerInfo)
	}
	if p.txnOpenOffsets == nil {
		p.txnOpenOffsets = make(map[transactionMarkerKey]uint64)
	}
	if msg.TransactionMarker != types.TransactionMarkerNone {
		if existing, ok := p.txnMarkers[key]; !ok || msg.Offset >= existing.offset {
			p.txnMarkers[key] = transactionMarkerInfo{marker: msg.TransactionMarker, offset: msg.Offset}
		}
		return
	}
	if msg.TransactionState == types.TransactionStateOpen {
		if existing, ok := p.txnOpenOffsets[key]; !ok || msg.Offset < existing {
			p.txnOpenOffsets[key] = msg.Offset
		}
	}
}

func (p *Partition) pruneTransactionIndex(retentionFloor uint64) {
	p.txnMarkerMu.Lock()
	defer p.txnMarkerMu.Unlock()
	if retentionFloor <= p.txnRetentionFloor {
		return
	}
	for key, marker := range p.txnMarkers {
		if marker.offset >= retentionFloor {
			continue
		}
		delete(p.txnMarkers, key)
		delete(p.txnOpenOffsets, key)
	}
	p.txnRetentionFloor = retentionFloor
}

func (p *Partition) rebuildTransactionMarkerIndex() {
	if p.dh == nil {
		return
	}
	first := p.dh.GetFirstOffset()
	durableTail := p.dh.GetAbsoluteOffset()
	if durableTail <= first {
		return
	}
	markers := make(map[transactionMarkerKey]transactionMarkerInfo)
	openOffsets := make(map[transactionMarkerKey]uint64)
	offset := first
	const batchSize = 1024
	for offset < durableTail {
		msgs, err := p.dh.ReadMessages(offset, batchSize)
		if err != nil || len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			if msg.TransactionalID != "" && msg.TransactionMarker != types.TransactionMarkerNone {
				key := messageTransactionMarkerKey(msg)
				if existing, ok := markers[key]; !ok || msg.Offset >= existing.offset {
					markers[key] = transactionMarkerInfo{marker: msg.TransactionMarker, offset: msg.Offset}
				}
			}
			if msg.TransactionalID != "" && msg.TransactionMarker == types.TransactionMarkerNone && msg.TransactionState == types.TransactionStateOpen {
				key := messageTransactionMarkerKey(msg)
				if existing, ok := openOffsets[key]; !ok || msg.Offset < existing {
					openOffsets[key] = msg.Offset
				}
			}
			next := msg.Offset + 1
			if next <= offset {
				next = offset + 1
			}
			offset = next
		}
		if len(msgs) < batchSize {
			break
		}
	}
	p.txnMarkerMu.Lock()
	p.txnMarkers = markers
	p.txnOpenOffsets = openOffsets
	p.txnRetentionFloor = first
	p.txnMarkerMu.Unlock()
}

type transactionMarkerKey struct {
	transactionalID string
	epoch           int64
}

type transactionMarkerInfo struct {
	marker string
	offset uint64
}

func messageTransactionMarkerKey(msg types.Message) transactionMarkerKey {
	return transactionMarkerKey{transactionalID: msg.TransactionalID, epoch: msg.Epoch}
}

func firstUnresolvedOpenOffset(hwm, retentionFloor uint64, openOffsets map[transactionMarkerKey]uint64, markers map[transactionMarkerKey]transactionMarkerInfo, resolver TransactionDecisionResolver) uint64 {
	firstUnresolved := hwm
	for key, offset := range openOffsets {
		if offset >= hwm {
			continue
		}
		marker, ok := markers[key]
		if ok && marker.offset < hwm && marker.offset > offset && transactionDecisionMatchesMarker(key, marker, resolver) {
			continue
		}
		if offset < retentionFloor {
			offset = retentionFloor
		}
		if offset < firstUnresolved {
			firstUnresolved = offset
		}
	}
	return firstUnresolved
}

func transactionDecisionMatchesMarker(key transactionMarkerKey, marker transactionMarkerInfo, resolver TransactionDecisionResolver) bool {
	if resolver == nil {
		return true
	}
	state, known := resolver.TransactionDecision(key.transactionalID, key.epoch)
	if !known {
		return true
	}
	return marker.marker == types.TransactionMarkerCommit && state == types.TransactionStateCommitted ||
		marker.marker == types.TransactionMarkerAbort && state == types.TransactionStateAborted
}

func isReadCommittedVisible(msg types.Message, hwm uint64, markers map[transactionMarkerKey]transactionMarkerInfo, resolver TransactionDecisionResolver) bool {
	if msg.TransactionMarker != types.TransactionMarkerNone {
		return false
	}
	if msg.TransactionalID == "" {
		return true
	}
	if msg.TransactionState == types.TransactionStateAborted {
		return false
	}
	key := messageTransactionMarkerKey(msg)
	marker, ok := markers[key]
	return ok && marker.offset < hwm && marker.offset > msg.Offset && marker.marker == types.TransactionMarkerCommit &&
		transactionDecisionMatchesMarker(key, marker, resolver)
}

func (p *Partition) RecoverProducerStateFromLog() {
	if p.dh == nil || p.producerStateCh == nil {
		return
	}

	first := p.dh.GetFirstOffset()
	durableTail := p.dh.GetAbsoluteOffset()
	if durableTail <= first {
		return
	}
	offset := first
	const batchSize = 1024
	now := time.Now()

	for {
		msgs, err := p.dh.ReadMessages(offset, batchSize)
		if err != nil || len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			if msg.ProducerID != "" && msg.SeqNum > 0 {
				p.producerState.Store(msg.ProducerID, &producerEntry{lastEpoch: msg.Epoch, lastSeq: msg.SeqNum, lastSeen: now})
			}
			next := msg.Offset + 1
			if next <= offset {
				next = offset + 1
			}
			offset = next
		}
		if len(msgs) < batchSize || offset >= durableTail {
			break
		}
	}
	p.signalProducerStateCheckpoint()
}

func (p *Partition) StartProducerStateMaintenance() {
	if p.producerStateCh == nil {
		return
	}
	p.producerStateWG.Add(1)
	go p.runProducerStateCheckpointLoop()
	go p.runProducerCleanup()
}

// FlushDisk forces all pending async writes to disk.
func (p *Partition) FlushDisk() {
	p.dh.Flush()
	p.persistHWMCheckpoint()
	p.persistProducerStateCheckpoint()
}

func (p *Partition) GetFirstOffset() uint64 {
	if p.dh == nil {
		return 0
	}
	return p.dh.GetFirstOffset()
}
func (p *Partition) GetLatestOffset() uint64 {
	if p.dh == nil {
		return 0
	}
	return p.dh.GetLatestOffset()
}

func (p *Partition) OffsetRange() PartitionOffsetRange {
	if p.dh == nil {
		return PartitionOffsetRange{}
	}

	p.mu.RLock()
	hwm := p.HWM
	p.mu.RUnlock()

	latest := hwm
	flushed := p.dh.GetFlushedOffset()
	if flushed < latest {
		latest = flushed
	}

	return PartitionOffsetRange{
		Earliest: p.dh.GetFirstOffset(),
		Latest:   latest,
		LEO:      p.LEO.Load(),
		HWM:      hwm,
	}
}
func (p *Partition) ID() int {
	return p.id
}

// NextOffset returns the next available offset in the partition (Log End Offset).
func (p *Partition) NextOffset() uint64 {
	return p.LEO.Load()
}

func (p *Partition) ReserveOffsets(count int) (uint64, error) {
	if count <= 0 {
		return p.LEO.Load(), nil
	}
	delta := uint64(count) // #nosec G115 -- count is positive before widening.
	for {
		current := p.LEO.Load()
		if delta > math.MaxUint64-current {
			return current, fmt.Errorf("offset reservation overflow: current=%d count=%d", current, count)
		}
		if p.LEO.CompareAndSwap(current, current+delta) {
			return current, nil
		}
	}
}

// ApplyReplicaHWM advances the local committed watermark only when the data is present.
func (p *Partition) ApplyReplicaHWM(hwm uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if hwm > p.LEO.Load() {
		return fmt.Errorf("commit watermark %d is ahead of local LEO %d", hwm, p.LEO.Load())
	}
	if p.setHWMLocked(hwm) {
		p.NotifyNewMessage()
	}
	return nil
}

// ReconcileCommittedHWM prepares a replica for leadership using the durable cluster watermark.
// Any local tail beyond that watermark was never committed and must not survive leader promotion.
func (p *Partition) ReconcileCommittedHWM(hwm uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snapshotRecovery {
		return fmt.Errorf("snapshot replay is still pending: visible_hwm=%d durable_hwm=%d requested_hwm=%d", p.HWM, p.recoveryCheckpointHWM, hwm)
	}
	return p.reconcileCommittedHWMLocked(hwm)
}

// ReconcileSnapshotHWM stages the snapshot visibility boundary without
// deleting any later local records. Raft replays its committed log tail after
// Restore returns, then FinalizeSnapshotRecovery performs the only truncation.
func (p *Partition) ReconcileSnapshotHWM(snapshotHWM uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snapshotRecovery {
		if p.recoverySnapshotHWM != snapshotHWM {
			return fmt.Errorf("snapshot recovery boundary changed: current=%d requested=%d", p.recoverySnapshotHWM, snapshotHWM)
		}
		return nil
	}
	leo := p.LEO.Load()
	p.recoveryCheckpointHWM = p.HWM
	p.recoverySnapshotHWM = snapshotHWM
	p.HWM = min(snapshotHWM, leo)
	p.snapshotRecovery = true
	return nil
}

// FinalizeSnapshotRecovery reconciles to the FSM watermark after Raft has
// applied every committed post-snapshot log entry.
func (p *Partition) FinalizeSnapshotRecovery(hwm uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.snapshotRecovery {
		return p.reconcileCommittedHWMLocked(hwm)
	}
	checkpointHWM := p.recoveryCheckpointHWM
	snapshotHWM := p.recoverySnapshotHWM
	p.snapshotRecovery = false
	p.recoveryCheckpointHWM = 0
	p.recoverySnapshotHWM = 0
	if p.LEO.Load() < hwm {
		if p.HWM > p.LEO.Load() {
			p.HWM = p.LEO.Load()
		}
		if checkpointHWM != p.HWM {
			p.signalHWMCheckpointLocked()
		}
		return nil
	}
	if err := p.reconcileCommittedHWMLocked(hwm); err != nil {
		p.snapshotRecovery = true
		p.recoveryCheckpointHWM = checkpointHWM
		p.recoverySnapshotHWM = snapshotHWM
		return err
	}
	if checkpointHWM != hwm {
		p.signalHWMCheckpointLocked()
	}
	return nil
}

func (p *Partition) SnapshotRecoveryPending() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snapshotRecovery
}

func (p *Partition) reconcileCommittedHWMLocked(hwm uint64) error {
	leo := p.LEO.Load()
	if leo < hwm {
		return fmt.Errorf("replica is behind committed watermark: leo=%d hwm=%d", leo, hwm)
	}
	if leo == hwm && p.HWM == hwm {
		return nil
	}
	truncated := false
	if leo > hwm {
		if err := p.dh.TruncateTo(hwm); err != nil {
			return fmt.Errorf("truncate uncommitted tail to %d: %w", hwm, err)
		}
		p.LEO.Store(hwm)
		truncated = true
	}
	if p.HWM != hwm {
		p.HWM = hwm
		p.signalHWMCheckpointLocked()
	}
	if !truncated {
		return nil
	}

	p.txnMarkerMu.Lock()
	p.txnMarkers = make(map[transactionMarkerKey]transactionMarkerInfo)
	p.txnOpenOffsets = make(map[transactionMarkerKey]uint64)
	p.txnRetentionFloor = 0
	p.txnMarkerMu.Unlock()
	p.rebuildTransactionMarkerIndex()
	p.producerState.Range(func(key, _ any) bool {
		p.producerState.Delete(key)
		return true
	})
	p.RecoverProducerStateFromLog()
	return nil
}
func (p *Partition) SetHWM(hwm uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.setHWMLocked(hwm) {
		p.NotifyNewMessage()
	}
}

func (p *Partition) setHWMLocked(hwm uint64) bool {
	if hwm <= p.HWM {
		return false
	}
	p.HWM = hwm
	if !p.snapshotRecovery {
		p.signalHWMCheckpointLocked()
	}
	return true
}

func (p *Partition) signalHWMCheckpointLocked() {
	if p.hwmCheckpointCh == nil {
		return
	}
	select {
	case p.hwmCheckpointCh <- struct{}{}:
	default:
	}
}

func (p *Partition) runHWMCheckpointLoop() {
	defer p.hwmCheckpointWG.Done()

	ticker := time.NewTicker(hwmCheckpointInterval)
	defer ticker.Stop()

	dirty := false
	for {
		select {
		case <-p.hwmCheckpointCh:
			dirty = true
		case <-ticker.C:
			if dirty {
				p.persistHWMCheckpoint()
				dirty = false
			}
		case <-p.closeCh:
			p.persistHWMCheckpoint()
			return
		}
	}
}

func (p *Partition) persistHWMCheckpoint() {
	p.mu.RLock()
	checkpointPath := p.hwmCheckpointPath
	hwm := p.HWM
	p.mu.RUnlock()

	if checkpointPath == "" {
		return
	}

	p.hwmCheckpointMu.Lock()
	defer p.hwmCheckpointMu.Unlock()

	tmp := checkpointPath + ".tmp"
	data := []byte(strconv.FormatUint(hwm, 10) + "\n")
	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		util.Warn("failed to open HWM checkpoint %s: %v", tmp, err)
		return
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		util.Warn("failed to write HWM checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		util.Warn("failed to sync HWM checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Close(); err != nil {
		util.Warn("failed to close HWM checkpoint %s: %v", tmp, err)
		return
	}
	if err := replaceCheckpointFile(tmp, checkpointPath); err != nil {
		util.Warn("failed to rename HWM checkpoint %s: %v", checkpointPath, err)
		return
	}
	syncParentDir(filepath.Dir(checkpointPath))
}

func syncParentDir(path string) {
	if runtime.GOOS == "windows" {
		return
	}
	// #nosec G304 -- checkpoint directory is derived from the broker-owned partition log directory.
	dir, err := os.Open(path)
	if err != nil {
		util.Warn("failed to open HWM checkpoint directory %s: %v", path, err)
		return
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			util.Warn("failed to close HWM checkpoint directory %s: %v", path, closeErr)
		}
	}()
	if err := dir.Sync(); err != nil {
		util.Warn("failed to sync HWM checkpoint directory %s: %v", path, err)
	}
}

func hwmCheckpointPath(dh types.StorageHandler, partitionID int) string {
	if dh == nil {
		return ""
	}
	segmentPath := dh.GetSegmentPath(0)
	if segmentPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(segmentPath), fmt.Sprintf("partition_%d.hwm", partitionID))
}

func loadHWMCheckpoint(path string) (uint64, bool) {
	if path == "" {
		return 0, false
	}
	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	hwm, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		util.Warn("ignoring invalid HWM checkpoint %s: %v", path, err)
		return 0, false
	}
	return hwm, true
}

const producerStateCheckpointVersion = 2

type producerStateCheckpoint struct {
	Version   int                                     `json:"version"`
	Producers map[string]producerStateCheckpointEntry `json:"producers"`
}

type producerStateCheckpointEntry struct {
	Epoch int64  `json:"epoch"`
	Seq   uint64 `json:"seq"`
}

func (p *Partition) signalProducerStateCheckpoint() {
	if p.producerStateCh == nil {
		return
	}
	select {
	case p.producerStateCh <- struct{}{}:
	default:
	}
}

func (p *Partition) runProducerStateCheckpointLoop() {
	defer p.producerStateWG.Done()

	ticker := time.NewTicker(producerStateCheckpointInterval)
	defer ticker.Stop()

	dirty := false
	for {
		select {
		case <-p.producerStateCh:
			dirty = true
		case <-ticker.C:
			if dirty {
				p.persistProducerStateCheckpoint()
				dirty = false
			}
		case <-p.closeCh:
			p.persistProducerStateCheckpoint()
			return
		}
	}
}

func (p *Partition) loadProducerStateCheckpoint() {
	if p.producerStatePath == "" {
		return
	}
	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	data, err := os.ReadFile(p.producerStatePath)
	if err != nil {
		return
	}
	checkpoint, err := decodeProducerStateCheckpoint(data)
	if err != nil {
		util.Warn("ignoring invalid producer state checkpoint %s: %v", p.producerStatePath, err)
		return
	}
	now := time.Now()
	for producerID, entry := range checkpoint.Producers {
		if producerID == "" || entry.Seq == 0 {
			continue
		}
		p.producerState.Store(producerID, &producerEntry{lastEpoch: entry.Epoch, lastSeq: entry.Seq, lastSeen: now})
	}
}

func decodeProducerStateCheckpoint(data []byte) (producerStateCheckpoint, error) {
	var checkpoint producerStateCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return producerStateCheckpoint{}, fmt.Errorf("decode producer state checkpoint: %w", err)
	}
	if checkpoint.Version != producerStateCheckpointVersion {
		return producerStateCheckpoint{}, fmt.Errorf("unsupported producer state checkpoint version %d; clean bootstrap required", checkpoint.Version)
	}
	if checkpoint.Producers == nil {
		return producerStateCheckpoint{}, fmt.Errorf("producer state checkpoint is missing producers")
	}
	return checkpoint, nil
}

func (p *Partition) persistProducerStateCheckpoint() {
	if p.producerStatePath == "" {
		return
	}

	checkpoint := producerStateCheckpoint{
		Version:   producerStateCheckpointVersion,
		Producers: make(map[string]producerStateCheckpointEntry),
	}
	p.producerState.Range(func(key, value any) bool {
		producerID, ok := key.(string)
		if !ok || producerID == "" {
			return true
		}
		entry, ok := value.(*producerEntry)
		if !ok || entry.lastSeq == 0 {
			return true
		}
		checkpoint.Producers[producerID] = producerStateCheckpointEntry{Epoch: entry.lastEpoch, Seq: entry.lastSeq}
		return true
	})

	p.producerStateMu.Lock()
	defer p.producerStateMu.Unlock()

	tmp := p.producerStatePath + ".tmp"
	data, err := json.Marshal(checkpoint)
	if err != nil {
		util.Warn("failed to marshal producer state checkpoint %s: %v", p.producerStatePath, err)
		return
	}
	data = append(data, '\n')

	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		util.Warn("failed to open producer state checkpoint %s: %v", tmp, err)
		return
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		util.Warn("failed to write producer state checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		util.Warn("failed to sync producer state checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Close(); err != nil {
		util.Warn("failed to close producer state checkpoint %s: %v", tmp, err)
		return
	}
	if err := replaceCheckpointFile(tmp, p.producerStatePath); err != nil {
		util.Warn("failed to rename producer state checkpoint %s: %v", p.producerStatePath, err)
		return
	}
	syncParentDir(filepath.Dir(p.producerStatePath))
}

func producerStateCheckpointPath(dh types.StorageHandler, partitionID int) string {
	if dh == nil {
		return ""
	}
	segmentPath := dh.GetSegmentPath(0)
	if segmentPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(segmentPath), fmt.Sprintf("partition_%d.producers", partitionID))
}

// GetHWM returns the high water mark in a thread-safe manner.
func (p *Partition) GetHWM() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.HWM
}

func (p *Partition) UpdateLEO(leo uint64) {
	p.LEO.Store(leo)
}

// Close shuts down the partition.
func (p *Partition) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.closeCh)
	p.mu.Unlock()
	p.hwmCheckpointWG.Wait()
	p.producerStateWG.Wait()
}

const hwmCheckpointInterval = 250 * time.Millisecond

const producerStateCheckpointInterval = 250 * time.Millisecond

const defaultProducerStateTTL = 30 * time.Minute

func producerStateTTLFromConfig(cfg *config.Config) time.Duration {
	if cfg == nil || cfg.ProducerStateTTLMS <= 0 {
		return defaultProducerStateTTL
	}
	return time.Duration(cfg.ProducerStateTTLMS) * time.Millisecond
}

// cleanStaleProducers removes producer entries that have not been seen within the TTL.
func (p *Partition) cleanStaleProducers() {
	ttl := p.producerStateTTL
	if ttl <= 0 {
		ttl = defaultProducerStateTTL
	}
	cutoff := time.Now().Add(-ttl)
	p.producerState.Range(func(key, value any) bool {
		if entry := value.(*producerEntry); entry.lastSeen.Before(cutoff) {
			p.producerState.Delete(key)
		}
		return true
	})
}

// runProducerCleanup periodically evicts stale producer state to bound memory usage.
func (p *Partition) runProducerCleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.cleanStaleProducers()
		case <-p.closeCh:
			return
		}
	}
}
