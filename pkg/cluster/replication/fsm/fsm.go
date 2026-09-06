package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

const (
	TopicLifecycleProtocolVersion        = 1
	DistributedCompactionProtocolVersion = 2
	PreparedTransactionProtocolVersion   = 3
	BrokerProtocolVersionCurrent         = PreparedTransactionProtocolVersion
)

type ReplicationEntry struct {
	Topic     string
	Partition int
	Message   types.Message
	Term      uint64
}

type BrokerInfo struct {
	ID                string    `json:"id"`
	Addr              string    `json:"addr"`
	ClientAddr        string    `json:"client_addr,omitempty"`
	Status            string    `json:"status"`
	LastSeen          time.Time `json:"last_seen"`
	LifecycleProtocol int       `json:"lifecycle_protocol,omitempty"`
}

type ProducerSequence struct {
	Epoch int64  `json:"epoch"`
	Seq   uint64 `json:"seq"`
}

type BrokerFSMState struct {
	Version           int                                            `json:"version"`
	Applied           uint64                                         `json:"applied"`
	Logs              map[uint64]*ReplicationEntry                   `json:"logs"`
	Brokers           map[string]*BrokerInfo                         `json:"brokers"`
	PartitionMetadata map[string]*PartitionMetadata                  `json:"partitionMetadata"`
	ProducerState     map[string]map[int]map[string]ProducerSequence `json:"producerState"`
	GroupState        map[string]*coordinator.GroupStateSnapshot     `json:"groupState,omitempty"`
	TransactionState  map[string]*transaction.Snapshot               `json:"transactionState,omitempty"`
	TopicState        map[string]*topic.Definition                   `json:"topicState,omitempty"`
}

type BrokerFSM struct {
	notifiers         map[string]chan interface{}
	mu                sync.RWMutex
	transitionMu      sync.Mutex
	materializationMu sync.Mutex

	logs                     map[uint64]*ReplicationEntry
	brokers                  map[string]*BrokerInfo
	partitionMetadata        map[string]*PartitionMetadata
	producerState            map[string]map[int]map[string]ProducerSequence // Topic -> Partition -> ProducerID -> Last Epoch/Seq
	applied                  uint64
	partitionRecoveryPending bool

	tm                       *topic.TopicManager
	cd                       *coordinator.Coordinator
	txn                      *transaction.Manager
	restoredTransactionState map[string]*transaction.Snapshot
	topicState               map[string]*topic.Definition
	topicMaterialization     map[string]TopicMaterializationIssue
	topicMaterializationRuns map[string]TopicMaterializationAttempts
}

func NewBrokerFSM(tm *topic.TopicManager, cd *coordinator.Coordinator) *BrokerFSM {
	return &BrokerFSM{
		notifiers:                make(map[string]chan interface{}),
		logs:                     make(map[uint64]*ReplicationEntry),
		brokers:                  make(map[string]*BrokerInfo),
		partitionMetadata:        make(map[string]*PartitionMetadata),
		producerState:            make(map[string]map[int]map[string]ProducerSequence),
		topicState:               make(map[string]*topic.Definition),
		topicMaterialization:     make(map[string]TopicMaterializationIssue),
		topicMaterializationRuns: make(map[string]TopicMaterializationAttempts),
		tm:                       tm,
		cd:                       cd,
	}
}

func (f *BrokerFSM) GetBrokers() []BrokerInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var brokers []BrokerInfo
	for _, broker := range f.brokers {
		brokers = append(brokers, *broker)
	}
	return brokers
}

func (f *BrokerFSM) GetAllPartitionKeys() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	keys := make([]string, 0, len(f.partitionMetadata))
	for k := range f.partitionMetadata {
		keys = append(keys, k)
	}
	return keys
}

func (f *BrokerFSM) AppliedIndex() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

func (f *BrokerFSM) HasPendingPartitionRecovery() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.partitionRecoveryPending
}

// BeginPartitionRecovery must run before Raft starts restoring or replaying.
func (f *BrokerFSM) BeginPartitionRecovery() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitionRecoveryPending = true
}

// GetTopicDefinition returns a detached copy of the authoritative replicated
// topic definition, including when node-local materialization is pending.
func (f *BrokerFSM) GetTopicDefinition(name string) (topic.Definition, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	definition := f.topicState[name]
	if definition == nil {
		return topic.Definition{}, false
	}
	return *copyTopicDefinition(definition), true
}

func (f *BrokerFSM) SetCoordinator(cd *coordinator.Coordinator) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cd = cd
}

func (f *BrokerFSM) SetTransactionManager(txn *transaction.Manager) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txn = txn
	if f.txn != nil && f.restoredTransactionState != nil {
		if err := f.txn.ImportState(f.restoredTransactionState); err != nil {
			util.Error("FSM: Rejected deferred restored transactions: %v", err)
			return
		}
		util.Info("FSM: Imported %d deferred restored transactions", len(f.restoredTransactionState))
		f.restoredTransactionState = nil
	}
}

func (f *BrokerFSM) TransactionManager() *transaction.Manager {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.txn
}

func (f *BrokerFSM) Apply(log *raft.Log) interface{} {
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()

	data := string(log.Data)
	var reqID string

	// skip prefixs like "MESSAGE:" or "REGISTER:"
	if startIdx := strings.Index(data, "{"); startIdx != -1 {
		jsonData := data[startIdx:]
		dec := json.NewDecoder(strings.NewReader(jsonData))
		var meta struct {
			ReqID string `json:"req_id"`
		}

		if err := dec.Decode(&meta); err != nil {
			if !strings.Contains(data, "REGISTER:") && !strings.Contains(data, "DEREGISTER:") {
				util.Debug("FSM Apply: potential JSON decode issue for req_id (Prefix: %s): %v", data[:startIdx], err)
			}
		} else {
			reqID = meta.ReqID
		}
	}

	var res interface{}
	switch {
	case strings.HasPrefix(data, "REGISTER:"):
		res = f.applyRegisterCommand(strings.TrimPrefix(data, "REGISTER:"))
	case strings.HasPrefix(data, "DEREGISTER:"):
		res = f.applyDeregisterCommand(strings.TrimPrefix(data, "DEREGISTER:"))
	case strings.HasPrefix(data, "MESSAGE:"):
		res = f.applyMessageCommand(strings.TrimPrefix(data, "MESSAGE:"))
	case strings.HasPrefix(data, "BATCH:"):
		res = f.applyMessageCommand(strings.TrimPrefix(data, "BATCH:"))
	case strings.HasPrefix(data, "TOPIC:"):
		res = f.applyTopicCommand(strings.TrimPrefix(data, "TOPIC:"))
	case strings.HasPrefix(data, "TOPIC_CONFIG:"):
		res = f.applyTopicConfigCommand(strings.TrimPrefix(data, "TOPIC_CONFIG:"))
	case strings.HasPrefix(data, "TOPIC_DELETE:"):
		res = f.applyTopicDeleteCommand(strings.TrimPrefix(data, "TOPIC_DELETE:"))
	case strings.HasPrefix(data, "TOPIC_TRUNCATE:"):
		res = f.applyTopicTruncateCommand(strings.TrimPrefix(data, "TOPIC_TRUNCATE:"))
	case strings.HasPrefix(data, "PARTITION:"):
		res = f.applyPartitionCommand(strings.TrimPrefix(data, "PARTITION:"))
	case strings.HasPrefix(data, "PARTITION_COMMIT:"):
		res = f.applyPartitionCommitCommand(strings.TrimPrefix(data, "PARTITION_COMMIT:"))
	case strings.HasPrefix(data, "ISR_CATCHUP:"):
		res = f.applyISRCatchupCommand(strings.TrimPrefix(data, "ISR_CATCHUP:"))
	case strings.HasPrefix(data, "LEADER_ELECTION:"):
		res = f.applyLeaderElectionCommand(strings.TrimPrefix(data, "LEADER_ELECTION:"))
	case strings.HasPrefix(data, "GROUP_SYNC:"):
		res = f.applyGroupSyncCommand(strings.TrimPrefix(data, "GROUP_SYNC:"))
	case strings.HasPrefix(data, "OFFSET_SYNC:"):
		res = f.applyOffsetSyncCommand(strings.TrimPrefix(data, "OFFSET_SYNC:"))
	case strings.HasPrefix(data, "BATCH_OFFSET:"):
		res = f.applyBatchOffsetSyncCommand(strings.TrimPrefix(data, "BATCH_OFFSET:"))
	case strings.HasPrefix(data, "TXN_SYNC:"):
		res = f.applyTransactionSyncCommand(strings.TrimPrefix(data, "TXN_SYNC:"))
	case strings.HasPrefix(data, "TXN_OFFSETS:"):
		res = f.applyPreparedTransactionOffsetsCommand(strings.TrimPrefix(data, "TXN_OFFSETS:"))
	default:
		res = f.handleUnknownCommand(data)
	}

	if reqID != "" {
		f.notify(reqID, res)
	}

	f.mu.Lock()
	f.applied = log.Index
	f.mu.Unlock()

	return res
}

func (f *BrokerFSM) Restore(rc io.ReadCloser) error {
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()

	defer func() {
		if err := rc.Close(); err != nil {
			util.Error("failed to close rc: %v", err)
		}
	}()

	util.Info("Starting FSM restore from snapshot")

	snapshotData, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(snapshotData, &header); err != nil {
		util.Error("Failed to decode snapshot: %v", err)
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}
	if header.Version != SnapshotVersionCurrent {
		return fmt.Errorf("%w: snapshot version %d is not supported; remove all Cursus persistent state and clean bootstrap version %d", ErrUnsupportedRecoveryProtocol, header.Version, SnapshotVersionCurrent)
	}

	var state BrokerFSMState
	if err := decodeStrictJSON(snapshotData, &state); err != nil {
		util.Error("Failed to decode snapshot version %d: %v", header.Version, err)
		return fmt.Errorf("failed to restore snapshot version %d: %w", header.Version, err)
	}
	util.Info("FSM Restore: Validating snapshot Version %d", state.Version)

	restoredTopicState := copyTopicState(state.TopicState)
	if len(restoredTopicState) == 0 && len(state.PartitionMetadata) > 0 {
		return fmt.Errorf("snapshot version %d is missing topic state", state.Version)
	}
	if err := validateSnapshotTopicDefinitionFields(restoredTopicState, state.PartitionMetadata); err != nil {
		return fmt.Errorf("restore topic definitions: %w", err)
	}
	definitions, err := validateTopicState(restoredTopicState, state.PartitionMetadata)
	if err != nil {
		return fmt.Errorf("restore topic definitions: %w", err)
	}
	restoredTopicState = topicStateFromDefinitions(definitions)
	if err := coordinator.ValidateImportState(state.GroupState); err != nil {
		return fmt.Errorf("restore consumer groups: %w", err)
	}
	if err := transaction.ValidateImportState(state.TransactionState); err != nil {
		return fmt.Errorf("restore transactions: %w", err)
	}
	f.materializationMu.Lock()
	localDefinitions := []topic.Definition(nil)
	persistedTopicStorage := []string(nil)
	if f.tm != nil {
		localDefinitions = f.tm.ExportDefinitions()
		persistedTopicStorage, err = f.tm.PersistedTopicStorageNames()
		if err != nil {
			f.materializationMu.Unlock()
			return fmt.Errorf("inspect local topic storage before restore: %w", err)
		}
	}

	f.mu.Lock()
	previousMaterialization := f.topicMaterialization

	f.logs = state.Logs
	f.brokers = state.Brokers
	f.partitionMetadata = state.PartitionMetadata
	f.topicState = restoredTopicState
	f.topicMaterialization = make(map[string]TopicMaterializationIssue, len(restoredTopicState))
	now := time.Now()
	for name := range restoredTopicState {
		f.topicMaterialization[name] = TopicMaterializationIssue{
			Topic: name, Operation: TopicMaterializationRestore, PendingSince: now,
		}
	}
	for _, definition := range localDefinitions {
		if definition.Name == config.ConsumerOffsetsTopicName {
			continue
		}
		if restoredTopicState[definition.Name] == nil {
			f.topicMaterialization[definition.Name] = TopicMaterializationIssue{
				Topic: definition.Name, Operation: TopicMaterializationDelete, PendingSince: now,
			}
		}
	}
	for _, name := range persistedTopicStorage {
		if name == config.ConsumerOffsetsTopicName {
			continue
		}
		if restoredTopicState[name] == nil {
			f.topicMaterialization[name] = TopicMaterializationIssue{
				Topic: name, Operation: TopicMaterializationDelete, PendingSince: now,
			}
		}
	}
	for name, issue := range previousMaterialization {
		if name == config.ConsumerOffsetsTopicName {
			continue
		}
		if issue.Operation != TopicMaterializationDelete || restoredTopicState[name] != nil {
			continue
		}
		if issue.PendingSince.IsZero() {
			issue.PendingSince = now
		}
		f.topicMaterialization[name] = issue
	}
	if f.topicMaterializationRuns == nil {
		f.topicMaterializationRuns = make(map[string]TopicMaterializationAttempts)
	}
	f.applied = state.Applied
	f.producerState = state.ProducerState
	if f.producerState == nil {
		f.producerState = make(map[string]map[int]map[string]ProducerSequence)
	}

	if f.logs == nil {
		f.logs = make(map[uint64]*ReplicationEntry)
	}
	if f.brokers == nil {
		f.brokers = make(map[string]*BrokerInfo)
	}
	if f.partitionMetadata == nil {
		f.partitionMetadata = make(map[string]*PartitionMetadata)
	}
	if f.notifiers == nil {
		f.notifiers = make(map[string]chan interface{})
	}
	if state.GroupState != nil && f.cd != nil {
		if err := f.cd.ImportState(state.GroupState); err != nil {
			f.mu.Unlock()
			f.materializationMu.Unlock()
			return fmt.Errorf("restore consumer groups: %w", err)
		}
		util.Info("FSM Restore: Restored %d consumer groups from snapshot", len(state.GroupState))
	}

	if state.TransactionState != nil {
		if f.txn != nil {
			if err := f.txn.ImportState(state.TransactionState); err != nil {
				f.mu.Unlock()
				f.materializationMu.Unlock()
				return fmt.Errorf("restore transactions: %w", err)
			}
			util.Info("FSM Restore: Restored %d transactions from snapshot", len(state.TransactionState))
		} else {
			f.restoredTransactionState = state.TransactionState
			util.Info("FSM Restore: Deferred %d transactions until transaction manager is attached", len(state.TransactionState))
		}
	}

	f.mu.Unlock()
	f.materializationMu.Unlock()
	if err := f.ReconcileTopicMaterializations(); err != nil {
		util.Warn("FSM Restore: Topic materialization pending: %v", err)
	}
	if err := f.reconcileCommittedPartitions(); err != nil {
		return err
	}
	util.Info("FSM restore completed: %d logs, %d brokers, %d partitions", len(state.Logs), len(state.Brokers), len(state.PartitionMetadata))
	return nil
}

func (f *BrokerFSM) reconcileCommittedPartitions() error {
	f.mu.RLock()
	metadata := make(map[string]PartitionMetadata, len(f.partitionMetadata))
	for key, value := range f.partitionMetadata {
		if value != nil {
			metadata[key] = *value
		}
	}
	tm := f.tm
	f.mu.RUnlock()
	if tm == nil {
		return nil
	}
	pending := false
	for key, meta := range metadata {
		if !meta.CommittedHWMKnown {
			return fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, key)
		}
		idx := strings.LastIndex(key, "-")
		if idx < 0 {
			continue
		}
		partition, err := strconv.Atoi(key[idx+1:])
		if err != nil {
			continue
		}
		t := tm.GetTopic(key[:idx])
		if t == nil {
			continue
		}
		p, err := t.GetPartition(partition)
		if err == nil {
			if err := p.ReconcileSnapshotHWM(meta.CommittedHWM); err != nil {
				return fmt.Errorf("reconcile committed HWM for %s: %w", key, err)
			}
			pending = pending || p.SnapshotRecoveryPending()
			p.FlushDisk()
		}
	}
	f.mu.Lock()
	f.partitionRecoveryPending = f.partitionRecoveryPending || pending
	f.mu.Unlock()
	return nil
}

// FinalizeRecoveredPartitions performs destructive reconciliation only after
// the replication manager has observed every committed recovery command
// applied to this FSM.
func (f *BrokerFSM) FinalizeRecoveredPartitions() error {
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()

	f.mu.RLock()
	metadata := make(map[string]PartitionMetadata, len(f.partitionMetadata))
	for key, value := range f.partitionMetadata {
		if value != nil {
			metadata[key] = *value
		}
	}
	tm := f.tm
	f.mu.RUnlock()
	if tm == nil {
		f.mu.Lock()
		f.partitionRecoveryPending = false
		f.mu.Unlock()
		return nil
	}
	for key, meta := range metadata {
		if !meta.CommittedHWMKnown {
			return fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, key)
		}
		idx := strings.LastIndex(key, "-")
		if idx < 0 {
			continue
		}
		partitionID, err := strconv.Atoi(key[idx+1:])
		if err != nil {
			continue
		}
		localTopic := tm.GetTopic(key[:idx])
		if localTopic == nil {
			continue
		}
		partition, err := localTopic.GetPartition(partitionID)
		if err != nil {
			continue
		}
		if err := partition.FinalizeSnapshotRecovery(meta.CommittedHWM); err != nil {
			return fmt.Errorf("finalize committed HWM for %s: %w", key, err)
		}
		partition.FlushDisk()
	}
	f.mu.Lock()
	f.partitionRecoveryPending = false
	f.mu.Unlock()
	return nil
}

// ValidateLocalLeaderLogs prevents a broker from serving as the clean leader
// for a partition whose committed range is missing locally. Followers may be
// below HWM because the background catch-up path can repair them.
func (f *BrokerFSM) ValidateLocalLeaderLogs(brokerID string) error {
	if brokerID == "" {
		return nil
	}
	f.mu.RLock()
	metadata := make(map[string]PartitionMetadata)
	for key, value := range f.partitionMetadata {
		if value != nil && value.Leader == brokerID {
			metadata[key] = *value
		}
	}
	tm := f.tm
	f.mu.RUnlock()
	if tm == nil {
		return nil
	}
	for key, meta := range metadata {
		idx := strings.LastIndex(key, "-")
		if idx < 0 {
			continue
		}
		partitionID, err := strconv.Atoi(key[idx+1:])
		if err != nil {
			continue
		}
		localTopic := tm.GetTopic(key[:idx])
		if localTopic == nil {
			continue
		}
		partition, err := localTopic.GetPartition(partitionID)
		if err != nil {
			continue
		}
		if partition.NextOffset() < meta.CommittedHWM {
			return fmt.Errorf("local leader %s is missing committed data for %s: leo=%d hwm=%d", brokerID, key, partition.NextOffset(), meta.CommittedHWM)
		}
	}
	return nil
}

func (f *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()

	f.mu.RLock()
	defer f.mu.RUnlock()

	logsCopy := make(map[uint64]*ReplicationEntry, len(f.logs))
	for k, v := range f.logs {
		entryCopy := *v
		logsCopy[k] = &entryCopy
	}
	brokersCopy := make(map[string]*BrokerInfo, len(f.brokers))
	for k, v := range f.brokers {
		brokerCopy := *v
		brokersCopy[k] = &brokerCopy
	}
	metadataCopy := make(map[string]*PartitionMetadata, len(f.partitionMetadata))
	for k, v := range f.partitionMetadata {
		if !v.CommittedHWMKnown {
			return nil, fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, k)
		}
		metaCopy := *v
		if v.Replicas != nil {
			metaCopy.Replicas = make([]string, len(v.Replicas))
			copy(metaCopy.Replicas, v.Replicas)
		}
		if v.ISR != nil {
			metaCopy.ISR = make([]string, len(v.ISR))
			copy(metaCopy.ISR, v.ISR)
		}
		metadataCopy[k] = &metaCopy
	}
	producerStateCopy := make(map[string]map[int]map[string]ProducerSequence, len(f.producerState))
	for topic, partitions := range f.producerState {
		partitionMap := make(map[int]map[string]ProducerSequence, len(partitions))
		for pID, producers := range partitions {
			producerMap := make(map[string]ProducerSequence, len(producers))
			for prodID, seq := range producers {
				producerMap[prodID] = seq
			}
			partitionMap[pID] = producerMap
		}
		producerStateCopy[topic] = partitionMap
	}

	topicStateCopy := copyTopicState(f.topicState)
	if len(topicStateCopy) == 0 && len(metadataCopy) > 0 {
		return nil, fmt.Errorf("snapshot version %d requires durable topic state", SnapshotVersionCurrent)
	}
	definitions, err := validateTopicState(topicStateCopy, metadataCopy)
	if err != nil {
		return nil, fmt.Errorf("snapshot topic definitions: %w", err)
	}
	topicStateCopy = topicStateFromDefinitions(definitions)

	var groupState map[string]*coordinator.GroupStateSnapshot
	if f.cd != nil {
		groupState = f.cd.ExportState()
	}
	var transactionState map[string]*transaction.Snapshot
	if f.txn != nil {
		transactionState = f.txn.ExportState()
	}

	util.Debug("Creating FSM snapshot")
	return &BrokerFSMSnapshot{
		applied:           f.applied,
		logs:              logsCopy,
		brokers:           brokersCopy,
		partitionMetadata: metadataCopy,
		producerState:     producerStateCopy,
		groupState:        groupState,
		transactionState:  transactionState,
		topicState:        topicStateCopy,
	}, nil
}

func (f *BrokerFSM) GetPartitionMetadata(key string) *PartitionMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if meta := f.partitionMetadata[key]; meta != nil {
		copy := *meta
		// Deep copy slices to avoid aliasing internal FSM state
		if meta.Replicas != nil {
			copy.Replicas = make([]string, len(meta.Replicas))
			for i, r := range meta.Replicas {
				copy.Replicas[i] = r
			}
		}
		if meta.ISR != nil {
			copy.ISR = make([]string, len(meta.ISR))
			for i, r := range meta.ISR {
				copy.ISR[i] = r
			}
		}
		return &copy
	}
	return nil
}

func (f *BrokerFSM) GetBroker(id string) *BrokerInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if broker, ok := f.brokers[id]; ok {
		copy := *broker
		return &copy
	}
	return nil
}

func (f *BrokerFSM) RegisterNotifier(reqID string) chan interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	ch := make(chan interface{}, 1)
	f.notifiers[reqID] = ch
	return ch
}

func (f *BrokerFSM) UnregisterNotifier(reqID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.notifiers, reqID)
}

func (f *BrokerFSM) notify(reqID string, res interface{}) {
	f.mu.RLock()
	ch, ok := f.notifiers[reqID]
	f.mu.RUnlock()

	if ok {
		select {
		case ch <- res:
		default:
		}
	}
}
