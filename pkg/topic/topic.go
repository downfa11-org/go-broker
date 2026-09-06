package topic

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/config"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const DefaultBufSize = 10000
const DefaultConsumerBufSize = 1000

// Topic represents a logical message stream divided into partitions and consumer groups.
type Topic struct {
	Name              string
	Partitions        []*Partition
	counter           uint64
	consumerGroups    map[string]*types.ConsumerGroup
	mu                sync.RWMutex
	cfg               *config.Config
	streamManager     StreamManager
	IsIdempotent      bool
	IsEventSourcing   bool
	ReplicationFactor int
	Revision          uint64
	LifecycleEpoch    uint64
	Policy            Policy
	txnResolver       TransactionDecisionResolver
	compactionGate    func(topic string, partition int) (bool, string)
}

func (t *Topic) SetTransactionDecisionResolver(resolver TransactionDecisionResolver) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.txnResolver = resolver
	for _, partition := range t.Partitions {
		partition.SetTransactionDecisionResolver(resolver)
	}
}

func (t *Topic) SetDistributedCompactionGate(gate func(topic string, partition int) (bool, string)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.compactionGate = gate
	for _, partition := range t.Partitions {
		partition.SetDistributedCompactionGate(gate)
	}
}

type storagePolicySetter interface {
	SetStoragePolicy(cleanupPolicy string, hours int, bytes int64)
}

type retentionPolicySetter interface {
	SetRetentionPolicy(hours int, bytes int64)
}

type cleanupPolicySetter interface {
	SetCleanupPolicy(policy string)
}

type policyHandlerProvider interface {
	GetHandlerWithPolicy(topic string, partitionID int, cleanupPolicy string, retentionHours int, retentionBytes int64) (types.StorageHandler, error)
}

type partitionHandlerCloser interface {
	ClosePartitionHandler(topic string, partitionID int)
}

func getHandlerWithStoragePolicy(provider HandlerProvider, topic string, partitionID int, policy Policy) (types.StorageHandler, error) {
	if policyProvider, ok := provider.(policyHandlerProvider); ok {
		return policyProvider.GetHandlerWithPolicy(topic, partitionID, policy.CleanupPolicy, policy.RetentionHours, policy.RetentionBytes)
	}
	handler, err := provider.GetHandler(topic, partitionID)
	if err != nil {
		return nil, err
	}
	applyStoragePolicy(handler, policy)
	return handler, nil
}

func applyStoragePolicy(handler types.StorageHandler, policy Policy) {
	if setter, ok := handler.(storagePolicySetter); ok {
		setter.SetStoragePolicy(policy.CleanupPolicy, policy.RetentionHours, policy.RetentionBytes)
		return
	}
	if setter, ok := handler.(retentionPolicySetter); ok {
		setter.SetRetentionPolicy(policy.RetentionHours, policy.RetentionBytes)
	}
	if setter, ok := handler.(cleanupPolicySetter); ok {
		setter.SetCleanupPolicy(policy.CleanupPolicy)
	}
}

// NewTopic initializes a topic with partitions.
func NewTopic(name string, partitionCount int, hp HandlerProvider, cfg *config.Config, sm StreamManager, idempotent bool, eventSourcing bool) (*Topic, error) {
	policy := DefaultPolicy()
	if cfg != nil && cfg.CleanupPolicy != "" {
		policy.CleanupPolicy = cfg.CleanupPolicy
	}
	return NewTopicWithPolicy(name, partitionCount, hp, cfg, sm, idempotent, eventSourcing, policy)
}

func NewTopicWithPolicy(name string, partitionCount int, hp HandlerProvider, cfg *config.Config, sm StreamManager, idempotent bool, eventSourcing bool, policy Policy) (*Topic, error) {
	definition := DefaultDefinition(name, cfg)
	definition.Partitions = partitionCount
	definition.Idempotent = idempotent
	definition.EventSourcing = eventSourcing
	definition.Policy = policy
	return newTopicWithDefinition(definition, hp, cfg, sm)
}

func newTopicWithDefinition(definition Definition, hp HandlerProvider, cfg *config.Config, sm StreamManager) (*Topic, error) {
	definition, err := definition.Normalize()
	if err != nil {
		return nil, err
	}
	if definition.Name != config.ConsumerOffsetsTopicName {
		if err := validateCleanupPolicyForTopic(definition.Policy, cfg, definition.EventSourcing); err != nil {
			return nil, err
		}
	}

	partitions := make([]*Partition, definition.Partitions)
	for i := 0; i < definition.Partitions; i++ {
		dh, err := getHandlerWithStoragePolicy(hp, definition.Name, i, storagePolicyForTopic(definition.Policy, definition.EventSourcing))
		if err != nil {
			closePartiallyInitializedTopic(definition.Name, hp, partitions[:i])
			return nil, fmt.Errorf("open handler for %s[%d]: %w", definition.Name, i, err)
		}
		p := NewPartition(i, definition.Name, dh, sm, cfg)
		p.isIdempotent = definition.Idempotent
		p.RecoverProducerStateFromLog()
		p.StartProducerStateMaintenance()
		partitions[i] = p
	}
	return &Topic{
		Name:              definition.Name,
		Partitions:        partitions,
		consumerGroups:    make(map[string]*types.ConsumerGroup),
		cfg:               cfg,
		streamManager:     sm,
		IsIdempotent:      definition.Idempotent,
		IsEventSourcing:   definition.EventSourcing,
		ReplicationFactor: definition.ReplicationFactor,
		Revision:          definition.Revision,
		LifecycleEpoch:    definition.LifecycleEpoch,
		Policy:            definition.Policy,
	}, nil
}

func closePartiallyInitializedTopic(name string, provider HandlerProvider, partitions []*Partition) {
	for _, partition := range partitions {
		partition.Close()
	}
	if closer, ok := provider.(topicHandlerCloser); ok {
		closer.CloseTopicHandlers(name)
		return
	}
	for _, partition := range partitions {
		if err := partition.dh.Close(); err != nil {
			util.Warn("Failed to close storage handler for partially initialized topic %s[%d]: %v", name, partition.ID, err)
		}
	}
}

// getPartitionIndex computes the target partition index without acquiring any lock.
// The caller must hold at least RLock and pass the current partition count.
func (t *Topic) getPartitionIndex(msg types.Message, partitionsLen int) int {
	partitionCount, ok := util.SafeIntToUint64(partitionsLen)
	if !ok || partitionCount == 0 {
		return -1
	}

	var candidate uint64
	if t.Policy.Partitioner == PartitionerHashKey && msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		candidate = keyID % partitionCount
	} else {
		oldCounter := atomic.AddUint64(&t.counter, 1) - 1
		candidate = oldCounter % partitionCount
	}
	partitionIndex, ok := util.SafeUint64ToInt(candidate)
	if !ok {
		return -1
	}
	return partitionIndex
}

// GetPartitionForMessage returns the partition index for a message.
// This is intended for external callers (e.g. TopicManager). Internal publish
// methods use getPartitionIndex under an already-held RLock to avoid TOCTOU races.
func (t *Topic) GetPartitionForMessage(msg types.Message) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.getPartitionIndex(msg, len(t.Partitions))
}

// AddPartitions atomically extends the topic with new partitions.
// A staging failure closes every newly prepared partition and leaves the
// visible partition count unchanged.
func (t *Topic) AddPartitions(extra int, hp HandlerProvider) error {
	if extra < 0 {
		return fmt.Errorf("partition increment must be >= 0")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	partitions := len(t.Partitions) + extra
	definition, err := MergeDefinitionPatch(t.definitionLocked(), DefinitionPatch{Partitions: &partitions}, true)
	if err != nil {
		return err
	}
	return t.applyFullDefinitionLocked(definition, hp, nil)
}

// Definition returns a detached durable definition for the topic.
func (t *Topic) Definition() Definition {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.definitionLocked()
}

// PolicySnapshot returns a detached, concurrency-safe view of the current
// topic policy.
func (t *Topic) PolicySnapshot() Policy {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Policy.Clone()
}

func (t *Topic) definitionLocked() Definition {
	policy := t.Policy.Clone()
	return Definition{
		Name:              t.Name,
		Revision:          t.Revision,
		LifecycleEpoch:    t.LifecycleEpoch,
		Partitions:        len(t.Partitions),
		ReplicationFactor: t.ReplicationFactor,
		Idempotent:        t.IsIdempotent,
		EventSourcing:     t.IsEventSourcing,
		Policy:            policy,
	}
}

func (t *Topic) applyFullDefinition(definition Definition, hp HandlerProvider, persist func(Definition) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.applyFullDefinitionLocked(definition, hp, persist)
}

func (t *Topic) applyFullDefinitionLocked(definition Definition, hp HandlerProvider, persist func(Definition) error) error {
	definition, err := definition.Normalize()
	if err != nil {
		return err
	}
	if definition.Name != t.Name {
		return fmt.Errorf("topic definition name mismatch: current=%q requested=%q", t.Name, definition.Name)
	}
	if definition.Idempotent != t.IsIdempotent {
		return fmt.Errorf("idempotent mode is immutable for existing topic %q", t.Name)
	}
	if definition.EventSourcing != t.IsEventSourcing {
		return fmt.Errorf("event_sourcing mode is immutable for existing topic %q", t.Name)
	}
	if definition.LifecycleEpoch != t.LifecycleEpoch {
		return fmt.Errorf("lifecycle epoch is immutable outside truncate for topic %q: current=%d requested=%d", t.Name, t.LifecycleEpoch, definition.LifecycleEpoch)
	}
	partitionCount := definition.Partitions
	policy := definition.Policy
	current := len(t.Partitions)
	if partitionCount < current {
		return fmt.Errorf("cannot decrease partition count for topic '%s': %d -> %d", t.Name, current, partitionCount)
	}

	staged := make([]*Partition, 0, partitionCount-current)
	for idx := current; idx < partitionCount; idx++ {
		dh, err := getHandlerWithStoragePolicy(hp, t.Name, idx, storagePolicyForTopic(policy, t.IsEventSourcing))
		if err != nil {
			closePreparedPartitions(t.Name, hp, staged)
			return fmt.Errorf("failed to attach partition %d for topic '%s': %w", idx, t.Name, err)
		}
		partition := NewPartition(idx, t.Name, dh, t.streamManager, t.cfg)
		partition.SetTransactionDecisionResolver(t.txnResolver)
		partition.SetDistributedCompactionGate(t.compactionGate)
		partition.isIdempotent = t.IsIdempotent
		partition.RecoverProducerStateFromLog()
		partition.StartProducerStateMaintenance()
		staged = append(staged, partition)
	}

	if persist != nil {
		if err := persist(definition); err != nil {
			closePreparedPartitions(t.Name, hp, staged)
			return err
		}
	}

	for _, partition := range t.Partitions {
		applyStoragePolicy(partition.dh, storagePolicyForTopic(policy, t.IsEventSourcing))
	}
	t.Policy = policy
	t.ReplicationFactor = definition.ReplicationFactor
	t.Revision = definition.Revision
	t.LifecycleEpoch = definition.LifecycleEpoch
	t.Partitions = append(t.Partitions, staged...)
	return nil
}

func closePreparedPartitions(topicName string, provider HandlerProvider, partitions []*Partition) {
	for _, partition := range partitions {
		partition.Close()
	}
	if closer, ok := provider.(partitionHandlerCloser); ok {
		for _, partition := range partitions {
			closer.ClosePartitionHandler(topicName, partition.ID())
		}
		return
	}
	for _, partition := range partitions {
		if err := partition.dh.Close(); err != nil {
			util.Warn("Failed to close staged storage handler for %s[%d]: %v", topicName, partition.ID(), err)
		}
	}
}

func (t *Topic) ApplyPolicy(policy Policy) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Policy = policy
	for _, partition := range t.Partitions {
		applyStoragePolicy(partition.dh, storagePolicyForTopic(policy, t.IsEventSourcing))
	}
}

// RegisterConsumerGroup registers a consumer group to the topic.
func (t *Topic) RegisterConsumerGroup(groupName string, consumerCount int) *types.ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	if g, ok := t.consumerGroups[groupName]; ok {
		return g
	}

	group := &types.ConsumerGroup{
		Name:      groupName,
		Consumers: make([]*types.Consumer, consumerCount),
	}

	for i := 0; i < consumerCount; i++ {
		group.Consumers[i] = &types.Consumer{
			ID: i,
		}
	}

	t.consumerGroups[groupName] = group
	return group
}

// DeregisterConsumerGroup removes a consumer group from the topic.
func (t *Topic) DeregisterConsumerGroup(groupName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.consumerGroups[groupName]; !ok {
		return fmt.Errorf("consumer group '%s' does not exist", groupName)
	}

	delete(t.consumerGroups, groupName)
	util.Info("Consumer group '%s' deregistered from topic '%s'", groupName, t.Name)
	return nil
}

// Publish sends a message to one partition.
// Partition selection and enqueue happen under a single RLock to prevent
// TOCTOU races with AddPartitions.
func (t *Topic) Publish(msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx := t.getPartitionIndex(msg, len(t.Partitions))
	if idx == -1 {
		return fmt.Errorf("no partitions available for topic '%s'", t.Name)
	}

	return t.Partitions[idx].Enqueue(msg)
}

func (t *Topic) PublishToPartition(partition int, msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partition, t.Name, len(t.Partitions)-1)
	}

	return t.Partitions[partition].Enqueue(msg)
}

// PublishSync sends a message synchronously to one partition.
// Partition selection and enqueue happen under a single RLock to prevent
// TOCTOU races with AddPartitions.
func (t *Topic) PublishSync(msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx := t.getPartitionIndex(msg, len(t.Partitions))
	if idx == -1 {
		return fmt.Errorf("no partitions available for topic '%s'", t.Name)
	}

	return t.Partitions[idx].EnqueueSync(msg)
}

func (t *Topic) PublishToPartitionSync(partition int, msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partition, t.Name, len(t.Partitions)-1)
	}

	return t.Partitions[partition].EnqueueSync(msg)
}
func (t *Topic) PublishToPartitionSyncIdempotent(partition int, msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partition, t.Name, len(t.Partitions)-1)
	}

	return t.Partitions[partition].EnqueueSyncIdempotent(msg)
}

// PublishBatchSync sends a batch of messages synchronously, grouping by partition.
// Partition selection and enqueue happen under a single RLock to prevent
// TOCTOU races with AddPartitions.
func (t *Topic) PublishBatchSync(msgs []types.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	partitionsLen := len(t.Partitions)
	partitioned := make(map[int][]types.Message)
	for _, msg := range msgs {
		idx := t.getPartitionIndex(msg, partitionsLen)
		if idx != -1 {
			partitioned[idx] = append(partitioned[idx], msg)
		}
	}

	for idx, pm := range partitioned {
		if err := t.Partitions[idx].EnqueueBatchSync(pm); err != nil {
			return fmt.Errorf("partition %d: failed to publish batch: %w", idx, err)
		}
	}
	return nil
}

func (t *Topic) GetPartition(partitionID int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partitionID < 0 || partitionID >= len(t.Partitions) {
		return nil, fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partitionID, t.Name, len(t.Partitions)-1)
	}

	return t.Partitions[partitionID], nil
}

func (t *Topic) ReadSafeMessages(partitionID int, offset uint64, max int) ([]types.Message, error) {
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}
	return p.ReadCommitted(offset, max)
}

// applyAssignments connects partitions to consumers according to coordinator results.
func (t *Topic) applyAssignments(groupName string, assignments map[string][]int) {
	group := t.consumerGroups[groupName]
	if group == nil {
		return
	}

	util.Debug("Applied assignments for group '%s': %v", groupName, assignments)
}
