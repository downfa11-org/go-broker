package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBrokerFSMSnapshotRestoresTopicDefinition(t *testing.T) {
	f := newTestFSM()
	registerActiveBroker(t, f, "broker-1")
	policy := topic.Policy{
		CleanupPolicy:  "delete",
		Partitioner:    topic.PartitionerRoundRobin,
		AuthPolicy:     topic.AuthPolicyACL,
		ReadACL:        []string{"reader"},
		WriteACL:       []string{"writer"},
		RetentionHours: 48,
		RetentionBytes: 8192,
	}
	minISR := 1
	policy.MinInSyncReplicas = &minISR
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 2
	definition.Idempotent = true
	definition.ReplicationFactor = 1
	definition.Policy = policy
	command := TopicCommand{Definition: &definition}
	data, err := json.Marshal(command)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(data)), Index: 2}))

	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buffer}))
	var persisted BrokerFSMState
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &persisted))
	require.Equal(t, SnapshotVersionCurrent, persisted.Version)

	restored := newTestFSM()
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	restoredTopic := restored.tm.GetTopic("orders")
	require.NotNil(t, restoredTopic)
	require.Len(t, restoredTopic.Partitions, 2)
	require.True(t, restoredTopic.IsIdempotent)
	require.False(t, restoredTopic.IsEventSourcing)
	require.Equal(t, topic.PartitionerRoundRobin, restoredTopic.Policy.Partitioner)
	require.Equal(t, topic.AuthPolicyACL, restoredTopic.Policy.AuthPolicy)
	require.Equal(t, []string{"reader"}, restoredTopic.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, restoredTopic.Policy.WriteACL)
	require.Equal(t, 48, restoredTopic.Policy.RetentionHours)
	require.Equal(t, int64(8192), restoredTopic.Policy.RetentionBytes)
	require.NotNil(t, restoredTopic.Policy.MinInSyncReplicas)
	require.Equal(t, 1, *restoredTopic.Policy.MinInSyncReplicas)
	require.Equal(t, 1, restoredTopic.ReplicationFactor)
	require.Equal(t, uint64(1), restoredTopic.Revision)
	require.Equal(t, uint64(topic.InitialLifecycleEpoch), restoredTopic.LifecycleEpoch)
}

func TestBrokerFSMAllowsCompactedTopicWhenAllBrokersSupportProtocol(t *testing.T) {
	f := newTestFSM()
	for _, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
		payload, err := json.Marshal(BrokerInfo{
			ID: brokerID, Addr: "127.0.0.1:9000", Status: "active",
			LifecycleProtocol: BrokerProtocolVersionCurrent,
		})
		require.NoError(t, err)
		require.Nil(t, f.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...), Index: 1}))
	}
	command := testTopicCommand("state", 1, 3)
	command.Definition.Policy.CleanupPolicy = config.CleanupPolicyCompact
	payload, err := json.Marshal(command)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...), Index: 4}))
	require.Equal(t, config.CleanupPolicyCompact, f.tm.GetTopic("state").Policy.CleanupPolicy)
}

func TestBrokerFSMRejectsCompactedTopicDuringMixedVersionRollout(t *testing.T) {
	f := newTestFSM()
	for index, protocolVersion := range []int{BrokerProtocolVersionCurrent, DistributedCompactionProtocolVersion - 1} {
		payload, err := json.Marshal(BrokerInfo{
			ID: fmt.Sprintf("broker-%d", index+1), Addr: "127.0.0.1:9000", Status: "active",
			LifecycleProtocol: protocolVersion,
		})
		require.NoError(t, err)
		require.Nil(t, f.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...), Index: uint64(index + 1)}))
	}
	command := testTopicCommand("state", 1, 2)
	command.Definition.Policy.CleanupPolicy = config.CleanupPolicyCompact
	payload, err := json.Marshal(command)
	require.NoError(t, err)
	result := f.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...), Index: 3})
	require.ErrorContains(t, result.(error), "requires broker protocol")
	require.Nil(t, f.tm.GetTopic("state"))
}

func TestBrokerFSMSnapshotRestoresAlteredTopicMinInSyncReplicas(t *testing.T) {
	f := newTestFSM()
	for _, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
		registerActiveBroker(t, f, brokerID)
	}
	create, err := json.Marshal(testTopicCommand("orders", 1, 3))
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 4}))
	configured := 2
	alter, err := json.Marshal(TopicConfigCommand{Name: "orders", MinInSyncReplicas: &configured})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC_CONFIG:" + string(alter)), Index: 5}))

	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buffer}))
	restored := newTestFSM()
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	restoredTopic := restored.tm.GetTopic("orders")
	require.NotNil(t, restoredTopic.Policy.MinInSyncReplicas)
	require.Equal(t, 2, *restoredTopic.Policy.MinInSyncReplicas)
}

func TestBrokerFSMRestoreRejectsLegacySnapshotBeforeReconciliation(t *testing.T) {
	manager, partition := newDurableFSMTopic(t, "legacy-orders")
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "durable"}))
	partition.FlushDisk()

	err := NewBrokerFSM(manager, nil).Restore(io.NopCloser(bytes.NewBufferString(`{"version":8}`)))
	require.ErrorIs(t, err, ErrUnsupportedRecoveryProtocol)
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
}

func TestBrokerFSMRestoreTruncatesTailBeyondExplicitCommittedHWMZero(t *testing.T) {
	manager, partition := newDurableFSMTopic(t, "current-orders")
	uncommitted := []types.Message{{Payload: "uncommitted"}}
	require.NoError(t, partition.EnqueueBatchLeader(uncommitted))
	partition.FlushDisk()
	require.Equal(t, uint64(1), partition.NextOffset())

	definition := manager.GetTopic("current-orders").Definition()
	state := BrokerFSMState{
		Version:    SnapshotVersionCurrent,
		TopicState: map[string]*topic.Definition{"current-orders": &definition},
		PartitionMetadata: map[string]*PartitionMetadata{
			"current-orders-0": {
				Leader: "broker-1", LeaderEpoch: 7, LifecycleEpoch: definition.LifecycleEpoch,
				CommittedHWMKnown: true, PartitionCount: 1,
				Replicas: []string{"broker-1"}, ISR: []string{"broker-1"},
			},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)
	require.Contains(t, string(data), `"committed_hwm":0`)

	restored := NewBrokerFSM(manager, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(data))))
	require.Equal(t, uint64(1), partition.NextOffset(), "restore stages the tail until replay is finalized")
	require.NoError(t, restored.FinalizeRecoveredPartitions())
	require.Zero(t, partition.NextOffset())
	require.Zero(t, partition.GetHWM())
	messages, err := partition.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Empty(t, messages)
}

func TestBrokerFSMRestoreTruncatesTailToAuthoritativeCommittedHWM(t *testing.T) {
	manager, partition := newDurableFSMTopic(t, "bounded-orders")
	require.NoError(t, partition.EnqueueBatchLeader([]types.Message{
		{Payload: "committed"},
		{Payload: "uncommitted"},
	}))
	partition.FlushDisk()

	data := currentSnapshotData(t, manager, "bounded-orders", 1)
	restored := NewBrokerFSM(manager, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(data))))
	require.Equal(t, uint64(2), partition.NextOffset(), "restore must not truncate before Raft replay completes")
	require.NoError(t, restored.FinalizeRecoveredPartitions())
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
	messages, err := partition.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "committed", messages[0].Payload)
}

func TestBrokerFSMRestorePreservesLocallyCommittedPostSnapshotTail(t *testing.T) {
	manager, partition := newDurableFSMTopic(t, "post-snapshot-orders")
	require.NoError(t, partition.EnqueueBatchLeader([]types.Message{
		{Payload: "in-snapshot"},
		{Payload: "committed-after-snapshot"},
		{Payload: "uncommitted-tail"},
	}))
	partition.SetHWM(2)
	partition.FlushDisk()

	data := currentSnapshotData(t, manager, "post-snapshot-orders", 1)
	restored := NewBrokerFSM(manager, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(data))))
	require.Equal(t, uint64(3), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM(), "post-snapshot data stays invisible before replay")
	definition := manager.GetTopic("post-snapshot-orders").Definition()
	commit, err := json.Marshal(partitionCommitCommand{
		Topic: "post-snapshot-orders", Partition: 0, Leader: "broker-1", LeaderEpoch: 7,
		HWM: 2, LifecycleEpoch: definition.LifecycleEpoch,
	})
	require.NoError(t, err)
	require.Nil(t, restored.Apply(&raft.Log{Data: append([]byte("PARTITION_COMMIT:"), commit...), Index: 2}))
	require.Equal(t, uint64(2), partition.GetHWM())
	require.Equal(t, uint64(3), partition.NextOffset())
	require.NoError(t, restored.FinalizeRecoveredPartitions())
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, uint64(2), partition.GetHWM())
	messages, err := partition.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, "in-snapshot", messages[0].Payload)
	require.Equal(t, "committed-after-snapshot", messages[1].Payload)
}

func TestBrokerFSMRestoreLeavesReplicaBelowCommittedHWMForCatchup(t *testing.T) {
	manager, partition := newDurableFSMTopic(t, "behind-orders")
	data := currentSnapshotData(t, manager, "behind-orders", 1)

	restored := NewBrokerFSM(manager, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(data))))
	require.True(t, restored.HasPendingPartitionRecovery())
	require.NoError(t, restored.FinalizeRecoveredPartitions())
	require.Zero(t, partition.NextOffset())
	require.Zero(t, partition.GetHWM())
	require.ErrorContains(t, restored.ValidateLocalLeaderLogs("broker-1"), "missing committed data")
	require.NoError(t, restored.ValidateLocalLeaderLogs("broker-2"), "a follower may start and fetch the missing committed range")
}

func currentSnapshotData(t *testing.T, manager *topic.TopicManager, name string, committedHWM uint64) []byte {
	t.Helper()
	definition := manager.GetTopic(name).Definition()
	state := BrokerFSMState{
		Version:    SnapshotVersionCurrent,
		TopicState: map[string]*topic.Definition{name: &definition},
		PartitionMetadata: map[string]*PartitionMetadata{
			name + "-0": {
				Leader: "broker-1", LeaderEpoch: 7, LifecycleEpoch: definition.LifecycleEpoch,
				CommittedHWM: committedHWM, CommittedHWMKnown: true, PartitionCount: 1,
				Replicas: []string{"broker-1"}, ISR: []string{"broker-1"},
			},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)
	return data
}

func newDurableFSMTopic(t *testing.T, name string) (*topic.TopicManager, *topic.Partition) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, manager.CreateTopic(name, 1, false, false))
	partition, err := manager.GetTopic(name).GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(partition.Close)
	return manager, partition
}

func TestBrokerFSMPatchCommandsMergeAgainstSerializedAuthoritativeState(t *testing.T) {
	f := newTestFSM()
	registerActiveBroker(t, f, "broker-1")
	defaults := topic.DefaultDefinition("orders", nil)
	partitions := 1
	retentionHours := 24
	readACL := []string{"reader"}
	initialPatch := topic.DefinitionPatch{
		Partitions:     &partitions,
		RetentionHours: &retentionHours,
		ReadACL:        &readACL,
	}
	applyTopicPatch(t, f, 2, defaults, initialPatch)

	retentionBytes := int64(8192)
	applyTopicPatch(t, f, 3, defaults, topic.DefinitionPatch{RetentionBytes: &retentionBytes})
	writeACL := []string{"writer"}
	applyTopicPatch(t, f, 4, defaults, topic.DefinitionPatch{WriteACL: &writeACL})

	definition := f.tm.GetTopic("orders").Definition()
	require.Equal(t, uint64(3), definition.Revision)
	require.Equal(t, topic.DefaultReplicationFactor, definition.ReplicationFactor)
	require.Equal(t, 24, definition.Policy.RetentionHours)
	require.Equal(t, int64(8192), definition.Policy.RetentionBytes)
	require.Equal(t, []string{"reader"}, definition.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, definition.Policy.WriteACL)
	require.Len(t, f.GetPartitionMetadata("orders-0").Replicas, 1)

	applyTopicPatch(t, f, 5, defaults, topic.DefinitionPatch{})
	require.Equal(t, uint64(3), f.tm.GetTopic("orders").Definition().Revision)

	retentionHours = 0
	readACL = []string{}
	applyTopicPatch(t, f, 6, defaults, topic.DefinitionPatch{RetentionHours: &retentionHours, ReadACL: &readACL})
	definition = f.tm.GetTopic("orders").Definition()
	require.Equal(t, uint64(4), definition.Revision)
	require.Zero(t, definition.Policy.RetentionHours)
	require.Empty(t, definition.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, definition.Policy.WriteACL)
}

func applyTopicPatch(t *testing.T, f *BrokerFSM, index uint64, defaults topic.Definition, patch topic.DefinitionPatch) {
	t.Helper()
	payload, err := json.Marshal(TopicCommand{Definition: &defaults, Patch: &patch})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(payload)), Index: index}))
}

func TestBrokerFSMRepeatedCreatePreservesExistingPartitionState(t *testing.T) {
	f := newTestFSM()
	registerActiveBroker(t, f, "broker-1")
	initial := testTopicCommand("orders", 1, 1)
	data, err := json.Marshal(initial)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(data)), Index: 2}))

	f.mu.Lock()
	f.partitionMetadata["orders-0"].CommittedHWM = 41
	f.partitionMetadata["orders-0"].LeaderEpoch = 7
	f.mu.Unlock()
	partition, err := f.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.UpdateLEO(41)

	updated := initial
	partitions := 2
	retentionHours := 24
	updated.Patch = &topic.DefinitionPatch{Partitions: &partitions, RetentionHours: &retentionHours}
	data, err = json.Marshal(updated)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(data)), Index: 3}))

	existing := f.GetPartitionMetadata("orders-0")
	require.Equal(t, uint64(41), existing.CommittedHWM)
	require.Equal(t, 7, existing.LeaderEpoch)
	require.Equal(t, 2, existing.PartitionCount)
	require.NotNil(t, f.GetPartitionMetadata("orders-1"))
	require.Equal(t, 24, f.tm.GetTopic("orders").Policy.RetentionHours)
}

func TestBrokerFSMRestoreRejectsLegacyPartitionMetadataSnapshot(t *testing.T) {
	for _, version := range []int{5, 7, 8} {
		data := []byte(fmt.Sprintf(`{"version":%d}`, version))
		err := newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
		require.ErrorIs(t, err, ErrUnsupportedRecoveryProtocol)
	}
}

func TestBrokerFSMRestoreVersionNineRequiresDefinitionFields(t *testing.T) {
	for _, missing := range []string{"revision", "replication_factor", "lifecycle_epoch"} {
		t.Run(missing, func(t *testing.T) {
			definition := &topic.Definition{
				Name: "orders", Revision: 1, LifecycleEpoch: topic.InitialLifecycleEpoch,
				Partitions: 1, ReplicationFactor: 3, Policy: topic.DefaultPolicy(),
			}
			switch missing {
			case "revision":
				definition.Revision = 0
			case "replication_factor":
				definition.ReplicationFactor = 0
			default:
				definition.LifecycleEpoch = 0
			}
			state := BrokerFSMState{
				Version:    SnapshotVersionCurrent,
				TopicState: map[string]*topic.Definition{"orders": definition},
				PartitionMetadata: map[string]*PartitionMetadata{
					"orders-0": authoritativePartitionMetadata(1),
				},
			}
			data, err := json.Marshal(state)
			require.NoError(t, err)

			err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
			require.ErrorContains(t, err, "missing "+missing)
		})
	}
}

func TestBrokerFSMRestoreRejectsVersionNineWithoutTopicState(t *testing.T) {
	state := BrokerFSMState{
		Version: SnapshotVersionCurrent,
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": authoritativePartitionMetadata(1),
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "missing topic state")
}

func TestBrokerFSMRestoreRejectsMissingPartitionMetadata(t *testing.T) {
	state := BrokerFSMState{
		Version: SnapshotVersionCurrent,
		TopicState: map[string]*topic.Definition{
			"orders": snapshotTopicDefinition("orders", 2),
		},
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": authoritativePartitionMetadata(2),
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "missing partition metadata 1")
}

func TestBrokerFSMRestoreRejectsPartitionWithoutTopicDefinition(t *testing.T) {
	state := BrokerFSMState{
		Version: SnapshotVersionCurrent,
		TopicState: map[string]*topic.Definition{
			"orders": snapshotTopicDefinition("orders", 1),
		},
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": authoritativePartitionMetadata(1),
			"audit-0":  authoritativePartitionMetadata(1),
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "has no topic definition")
}

func registerActiveBroker(t *testing.T, f *BrokerFSM, id string) {
	t.Helper()
	payload, err := json.Marshal(BrokerInfo{ID: id, Addr: "127.0.0.1:9000", Status: "active"})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", payload)), Index: 1}))
}
