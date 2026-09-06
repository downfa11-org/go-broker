package fsm

import (
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestLogReplayPreservesDurableRecordsUntilFinalWatermark(t *testing.T) {
	manager, partition := newDurableFSMTopic(t, "replayed-orders")
	require.NoError(t, partition.EnqueueBatchLeader([]types.Message{
		{Payload: "first"}, {Payload: "second"}, {Payload: "uncommitted"},
	}))
	partition.SetHWM(2)
	partition.FlushDisk()

	f := NewBrokerFSM(manager, nil)
	f.BeginPartitionRecovery()
	registerActiveBroker(t, f, "broker-1")
	definition := manager.GetTopic("replayed-orders").Definition()
	create, err := json.Marshal(TopicCommand{Definition: &definition, LeaderID: "broker-1"})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Index: 2, Data: append([]byte("TOPIC:"), create...)}))
	require.Equal(t, uint64(3), partition.NextOffset(), "historical creation must not truncate recovered data")
	require.True(t, partition.SnapshotRecoveryPending())

	for hwm := uint64(1); hwm <= 2; hwm++ {
		commit, err := json.Marshal(partitionCommitCommand{
			Topic: definition.Name, Leader: "broker-1", LeaderEpoch: 1,
			HWM: hwm, LifecycleEpoch: definition.LifecycleEpoch,
		})
		require.NoError(t, err)
		require.Nil(t, f.Apply(&raft.Log{Index: 2*hwm + 1, Data: append([]byte("PARTITION_COMMIT:"), commit...)}))
		require.Nil(t, f.Apply(&raft.Log{Index: 2*hwm + 2, Data: append([]byte("TOPIC:"), create...)}))
		require.Equal(t, uint64(3), partition.NextOffset())
	}
	require.NoError(t, f.FinalizeRecoveredPartitions())
	require.False(t, f.HasPendingPartitionRecovery())
	require.False(t, partition.SnapshotRecoveryPending())
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, uint64(2), partition.GetHWM())
	messages, err := partition.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, "first", messages[0].Payload)
	require.Equal(t, "second", messages[1].Payload)
}
