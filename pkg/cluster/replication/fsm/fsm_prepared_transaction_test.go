package fsm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func preparedTransactionFSM(t *testing.T) (*BrokerFSM, *transaction.Snapshot) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cd := coordinator.NewCoordinator(context.Background(), cfg, &recoverableLifecyclePublisher{})
	t.Cleanup(cd.Stop)
	require.NoError(t, cd.RegisterGroup("events", "workers", 1))
	_, err := cd.AddConsumer("workers", "member")
	require.NoError(t, err)
	txn := transaction.NewManager()
	producer, epoch, err := txn.InitProducer("tx")
	require.NoError(t, err)
	require.NoError(t, txn.Begin("tx", producer, epoch))
	require.NoError(t, txn.AddOffsets("tx", producer, epoch, []transaction.OffsetOperation{{
		Topic: "events", Group: "workers", Member: "member", Generation: cd.GetGeneration("workers"), Partition: 0, Offset: 13,
	}}))
	snap, err := txn.BuildPreparedSnapshot("tx", producer, epoch)
	require.NoError(t, err)
	snap.Offsets[0].RegistrationEpoch = cd.GetGroup("workers").RegistrationEpoch
	f := NewBrokerFSM(nil, cd)
	f.SetTransactionManager(txn)
	f.topicState["events"] = &topic.Definition{Name: "events"}
	registerLifecycleBroker(t, f, "broker-1", BrokerProtocolVersionCurrent)
	return f, snap
}

func applyTransactionSnapshot(t *testing.T, f *BrokerFSM, snap *transaction.Snapshot) interface{} {
	t.Helper()
	data, err := json.Marshal(map[string]interface{}{"transaction": snap})
	require.NoError(t, err)
	return f.Apply(&raft.Log{Data: append([]byte("TXN_SYNC:"), data...), Index: 2})
}

func applyPreparedOffsets(t *testing.T, f *BrokerFSM, snap *transaction.Snapshot) interface{} {
	t.Helper()
	data, err := json.Marshal(map[string]interface{}{
		"transactional_id": snap.ID, "producer": snap.Producer, "epoch": snap.Epoch, "revision": snap.Revision,
	})
	require.NoError(t, err)
	return f.Apply(&raft.Log{Data: append([]byte("TXN_OFFSETS:"), data...), Index: 3})
}

func TestFSMPreparedOffsetsRecoverAcrossGenerationWithoutRegression(t *testing.T) {
	f, snap := preparedTransactionFSM(t)
	require.Nil(t, applyTransactionSnapshot(t, f, snap))
	_, err := f.cd.AddConsumer("workers", "new-member")
	require.NoError(t, err)
	require.Nil(t, applyTransactionSnapshot(t, f, snap))
	require.Nil(t, applyPreparedOffsets(t, f, snap))
	offset, ok := f.cd.GetOffset("workers", "events", 0)
	require.True(t, ok)
	require.Equal(t, uint64(13), offset)
	require.NoError(t, f.cd.CommitOffset("workers", "events", 0, 21))
	require.Nil(t, applyPreparedOffsets(t, f, snap))
	offset, _ = f.cd.GetOffset("workers", "events", 0)
	require.Equal(t, uint64(21), offset)
}

func TestFSMPrepareFencesOwnershipAtLogApply(t *testing.T) {
	f, snap := preparedTransactionFSM(t)
	_, err := f.cd.AddConsumer("workers", "new-member")
	require.NoError(t, err)
	require.Error(t, applyTransactionSnapshot(t, f, snap).(error))
	tx, err := f.txn.Status("tx")
	require.NoError(t, err)
	require.Equal(t, transaction.StateOpen, tx.State)
}

func TestFSMPreparedOffsetsRejectUnpreparedOrMismatchedDecision(t *testing.T) {
	f, snap := preparedTransactionFSM(t)
	require.Error(t, applyPreparedOffsets(t, f, snap).(error))
	require.Nil(t, applyTransactionSnapshot(t, f, snap))
	for _, field := range []string{"producer", "epoch", "revision"} {
		wrong := *snap
		switch field {
		case "producer":
			wrong.Producer = "other"
		case "epoch":
			wrong.Epoch++
		case "revision":
			wrong.Revision++
		}
		require.Error(t, applyPreparedOffsets(t, f, &wrong).(error))
	}
	_, exists := f.cd.GetOffset("workers", "events", 0)
	require.False(t, exists)
}

func TestFSMPrepareRequiresUpgradedBrokerTopology(t *testing.T) {
	f, snap := preparedTransactionFSM(t)
	registerLifecycleBroker(t, f, "old-broker", PreparedTransactionProtocolVersion-1)
	require.ErrorContains(t, applyTransactionSnapshot(t, f, snap).(error), "requires broker protocol")
}
