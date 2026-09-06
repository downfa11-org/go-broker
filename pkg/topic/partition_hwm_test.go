package topic

import (
	"errors"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type testTransactionDecisionResolver struct {
	state string
	known bool
}

func (r *testTransactionDecisionResolver) TransactionDecision(string, int64) (string, bool) {
	return r.state, r.known
}

func TestPartition_RestoresPersistedHWMCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "committed"}))
	require.Equal(t, uint64(1), p.GetHWM())

	leaderBatch := []types.Message{{Payload: "uncommitted"}}
	require.NoError(t, p.EnqueueBatchLeader(leaderBatch))
	p.FlushDisk()
	require.Equal(t, uint64(1), p.GetHWM(), "leader append must not advance HWM before replication commit")
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()

	require.Equal(t, uint64(1), restarted.GetHWM())
	msgs, err := restarted.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "committed", msgs[0].Payload)
}

func TestPartition_ReplacesPersistedHWMCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "first"}))
	p.FlushDisk()
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "second"}))
	p.FlushDisk()
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()

	require.Equal(t, uint64(2), restarted.GetHWM())
}

func TestPartition_RestoresProducerStateCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "first", ProducerID: "producer-1", SeqNum: 1}))
	p.FlushDisk()
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()
	restarted.isIdempotent = true

	require.NoError(t, restarted.EnqueueSync(types.Message{Payload: "duplicate", ProducerID: "producer-1", SeqNum: 1}))
	msgs, err := restarted.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "first", msgs[0].Payload)

	require.NoError(t, restarted.EnqueueSync(types.Message{Payload: "second", ProducerID: "producer-1", SeqNum: 2}))
	msgs, err = restarted.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.Equal(t, "second", msgs[1].Payload)
}

func TestPartition_RecoversProducerStateFromLogWithoutCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	msg := types.Message{Payload: "first", ProducerID: "producer-1", SeqNum: 1, TransactionalID: "tx-1", TransactionState: types.TransactionStateCommitted}
	require.NoError(t, p.EnqueueSyncIdempotent(msg))
	p.FlushDisk()
	checkpointPath := p.producerStatePath
	p.Close()
	require.NoError(t, dh.Close())
	require.NoError(t, os.Remove(checkpointPath))

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()
	restarted.RecoverProducerStateFromLog()

	retry := types.Message{Payload: "duplicate", ProducerID: "producer-1", SeqNum: 1, TransactionalID: "tx-1", TransactionState: types.TransactionStateCommitted}
	require.NoError(t, restarted.EnqueueSyncIdempotent(retry))
	msgs, err := restarted.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "first", msgs[0].Payload)
}

func TestDecodeProducerStateCheckpointRequiresVersionTwo(t *testing.T) {
	_, err := decodeProducerStateCheckpoint([]byte(`{"producer-1":1}`))
	require.ErrorContains(t, err, "clean bootstrap required")

	checkpoint, err := decodeProducerStateCheckpoint([]byte(`{"version":2,"producers":{"producer-1":{"epoch":3,"seq":7}}}`))
	require.NoError(t, err)
	require.Equal(t, producerStateCheckpointVersion, checkpoint.Version)
	require.Equal(t, producerStateCheckpointEntry{Epoch: 3, Seq: 7}, checkpoint.Producers["producer-1"])
}

func TestPartition_ProducerEpochFencing(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = dh.Close() }()
	p := NewPartition(0, "orders", dh, nil, cfg)
	defer p.Close()
	p.isIdempotent = true

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "epoch-10", ProducerID: "producer-1", Epoch: 10, SeqNum: 1}))
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "epoch-11", ProducerID: "producer-1", Epoch: 11, SeqNum: 1}))
	require.Error(t, p.EnqueueSync(types.Message{Payload: "stale-epoch", ProducerID: "producer-1", Epoch: 10, SeqNum: 2}))

	msgs, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.Equal(t, "epoch-10", msgs[0].Payload)
	require.Equal(t, "epoch-11", msgs[1].Payload)
}
func TestPartition_EnqueueBatchLeaderUsesSingleBatchWrite(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("WriteBatch", mock.Anything).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	batch := []types.Message{
		{Payload: "one", ProducerID: "producer-1", SeqNum: 1},
		{Payload: "two", ProducerID: "producer-1", SeqNum: 2},
	}
	require.NoError(t, p.EnqueueBatchLeader(batch))

	require.Equal(t, uint64(0), batch[0].Offset)
	require.Equal(t, uint64(1), batch[1].Offset)
	require.Equal(t, uint64(2), p.NextOffset())
	require.Equal(t, uint64(0), p.GetHWM(), "leader append must not advance HWM before replication commit")

	diskBatch := dh.Calls[1].Arguments.Get(0).([]types.DiskMessage)
	require.Len(t, diskBatch, 2)
	require.Equal(t, uint64(0), diskBatch[0].Offset)
	require.Equal(t, uint64(1), diskBatch[1].Offset)
	dh.AssertNotCalled(t, "AppendMessage", mock.Anything, mock.Anything, mock.Anything)
}

func TestPartition_InitialLEOMatchesStorageNextOffset(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(7)).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	require.Equal(t, uint64(7), p.NextOffset())
	require.Equal(t, uint64(7), p.GetHWM())
	dh.AssertExpectations(t)
}

func TestPartition_ReplicaAppendSeparatesDurableTailFromCommitWatermark(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	msg := types.Message{Offset: 0, Payload: "replicated", ProducerID: "producer-1", SeqNum: 1}
	dh.On("WriteBatch", mock.MatchedBy(func(got []types.DiskMessage) bool {
		return len(got) == 1 && got[0].Offset == 0 && got[0].Payload == "replicated"
	})).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	require.NoError(t, p.ReplicaAppend([]types.Message{msg}))
	require.Equal(t, uint64(1), p.NextOffset())
	require.Equal(t, uint64(0), p.GetHWM(), "replica append must remain invisible until commit metadata arrives")

	require.Error(t, p.ApplyReplicaHWM(2))
	require.NoError(t, p.ApplyReplicaHWM(1))
	require.Equal(t, uint64(1), p.GetHWM())
	dh.AssertExpectations(t)
}

func TestPartition_ReplicaAppendRetryIsIdempotentAndRejectsConflictOrGap(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	msg := types.Message{Offset: 0, Payload: "replicated", ProducerID: "producer-1", SeqNum: 1}
	dh.On("WriteBatch", mock.Anything).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	require.NoError(t, p.ReplicaAppend([]types.Message{msg}))

	dh.On("ReadMessages", uint64(0), 1).Return([]types.Message{msg}, nil).Once()
	require.NoError(t, p.ReplicaAppend([]types.Message{msg}), "an identical retry must be a no-op")
	dh.AssertNumberOfCalls(t, "WriteBatch", 1)

	dh.On("ReadMessages", uint64(0), 1).Return([]types.Message{{Offset: 0, Payload: "different"}}, nil).Once()
	require.ErrorContains(t, p.ReplicaAppend([]types.Message{msg}), "offset conflict")
	require.ErrorContains(t, p.ReplicaAppend([]types.Message{{Offset: 2, Payload: "gap"}}), "offset gap")
	dh.AssertExpectations(t)
}

func TestPartition_ReplicaAppendPreservesRequestIdempotenceForPromotion(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("WriteBatch", mock.Anything).Return(nil).Once()
	p := NewPartition(0, "orders", dh, nil, cfg)
	replicated := []types.Message{{
		Offset: 0, Payload: "replicated", ProducerID: "producer-1", SeqNum: 1, Epoch: 7,
	}}
	require.NoError(t, p.ReplicaAppendWithMode(replicated, true))

	retry := []types.Message{{
		Payload: "replicated", ProducerID: "producer-1", SeqNum: 1, Epoch: 7,
	}}
	require.NoError(t, p.EnqueueBatchLeaderWithMode(retry, true))
	require.Equal(t, uint64(1), p.NextOffset(), "promoted follower appended an idempotent retry")
	dh.AssertNumberOfCalls(t, "WriteBatch", 1)
}

func TestPartition_ReconcileCommittedHWMTruncatesUncommittedTail(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "committed"}))
	p.FlushDisk()
	uncommitted := []types.Message{
		{Payload: "open", ProducerID: "txn-producer", SeqNum: 1, TransactionalID: "tx-tail", TransactionState: types.TransactionStateOpen},
		{Payload: "marker", ProducerID: "txn-producer", SeqNum: 2, TransactionalID: "tx-tail", TransactionMarker: types.TransactionMarkerCommit},
	}
	require.NoError(t, p.EnqueueBatchLeader(uncommitted))
	p.FlushDisk()
	require.Equal(t, uint64(3), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())

	require.NoError(t, p.ReconcileCommittedHWM(1))
	p.FlushDisk()
	require.Equal(t, uint64(1), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())
	msgs, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "committed", msgs[0].Payload)

	replacement := []types.Message{{Payload: "replacement"}}
	require.NoError(t, p.EnqueueBatchLeader(replacement))
	require.Equal(t, uint64(1), replacement[0].Offset)
	require.NoError(t, p.ApplyReplicaHWM(2))
	p.FlushDisk()
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, restartedDH.Close()) }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()
	require.Equal(t, uint64(2), restarted.GetHWM())
	msgs, err = restarted.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.Equal(t, "replacement", msgs[1].Payload)
}

func TestPartition_ReconcileSnapshotHWMPreservesLaterCommittedData(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1
	dh, err := disk.NewDiskHandler(cfg, "snapshot-hwm", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dh.Close() })
	p := NewPartition(0, "snapshot-hwm", dh, nil, cfg)
	t.Cleanup(p.Close)

	require.NoError(t, p.EnqueueBatchLeader([]types.Message{
		{Payload: "snapshot"},
		{Payload: "later-commit"},
		{Payload: "uncommitted"},
	}))
	p.SetHWM(2)
	require.NoError(t, p.ReconcileSnapshotHWM(1))
	require.True(t, p.SnapshotRecoveryPending())
	require.Equal(t, uint64(3), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())
	require.NoError(t, p.ApplyReplicaHWM(2))
	require.NoError(t, p.FinalizeSnapshotRecovery(2))
	require.False(t, p.SnapshotRecoveryPending())
	require.Equal(t, uint64(2), p.NextOffset())
	require.Equal(t, uint64(2), p.GetHWM())
	messages, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
}

func TestPartition_EnqueueBatchLeaderDoesNotAdvanceStateOnWriteFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("WriteBatch", mock.Anything).Return(errors.New("disk unavailable")).Once()
	dh.On("WriteBatch", mock.Anything).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	failedBatch := []types.Message{{Payload: "one", ProducerID: "producer-1", SeqNum: 1}}
	require.Error(t, p.EnqueueBatchLeader(failedBatch))
	require.Equal(t, uint64(0), failedBatch[0].Offset)
	require.Equal(t, uint64(0), p.NextOffset())

	retryBatch := []types.Message{{Payload: "one", ProducerID: "producer-1", SeqNum: 1}}
	require.NoError(t, p.EnqueueBatchLeader(retryBatch))
	require.Equal(t, uint64(0), retryBatch[0].Offset)
	require.Equal(t, uint64(1), p.NextOffset())
}

func TestPartition_RejectsNewProducerSequenceGap(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	require.Error(t, p.EnqueueSync(types.Message{Payload: "gap", ProducerID: "producer-1", SeqNum: 2}))
}

func TestPartition_EnqueueBatchLeaderStagesProducerState(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("WriteBatch", mock.MatchedBy(func(batch []types.DiskMessage) bool {
		return len(batch) == 2 && batch[0].SeqNum == 1 && batch[1].SeqNum == 2
	})).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	batch := []types.Message{
		{Payload: "one", ProducerID: "producer-1", SeqNum: 1},
		{Payload: "two", ProducerID: "producer-1", SeqNum: 2},
	}
	require.NoError(t, p.EnqueueBatchLeader(batch))
	dh.AssertExpectations(t)
}

func TestPartition_EnqueueBatchLeaderRejectsSequenceGapWithinBatch(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	batch := []types.Message{
		{Payload: "one", ProducerID: "producer-1", SeqNum: 1},
		{Payload: "gap", ProducerID: "producer-1", SeqNum: 3},
	}
	require.Error(t, p.EnqueueBatchLeader(batch))
	dh.AssertNotCalled(t, "WriteBatch", mock.Anything)
}

func TestPartition_EnqueueBatchLeaderSkipsDuplicateWithinBatch(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("WriteBatch", mock.MatchedBy(func(batch []types.DiskMessage) bool {
		return len(batch) == 1 && batch[0].SeqNum == 1
	})).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	batch := []types.Message{
		{Payload: "one", ProducerID: "producer-1", SeqNum: 1},
		{Payload: "duplicate", ProducerID: "producer-1", SeqNum: 1},
	}
	require.NoError(t, p.EnqueueBatchLeader(batch))
	dh.AssertExpectations(t)
}

func TestPartition_ReadCommittedReturnsOutOfRangeWhenEarliestEqualsHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(5)).Once()
	dh.On("GetFirstOffset").Return(uint64(5)).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(5)

	_, err := p.ReadCommitted(4, 10)
	var offsetErr *types.OffsetOutOfRangeError
	require.ErrorAs(t, err, &offsetErr)
}
func TestPartition_ReadCommittedStopsAtUnresolvedOpenTransaction(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(5)).Once()
	dh.On("GetFirstOffset").Return(uint64(0)).Once()
	dh.On("ReadMessages", uint64(0), 1).Return([]types.Message{
		{Offset: 0, Payload: "plain"},
		{Offset: 1, Payload: "open", TransactionalID: "tx-open", TransactionState: types.TransactionStateOpen},
		{Offset: 2, Payload: "committed", TransactionalID: "tx-commit", TransactionState: types.TransactionStateCommitted},
		{Offset: 3, Payload: "marker", TransactionalID: "tx-commit", TransactionMarker: types.TransactionMarkerCommit},
		{Offset: 4, Payload: "aborted", TransactionalID: "tx-abort", TransactionState: types.TransactionStateAborted},
	}, nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(5)
	p.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx-open", TransactionState: types.TransactionStateOpen})

	msgs, err := p.ReadCommitted(0, 5)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "plain", msgs[0].Payload)
	dh.AssertExpectations(t)
}

func TestPartition_ReadCommittedUsesTransactionMarkers(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(6)).Once()
	dh.On("GetFirstOffset").Return(uint64(0)).Once()
	dh.On("ReadMessages", uint64(0), 6).Return([]types.Message{
		{Offset: 0, Payload: "before"},
		{Offset: 1, Payload: "committed-by-marker", TransactionalID: "tx-commit", TransactionState: types.TransactionStateOpen},
		{Offset: 2, Payload: "commit-marker", TransactionalID: "tx-commit", TransactionMarker: types.TransactionMarkerCommit},
		{Offset: 3, Payload: "aborted-by-marker", TransactionalID: "tx-abort", TransactionState: types.TransactionStateOpen},
		{Offset: 4, Payload: "abort-marker", TransactionalID: "tx-abort", TransactionMarker: types.TransactionMarkerAbort},
		{Offset: 5, Payload: "after"},
	}, nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(6)
	p.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx-commit", TransactionState: types.TransactionStateOpen})
	p.indexTransactionMessage(types.Message{Offset: 2, TransactionalID: "tx-commit", TransactionMarker: types.TransactionMarkerCommit})
	p.indexTransactionMessage(types.Message{Offset: 3, TransactionalID: "tx-abort", TransactionState: types.TransactionStateOpen})
	p.indexTransactionMessage(types.Message{Offset: 4, TransactionalID: "tx-abort", TransactionMarker: types.TransactionMarkerAbort})

	msgs, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	require.Equal(t, "before", msgs[0].Payload)
	require.Equal(t, "committed-by-marker", msgs[1].Payload)
	require.Equal(t, "after", msgs[2].Payload)
	dh.AssertExpectations(t)
}

func TestPartition_ReadCommittedWaitsForCoordinatorCommitDecision(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(3)).Times(4)
	dh.On("GetFirstOffset").Return(uint64(0)).Times(4)
	dh.On("ReadMessages", uint64(0), 3).Return([]types.Message{
		{Offset: 0, Payload: "transactional", TransactionalID: "tx-decision", TransactionState: types.TransactionStateOpen, Epoch: 4},
		{Offset: 1, Payload: "commit-marker", TransactionalID: "tx-decision", TransactionMarker: types.TransactionMarkerCommit, Epoch: 4},
		{Offset: 2, Payload: "after"},
	}, nil).Once()
	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(3)
	p.indexTransactionMessage(types.Message{Offset: 0, TransactionalID: "tx-decision", TransactionState: types.TransactionStateOpen, Epoch: 4})
	p.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx-decision", TransactionMarker: types.TransactionMarkerCommit, Epoch: 4})
	resolver := &testTransactionDecisionResolver{state: "committing", known: true}
	p.SetTransactionDecisionResolver(resolver)

	require.Equal(t, uint64(0), p.LastStableOffset())
	msgs, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Empty(t, msgs)

	resolver.state = types.TransactionStateCommitted
	require.Equal(t, uint64(3), p.LastStableOffset())
	msgs, err = p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.Equal(t, "transactional", msgs[0].Payload)
	require.Equal(t, "after", msgs[1].Payload)
	dh.AssertExpectations(t)
}

func TestPartition_ReadCommittedUsesIndexedMarkerForSmallBatch(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(3)).Once()
	dh.On("GetFirstOffset").Return(uint64(0)).Once()
	dh.On("ReadMessages", uint64(0), 1).Return([]types.Message{
		{Offset: 0, Payload: "committed-by-marker", TransactionalID: "tx-commit", TransactionState: types.TransactionStateOpen},
	}, nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(3)

	p.indexTransactionMessage(types.Message{Offset: 0, TransactionalID: "tx-commit", TransactionState: types.TransactionStateOpen})
	p.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx-commit", TransactionMarker: types.TransactionMarkerCommit})
	msgs, err := p.ReadCommitted(0, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "committed-by-marker", msgs[0].Payload)
	dh.AssertExpectations(t)
}

func TestPartition_ReadCommittedRequiresMarkerForTransactionalRecord(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(2)).Once()
	dh.On("GetFirstOffset").Return(uint64(0)).Once()
	dh.On("ReadMessages", uint64(0), 2).Return([]types.Message{
		{Offset: 0, Payload: "committed-state-without-marker", TransactionalID: "tx-state", TransactionState: types.TransactionStateCommitted},
		{Offset: 1, Payload: "plain"},
	}, nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(2)

	msgs, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "plain", msgs[0].Payload)
	dh.AssertExpectations(t)
}

func TestPartition_ReadCommittedSeparatesMarkersByEpoch(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(4)).Once()
	dh.On("GetFirstOffset").Return(uint64(0)).Once()
	dh.On("ReadMessages", uint64(0), 2).Return([]types.Message{
		{Offset: 0, Payload: "epoch0", TransactionalID: "tx-reused", TransactionState: types.TransactionStateOpen, Epoch: 0},
		{Offset: 1, Payload: "epoch0-marker", TransactionalID: "tx-reused", TransactionMarker: types.TransactionMarkerCommit, Epoch: 0},
		{Offset: 2, Payload: "epoch1-open", TransactionalID: "tx-reused", TransactionState: types.TransactionStateOpen, Epoch: 1},
		{Offset: 3, Payload: "after"},
	}, nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(4)
	p.indexTransactionMessage(types.Message{Offset: 0, TransactionalID: "tx-reused", TransactionState: types.TransactionStateOpen, Epoch: 0})
	p.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx-reused", TransactionMarker: types.TransactionMarkerCommit, Epoch: 0})
	p.indexTransactionMessage(types.Message{Offset: 2, TransactionalID: "tx-reused", TransactionState: types.TransactionStateOpen, Epoch: 1})

	msgs, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "epoch0", msgs[0].Payload)
	dh.AssertExpectations(t)
}

func TestPartition_ReadCommittedIgnoresMarkerBeforeRecord(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(3)).Once()
	dh.On("GetFirstOffset").Return(uint64(0)).Once()
	dh.On("ReadMessages", uint64(0), 1).Return([]types.Message{
		{Offset: 0, Payload: "early-marker", TransactionalID: "tx-order", TransactionMarker: types.TransactionMarkerCommit, Epoch: 0},
		{Offset: 1, Payload: "late-record", TransactionalID: "tx-order", TransactionState: types.TransactionStateOpen, Epoch: 0},
		{Offset: 2, Payload: "after"},
	}, nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(3)
	p.indexTransactionMessage(types.Message{Offset: 0, TransactionalID: "tx-order", TransactionMarker: types.TransactionMarkerCommit, Epoch: 0})
	p.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx-order", TransactionState: types.TransactionStateOpen, Epoch: 0})

	msgs, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Empty(t, msgs)
	dh.AssertExpectations(t)
}

func TestPartition_LastStableOffsetUsesTransactionIndex(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("GetFlushedOffset").Return(uint64(6)).Once()
	dh.On("GetFirstOffset").Return(uint64(0)).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.SetHWM(6)
	p.indexTransactionMessage(types.Message{
		Offset:           1,
		TransactionalID:  "tx-open",
		TransactionState: types.TransactionStateOpen,
	})
	p.indexTransactionMessage(types.Message{
		Offset:           3,
		TransactionalID:  "tx-committed",
		TransactionState: types.TransactionStateOpen,
	})
	p.indexTransactionMessage(types.Message{
		Offset:            4,
		TransactionalID:   "tx-committed",
		TransactionMarker: types.TransactionMarkerCommit,
	})

	require.Equal(t, uint64(1), p.LastStableOffset())
	dh.AssertExpectations(t)
}

func TestPartition_LastStableOffsetRestoresTransactionIndexFromLog(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "plain"}))
	require.NoError(t, p.EnqueueSyncIdempotent(types.Message{
		Payload:          "open",
		TransactionalID:  "tx-open",
		TransactionState: types.TransactionStateOpen,
		ProducerID:       "producer-open",
		Epoch:            1,
		SeqNum:           1,
	}))
	require.NoError(t, p.EnqueueSyncIdempotent(types.Message{
		Payload:          "committed",
		TransactionalID:  "tx-committed",
		TransactionState: types.TransactionStateOpen,
		ProducerID:       "producer-committed",
		Epoch:            2,
		SeqNum:           1,
	}))
	require.NoError(t, p.EnqueueSyncIdempotent(types.Message{
		Payload:           "commit-marker",
		TransactionalID:   "tx-committed",
		TransactionMarker: types.TransactionMarkerCommit,
		ProducerID:        "producer-committed",
		Epoch:             2,
		SeqNum:            2,
	}))
	p.FlushDisk()
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()

	require.Equal(t, uint64(1), restarted.LastStableOffset())
}

type durableBatchMockStorage struct {
	*MockStorageHandler
}

func (m *durableBatchMockStorage) WriteBatchSync(batch []types.DiskMessage) error {
	args := m.Called(batch)
	return args.Error(0)
}

func TestPartition_ReplicaAppendUsesOneDurableWriteForLargeBatch(t *testing.T) {
	cfg := config.DefaultConfig()
	base := new(MockStorageHandler)
	base.On("GetLatestOffset").Return(uint64(0)).Once()
	storage := &durableBatchMockStorage{MockStorageHandler: base}
	messages := make([]types.Message, 1000)
	for i := range messages {
		messages[i] = types.Message{
			Offset:     uint64(i),
			Payload:    "replicated",
			ProducerID: "producer-1",
			SeqNum:     uint64(i + 1),
		}
	}
	storage.On("WriteBatchSync", mock.MatchedBy(func(batch []types.DiskMessage) bool {
		return len(batch) == len(messages) && batch[0].Offset == 0 && batch[len(batch)-1].Offset == 999
	})).Return(nil).Once()

	p := NewPartition(0, "orders", storage, nil, cfg)
	require.NoError(t, p.ReplicaAppend(messages))
	require.Equal(t, uint64(1000), p.NextOffset())
	require.Equal(t, uint64(0), p.GetHWM(), "replication must not advance the commit watermark")
	storage.AssertNumberOfCalls(t, "WriteBatchSync", 1)
	storage.AssertNotCalled(t, "WriteBatch", mock.Anything)
	storage.AssertNotCalled(t, "AppendMessageWithOffset", mock.Anything, mock.Anything, mock.Anything)
	storage.AssertExpectations(t)
}

func TestPartition_ReplicaAppendDoesNotAdvanceStateWhenDurableBatchFails(t *testing.T) {
	cfg := config.DefaultConfig()
	base := new(MockStorageHandler)
	base.On("GetLatestOffset").Return(uint64(0)).Once()
	storage := &durableBatchMockStorage{MockStorageHandler: base}
	storage.On("WriteBatchSync", mock.Anything).Return(errors.New("disk unavailable")).Once()

	p := NewPartition(0, "orders", storage, nil, cfg)
	err := p.ReplicaAppend([]types.Message{{
		Offset: 0, Payload: "replicated", ProducerID: "producer-1", SeqNum: 1,
	}})
	require.ErrorContains(t, err, "replica batch append failed")
	require.Equal(t, uint64(0), p.NextOffset())
	require.Equal(t, uint64(0), p.GetHWM())
	storage.AssertExpectations(t)
}
