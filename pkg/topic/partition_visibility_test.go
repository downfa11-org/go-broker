package topic

import (
	"strconv"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestTransactionVisibilityIndexPrunesResolvedEntriesAtRetentionFloor(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(0)).Once()
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())

	partition.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "resolved", TransactionState: types.TransactionStateOpen})
	partition.indexTransactionMessage(types.Message{Offset: 2, TransactionalID: "resolved", TransactionMarker: types.TransactionMarkerCommit})
	partition.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "unresolved", TransactionState: types.TransactionStateOpen})
	partition.pruneTransactionIndex(3)

	partition.txnMarkerMu.RLock()
	_, resolvedOpen := partition.txnOpenOffsets[transactionMarkerKey{transactionalID: "resolved"}]
	_, resolvedMarker := partition.txnMarkers[transactionMarkerKey{transactionalID: "resolved"}]
	unresolvedOffset := partition.txnOpenOffsets[transactionMarkerKey{transactionalID: "unresolved"}]
	floor := partition.txnRetentionFloor
	stableOffset := firstUnresolvedOpenOffset(10, floor, partition.txnOpenOffsets, partition.txnMarkers, nil)
	partition.txnMarkerMu.RUnlock()
	require.False(t, resolvedOpen)
	require.False(t, resolvedMarker)
	require.Equal(t, uint64(1), unresolvedOffset)
	require.Equal(t, uint64(3), floor)
	require.Equal(t, uint64(3), stableOffset)
}

func TestReadCommittedDoesNotUseMarkerAtHWM(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(0)).Once()
	storage.On("GetFlushedOffset").Return(uint64(2)).Twice()
	storage.On("GetFirstOffset").Return(uint64(0)).Twice()
	storage.On("ReadMessages", uint64(0), 2).Return([]types.Message{
		{Offset: 0, Payload: "transactional", TransactionalID: "tx", TransactionState: types.TransactionStateOpen},
		{Offset: 1, Payload: "marker", TransactionalID: "tx", TransactionMarker: types.TransactionMarkerCommit},
	}, nil).Once()
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	partition.indexTransactionMessage(types.Message{Offset: 0, TransactionalID: "tx", TransactionState: types.TransactionStateOpen})
	partition.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerCommit})

	partition.SetHWM(1)
	messages, err := partition.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Empty(t, messages)

	partition.SetHWM(2)
	messages, err = partition.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "transactional", messages[0].Payload)
	storage.AssertExpectations(t)
}

type visibilityBenchmarkStorage struct {
	messages []types.Message
}

func TestSingleCommittedReadDoesNotPrefetchAnEntireBatch(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(0)).Once()
	storage.On("GetFlushedOffset").Return(uint64(1000)).Once()
	storage.On("ReadMessages", uint64(0), 1).Return([]types.Message{{Offset: 0, TransactionMarker: types.TransactionMarkerAbort}}, nil).Once()
	storage.On("ReadMessages", uint64(1), 1).Return([]types.Message{{Offset: 1, Payload: "visible"}}, nil).Once()
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	partition.SetHWM(1000)
	messages, err := partition.ReadCommitted(0, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "visible", messages[0].Payload)
	storage.AssertExpectations(t)
}

func (s *visibilityBenchmarkStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	if offset >= uint64(len(s.messages)) || max <= 0 {
		return nil, nil
	}
	start, ok := util.SafeUint64ToInt(offset)
	if !ok {
		return nil, nil
	}
	end := start + max
	if end > len(s.messages) {
		end = len(s.messages)
	}
	return s.messages[start:end], nil
}
func (*visibilityBenchmarkStorage) GetFirstOffset() uint64       { return 0 }
func (s *visibilityBenchmarkStorage) GetAbsoluteOffset() uint64  { return uint64(len(s.messages)) }
func (s *visibilityBenchmarkStorage) GetFlushedOffset() uint64   { return uint64(len(s.messages)) }
func (s *visibilityBenchmarkStorage) GetLatestOffset() uint64    { return uint64(len(s.messages)) }
func (*visibilityBenchmarkStorage) GetSegmentPath(uint64) string { return "" }
func (*visibilityBenchmarkStorage) AppendMessage(string, int, *types.Message) (uint64, error) {
	return 0, nil
}
func (*visibilityBenchmarkStorage) AppendMessageSync(string, int, *types.Message) (uint64, error) {
	return 0, nil
}
func (*visibilityBenchmarkStorage) AppendMessageWithOffset(string, int, *types.Message) error {
	return nil
}
func (*visibilityBenchmarkStorage) WriteBatch([]types.DiskMessage) error { return nil }
func (*visibilityBenchmarkStorage) TruncateTo(uint64) error              { return nil }
func (*visibilityBenchmarkStorage) Flush()                               {}
func (*visibilityBenchmarkStorage) Close() error                         { return nil }

func BenchmarkPartitionReadCommittedLargeVisibilityIndex(b *testing.B) {
	storage := &visibilityBenchmarkStorage{messages: []types.Message{{Offset: 0, Payload: "plain"}}}
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	partition.SetHWM(1)
	for index := 0; index < 10_000; index++ {
		offset := uint64(index + 100)
		id := strconv.Itoa(index)
		partition.indexTransactionMessage(types.Message{Offset: offset, TransactionalID: id, TransactionState: types.TransactionStateOpen})
		partition.indexTransactionMessage(types.Message{Offset: offset + 1, TransactionalID: id, TransactionMarker: types.TransactionMarkerCommit})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		messages, err := partition.ReadCommitted(0, 1)
		if err != nil || len(messages) != 1 {
			b.Fatalf("ReadCommitted = %d messages, %v", len(messages), err)
		}
	}
}
