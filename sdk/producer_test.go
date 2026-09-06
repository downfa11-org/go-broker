package sdk

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNewProducerClient(cfg *PublisherConfig) *ProducerClient {
	pc, _ := NewProducerClient(cfg)
	return pc
}

func TestNewProducerClient_BasicConstruction(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	client, _ := NewProducerClient(cfg)

	if client == nil {
		t.Fatal("expected non-nil ProducerClient")
	}
	if client.ID == "" {
		t.Error("expected non-empty ID")
	}
	if client.config != cfg {
		t.Error("expected config to be stored")
	}
	if client.Epoch == 0 {
		t.Error("expected non-zero Epoch")
	}

	// Leader should be initialized with empty addr
	info := client.leader.Load()
	if info == nil {
		t.Fatal("expected non-nil leader info")
	}
	if info.addr != "" {
		t.Errorf("expected empty leader addr, got %q", info.addr)
	}
}

func TestNewProducerClient_UniqueIDs(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	p1, _ := NewProducerClient(cfg)
	p2, _ := NewProducerClient(cfg)

	if p1.ID == p2.ID {
		t.Error("expected different IDs for different clients")
	}
}

func TestNextSeqNum_GlobalIncrement(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = false
	client, _ := NewProducerClient(cfg)

	s1 := client.NextSeqNum(0)
	s2 := client.NextSeqNum(0)
	s3 := client.NextSeqNum(1) // different partition, same global counter

	if s1 != 1 {
		t.Errorf("expected first seq=1, got %d", s1)
	}
	if s2 != 2 {
		t.Errorf("expected second seq=2, got %d", s2)
	}
	if s3 != 3 {
		t.Errorf("expected third seq=3, got %d", s3)
	}
}

func TestNextSeqNum_PerPartitionWithIdempotence(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = true
	cfg.Acks = "all"
	client, _ := NewProducerClient(cfg)

	// Partition 0
	s1 := client.NextSeqNum(0)
	s2 := client.NextSeqNum(0)
	// Partition 1
	s3 := client.NextSeqNum(1)

	if s1 != 1 {
		t.Errorf("expected partition 0 first seq=1, got %d", s1)
	}
	if s2 != 2 {
		t.Errorf("expected partition 0 second seq=2, got %d", s2)
	}
	if s3 != 1 {
		t.Errorf("expected partition 1 first seq=1, got %d", s3)
	}
}

func TestNextSeqNum_ConcurrentSafety(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = false
	client, _ := NewProducerClient(cfg)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			client.NextSeqNum(0)
		}()
	}
	wg.Wait()

	// After 100 increments, next should be 101
	next := client.NextSeqNum(0)
	if next != uint64(goroutines+1) {
		t.Errorf("expected seq=%d after %d concurrent calls, got %d", goroutines+1, goroutines, next)
	}
}

func TestProducerClient_SelectBroker_NilConfig(t *testing.T) {
	pc := &ProducerClient{}
	assert.Equal(t, "", pc.selectBroker())
}

func TestProducerClient_SelectBroker_NoBrokers(t *testing.T) {
	cfg := &PublisherConfig{BrokerAddrs: []string{}}
	pc := &ProducerClient{config: cfg}
	assert.Equal(t, "", pc.selectBroker())
}

func TestProducerClient_SelectBroker_FreshLeader(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)

	pc.leader.Store(&leaderInfo{
		addr:    "leader:9000",
		updated: time.Now(),
	})
	assert.Equal(t, "leader:9000", pc.selectBroker())
}

func TestProducerClient_SelectBroker_StaleLeader(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)

	pc.leader.Store(&leaderInfo{
		addr:    "old-leader:9000",
		updated: time.Now().Add(-60 * time.Second),
	})
	assert.Equal(t, "localhost:9000", pc.selectBroker())
}

func TestProducerClient_SelectBroker_EmptyLeader(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)
	assert.Equal(t, "localhost:9000", pc.selectBroker())
}

func TestProducerClient_UpdateLeader(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)

	pc.UpdateLeader("broker-1:9000")
	info := pc.leader.Load()
	assert.Equal(t, "broker-1:9000", info.addr)
	assert.False(t, info.updated.IsZero())
}

func TestProducerClient_UpdateLeader_SameAddrNoOp(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)

	pc.UpdateLeader("broker-1:9000")
	firstUpdate := pc.leader.Load().updated

	time.Sleep(1 * time.Millisecond)
	pc.UpdateLeader("broker-1:9000")
	secondUpdate := pc.leader.Load().updated

	assert.Equal(t, firstUpdate, secondUpdate)
}

func TestProducerClient_UpdateLeader_DifferentAddrUpdates(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)

	pc.UpdateLeader("broker-1:9000")
	pc.UpdateLeader("broker-2:9000")

	info := pc.leader.Load()
	assert.Equal(t, "broker-2:9000", info.addr)
}

func TestProducerClient_GetLeaderAddr_Empty(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)
	assert.Equal(t, "", pc.GetLeaderAddr())
}

func TestProducerClient_GetLeaderAddr_Set(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)
	pc.UpdateLeader("broker:9000")
	assert.Equal(t, "broker:9000", pc.GetLeaderAddr())
}

func TestProducerClient_GetConn_Nil(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)
	assert.Nil(t, pc.GetConn(0))
}

func TestProducerClient_GetConn_OutOfRange(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)

	assert.Nil(t, pc.GetConn(5))
	assert.Nil(t, pc.GetConn(-1))
}

func TestProducerClient_Close_NilConns(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)
	assert.NoError(t, pc.Close())
}

func TestProducerClient_ConnectPartition_NoBroker(t *testing.T) {
	cfg := &PublisherConfig{BrokerAddrs: []string{}}
	pc := &ProducerClient{config: cfg}
	err := pc.ConnectPartition(0, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no broker address available")
}

func TestProducerClient_ConnectPartitionLocked_NegativeIndex(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)
	err := pc.connectPartitionLocked(-1, "localhost:9000")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid partition index")
}

func TestNewPartitionBuffer(t *testing.T) {
	buf := newPartitionBuffer()
	require.NotNil(t, buf)
	assert.NotNil(t, buf.cond)
	assert.Empty(t, buf.msgs)
	assert.False(t, buf.closed)
}

func TestProducer_NextPartition_RoundRobin(t *testing.T) {
	p := &Producer{
		partitions: 3,
	}

	results := make([]int, 9)
	for i := 0; i < 9; i++ {
		results[i] = p.nextPartition()
	}

	assert.Equal(t, []int{0, 1, 2, 0, 1, 2, 0, 1, 2}, results)
}

func TestProducer_NextPartition_SinglePartition(t *testing.T) {
	p := &Producer{
		partitions: 1,
	}

	for i := 0; i < 5; i++ {
		assert.Equal(t, 0, p.nextPartition())
	}
}

func TestProducer_NextPartition_Concurrent(t *testing.T) {
	p := &Producer{
		partitions: 4,
	}

	const goroutines = 100
	counts := make([]int32, 4)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			idx := p.nextPartition()
			atomic.AddInt32(&counts[idx], 1)
		}()
	}
	wg.Wait()

	total := int32(0)
	for _, c := range counts {
		total += c
		assert.Equal(t, int32(25), c)
	}
	assert.Equal(t, int32(goroutines), total)
}

func TestProducer_Extract_EmptyBuffer(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{BatchSize: 10},
	}
	buf := newPartitionBuffer()
	result := p.extract(buf)
	assert.Nil(t, result)
}

func TestProducer_Extract_LessThanBatchSize(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{BatchSize: 10},
	}
	buf := newPartitionBuffer()
	buf.msgs = []Message{
		{SeqNum: 1, Payload: "a"},
		{SeqNum: 2, Payload: "b"},
		{SeqNum: 3, Payload: "c"},
	}

	result := p.extract(buf)
	assert.Len(t, result, 3)
	assert.Empty(t, buf.msgs)
}

func TestProducer_Extract_ExactBatchSize(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{BatchSize: 3},
	}
	buf := newPartitionBuffer()
	buf.msgs = []Message{
		{SeqNum: 1}, {SeqNum: 2}, {SeqNum: 3},
	}

	result := p.extract(buf)
	assert.Len(t, result, 3)
	assert.Empty(t, buf.msgs)
}

func TestProducer_Extract_MoreThanBatchSize(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{BatchSize: 2},
	}
	buf := newPartitionBuffer()
	buf.msgs = []Message{
		{SeqNum: 1}, {SeqNum: 2}, {SeqNum: 3}, {SeqNum: 4}, {SeqNum: 5},
	}

	result := p.extract(buf)
	assert.Len(t, result, 2)
	assert.Equal(t, uint64(1), result[0].SeqNum)
	assert.Equal(t, uint64(2), result[1].SeqNum)
	assert.Len(t, buf.msgs, 3)
	assert.Equal(t, uint64(3), buf.msgs[0].SeqNum)
}

func TestProducer_ExtractAny_EmptyBuffer(t *testing.T) {
	p := &Producer{}
	buf := newPartitionBuffer()
	result := p.extractAny(buf)
	assert.Nil(t, result)
}

func TestProducer_ExtractAny_DrainAll(t *testing.T) {
	p := &Producer{}
	buf := newPartitionBuffer()
	buf.msgs = []Message{
		{SeqNum: 1}, {SeqNum: 2}, {SeqNum: 3},
	}

	result := p.extractAny(buf)
	assert.Len(t, result, 3)
	assert.Empty(t, buf.msgs)
}

func TestProducer_ParseAckResponse_OK(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{},
		client: mustNewProducerClient(NewDefaultPublisherConfig()),
	}

	ack := AckResponse{
		Status:     "OK",
		LastOffset: 100,
		SeqStart:   1,
		SeqEnd:     10,
	}
	data, _ := json.Marshal(ack)

	resp, err := p.parseAckResponse(data)
	require.NoError(t, err)
	assert.Equal(t, "OK", resp.Status)
	assert.Equal(t, uint64(100), resp.LastOffset)
}

func TestProducer_ParseAckResponseRejectsTextErrorPayload(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{},
		client: mustNewProducerClient(NewDefaultPublisherConfig()),
	}

	_, err := p.parseAckResponse([]byte(`ERROR: broker_error class=internal retryable=false reason="busy"`))
	var brokerErr *BrokerError
	require.False(t, errors.As(err, &brokerErr))
	require.ErrorContains(t, err, "invalid ack format")
}

func TestProducer_ParseAckResponse_InvalidJSON(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{},
		client: mustNewProducerClient(NewDefaultPublisherConfig()),
	}

	_, err := p.parseAckResponse([]byte("not json"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid ack format")
}

func TestProducer_ParseAckResponse_ErrorStatus(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{},
		client: mustNewProducerClient(NewDefaultPublisherConfig()),
	}

	ack := AckResponse{
		Status:   "ERROR",
		ErrorMsg: "partition unavailable",
	}
	data, _ := json.Marshal(ack)

	resp, err := p.parseAckResponse(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition unavailable")
	assert.NotNil(t, resp)
	assert.Equal(t, "ERROR", resp.Status)
}

func TestProducer_ParseAckResponse_LeaderUpdate(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	p := &Producer{
		config: cfg,
		client: mustNewProducerClient(cfg),
	}

	ack := AckResponse{
		Status: "OK",
		Leader: "new-leader:9000",
	}
	data, _ := json.Marshal(ack)

	resp, err := p.parseAckResponse(data)
	require.NoError(t, err)
	assert.Equal(t, "OK", resp.Status)
	assert.Equal(t, "new-leader:9000", p.client.GetLeaderAddr())
}

func TestProducer_ParseAckResponse_Idempotence_MissingProducerID(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = true
	cfg.Acks = "all"
	p := &Producer{
		config: cfg,
		client: mustNewProducerClient(cfg),
	}

	ack := AckResponse{
		Status:        "OK",
		ProducerEpoch: p.client.Epoch,
	}
	data, _ := json.Marshal(ack)

	_, err := p.parseAckResponse(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing ProducerID")
}

func TestProducer_ParseAckResponse_Idempotence_EpochMismatch(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = true
	cfg.Acks = "all"
	p := &Producer{
		config: cfg,
		client: mustNewProducerClient(cfg),
	}

	ack := AckResponse{
		Status:        "OK",
		ProducerID:    p.client.ID,
		ProducerEpoch: p.client.Epoch + 1,
	}
	data, _ := json.Marshal(ack)

	_, err := p.parseAckResponse(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "epoch mismatch")
}

func TestProducer_ParseAckResponse_Idempotence_Valid(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = true
	cfg.Acks = "all"
	p := &Producer{
		config: cfg,
		client: mustNewProducerClient(cfg),
	}

	ack := AckResponse{
		Status:        "OK",
		ProducerID:    p.client.ID,
		ProducerEpoch: p.client.Epoch,
	}
	data, _ := json.Marshal(ack)

	resp, err := p.parseAckResponse(data)
	require.NoError(t, err)
	assert.Equal(t, "OK", resp.Status)
}

func TestProducer_NonRetryableProducerError(t *testing.T) {
	assert.False(t, isNonRetryableProducerError(assert.AnError))
	assert.True(t, isNonRetryableProducerError(&staticError{msg: "broker error: ERROR: stale_producer_epoch producer=p1"}))
	assert.False(t, isNonRetryableProducerError(&staticError{msg: "broker error: ERROR: NOT_LEADER"}))
}

type staticError struct {
	msg string
}

func (e *staticError) Error() string {
	return e.msg
}
func TestProducer_GetPartitionCount(t *testing.T) {
	p := &Producer{partitions: 5}
	assert.Equal(t, 5, p.GetPartitionCount())
}

func TestProducer_GetUniqueAckCount_Zero(t *testing.T) {
	p := &Producer{}
	assert.Equal(t, uint64(0), p.GetUniqueAckCount())
}

func TestProducer_GetAttemptsCount_Zero(t *testing.T) {
	p := &Producer{}
	assert.Equal(t, uint64(0), p.GetAttemptsCount())
}

func TestProducer_GetLatencies_Empty(t *testing.T) {
	p := &Producer{
		bmLatencies: make([]time.Duration, 0),
	}
	assert.Empty(t, p.GetLatencies())
}

func TestProducer_GetLatencies_ReturnsCopy(t *testing.T) {
	p := &Producer{
		bmLatencies: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond},
	}
	lat := p.GetLatencies()
	assert.Len(t, lat, 2)
	lat[0] = 999 * time.Second
	assert.Equal(t, 1*time.Millisecond, p.bmLatencies[0])
}

func TestProducer_GetPartitionStats(t *testing.T) {
	p := &Producer{
		partitions:   2,
		bmTotalTime:  map[int]time.Duration{0: 10 * time.Millisecond, 1: 20 * time.Millisecond},
		bmTotalCount: map[int]int{0: 2, 1: 4},
	}

	stats := p.GetPartitionStats()
	require.Len(t, stats, 2)
	assert.Equal(t, 0, stats[0].PartitionID)
	assert.Equal(t, 2, stats[0].BatchCount)
	assert.Equal(t, 5*time.Millisecond, stats[0].AvgDuration)
	assert.Equal(t, 1, stats[1].PartitionID)
	assert.Equal(t, 4, stats[1].BatchCount)
	assert.Equal(t, 5*time.Millisecond, stats[1].AvgDuration)
}

func TestProducer_GetPartitionStats_NoBatches(t *testing.T) {
	p := &Producer{
		partitions:   2,
		bmTotalTime:  map[int]time.Duration{},
		bmTotalCount: map[int]int{},
	}

	stats := p.GetPartitionStats()
	require.Len(t, stats, 2)
	assert.Equal(t, 0, stats[0].BatchCount)
	assert.Equal(t, time.Duration(0), stats[0].AvgDuration)
}

func TestBatchState_Fields(t *testing.T) {
	now := time.Now()
	bs := &BatchState{
		BatchID:     "test-batch-1",
		StartSeqNum: 1,
		EndSeqNum:   10,
		Partition:   2,
		SentTime:    now,
		Acked:       false,
	}

	assert.Equal(t, "test-batch-1", bs.BatchID)
	assert.Equal(t, uint64(1), bs.StartSeqNum)
	assert.Equal(t, uint64(10), bs.EndSeqNum)
	assert.Equal(t, 2, bs.Partition)
	assert.Equal(t, now, bs.SentTime)
	assert.False(t, bs.Acked)
}

func TestProducer_MarkBatchAckedByID(t *testing.T) {
	p := &Producer{
		config:               NewDefaultPublisherConfig(),
		partitions:           2,
		partitionBatchStates: make([]map[string]*BatchState, 2),
		partitionBatchMus:    make([]sync.Mutex, 2),
		partitionSentSeqs:    make([]map[uint64]struct{}, 2),
		partitionSentMus:     make([]sync.Mutex, 2),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}
	for i := 0; i < 2; i++ {
		p.partitionBatchStates[i] = make(map[string]*BatchState)
		p.partitionSentSeqs[i] = make(map[uint64]struct{})
	}

	p.partitionBatchStates[0]["batch-1"] = &BatchState{
		BatchID:     "batch-1",
		StartSeqNum: 1,
		EndSeqNum:   5,
		Partition:   0,
		SentTime:    time.Now(),
	}

	p.partitionSentSeqs[0][1] = struct{}{}
	p.partitionSentSeqs[0][2] = struct{}{}
	p.partitionSentSeqs[0][3] = struct{}{}

	p.markBatchAckedByID(0, "batch-1", 5)

	assert.Equal(t, uint64(5), p.GetUniqueAckCount())
	assert.Equal(t, uint64(5), p.ackedCount.Load())

	p.partitionSentMus[0].Lock()
	assert.Empty(t, p.partitionSentSeqs[0])
	p.partitionSentMus[0].Unlock()

	p.partitionBatchMus[0].Lock()
	_, exists := p.partitionBatchStates[0]["batch-1"]
	p.partitionBatchMus[0].Unlock()
	assert.False(t, exists)
}

func TestProducer_MarkBatchAckedByID_AlreadyAcked(t *testing.T) {
	p := &Producer{
		partitions:           1,
		partitionBatchStates: make([]map[string]*BatchState, 1),
		partitionBatchMus:    make([]sync.Mutex, 1),
		partitionSentSeqs:    make([]map[uint64]struct{}, 1),
		partitionSentMus:     make([]sync.Mutex, 1),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}
	p.partitionBatchStates[0] = make(map[string]*BatchState)
	p.partitionSentSeqs[0] = make(map[uint64]struct{})

	p.partitionBatchStates[0]["batch-1"] = &BatchState{
		BatchID:     "batch-1",
		StartSeqNum: 1,
		EndSeqNum:   5,
		Partition:   0,
		SentTime:    time.Now(),
		Acked:       true,
	}

	p.markBatchAckedByID(0, "batch-1", 5)
	assert.Equal(t, uint64(0), p.GetUniqueAckCount())
}

func TestProducer_MarkBatchAckedByID_NotFound(t *testing.T) {
	p := &Producer{
		partitions:           1,
		partitionBatchStates: make([]map[string]*BatchState, 1),
		partitionBatchMus:    make([]sync.Mutex, 1),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}
	p.partitionBatchStates[0] = make(map[string]*BatchState)

	p.markBatchAckedByID(0, "nonexistent", 5)
	assert.Equal(t, uint64(0), p.GetUniqueAckCount())
}

func TestProducer_CleanupBatchState(t *testing.T) {
	p := &Producer{
		partitions:           1,
		partitionBatchStates: make([]map[string]*BatchState, 1),
		partitionBatchMus:    make([]sync.Mutex, 1),
	}
	p.partitionBatchStates[0] = map[string]*BatchState{
		"batch-1": {BatchID: "batch-1"},
		"batch-2": {BatchID: "batch-2"},
	}

	p.cleanupBatchState(0, "batch-1")

	p.partitionBatchMus[0].Lock()
	assert.Len(t, p.partitionBatchStates[0], 1)
	_, exists := p.partitionBatchStates[0]["batch-1"]
	assert.False(t, exists)
	_, exists = p.partitionBatchStates[0]["batch-2"]
	assert.True(t, exists)
	p.partitionBatchMus[0].Unlock()
}

func TestProducer_HandleSendFailure_EmptyBatch(t *testing.T) {
	p := &Producer{
		partitions:        1,
		buffers:           []*partitionBuffer{newPartitionBuffer()},
		partitionSentSeqs: make([]map[uint64]struct{}, 1),
		partitionSentMus:  make([]sync.Mutex, 1),
	}
	p.partitionSentSeqs[0] = make(map[uint64]struct{})

	p.handleSendFailure(0, nil)
	assert.Empty(t, p.buffers[0].msgs)
}

func TestProducer_HandleSendFailure_RequeuesUnsent(t *testing.T) {
	p := &Producer{
		partitions:        1,
		buffers:           []*partitionBuffer{newPartitionBuffer()},
		partitionSentSeqs: make([]map[uint64]struct{}, 1),
		partitionSentMus:  make([]sync.Mutex, 1),
	}
	p.partitionSentSeqs[0] = map[uint64]struct{}{
		1: {},
	}

	batch := []Message{
		{SeqNum: 1, Payload: "already-sent"},
		{SeqNum: 2, Payload: "not-sent"},
		{SeqNum: 3, Payload: "not-sent-2"},
	}

	p.handleSendFailure(0, batch)

	p.buffers[0].mu.Lock()
	msgs := p.buffers[0].msgs
	p.buffers[0].mu.Unlock()

	assert.Len(t, msgs, 2)
	assert.Equal(t, uint64(2), msgs[0].SeqNum)
	assert.Equal(t, uint64(3), msgs[1].SeqNum)
	assert.True(t, msgs[0].Retry)
	assert.True(t, msgs[1].Retry)
}

func TestProducer_HandleSendFailure_MergesWithExisting(t *testing.T) {
	buf := newPartitionBuffer()
	buf.msgs = []Message{
		{SeqNum: 10, Payload: "existing"},
	}

	p := &Producer{
		partitions:        1,
		buffers:           []*partitionBuffer{buf},
		partitionSentSeqs: make([]map[uint64]struct{}, 1),
		partitionSentMus:  make([]sync.Mutex, 1),
	}
	p.partitionSentSeqs[0] = make(map[uint64]struct{})

	batch := []Message{
		{SeqNum: 5, Payload: "retry"},
	}

	p.handleSendFailure(0, batch)

	buf.mu.Lock()
	msgs := buf.msgs
	buf.mu.Unlock()

	assert.Len(t, msgs, 2)
	assert.Equal(t, uint64(5), msgs[0].SeqNum)
	assert.True(t, msgs[0].Retry)
	assert.Equal(t, uint64(10), msgs[1].SeqNum)
}

func TestProducer_HandlePartialFailure(t *testing.T) {
	buf := newPartitionBuffer()

	p := &Producer{
		partitions: 1,
		buffers:    []*partitionBuffer{buf},
	}

	batch := []Message{
		{SeqNum: 1, Payload: "ok"},
		{SeqNum: 2, Payload: "ok"},
		{SeqNum: 3, Payload: "failed"},
		{SeqNum: 4, Payload: "failed"},
	}

	ackResp := &AckResponse{
		Status: "PARTIAL",
		SeqEnd: 2,
	}

	p.handlePartialFailure(0, batch, ackResp)

	buf.mu.Lock()
	msgs := buf.msgs
	buf.mu.Unlock()

	assert.Len(t, msgs, 2)
	assert.Equal(t, uint64(3), msgs[0].SeqNum)
	assert.Equal(t, uint64(4), msgs[1].SeqNum)
}

func TestProducer_HandlePartialFailure_AllSucceeded(t *testing.T) {
	buf := newPartitionBuffer()

	p := &Producer{
		partitions: 1,
		buffers:    []*partitionBuffer{buf},
	}

	batch := []Message{
		{SeqNum: 1}, {SeqNum: 2},
	}
	ackResp := &AckResponse{Status: "PARTIAL", SeqEnd: 2}

	p.handlePartialFailure(0, batch, ackResp)

	buf.mu.Lock()
	assert.Empty(t, buf.msgs)
	buf.mu.Unlock()
}

func TestProducer_VerifySentSequences_Match(t *testing.T) {
	p := &Producer{
		partitions:        2,
		partitionSentSeqs: make([]map[uint64]struct{}, 2),
		partitionSentMus:  make([]sync.Mutex, 2),
	}
	p.partitionSentSeqs[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	p.partitionSentSeqs[1] = map[uint64]struct{}{4: {}, 5: {}}

	err := p.VerifySentSequences(5)
	assert.NoError(t, err)
}

func TestProducer_VerifySentSequences_Mismatch(t *testing.T) {
	p := &Producer{
		partitions:        1,
		partitionSentSeqs: make([]map[uint64]struct{}, 1),
		partitionSentMus:  make([]sync.Mutex, 1),
	}
	p.partitionSentSeqs[0] = map[uint64]struct{}{1: {}, 2: {}}

	err := p.VerifySentSequences(5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 5 messages sent, got 2")
}

func TestProducer_CommitBatch_FailureNotifiesChannels(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	c, err := NewConsumer(cfg)
	require.NoError(t, err)
	c.state.Store(uint32(ConsumerStateRunning))
	generation := c.assignmentGeneration.Add(1)

	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)

	offsets := map[int]uint64{0: 100}
	respChannels := map[int][]chan error{0: {ch1, ch2}}

	c.commitBatch(offsets, respChannels, generation)

	err1 := <-ch1
	err2 := <-ch2
	assert.Error(t, err1)
	assert.Error(t, err2)
}

func TestProducerClient_SelectBroker_RecentLeader(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{"fallback:9000"}
	pc, _ := NewProducerClient(cfg)

	pc.leader.Store(&leaderInfo{
		addr:    "current-leader:9000",
		updated: time.Now().Add(-10 * time.Second),
	})

	assert.Equal(t, "current-leader:9000", pc.selectBroker())
}

func TestProducerClient_SelectBroker_ExactlyStaleThreshold(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{"fallback:9000"}
	pc, _ := NewProducerClient(cfg)

	pc.leader.Store(&leaderInfo{
		addr:    "stale-leader:9000",
		updated: time.Now().Add(-31 * time.Second),
	})

	assert.Equal(t, "fallback:9000", pc.selectBroker())
}

func TestNewProducerClient_TLSError(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.UseTLS = true
	cfg.TLSCertPath = "/nonexistent/cert.pem"
	cfg.TLSKeyPath = "/nonexistent/key.pem"

	client, err := NewProducerClient(cfg)
	assert.Nil(t, client)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load TLS cert")
}

func TestProducerClient_ReconnectPartition_EmptyAddrFallsBackToSelectBroker(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	pc, _ := NewProducerClient(cfg)
	pc.config.BrokerAddrs = nil

	err := pc.ReconnectPartition(0, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no broker address available")
}

func TestProducerClient_SelectBroker_CustomLeaderStaleness(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{"fallback:9000"}
	cfg.LeaderStaleness = 5 * time.Second
	pc, _ := NewProducerClient(cfg)

	pc.leader.Store(&leaderInfo{
		addr:    "leader:9000",
		updated: time.Now().Add(-3 * time.Second),
	})
	assert.Equal(t, "leader:9000", pc.selectBroker())

	pc.leader.Store(&leaderInfo{
		addr:    "leader:9000",
		updated: time.Now().Add(-6 * time.Second),
	})
	assert.Equal(t, "fallback:9000", pc.selectBroker())
}

func TestProducerClient_SelectBroker_ZeroLeaderStaleness(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{"fallback:9000"}
	cfg.LeaderStaleness = 0
	pc, _ := NewProducerClient(cfg)

	pc.leader.Store(&leaderInfo{
		addr:    "leader:9000",
		updated: time.Now().Add(-20 * time.Second),
	})
	assert.Equal(t, "leader:9000", pc.selectBroker())
}

func TestProducer_MarkBatchAckedByID_BenchmarkEnabled(t *testing.T) {
	p := &Producer{
		config:               &PublisherConfig{EnableBenchmark: true},
		partitions:           1,
		partitionBatchStates: make([]map[string]*BatchState, 1),
		partitionBatchMus:    make([]sync.Mutex, 1),
		partitionSentSeqs:    make([]map[uint64]struct{}, 1),
		partitionSentMus:     make([]sync.Mutex, 1),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}
	p.partitionBatchStates[0] = make(map[string]*BatchState)
	p.partitionSentSeqs[0] = make(map[uint64]struct{})

	p.partitionBatchStates[0]["batch-bm"] = &BatchState{
		BatchID:     "batch-bm",
		StartSeqNum: 1,
		EndSeqNum:   3,
		Partition:   0,
		SentTime:    time.Now().Add(-10 * time.Millisecond),
	}

	p.markBatchAckedByID(0, "batch-bm", 3)

	p.bmMu.Lock()
	assert.Len(t, p.bmLatencies, 1)
	assert.Greater(t, p.bmLatencies[0], time.Duration(0))
	p.bmMu.Unlock()
}

func TestProducer_MarkBatchAckedByID_BenchmarkDisabled(t *testing.T) {
	p := &Producer{
		config:               &PublisherConfig{EnableBenchmark: false},
		partitions:           1,
		partitionBatchStates: make([]map[string]*BatchState, 1),
		partitionBatchMus:    make([]sync.Mutex, 1),
		partitionSentSeqs:    make([]map[uint64]struct{}, 1),
		partitionSentMus:     make([]sync.Mutex, 1),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}
	p.partitionBatchStates[0] = make(map[string]*BatchState)
	p.partitionSentSeqs[0] = make(map[uint64]struct{})

	p.partitionBatchStates[0]["batch-nobm"] = &BatchState{
		BatchID:     "batch-nobm",
		StartSeqNum: 1,
		EndSeqNum:   3,
		Partition:   0,
		SentTime:    time.Now().Add(-10 * time.Millisecond),
	}

	p.markBatchAckedByID(0, "batch-nobm", 3)

	p.bmMu.Lock()
	assert.Empty(t, p.bmLatencies)
	assert.Equal(t, 1, p.bmTotalCount[0])
	p.bmMu.Unlock()
}

func TestProducer_Extract_PreservesOrder(t *testing.T) {
	p := &Producer{
		config: &PublisherConfig{BatchSize: 3},
	}
	buf := newPartitionBuffer()
	buf.msgs = []Message{
		{SeqNum: 10}, {SeqNum: 20}, {SeqNum: 30}, {SeqNum: 40}, {SeqNum: 50},
	}

	batch1 := p.extract(buf)
	assert.Len(t, batch1, 3)
	assert.Equal(t, uint64(10), batch1[0].SeqNum)
	assert.Equal(t, uint64(20), batch1[1].SeqNum)
	assert.Equal(t, uint64(30), batch1[2].SeqNum)

	batch2 := p.extract(buf)
	assert.Len(t, batch2, 2)
	assert.Equal(t, uint64(40), batch2[0].SeqNum)
	assert.Equal(t, uint64(50), batch2[1].SeqNum)

	assert.Empty(t, buf.msgs)
}

type producerDrainBrokerResult struct {
	messages []Message
	err      error
}

func newProducerDrainTestHarness(t *testing.T, rejection ...string) (*Producer, <-chan producerDrainBrokerResult) {
	t.Helper()

	cfg := NewDefaultPublisherConfig()
	cfg.Topic = "producer-drain-test"
	cfg.Partitions = 1
	cfg.BatchSize = 100
	cfg.BufferSize = 10
	cfg.LingerMS = int(time.Hour / time.Millisecond)
	cfg.FlushTimeoutMS = 2_000
	cfg.MaxRetries = 0
	cfg.EnableIdempotence = false

	client := mustNewProducerClient(cfg)
	brokerConn, producerConn := net.Pipe()
	resultCh := make(chan producerDrainBrokerResult, 1)
	go func() {
		connection, request, _, err := acceptWireTestRequest(brokerConn)
		if err != nil {
			resultCh <- producerDrainBrokerResult{err: err}
			return
		}
		messages, _, _, err := DecodeBatchMessages(request.Payload)
		if err != nil {
			resultCh <- producerDrainBrokerResult{err: err}
			return
		}
		ack, err := json.Marshal(AckResponse{
			Status: "OK", ProducerID: messages[0].ProducerID, ProducerEpoch: messages[0].Epoch,
			SeqStart: messages[0].SeqNum, SeqEnd: messages[len(messages)-1].SeqNum,
		})
		if err == nil {
			if len(rejection) > 0 {
				ack = []byte(rejection[0])
			}
			err = writeWireTestResponse(connection, request, string(ack))
		}
		resultCh <- producerDrainBrokerResult{messages: messages, err: err}
	}()
	framed, err := openWireConnection(producerConn, 1000, "none")
	require.NoError(t, err)
	connections := []net.Conn{framed}
	client.conns.Store(&connections)

	p := &Producer{
		config:               cfg,
		client:               client,
		partitions:           1,
		buffers:              []*partitionBuffer{newPartitionBuffer()},
		inFlight:             make([]int32, 1),
		partitionSentMus:     make([]sync.Mutex, 1),
		partitionSentSeqs:    []map[uint64]struct{}{{}},
		partitionBatchStates: []map[string]*BatchState{{}},
		partitionBatchMus:    make([]sync.Mutex, 1),
		gcTicker:             time.NewTicker(time.Hour),
		partitionLeaders:     make(map[int]string),
		done:                 make(chan struct{}),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}

	p.sendersWG.Add(1)
	go p.partitionSender(0)

	t.Cleanup(func() {
		_ = p.Close()
		_ = brokerConn.Close()
	})
	return p, resultCh
}

func TestProducerFlushDrainsPartialBatchWithoutWaitingForLinger(t *testing.T) {
	p, resultCh := newProducerDrainTestHarness(t)

	_, err := p.Send("flush-now")
	require.NoError(t, err)

	started := time.Now()
	require.NoError(t, p.Flush())
	require.Less(t, time.Since(started), 2*time.Second)

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Len(t, result.messages, 1)
		assert.Equal(t, "flush-now", result.messages[0].Payload)
	case <-time.After(time.Second):
		t.Fatal("Flush returned before the broker acknowledged the partial batch")
	}
}

func TestProducerCloseDrainsPartialBatchWithoutWaitingForLinger(t *testing.T) {
	p, resultCh := newProducerDrainTestHarness(t)

	_, err := p.Send("close-now")
	require.NoError(t, err)

	started := time.Now()
	require.NoError(t, p.Close())
	require.Less(t, time.Since(started), 2*time.Second)

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Len(t, result.messages, 1)
		assert.Equal(t, "close-now", result.messages[0].Payload)
	case <-time.After(time.Second):
		t.Fatal("Close returned before the broker acknowledged the partial batch")
	}
}
