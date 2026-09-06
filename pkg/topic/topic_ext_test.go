package topic

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTopic_Basic(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := new(MockHandlerProvider)
	sm := new(MockStreamManager)

	topicName := "test-topic"
	partitionCount := 2

	sm.On("GetStreamsForPartition", mock.Anything, mock.Anything).Return([]*stream.StreamConnection{}).Maybe()

	for i := 0; i < partitionCount; i++ {
		mh := new(MockStorageHandler)
		mh.On("GetLatestOffset").Return(uint64(0))
		hp.On("GetHandler", topicName, i).Return(mh, nil)
	}

	topic, err := NewTopic(topicName, partitionCount, hp, cfg, sm, false, false)
	assert.NoError(t, err)
	assert.NotNil(t, topic)
	assert.Equal(t, topicName, topic.Name)
	assert.Equal(t, partitionCount, len(topic.Partitions))

	t.Run("GetPartitionForMessage", func(t *testing.T) {
		msgNoKey := types.Message{Payload: "hello"}
		p1 := topic.GetPartitionForMessage(msgNoKey)
		p2 := topic.GetPartitionForMessage(msgNoKey)
		assert.True(t, p1 == 0 || p1 == 1)
		assert.NotEqual(t, p1, p2) // Round-robin check

		msgWithKey := types.Message{Payload: "hello", Key: "fixed-key"}
		pk1 := topic.GetPartitionForMessage(msgWithKey)
		pk2 := topic.GetPartitionForMessage(msgWithKey)
		assert.Equal(t, pk1, pk2) // Hash check
	})

	t.Run("ConsumerGroups", func(t *testing.T) {
		groupName := "group1"
		g := topic.RegisterConsumerGroup(groupName, 3)
		assert.NotNil(t, g)
		assert.Equal(t, groupName, g.Name)
		assert.Equal(t, 3, len(g.Consumers))

		g2 := topic.RegisterConsumerGroup(groupName, 5) // Existing
		assert.Equal(t, 3, len(g2.Consumers))

		err := topic.DeregisterConsumerGroup(groupName)
		assert.NoError(t, err)

		err = topic.DeregisterConsumerGroup("non-existent")
		assert.Error(t, err)
	})

	t.Run("AddPartitions", func(t *testing.T) {
		mh := new(MockStorageHandler)
		mh.On("GetLatestOffset").Return(uint64(0))
		hp.On("GetHandler", topicName, 2).Return(mh, nil)

		err := topic.AddPartitions(1, hp)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(topic.Partitions))
	})

	t.Run("PublishSync", func(t *testing.T) {
		msg := types.Message{Payload: "sync-msg", Key: "fixed-key"}
		pID := topic.GetPartitionForMessage(msg)
		mh := topic.Partitions[pID].dh.(*MockStorageHandler)
		mh.On("AppendMessageSync", topicName, pID, mock.Anything).Return(uint64(50), nil).Once()

		err := topic.PublishSync(msg)
		assert.NoError(t, err)
		mh.AssertExpectations(t)
	})
}

func TestPartition_Basic(t *testing.T) {
	cfg := config.DefaultConfig()
	sm := new(MockStreamManager)
	sm.On("GetStreamsForPartition", mock.Anything, mock.Anything).Return([]*stream.StreamConnection{})
	mh := new(MockStorageHandler)
	mh.On("GetLatestOffset").Return(uint64(10))

	p := NewPartition(0, "test-topic", mh, sm, cfg)
	assert.Equal(t, uint64(10), p.NextOffset())

	t.Run("Enqueue", func(t *testing.T) {
		msg := types.Message{Payload: "msg1"}
		mh.On("AppendMessage", "test-topic", 0, mock.Anything).Return(uint64(10), nil).Once()
		assert.NoError(t, p.Enqueue(msg))

		assert.Eventually(t, func() bool {
			return p.NextOffset() == 11
		}, 500*time.Millisecond, 5*time.Millisecond, "offset should increment after enqueue")
	})

	t.Run("EnqueueSync", func(t *testing.T) {
		msg := types.Message{Payload: "msg-sync"}
		mh.On("AppendMessageSync", "test-topic", 0, mock.Anything).Return(uint64(11), nil).Once()
		err := p.EnqueueSync(msg)
		assert.NoError(t, err)
		assert.Equal(t, uint64(12), p.NextOffset())
	})

	t.Run("Idempotence", func(t *testing.T) {
		p.isIdempotent = true
		msg := types.Message{Payload: "idemp", ProducerID: "p1", SeqNum: 1}

		mh.On("AppendMessageSync", "test-topic", 0, mock.Anything).Return(uint64(12), nil).Once()
		err := p.EnqueueSync(msg)
		assert.NoError(t, err)

		// Duplicate
		err = p.EnqueueSync(msg)
		assert.NoError(t, err)                      // Should skip without error
		assert.Equal(t, uint64(13), p.NextOffset()) // Offset didn't increase
	})

	t.Run("ProducerStateOnlyTracksIdempotentTopics", func(t *testing.T) {
		nonIdempotent := NewPartition(1, "bench-topic", mh, sm, cfg)
		nonIdempotent.updateProducerState(&types.Message{ProducerID: "producer-1", SeqNum: 10})
		_, found := nonIdempotent.producerState.Load("producer-1")
		assert.False(t, found)

		idempotent := NewPartition(2, "idem-topic", mh, sm, cfg)
		idempotent.isIdempotent = true
		idempotent.updateProducerState(&types.Message{ProducerID: "producer-1", SeqNum: 10})
		_, found = idempotent.producerState.Load("producer-1")
		assert.True(t, found)
	})
	t.Run("ReadCommitted", func(t *testing.T) {
		p.SetHWM(20)
		mh.On("GetFlushedOffset").Return(uint64(20)).Maybe()
		mh.On("ReadMessages", uint64(11), 2).Return([]types.Message{{Offset: 11, Payload: "m1"}, {Offset: 12, Payload: "m2"}}, nil).Once()
		msgs, err := p.ReadCommitted(11, 2)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(msgs))

		msgs, err = p.ReadCommitted(25, 2) // Beyond HWM
		assert.NoError(t, err)
		assert.Nil(t, msgs)
	})

	t.Run("Close", func(t *testing.T) {
		p.Close()
		assert.True(t, p.closed)
		err := p.EnqueueSync(types.Message{Payload: "after-close"})
		assert.Error(t, err)
	})
}
