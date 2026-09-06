package sdk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumerManualCommitDoesNotQueueAutomaticCommit(t *testing.T) {
	c := newTestConsumer(t)
	t.Cleanup(func() { _ = c.Close() })
	c.config.EnableAutoCommit = false
	handled := 0
	c.MessageHandler = func(Message) error { handled++; return nil }
	pc := &PartitionConsumer{consumer: c, partitionID: 0, dataCh: make(chan *messageBatch, 1)}
	require.True(t, pc.assignmentActive())
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 42, Payload: "manual"}}}
	close(pc.dataCh)
	c.wg.Add(1)
	done := make(chan struct{})
	go func() { pc.runWorker(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manual commit worker blocked on automatic commit")
	}
	require.Equal(t, 1, handled)
	require.Empty(t, c.commitCh)
	require.Empty(t, c.offsets)
}

func TestConsumerGenerationWorkersStopWithoutClosingConsumer(t *testing.T) {
	c := newTestConsumer(t)
	t.Cleanup(func() { _ = c.Close() })
	c.config.HeartbeatIntervalMS = int(time.Hour / time.Millisecond)
	c.config.MetadataRefreshInterval = time.Hour
	ctx := c.assignmentContext()
	generation := c.assignmentGeneration.Load()
	c.startAssignmentWorkers(ctx, generation)
	c.cancelAssignment()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("generation workers did not stop")
	}
	select {
	case <-c.Done():
		t.Fatal("generation cancellation closed consumer")
	default:
	}
}
