package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// messageBatch groups messages from a single poll/stream response.
type messageBatch struct {
	topic    string
	messages []Message
}

// PartitionConsumer handles consuming and committing for a single partition.
type PartitionConsumer struct {
	partitionID          int
	consumer             *Consumer
	fetchOffset          uint64
	commitOffset         uint64
	assignmentGeneration uint64
	ctx                  context.Context

	conn   net.Conn
	mu     sync.Mutex
	closed bool
	bo     *backoff

	dataCh    chan *messageBatch
	once      sync.Once
	closeOnce sync.Once
}

func (pc *PartitionConsumer) workerContext() context.Context {
	if pc.ctx != nil {
		return pc.ctx
	}
	return pc.consumer.assignmentContext()
}

func (pc *PartitionConsumer) assignmentToken() uint64 {
	if pc.assignmentGeneration != 0 {
		return pc.assignmentGeneration
	}
	return pc.consumer.assignmentGeneration.Load()
}

func (pc *PartitionConsumer) assignmentActive() bool {
	return pc.consumer.assignmentActive(pc.assignmentToken())
}

// initWorker lazily starts the per-partition worker goroutine (once).
func (pc *PartitionConsumer) initWorker() {
	pc.once.Do(func() {
		pc.mu.Lock()
		if pc.closed {
			pc.mu.Unlock()
			return
		}
		pc.mu.Unlock()

		channelSize := pc.consumer.config.WorkerChannelSize
		if channelSize <= 0 {
			channelSize = 1000
		}
		pc.dataCh = make(chan *messageBatch, channelSize)
		pc.consumer.wg.Add(1)
		go pc.runWorker()
	})
}

// closeDataCh closes the data channel exactly once.
func (pc *PartitionConsumer) closeDataCh() {
	pc.closeOnce.Do(func() {
		if pc.dataCh != nil {
			close(pc.dataCh)
		}
	})
}

// runWorker processes batches from dataCh, calls the message handler, and commits offsets.
func (pc *PartitionConsumer) runWorker() {
	defer pc.consumer.wg.Done()
	defer pc.consumer.recordStaleWorker("processor", pc.assignmentToken())

	for batch := range pc.dataCh {
		select {
		case <-pc.workerContext().Done():
			// Roll back fetchOffset to last committed so the next consumer picks up correctly.
			pc.consumer.mu.RLock()
			committed := pc.consumer.offsets[pc.partitionID]
			pc.consumer.mu.RUnlock()
			atomic.StoreUint64(&pc.fetchOffset, committed)
			LogWarn("Partition [%d] worker stopping: context cancelled, rolled back to offset %d", pc.partitionID, committed)
			continue
		default:
		}

		if len(batch.messages) == 0 {
			continue
		}

		// Deliver messages to user handler.
		handler := pc.consumer.MessageHandler
		processingFailed := false
		if handler != nil {
			for _, msg := range batch.messages {
				if err := handler(msg); err != nil {
					LogError("Partition [%d] handler error at offset %d: %v", pc.partitionID, msg.Offset, err)
					processingFailed = true
					break
				}
			}
		}
		if processingFailed {
			pc.consumer.mu.RLock()
			committed := pc.consumer.offsets[pc.partitionID]
			pc.consumer.mu.RUnlock()
			atomic.StoreUint64(&pc.fetchOffset, committed)
			pc.closeConnection()
			select {
			case pc.consumer.rebalanceSig <- struct{}{}:
			default:
			}
			return
		}

		if !pc.assignmentActive() || pc.workerContext().Err() != nil {
			// Ownership lost after handler — skip commit, roll back.
			pc.consumer.mu.RLock()
			committed := pc.consumer.offsets[pc.partitionID]
			pc.consumer.mu.RUnlock()
			atomic.StoreUint64(&pc.fetchOffset, committed)
			continue
		}

		lastOffset := batch.messages[len(batch.messages)-1].Offset
		commitOffset := lastOffset + 1
		if !pc.consumer.config.EnableAutoCommit {
			continue
		}

		if err := pc.commitOffsetWithRetry(commitOffset); err != nil {
			LogError("Partition [%d] failed to commit offset %d: %v", pc.partitionID, commitOffset, err)
		} else {
			if !pc.assignmentActive() {
				continue
			}
			atomic.StoreUint64(&pc.commitOffset, commitOffset)

			pc.consumer.mu.Lock()
			pc.consumer.offsets[pc.partitionID] = commitOffset
			pc.consumer.mu.Unlock()
		}
	}
}

// pollAndProcess sends one CONSUME command and pushes the resulting batch to dataCh.
func (pc *PartitionConsumer) pollAndProcess() {
	select {
	case <-pc.workerContext().Done():
		return
	default:
	}

	pc.initWorker()
	LogInfo("Partition [%d] pollAndProcess starting, fetchOffset=%d", pc.partitionID, atomic.LoadUint64(&pc.fetchOffset))

	if err := pc.ensureConnection(); err != nil {
		LogWarn("Partition [%d] cannot poll: %v", pc.partitionID, err)
		return
	}

	pollStart := time.Now()
	cfg := pc.consumer.config
	defer func() {
		if cfg.EnableMetrics {
			consumerPollLatency.WithLabelValues(cfg.Topic, cfg.GroupID).Observe(time.Since(pollStart).Seconds())
		}
	}()

	pc.mu.Lock()
	conn := pc.conn
	currentOffset := atomic.LoadUint64(&pc.fetchOffset)
	pc.mu.Unlock()

	c := pc.consumer
	c.mu.RLock()
	memberID, generation := c.memberID, c.generation
	c.mu.RUnlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s generation=%d member=%s%s%s batch=%d",
		c.config.Topic, pc.partitionID, currentOffset, c.config.GroupID, generation, memberID, autoOffsetResetArgument(c.config.AutoOffsetReset), readIsolationArgument(c.config.ReadIsolation), effectivePollBatchSize(c.config))

	if err := WriteWithLength(conn, []byte(consumeCmd)); err != nil {
		LogError("Partition [%d] send CONSUME failed: %v", pc.partitionID, err)
		pc.closeConnection()
		return
	}

	pollTimeout := time.Duration(c.config.PollTimeoutMS) * time.Millisecond
	if pollTimeout <= 0 {
		pollTimeout = 5 * time.Second
	}
	if err := conn.SetReadDeadline(time.Now().Add(pollTimeout)); err != nil {
		pc.closeConnection()
		return
	}

	bo := pc.getBackoff()
	batchData, err := ReadWithLength(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		if pc.handleBrokerError(err) {
			pc.waitWithBackoff(bo)
			return
		}
		LogError("Partition [%d] read batch error: %v", pc.partitionID, err)
		pc.closeConnection()
		pc.waitWithBackoff(bo)
		return
	}

	if pc.handleStreamControl(batchData) {
		pc.waitWithBackoff(bo)
		return
	}

	// Empty data is a keepalive signal from the broker; reset backoff.
	if len(batchData) == 0 {
		bo.reset()
		return
	}

	messages, topic, _, err := DecodeBatchMessages(batchData)
	if err != nil {
		LogError("Partition [%d] decode error: %v", pc.partitionID, err)
		return
	}

	if len(messages) == 0 {
		return
	}

	expectedOffset := atomic.LoadUint64(&pc.fetchOffset)
	pc.recordOffsetAdvance(expectedOffset, messages, true)

	newOffset := messages[len(messages)-1].Offset + 1
	atomic.StoreUint64(&pc.fetchOffset, newOffset)
	bo.reset()

	if cfg.EnableMetrics {
		consumerMessagesReceived.WithLabelValues(cfg.Topic, cfg.GroupID).Add(float64(len(messages)))
	}

	select {
	case pc.dataCh <- &messageBatch{topic: topic, messages: messages}:
	case <-pc.workerContext().Done():
		pc.closeDataCh()
	}
}

// startStreamLoop sends a STREAM command and continuously reads batches until rebalance or close.
func (pc *PartitionConsumer) startStreamLoop() {
	pc.initWorker()
	pid := pc.partitionID
	c := pc.consumer
	bo := pc.getBackoff()
	defer pc.closeDataCh()

	for {
		select {
		case <-pc.workerContext().Done():
			pc.closeConnection()
			return
		default:
		}

		if !pc.assignmentActive() {
			pc.closeConnection()
			return
		}

		if err := pc.ensureConnection(); err != nil {
			LogWarn("Partition [%d] stream connection failed, retrying: %v", pid, err)
			if !pc.waitWithBackoff(bo) {
				return
			}
			continue
		}

		// On reconnect, roll back to the last committed offset to avoid gaps.
		c.mu.RLock()
		committed, ok := c.offsets[pid]
		c.mu.RUnlock()
		if ok {
			atomic.StoreUint64(&pc.fetchOffset, committed)
			LogInfo("Partition [%d] reconnected, rolling back to committed offset %d", pid, committed)
		}

		pc.mu.Lock()
		conn := pc.conn
		currentOffset := atomic.LoadUint64(&pc.fetchOffset)
		pc.mu.Unlock()

		c.mu.RLock()
		memberID, generation := c.memberID, c.generation
		c.mu.RUnlock()

		streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s offset=%d generation=%d member=%s%s%s batch=%d",
			c.config.Topic, pid, c.config.GroupID, currentOffset, generation, memberID, autoOffsetResetArgument(c.config.AutoOffsetReset), readIsolationArgument(c.config.ReadIsolation), effectiveStreamBatchSize(c.config))

		if err := WriteWithLength(conn, []byte(streamCmd)); err != nil {
			LogError("Partition [%d] STREAM send failed: %v", pid, err)
			pc.closeConnection()
			if !pc.waitWithBackoff(bo) {
				return
			}
			continue
		}

		LogInfo("Partition [%d] streaming from offset %d", pid, currentOffset)

		idleTimeout := time.Duration(c.config.StreamingReadDeadlineMS) * time.Millisecond
		if idleTimeout <= 0 {
			idleTimeout = 5 * time.Minute
		}

		for pc.assignmentActive() {
			if err := conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
				pc.closeConnection()
				break
			}

			batchData, err := ReadWithLength(conn)
			if err != nil {
				if pc.handleBrokerError(err) {
					if !pc.waitWithBackoff(bo) {
						return
					}
					continue
				}
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue // idle timeout — retry read
				}
				if pc.workerContext().Err() != nil {
					return
				}
				select {
				case <-pc.workerContext().Done():
					return
				default:
				}
				LogError("Partition [%d] stream read error: %v", pid, err)
				pc.closeConnection()
				if !pc.waitWithBackoff(bo) {
					return
				}
				break
			}

			if pc.handleStreamControl(batchData) {
				if !pc.waitWithBackoff(bo) {
					return
				}
				continue
			}

			// Empty data is a keepalive signal from the broker; continue without backoff.
			if len(batchData) == 0 {
				continue
			}

			messages, topic, _, err := DecodeBatchMessages(batchData)
			if err != nil {
				LogError("Partition [%d] stream decode error: %v", pid, err)
				if !pc.waitWithBackoff(bo) {
					return
				}
				continue
			}

			if len(messages) == 0 {
				bo.reset()
				select {
				case <-time.After(100 * time.Millisecond):
				case <-pc.workerContext().Done():
					return
				}
				continue
			}

			expectedOffset := atomic.LoadUint64(&pc.fetchOffset)
			pc.recordOffsetAdvance(expectedOffset, messages, false)
			lastOffset := messages[len(messages)-1].Offset
			atomic.StoreUint64(&pc.fetchOffset, lastOffset+1)
			bo.reset()

			if c.config.EnableMetrics {
				consumerMessagesReceived.WithLabelValues(c.config.Topic, c.config.GroupID).Add(float64(len(messages)))
			}

			select {
			case pc.dataCh <- &messageBatch{topic: topic, messages: messages}:
			case <-pc.workerContext().Done():
				return
			}
		}
	}
}

func (pc *PartitionConsumer) recordOffsetAdvance(expectedOffset uint64, messages []Message, reportUnexpected bool) {
	if len(messages) == 0 {
		return
	}
	if pc.consumer.compactionEnabled.Load() {
		skipped := countSkippedOffsets(expectedOffset, messages)
		if skipped == 0 {
			return
		}
		LogDebug("Partition [%d] skipped %d compacted offsets from %d", pc.partitionID, skipped, expectedOffset)
		if pc.consumer.config.EnableMetrics {
			consumerCompactedOffsetsSkipped.WithLabelValues(pc.consumer.config.Topic, pc.consumer.config.GroupID).Add(float64(skipped))
		}
		return
	}
	if !reportUnexpected {
		return
	}
	firstOffset := messages[0].Offset
	if expectedOffset > 0 && firstOffset > expectedOffset {
		LogError("Partition [%d] offset gap: expected %d, received %d (missing %d messages)",
			pc.partitionID, expectedOffset, firstOffset, firstOffset-expectedOffset)
		if pc.consumer.config.EnableMetrics {
			consumerOffsetGapTotal.WithLabelValues(pc.consumer.config.Topic, pc.consumer.config.GroupID).Add(float64(firstOffset - expectedOffset))
		}
	}
}

func countSkippedOffsets(expectedOffset uint64, messages []Message) uint64 {
	nextOffset := expectedOffset
	var skipped uint64
	for _, message := range messages {
		if message.Offset > nextOffset {
			skipped += message.Offset - nextOffset
		}
		if message.Offset >= nextOffset {
			nextOffset = message.Offset + 1
		}
	}
	return skipped
}

func effectivePollBatchSize(cfg *ConsumerConfig) int {
	batchSize := cfg.BatchSize
	if cfg.MaxPollRecords > 0 && (batchSize <= 0 || cfg.MaxPollRecords < batchSize) {
		batchSize = cfg.MaxPollRecords
	}
	if batchSize <= 0 {
		return 100
	}
	return batchSize
}

func effectiveStreamBatchSize(cfg *ConsumerConfig) int {
	if cfg.BatchSize > 0 {
		return cfg.BatchSize
	}
	return 100
}

// ensureConnection establishes a connection to the broker with retries and backoff.
func (pc *PartitionConsumer) ensureConnection() error {
	if pc.workerContext().Err() != nil || !pc.assignmentActive() {
		return fmt.Errorf("consumer shutting down")
	}

	pc.mu.Lock()
	if pc.conn != nil {
		pc.mu.Unlock()
		return nil
	}
	if pc.closed {
		pc.mu.Unlock()
		return fmt.Errorf("%w", ErrConsumerClosed)
	}
	pc.mu.Unlock()

	bo := pc.getBackoff()
	maxRetries := pc.consumer.config.MaxConnectRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	leaderAddr := pc.consumer.getPartitionLeaderAddr(pc.partitionID)
	if leaderAddr != "" {
		conn, err := pc.consumer.client.ConnectToAddr(leaderAddr)
		if err == nil {
			pc.mu.Lock()
			if pc.closed {
				_ = conn.Close()
				pc.mu.Unlock()
				return fmt.Errorf("%w", ErrConsumerClosed)
			}
			pc.conn = conn
			pc.mu.Unlock()
			return nil
		}
		LogWarn("Partition [%d] leader %s unreachable: %v, falling back", pc.partitionID, leaderAddr, err)
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		pc.mu.Lock()
		if pc.closed {
			pc.mu.Unlock()
			return fmt.Errorf("%w", ErrConsumerClosed)
		}
		pc.mu.Unlock()

		conn, _, connectErr := pc.consumer.client.ConnectWithFailover()
		if connectErr == nil {
			pc.mu.Lock()
			if pc.closed {
				_ = conn.Close()
				pc.mu.Unlock()
				return fmt.Errorf("%w", ErrConsumerClosed)
			}
			pc.conn = conn
			pc.mu.Unlock()
			return nil
		}

		lastErr = connectErr
		waitDur := bo.duration()
		LogWarn("Partition [%d] connect failed (attempt %d/%d): %v, retrying in %v",
			pc.partitionID, attempt+1, maxRetries, connectErr, waitDur)

		if !pc.waitDuration(waitDur) {
			return fmt.Errorf("connection aborted by shutdown")
		}
	}
	return fmt.Errorf("partition [%d] failed to connect after %d retries: %w", pc.partitionID, maxRetries, lastErr)
}

type offsetOutOfRangeFrame struct {
	Requested uint64
	Earliest  uint64
	Latest    uint64
}

func brokerOffsetOutOfRangeFrame(brokerErr *BrokerError) (offsetOutOfRangeFrame, bool) {
	if brokerErr == nil || !strings.EqualFold(brokerErr.Code, "OFFSET_OUT_OF_RANGE") {
		return offsetOutOfRangeFrame{}, false
	}

	frame := offsetOutOfRangeFrame{}
	hasEarliest := false
	hasLatest := false
	for key, value := range brokerErr.Fields {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "requested":
			frame.Requested = parsed
		case "earliest":
			frame.Earliest = parsed
			hasEarliest = true
		case "latest":
			frame.Latest = parsed
			hasLatest = true
		}
	}
	return frame, hasEarliest && hasLatest
}

type streamControlFrame struct {
	Type           string
	Reason         string
	Offset         uint64
	HasOffset      bool
	Requested      uint64
	Earliest       uint64
	Latest         uint64
	HasOffsetRange bool
}

func parseStreamControlFrame(data []byte) (streamControlFrame, bool) {
	respStr := string(data)
	if !strings.HasPrefix(respStr, "STREAM_CONTROL") {
		return streamControlFrame{}, false
	}

	frame := streamControlFrame{}
	for _, field := range strings.Fields(respStr) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "type":
			frame.Type = value
		case "reason":
			frame.Reason = value
		case "offset":
			offset, err := strconv.ParseUint(value, 10, 64)
			if err == nil {
				frame.Offset = offset
				frame.HasOffset = true
			}
		case "requested":
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				frame.Requested = parsed
			}
		case "earliest":
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				frame.Earliest = parsed
				frame.HasOffsetRange = true
			}
		case "latest":
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				frame.Latest = parsed
			}
		}
	}
	return frame, true
}

func (pc *PartitionConsumer) handleOffsetOutOfRange(frame offsetOutOfRangeFrame) bool {
	policy := pc.consumer.config.AutoOffsetReset
	if policy == "" {
		policy = AutoOffsetResetEarliest
	}

	var next uint64
	switch policy {
	case AutoOffsetResetEarliest:
		next = frame.Earliest
	case AutoOffsetResetLatest:
		next = frame.Latest
	case AutoOffsetResetError:
		LogError("Partition [%d] offset out of range requested=%d earliest=%d latest=%d", pc.partitionID, frame.Requested, frame.Earliest, frame.Latest)
		pc.consumer.cancelAssignment()
		pc.closeConnection()
		return true
	default:
		LogWarn("Partition [%d] unknown auto_offset_reset=%q, defaulting to earliest", pc.partitionID, policy)
		next = frame.Earliest
	}

	atomic.StoreUint64(&pc.fetchOffset, next)
	pc.consumer.mu.Lock()
	pc.consumer.offsets[pc.partitionID] = next
	pc.consumer.mu.Unlock()
	LogWarn("Partition [%d] offset out of range requested=%d earliest=%d latest=%d; reset fetch offset to %d (%s)", pc.partitionID, frame.Requested, frame.Earliest, frame.Latest, next, policy)
	pc.closeConnection()
	return true
}
func (pc *PartitionConsumer) handleStreamControl(data []byte) bool {
	frame, ok := parseStreamControlFrame(data)
	if !ok {
		return false
	}

	switch frame.Type {
	case "CLOSE":
		if frame.Reason == "offset_out_of_range" && frame.HasOffsetRange {
			return pc.handleOffsetOutOfRange(offsetOutOfRangeFrame{Requested: frame.Requested, Earliest: frame.Earliest, Latest: frame.Latest})
		}
		if frame.HasOffset {
			atomic.StoreUint64(&pc.fetchOffset, frame.Offset)
		}
		LogInfo("Partition [%d] stream closed by broker reason=%s offset=%d", pc.partitionID, frame.Reason, frame.Offset)
		pc.closeConnection()
		return true
	default:
		LogWarn("Partition [%d] unknown stream control frame: %s", pc.partitionID, string(data))
		return true
	}
}

// handleBrokerError returns true if err is a structured broker response.
func (pc *PartitionConsumer) handleBrokerError(err error) bool {
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		return false
	}

	LogWarn("Partition [%d] broker error: %v", pc.partitionID, brokerErr)

	if frame, ok := brokerOffsetOutOfRangeFrame(brokerErr); ok {
		return pc.handleOffsetOutOfRange(frame)
	}

	if strings.EqualFold(brokerErr.Code, "NOT_LEADER") {
		if leader := brokerErr.Fields["leader"]; leader != "" {
			pc.consumer.updatePartitionLeader(pc.partitionID, leader)
		}
	}

	switch strings.ToUpper(brokerErr.Code) {
	case "GEN_MISMATCH", "REBALANCE_REQUIRED", "NOT_OWNER", "MEMBER_NOT_FOUND":
		pc.close()
		select {
		case pc.consumer.rebalanceSig <- struct{}{}:
		default:
		}
		return true
	}

	pc.closeConnection()
	return true
}

// commitOffsetWithRetry tries the commit channel first, falling back to directCommit if full.
func (pc *PartitionConsumer) commitOffsetWithRetry(offset uint64) error {
	maxRetries := pc.consumer.config.MaxCommitRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	minBO := pc.consumer.config.CommitRetryBackoff
	maxBO := pc.consumer.config.CommitRetryMaxBackoff
	bo := newBackoff(minBO, maxBO)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if pc.workerContext().Err() != nil || !pc.assignmentActive() {
			return fmt.Errorf("commit cancelled: consumer context done")
		}

		resultCh := make(chan error, 1)
		err := func() error {
			select {
			case pc.consumer.commitCh <- commitEntry{
				partition:            pc.partitionID,
				offset:               offset,
				assignmentGeneration: pc.assignmentToken(),
				respCh:               resultCh,
			}:
				timer := time.NewTimer(5 * time.Second)
				defer timer.Stop()
				select {
				case err := <-resultCh:
					return err
				case <-pc.workerContext().Done():
					return fmt.Errorf("commit cancelled during wait")
				case <-timer.C:
					return fmt.Errorf("commit timeout")
				}
			default:
				LogWarn("Partition [%d] commitCh full, falling back to directCommit", pc.partitionID)
				return pc.consumer.directCommit(pc.partitionID, offset, pc.assignmentToken())
			}
		}()

		if err == nil {
			if pc.consumer.config.EnableMetrics {
				consumerCommitTotal.WithLabelValues(pc.consumer.config.Topic, pc.consumer.config.GroupID).Inc()
			}
			return nil
		}

		lastErr = err
		if pc.consumer.config.EnableMetrics {
			consumerCommitErrors.WithLabelValues(pc.consumer.config.Topic, pc.consumer.config.GroupID).Inc()
		}
		LogError("Partition [%d] commit attempt %d/%d failed: %v", pc.partitionID, attempt+1, maxRetries, err)

		if !pc.waitWithBackoff(bo) {
			return fmt.Errorf("commit aborted by shutdown")
		}
	}

	return fmt.Errorf("commit failed after %d attempts: %w", maxRetries, lastErr)
}

func (pc *PartitionConsumer) getBackoff() *backoff {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.bo == nil {
		minDur := time.Duration(pc.consumer.config.ConnectRetryBackoffMS) * time.Millisecond
		if minDur < 200*time.Millisecond {
			minDur = 200 * time.Millisecond
		}
		pc.bo = newBackoff(minDur, 30*time.Second)
	}
	return pc.bo
}

func (pc *PartitionConsumer) waitWithBackoff(bo *backoff) bool {
	d := bo.duration()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-pc.workerContext().Done():
		return false
	case <-pc.consumer.doneCh:
		return false
	case <-t.C:
		return true
	}
}

func (pc *PartitionConsumer) waitDuration(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-pc.workerContext().Done():
		return false
	case <-pc.consumer.doneCh:
		return false
	case <-t.C:
		return true
	}
}

func (pc *PartitionConsumer) PrintConsumedMessage(batch *messageBatch) {
	if len(batch.messages) == 0 {
		return
	}
	LogInfo("Partition [%d] batch received: topic='%s', count=%d", pc.partitionID, batch.topic, len(batch.messages))

	limit := 5
	if len(batch.messages) < limit {
		limit = len(batch.messages)
	}
	for i := 0; i < limit; i++ {
		msg := batch.messages[i]
		payload := msg.Payload
		if len(payload) > 50 {
			payload = payload[:50] + "..."
		}
		if msg.Key == "" {
			LogInfo("  msg[%d]: payload='%s'", i, payload)
		} else {
			LogInfo("  msg[%d]: key=%s payload='%s'", i, msg.Key, payload)
		}
	}
	if len(batch.messages) > 5 {
		LogInfo("  ... and %d more messages.", len(batch.messages)-5)
	}
}

// close marks the consumer as closed and closes the connection and data channel.
func (pc *PartitionConsumer) close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return
	}
	pc.closed = true
	if pc.conn != nil {
		_ = pc.conn.Close()
		pc.conn = nil
	}
	pc.closeDataCh()
}

// closeConnection drops the current connection without marking the consumer closed.
func (pc *PartitionConsumer) closeConnection() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		_ = pc.conn.Close()
		pc.conn = nil
	}
}

func readIsolationArgument(isolation ReadIsolation) string {
	switch isolation {
	case ReadUncommitted:
		return " isolation=read_uncommitted"
	case "", ReadCommitted:
		return " isolation=read_committed"
	default:
		return " isolation=read_committed"
	}
}

func autoOffsetResetArgument(policy AutoOffsetResetPolicy) string {
	switch policy {
	case AutoOffsetResetLatest:
		return " autoOffsetReset=latest"
	case AutoOffsetResetError:
		return ""
	case "", AutoOffsetResetEarliest:
		return " autoOffsetReset=earliest"
	default:
		return " autoOffsetReset=earliest"
	}
}
