package stream

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

const (
	StreamControlPrefix                 = "STREAM_CONTROL"
	StreamControlTypeClose              = "CLOSE"
	StreamControlReasonStopped          = "stopped"
	StreamControlReasonReplaced         = "replaced"
	StreamControlReasonRemoved          = "removed"
	StreamControlReasonTimeout          = "timeout"
	StreamControlReasonError            = "error"
	StreamControlReasonOffsetOutOfRange = "offset_out_of_range"
)

type StreamConnection struct {
	conn      net.Conn
	topic     string
	partition int
	group     string

	mu         sync.RWMutex
	offset     uint64
	lastActive time.Time

	stopCh             chan struct{}
	stopOnce           sync.Once
	stopReason         string
	stopRequested      uint64
	stopEarliest       uint64
	stopLatest         uint64
	stopHasOffsetRange bool

	batchSize         int
	interval          time.Duration
	keepaliveInterval time.Duration
	messageSource     func() (uint64, <-chan struct{})
	wakeCh            chan struct{}
	nextPoll          time.Time
	nextKeepalive     time.Time
	pollPending       bool
	keepalivePending  bool
}

// NewStreamConnection creates a new stream connection
func NewStreamConnection(conn net.Conn, topic string, partition int, group string, offset uint64) *StreamConnection {
	sc := &StreamConnection{
		conn:              conn,
		topic:             topic,
		partition:         partition,
		group:             group,
		offset:            offset,
		lastActive:        time.Now(),
		stopCh:            make(chan struct{}),
		batchSize:         10,
		interval:          100 * time.Millisecond,
		keepaliveInterval: 5 * time.Second,
		wakeCh:            make(chan struct{}, 1),
	}
	return sc
}

func (sc *StreamConnection) SetBatchSize(size int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.batchSize = size
}

func (sc *StreamConnection) SetInterval(interval time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	sc.interval = interval
}

func (sc *StreamConnection) SetKeepaliveInterval(d time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if d <= 0 {
		d = 5 * time.Second
	}
	sc.keepaliveInterval = d
}

func (sc *StreamConnection) SetMessageSource(source func() (uint64, <-chan struct{})) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.messageSource = source
}

func (sc *StreamConnection) Run(readFn func(offset uint64, max int) ([]types.Message, error)) {
	defer func() {
		sc.sendCloseControlFrame()
		sc.closeConn()
	}()

	// sendMessages reads and sends one available batch. The manager schedules
	// catch-up polls; partition generations provide immediate broadcast wakeups.
	sendMessages := func() (bool, bool) {
		sc.mu.RLock()
		conn := sc.conn
		bs := sc.batchSize
		sc.mu.RUnlock()

		if conn == nil {
			return false, false
		}

		currentOffset := sc.Offset()
		budget, err := util.NewBatchReadBudget(sc.topic, sc.partition, wire.MaxFramePayload)
		if err != nil {
			sc.setStopReason(StreamControlReasonError)
			return false, false
		}
		msgs, err := budget.Read(currentOffset, bs, func(offset uint64, max int) ([]types.Message, error) {
			select {
			case <-sc.stopCh:
				return nil, fmt.Errorf("stream stopped")
			default:
				return readFn(offset, max)
			}
		})
		if err != nil {
			util.Error("Stream read error for %s/%d: %v", sc.topic, sc.partition, err)
			var offsetErr *types.OffsetOutOfRangeError
			if errors.As(err, &offsetErr) {
				sc.setOffsetOutOfRange(offsetErr.Requested, offsetErr.Earliest, offsetErr.Latest)
			} else {
				sc.setStopReason(StreamControlReasonError)
			}
			return false, false
		}

		if len(msgs) == 0 {
			return false, true
		}

		// Offset gap detection
		firstOffset := msgs[0].Offset
		if currentOffset > 0 && firstOffset > currentOffset {
			util.Warn("Stream %s/%d: offset gap detected, expected %d but got %d (missing %d messages)",
				sc.topic, sc.partition, currentOffset, firstOffset, firstOffset-currentOffset)
		}

		batchData, err := util.EncodeBatchMessages(sc.topic, sc.partition, "1", false, msgs)
		if err != nil {
			util.Error("Failed to encode batch messages: %v", err)
			sc.setStopReason(StreamControlReasonError)
			return false, false
		}

		if err := sc.writeBatch(conn, batchData); err != nil {
			util.Debug("Batch write error in stream: %v", err)
			sc.setStopReason(StreamControlReasonError)
			return false, false
		}

		lastOffset := msgs[len(msgs)-1].Offset
		sc.SetOffset(lastOffset + 1)
		sc.SetLastActive(time.Now())
		return true, true
	}

	sc.mu.Lock()
	messageSource := sc.messageSource
	sc.nextPoll = time.Now().Add(sc.interval)
	sc.nextKeepalive = time.Now().Add(sc.keepaliveInterval)
	sc.mu.Unlock()
	var observedGeneration uint64
	var messageCh <-chan struct{}
	if messageSource != nil {
		observedGeneration, messageCh = messageSource()
	}
	if _, ok := sendMessages(); !ok {
		sc.StopWithReason(StreamControlReasonError)
		return
	}

	for {
		select {
		case <-sc.stopCh:
			return

		case <-messageCh:
			generation, nextCh := messageSource()
			messageCh = nextCh
			if generation == observedGeneration {
				continue
			}
			observedGeneration = generation
			if _, ok := sendMessages(); !ok {
				sc.StopWithReason(StreamControlReasonError)
				return
			}

		case <-sc.wakeCh:
			poll, keepalive := sc.takeScheduledWork()
			if !poll && !keepalive {
				continue
			}
			sent, ok := sendMessages()
			if !ok {
				sc.StopWithReason(StreamControlReasonError)
				return
			}
			if keepalive && !sent && !sc.sendKeepalive() {
				sc.StopWithReason(StreamControlReasonError)
				return
			}
		}
	}
}

func (sc *StreamConnection) schedule(now time.Time) {
	sc.mu.Lock()
	if sc.nextPoll.IsZero() {
		sc.nextPoll = now.Add(sc.interval)
	}
	if sc.nextKeepalive.IsZero() {
		sc.nextKeepalive = now.Add(sc.keepaliveInterval)
	}
	if !now.Before(sc.nextPoll) {
		sc.pollPending = true
		sc.nextPoll = now.Add(sc.interval)
	}
	if !now.Before(sc.nextKeepalive) {
		sc.keepalivePending = true
		sc.nextKeepalive = now.Add(sc.keepaliveInterval)
	}
	pending := sc.pollPending || sc.keepalivePending
	sc.mu.Unlock()
	if pending {
		select {
		case sc.wakeCh <- struct{}{}:
		default:
		}
	}
}

func (sc *StreamConnection) takeScheduledWork() (bool, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	poll, keepalive := sc.pollPending, sc.keepalivePending
	sc.pollPending = false
	sc.keepalivePending = false
	return poll, keepalive
}

func (sc *StreamConnection) nextScheduledAt() time.Time {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.nextPoll.IsZero() || (!sc.nextKeepalive.IsZero() && sc.nextKeepalive.Before(sc.nextPoll)) {
		return sc.nextKeepalive
	}
	return sc.nextPoll
}

func (sc *StreamConnection) sendKeepalive() bool {
	sc.mu.RLock()
	conn := sc.conn
	sc.mu.RUnlock()
	if conn == nil {
		return false
	}
	_ = conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
	err := util.WriteWithLength(conn, []byte{})
	_ = conn.SetWriteDeadline(time.Time{})
	if err != nil {
		util.Debug("Keepalive write error: %v", err)
		return false
	}
	sc.SetLastActive(time.Now())
	return true
}

func (sc *StreamConnection) Topic() string  { return sc.topic }
func (sc *StreamConnection) Partition() int { return sc.partition }
func (sc *StreamConnection) Group() string  { return sc.group }

func (sc *StreamConnection) Offset() uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.offset
}

func (sc *StreamConnection) SetOffset(o uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.offset = o
}

func (sc *StreamConnection) IncrementOffset() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.offset++
}

func (sc *StreamConnection) SetLastActive(t time.Time) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastActive = t
}

func (sc *StreamConnection) LastActive() time.Time {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.lastActive
}

func (sc *StreamConnection) StopCh() <-chan struct{} { return sc.stopCh }

func (sc *StreamConnection) Stop() {
	sc.StopWithReason(StreamControlReasonStopped)
}

func (sc *StreamConnection) StopWithReason(reason string) {
	if reason == "" {
		reason = StreamControlReasonStopped
	}
	sc.setStopReason(reason)
	sc.stopOnce.Do(func() {
		close(sc.stopCh)
		sc.mu.RLock()
		conn := sc.conn
		sc.mu.RUnlock()
		if conn != nil {
			_ = conn.SetWriteDeadline(time.Now())
		}
	})
}

func (sc *StreamConnection) writeBatch(conn net.Conn, data []byte) error {
	sc.mu.Lock()
	select {
	case <-sc.stopCh:
		sc.mu.Unlock()
		return fmt.Errorf("stream stopped")
	default:
	}
	err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	sc.mu.Unlock()
	if err != nil {
		return err
	}
	return util.WriteWithLength(conn, data)
}

func (sc *StreamConnection) setStopReason(reason string) {
	if reason == "" {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.stopReason == "" {
		sc.stopReason = reason
	}
}

func (sc *StreamConnection) setOffsetOutOfRange(requested, earliest, latest uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.stopReason == "" {
		sc.stopReason = StreamControlReasonOffsetOutOfRange
	}
	sc.stopRequested = requested
	sc.stopEarliest = earliest
	sc.stopLatest = latest
	sc.stopHasOffsetRange = true
}
func (sc *StreamConnection) sendCloseControlFrame() {
	sc.mu.RLock()
	conn := sc.conn
	reason := sc.stopReason
	offset := sc.offset
	requested := sc.stopRequested
	earliest := sc.stopEarliest
	latest := sc.stopLatest
	hasOffsetRange := sc.stopHasOffsetRange
	sc.mu.RUnlock()

	if conn == nil {
		return
	}
	if reason == "" {
		reason = StreamControlReasonStopped
	}

	frame := fmt.Sprintf("%s type=%s reason=%s offset=%d", StreamControlPrefix, StreamControlTypeClose, reason, offset)
	if hasOffsetRange {
		frame = fmt.Sprintf("%s requested=%d earliest=%d latest=%d", frame, requested, earliest, latest)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
	if err := util.WriteWithLength(conn, []byte(frame)); err != nil {
		util.Debug("Stream close control write error: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Time{})
}

func (sc *StreamConnection) closeConn() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.conn != nil {
		_ = sc.conn.Close()
		sc.conn = nil
	}
}
