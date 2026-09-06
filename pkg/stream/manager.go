package stream

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

type StreamManager struct {
	streams    map[string]*StreamConnection // key: "topic:partition:group"
	mu         sync.RWMutex
	maxConn    int
	timeout    time.Duration
	scheduling bool
	scheduleCh chan struct{}
	closed     bool
	wg         sync.WaitGroup
}

func NewStreamManager(maxConn int, timeout time.Duration) *StreamManager {
	return &StreamManager{
		streams:    make(map[string]*StreamConnection),
		maxConn:    maxConn,
		timeout:    timeout,
		scheduleCh: make(chan struct{}, 1),
	}
}

func (sm *StreamManager) AddStream(key string, stream *StreamConnection,
	readFn func(offset uint64, max int) ([]types.Message, error),
) error {
	sm.mu.Lock()
	if sm.closed {
		sm.mu.Unlock()
		return fmt.Errorf("stream manager closed")
	}
	previous, exists := sm.streams[key]
	if !exists && len(sm.streams) >= sm.maxConn {
		sm.mu.Unlock()
		return fmt.Errorf("maximum connections (%d) reached", sm.maxConn)
	}
	sm.streams[key] = stream
	startScheduler := !sm.scheduling
	if startScheduler {
		sm.scheduling = true
		sm.wg.Add(1)
	}
	sm.wg.Add(1)
	sm.mu.Unlock()

	if previous != nil && previous != stream {
		previous.StopWithReason(StreamControlReasonReplaced)
	}
	go func() {
		defer sm.wg.Done()
		stream.Run(readFn)
		sm.removeStreamIfCurrent(key, stream)
	}()
	if startScheduler {
		go sm.runScheduler()
	} else {
		sm.wakeScheduler()
	}
	return nil
}

func (sm *StreamManager) RemoveStream(key string) {
	sm.mu.Lock()
	stream, ok := sm.streams[key]
	if ok {
		delete(sm.streams, key)
	}
	sm.mu.Unlock()
	if ok {
		stream.StopWithReason(StreamControlReasonRemoved)
		sm.wakeScheduler()
	}
}

func (sm *StreamManager) removeStreamIfCurrent(key string, stream *StreamConnection) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	current, ok := sm.streams[key]
	if !ok || current != stream {
		return false
	}
	delete(sm.streams, key)
	sm.wakeScheduler()
	return true
}

func (sm *StreamManager) runScheduler() {
	defer sm.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	type scheduledStream struct {
		key    string
		stream *StreamConnection
	}
	scheduled := make([]scheduledStream, 0)
	for {
		var now time.Time
		select {
		case now = <-timer.C:
		case <-sm.scheduleCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			now = time.Now()
		}
		sm.mu.Lock()
		if len(sm.streams) == 0 {
			sm.scheduling = false
			sm.mu.Unlock()
			return
		}
		scheduled = scheduled[:0]
		for key, stream := range sm.streams {
			scheduled = append(scheduled, scheduledStream{key: key, stream: stream})
		}
		sm.mu.Unlock()

		var nextWake time.Time
		for _, item := range scheduled {
			key, stream := item.key, item.stream
			if sm.timeout > 0 && now.Sub(stream.LastActive()) > sm.timeout {
				if sm.removeStreamIfCurrent(key, stream) {
					stream.StopWithReason(StreamControlReasonTimeout)
				}
				continue
			}
			stream.schedule(now)
			next := stream.nextScheduledAt()
			if sm.timeout > 0 {
				timeoutAt := stream.LastActive().Add(sm.timeout)
				if next.IsZero() || timeoutAt.Before(next) {
					next = timeoutAt
				}
			}
			if nextWake.IsZero() || next.Before(nextWake) {
				nextWake = next
			}
		}

		sm.mu.Lock()
		if len(sm.streams) == 0 {
			sm.scheduling = false
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()
		wait := time.Until(nextWake)
		if nextWake.IsZero() || wait < time.Millisecond {
			wait = time.Millisecond
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
	}
}

func (sm *StreamManager) wakeScheduler() {
	select {
	case sm.scheduleCh <- struct{}{}:
	default:
	}
}

func (sm *StreamManager) Close() {
	sm.mu.Lock()
	sm.closed = true
	for _, connection := range sm.streams {
		connection.Stop()
	}
	clear(sm.streams)
	sm.mu.Unlock()
	sm.wakeScheduler()
	sm.wg.Wait()
}

func (sc *StreamConnection) Conn() net.Conn {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.conn
}

func (sm *StreamManager) StopStream(key string) {
	sm.mu.RLock()
	stream, ok := sm.streams[key]
	sm.mu.RUnlock()
	if !ok {
		return
	}
	stream.StopWithReason(StreamControlReasonStopped)
}

func (sm *StreamManager) GetStreamsForPartition(topic string, partitionID int) []*StreamConnection {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var streams []*StreamConnection
	for _, stream := range sm.streams {
		if stream.topic == topic && stream.partition == partitionID {
			streams = append(streams, stream)
		}
	}
	return streams
}
