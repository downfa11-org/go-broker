package topic

import (
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

type TopicManager struct {
	topics     map[string]*Topic
	dedupMap   sync.Map
	cleanupInt time.Duration
	stopCh     chan struct{}
	hp         HandlerProvider
	mu         sync.RWMutex
}

// HandlerProvider defines an interface to provide disk handlers.
type HandlerProvider interface {
	GetHandler(topic string, partitionID int) (*disk.DiskHandler, error)
}

func NewTopicManager(cfg *config.Config, hp HandlerProvider) *TopicManager {
	cleanupSec := cfg.CleanupInterval
	if cleanupSec <= 0 {
		cleanupSec = 60
	}

	tm := &TopicManager{
		topics:     make(map[string]*Topic),
		cleanupInt: time.Duration(cleanupSec) * time.Second,
		stopCh:     make(chan struct{}),
		hp:         hp,
	}
	go tm.cleanupLoop()
	return tm
}

func (tm *TopicManager) CreateTopic(name string, partitionCount int) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if existing, ok := tm.topics[name]; ok {
		current := len(existing.Partitions)
		switch {
		case partitionCount < current:
			fmt.Printf("⚠️ cannot decrease partitions for topic '%s' (%d → %d)\n", name, current, partitionCount)
			return existing
		case partitionCount > current:
			existing.AddPartitions(partitionCount-current, tm.hp)
			fmt.Printf("🔄 topic '%s' partitions increased: %d → %d\n", name, current, len(existing.Partitions))
			return existing
		default:
			fmt.Printf("ℹ️ topic '%s' already exists with %d partitions\n", name, current)
			return existing
		}
	}

	t := NewTopic(name, partitionCount, tm.hp)
	tm.topics[name] = t
	fmt.Printf("✅ topic '%s' created with %d partitions\n", name, partitionCount)
	return t
}

func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

func (tm *TopicManager) Publish(topicName string, msg types.Message) {
	msg.ID = util.GenerateID(msg.Payload)
	now := time.Now()
	if _, loaded := tm.dedupMap.LoadOrStore(msg.ID, now); loaded {
		return
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return
	}

	start := time.Now()
	t.Publish(msg)
	elapsed := time.Since(start).Seconds()

	metrics.MessagesProcessed.Inc()
	metrics.LatencyHist.Observe(elapsed)
}

func (tm *TopicManager) RegisterConsumerGroup(topicName, groupName string, consumerCount int) *ConsumerGroup {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil
	}
	return t.RegisterConsumerGroup(groupName, consumerCount)
}

func (tm *TopicManager) Consume(topicName, groupName string, consumerIdx int) <-chan types.Message {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil
	}
	return t.Consume(groupName, consumerIdx)
}

func (tm *TopicManager) Flush() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, t := range tm.topics {
		for _, p := range t.Partitions {
			if p.dh != nil {
				if f, ok := p.dh.(interface{ Flush() }); ok {
					f.Flush()
				}
			}
		}
	}
}

func (tm *TopicManager) Stop() {
	close(tm.stopCh)
}

func (tm *TopicManager) cleanupLoop() {
	ticker := time.NewTicker(tm.cleanupInt)
	for {
		select {
		case <-ticker.C:
			tm.CleanupDedup()
		case <-tm.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (tm *TopicManager) CleanupDedup() {
	expireBefore := time.Now().Add(-30 * time.Minute)
	tm.dedupMap.Range(func(key, value any) bool {
		if ts, ok := value.(time.Time); ok && ts.Before(expireBefore) {
			tm.dedupMap.Delete(key)
			metrics.CleanupCount.Inc()
		}
		return true
	})
}

func (tm *TopicManager) DeleteTopic(name string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, ok := tm.topics[name]; ok {
		delete(tm.topics, name)
		return true
	}
	return false
}

func (tm *TopicManager) ListTopics() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	return names
}
