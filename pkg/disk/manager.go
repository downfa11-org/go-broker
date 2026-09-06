package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type initialStoragePolicy struct {
	cleanupPolicy  string
	retentionHours int
	retentionBytes int64
}

type DiskManager struct {
	mu       sync.Mutex
	handlers map[string]*DiskHandler
	cfg      *config.Config
	closed   bool
	closeErr error
}

func NewDiskManager(cfg *config.Config) *DiskManager {
	return &DiskManager{
		handlers: make(map[string]*DiskHandler),
		cfg:      cfg,
	}
}

// TopicMetadataPath returns the broker-owned standalone topic manifest path.
func (dm *DiskManager) TopicMetadataPath() string {
	if dm == nil || dm.cfg == nil || strings.TrimSpace(dm.cfg.LogDir) == "" {
		return ""
	}
	return filepath.Join(dm.cfg.LogDir, config.TopicMetadataFileName)
}

// GetHandler returns a StorageHandler for a given name or creates one if missing.
func (dm *DiskManager) GetHandler(topic string, partitionID int) (types.StorageHandler, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.getHandlerLocked(topic, partitionID, nil)
}

// GetHandlerWithPolicy initializes maintenance with the topic policy before its
// background loops can run.
func (dm *DiskManager) GetHandlerWithPolicy(topic string, partitionID int, cleanupPolicy string, retentionHours int, retentionBytes int64) (types.StorageHandler, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	key := diskHandlerKey(topic, partitionID)
	if handler, ok := dm.handlers[key]; ok {
		handler.SetStoragePolicy(cleanupPolicy, retentionHours, retentionBytes)
		return handler, nil
	}

	return dm.getHandlerLocked(topic, partitionID, &initialStoragePolicy{
		cleanupPolicy:  cleanupPolicy,
		retentionHours: retentionHours,
		retentionBytes: retentionBytes,
	})
}

func (dm *DiskManager) getHandlerLocked(topic string, partitionID int, policy *initialStoragePolicy) (types.StorageHandler, error) {
	if dm.closed {
		return nil, fmt.Errorf("disk manager is closed")
	}
	key := diskHandlerKey(topic, partitionID)
	if handler, ok := dm.handlers[key]; ok {
		return handler, nil
	}

	if err := os.MkdirAll(dm.cfg.LogDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", dm.cfg.LogDir, err)
	}

	var handler *DiskHandler
	var err error
	if policy == nil {
		handler, err = NewDiskHandler(dm.cfg, topic, partitionID)
	} else {
		handler, err = newDiskHandlerWithPolicy(dm.cfg, topic, partitionID, policy.cleanupPolicy, policy.retentionHours, policy.retentionBytes)
	}
	if err != nil {
		return nil, err
	}
	dm.handlers[key] = handler
	return handler, nil
}

// ExistingPartitionCount discovers the highest persisted partition for a topic
// without creating handlers or files.
func (dm *DiskManager) ExistingPartitionCount(topic string) (int, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	entries, err := os.ReadDir(filepath.Join(dm.cfg.LogDir, topic))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read topic directory: %w", err)
	}

	maxPartition := -1
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "partition_") {
			continue
		}
		remainder := strings.TrimPrefix(name, "partition_")
		separator := strings.Index(remainder, "_segment_")
		if separator <= 0 || !strings.HasSuffix(name, ".log") {
			continue
		}
		partition, parseErr := strconv.Atoi(remainder[:separator])
		if parseErr != nil || partition < 0 {
			continue
		}
		if partition > maxPartition {
			maxPartition = partition
		}
	}
	return maxPartition + 1, nil
}

// RemoveTopicStorage removes a topic directory after TopicManager has
// validated that the path is contained by the configured log root.
func (dm *DiskManager) RemoveTopicStorage(path string) error {
	return os.RemoveAll(path)
}

// CloseAllHandlers evicts handlers while keeping the manager reusable.
func (dm *DiskManager) CloseAllHandlers() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.closeAllHandlersLocked()
}

// Close stops the manager permanently and returns retained storage close errors.
func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.closed = true
	dm.closeAllHandlersLocked()
	return dm.closeErr
}

func (dm *DiskManager) closeAllHandlersLocked() {
	for name, dh := range dm.handlers {
		util.Debug("Closing DiskHandler for %s", name)
		if err := dh.Close(); err != nil {
			dm.closeErr = errors.Join(dm.closeErr, fmt.Errorf("close storage %s: %w", name, err))
			util.Warn("Failed to close DiskHandler for %s: %v", name, err)
		}
		delete(dm.handlers, name)
	}
}

// CloseTopicHandlers closes and forgets all handlers for a topic so its log
// directory can be safely deleted before the topic is recreated.
func (dm *DiskManager) CloseTopicHandlers(topic string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for name, dh := range dm.handlers {
		if !diskHandlerKeyMatchesTopic(name, topic) {
			continue
		}
		util.Debug("Closing DiskHandler for %s", name)
		if err := dh.Close(); err != nil {
			dm.closeErr = errors.Join(dm.closeErr, fmt.Errorf("close storage %s: %w", name, err))
			util.Warn("Failed to close DiskHandler for %s: %v", name, err)
		}
		delete(dm.handlers, name)
	}
}

// ClosePartitionHandler closes and evicts one staged partition handler.
func (dm *DiskManager) ClosePartitionHandler(topic string, partitionID int) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	key := diskHandlerKey(topic, partitionID)
	handler := dm.handlers[key]
	if handler == nil {
		return
	}
	if err := handler.Close(); err != nil {
		dm.closeErr = errors.Join(dm.closeErr, fmt.Errorf("close storage %s: %w", key, err))
		util.Warn("Failed to close DiskHandler for %s: %v", key, err)
	}
	delete(dm.handlers, key)
}

func diskHandlerKey(topic string, partitionID int) string {
	return topic + "_" + strconv.Itoa(partitionID)
}

func diskHandlerKeyMatchesTopic(key, topic string) bool {
	if key == topic {
		return true
	}
	if !strings.HasPrefix(key, topic+"_") {
		return false
	}
	suffix := strings.TrimPrefix(key, topic+"_")
	if suffix == "" {
		return false
	}
	_, err := strconv.Atoi(suffix)
	return err == nil
}
