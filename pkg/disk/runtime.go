package disk

import (
	"fmt"
	"os"
)

// RuntimeSnapshot is an aggregate view of open broker storage handlers.
type RuntimeSnapshot struct {
	Handlers              int
	Segments              int
	Bytes                 int64
	PendingWrites         int
	ActiveReaders         int
	StatFailures          int
	SegmentCacheEntries   int
	SegmentCacheHits      uint64
	SegmentCacheMisses    uint64
	SegmentCacheEvictions uint64
}

// RuntimeSnapshot returns storage state without retaining manager locks during I/O.
func (dm *DiskManager) RuntimeSnapshot() RuntimeSnapshot {
	if dm == nil {
		return RuntimeSnapshot{}
	}

	dm.mu.Lock()
	handlers := make([]*DiskHandler, 0, len(dm.handlers))
	for _, handler := range dm.handlers {
		handlers = append(handlers, handler)
	}
	dm.mu.Unlock()

	snapshot := RuntimeSnapshot{Handlers: len(handlers)}
	for _, handler := range handlers {
		if handler == nil {
			continue
		}

		handler.mu.Lock()
		segments := append([]uint64(nil), handler.segments...)
		current := handler.CurrentSegment
		pending := len(handler.writeCh)
		handler.mu.Unlock()

		seen := make(map[uint64]struct{}, len(segments)+1)
		segments = append(segments, current)
		for _, baseOffset := range segments {
			if _, exists := seen[baseOffset]; exists {
				continue
			}
			seen[baseOffset] = struct{}{}
			snapshot.Segments++
			for _, path := range []string{handler.GetSegmentPath(baseOffset), handler.GetIndexPath(baseOffset)} {
				info, err := os.Stat(path)
				if err != nil {
					if !os.IsNotExist(err) {
						snapshot.StatFailures++
					}
					continue
				}
				snapshot.Bytes += info.Size()
			}
		}

		snapshot.PendingWrites += pending
		snapshot.ActiveReaders += int(handler.GetActiveReaders())
		cache := handler.segmentReaders.stats()
		snapshot.SegmentCacheEntries += cache.Entries
		snapshot.SegmentCacheHits += cache.Hits
		snapshot.SegmentCacheMisses += cache.Misses
		snapshot.SegmentCacheEvictions += cache.Evictions
	}

	return snapshot
}

// Ready verifies that the configured log directory and active handlers are available.
func (dm *DiskManager) Ready() error {
	if dm == nil || dm.cfg == nil {
		return fmt.Errorf("disk manager unavailable")
	}
	if dm.cfg.LogDir == "" {
		return fmt.Errorf("log directory is not configured")
	}
	info, err := os.Stat(dm.cfg.LogDir)
	if err != nil {
		return fmt.Errorf("inspect log directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("log path is not a directory")
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()
	for key, handler := range dm.handlers {
		if handler == nil {
			return fmt.Errorf("storage handler %s is unavailable", key)
		}
		if err := handler.writeAvailabilityError(); err != nil {
			return fmt.Errorf("storage handler %s is not writable: %w", key, err)
		}
		select {
		case <-handler.done:
			return fmt.Errorf("storage handler %s is closed", key)
		default:
		}
	}
	return nil
}
