package disk

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// flushLoop continuously processes write batches and handles segment rotation.
func (d *DiskHandler) flushLoop() {
	batch := make([]types.DiskMessage, 0, d.batchSize)
	ticker := time.NewTicker(d.linger)
	defer ticker.Stop()

	var segmentTicker *time.Ticker
	if d.segmentRollTime > 0 {
		segmentTicker = time.NewTicker(d.segmentRollTime)
		defer segmentTicker.Stop()
	}

	for {
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				d.drainAndShutdown(batch)
				return
			}

			batch = append(batch, msg)
			if len(batch) >= d.batchSize {
				util.Debug("Batch size threshold reached, flushing %d messages", len(batch))
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		case done := <-d.flushSignal:
			draining := true
			for draining {
				select {
				case msg, ok := <-d.writeCh:
					if !ok {
						draining = false
					} else {
						batch = append(batch, msg)
					}
				default:
					draining = false
				}
			}
			if len(batch) > 0 {
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed during flush: %v", err)
				}
				batch = batch[:0]
			}
			d.ioMu.Lock()
			if d.writer != nil {
				if err := d.writer.Flush(); err != nil {
					_ = d.markWriteUnavailable(fmt.Errorf("flush on request: %w", err))
				}
			}
			if d.file != nil {
				if err := d.syncFile(d.file); err != nil {
					_ = d.markWriteUnavailable(fmt.Errorf("sync on request: %w", err))
				}
			}
			d.ioMu.Unlock()
			close(done)
		case <-ticker.C:
			if len(batch) > 0 {
				util.Debug("Flushing %d messages on timer", len(batch))
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		case <-d.getSegmentTickerChan(segmentTicker):
			d.mu.Lock()
			d.ioMu.Lock()
			if time.Since(d.segmentCreatedAt) >= d.segmentRollTime {
				if err := d.rotateSegment(d.AbsoluteOffset); err != nil {
					util.Error("time-based segment rotation failed: %v", err)
				}
			}
			d.ioMu.Unlock()
			d.mu.Unlock()

		case <-d.done:
			d.drainAndShutdown(batch)
			return
		}
	}
}

func (d *DiskHandler) syncLoop() {
	ticker := time.NewTicker(time.Duration(d.syncIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.ioMu.Lock()
			syncSuccess := false

			if d.file != nil {
				if err := d.syncFile(d.file); err == nil {
					syncSuccess = true
				} else {
					util.Error("failed to sync data file: %v", err)
					_ = d.markWriteUnavailable(fmt.Errorf("periodic data sync: %w", err))
					syncSuccess = false
				}
			}

			if d.indexFile != nil {
				if err := d.indexFile.Sync(); err != nil {
					util.Error("failed to sync index file: %v", err)
					_ = d.markWriteUnavailable(fmt.Errorf("periodic index sync: %w", err))
					syncSuccess = false
				}
			}

			currentOffset := atomic.LoadUint64(&d.AbsoluteOffset)
			d.ioMu.Unlock()

			if syncSuccess {
				d.notifySync(currentOffset)
			}
		case <-d.done:
			return
		}
	}
}

// WriteBatch writes a batch of messages into the current segment file.
func (d *DiskHandler) WriteBatch(batch []types.DiskMessage) error {
	return d.writeBatch(batch, false)
}

// WriteBatchSync writes a batch and waits for one filesystem sync boundary.
// It is used for acknowledged leader and follower replication so a large batch
// remains durable without issuing one fsync per message.
func (d *DiskHandler) WriteBatchSync(batch []types.DiskMessage) error {
	return d.writeBatch(batch, true)
}

func (d *DiskHandler) writeBatch(batch []types.DiskMessage, syncData bool) error {
	if len(batch) == 0 {
		return nil
	}
	if err := d.writeAvailabilityError(); err != nil {
		return err
	}

	interval := d.indexInterval
	if interval == 0 {
		interval = 4096 // default interval (4KB)
	}

	// Serialize outside of lock to reduce lock hold time
	serializedMsgs := make([][]byte, len(batch))
	totalSize := 0
	for i, msg := range batch {
		serialized, err := util.SerializeDiskMessage(msg)
		if err != nil {
			return fmt.Errorf("serialize failed at index %d: %w", i, err)
		}
		if err := validateSerializedDiskMessageSize(serialized); err != nil {
			return fmt.Errorf("message at index %d: %w", i, err)
		}
		if _, ok := util.SafeIntToUint32(len(serialized)); !ok {
			return fmt.Errorf("message too large at index %d: %d bytes", i, len(serialized))
		}
		serializedMsgs[i] = serialized
		totalSize += 4 + len(serialized)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			return fmt.Errorf("open segment failed: %w", err)
		}
		if err := d.openIndexFiles(); err != nil {
			return err
		}
	}

	const entrySize = uint64(types.IndexEntrySize)
	willExceedData := d.CurrentOffset+uint64(totalSize) > d.SegmentSize

	maxIndexEntries := (uint64(totalSize) / interval) + 1
	requiredIndexSpace := maxIndexEntries * entrySize
	willExceedIndex := d.indexBytesWritten+requiredIndexSpace > d.IndexSize

	if willExceedData || willExceedIndex {
		util.Debug("Rolling segment: DataExceed=%v, IndexExceed=%v, CurrentIdxBytes=%d", willExceedData, willExceedIndex, d.indexBytesWritten)
		if err := d.rotateSegment(batch[0].Offset); err != nil {
			return err
		}
	}

	accumulatedLen := uint64(0)
	var lenBuf [4]byte
	for i, serialized := range serializedMsgs {
		msgPosition := d.CurrentOffset + accumulatedLen
		msg := batch[i]

		if msgPosition-d.lastIndexPosition >= interval {
			if d.indexWriter != nil {
				entry := types.IndexEntry{
					Offset:   msg.Offset,
					Position: msgPosition,
				}

				if err := binary.Write(d.indexWriter, binary.BigEndian, entry); err != nil {
					return d.markWriteUnavailable(fmt.Errorf("failed to write index entry for offset %d: %w", msg.Offset, err))
				}

				d.indexBytesWritten += entrySize
				d.lastIndexPosition = msgPosition
			}
		}

		sLen, _ := util.SafeIntToUint32(len(serialized)) // validated in pre-loop
		binary.BigEndian.PutUint32(lenBuf[:], sLen)
		if _, err := d.writer.Write(lenBuf[:]); err != nil {
			return d.markWriteUnavailable(fmt.Errorf("write length failed: %w", err))
		}
		if _, err := d.writer.Write(serialized); err != nil {
			return d.markWriteUnavailable(fmt.Errorf("write payload failed: %w", err))
		}
		accumulatedLen += uint64(4 + len(serialized))
	}

	if err := d.writer.Flush(); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("flush failed after batch: %w", err))
	}

	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			return d.markWriteUnavailable(fmt.Errorf("flush index writer failed: %w", err))
		}
	}
	if syncData {
		if err := d.syncFile(d.file); err != nil {
			return d.markWriteUnavailable(fmt.Errorf("sync disk batch: %w", err))
		}
	}

	d.CurrentOffset += uint64(totalSize)
	lastOffset := batch[len(batch)-1].Offset

	newAbsOffset := lastOffset + 1
	for {
		currentAbs := atomic.LoadUint64(&d.AbsoluteOffset)
		if newAbsOffset <= currentAbs {
			break
		}
		if atomic.CompareAndSwapUint64(&d.AbsoluteOffset, currentAbs, newAbsOffset) {
			break
		}
	}

	// Update flushed offset so readers know data is on disk
	atomic.StoreUint64(&d.FlushedOffset, newAbsOffset)

	return nil
}

// WriteDirect writes a single message immediately without batching.
func (d *DiskHandler) WriteDirect(topic string, partition int, msg types.Message) error {
	if partition < 0 || partition > math.MaxInt32 {
		return fmt.Errorf("partition out of int32 range: %d", partition)
	}
	if err := d.writeAvailabilityError(); err != nil {
		return err
	}

	interval := d.indexInterval
	if interval == 0 {
		interval = 4096 // default interval (4KB)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	diskMsg := types.DiskMessage{
		Topic:                        topic,
		Partition:                    int32(partition),
		Offset:                       msg.Offset,
		ProducerID:                   msg.ProducerID,
		SeqNum:                       msg.SeqNum,
		Epoch:                        msg.Epoch,
		Payload:                      msg.Payload,
		Key:                          msg.Key,
		EventType:                    msg.EventType,
		SchemaVersion:                msg.SchemaVersion,
		AggregateVersion:             msg.AggregateVersion,
		Metadata:                     msg.Metadata,
		TransactionalID:              msg.TransactionalID,
		TransactionState:             msg.TransactionState,
		TransactionMarker:            msg.TransactionMarker,
		ControlBatchType:             msg.ControlBatchType,
		ControlBatchVersion:          msg.ControlBatchVersion,
		ControlBatchCoordinatorEpoch: msg.ControlBatchCoordinatorEpoch,
		ControlBatchKey:              msg.ControlBatchKey,
		ControlBatchValue:            msg.ControlBatchValue,
	}

	serialized, err := util.SerializeDiskMessage(diskMsg)
	if err != nil {
		return fmt.Errorf("serialize failed: %w", err)
	}
	if err := validateSerializedDiskMessageSize(serialized); err != nil {
		return err
	}

	serLen, ok := util.SafeIntToUint32(len(serialized))
	if !ok {
		return fmt.Errorf("message too large: %d bytes", len(serialized))
	}

	totalLen := uint64(4 + len(serialized))
	const entrySize = uint64(types.IndexEntrySize)
	msgPosition := d.CurrentOffset

	willWriteIndex := msgPosition-d.lastIndexPosition >= interval
	willExceedData := d.CurrentOffset+totalLen > d.SegmentSize
	willExceedIndex := willWriteIndex && (d.indexBytesWritten+entrySize > d.IndexSize)

	if willExceedData || willExceedIndex {
		util.Debug("rolling segment: data exceed=%v, index exceed=%v", willExceedData, willExceedIndex)
		if err := d.rotateSegment(msg.Offset); err != nil {
			return fmt.Errorf("rotateSegment failed: %w", err)
		}
		msgPosition = d.CurrentOffset
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], serLen)

	if _, err := d.writer.Write(lenBuf[:]); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("write record length: %w", err))
	}
	if _, err := d.writer.Write(serialized); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("write record payload: %w", err))
	}
	if err := d.writer.Flush(); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("flush failed: %w", err))
	}
	// WriteDirect backs acknowledged application writes, follower replication,
	// and consumer metadata. All must cross the filesystem sync boundary.
	if err := d.syncFile(d.file); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("sync disk log: %w", err))
	}

	d.CurrentOffset += totalLen
	newAbsOffset := msg.Offset + 1
	if msg.Offset >= atomic.LoadUint64(&d.AbsoluteOffset) {
		atomic.StoreUint64(&d.AbsoluteOffset, newAbsOffset)
	}
	atomic.StoreUint64(&d.FlushedOffset, newAbsOffset)

	if msgPosition-d.lastIndexPosition >= interval {
		indexEntry := types.IndexEntry{
			Offset:   msg.Offset,
			Position: msgPosition,
		}
		if d.indexWriter != nil {
			if err := binary.Write(d.indexWriter, binary.BigEndian, indexEntry); err != nil {
				return d.markWriteUnavailable(fmt.Errorf("failed to write index entry for offset %d: %w", msg.Offset, err))
			}
			if err := d.indexWriter.Flush(); err != nil {
				return d.markWriteUnavailable(fmt.Errorf("failed to flush index writer: %w", err))
			}
			if d.internalMetadata && d.indexFile != nil {
				if err := d.indexFile.Sync(); err != nil {
					return d.markWriteUnavailable(fmt.Errorf("sync internal metadata index: %w", err))
				}
			}
			d.lastIndexPosition = msgPosition
			d.indexBytesWritten += uint64(types.IndexEntrySize)
		}
	}
	return nil
}

// rotateSegment closes the current segment and opens a new one.
func (d *DiskHandler) rotateSegment(nextBaseOffset uint64) error {
	if nextBaseOffset <= d.CurrentSegment && d.CurrentOffset == 0 {
		return nil
	}

	var errs []error
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			util.Error("flush failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			util.Error("failed to sync disk file: %v", err)
			if d.internalMetadata {
				errs = append(errs, err)
			}
		}
		if err := d.file.Close(); err != nil {
			util.Error("close failed during rotation: %v", err)
			errs = append(errs, err)
		}
		d.file = nil
	}

	d.indexMu.Lock()
	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			util.Error("flush failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}
	if err := d.closeIndexFiles(); err != nil {
		util.Error("close index files failed during rotation: %v", err)
		errs = append(errs, err)
	}
	d.indexMu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("rotation completed with errors: %v", errs)
	}

	d.CurrentSegment = nextBaseOffset
	d.CurrentOffset = 0
	d.lastIndexPosition = 0
	d.indexBytesWritten = 0
	d.segmentCreatedAt = time.Now()

	d.segments = append(d.segments, nextBaseOffset)

	if err := d.openSegment(); err != nil {
		util.Error("Failed to open new segment: %v", err)
		return err
	}
	if err := d.openIndexFiles(); err != nil {
		return err
	}
	if d.internalMetadata {
		if err := syncDirectory(filepath.Dir(d.BaseName)); err != nil {
			return fmt.Errorf("sync internal metadata segment rotation: %w", err)
		}
	}
	return nil
}

// RollSegmentAt closes the active segment at the current logical tail. It is
// primarily used by replica maintenance and tests that need a stable closed
// segment boundary.
func (d *DiskHandler) RollSegmentAt(nextBaseOffset uint64) error {
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	if nextBaseOffset != atomic.LoadUint64(&d.AbsoluteOffset) {
		return fmt.Errorf("segment roll boundary %d does not match local tail %d", nextBaseOffset, atomic.LoadUint64(&d.AbsoluteOffset))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if err := d.rotateSegment(nextBaseOffset); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(d.BaseName))
}

// openSegment opens or creates the current segment file for writing.
func (d *DiskHandler) openSegment() error {
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND
	filePath := d.GetSegmentPath(d.CurrentSegment)

	// #nosec G304 -- filePath is generated by DiskHandler beneath its configured topic-partition root.
	f, err := os.OpenFile(filePath, flags, 0o600)
	if err != nil {
		return err
	}
	d.file = f
	d.writer = bufio.NewWriter(f)
	return nil
}

// Flush forces all pending data to be written and synced to disk.
// It signals the flushLoop to drain the write channel, avoiding a race condition
// where both Flush and flushLoop read from writeCh concurrently.
func (d *DiskHandler) Flush() {
	done := make(chan struct{})
	select {
	case d.flushSignal <- done:
		select {
		case <-done:
		case <-d.done:
		}
	case <-d.done:
		return
	}
}

// GetAbsoluteOffset returns the current absolute offset in a thread-safe manner
func (d *DiskHandler) GetAbsoluteOffset() uint64 {
	return atomic.LoadUint64(&d.AbsoluteOffset)
}

// GetFlushedOffset returns the offset up to which data has been written to disk
func (d *DiskHandler) GetFlushedOffset() uint64 {
	return atomic.LoadUint64(&d.FlushedOffset)
}

// GetCurrentSegment returns the current segment number in a thread-safe manner
func (d *DiskHandler) GetCurrentSegment() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.CurrentSegment
}

func (d *DiskHandler) drainAndShutdown(batch []types.DiskMessage) {
	var errs []error
	defer func() { d.drainErr = errors.Join(errs...) }()
	for {
		stop := false
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				stop = true
			} else {
				batch = append(batch, msg)
			}
		default:
			stop = true
		}

		if len(batch) >= d.batchSize {
			if err := d.WriteBatch(batch); err != nil {
				errs = append(errs, fmt.Errorf("drain batch: %w", err))
				util.Error("WriteBatch failed: %v", err)
			}
			batch = batch[:0]
		}

		if stop {
			break
		}
	}

	if len(batch) > 0 {
		if err := d.WriteBatch(batch); err != nil {
			errs = append(errs, fmt.Errorf("final batch: %w", err))
			util.Error("finalize WriteBatch failed: %v", err)
		}
	}

	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("final writer flush: %w", err))
			util.Error("writer flush failed: %v", err)
		}
		d.writer = nil
	}

	if d.file != nil {
		if err := d.syncFile(d.file); err != nil {
			errs = append(errs, fmt.Errorf("final file sync: %w", err))
			util.Error("file sync failed: %v", err)
		}
		if err := d.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("final file close: %w", err))
			util.Error("file close failed: %v", err)
		}
		d.file = nil
	}

	if err := d.closeIndexFiles(); err != nil {
		errs = append(errs, fmt.Errorf("final index close: %w", err))
		util.Error("close index files failed during shutdown: %v", err)
	}
}

func (d *DiskHandler) getSegmentTickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker != nil {
		return ticker.C
	}
	return nil
}
