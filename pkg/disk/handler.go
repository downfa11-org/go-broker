package disk

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"golang.org/x/exp/mmap"
)

const MaxMessageSize = util.MaxMessageSize

type DiskHandler struct {
	BaseName       string
	SegmentSize    uint64
	IndexSize      uint64
	CurrentOffset  uint64
	AbsoluteOffset uint64
	CurrentSegment uint64
	segments       []uint64

	indexFile         *os.File
	indexWriter       *bufio.Writer
	indexMapper       *mmap.ReaderAt
	indexInterval     uint64
	indexBytesWritten uint64
	indexMu           sync.RWMutex
	activeReaders     int32
	lastIndexPosition uint64

	FlushedOffset uint64 // last offset confirmed written to disk

	writeCh     chan types.DiskMessage
	done        chan struct{}
	flushSignal chan chan struct{}
	onSyncMu    sync.RWMutex
	onSync      func(uint64)

	batchSize      int
	linger         time.Duration
	writeTimeout   time.Duration
	syncIntervalMS int

	segmentRollTime  time.Duration
	segmentCreatedAt time.Time

	retentionMu            sync.RWMutex
	retentionDefaults      retentionLimits
	retentionPolicy        retentionLimits
	retentionConfigured    bool
	cleanupPolicy          string
	minCleanableDirtyRatio float64
	distributed            bool
	internalMetadata       bool

	maintenanceMu     sync.Mutex
	compactionMu      sync.RWMutex
	compactedSegments map[uint64]int64
	compactionGateMu  sync.RWMutex
	compactionGate    func() (bool, string)
	compactionStateMu sync.Mutex
	compactionFailure string
	segmentReaders    *segmentReaderCache
	mu                sync.Mutex // metadata(offset, segment), file handler(d.file)
	ioMu              sync.Mutex // bufio.Writer, flush

	// appendMu serializes automatic offset allocation through its write.
	appendMu sync.Mutex

	writeFailureMu sync.RWMutex
	writeFailure   error
	syncFileFn     func(*os.File) error

	file   *os.File
	writer *bufio.Writer

	closeOnce sync.Once
	shutdown  sync.WaitGroup
}

func (d *DiskHandler) SetOnSync(callback func(uint64)) {
	d.onSyncMu.Lock()
	defer d.onSyncMu.Unlock()
	d.onSync = callback
}

func (d *DiskHandler) notifySync(offset uint64) {
	d.onSyncMu.RLock()
	callback := d.onSync
	d.onSyncMu.RUnlock()
	if callback != nil {
		callback(offset)
	}
}

func (d *DiskHandler) syncFile(file *os.File) error {
	if d.syncFileFn != nil {
		return d.syncFileFn(file)
	}
	return file.Sync()
}

func (d *DiskHandler) markWriteUnavailable(err error) error {
	if err == nil {
		return nil
	}
	d.writeFailureMu.Lock()
	if d.writeFailure == nil {
		d.writeFailure = err
	}
	failure := d.writeFailure
	d.writeFailureMu.Unlock()
	return fmt.Errorf("disk handler write unavailable until restart: %w", failure)
}

func (d *DiskHandler) writeAvailabilityError() error {
	d.writeFailureMu.RLock()
	failure := d.writeFailure
	d.writeFailureMu.RUnlock()
	if failure == nil {
		return nil
	}
	return fmt.Errorf("disk handler write unavailable until restart: %w", failure)
}

func (d *DiskHandler) GetActiveReaders() int32 {
	return atomic.LoadInt32(&d.activeReaders)
}

func NewDiskHandler(cfg *config.Config, topicName string, partitionID int) (*DiskHandler, error) {
	return newDiskHandler(cfg, topicName, partitionID, cfg.CleanupPolicy, retentionLimits{
		hours: cfg.RetentionHours,
		bytes: cfg.RetentionBytes,
	})
}

func newDiskHandlerWithPolicy(cfg *config.Config, topicName string, partitionID int, cleanupPolicy string, retentionHours int, retentionBytes int64) (*DiskHandler, error) {
	if cleanupPolicy == "" {
		cleanupPolicy = cfg.CleanupPolicy
	}
	if retentionHours == 0 {
		retentionHours = cfg.RetentionHours
	}
	if retentionBytes == 0 {
		retentionBytes = cfg.RetentionBytes
	}
	return newDiskHandler(cfg, topicName, partitionID, cleanupPolicy, retentionLimits{
		hours: retentionHours,
		bytes: retentionBytes,
	})
}

func newDiskHandler(cfg *config.Config, topicName string, partitionID int, cleanupPolicy string, initialRetention retentionLimits) (*DiskHandler, error) {
	base := fmt.Sprintf("%s%c%s%cpartition_%d", cfg.LogDir, os.PathSeparator, topicName, os.PathSeparator, partitionID)
	if err := os.MkdirAll(filepath.Dir(base), 0o750); err != nil {
		return nil, err
	}

	internalMetadata := topicName == config.ConsumerOffsetsTopicName
	standaloneInternalMetadata := internalMetadata && !cfg.EnabledDistribution
	tempDh := &DiskHandler{BaseName: base}
	// Recovery must run before generic compaction-temp cleanup because a pending
	// compacted range still owns its staged .range.compacting files.
	if err := recoverPendingCompactedRange(base); err != nil {
		return nil, fmt.Errorf("recover pending compacted replica range: %w", err)
	}
	if err := cleanupDeletedSegments(base); err != nil {
		return nil, fmt.Errorf("cleanup deleted segment tombstones: %w", err)
	}
	if err := cleanupCompactionTemps(base); err != nil {
		return nil, fmt.Errorf("cleanup compaction temporary files: %w", err)
	}
	compactedSegments, markerErr := loadCompactionMarkers(base)
	if markerErr != nil {
		return nil, fmt.Errorf("load compaction markers: %w", markerErr)
	}
	pattern := base + "_segment_*.log"
	files, _ := filepath.Glob(pattern)
	sort.Strings(files)

	var currentSegmentBase uint64
	var err error
	prefix := fmt.Sprintf("partition_%d_segment_", partitionID)
	var recovery segmentRecovery

	if len(files) > 0 {
		lastFile := files[len(files)-1]
		fileName := filepath.Base(lastFile)

		if strings.HasPrefix(fileName, prefix) {
			numStr := fileName[len(prefix) : len(fileName)-4]
			currentSegmentBase, err = strconv.ParseUint(numStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("critical: failed to parse last segment filename %s: %w", fileName, err)
			}
		}
		if standaloneInternalMetadata {
			recovery, err = recoverActiveSegmentStrict(lastFile, tempDh.GetIndexPath(currentSegmentBase), currentSegmentBase)
		} else {
			recovery, err = recoverActiveSegment(lastFile, tempDh.GetIndexPath(currentSegmentBase), currentSegmentBase)
		}
		if err != nil {
			return nil, fmt.Errorf("recover active segment %s: %w", lastFile, err)
		}
	}

	filePath := tempDh.GetSegmentPath(currentSegmentBase)
	// #nosec G304 -- filePath is generated by DiskHandler beneath its configured topic-partition root.
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		recovery.nextOffset = currentSegmentBase
	}
	normalizedCleanupPolicy, ok := config.NormalizeCleanupPolicy(cleanupPolicy)
	if !ok {
		normalizedCleanupPolicy = config.CleanupPolicyDelete
	}
	if internalMetadata {
		normalizedCleanupPolicy = config.CleanupPolicyCompact
		initialRetention = retentionLimits{}
	}
	minCleanableDirtyRatio := cfg.MinCleanableDirtyRatio
	if minCleanableDirtyRatio <= 0 || minCleanableDirtyRatio >= 1 {
		minCleanableDirtyRatio = 0.5
	}

	dh := &DiskHandler{
		BaseName:       base,
		SegmentSize:    uint64(cfg.SegmentSize),
		IndexSize:      uint64(cfg.IndexSize),
		CurrentSegment: currentSegmentBase,
		segments:       make([]uint64, 0),
		CurrentOffset:  recovery.validBytes,
		AbsoluteOffset: recovery.nextOffset,
		FlushedOffset:  recovery.nextOffset,

		indexInterval: safeIntToUint64(cfg.IndexIntervalBytes),

		writeCh:        make(chan types.DiskMessage, cfg.ChannelBufferSize),
		done:           make(chan struct{}),
		flushSignal:    make(chan chan struct{}, 1),
		batchSize:      cfg.DiskFlushBatchSize,
		linger:         time.Duration(cfg.LingerMS) * time.Millisecond,
		writeTimeout:   time.Duration(cfg.DiskWriteTimeoutMS) * time.Millisecond,
		syncIntervalMS: cfg.DiskFlushIntervalMS,

		segmentRollTime:        time.Duration(cfg.SegmentRollTimeMS) * time.Millisecond,
		segmentCreatedAt:       time.Now(),
		retentionDefaults:      retentionLimits{hours: cfg.RetentionHours, bytes: cfg.RetentionBytes},
		retentionPolicy:        initialRetention,
		retentionConfigured:    true,
		cleanupPolicy:          normalizedCleanupPolicy,
		minCleanableDirtyRatio: minCleanableDirtyRatio,
		distributed:            cfg.EnabledDistribution,
		internalMetadata:       internalMetadata,
		compactedSegments:      compactedSegments,
		segmentReaders:         newSegmentReaderCache(defaultSegmentReaderCacheEntries),
		file:                   file,
		writer:                 bufio.NewWriter(file),
	}
	if recovery.modTime != 0 {
		dh.segmentCreatedAt = time.Unix(0, recovery.modTime)
	}

	if err := dh.openIndexFiles(); err != nil {
		return nil, err
	}
	if internalMetadata {
		// A successful metadata append may be the first record in this
		// partition. Persist the segment/index directory entries before the
		// coordinator can acknowledge that record.
		if err := syncDirectory(filepath.Dir(base)); err != nil {
			_ = dh.closeIndexFiles()
			_ = file.Close()
			return nil, fmt.Errorf("sync internal metadata partition directory: %w", err)
		}
	}

	for _, f := range files {
		fileName := filepath.Base(f)
		if strings.HasPrefix(fileName, prefix) {
			numStr := fileName[len(prefix) : len(fileName)-4]
			segBase, err := strconv.ParseUint(numStr, 10, 64)
			if err != nil {
				util.Error("skipping invalid segment file %s: %v", fileName, err)
				continue
			}
			dh.segments = append(dh.segments, segBase)
		}
	}

	if len(dh.segments) == 0 {
		dh.segments = append(dh.segments, currentSegmentBase)
	}
	sort.Slice(dh.segments, func(i, j int) bool { return dh.segments[i] < dh.segments[j] })

	dh.shutdown.Add(2)
	go func() {
		defer dh.shutdown.Done()
		dh.flushLoop()
	}()

	go func() {
		defer dh.shutdown.Done()
		dh.syncLoop()
	}()

	dh.shutdown.Add(1)
	go func() {
		defer dh.shutdown.Done()
		dh.retentionLoop(cfg)
	}()

	return dh, nil
}

func (d *DiskHandler) AppendMessageSync(topic string, partition int, msg *types.Message) (uint64, error) {
	if partition < 0 || partition > math.MaxInt32 {
		return 0, fmt.Errorf("partition out of int32 range: %d", partition)
	}
	if err := d.writeAvailabilityError(); err != nil {
		return 0, err
	}
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	offset := atomic.LoadUint64(&d.AbsoluteOffset)
	msg.Offset = offset
	diskMsg := diskMessageFromMessage(topic, int32(partition), *msg)
	if err := validateDiskMessageSize(diskMsg); err != nil {
		return 0, err
	}
	select {
	case <-d.done:
		return 0, fmt.Errorf("disk handler is shutting down")
	case d.writeCh <- diskMsg:
	}
	d.Flush()
	select {
	case <-d.done:
		return 0, fmt.Errorf("disk handler is shutting down")
	default:
	}
	if err := d.writeAvailabilityError(); err != nil {
		return 0, err
	}
	atomic.StoreUint64(&d.AbsoluteOffset, offset+1)
	return offset, nil
}

func diskMessageFromMessage(topic string, partition int32, msg types.Message) types.DiskMessage {
	return types.DiskMessage{
		Topic:                        topic,
		Partition:                    partition,
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
}

// AppendMessageWithOffset writes a message with a pre-assigned offset (for follower replication).
// Unlike AppendMessage/AppendMessageSync, it does NOT allocate a new offset.
func (d *DiskHandler) AppendMessageWithOffset(topic string, partition int, msg *types.Message) error {
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	if err := d.WriteDirect(topic, partition, *msg); err != nil {
		return fmt.Errorf("WriteDirect failed: %w", err)
	}
	for {
		current := atomic.LoadUint64(&d.AbsoluteOffset)
		newOffset := msg.Offset + 1
		if newOffset <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&d.AbsoluteOffset, current, newOffset) {
			break
		}
	}
	return nil
}

// AppendMessage sends a message to the internal write channel for asynchronous disk persistence.
func (d *DiskHandler) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	if partition < 0 || partition > math.MaxInt32 {
		return 0, fmt.Errorf("partition out of int32 range: %d", partition)
	}
	if err := d.writeAvailabilityError(); err != nil {
		return 0, err
	}
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	offset := atomic.LoadUint64(&d.AbsoluteOffset)

	msg.Offset = offset
	diskMsg := diskMessageFromMessage(topic, int32(partition), *msg)
	if err := validateDiskMessageSize(diskMsg); err != nil {
		return 0, err
	}

	if d.writeTimeout > 0 {
		timer := time.NewTimer(d.writeTimeout)
		defer timer.Stop()

		select {
		case <-d.done:
			util.Debug("done channel closed for %s", d.BaseName)
			return 0, fmt.Errorf("disk handler is shutting down")
		case d.writeCh <- diskMsg:
			atomic.StoreUint64(&d.AbsoluteOffset, offset+1)
			return diskMsg.Offset, nil
		case <-timer.C:
			util.Error("enqueue timed out after %s for topic %s", d.writeTimeout, topic)
			return 0, fmt.Errorf("enqueue timeout after %s", d.writeTimeout)
		}
	} else {
		select {
		case <-d.done:
			return 0, fmt.Errorf("disk handler is shutting down")
		case d.writeCh <- diskMsg:
			atomic.StoreUint64(&d.AbsoluteOffset, offset+1)
			return diskMsg.Offset, nil
		}
	}
}

// ReadMessages reads a batch of messages from the disk log, starting from the given offset.
func (dh *DiskHandler) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	atomic.AddInt32(&dh.activeReaders, 1)
	defer atomic.AddInt32(&dh.activeReaders, -1)

	if max <= 0 {
		return nil, nil
	}
	_, targetSeg, err := dh.findSegmentForOffset(offset)
	if err != nil {
		util.Error("Segment not found for offset %d", offset)
		return nil, err
	}

	position, err := dh.findOffsetPosition(offset, targetSeg)
	if err != nil {
		util.Error("Position not found for offset %d", offset)
		return nil, err
	}

	dh.mu.Lock()
	currentSegment := dh.CurrentSegment
	readableSegments := make([]uint64, len(dh.segments))
	copy(readableSegments, dh.segments)
	if len(readableSegments) == 0 || readableSegments[len(readableSegments)-1] != currentSegment {
		readableSegments = append(readableSegments, currentSegment)
	}
	dh.mu.Unlock()

	var messages []types.Message
	startReading := false

	for _, segBase := range readableSegments {
		if !startReading {
			if segBase == targetSeg {
				startReading = true
			} else {
				continue
			}
		}

		currentFile := dh.GetSegmentPath(segBase)
		fi, err := os.Stat(currentFile)
		if err != nil {
			return nil, fmt.Errorf("stat segment %d: %w", segBase, err)
		}

		actualSize := fi.Size()
		if actualSize <= 0 {
			util.Debug("Segment %d is currently empty, skipping", segBase)
			continue
		}

		var lease *segmentReaderLease
		if segBase == currentSegment {
			// mmap.ReaderAt captures the file length at open time. Never cache the
			// active segment because an append would leave the mapping stale.
			reader, openErr := mmap.Open(currentFile)
			if openErr != nil {
				return nil, fmt.Errorf("open segment %d: %w", segBase, openErr)
			}
			lease = &segmentReaderLease{reader: reader}
		} else {
			lease, err = dh.segmentReaders.acquire(segBase, currentFile)
		}
		if err != nil {
			return nil, fmt.Errorf("open segment %d: %w", segBase, err)
		}
		reader := lease.Reader()

		remaining := max - len(messages)
		readPos := uint64(0)
		if segBase == targetSeg {
			readPos = position
		}

		activeSegment := segBase == currentSegment
		allowOffsetGaps := !activeSegment && dh.segmentAllowsOffsetGaps(segBase, actualSize)
		batch, readErr := dh.readMessagesFromPosition(reader, readPos, remaining, offset, segBase, activeSegment, allowOffsetGaps)
		if readErr != nil {
			_ = lease.Close()
			return nil, fmt.Errorf("read segment %d: %w", segBase, readErr)
		}
		if activeSegment && len(batch) == 0 && actualSize > 0 && uint64(actualSize) > readPos+4 {
			if err := lease.Close(); err != nil {
				util.Debug("error closing reader: %v", err)
			}

			reader, reErr := mmap.Open(currentFile)
			if reErr != nil {
				return nil, fmt.Errorf("reopen segment %d: %w", segBase, reErr)
			}
			lease = &segmentReaderLease{reader: reader}

			batch, readErr = dh.readMessagesFromPosition(reader, readPos, remaining, offset, segBase, activeSegment, allowOffsetGaps)
			if readErr != nil {
				_ = lease.Close()
				return nil, fmt.Errorf("reread segment %d: %w", segBase, readErr)
			}
		}

		messages = append(messages, batch...)
		if err := lease.Close(); err != nil {
			util.Debug("error closing reader: %v", err)
		}

		if len(messages) >= max {
			break
		}
		if len(messages) > 0 {
			offset = messages[len(messages)-1].Offset + 1
		}
	}

	if len(messages) > 0 {
		first := messages[0].Offset
		last := messages[len(messages)-1].Offset
		util.Debug("Success: From=%d To=%d (Count=%d, Range: [%d-%d])", first, last, len(messages), messages[0].Offset, messages[len(messages)-1].Offset)
	}
	return messages, nil
}

// readMessagesFromPosition reads messages starting from a specific byte position
func (dh *DiskHandler) readMessagesFromPosition(reader *mmap.ReaderAt, position uint64, max int, targetOffset, segmentBase uint64, allowPartialTail, allowOffsetGaps bool) ([]types.Message, error) {
	if position > math.MaxInt {
		return nil, fmt.Errorf("read position %d exceeds int range", position)
	}
	messages := make([]types.Message, 0, max)
	pos := int(position)
	var lenBuf [4]byte
	var dataBuf []byte
	var previousOffset uint64
	havePreviousOffset := false

	for len(messages) < max && pos+4 <= reader.Len() {
		if _, err := reader.ReadAt(lenBuf[:], int64(pos)); err != nil {
			return nil, fmt.Errorf("read length at byte %d: %w", pos, err)
		}

		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		if msgLen == 0 || msgLen > MaxMessageSize {
			return nil, fmt.Errorf("corrupt message length %d at byte %d", msgLen, pos)
		}

		if pos+4+int(msgLen) > reader.Len() {
			if allowPartialTail {
				return messages, nil
			}
			return nil, fmt.Errorf("truncated message at byte %d: expected %d payload bytes", pos, msgLen)
		}

		if cap(dataBuf) < int(msgLen) {
			dataBuf = make([]byte, msgLen)
		} else {
			dataBuf = dataBuf[:msgLen]
		}
		if _, err := reader.ReadAt(dataBuf, int64(pos+4)); err != nil {
			return nil, fmt.Errorf("read payload at byte %d: %w", pos, err)
		}

		diskMsg, err := util.DeserializeDiskMessage(dataBuf)
		if err != nil {
			return nil, dh.markWriteUnavailable(fmt.Errorf("decode message at byte %d: %w", pos, err))
		}
		if !havePreviousOffset && pos == 0 && !allowOffsetGaps && diskMsg.Offset != segmentBase {
			return nil, fmt.Errorf("unexpected first offset at byte %d: got %d for segment base %d", pos, diskMsg.Offset, segmentBase)
		}
		if havePreviousOffset && diskMsg.Offset <= previousOffset {
			return nil, fmt.Errorf("non-increasing offset at byte %d: got %d after %d", pos, diskMsg.Offset, previousOffset)
		}
		if havePreviousOffset && !allowOffsetGaps && diskMsg.Offset != previousOffset+1 {
			return nil, fmt.Errorf("non-contiguous offset at byte %d: got %d after %d", pos, diskMsg.Offset, previousOffset)
		}
		previousOffset = diskMsg.Offset
		havePreviousOffset = true

		if diskMsg.Offset < targetOffset {
			pos += 4 + int(msgLen)
			continue
		}

		messages = append(messages, types.Message{
			Offset:                       diskMsg.Offset,
			Payload:                      diskMsg.Payload,
			ProducerID:                   diskMsg.ProducerID,
			SeqNum:                       diskMsg.SeqNum,
			Epoch:                        diskMsg.Epoch,
			Key:                          diskMsg.Key,
			EventType:                    diskMsg.EventType,
			SchemaVersion:                diskMsg.SchemaVersion,
			AggregateVersion:             diskMsg.AggregateVersion,
			Metadata:                     diskMsg.Metadata,
			TransactionalID:              diskMsg.TransactionalID,
			TransactionState:             diskMsg.TransactionState,
			TransactionMarker:            diskMsg.TransactionMarker,
			ControlBatchType:             diskMsg.ControlBatchType,
			ControlBatchVersion:          diskMsg.ControlBatchVersion,
			ControlBatchCoordinatorEpoch: diskMsg.ControlBatchCoordinatorEpoch,
			ControlBatchKey:              diskMsg.ControlBatchKey,
			ControlBatchValue:            diskMsg.ControlBatchValue,
		})
		pos += 4 + int(msgLen)
	}
	if len(messages) < max && pos < reader.Len() {
		if allowPartialTail {
			return messages, nil
		}
		return nil, fmt.Errorf("truncated record length at byte %d", pos)
	}
	return messages, nil
}

func (d *DiskHandler) countMessagesInSegment() (int, error) {
	return d.countMessagesInSegmentID(d.CurrentSegment)
}

func safeIntToUint64(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func (d *DiskHandler) countMessagesInSegmentID(segmentID uint64) (int, error) {
	filePath := d.GetSegmentPath(segmentID)
	reader, err := mmap.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("mmap open failed: %w", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			util.Error("failed to close: %v", err)
		}
	}()

	count := 0
	pos := 0
	for pos < reader.Len() {
		if pos+4 > reader.Len() {
			break
		}

		lenBytes := make([]byte, 4)
		_, err := reader.ReadAt(lenBytes, int64(pos))
		if err != nil {
			break
		}

		msgLen := binary.BigEndian.Uint32(lenBytes)
		if msgLen == 0 || msgLen > MaxMessageSize {
			util.Error("Corrupted data: invalid msgLen %d at pos %d", msgLen, pos)
			break
		}

		if pos+4+int(msgLen) > reader.Len() {
			break
		}
		pos += 4 + int(msgLen)
		count++
	}
	return count, nil
}
