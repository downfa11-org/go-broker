package disk

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"golang.org/x/exp/mmap"
)

func (d *DiskHandler) openIndexFiles() error {
	if d.indexFile != nil {
		if err := d.indexFile.Close(); err != nil {
			return fmt.Errorf("failed to close existing index file: %w", err)
		}
		d.indexFile = nil
	}
	indexPath := d.GetIndexPath(d.CurrentSegment)

	info, statErr := os.Stat(indexPath)
	var isNew bool
	var existingSize uint64
	if os.IsNotExist(statErr) {
		isNew = true
	} else if statErr == nil && info.Size() >= 0 {
		// #nosec G115 -- the file size is explicitly non-negative above.
		existingSize = uint64(info.Size())
	}

	isReadOnly := false
	// #nosec G304 -- indexPath is generated from this handler's broker-owned base name and numeric segment ID.
	f, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		if os.IsPermission(err) {
			util.Debug("Index file %s is read-only, opening in read-only mode", indexPath)
			// #nosec G304 -- indexPath is derived from the broker-owned log directory.
			f, err = os.Open(indexPath)
			if err != nil {
				return fmt.Errorf("failed to open read-only index: %w", err)
			}
			isReadOnly = true
		} else {
			return fmt.Errorf("failed to open index file: %w", err)
		}
	}

	if !isReadOnly {
		if d.IndexSize > math.MaxInt64 {
			return fmt.Errorf("index size %d exceeds int64 max", d.IndexSize)
		}
		// #nosec G115 -- d.IndexSize is bounded by math.MaxInt64 above.
		if err := f.Truncate(int64(d.IndexSize)); err != nil {
			if err := f.Close(); err != nil {
				util.Error("failed to close file: %v", err)
			}
			return fmt.Errorf("failed to truncate index file: %w", err)
		}
	}

	d.indexMu.Lock()
	defer d.indexMu.Unlock()

	validBytes := uint64(0)
	lastPosition := uint64(0)
	if !isNew {
		validBytes, lastPosition = d.validIndexPrefix(f, existingSize)
	}
	d.indexFile = f
	d.indexBytesWritten = validBytes
	d.lastIndexPosition = lastPosition

	if !isReadOnly {
		if validBytes > math.MaxInt64 {
			return fmt.Errorf("valid index prefix %d exceeds int64 max", validBytes)
		}
		if existingSize > validBytes {
			// #nosec G115 -- validBytes is bounded by math.MaxInt64 above.
			if err := f.Truncate(int64(validBytes)); err != nil {
				return fmt.Errorf("discard invalid index suffix: %w", err)
			}
			// #nosec G115 -- d.IndexSize is bounded by math.MaxInt64 above.
			if err := f.Truncate(int64(d.IndexSize)); err != nil {
				return fmt.Errorf("restore index capacity: %w", err)
			}
		}
		// #nosec G115 -- validBytes is bounded by math.MaxInt64 above.
		if _, err := f.Seek(int64(validBytes), io.SeekStart); err != nil {
			return fmt.Errorf("seek index append position: %w", err)
		}
		d.indexWriter = bufio.NewWriter(f)
	}

	return d.refreshIndexMapper(indexPath)
}

func (d *DiskHandler) validIndexPrefix(f *os.File, fileSize uint64) (uint64, uint64) {
	limit := fileSize
	if limit > d.IndexSize {
		limit = d.IndexSize
	}
	limit -= limit % uint64(types.IndexEntrySize)
	var entryBytes [types.IndexEntrySize]byte
	var validBytes, lastOffset, lastPosition uint64
	for position := uint64(0); position < limit; position += uint64(types.IndexEntrySize) {
		if position > math.MaxInt64 {
			break
		}
		// #nosec G115 -- position is bounded by math.MaxInt64 above.
		if _, err := f.ReadAt(entryBytes[:], int64(position)); err != nil {
			break
		}
		offset := binary.BigEndian.Uint64(entryBytes[0:8])
		logPosition := binary.BigEndian.Uint64(entryBytes[8:16])
		if offset == 0 && logPosition == 0 {
			break
		}
		if validBytes > 0 && (offset <= lastOffset || logPosition <= lastPosition) {
			break
		}
		if logPosition >= d.CurrentOffset {
			break
		}
		validBytes = position + uint64(types.IndexEntrySize)
		lastOffset = offset
		lastPosition = logPosition
	}
	return validBytes, lastPosition
}

func (d *DiskHandler) seekLastValidIndexEntry(f *os.File, fileSize uint64) uint64 {
	const entrySize = uint64(types.IndexEntrySize)
	if fileSize < entrySize {
		return 0
	}

	const scanBlockSize = uint64(64 * 1024)
	end := fileSize - fileSize%entrySize
	for end > 0 {
		start := uint64(0)
		if end > scanBlockSize {
			start = end - scanBlockSize
		}
		start -= start % entrySize
		if start > math.MaxInt64 || end-start > math.MaxInt {
			return 0
		}

		buf := make([]byte, int(end-start))
		n, err := f.ReadAt(buf, int64(start))
		if err != nil && n == 0 {
			return 0
		}
		usable := n - n%int(entrySize)
		for pos := usable - int(entrySize); pos >= 0; pos -= int(entrySize) {
			entry := buf[pos : pos+int(entrySize)]
			if binary.BigEndian.Uint64(entry[0:8]) != 0 || binary.BigEndian.Uint64(entry[8:16]) != 0 {
				return start + uint64(pos) + entrySize
			}
		}
		end = start
	}
	return 0
}

func getLastOffsetFromIndex(indexPath string, baseOffset uint64) (lastOffset uint64, count int, err error) {
	entry, found := lastUsableIndexEntry(indexPath, math.MaxUint64)
	if !found {
		return 0, 0, fmt.Errorf("index empty (contains only zeros)")
	}
	if entry.Offset < baseOffset {
		return 0, 0, fmt.Errorf("index empty or corrupt")
	}
	diff := entry.Offset - baseOffset + 1
	if diff > math.MaxInt {
		return 0, 0, fmt.Errorf("offset range too large: %d", diff)
	}
	return entry.Offset, int(diff), nil
}

func lastUsableIndexEntry(indexPath string, logSize uint64) (types.IndexEntry, bool) {
	// #nosec G304 -- indexPath is derived from the broker-owned log directory.
	f, err := os.Open(indexPath)
	if err != nil {
		return types.IndexEntry{}, false
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || info.Size() < types.IndexEntrySize {
		return types.IndexEntry{}, false
	}

	const entrySize = int64(types.IndexEntrySize)
	const blockSize = int64(64 * 1024)
	end := info.Size() - (info.Size() % entrySize)
	for end > 0 {
		start := end - blockSize
		if start < 0 {
			start = 0
		}
		start -= start % entrySize
		buf := make([]byte, end-start)
		if _, err := f.ReadAt(buf, start); err != nil && err != io.EOF {
			return types.IndexEntry{}, false
		}
		for pos := len(buf) - types.IndexEntrySize; pos >= 0; pos -= types.IndexEntrySize {
			entry := types.IndexEntry{
				Offset:   binary.BigEndian.Uint64(buf[pos : pos+8]),
				Position: binary.BigEndian.Uint64(buf[pos+8 : pos+16]),
			}
			if entry.Offset == 0 && entry.Position == 0 {
				continue
			}
			if entry.Position < logSize {
				return entry, true
			}
		}
		end = start
	}
	return types.IndexEntry{}, false
}

func (d *DiskHandler) refreshIndexMapper(path string) error {
	if d.indexMapper != nil {
		if err := d.indexMapper.Close(); err != nil {
			util.Error("failed to close indexMapper: %v", err)
		}
	}
	mapper, err := mmap.Open(path)
	if err != nil {
		return err
	}
	d.indexMapper = mapper
	return nil
}

func (d *DiskHandler) closeIndexFiles() error {
	var errs []error

	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush index writer failed: %w", err))
		}
	}
	if d.indexFile != nil {
		if err := d.indexFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync index file failed: %w", err))
		}
	}

	if d.indexMapper != nil {
		if err := d.indexMapper.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexMapper = nil
	}

	if d.indexFile != nil {
		if err := d.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexFile = nil
	}
	d.indexWriter = nil

	return errors.Join(errs...)
}

// findSegmentForOffset finds which segment contains the given offset
func (dh *DiskHandler) findSegmentForOffset(offset uint64) (string, uint64, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	earliest := dh.CurrentSegment
	if len(dh.segments) > 0 {
		earliest = dh.segments[0]
	}
	if offset < earliest {
		return "", 0, &types.OffsetOutOfRangeError{Requested: offset, Earliest: earliest, Latest: dh.AbsoluteOffset}
	}
	if offset > dh.AbsoluteOffset {
		return "", 0, &types.OffsetOutOfRangeError{Requested: offset, Earliest: earliest, Latest: dh.AbsoluteOffset}
	}

	if offset >= dh.CurrentSegment {
		return dh.GetSegmentPath(dh.CurrentSegment), dh.CurrentSegment, nil
	}

	if len(dh.segments) == 0 {
		return "", 0, fmt.Errorf("no segments available")
	}

	idx := sort.Search(len(dh.segments), func(i int) bool {
		return dh.segments[i] > offset
	})

	var baseOffset uint64
	if idx > 0 {
		baseOffset = dh.segments[idx-1]
	} else {
		baseOffset = dh.segments[0]
	}
	return dh.GetSegmentPath(baseOffset), baseOffset, nil
}

func (d *DiskHandler) findOffsetPosition(offset uint64, segmentBase ...uint64) (uint64, error) {
	base := d.CurrentSegment
	if len(segmentBase) > 0 {
		base = segmentBase[0]
	}
	if base != d.CurrentSegment {
		return d.findOffsetPositionInIndex(offset, base)
	}

	d.indexMu.RLock()
	defer d.indexMu.RUnlock()
	entry, found, err := findIndexEntry(d.indexMapper, d.indexBytesWritten, offset)
	if err != nil || !found {
		return 0, err
	}
	valid, validateErr := indexEntryMatchesRecord(d.GetSegmentPath(base), entry)
	if validateErr != nil || !valid {
		return 0, nil
	}
	return entry.Position, nil
}

func (d *DiskHandler) findOffsetPositionInIndex(offset, base uint64) (uint64, error) {
	indexPath := d.GetIndexPath(base)
	info, err := os.Stat(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if info.Size() == 0 {
		return 0, nil
	}

	mapper, err := mmap.Open(indexPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = mapper.Close() }()
	entry, found, err := findIndexEntry(mapper, indexValidBytes(mapper), offset)
	if err != nil || !found {
		return 0, err
	}
	valid, validateErr := indexEntryMatchesRecord(d.GetSegmentPath(base), entry)
	if validateErr != nil || !valid {
		return 0, nil
	}
	return entry.Position, nil
}

type indexReaderAt interface {
	ReadAt([]byte, int64) (int, error)
}

func findIndexEntry(reader indexReaderAt, validBytes uint64, offset uint64) (types.IndexEntry, bool, error) {

	if reader == nil {
		return types.IndexEntry{}, false, fmt.Errorf("index not available")
	}

	const entrySize = types.IndexEntrySize
	entryCount := validBytes / entrySize

	if entryCount == 0 {
		return types.IndexEntry{}, false, nil
	}

	if entryCount > math.MaxInt {
		return types.IndexEntry{}, false, fmt.Errorf("entry count too large: %d", entryCount)
	}
	low, high := 0, int(entryCount)-1
	var candidate types.IndexEntry
	found := false
	buf := make([]byte, entrySize)

	for low <= high {
		mid := (low + high) / 2
		_, err := reader.ReadAt(buf, int64(mid*entrySize))
		if err != nil {
			return types.IndexEntry{}, false, err
		}

		eOffset := binary.BigEndian.Uint64(buf[0:8])
		ePosition := binary.BigEndian.Uint64(buf[8:16])

		if eOffset == offset {
			return types.IndexEntry{Offset: eOffset, Position: ePosition}, true, nil
		} else if eOffset < offset {
			candidate = types.IndexEntry{Offset: eOffset, Position: ePosition}
			found = true
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return candidate, found, nil
}

func indexValidBytes(reader *mmap.ReaderAt) uint64 {
	if reader == nil || reader.Len() < types.IndexEntrySize {
		return 0
	}
	var bytes [types.IndexEntrySize]byte
	var valid uint64
	var lastOffset, lastPosition uint64
	for position := 0; position+types.IndexEntrySize <= reader.Len(); position += types.IndexEntrySize {
		if _, err := reader.ReadAt(bytes[:], int64(position)); err != nil {
			break
		}
		offset := binary.BigEndian.Uint64(bytes[0:8])
		logPosition := binary.BigEndian.Uint64(bytes[8:16])
		if offset == 0 && logPosition == 0 {
			break
		}
		if valid > 0 && (offset <= lastOffset || logPosition <= lastPosition) {
			break
		}
		valid += types.IndexEntrySize
		lastOffset = offset
		lastPosition = logPosition
	}
	return valid
}
