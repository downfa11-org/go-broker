package disk

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type segmentRecovery struct {
	validBytes uint64
	nextOffset uint64
	modTime    int64
}

// recoverActiveSegment validates the active log tail and removes only a
// trailing partial record. Complete but malformed records fail startup.
func recoverActiveSegment(logPath, indexPath string, baseOffset uint64) (segmentRecovery, error) {
	return recoverActiveSegmentMode(logPath, indexPath, baseOffset, false)
}

func recoverActiveSegmentStrict(logPath, indexPath string, baseOffset uint64) (segmentRecovery, error) {
	return recoverActiveSegmentMode(logPath, indexPath, baseOffset, true)
}

func recoverActiveSegmentMode(logPath, indexPath string, baseOffset uint64, rejectPartialTail bool) (segmentRecovery, error) {
	info, err := os.Stat(logPath)
	if err != nil {
		return segmentRecovery{}, err
	}
	if info.Size() < 0 {
		return segmentRecovery{}, fmt.Errorf("negative segment size for %s", logPath)
	}
	validBytes, nextOffset, partial, err := scanSegmentTail(logPath, 0, baseOffset)
	if err != nil {
		return segmentRecovery{}, err
	}
	if partial {
		if rejectPartialTail {
			return segmentRecovery{}, fmt.Errorf("truncated internal metadata record at byte %d", validBytes)
		}
		if err := truncateAndSync(logPath, validBytes); err != nil {
			return segmentRecovery{}, fmt.Errorf("truncate partial segment tail: %w", err)
		}
	}

	return segmentRecovery{
		validBytes: validBytes,
		nextOffset: nextOffset,
		modTime:    info.ModTime().UnixNano(),
	}, nil
}

func scanSegmentTail(logPath string, startPosition, expectedOffset uint64) (uint64, uint64, bool, error) {
	// #nosec G304 -- logPath is derived from the broker-owned log directory.
	f, err := os.Open(logPath)
	if err != nil {
		return 0, 0, false, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, 0, false, err
	}
	if info.Size() < 0 {
		return 0, 0, false, fmt.Errorf("negative segment size %d", info.Size())
	}
	// #nosec G115 -- negative sizes are rejected above.
	logSize := uint64(info.Size())
	if startPosition > logSize {
		return 0, 0, false, fmt.Errorf("invalid scan position %d for segment size %d", startPosition, info.Size())
	}

	position := startPosition
	nextOffset := expectedOffset
	var lengthBytes [4]byte
	for position < logSize {
		remaining := logSize - position
		if remaining < uint64(len(lengthBytes)) {
			return position, nextOffset, true, nil
		}
		// #nosec G115 -- position is bounded by the int64-backed file size.
		if _, err := f.ReadAt(lengthBytes[:], int64(position)); err != nil {
			return 0, 0, false, fmt.Errorf("read record length at byte %d: %w", position, err)
		}
		messageLength := uint64(binary.BigEndian.Uint32(lengthBytes[:]))
		if messageLength == 0 || messageLength > MaxMessageSize {
			return 0, 0, false, fmt.Errorf("corrupt record length %d at byte %d", messageLength, position)
		}
		recordEnd := position + 4 + messageLength
		if recordEnd > logSize {
			return position, nextOffset, true, nil
		}
		if messageLength > uint64(^uint(0)>>1) {
			return 0, 0, false, fmt.Errorf("record length %d exceeds addressable memory", messageLength)
		}
		// #nosec G115 -- messageLength is capped at MaxMessageSize.
		data := make([]byte, int(messageLength))
		// #nosec G115 -- recordEnd was checked against the int64-backed file size.
		if _, err := f.ReadAt(data, int64(position+4)); err != nil {
			return 0, 0, false, fmt.Errorf("read record at byte %d: %w", position, err)
		}
		message, err := util.DeserializeDiskMessage(data)
		if err != nil {
			return 0, 0, false, fmt.Errorf("decode record at byte %d: %w", position, err)
		}
		if message.Offset != nextOffset {
			return 0, 0, false, fmt.Errorf("non-contiguous offset at byte %d: got %d, expected %d", position, message.Offset, nextOffset)
		}
		nextOffset++
		position = recordEnd
	}
	return position, nextOffset, false, nil
}

func indexEntryMatchesRecord(logPath string, entry types.IndexEntry) (bool, error) {
	// #nosec G304 -- logPath is derived from the broker-owned log directory.
	f, err := os.Open(logPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()

	if entry.Position > math.MaxInt64-4 {
		return false, nil
	}
	// #nosec G115 -- entry.Position is bounded above before conversion.
	recordPosition := int64(entry.Position)

	var lengthBytes [4]byte
	if _, err := f.ReadAt(lengthBytes[:], recordPosition); err != nil {
		return false, err
	}
	messageLength := binary.BigEndian.Uint32(lengthBytes[:])
	if messageLength == 0 || messageLength > MaxMessageSize {
		return false, nil
	}
	// #nosec G115 -- messageLength is capped at MaxMessageSize.
	data := make([]byte, int(messageLength))
	if _, err := f.ReadAt(data, recordPosition+4); err != nil {
		return false, err
	}
	message, err := util.DeserializeDiskMessage(data)
	if err != nil {
		return false, nil
	}
	return message.Offset == entry.Offset, nil
}

func truncateAndSync(path string, size uint64) error {
	if size > math.MaxInt64 {
		return fmt.Errorf("truncate size %d exceeds int64", size)
	}
	// #nosec G304 -- path is derived from the broker-owned log directory.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	// #nosec G115 -- size is bounded by math.MaxInt64 above.
	if err := f.Truncate(int64(size)); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}
