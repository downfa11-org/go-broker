package transaction

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	maxJournalRecordBytes    = 32 << 20
	journalFormatVersion     = 1
	journalRecordOverhead    = 8
	journalCompactionBytes   = 16 << 20
	journalCompactionRecords = 256
)

type journalRecord struct {
	Version     int       `json:"version"`
	Transaction *Snapshot `json:"transaction"`
}

// Journal durably appends standalone transaction coordinator snapshots.
type Journal struct {
	mu               sync.Mutex
	path             string
	validEnd         int64
	loaded           bool
	latest           map[string]*Snapshot
	records          int
	compactedBytes   int64
	compactedRecords int
}

func OpenJournal(path string) (*Journal, error) {
	if path == "" {
		return nil, fmt.Errorf("transaction journal path is empty")
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create transaction journal directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open transaction journal: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close transaction journal: %w", err)
	}
	return &Journal{path: path}, nil
}

func (j *Journal) Append(snap *Snapshot) (err error) {
	if snap == nil || snap.ID == "" {
		return fmt.Errorf("invalid transaction snapshot")
	}
	payload, err := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
	if err != nil {
		return fmt.Errorf("marshal transaction snapshot: %w", err)
	}
	payloadLen := len(payload)
	if payloadLen == 0 || payloadLen > maxJournalRecordBytes {
		return fmt.Errorf("transaction snapshot size %d exceeds journal limit", payloadLen)
	}

	var header [4]byte
	payloadSize := uint32(payloadLen) // #nosec G115 -- bounded by maxJournalRecordBytes above.
	binary.BigEndian.PutUint32(header[:], payloadSize)
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))

	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.loaded {
		if _, err := j.loadLocked(); err != nil {
			return fmt.Errorf("recover transaction journal before append: %w", err)
		}
	}
	if j.shouldCompactLocked() {
		if err := j.compactLocked(); err != nil {
			return fmt.Errorf("compact transaction journal: %w", err)
		}
	}

	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open transaction journal for append: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	if err := file.Truncate(j.validEnd); err != nil {
		return fmt.Errorf("truncate unacknowledged transaction journal tail: %w", err)
	}
	if _, err := file.Seek(j.validEnd, io.SeekStart); err != nil {
		return fmt.Errorf("seek transaction journal append position: %w", err)
	}
	if err := writeFull(file, header[:]); err != nil {
		return fmt.Errorf("append transaction journal header: %w", err)
	}
	if err := writeFull(file, payload); err != nil {
		return fmt.Errorf("append transaction journal payload: %w", err)
	}
	if err := writeFull(file, checksum[:]); err != nil {
		return fmt.Errorf("append transaction journal checksum: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync transaction journal: %w", err)
	}
	j.validEnd += int64(journalRecordOverhead) + int64(payloadLen)
	j.latest[snap.ID] = snapshot(transactionFromSnapshot(snap))
	j.records++
	return nil
}

func (j *Journal) shouldCompactLocked() bool {
	return j.records-j.compactedRecords >= journalCompactionRecords || j.validEnd-j.compactedBytes >= journalCompactionBytes
}

// Rewrite atomically replaces the journal with the supplied authoritative
// transaction state. Callers use this after pruning expired tombstones so a
// removed transactional ID cannot be resurrected during restart recovery.
func (j *Journal) Rewrite(state map[string]*Snapshot) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.loaded {
		if _, err := j.loadLocked(); err != nil {
			return fmt.Errorf("recover transaction journal before rewrite: %w", err)
		}
	}

	next := make(map[string]*Snapshot, len(state))
	for id, snap := range state {
		if snap == nil || id == "" || snap.ID != id {
			return fmt.Errorf("invalid transaction snapshot for %q during rewrite", id)
		}
		next[id] = snapshot(transactionFromSnapshot(snap))
	}
	j.latest = next
	if err := j.compactLocked(); err != nil {
		return fmt.Errorf("rewrite transaction journal: %w", err)
	}
	return nil
}

func (j *Journal) compactLocked() (err error) {
	dir := filepath.Dir(j.path)
	temp, err := os.CreateTemp(dir, filepath.Base(j.path)+".compact-*")
	if err != nil {
		return fmt.Errorf("create compacted transaction journal: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		if temp != nil {
			err = errors.Join(err, temp.Close())
		}
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove transaction journal temp file: %w", removeErr))
		}
	}()

	ids := make([]string, 0, len(j.latest))
	for id := range j.latest {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var compactedSize int64
	for _, id := range ids {
		snap := j.latest[id]
		if snap == nil || snap.ID == "" {
			return fmt.Errorf("invalid transaction snapshot for %q during compaction", id)
		}
		payload, marshalErr := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
		if marshalErr != nil {
			return fmt.Errorf("marshal transaction snapshot %q during compaction: %w", id, marshalErr)
		}
		if len(payload) == 0 || len(payload) > maxJournalRecordBytes {
			return fmt.Errorf("transaction snapshot %q size %d exceeds journal limit", id, len(payload))
		}
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload))) // #nosec G115 -- bounded above.
		var checksum [4]byte
		binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))
		if writeErr := writeFull(temp, header[:]); writeErr != nil {
			return fmt.Errorf("write compacted transaction journal header: %w", writeErr)
		}
		if writeErr := writeFull(temp, payload); writeErr != nil {
			return fmt.Errorf("write compacted transaction journal payload: %w", writeErr)
		}
		if writeErr := writeFull(temp, checksum[:]); writeErr != nil {
			return fmt.Errorf("write compacted transaction journal checksum: %w", writeErr)
		}
		compactedSize += int64(journalRecordOverhead + len(payload))
	}
	if syncErr := temp.Sync(); syncErr != nil {
		return fmt.Errorf("sync compacted transaction journal: %w", syncErr)
	}
	if closeErr := temp.Close(); closeErr != nil {
		return fmt.Errorf("close compacted transaction journal: %w", closeErr)
	}
	temp = nil
	if renameErr := os.Rename(tempPath, j.path); renameErr != nil {
		return fmt.Errorf("replace transaction journal with compacted state: %w", renameErr)
	}
	j.validEnd = compactedSize
	j.records = len(ids)
	if syncErr := syncJournalDirectory(dir); syncErr != nil {
		return syncErr
	}
	j.compactedBytes = j.validEnd
	j.compactedRecords = j.records
	return nil
}

func (j *Journal) Load() (map[string]*Snapshot, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.loadLocked()
}

func (j *Journal) loadLocked() (map[string]*Snapshot, error) {
	j.compactedBytes, j.compactedRecords = 0, 0
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open transaction journal for recovery: %w", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat transaction journal: %w", err)
	}
	size := info.Size()
	latest := make(map[string]*Snapshot)
	var offset int64
	records := 0

	for offset < size {
		if size-offset < 4 {
			return j.repairTail(file, offset, latest, records)
		}

		var header [4]byte
		if _, err := file.ReadAt(header[:], offset); err != nil {
			return nil, fmt.Errorf("read transaction journal header at %d: %w", offset, err)
		}
		payloadSize := int64(binary.BigEndian.Uint32(header[:]))
		if payloadSize <= 0 || payloadSize > maxJournalRecordBytes {
			if offset+4 == size {
				return j.repairTail(file, offset, latest, records)
			}
			return nil, fmt.Errorf("invalid transaction journal record size %d at %d", payloadSize, offset)
		}

		recordEnd := offset + 4 + payloadSize + 4
		if recordEnd > size {
			return j.repairTail(file, offset, latest, records)
		}

		payload := make([]byte, payloadSize)
		if _, err := file.ReadAt(payload, offset+4); err != nil {
			return nil, fmt.Errorf("read transaction journal payload at %d: %w", offset, err)
		}
		var checksumBytes [4]byte
		if _, err := file.ReadAt(checksumBytes[:], offset+4+payloadSize); err != nil {
			return nil, fmt.Errorf("read transaction journal checksum at %d: %w", offset, err)
		}
		expected := binary.BigEndian.Uint32(checksumBytes[:])
		actual := crc32.ChecksumIEEE(payload)
		if actual != expected {
			if recordEnd == size {
				return j.repairTail(file, offset, latest, records)
			}
			return nil, fmt.Errorf("transaction journal checksum mismatch at %d", offset)
		}

		snap, err := decodeJournalSnapshot(payload)
		if err != nil {
			return nil, fmt.Errorf("decode transaction journal record at %d: %w", offset, err)
		}
		if err := mergeJournalSnapshot(latest, snap); err != nil {
			return nil, fmt.Errorf("merge transaction journal record at %d: %w", offset, err)
		}
		records++
		offset = recordEnd
	}
	j.validEnd = offset
	j.loaded = true
	j.latest = latest
	j.records = records
	return cloneJournalState(latest), nil
}

func (j *Journal) repairTail(file *os.File, offset int64, latest map[string]*Snapshot, records int) (map[string]*Snapshot, error) {
	if err := repairJournalTail(file, offset); err != nil {
		return nil, err
	}
	j.validEnd = offset
	j.loaded = true
	j.latest = latest
	j.records = records
	return cloneJournalState(latest), nil
}

func cloneJournalState(state map[string]*Snapshot) map[string]*Snapshot {
	cloned := make(map[string]*Snapshot, len(state))
	for id, snap := range state {
		if snap == nil {
			continue
		}
		copySnapshot := *snap
		copySnapshot.Messages = append([]MessageOperation(nil), snap.Messages...)
		for i := range copySnapshot.Messages {
			message := &copySnapshot.Messages[i].Message
			message.ControlBatchKey = append([]byte(nil), message.ControlBatchKey...)
			message.ControlBatchValue = append([]byte(nil), message.ControlBatchValue...)
		}
		copySnapshot.Offsets = append([]OffsetOperation(nil), snap.Offsets...)
		cloned[id] = &copySnapshot
	}
	return cloned
}

func decodeJournalSnapshot(payload []byte) (*Snapshot, error) {
	var record journalRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, err
	}
	if record.Version != journalFormatVersion {
		return nil, fmt.Errorf("unsupported transaction journal version %d; clean bootstrap required", record.Version)
	}
	if record.Transaction == nil || record.Transaction.ID == "" {
		return nil, fmt.Errorf("journal transaction is missing")
	}
	return record.Transaction, nil
}

func mergeJournalSnapshot(latest map[string]*Snapshot, incoming *Snapshot) error {
	// Per-transaction controller locks serialize journal appends. The final
	// record is authoritative even when an expired transactional ID starts a
	// fresh producer epoch with lower revision metadata.
	latest[incoming.ID] = incoming
	return nil
}

func repairJournalTail(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate incomplete transaction journal tail: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync repaired transaction journal: %w", err)
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
