package transaction

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
)

type journalBase struct {
	digest       string
	messageBytes int
}

func encodeJournalUpdate(snap, previous *Snapshot, basis journalBase) ([]byte, journalBase, error) {
	if snap == nil || snap.ID == "" {
		return nil, journalBase{}, fmt.Errorf("invalid transaction snapshot")
	}
	record := journalRecord{Version: journalFormatVersion, Transaction: snap}
	if sameJournalIdentity(previous, snap) && basis.digest != "" && len(previous.Messages) > 0 &&
		len(snap.Messages) >= len(previous.Messages) && reflect.DeepEqual(previous.Messages, snap.Messages[:len(previous.Messages)]) {
		prefix := len(previous.Messages)
		tail := *snap
		tail.Messages = snap.Messages[prefix:]
		record = journalRecord{Version: journalDeltaVersion, Transaction: &tail, BaseDigest: basis.digest, MessagePrefix: &prefix}
	}
	messageBytes, err := journalMessageBytes(record.Transaction.Messages)
	if err != nil {
		return nil, journalBase{}, err
	}
	if record.MessagePrefix != nil {
		messageBytes += basis.messageBytes
		if len(record.Transaction.Messages) > 0 {
			messageBytes++ // JSON comma between the prefix and the appended messages.
		}
	}
	if err := validateJournalStateSize(snap, len(snap.Messages), messageBytes); err != nil {
		return nil, journalBase{}, err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, journalBase{}, fmt.Errorf("marshal transaction journal record: %w", err)
	}
	return payload, journalBase{digest: journalDigest(payload), messageBytes: messageBytes}, nil
}

func decodeJournalUpdate(payload []byte, latest map[string]*Snapshot, bases map[string]journalBase) (*Snapshot, journalBase, error) {
	var record journalRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, journalBase{}, err
	}
	snap := record.Transaction
	if snap == nil || snap.ID == "" {
		return nil, journalBase{}, fmt.Errorf("journal transaction is missing")
	}
	var previous *Snapshot
	switch record.Version {
	case journalFormatVersion:
		if record.MessagePrefix != nil || record.BaseDigest != "" {
			return nil, journalBase{}, fmt.Errorf("full journal snapshot contains delta fields")
		}
	case journalDeltaVersion:
		previous = latest[snap.ID]
		if !sameJournalIdentity(previous, snap) || record.MessagePrefix == nil || *record.MessagePrefix <= 0 ||
			*record.MessagePrefix != len(previous.Messages) || record.BaseDigest == "" || record.BaseDigest != bases[snap.ID].digest {
			return nil, journalBase{}, fmt.Errorf("transaction journal delta base mismatch for %q", snap.ID)
		}
	default:
		return nil, journalBase{}, fmt.Errorf("unsupported transaction journal version %d", record.Version)
	}
	messageBytes, err := journalMessageBytes(snap.Messages)
	if err != nil {
		return nil, journalBase{}, err
	}
	count := len(snap.Messages)
	if previous != nil {
		messageBytes += bases[snap.ID].messageBytes
		if count > 0 {
			messageBytes++
		}
		count += len(previous.Messages)
	}
	if err := validateJournalStateSize(snap, count, messageBytes); err != nil {
		return nil, journalBase{}, err
	}
	if previous != nil {
		// Recovered prefix storage is private to this journal, so growth is amortized.
		snap.Messages = append(previous.Messages, snap.Messages...)
	}
	return snap, journalBase{digest: journalDigest(payload), messageBytes: messageBytes}, nil
}

func sameJournalIdentity(a, b *Snapshot) bool {
	return a != nil && b != nil && a.ID == b.ID && a.Producer == b.Producer && a.Epoch == b.Epoch && a.CreatedAt.Equal(b.CreatedAt)
}

func journalDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func journalMessageBytes(messages []MessageOperation) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}
	data, err := json.Marshal(messages)
	if err != nil {
		return 0, fmt.Errorf("marshal journal messages: %w", err)
	}
	return len(data) - 2, nil
}

func validateJournalStateSize(snap *Snapshot, count, messageBytes int) error {
	metadata := *snap
	metadata.Messages = nil
	payload, err := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: &metadata})
	if err != nil {
		return fmt.Errorf("marshal journal metadata: %w", err)
	}
	size := len(payload)
	if count > 0 {
		size += len(`,"messages":[]`) + messageBytes
	}
	if size > maxJournalRecordBytes {
		return fmt.Errorf("transaction snapshot size %d exceeds journal limit", size)
	}
	return nil
}
