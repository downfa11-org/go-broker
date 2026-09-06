package transaction

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrLimitExceeded = errors.New("transaction resource limit exceeded")

type Limits struct {
	Transactions     int
	Messages         int
	Offsets          int
	TransactionBytes int64
	TotalBytes       int64
}

func DefaultLimits() Limits {
	return Limits{Transactions: 10000, Messages: 10000, Offsets: 1024,
		TransactionBytes: 8 << 20, TotalBytes: 128 << 20}
}

func NewManagerWithLimits(expiration time.Duration, limits Limits) (*Manager, error) {
	if limits.Transactions <= 0 || limits.Messages <= 0 || limits.Offsets <= 0 ||
		limits.TransactionBytes <= 0 || limits.TransactionBytes > maxJournalRecordBytes-1024 || limits.TotalBytes <= 0 {
		return nil, fmt.Errorf("invalid transaction resource limits")
	}
	m := NewManagerWithExpiration(expiration)
	m.limits = limits
	return m, nil
}

func (m *Manager) admissionErrorLocked(id string, size int64, messages, offsets int) error {
	if messages > m.limits.Messages || offsets > m.limits.Offsets || size > m.limits.TransactionBytes {
		return fmt.Errorf("%w: transaction bytes=%d messages=%d offsets=%d", ErrLimitExceeded, size, messages, offsets)
	}
	previous, exists := m.txns[id]
	if !exists && len(m.txns) >= m.limits.Transactions {
		return fmt.Errorf("%w: tracked transactions=%d maximum=%d", ErrLimitExceeded, len(m.txns), m.limits.Transactions)
	}
	oldSize := m.sizes[id]
	if previous == nil {
		oldSize = 0
	}
	if size > oldSize && size-oldSize > m.limits.TotalBytes-m.totalBytes {
		return fmt.Errorf("%w: retained bytes=%d maximum=%d", ErrLimitExceeded, m.totalBytes, m.limits.TotalBytes)
	}
	return nil
}

func (m *Manager) recordSizeLocked(id string, size int64) {
	if m.sizes == nil {
		m.sizes = make(map[string]int64)
	}
	m.totalBytes += size - m.sizes[id]
	m.sizes[id] = size
}

func (m *Manager) replaceLocked(id string, tx *Transaction) {
	m.txns[id] = tx
	m.recordSizeLocked(id, transactionCharge(tx))
}

func (m *Manager) deleteLocked(id string) {
	m.totalBytes -= m.sizes[id]
	delete(m.sizes, id)
	delete(m.txns, id)
}

func transactionCharge(tx *Transaction) int64 {
	if tx == nil {
		return 0
	}
	size := int64(1024) + textCharge(tx.ID, tx.Producer)
	for _, op := range tx.Messages {
		size += messageCharge(op)
	}
	for _, op := range tx.Offsets {
		size += offsetCharge(op)
	}
	return size
}

// Charge worst-case JSON escaping plus record overhead, without serializing
// the growing transaction on every admission. This is not an RSS measurement.
func textCharge(values ...string) int64 {
	var size int64
	for _, value := range values {
		size += 6 * int64(len(value))
	}
	return size
}

func messageCharge(op MessageOperation) int64 {
	m := op.Message
	return 1024 + textCharge(op.Topic, m.Topic, m.ProducerID, m.Payload, m.Key, m.EventType, m.Metadata,
		m.TransactionalID, m.TransactionState, m.TransactionMarker, m.ControlBatchType) +
		2*int64(len(m.ControlBatchKey)) + 2*int64(len(m.ControlBatchValue))
}

func offsetCharge(op OffsetOperation) int64 {
	return 512 + textCharge(op.Topic, op.Group, op.Member)
}

func ownMessageOperation(op MessageOperation) MessageOperation {
	op.Topic = strings.Clone(op.Topic)
	m := &op.Message
	m.Topic = strings.Clone(m.Topic)
	m.ProducerID = strings.Clone(m.ProducerID)
	m.Payload = strings.Clone(m.Payload)
	m.Key = strings.Clone(m.Key)
	m.EventType = strings.Clone(m.EventType)
	m.Metadata = strings.Clone(m.Metadata)
	m.TransactionalID = strings.Clone(m.TransactionalID)
	m.TransactionState = strings.Clone(m.TransactionState)
	m.TransactionMarker = strings.Clone(m.TransactionMarker)
	m.ControlBatchType = strings.Clone(m.ControlBatchType)
	m.ControlBatchKey = append([]byte(nil), m.ControlBatchKey...)
	m.ControlBatchValue = append([]byte(nil), m.ControlBatchValue...)
	return op
}
