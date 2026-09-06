package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/cursus-io/cursus/pkg/types"
)

// serializeMessage converts types.Message to serialized bytes
func SerializeMessage(msg types.Message) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	// ProducerID (length + string)
	producerBytes := []byte(msg.ProducerID)
	producerLen, ok := SafeIntToUint16(len(producerBytes))
	if !ok {
		return nil, fmt.Errorf("producerID too long: %d", len(producerBytes))
	}
	if err = binary.Write(&buf, binary.BigEndian, producerLen); err != nil {
		return nil, fmt.Errorf("write producer length: %w", err)
	}
	if _, err = buf.Write(producerBytes); err != nil {
		return nil, fmt.Errorf("write producer bytes: %w", err)
	}

	// SeqNum (8 bytes)
	if err = binary.Write(&buf, binary.BigEndian, msg.SeqNum); err != nil {
		return nil, fmt.Errorf("write sequence number: %w", err)
	}

	// Payload (length + string)
	payloadBytes := []byte(msg.Payload)
	payloadLen, ok := SafeIntToUint32(len(payloadBytes))
	if !ok {
		return nil, fmt.Errorf("payload too long: %d", len(payloadBytes))
	}
	if err = binary.Write(&buf, binary.BigEndian, payloadLen); err != nil {
		return nil, fmt.Errorf("write payload length: %w", err)
	}
	if _, err = buf.Write(payloadBytes); err != nil {
		return nil, fmt.Errorf("write payload bytes: %w", err)
	}

	// Key (length + string)
	keyBytes := []byte(msg.Key)
	keyLen, ok := SafeIntToUint16(len(keyBytes))
	if !ok {
		return nil, fmt.Errorf("key too long: %d", len(keyBytes))
	}
	if err = binary.Write(&buf, binary.BigEndian, keyLen); err != nil {
		return nil, fmt.Errorf("write key length: %w", err)
	}
	if _, err = buf.Write(keyBytes); err != nil {
		return nil, fmt.Errorf("write key bytes: %w", err)
	}

	// Epoch (8 bytes)
	if err = binary.Write(&buf, binary.BigEndian, msg.Epoch); err != nil {
		return nil, fmt.Errorf("write epoch: %w", err)
	}

	return buf.Bytes(), nil
}

// deserializeMessage converts bytes back to types.Message
func DeserializeMessage(data []byte) (types.Message, error) {
	var msg types.Message
	offset := 0

	if len(data) < 2 {
		return msg, fmt.Errorf("data too short for producer length")
	}

	// ProducerID
	producerLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if offset+producerLen > len(data) {
		return msg, fmt.Errorf("data too short for producer ID")
	}

	msg.ProducerID = string(data[offset : offset+producerLen])
	offset += producerLen

	if offset+8 > len(data) {
		return msg, fmt.Errorf("data too short for sequence number")
	}

	// SeqNum (8 bytes)
	msg.SeqNum = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	if offset+4 > len(data) {
		return msg, fmt.Errorf("data too short for payload length")
	}

	// Payload
	payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+payloadLen > len(data) {
		return msg, fmt.Errorf("data too short for payload")
	}

	msg.Payload = string(data[offset : offset+payloadLen])
	offset += payloadLen

	if offset+2 > len(data) {
		return msg, fmt.Errorf("data too short for key length")
	}

	// Key
	keyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if offset+keyLen > len(data) {
		return msg, fmt.Errorf("data too short for key")
	}

	msg.Key = string(data[offset : offset+keyLen])
	offset += keyLen

	if offset+8 > len(data) {
		return msg, fmt.Errorf("data too short for epoch")
	}

	// Epoch (8 bytes)
	epochRaw := binary.BigEndian.Uint64(data[offset : offset+8])
	epochVal, ok := SafeUint64ToInt64(epochRaw)
	if !ok {
		return msg, fmt.Errorf("epoch value %d exceeds int64 max", epochRaw)
	}
	msg.Epoch = epochVal

	return msg, nil
}

var diskMsgBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

const diskMessageMagic = "CDM3"
const legacyDiskMessageMagic = "CDM2"

var ErrDiskRecordChecksum = errors.New("disk record checksum mismatch")
var diskChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// EstimateDiskMessageSize returns the serialized size of a DiskMessage without allocating.
func EstimateDiskMessageSize(msg types.DiskMessage) int {
	return len(diskMessageMagic) + 4 + 2 + len(msg.Topic) + 4 + 8 + 2 + len(msg.ProducerID) + 8 + 8 +
		4 + len(msg.Payload) + 2 + len(msg.Key) +
		2 + len(msg.EventType) + 4 + 8 + 2 + len(msg.Metadata) + 2 + len(msg.TransactionalID) + 2 + len(msg.TransactionState) + 2 + len(msg.TransactionMarker) + 2 + len(msg.ControlBatchType) + 2 + 8 + 2 + len(msg.ControlBatchKey) + 2 + len(msg.ControlBatchValue)
}

// SerializeDiskMessage serializes a DiskMessage for disk storage
func SerializeDiskMessage(msg types.DiskMessage) ([]byte, error) {
	topicLen, ok := SafeIntToUint16(len(msg.Topic))
	if !ok {
		return nil, fmt.Errorf("topic too long: %d", len(msg.Topic))
	}
	producerLen, ok := SafeIntToUint16(len(msg.ProducerID))
	if !ok {
		return nil, fmt.Errorf("producerID too long: %d", len(msg.ProducerID))
	}
	payloadLen, ok := SafeIntToUint32(len(msg.Payload))
	if !ok {
		return nil, fmt.Errorf("payload too long: %d", len(msg.Payload))
	}
	eventTypeLen, ok := SafeIntToUint16(len(msg.EventType))
	if !ok {
		return nil, fmt.Errorf("eventType too long: %d", len(msg.EventType))
	}
	metadataLen, ok := SafeIntToUint16(len(msg.Metadata))
	if !ok {
		return nil, fmt.Errorf("metadata too long: %d", len(msg.Metadata))
	}
	partitionVal, ok := SafeInt32ToUint32(msg.Partition)
	if !ok {
		return nil, fmt.Errorf("negative partition: %d", msg.Partition)
	}
	keyLen, ok := SafeIntToUint16(len(msg.Key))
	if !ok {
		return nil, fmt.Errorf("key too long: %d", len(msg.Key))
	}
	txnIDLen, ok := SafeIntToUint16(len(msg.TransactionalID))
	if !ok {
		return nil, fmt.Errorf("transactionalID too long: %d", len(msg.TransactionalID))
	}
	txnStateLen, ok := SafeIntToUint16(len(msg.TransactionState))
	if !ok {
		return nil, fmt.Errorf("transactionState too long: %d", len(msg.TransactionState))
	}
	txnMarkerLen, ok := SafeIntToUint16(len(msg.TransactionMarker))
	if !ok {
		return nil, fmt.Errorf("transactionMarker too long: %d", len(msg.TransactionMarker))
	}
	controlBatchTypeLen, ok := SafeIntToUint16(len(msg.ControlBatchType))
	if !ok {
		return nil, fmt.Errorf("controlBatchType too long: %d", len(msg.ControlBatchType))
	}
	controlBatchKeyLen, ok := SafeIntToUint16(len(msg.ControlBatchKey))
	if !ok {
		return nil, fmt.Errorf("controlBatchKey too long: %d", len(msg.ControlBatchKey))
	}
	controlBatchValueLen, ok := SafeIntToUint16(len(msg.ControlBatchValue))
	if !ok {
		return nil, fmt.Errorf("controlBatchValue too long: %d", len(msg.ControlBatchValue))
	}
	epochVal, ok := SafeInt64ToUint64(msg.Epoch)
	if !ok {
		return nil, fmt.Errorf("negative epoch: %d", msg.Epoch)
	}

	size := EstimateDiskMessageSize(msg)
	bufp := diskMsgBufPool.Get().(*[]byte)
	buf := (*bufp)[:0]
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	}

	var tmp [8]byte
	buf = append(buf, diskMessageMagic...)

	// Topic (length + string)
	binary.BigEndian.PutUint16(tmp[:2], topicLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.Topic...)

	// Partition (4 bytes)
	binary.BigEndian.PutUint32(tmp[:4], partitionVal)
	buf = append(buf, tmp[:4]...)

	// Offset (8 bytes)
	binary.BigEndian.PutUint64(tmp[:8], msg.Offset)
	buf = append(buf, tmp[:8]...)

	// ProducerID (length + string)
	binary.BigEndian.PutUint16(tmp[:2], producerLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.ProducerID...)

	// SeqNum (8 bytes)
	binary.BigEndian.PutUint64(tmp[:8], msg.SeqNum)
	buf = append(buf, tmp[:8]...)

	// Epoch (8 bytes)
	binary.BigEndian.PutUint64(tmp[:8], epochVal)
	buf = append(buf, tmp[:8]...)

	// Payload (length + string)
	binary.BigEndian.PutUint32(tmp[:4], payloadLen)
	buf = append(buf, tmp[:4]...)
	buf = append(buf, msg.Payload...)

	// EventType (length + string)
	binary.BigEndian.PutUint16(tmp[:2], eventTypeLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.EventType...)

	// SchemaVersion (4 bytes)
	binary.BigEndian.PutUint32(tmp[:4], msg.SchemaVersion)
	buf = append(buf, tmp[:4]...)

	// AggregateVersion (8 bytes)
	binary.BigEndian.PutUint64(tmp[:8], msg.AggregateVersion)
	buf = append(buf, tmp[:8]...)

	// Metadata (length + string)
	binary.BigEndian.PutUint16(tmp[:2], metadataLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.Metadata...)

	// Key (length + string)
	binary.BigEndian.PutUint16(tmp[:2], keyLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.Key...)

	// Transaction metadata (optional trailer; empty for non-transactional records)
	binary.BigEndian.PutUint16(tmp[:2], txnIDLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.TransactionalID...)

	binary.BigEndian.PutUint16(tmp[:2], txnStateLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.TransactionState...)

	binary.BigEndian.PutUint16(tmp[:2], txnMarkerLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.TransactionMarker...)

	// Control batch metadata (optional trailer; Cursus transaction marker semantics)
	binary.BigEndian.PutUint16(tmp[:2], controlBatchTypeLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.ControlBatchType...)

	controlVersionBuf := bytes.Buffer{}
	if err := binary.Write(&controlVersionBuf, binary.BigEndian, msg.ControlBatchVersion); err != nil {
		return nil, fmt.Errorf("write control batch version: %w", err)
	}
	buf = append(buf, controlVersionBuf.Bytes()...)

	controlEpochBuf := bytes.Buffer{}
	if err := binary.Write(&controlEpochBuf, binary.BigEndian, msg.ControlBatchCoordinatorEpoch); err != nil {
		return nil, fmt.Errorf("write control batch coordinator epoch: %w", err)
	}
	buf = append(buf, controlEpochBuf.Bytes()...)

	binary.BigEndian.PutUint16(tmp[:2], controlBatchKeyLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.ControlBatchKey...)

	binary.BigEndian.PutUint16(tmp[:2], controlBatchValueLen)
	buf = append(buf, tmp[:2]...)
	buf = append(buf, msg.ControlBatchValue...)
	binary.BigEndian.PutUint32(tmp[:4], crc32.Checksum(buf, diskChecksumTable))
	buf = append(buf, tmp[:4]...)

	// Return a copy so the pooled buffer can be reused
	result := make([]byte, len(buf))
	copy(result, buf)
	*bufp = buf
	diskMsgBufPool.Put(bufp)

	return result, nil
}

// DeserializeDiskMessage deserializes bytes back to DiskMessage
func DeserializeDiskMessage(data []byte) (types.DiskMessage, error) {
	var msg types.DiskMessage
	if len(data) < len(diskMessageMagic) {
		return msg, fmt.Errorf("unsupported disk message format: clean bootstrap required")
	}
	switch string(data[:len(diskMessageMagic)]) {
	case diskMessageMagic:
		if len(data) < len(diskMessageMagic)+4 {
			return msg, fmt.Errorf("%w: incomplete record", ErrDiskRecordChecksum)
		}
		body := data[:len(data)-4]
		if crc32.Checksum(body, diskChecksumTable) != binary.BigEndian.Uint32(data[len(data)-4:]) {
			return msg, ErrDiskRecordChecksum
		}
		data = body
	case legacyDiskMessageMagic:
	default:
		return msg, fmt.Errorf("unsupported disk message format: clean bootstrap required")
	}
	offset := len(diskMessageMagic)

	// Topic
	if offset+2 > len(data) {
		return msg, errors.New("data too short for topic length")
	}
	topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if offset+topicLen > len(data) {
		return msg, errors.New("data too short for topic")
	}
	msg.Topic = string(data[offset : offset+topicLen])
	offset += topicLen

	// Partition
	if offset+4 > len(data) {
		return msg, errors.New("data too short for partition")
	}
	partRaw := binary.BigEndian.Uint32(data[offset : offset+4])
	partVal, ok := SafeUint32ToInt32(partRaw)
	if !ok {
		return msg, fmt.Errorf("partition value %d exceeds int32 max", partRaw)
	}
	msg.Partition = partVal
	offset += 4

	// Offset
	if offset+8 > len(data) {
		return msg, errors.New("data too short for offset")
	}
	msg.Offset = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// ProducerID
	if offset+2 > len(data) {
		return msg, errors.New("data too short for producer ID length")
	}
	prodLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+prodLen > len(data) {
		return msg, errors.New("data too short for producer ID")
	}
	msg.ProducerID = string(data[offset : offset+prodLen])
	offset += prodLen

	// SeqNum
	if offset+8 > len(data) {
		return msg, errors.New("data too short for sequence number")
	}
	msg.SeqNum = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Epoch
	if offset+8 > len(data) {
		return msg, errors.New("data too short for epoch")
	}
	diskEpochRaw := binary.BigEndian.Uint64(data[offset : offset+8])
	diskEpochVal, ok := SafeUint64ToInt64(diskEpochRaw)
	if !ok {
		return msg, fmt.Errorf("epoch value %d exceeds int64 max", diskEpochRaw)
	}
	msg.Epoch = diskEpochVal
	offset += 8

	// Payload
	if offset+4 > len(data) {
		return msg, errors.New("data too short for payload length")
	}
	payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+payloadLen > len(data) {
		return msg, errors.New("data too short for payload")
	}
	msg.Payload = string(data[offset : offset+payloadLen])
	offset += payloadLen

	if offset+2 > len(data) {
		return msg, fmt.Errorf("incomplete event-sourcing fields: truncated event type length")
	}
	eventTypeLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+eventTypeLen > len(data) {
		return msg, fmt.Errorf("incomplete event-sourcing fields: truncated event type")
	}
	msg.EventType = string(data[offset : offset+eventTypeLen])
	offset += eventTypeLen

	if offset+4 > len(data) {
		return msg, fmt.Errorf("incomplete event-sourcing fields: truncated schema version")
	}
	msg.SchemaVersion = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+8 > len(data) {
		return msg, fmt.Errorf("incomplete event-sourcing fields: truncated aggregate version")
	}
	msg.AggregateVersion = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	if err := readDiskString(data, &offset, &msg.Metadata, "metadata"); err != nil {
		return msg, err
	}
	if err := readDiskString(data, &offset, &msg.Key, "key"); err != nil {
		return msg, err
	}
	if err := readDiskString(data, &offset, &msg.TransactionalID, "transactional ID"); err != nil {
		return msg, err
	}
	if err := readDiskString(data, &offset, &msg.TransactionState, "transaction state"); err != nil {
		return msg, err
	}
	if err := readDiskString(data, &offset, &msg.TransactionMarker, "transaction marker"); err != nil {
		return msg, err
	}
	if err := readDiskString(data, &offset, &msg.ControlBatchType, "control batch type"); err != nil {
		return msg, err
	}
	if offset+2 > len(data) {
		return msg, fmt.Errorf("incomplete control batch version field: truncated value")
	}
	// #nosec G115 -- disk format stores the signed value's two's-complement bits.
	msg.ControlBatchVersion = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+8 > len(data) {
		return msg, fmt.Errorf("incomplete control batch coordinator epoch field: truncated value")
	}
	// #nosec G115 -- disk format stores the signed value's two's-complement bits.
	msg.ControlBatchCoordinatorEpoch = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	if err := readDiskBytes(data, &offset, &msg.ControlBatchKey, "control batch key"); err != nil {
		return msg, err
	}
	if err := readDiskBytes(data, &offset, &msg.ControlBatchValue, "control batch value"); err != nil {
		return msg, err
	}
	if offset != len(data) {
		return msg, fmt.Errorf("disk message has %d trailing bytes", len(data)-offset)
	}

	return msg, nil
}

func readDiskString(data []byte, offset *int, out *string, field string) error {
	if *offset+2 > len(data) {
		return fmt.Errorf("incomplete %s field: truncated length", field)
	}
	fieldLen := int(binary.BigEndian.Uint16(data[*offset : *offset+2]))
	*offset += 2
	if *offset+fieldLen > len(data) {
		return fmt.Errorf("incomplete %s field: truncated value", field)
	}
	*out = string(data[*offset : *offset+fieldLen])
	*offset += fieldLen
	return nil
}

func readDiskBytes(data []byte, offset *int, dest *[]byte, field string) error {
	if *offset+2 > len(data) {
		return fmt.Errorf("incomplete %s length field: truncated value", field)
	}
	length := int(binary.BigEndian.Uint16(data[*offset : *offset+2]))
	*offset += 2
	if *offset+length > len(data) {
		return fmt.Errorf("incomplete %s field: expected %d bytes", field, length)
	}
	if length == 0 {
		*dest = nil
	} else {
		*dest = append((*dest)[:0], data[*offset:*offset+length]...)
	}
	*offset += length
	return nil
}
