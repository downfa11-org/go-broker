package util

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestMessageSerialization(t *testing.T) {
	msg := types.Message{
		ProducerID: "prod-1",
		SeqNum:     12345,
		Payload:    "test-payload",
		Key:        "test-key",
		Epoch:      100,
	}

	data, err := SerializeMessage(msg)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := DeserializeMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, msg.ProducerID, decoded.ProducerID)
	assert.Equal(t, msg.SeqNum, decoded.SeqNum)
	assert.Equal(t, msg.Payload, decoded.Payload)
	assert.Equal(t, msg.Key, decoded.Key)
	assert.Equal(t, msg.Epoch, decoded.Epoch)
}

func TestDiskMessageSerialization(t *testing.T) {
	msg := types.DiskMessage{
		Topic:      "test-topic",
		Partition:  2,
		Offset:     500,
		ProducerID: "p1",
		SeqNum:     10,
		Epoch:      5,
		Payload:    "hello world",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := DeserializeDiskMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, msg, decoded)
}

func TestDeserializeMessage_ErrorCases(t *testing.T) {
	// Too short for initial fields
	_, err := DeserializeMessage([]byte{0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer length")

	_, err = DeserializeDiskMessage([]byte{0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported disk message format")

	// Malformed length-prefixes (claims to have more data than provided)
	// DeserializeMessage: [2 bytes producer length] [producer ID...]
	_, err = DeserializeMessage([]byte{0, 10}) // claims 10 bytes, but 0 provided
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer ID")

	// DeserializeDiskMessage: [CDM2] [2 bytes topic length] [topic...] [4 bytes partition] ...
	_, err = DeserializeDiskMessage([]byte{'C', 'D', 'M', '2', 0, 5, 't', 'e', 's', 't', '1'}) // claims 5 bytes topic, OK, but missing partition
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition")

	// Large claims that exceed buffer
	_, err = DeserializeMessage([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255})
	assert.Error(t, err)
}

func TestDiskMessageSerialization_WithEventSourcingFields(t *testing.T) {
	msg := types.DiskMessage{
		Topic:            "orders",
		Partition:        2,
		Offset:           100,
		ProducerID:       "prod-1",
		SeqNum:           10,
		Epoch:            999,
		Payload:          `{"item":"widget"}`,
		EventType:        "OrderPlaced",
		SchemaVersion:    3,
		AggregateVersion: 7,
		Metadata:         `{"trace_id":"abc123"}`,
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	got, err := DeserializeDiskMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, msg.EventType, got.EventType)
	assert.Equal(t, msg.SchemaVersion, got.SchemaVersion)
	assert.Equal(t, msg.AggregateVersion, got.AggregateVersion)
	assert.Equal(t, msg.Metadata, got.Metadata)
}

func TestDeserializeDiskMessage_TruncatedEventSourcingFields(t *testing.T) {
	msg := types.DiskMessage{
		Topic:            "t",
		Partition:        0,
		Offset:           1,
		ProducerID:       "p",
		SeqNum:           1,
		Epoch:            1,
		Payload:          "x",
		EventType:        "SomeEvent",
		SchemaVersion:    1,
		AggregateVersion: 1,
		Metadata:         "meta",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	// Truncate so event-sourcing fields are incomplete
	truncated := data[:len(data)-10]
	_, err = DeserializeDiskMessage(truncated)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDiskRecordChecksum)
}

func TestDeserializeDiskMessageRejectsIncompleteStorageV2Record(t *testing.T) {
	msg := types.DiskMessage{
		Topic:      "orders",
		Partition:  1,
		Offset:     50,
		ProducerID: "producer",
		SeqNum:     5,
		Epoch:      500,
		Payload:    "payload",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	_, err = DeserializeDiskMessage(data[:len(data)-40])
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDiskRecordChecksum)
}

func TestDeserializeDiskMessageRejectsLegacyUnversionedRecord(t *testing.T) {
	_, err := DeserializeDiskMessage([]byte{0, 6, 'o', 'r', 'd', 'e', 'r', 's'})
	assert.ErrorContains(t, err, "clean bootstrap required")
}

func TestDiskMessageSerialization_EmptyFields(t *testing.T) {
	msg := types.DiskMessage{
		Topic:      "empty-test",
		Partition:  0,
		Offset:     0,
		ProducerID: "",
		SeqNum:     0,
		Epoch:      0,
		Payload:    "",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	got, err := DeserializeDiskMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, "empty-test", got.Topic)
	assert.Equal(t, "", got.ProducerID)
	assert.Equal(t, "", got.Payload)
	assert.Equal(t, "", got.EventType)
	assert.Equal(t, "", got.Metadata)
}
func TestDiskMessageTransactionMetadataRoundTrip(t *testing.T) {
	msg := types.DiskMessage{
		Topic:                        "txn-topic",
		Partition:                    0,
		Offset:                       7,
		ProducerID:                   "producer-1",
		SeqNum:                       3,
		Epoch:                        2,
		Payload:                      "payload",
		Key:                          "key",
		TransactionalID:              "tx-1",
		TransactionState:             types.TransactionStateCommitted,
		TransactionMarker:            types.TransactionMarkerCommit,
		ControlBatchType:             types.ControlBatchTransaction,
		ControlBatchVersion:          types.ControlBatchVersionCursusV2,
		ControlBatchCoordinatorEpoch: 3,
		ControlBatchKey:              []byte{0, 0, 0, 0},
		ControlBatchValue:            []byte{0, 0, 0, 0, 0, 3},
	}

	data, err := SerializeDiskMessage(msg)
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}
	got, err := DeserializeDiskMessage(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}
	if got.TransactionalID != msg.TransactionalID || got.TransactionState != msg.TransactionState || got.TransactionMarker != msg.TransactionMarker || got.ControlBatchType != msg.ControlBatchType || got.ControlBatchVersion != msg.ControlBatchVersion || got.ControlBatchCoordinatorEpoch != msg.ControlBatchCoordinatorEpoch || string(got.ControlBatchKey) != string(msg.ControlBatchKey) || string(got.ControlBatchValue) != string(msg.ControlBatchValue) {
		t.Fatalf("transaction metadata mismatch: %+v", got)
	}
}

func TestDiskMessageNegativeControlMetadataRoundTrip(t *testing.T) {
	msg := types.DiskMessage{
		Topic:                        "txn-topic",
		ControlBatchVersion:          -1,
		ControlBatchCoordinatorEpoch: -2,
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)
	got, err := DeserializeDiskMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, msg.ControlBatchVersion, got.ControlBatchVersion)
	assert.Equal(t, msg.ControlBatchCoordinatorEpoch, got.ControlBatchCoordinatorEpoch)
}
