package util

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDiskRecordChecksumDetectsEverySingleByteMutation(t *testing.T) {
	message := types.DiskMessage{Topic: "orders", Partition: 2, Offset: 5, ProducerID: "writer", SeqNum: 6, Epoch: 1, Payload: "payload", Key: "order", TransactionalID: "tx", TransactionState: "pending"}
	encoded, err := SerializeDiskMessage(message)
	require.NoError(t, err)
	require.Equal(t, EstimateDiskMessageSize(message), len(encoded))
	for i := range encoded {
		mutated := append([]byte(nil), encoded...)
		mutated[i] ^= 1
		_, err := DeserializeDiskMessage(mutated)
		require.Error(t, err, "byte %d", i)
	}
	decoded, err := DeserializeDiskMessage(encoded)
	require.NoError(t, err)
	require.Equal(t, message, decoded)
}

func TestDiskRecordChecksumPreservesLegacyReadCompatibility(t *testing.T) {
	message := types.DiskMessage{Topic: "orders", Payload: "legacy", Key: "key"}
	encoded, err := SerializeDiskMessage(message)
	require.NoError(t, err)
	legacy := append([]byte(nil), encoded[:len(encoded)-4]...)
	copy(legacy, legacyDiskMessageMagic)
	decoded, err := DeserializeDiskMessage(legacy)
	require.NoError(t, err)
	require.Equal(t, message, decoded)
	_, err = DeserializeDiskMessage(legacy[:len(legacy)-1])
	require.Error(t, err)
}
