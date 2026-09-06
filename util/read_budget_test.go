package util

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBatchReadBudgetStopsBeforeOversizedResponse(t *testing.T) {
	messages := []types.Message{{Offset: 0, Payload: "first"}, {Offset: 1, Payload: "second"}}
	encoded, err := EncodeBatchMessages("topic", 0, "1", false, messages[:1])
	require.NoError(t, err)
	budget, err := NewBatchReadBudget("topic", 0, len(encoded))
	require.NoError(t, err)
	read := func(offset uint64, max int) ([]types.Message, error) {
		require.Equal(t, 1, max)
		if offset >= uint64(len(messages)) {
			return nil, nil
		}
		return messages[offset : offset+1], nil
	}
	got, err := budget.Read(0, 1000, read)
	require.NoError(t, err)
	require.Equal(t, messages[:1], got)
	got, err = budget.Read(1, 1000, read)
	require.NoError(t, err)
	require.Empty(t, got)
	tooSmall, err := NewBatchReadBudget("topic", 0, len(encoded)-1)
	require.NoError(t, err)
	_, err = tooSmall.Read(0, 1, read)
	require.ErrorContains(t, err, "exceeds batch byte budget")
}
