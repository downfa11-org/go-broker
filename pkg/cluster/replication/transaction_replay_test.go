package replication

import (
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestReplayFSMHasTransactionManagerBeforeFirstCommand(t *testing.T) {
	f := newReplayFSM(config.DefaultConfig(), nil, nil)
	source := transaction.NewManager()
	producer, epoch, err := source.InitProducer("replayed")
	require.NoError(t, err)
	require.NoError(t, source.Begin("replayed", producer, epoch))
	snapshot, ok := source.SnapshotByID("replayed")
	require.True(t, ok)
	payload, err := json.Marshal(map[string]interface{}{"transaction": snapshot})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Index: 1, Data: append([]byte("TXN_SYNC:"), payload...)}))
	require.NotNil(t, f.TransactionManager())
	got, err := f.TransactionManager().Status("replayed")
	require.NoError(t, err)
	require.Equal(t, transaction.StateOpen, got.State)
	require.Equal(t, producer, got.Producer)
}
