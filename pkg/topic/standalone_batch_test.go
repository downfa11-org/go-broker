package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStandaloneBatchValidatesAllSequencesBeforeWriting(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = dh.Close() }()
	p := NewPartition(0, "orders", dh, nil, cfg)
	defer p.Close()
	batch := []types.Message{{ProducerID: "p", Epoch: 1, SeqNum: 1}, {ProducerID: "p", Epoch: 1, SeqNum: 3}}
	require.Error(t, p.EnqueueBatchSyncWithMode(batch, true))
	require.Zero(t, dh.GetAbsoluteOffset())
	require.Zero(t, p.GetHWM())
	batch[1].SeqNum = 2
	require.NoError(t, p.EnqueueBatchSyncWithMode(batch, true))
	require.Equal(t, uint64(2), p.GetHWM())
	require.NoError(t, p.EnqueueBatchSyncWithMode(batch, true))
	require.Equal(t, uint64(2), dh.GetAbsoluteOffset())
}
