package disk

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAppendBatchSyncSharesSyncAndPreservesQueuedPrefix(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir())
	cfg.DiskFlushIntervalMS, cfg.LingerMS = 3600000, 3600000
	h, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = h.Close() }()
	var syncs atomic.Int32
	h.ioMu.Lock()
	h.syncFileFn = func(file *os.File) error { syncs.Add(1); return file.Sync() }
	h.ioMu.Unlock()
	_, err = h.AppendMessage("orders", 0, &types.Message{Payload: "queued"})
	require.NoError(t, err)
	batch := []types.Message{{Payload: "one"}, {Payload: "two"}, {Payload: "three"}}
	require.NoError(t, h.AppendBatchSync("orders", 0, batch))
	require.Equal(t, int32(1), syncs.Load())
	require.Equal(t, uint64(1), batch[0].Offset)
	require.Equal(t, uint64(3), batch[2].Offset)
	got, err := h.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Equal(t, "queued", got[0].Payload)
	require.Equal(t, "three", got[3].Payload)
}

func TestAppendBatchSyncRejectsInvalidBatchBeforeEnqueue(t *testing.T) {
	h := &DiskHandler{writeCh: make(chan types.DiskMessage, 4), done: make(chan struct{})}
	err := h.AppendBatchSync("orders", 0, []types.Message{{Payload: "valid"}, {ProducerID: strings.Repeat("x", 65536)}})
	require.Error(t, err)
	require.Empty(t, h.writeCh)
	require.Zero(t, h.GetAbsoluteOffset())
}

func TestAppendBatchSyncFailureDoesNotReportSuccess(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir())
	cfg.DiskFlushIntervalMS = 3600000
	h, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = h.Close() }()
	h.ioMu.Lock()
	h.syncFileFn = func(*os.File) error { return errors.New("sync fault") }
	h.ioMu.Unlock()
	err = h.AppendBatchSync("orders", 0, []types.Message{{Payload: "one"}, {Payload: "two"}})
	require.ErrorContains(t, err, "sync fault")
	require.Error(t, h.writeAvailabilityError())
}
