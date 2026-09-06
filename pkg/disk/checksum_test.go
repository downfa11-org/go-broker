package disk

import (
	"bytes"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func corruptFirstPayload(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	position := bytes.Index(data, []byte("000000:"))
	require.GreaterOrEqual(t, position, 0)
	data[position] ^= 1
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt(data[position:position+1], int64(position))
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
}

func TestRestartRejectsChecksumCorruptionBeforeLastIndex(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir())
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	appendRecoveryMessages(t, handler, 0, 50, 96)
	require.NoError(t, handler.Close())
	path := handler.GetSegmentPath(0)
	corruptFirstPayload(t, path)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = NewDiskHandler(cfg, "orders", 0)
	require.ErrorIs(t, err, util.ErrDiskRecordChecksum)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "corrupt complete record must not be truncated")
}

func TestChecksumReadFailureFencesFurtherWrites(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir())
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = handler.Close() }()
	appendRecoveryMessages(t, handler, 0, 2, 96)
	corruptFirstPayload(t, handler.GetSegmentPath(0))
	_, err = handler.ReadMessages(0, 1)
	require.ErrorIs(t, err, util.ErrDiskRecordChecksum)
	require.Error(t, handler.writeAvailabilityError())
	_, err = handler.AppendMessageSync("orders", 0, &types.Message{Payload: "must not append"})
	require.Error(t, err)
}
