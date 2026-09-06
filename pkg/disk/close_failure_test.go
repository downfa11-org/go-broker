package disk

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCloseRetainsSyncFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 60000
	d, err := NewDiskHandler(cfg, "close", 0)
	require.NoError(t, err)
	want := errors.New("close sync failed")
	d.syncFileFn = func(*os.File) error { return want }
	require.ErrorIs(t, d.Close(), want)
	require.ErrorIs(t, d.Close(), want)
}

func TestCloseReportsTerminalWriteFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	d, err := NewDiskHandler(cfg, "failed", 0)
	require.NoError(t, err)
	want := errors.New("earlier write failed")
	require.ErrorIs(t, d.markWriteUnavailable(want), want)
	require.ErrorIs(t, d.Close(), want)
	require.ErrorIs(t, d.Close(), want)
}

func TestManagerCloseRetainsEvictionFailures(t *testing.T) {
	for _, eviction := range []string{"all", "topic", "partition", "none"} {
		t.Run(eviction, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			dm := NewDiskManager(cfg)
			handler, err := dm.GetHandler("failed", 0)
			require.NoError(t, err)
			want := errors.New("terminal storage failure")
			require.ErrorIs(t, handler.(*DiskHandler).markWriteUnavailable(want), want)
			switch eviction {
			case "all":
				dm.CloseAllHandlers()
			case "topic":
				dm.CloseTopicHandlers("failed")
			case "partition":
				dm.ClosePartitionHandler("failed", 0)
			}
			require.ErrorIs(t, dm.Close(), want)
			require.ErrorIs(t, dm.Close(), want)
			_, err = dm.GetHandler("new", 0)
			require.ErrorContains(t, err, "closed")
			_, err = dm.GetHandlerWithPolicy("failed", 0, config.CleanupPolicyDelete, 24, 0)
			require.ErrorContains(t, err, "closed")
		})
	}
}

func TestConcurrentCloseRetainsFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	d, err := NewDiskHandler(cfg, "concurrent", 0)
	require.NoError(t, err)
	want := errors.New("terminal write failure")
	require.ErrorIs(t, d.markWriteUnavailable(want), want)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.Close(); !errors.Is(err, want) {
				t.Errorf("Close = %v, want %v", err, want)
			}
		}()
	}
	wg.Wait()
}
