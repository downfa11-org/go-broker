package disk

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

func TestSyncFailureMakesHandlerTerminalUntilRestart(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 60_000

	handler, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	syncFailure := errors.New("injected fsync failure")
	t.Cleanup(func() {
		handler.syncFileFn = nil
		if closeErr := handler.Close(); !errors.Is(closeErr, syncFailure) {
			t.Errorf("close handler = %v, want terminal sync failure", closeErr)
		}
	})

	handler.syncFileFn = func(*os.File) error {
		return syncFailure
	}
	if _, err := handler.AppendMessageSync("orders", 0, &types.Message{Payload: "first"}); err == nil {
		t.Fatal("expected first append to report sync failure")
	}
	sizeAfterFailure := segmentSize(t, handler.GetSegmentPath(0))

	handler.syncFileFn = nil
	if _, err := handler.AppendMessageSync("orders", 0, &types.Message{Payload: "retry"}); err == nil ||
		!strings.Contains(err.Error(), "unavailable until restart") {
		t.Fatalf("sync append after failure = %v, want terminal unavailable error", err)
	}
	if _, err := handler.AppendMessage("orders", 0, &types.Message{Payload: "async-retry"}); err == nil ||
		!strings.Contains(err.Error(), "unavailable until restart") {
		t.Fatalf("async append after failure = %v, want terminal unavailable error", err)
	}
	if got := segmentSize(t, handler.GetSegmentPath(0)); got != sizeAfterFailure {
		t.Fatalf("terminal handler changed segment size: got %d want %d", got, sizeAfterFailure)
	}
}

func segmentSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestPostWriteFlushFailureMakesHandlerTerminalUntilRestart(t *testing.T) {
	writes := map[string]func(*DiskHandler) error{
		"direct": func(handler *DiskHandler) error {
			return handler.WriteDirect("orders", 0, types.Message{Offset: 0, Payload: "first"})
		},
		"batch": func(handler *DiskHandler) error {
			return handler.WriteBatch([]types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 0, Payload: "first"}})
		},
	}

	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.DiskFlushIntervalMS = 60_000
			handler, err := NewDiskHandler(cfg, "orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = handler.Close() })
			if err := handler.file.Close(); err != nil {
				t.Fatal(err)
			}

			firstErr := write(handler)
			if firstErr == nil || !strings.Contains(firstErr.Error(), "unavailable until restart") {
				t.Fatalf("post-write failure = %v, want terminal unavailable error", firstErr)
			}
			secondErr := write(handler)
			if secondErr == nil || !strings.Contains(secondErr.Error(), "unavailable until restart") {
				t.Fatalf("retry after post-write failure = %v, want terminal unavailable error", secondErr)
			}
		})
	}
}

func TestWriteBatchSyncUsesSingleFilesystemSync(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 60_000

	handler, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		handler.syncFileFn = nil
		_ = handler.Close()
	})

	syncCalls := 0
	handler.syncFileFn = func(*os.File) error {
		syncCalls++
		return nil
	}
	batch := make([]types.DiskMessage, 1000)
	for i := range batch {
		batch[i] = types.DiskMessage{
			Topic:      "orders",
			Partition:  0,
			Offset:     uint64(i),
			ProducerID: "producer-1",
			SeqNum:     uint64(i + 1),
			Payload:    "payload",
		}
	}

	if err := handler.WriteBatchSync(batch); err != nil {
		t.Fatalf("WriteBatchSync failed: %v", err)
	}
	if syncCalls != 1 {
		t.Fatalf("filesystem sync calls = %d, want 1 for the whole batch", syncCalls)
	}
}

func TestWriteBatchSyncFailureMakesHandlerTerminal(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 60_000

	handler, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		handler.syncFileFn = nil
		_ = handler.Close()
	})
	handler.syncFileFn = func(*os.File) error { return errors.New("injected batch fsync failure") }

	batch := []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 0, Payload: "first"}}
	if err := handler.WriteBatchSync(batch); err == nil || !strings.Contains(err.Error(), "sync disk batch") {
		t.Fatalf("durable batch error = %v, want sync failure", err)
	}
	handler.syncFileFn = nil
	if err := handler.WriteBatchSync(batch); err == nil || !strings.Contains(err.Error(), "unavailable until restart") {
		t.Fatalf("durable batch retry = %v, want terminal unavailable error", err)
	}
}
