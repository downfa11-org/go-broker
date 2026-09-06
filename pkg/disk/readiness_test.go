package disk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
)

func TestDiskManagerReadyChecksLogDirectory(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = filepath.Join(t.TempDir(), "missing")
	manager := NewDiskManager(cfg)
	if err := manager.Ready(); err == nil {
		t.Fatal("Ready accepted a missing log directory")
	}
	if err := os.MkdirAll(cfg.LogDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := manager.Ready(); err != nil {
		t.Fatalf("Ready rejected an available log directory: %v", err)
	}
}

func TestDiskManagerReadyRejectsClosedRegisteredHandler(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	manager := NewDiskManager(cfg)
	handler, err := manager.GetHandler("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Ready(); err == nil {
		t.Fatal("Ready accepted a closed registered handler")
	}
	manager.CloseAllHandlers()
}

func TestDiskManagerReadyRejectsTerminalWriteFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	manager := NewDiskManager(cfg)
	storage, err := manager.GetHandler("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.CloseAllHandlers)

	handler := storage.(*DiskHandler)
	handler.writeFailureMu.Lock()
	handler.writeFailure = errors.New("injected fsync failure")
	handler.writeFailureMu.Unlock()

	if err := manager.Ready(); err == nil {
		t.Fatal("Ready accepted a handler with a terminal write failure")
	}
}
