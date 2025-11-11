package disk_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
)

// TestDiskHandlerBasic verifies basic append and flush behavior
func TestDiskHandlerBasic(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize: 3,
		LingerMS:           50,
		ChannelBufferSize:  5,
		DiskWriteTimeoutMS: 100,
		LogDir:             dir,
	}

	topic := "testlog"
	segmentSize := 1024

	dh, err := disk.NewDiskHandler(cfg, topic, 0, segmentSize)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer dh.Close()

	messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	for _, msg := range messages {
		dh.AppendMessage(msg)
	}

	time.Sleep(150 * time.Millisecond)

	pattern := filepath.Join(cfg.LogDir, topic, "partition_0_segment_*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("Expected at least 1 segment file, got %d", len(files))
	}

	allContent := ""
	for _, f := range files {
		data, _ := os.ReadFile(f)
		allContent += string(data)
	}
	for _, msg := range messages {
		if !strings.Contains(allContent, msg) {
			t.Errorf("Message %q not found in logs", msg)
		}
	}
}

// TestDiskHandlerChannelOverflow ensures synchronous fallback works
func TestDiskHandlerChannelOverflow(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize: 2,
		LingerMS:           50,
		ChannelBufferSize:  2,
		DiskWriteTimeoutMS: 100,
		LogDir:             dir,
	}

	topic := "overflowlog"
	segmentSize := 1024

	dh, err := disk.NewDiskHandler(cfg, topic, 0, segmentSize)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer dh.Close()

	dh.AppendMessage("first")
	dh.AppendMessage("second")

	time.Sleep(50 * time.Millisecond)

	pattern := filepath.Join(cfg.LogDir, topic, "partition_0_segment_*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("Expected segment file to be created")
	}

	content, _ := os.ReadFile(files[0])
	if !strings.Contains(string(content), "first") || !strings.Contains(string(content), "second") {
		t.Errorf("Expected both messages in file, got %q", string(content))
	}
}

// TestDiskHandlerRotation verifies segment rotation
func TestDiskHandlerRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize: 1,
		LingerMS:           10,
		ChannelBufferSize:  10,
		DiskWriteTimeoutMS: 100,
		LogDir:             dir,
	}

	topic := "rotationlog"
	segmentSize := 10

	dh, err := disk.NewDiskHandler(cfg, topic, 0, segmentSize)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer dh.Close()

	msgs := []string{"12345", "67890", "abcde"}
	for _, m := range msgs {
		dh.AppendMessage(m)
	}

	time.Sleep(50 * time.Millisecond)

	pattern := filepath.Join(cfg.LogDir, topic, "partition_0_segment_*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) < 2 {
		t.Errorf("Expected multiple segment files, got %d", len(files))
	}

	all := ""
	for _, f := range files {
		data, _ := os.ReadFile(f)
		all += string(data)
	}
	for _, m := range msgs {
		if !strings.Contains(all, m) {
			t.Errorf("Message %q not found in files", m)
		}
	}
}
