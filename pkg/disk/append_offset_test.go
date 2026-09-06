package disk_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func TestAppendMessageSyncFailureDoesNotConsumeOffset(t *testing.T) {
	dh := setupDiskHandler(t)
	defer func() { _ = dh.Close() }()

	first := types.Message{Payload: "first"}
	firstOffset, err := dh.AppendMessageSync("testTopic", 0, &first)
	if err != nil {
		t.Fatalf("first append failed: %v", err)
	}
	if firstOffset != 0 {
		t.Fatalf("expected first offset 0, got %d", firstOffset)
	}

	failed := types.Message{Payload: "must-not-be-written"}
	if _, err := dh.AppendMessageSync("testTopic", -1, &failed); err == nil {
		t.Fatal("expected invalid partition append to fail")
	}
	if got := dh.GetAbsoluteOffset(); got != 1 {
		t.Fatalf("failed append consumed an offset: got tail %d, want 1", got)
	}

	second := types.Message{Payload: "second"}
	secondOffset, err := dh.AppendMessageSync("testTopic", 0, &second)
	if err != nil {
		t.Fatalf("second append failed: %v", err)
	}
	if secondOffset != 1 {
		t.Fatalf("expected failed offset to be reused, got %d, want 1", secondOffset)
	}

	messages, err := dh.ReadMessages(0, 2)
	if err != nil {
		t.Fatalf("read messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	for i, message := range messages {
		if message.Offset != uint64(i) {
			t.Fatalf("message %d has offset %d", i, message.Offset)
		}
	}
}

func TestAppendMessageSyncConcurrentOffsetsAreContiguous(t *testing.T) {
	dh := setupDiskHandler(t)
	defer func() { _ = dh.Close() }()

	const writers = 32
	start := make(chan struct{})
	offsets := make(chan uint64, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			msg := types.Message{Payload: fmt.Sprintf("message-%d", i)}
			offset, err := dh.AppendMessageSync("testTopic", 0, &msg)
			if err != nil {
				errs <- err
				return
			}
			offsets <- offset
		}(i)
	}
	close(start)
	wg.Wait()
	close(offsets)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent append failed: %v", err)
	}
	seen := make([]bool, writers)
	for offset := range offsets {
		index, ok := util.SafeUint64ToInt(offset)
		if !ok || index < 0 || index >= len(seen) {
			t.Fatalf("offset %d is outside expected range", offset)
		}
		if seen[index] {
			t.Fatalf("duplicate offset %d", offset)
		}
		seen[index] = true
	}
	for offset, present := range seen {
		if !present {
			t.Fatalf("missing offset %d", offset)
		}
	}
	if got := dh.GetAbsoluteOffset(); got != writers {
		t.Fatalf("got tail %d, want %d", got, writers)
	}
}

func TestAppendMessageSyncDrainsEarlierAsyncAppendInOrder(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.LingerMS = 60_000
	cfg.DiskFlushIntervalMS = 60_000
	handler, err := disk.NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	asyncOffset, err := handler.AppendMessage("orders", 0, &types.Message{Payload: "async"})
	if err != nil {
		t.Fatal(err)
	}
	syncOffset, err := handler.AppendMessageSync("orders", 0, &types.Message{Payload: "sync"})
	if err != nil {
		t.Fatal(err)
	}
	if asyncOffset != 0 || syncOffset != 1 {
		t.Fatalf("offsets = %d, %d; want 0, 1", asyncOffset, syncOffset)
	}

	messages, err := handler.ReadMessages(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Offset != 0 || messages[1].Offset != 1 {
		t.Fatalf("messages = %+v, want offsets 0 then 1", messages)
	}
}
