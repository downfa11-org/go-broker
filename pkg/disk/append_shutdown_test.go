package disk

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

func TestAppendMessageSyncRejectsShutdownBeforeFlushCompletes(t *testing.T) {
	for _, inFlight := range []bool{false, true} {
		name := "before_flush_request"
		if inFlight {
			name = "pending_flush_request"
		}
		t.Run(name, func(t *testing.T) {
			d := &DiskHandler{
				AbsoluteOffset: 7,
				FlushedOffset:  7,
				done:           make(chan struct{}),
				writeCh:        make(chan types.DiskMessage),
				flushSignal:    make(chan chan struct{}, 1),
			}
			if !inFlight {
				d.flushSignal = make(chan chan struct{})
			}
			var stopOnce sync.Once
			stop := func() { stopOnce.Do(func() { close(d.done) }) }
			t.Cleanup(stop)
			type result struct {
				offset uint64
				err    error
			}
			resultCh := make(chan result, 1)
			go func() {
				offset, err := d.AppendMessageSync("orders", 0, &types.Message{Payload: "pending"})
				resultCh <- result{offset: offset, err: err}
			}()
			select {
			case <-d.writeCh:
			case <-time.After(time.Second):
				t.Fatal("append did not enqueue the record")
			}
			if inFlight {
				select {
				case flushed := <-d.flushSignal:
					t.Cleanup(func() { close(flushed) })
				case <-time.After(time.Second):
					t.Fatal("append did not request a flush")
				}
			}
			stop()
			select {
			case got := <-resultCh:
				if got.err == nil || !strings.Contains(got.err.Error(), "shutting down") {
					t.Fatalf("append error = %v, want shutdown error", got.err)
				}
				if got.offset != 0 || d.GetAbsoluteOffset() != 7 {
					t.Fatalf("append advanced offset: returned=%d allocated=%d", got.offset, d.GetAbsoluteOffset())
				}
			case <-time.After(time.Second):
				t.Fatal("append did not stop after shutdown interrupted the flush")
			}
		})
	}
}
