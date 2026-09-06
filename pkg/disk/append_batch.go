package disk

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/types"
)

// AppendBatchSync allocates offsets through the ordered queue and shares a flush.
func (d *DiskHandler) AppendBatchSync(topic string, partition int, messages []types.Message) error {
	if partition < 0 || partition > math.MaxInt32 {
		return fmt.Errorf("partition out of int32 range: %d", partition)
	}
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	if err := d.writeAvailabilityError(); err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	start := atomic.LoadUint64(&d.AbsoluteOffset)
	if uint64(len(messages)) > math.MaxUint64-start {
		return fmt.Errorf("disk offset overflow")
	}
	batch := make([]types.DiskMessage, len(messages))
	for i, message := range messages {
		message.Offset = start + uint64(i)
		batch[i] = diskMessageFromMessage(topic, int32(partition), message)
		if err := validateDiskMessageSize(batch[i]); err != nil {
			return err
		}
	}
	for _, message := range batch {
		select {
		case <-d.done:
			return fmt.Errorf("disk handler is shutting down")
		case d.writeCh <- message:
		}
	}
	d.Flush()
	select {
	case <-d.done:
		return fmt.Errorf("disk handler is shutting down")
	default:
	}
	if err := d.writeAvailabilityError(); err != nil {
		return err
	}
	for i := range messages {
		messages[i].Offset = batch[i].Offset
	}
	atomic.StoreUint64(&d.AbsoluteOffset, start+uint64(len(messages)))
	return nil
}
