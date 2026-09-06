package topic

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

func (p *Partition) appendStandaloneBatch(storage types.AllocatingBatchStorage, messages []types.Message, force bool) error {
	staged := make(map[string]stagedProducerEntry)
	batch := make([]types.Message, 0, len(messages))
	indices := make([]int, 0, len(messages))
	for i := range messages {
		duplicate, err := p.validateProducerMessageWithStage(&messages[i], staged, force)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}
		if (p.isIdempotent || force) && messages[i].ProducerID != "" && messages[i].SeqNum > 0 {
			staged[messages[i].ProducerID] = stagedProducerEntry{lastEpoch: messages[i].Epoch, lastSeq: messages[i].SeqNum}
		}
		batch = append(batch, messages[i])
		indices = append(indices, i)
	}
	if len(batch) == 0 {
		return nil
	}
	if err := storage.AppendBatchSync(p.topic, p.id, batch); err != nil {
		return fmt.Errorf("disk batch write failed for partition %d: %w", p.id, err)
	}
	for i, message := range batch {
		messages[indices[i]].Offset = message.Offset
		p.updateProducerStateWithMode(&message, force)
		p.indexTransactionMessage(message)
	}
	next := batch[len(batch)-1].Offset + 1
	p.LEO.Store(next)
	p.setHWMLocked(next)
	p.NotifyNewMessage()
	return nil
}
