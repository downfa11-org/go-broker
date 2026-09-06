package util

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
)

// BatchReadBudget bounds materialized records across reads in one response.
type BatchReadBudget struct {
	topic     string
	partition int
	remaining int
	count     int
}

func NewBatchReadBudget(topic string, partition, maxBytes int) (*BatchReadBudget, error) {
	header, err := EncodeBatchMessages(topic, partition, "1", false, nil)
	if err != nil {
		return nil, err
	}
	if maxBytes < len(header) || maxBytes > wire.MaxFramePayload {
		return nil, fmt.Errorf("invalid batch byte budget")
	}
	return &BatchReadBudget{topic: topic, partition: partition, remaining: maxBytes - len(header)}, nil
}

func (b *BatchReadBudget) Read(offset uint64, max int, read func(uint64, int) ([]types.Message, error)) ([]types.Message, error) {
	var result []types.Message
	for len(result) < max {
		messages, err := read(offset, 1)
		if err != nil {
			return nil, err
		}
		if len(messages) == 0 {
			break
		}
		if len(messages) != 1 || messages[0].Offset < offset || messages[0].Offset == ^uint64(0) {
			return nil, fmt.Errorf("invalid bounded read result at offset %d", offset)
		}
		message := messages[0]
		message.Topic, message.Partition = b.topic, b.partition
		record, err := wire.EncodeRecord(message)
		if err != nil {
			return nil, err
		}
		if len(record)+4 > b.remaining {
			if b.count == 0 {
				return nil, fmt.Errorf("record exceeds batch byte budget at offset %d", offset)
			}
			break
		}
		b.remaining -= len(record) + 4
		b.count++
		result = append(result, messages[0])
		offset = message.Offset + 1
	}
	return result, nil
}
