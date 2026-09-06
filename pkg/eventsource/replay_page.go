package eventsource

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

func readIndexedStreamPage(p *topic.Partition, entries []StreamIndexEntry, key, topicName string, partitionID, maxBytes int) ([]types.Message, bool, error) {
	emptyBatch, err := util.EncodeBatchMessages(topicName, partitionID, "1", false, nil)
	if err != nil {
		return nil, false, fmt.Errorf("encode_stream_failed reason=%q", err.Error())
	}
	remaining := maxBytes - len(emptyBatch)
	var messages []types.Message
	for _, entry := range entries {
		batch, err := p.ReadCommitted(entry.Offset, 1)
		if err != nil {
			return nil, false, fmt.Errorf("partition_read_failed offset=%d reason=%q", entry.Offset, err.Error())
		}
		if len(batch) != 1 || batch[0].Key != key || batch[0].Offset != entry.Offset || batch[0].AggregateVersion != entry.AggregateVersion {
			return nil, false, fmt.Errorf("stream_event_unavailable version=%d offset=%d", entry.AggregateVersion, entry.Offset)
		}
		msg := batch[0]
		msg.Topic, msg.Partition = topicName, partitionID
		record, err := wire.EncodeRecord(msg)
		if err != nil {
			return nil, false, fmt.Errorf("encode_stream_failed reason=%q", err.Error())
		}
		if len(record)+4 > remaining {
			if len(messages) == 0 {
				return nil, false, fmt.Errorf("stream_event_too_large")
			}
			return messages, true, nil
		}
		remaining -= len(record) + 4
		messages = append(messages, msg)
	}
	return messages, false, nil
}
