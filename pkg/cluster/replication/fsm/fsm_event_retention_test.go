package fsm

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestFSMRejectsEventRetentionBeforeChangingTopicState(t *testing.T) {
	f := newTestFSM()
	registerActiveBroker(t, f, "broker-1")
	command := testTopicCommand("events", 1, 1)
	command.Definition.EventSourcing = true
	command.Definition.Policy.RetentionHours = 1
	payload, err := json.Marshal(command)
	require.NoError(t, err)
	result := f.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...), Index: 2})
	require.ErrorContains(t, result.(error), "complete event history")
	require.Nil(t, f.topicState["events"])
	require.Nil(t, f.GetPartitionMetadata("events-0"))
	require.Nil(t, f.tm.GetTopic("events"))
}
