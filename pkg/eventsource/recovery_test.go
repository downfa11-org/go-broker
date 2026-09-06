package eventsource

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestIndexRecoveryRejectsUnavailableHistory(t *testing.T) {
	for _, tc := range []struct {
		name    string
		first   uint64
		version uint64
		readErr error
		want    string
	}{
		{name: "retained suffix", first: 2, version: 3, want: "event history unavailable"},
		{name: "first read failure", version: 1, readErr: fmt.Errorf("disk read failed"), want: "disk read failed"},
		{name: "missing aggregate prefix", version: 2, want: "stream index gap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := &fakeStorageHandler{firstOffset: tc.first, offset: tc.first + 1, readErr: tc.readErr,
				msgs: []types.Message{{Offset: tc.first, Key: "order", AggregateVersion: tc.version, Payload: "event"}}}
			provider := newFakeHandlerProvider()
			provider.handlers["orders:0"] = storage
			manager := topic.NewTopicManager(&config.Config{LogDir: t.TempDir()}, provider, &fakeStreamManager{})
			require.NoError(t, manager.CreateTopic("orders", 1, false, true))
			defer manager.GetTopic("orders").Partitions[0].Close()
			h := NewHandler(manager)
			defer func() { require.NoError(t, h.Close()) }()
			_, err := h.getIndex("orders", 0)
			require.ErrorContains(t, err, tc.want)
			require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=order version=1 message=new"), "ERROR:")
			require.Len(t, storage.msgs, 1)
		})
	}
}

func TestFailedIndexRebuildFencesCachedIndex(t *testing.T) {
	h := newTestHandler(t)
	defer func() { require.NoError(t, h.Close()) }()
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=order version=1 message=first"), "OK")
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	require.NoError(t, h.tm.GetTopic("orders").Partitions[0].EnqueueSync(types.Message{Key: "order", AggregateVersion: 3, Payload: "gap"}))
	require.ErrorContains(t, h.RecoverIndexFromLog("orders", 0, idx), "stream index gap")
	_, err = h.getIndex("orders", 0)
	require.ErrorContains(t, err, "stream index gap")
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=order version=2 message=unsafe"), "ERROR:")
}
