package eventsource

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

type testStreamPage struct {
	Status         string        `json:"status"`
	Error          string        `json:"error"`
	Count          int           `json:"count"`
	StreamVersion  uint64        `json:"stream_version"`
	NextVersion    uint64        `json:"next_version"`
	HasMore        bool          `json:"has_more"`
	LifecycleEpoch uint64        `json:"lifecycle_epoch"`
	Snapshot       *SnapshotData `json:"snapshot"`
}

func readTestStreamPage(t *testing.T, h *Handler, args string) (testStreamPage, []types.Message) {
	t.Helper()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	done := make(chan struct{})
	go func() {
		defer func() { _ = server.Close() }()
		defer close(done)
		h.HandleReadStream("READ_STREAM topic=orders key=order "+args, server)
	}()
	data, err := util.ReadWithLength(client)
	require.NoError(t, err)
	var page testStreamPage
	if strings.HasPrefix(string(data), "ERROR:") {
		page.Status, page.Error = "ERROR", string(data)
	} else {
		require.NoError(t, json.Unmarshal(data, &page))
	}
	var messages []types.Message
	if page.Status == "OK" {
		data, err = util.ReadWithLength(client)
		require.NoError(t, err)
		batch, err := util.DecodeBatchMessages(data)
		require.NoError(t, err)
		messages = batch.Messages
		require.Len(t, messages, page.Count)
	}
	<-done
	return page, messages
}

func TestStreamPagesPinVersionAndIgnoreNewSnapshots(t *testing.T) {
	h := newTestHandler(t)
	defer func() { require.NoError(t, h.Close()) }()
	for version := 1; version <= 5; version++ {
		require.Contains(t, h.HandleAppendStream(fmt.Sprintf("APPEND_STREAM topic=orders key=order version=%d message=event", version)), "OK")
	}
	page, messages := readTestStreamPage(t, h, "limit=2 snapshot=false")
	require.Equal(t, "OK", page.Status)
	require.True(t, page.HasMore)
	require.Equal(t, uint64(3), page.NextVersion)
	require.Equal(t, uint64(5), page.StreamVersion)
	require.Len(t, messages, 2)
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=order version=6 message=later"), "OK")
	require.Contains(t, h.HandleSaveSnapshot("SAVE_SNAPSHOT topic=orders key=order version=6 message=snapshot"), "OK")
	page, messages = readTestStreamPage(t, h, "limit=2 from_version=3 through_version=5 lifecycle_epoch=1 snapshot=false")
	require.Nil(t, page.Snapshot)
	require.Equal(t, uint64(5), page.NextVersion)
	require.Equal(t, uint64(3), messages[0].AggregateVersion)
	page, messages = readTestStreamPage(t, h, "limit=2 from_version=5 through_version=5 lifecycle_epoch=1 snapshot=false")
	require.False(t, page.HasMore)
	require.Zero(t, page.NextVersion)
	require.Len(t, messages, 1)
	require.Equal(t, uint64(5), messages[0].AggregateVersion)
}

func TestReadStreamRequiresPaginationInsteadOfTruncatingLegacyResults(t *testing.T) {
	h := newTestHandler(t)
	defer func() { require.NoError(t, h.Close()) }()
	for version := 1; version <= wire.DefaultStreamPageEvents+1; version++ {
		require.Contains(t, h.HandleAppendStream(fmt.Sprintf("APPEND_STREAM topic=orders key=order version=%d message=event", version)), "OK")
	}
	page, _ := readTestStreamPage(t, h, "")
	require.Equal(t, "ERROR", page.Status)
	require.Contains(t, page.Error, "stream_page_required")
	for _, args := range []string{"limit=0", "limit=-1", "limit=1025", "limit=bad", "limit=1 lifecycle_epoch=2", "limit=1 through_version=99999", "limit=1 snapshot=bad"} {
		page, _ = readTestStreamPage(t, h, args)
		require.Equal(t, "ERROR", page.Status, args)
	}
}

func TestReadStreamRejectsMissingIndexedEvent(t *testing.T) {
	h := newTestHandler(t)
	defer func() { require.NoError(t, h.Close()) }()
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	require.NoError(t, idx.Append("order", 1, 999, 0))
	page, _ := readTestStreamPage(t, h, "limit=1")
	require.Equal(t, "ERROR", page.Status)
	require.Contains(t, page.Error, "stream_event_unavailable")
}

func TestReadStreamEncodingFailurePrecedesSuccessEnvelope(t *testing.T) {
	provider := newFakeHandlerProvider()
	provider.handlers["orders:0"] = &fakeStorageHandler{offset: 1, msgs: []types.Message{
		{Key: "order", AggregateVersion: 1, Payload: "event", TransactionState: "invalid"},
	}}
	manager := topic.NewTopicManager(&config.Config{LogDir: t.TempDir()}, provider, &fakeStreamManager{})
	require.NoError(t, manager.CreateTopic("orders", 1, false, true))
	defer manager.GetTopic("orders").Partitions[0].Close()
	h := NewHandler(manager)
	defer func() { require.NoError(t, h.Close()) }()
	page, _ := readTestStreamPage(t, h, "limit=1")
	require.Equal(t, "ERROR", page.Status)
	require.Contains(t, page.Error, "encode_stream_failed")
}

func TestStreamPageStopsAtExactEncodedByteBudget(t *testing.T) {
	h := newTestHandler(t)
	defer func() { require.NoError(t, h.Close()) }()
	for version := 1; version <= 3; version++ {
		require.Contains(t, h.HandleAppendStream(fmt.Sprintf("APPEND_STREAM topic=orders key=order version=%d message=payload", version)), "OK")
	}
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	entries, _, _, err := idx.LookupPage("order", 1, 0, 3)
	require.NoError(t, err)
	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	first, err := p.ReadCommitted(0, 1)
	require.NoError(t, err)
	encoded, err := util.EncodeBatchMessages("orders", 0, "1", false, first)
	require.NoError(t, err)
	page, more, err := readIndexedStreamPage(p, entries, "order", "orders", 0, len(encoded))
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.True(t, more)
	_, _, err = readIndexedStreamPage(p, entries, "order", "orders", 0, len(encoded)-1)
	require.ErrorContains(t, err, "stream_event_too_large")
}
