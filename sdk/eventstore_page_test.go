package sdk

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

type streamPageFixture struct {
	envelope map[string]any
	messages []Message
}

func pageFixture(versions []uint64, through, next uint64) streamPageFixture {
	f := streamPageFixture{envelope: map[string]any{
		"status": "OK", "topic": "orders", "key": "order", "partition": 0, "count": len(versions),
		"stream_version": through, "next_version": next, "has_more": next != 0, "lifecycle_epoch": 1,
	}}
	for _, version := range versions {
		f.messages = append(f.messages, Message{Key: "order", AggregateVersion: version, Payload: "event"})
	}
	return f
}

func streamPageStore(t *testing.T, pages []streamPageFixture) (*EventStore, <-chan []string) {
	t.Helper()
	client, server := net.Pipe()
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
	requests := make(chan []string, 1)
	go func() {
		var commands []string
		defer func() { _ = server.Close(); requests <- commands }()
		connection, request, command, err := acceptWireTestRequest(server)
		if err != nil {
			return
		}
		for i, page := range pages {
			if i > 0 {
				request, err = connection.ReadFrame()
				if err != nil {
					return
				}
				command, err = decodeWireTestCommand(request)
				if err != nil {
					return
				}
			}
			commands = append(commands, command)
			payload, err := json.Marshal(page.envelope)
			if err != nil {
				return
			}
			if err := writeWireTestResponse(connection, request, string(payload)); err != nil {
				return
			}
			payload, err = EncodeBatchMessages("orders", 0, "1", false, page.messages)
			if err != nil {
				return
			}
			if err := connection.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: payload}); err != nil {
				return
			}
		}
	}()
	framed, err := openWireConnection(client, 1000, "none")
	require.NoError(t, err)
	store := NewEventStore("unused", "orders", "producer")
	store.conn = framed
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, requests
}

func TestEventStoreWalksPinnedPages(t *testing.T) {
	store, requests := streamPageStore(t, []streamPageFixture{pageFixture([]uint64{1, 2}, 3, 3), pageFixture([]uint64{3}, 3, 0)})
	var versions []uint64
	require.NoError(t, store.WalkStream("order", 1, func(page *StreamData) error {
		for _, event := range page.Events {
			versions = append(versions, event.Version)
		}
		return nil
	}))
	require.Equal(t, []uint64{1, 2, 3}, versions)
	commands := <-requests
	require.Len(t, commands, 2)
	require.Contains(t, commands[1], "through_version=3")
	require.Contains(t, commands[1], "lifecycle_epoch=1")
	require.Contains(t, commands[1], "snapshot=false")
}

func TestEventStoreReadStreamDoesNotReturnPartialData(t *testing.T) {
	store, _ := streamPageStore(t, []streamPageFixture{pageFixture([]uint64{1, 2}, 3, 3)})
	result, err := store.ReadStream("order")
	require.ErrorIs(t, err, ErrStreamPageRequired)
	require.Nil(t, result)
}

func TestEventStoreRejectsMalformedPagesAndDiscardsConnection(t *testing.T) {
	for _, field := range []string{"topic", "count", "next_version", "lifecycle_epoch", "version", "key", "has_more"} {
		t.Run(field, func(t *testing.T) {
			page := pageFixture([]uint64{1, 2}, 3, 3)
			switch field {
			case "topic":
				page.envelope[field] = "other"
			case "count":
				page.envelope[field] = 3
			case "next_version":
				page.envelope[field] = 2
			case "lifecycle_epoch":
				page.envelope[field] = 0
			case "version":
				page.messages[1].AggregateVersion = 3
			case "key":
				page.messages[1].Key = "other"
			case "has_more":
				page.envelope[field] = false
				page.envelope["next_version"] = 0
			}
			store, _ := streamPageStore(t, []streamPageFixture{page})
			result, err := store.ReadStreamPage("order", 1, 2)
			require.Error(t, err)
			require.Nil(t, result)
			require.Nil(t, store.conn)
		})
	}
}

func TestEventStoreWalkStopsOnVisitorError(t *testing.T) {
	store, requests := streamPageStore(t, []streamPageFixture{pageFixture([]uint64{1, 2}, 3, 3)})
	want := fmt.Errorf("stop replay")
	require.ErrorIs(t, store.WalkStream("order", 1, func(*StreamData) error { return want }), want)
	require.Len(t, <-requests, 1)
}

type pagedRepositoryStore struct {
	*repositoryStore
	pages []*StreamData
}

func (s *pagedRepositoryStore) ReadStream(string) (*StreamData, error) {
	return nil, fmt.Errorf("unbounded read used")
}

func (s *pagedRepositoryStore) WalkStream(_ string, _ uint64, visit func(*StreamData) error) error {
	for _, page := range s.pages {
		if err := visit(page); err != nil {
			return err
		}
	}
	return nil
}

func TestFrameworkLoadsAndReplaysThroughPageVisitor(t *testing.T) {
	store := &pagedRepositoryStore{repositoryStore: &repositoryStore{}}
	for version := uint64(1); version <= 2; version++ {
		event, err := NewEventEnvelope("game", "game-1", "GameCreated", map[string]string{"status": "open"})
		require.NoError(t, err)
		store.pages = append(store.pages, &StreamData{Events: []StreamEvent{envelopeStreamEvent(t, event, version)}})
	}
	repository, err := NewAggregateRepository(store, func(id string) Aggregate { return &snapshotAggregate{id: id} })
	require.NoError(t, err)
	aggregate, err := repository.Load("game-1")
	require.NoError(t, err)
	require.Equal(t, uint64(2), aggregate.Version())
	var versions []uint64
	require.NoError(t, Replay(store, "game-1", 1, nil, func(event EventEnvelope) error { versions = append(versions, event.AggregateVersion); return nil }))
	require.Equal(t, []uint64{1, 2}, versions)
}
