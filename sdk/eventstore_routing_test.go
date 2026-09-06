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

type eventRouteResult struct {
	commands []string
	err      error
}

func eventRouteServer(t *testing.T, count int, respond func(string, *wire.Connection, wire.Frame) error) (string, <-chan eventRouteResult) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(10*time.Second)))
	t.Cleanup(func() { _ = listener.Close() })
	results := make(chan eventRouteResult, 1)
	go func() {
		result := eventRouteResult{}
		defer func() { results <- result }()
		for i := 0; i < count; i++ {
			conn, err := listener.Accept()
			if err != nil {
				result.err = err
				return
			}
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			connection, request, command, err := acceptWireTestRequest(conn)
			if err == nil {
				result.commands = append(result.commands, command)
				err = respond(listener.Addr().String(), connection, request)
			}
			_ = conn.Close()
			if err != nil {
				result.err = err
				return
			}
		}
	}()
	return listener.Addr().String(), results
}

func TestEventStoreFollowsReadLeaderWithoutChangingPageBoundary(t *testing.T) {
	page := pageFixture([]uint64{3}, 3, 0)
	envelope, err := json.Marshal(page.envelope)
	require.NoError(t, err)
	batch, err := EncodeBatchMessages("orders", 0, "1", false, page.messages)
	require.NoError(t, err)
	leader, leaderResults := eventRouteServer(t, 1, func(_ string, conn *wire.Connection, request wire.Frame) error {
		if err := writeWireTestResponse(conn, request, string(envelope)); err != nil {
			return err
		}
		return conn.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: batch})
	})
	seed, seedResults := eventRouteServer(t, 1, func(_ string, conn *wire.Connection, request wire.Frame) error {
		return writeWireTestResponse(conn, request, "ERROR: NOT_LEADER leader="+leader)
	})
	store := NewEventStore(seed, "orders", "producer")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	got, err := store.readStreamPage("order", 3, 1, 3, 1, false)
	require.NoError(t, err)
	require.Len(t, got.Events, 1)
	require.Equal(t, uint64(3), got.Events[0].Version)
	seedResult, leaderResult := <-seedResults, <-leaderResults
	require.NoError(t, seedResult.err)
	require.NoError(t, leaderResult.err)
	require.Equal(t, seedResult.commands, leaderResult.commands)
	require.Contains(t, leaderResult.commands[0], "through_version=3")
	require.Contains(t, leaderResult.commands[0], "lifecycle_epoch=1")
}

func TestEventStoreBoundsReadLeaderRedirects(t *testing.T) {
	addr, results := eventRouteServer(t, 4, func(addr string, conn *wire.Connection, request wire.Frame) error {
		return writeWireTestResponse(conn, request, "ERROR: NOT_LEADER leader="+addr)
	})
	store := NewEventStore(addr, "orders", "producer")
	page, err := store.ReadStream("order")
	require.Error(t, err)
	require.Nil(t, page)
	require.Nil(t, store.conn)
	result := <-results
	require.NoError(t, result.err)
	require.Len(t, result.commands, 4)
}

func TestEventStoreDoesNotRedirectAfterEnvelopeOrToInvalidLeader(t *testing.T) {
	for _, test := range []struct {
		name, leader string
		body         bool
	}{
		{name: "missing leader"}, {name: "invalid port", leader: "localhost:99999"},
		{name: "body error", leader: "localhost:1", body: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			addr, results := eventRouteServer(t, 1, func(_ string, conn *wire.Connection, request wire.Frame) error {
				if test.body {
					page := pageFixture([]uint64{1}, 1, 0)
					data, err := json.Marshal(page.envelope)
					if err != nil {
						return err
					}
					if err := writeWireTestResponse(conn, request, string(data)); err != nil {
						return err
					}
				}
				return writeWireTestResponse(conn, request, fmt.Sprintf("ERROR: NOT_LEADER leader=%s", test.leader))
			})
			store := NewEventStore(addr, "orders", "producer")
			page, err := store.ReadStream("order")
			require.Error(t, err)
			require.Nil(t, page)
			require.Nil(t, store.conn)
			require.Equal(t, addr, store.addr)
			result := <-results
			require.NoError(t, result.err)
			require.Len(t, result.commands, 1)
		})
	}
}
