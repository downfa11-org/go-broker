package stream

import (
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestManagerCloseInterruptsBlockedWriteAndRejectsNewStreams(t *testing.T) {
	manager := NewStreamManager(2, time.Minute)
	server, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	started := make(chan struct{}, 1)
	read := func(offset uint64, max int) ([]types.Message, error) {
		if offset != 0 {
			return nil, nil
		}
		select {
		case started <- struct{}{}:
		default:
		}
		return []types.Message{{Payload: "blocked", Offset: 0}}, nil
	}
	require.NoError(t, manager.AddStream("first", NewStreamConnection(server, "topic", 0, "group", 0), read))
	<-started
	closed := make(chan struct{})
	go func() { manager.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("stream manager did not drain a blocked writer")
	}
	require.ErrorContains(t, manager.AddStream("second", NewStreamConnection(nil, "topic", 0, "group", 0), read), "closed")
	manager.Close()
}
