package controller

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumeArgumentLimits(t *testing.T) {
	ch := &CommandHandler{}
	for _, args := range []map[string]string{
		{"batch": "0"}, {"batch": "-1"}, {"batch": "1025"}, {"batch": "bad"},
		{"wait_ms": "-1"}, {"wait_ms": "30001"}, {"wait_ms": "9223372036854775807"}, {"wait_ms": "bad"},
	} {
		_, err := ch.parseCommonArgs(args)
		require.Error(t, err, "%v", args)
	}
	args, err := ch.parseCommonArgs(map[string]string{"batch": "1024", "wait_ms": "30000"})
	require.NoError(t, err)
	require.Equal(t, MaxConsumeBatchRecords, args.BatchSize)
	require.Equal(t, MaxConsumeWait, args.WaitTimeout)
}

func TestConsumeLongPollCanceled(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("waiting", 1, false, false))
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClientContext("", 0)
	client.SetRequestContext(request)
	server, peer := net.Pipe()
	defer func() { _ = server.Close(); _ = peer.Close() }()
	done := make(chan error, 1)
	go func() {
		_, err := ch.HandleConsumeCommand(server, "CONSUME topic=waiting partition=0 offset=0 member=m wait_ms=30000", client)
		done <- err
	}()
	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("long poll ignored cancellation")
	}
}

func TestConsumeFailedWriteRestoresReadCursor(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("cursor", 1, false, false))
	client := NewClientContext("", 0)
	require.NotContains(t, ch.HandleCommand("PUBLISH topic=cursor acks=1 producerId=p message=event", client), "ERROR")
	client.OffsetCache["unrelated"] = 12
	server, peer := net.Pipe()
	defer func() { _ = server.Close() }()
	require.NoError(t, peer.Close())
	_, err := ch.HandleConsumeCommand(server, "CONSUME topic=cursor partition=0 offset=0 member=m", client)
	require.Error(t, err)
	require.Equal(t, map[string]uint64{"unrelated": 12}, client.OffsetCache)
}
