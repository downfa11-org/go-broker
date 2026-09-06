package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func newAdmissionPeer(t *testing.T, handler *controller.CommandHandler) (net.Conn, *wire.Connection, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnWithContext(ctx, server, handler, controller.NewClientContext("g", 0))
	}()
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("admission connection did not stop")
		}
	})
	connection, err := wire.ClientHandshake(client, []wire.Compression{wire.CompressionNone})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return handler.RequestBudget(false).Snapshot().Frames == 0 }, time.Second, time.Millisecond)
	return client, connection, cancel, done
}

func admissionHelpFrame(t *testing.T) wire.Frame {
	t.Helper()
	command, request, err := wire.ParseCommandText("HELP")
	require.NoError(t, err)
	payload, err := wire.EncodeCommandPayload(request)
	require.NoError(t, err)
	return wire.Frame{Kind: wire.KindRequest, Command: command, RequestID: 1, Payload: payload}
}

func TestRequestReservationHeldThroughBlockedResponse(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxInFlightRequests, cfg.MaxRequestBytes = 1, 4096
	handler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	defer func() { _ = handler.Close() }()
	_, first, _, _ := newAdmissionPeer(t, handler)
	_, second, _, secondDone := newAdmissionPeer(t, handler)
	frame := admissionHelpFrame(t)
	require.NoError(t, first.WriteFrame(frame))
	budget := handler.RequestBudget(false)
	require.Eventually(t, func() bool { return budget.Snapshot().Frames == 1 }, time.Second, time.Millisecond)
	require.Error(t, second.WriteFrame(frame))
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("rejected request connection remained open")
	}
	require.Equal(t, 1, budget.Snapshot().Frames)
	require.Equal(t, uint64(1), budget.Snapshot().Rejected)
	internalRelease, err := handler.RequestBudget(true).Reserve(wire.Frame{}, 1, 1)
	require.NoError(t, err)
	internalRelease()
	response, err := first.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, wire.StatusOK, response.Status)
	require.Eventually(t, func() bool { return budget.Snapshot().Frames == 0 }, time.Second, time.Millisecond)
	require.NoError(t, first.WriteFrame(frame))
	_, err = first.ReadFrame()
	require.NoError(t, err)
}

func TestRequestCancellationReleasesPartialPayload(t *testing.T) {
	handler := controller.NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	defer func() { _ = handler.Close() }()
	client, _, cancel, done := newAdmissionPeer(t, handler)
	codec, err := wire.NewCodec(wire.CompressionNone)
	require.NoError(t, err)
	encoded, err := codec.Encode(admissionHelpFrame(t))
	require.NoError(t, err)
	_, err = client.Write(encoded[:wire.HeaderSize])
	require.NoError(t, err)
	budget := handler.RequestBudget(false)
	require.Eventually(t, func() bool { return budget.Snapshot().Frames == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("partial frame read did not cancel")
	}
	require.Zero(t, budget.Snapshot().Frames)
	require.Zero(t, budget.Snapshot().Bytes)
}

func TestMalformedCommandReleasesReservation(t *testing.T) {
	handler := controller.NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	defer func() { _ = handler.Close() }()
	_, connection, _, done := newAdmissionPeer(t, handler)
	frame := admissionHelpFrame(t)
	frame.Payload = []byte{0xff}
	require.NoError(t, connection.WriteFrame(frame))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("malformed request connection remained open")
	}
	require.Zero(t, handler.RequestBudget(false).Snapshot().Frames)
	require.Zero(t, handler.RequestBudget(false).Snapshot().Bytes)
}

func TestRequestCancellationReleasesBlockedResponse(t *testing.T) {
	handler := controller.NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	defer func() { _ = handler.Close() }()
	_, connection, cancel, done := newAdmissionPeer(t, handler)
	require.NoError(t, connection.WriteFrame(admissionHelpFrame(t)))
	budget := handler.RequestBudget(false)
	require.Eventually(t, func() bool { return budget.Snapshot().Frames == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked response write did not cancel")
	}
	require.Zero(t, budget.Snapshot().Frames)
	require.Zero(t, budget.Snapshot().Bytes)
}

func TestResponseCorrelationDoesNotRetainRequestPayload(t *testing.T) {
	response := &serverWireConn{}
	response.setRequest(wire.Frame{Command: wire.CommandHelp, RequestID: 9, Payload: []byte("large request")})
	require.Equal(t, uint64(9), response.request.RequestID)
	require.Equal(t, wire.CommandHelp, response.request.Command)
	require.Nil(t, response.request.Payload)
}
