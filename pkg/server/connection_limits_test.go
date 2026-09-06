package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/stretchr/testify/require"
)

func TestIdleConnectionSurvivesReadDeadlinePoll(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ClientIdleTimeoutMS = 20000
	handler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	defer func() { _ = handler.Close() }()
	raw, connection, _, done := newAdmissionPeer(t, handler)
	select {
	case <-done:
		t.Fatal("connection closed before its idle deadline")
	case <-time.After(readDeadlinePoll + 100*time.Millisecond):
	}
	require.NoError(t, raw.SetDeadline(time.Now().Add(time.Second)))
	require.NoError(t, connection.WriteFrame(admissionHelpFrame(t)))
	_, err := connection.ReadFrame()
	require.NoError(t, err)
}

func TestHandleConnWithContextClosesIdleConnection(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ClientIdleTimeoutMS = 25
	handler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	defer func() { _ = handler.Close() }()

	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnWithContext(context.Background(), serverConn, handler, controller.NewClientContext("default-group", 0))
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle connection was not closed")
	}
}

func TestConnectionLimitDefaults(t *testing.T) {
	if got := maxClientConnections(nil); got != defaultMaxWorkers {
		t.Fatalf("default max connections = %d, want %d", got, defaultMaxWorkers)
	}
	cfg := &config.Config{MaxClientConnections: 7, ClientIdleTimeoutMS: 123}
	if got := maxClientConnections(cfg); got != 7 {
		t.Fatalf("configured max connections = %d, want 7", got)
	}
	if got := clientIdleTimeout(cfg); got != 123*time.Millisecond {
		t.Fatalf("configured idle timeout = %s, want 123ms", got)
	}
}

func TestConnectionLimiterCapsAcceptedAndActiveConnections(t *testing.T) {
	limiter := newConnectionLimiter(2)
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelBlocked()
	if err := limiter.Acquire(blockedCtx); err == nil {
		t.Fatal("third connection acquired a slot above the configured limit")
	}

	limiter.Release()
	availableCtx, cancelAvailable := context.WithTimeout(context.Background(), time.Second)
	defer cancelAvailable()
	if err := limiter.Acquire(availableCtx); err != nil {
		t.Fatalf("connection slot was not reusable after release: %v", err)
	}
	limiter.Release()
	limiter.Release()
}

func TestLimitedConnectionHoldsSlotUntilSocketClose(t *testing.T) {
	limiter := newConnectionLimiter(1)
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	conn := newLimitedConnection(serverConn, limiter.Release)

	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelBlocked()
	if err := limiter.Acquire(blockedCtx); err == nil {
		t.Fatal("slot was released before the transferred socket closed")
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	availableCtx, cancelAvailable := context.WithTimeout(context.Background(), time.Second)
	defer cancelAvailable()
	if err := limiter.Acquire(availableCtx); err != nil {
		t.Fatalf("slot was not released when socket closed: %v", err)
	}
	limiter.Release()
}
