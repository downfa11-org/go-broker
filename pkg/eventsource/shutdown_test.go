package eventsource

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandlerCloseFencesAdmissionAndDrainsOperations(t *testing.T) {
	handler := NewHandler(nil)
	require.True(t, handler.beginOperation())
	done := make(chan error, 1)
	go func() { done <- handler.Close() }()
	require.Eventually(t, func() bool {
		handler.mu.RLock()
		defer handler.mu.RUnlock()
		return handler.closed
	}, time.Second, time.Millisecond)
	require.False(t, handler.beginOperation())
	select {
	case <-done:
		t.Fatal("close returned before active operation finished")
	default:
	}
	handler.wg.Done()
	require.NoError(t, <-done)
	_, response := handler.AppendStream("", AppendOptions{})
	require.Equal(t, "ERROR: handler_closed", response)
	_, response = handler.SaveSnapshot("", nil)
	require.Equal(t, "ERROR: handler_closed", response)
	require.Equal(t, "ERROR: handler_closed", handler.HandleReadSnapshot(""))
	require.Equal(t, "ERROR: handler_closed", handler.HandleStreamVersion(""))
	require.NoError(t, handler.Close())
}
