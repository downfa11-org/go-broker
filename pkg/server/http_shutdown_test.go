package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPShutdownReturnsDeadlineAndClosesConnections(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer server.Close()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, err := server.Client().Get(server.URL)
		if err == nil {
			_ = response.Body.Close()
		}
	}()
	<-started
	err := shutdownHTTPServer(server.Config)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown = %v, want deadline exceeded", err)
	}
	<-stopped
	<-requestDone
	if err := shutdownHTTPServer(nil); err != nil {
		t.Fatal(err)
	}
}
