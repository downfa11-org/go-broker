package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestDialVerifiesTLSAddressWithoutMutatingConfig(t *testing.T) {
	fixture := httptest.NewTLSServer(nil)
	defer fixture.Close()
	roots := x509.NewCertPool()
	roots.AddCert(fixture.Certificate())
	for _, test := range []struct {
		name, serverName string
		roots            *x509.CertPool
		wantError        string
	}{
		{name: "infer address", roots: roots},
		{name: "preserve explicit name", serverName: "wrong.example", roots: roots, wantError: "certificate"},
		{name: "reject untrusted issuer", roots: x509.NewCertPool(), wantError: "unknown authority"},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := tls.Listen("tcp", "127.0.0.1:0", fixture.TLS.Clone())
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			done := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				_, err = wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
				done <- err
			}()
			config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: test.roots, ServerName: test.serverName}
			client, err := Dial(context.Background(), listener.Addr().String(), DialConfig{TLS: config, HandshakeTimeout: 2 * time.Second})
			if test.wantError == "" {
				require.NoError(t, err)
				require.NoError(t, client.Close())
				require.NoError(t, <-done)
			} else {
				require.ErrorContains(t, err, test.wantError)
				require.Nil(t, client)
				require.Error(t, <-done)
			}
			require.Equal(t, test.serverName, config.ServerName)
		})
	}
}

func TestDialEstablishesCanonicalWireConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	serverDone := make(chan error, 1)
	go func() {
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer func() { _ = raw.Close() }()
		server, handshakeErr := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
		if handshakeErr != nil {
			serverDone <- handshakeErr
			return
		}
		request, readErr := server.ReadFrame()
		if readErr != nil {
			serverDone <- readErr
			return
		}
		serverDone <- server.WriteFrame(wire.Frame{
			Kind:      wire.KindResponse,
			Command:   request.Command,
			Status:    wire.StatusOK,
			RequestID: request.RequestID,
			Payload:   []byte("OK commands=HELP"),
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := Dial(ctx, listener.Addr().String(), DialConfig{
		DialTimeout:      time.Second,
		HandshakeTimeout: time.Second,
		Compression:      "none",
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	require.NoError(t, client.Send([]byte("HELP")))
	response, err := client.Receive()
	require.NoError(t, err)
	require.Equal(t, "OK commands=HELP", string(response))
	require.NoError(t, <-serverDone)
}

func TestDialRejectsInvalidInputBeforeOpeningSocket(t *testing.T) {
	_, err := Dial(nil, "127.0.0.1:1", DialConfig{}) //nolint:staticcheck // intentionally exercises the nil-context guard
	require.ErrorContains(t, err, "context is nil")

	_, err = Dial(context.Background(), "", DialConfig{})
	require.ErrorContains(t, err, "address is empty")
}
