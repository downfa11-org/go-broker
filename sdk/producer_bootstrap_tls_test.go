package sdk

import (
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProducerAutoCreateStartsTLSBeforeWireHandshake(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	certificate := server.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	require.NoError(t, err)
	dir := t.TempDir()
	cfg := NewDefaultPublisherConfig()
	cfg.UseTLS, cfg.AutoCreateTopics = true, true
	cfg.TLSCertPath, cfg.TLSKeyPath = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(cfg.TLSCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600))
	require.NoError(t, os.WriteFile(cfg.TLSKeyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600))
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	require.NoError(t, listener.SetDeadline(time.Now().Add(5*time.Second)))
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	firstByte := make(chan byte, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			firstByte <- 0
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var header [1]byte
		_, _ = io.ReadFull(conn, header[:])
		firstByte <- header[0]
	}()
	producer, err := NewProducer(cfg)
	require.Error(t, err, "fixture closes before completing TLS")
	require.Nil(t, producer)
	require.Equal(t, byte(22), <-firstByte, "auto-create must send a TLS handshake, not plaintext Wire bytes")
}
