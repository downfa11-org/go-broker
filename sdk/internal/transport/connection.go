package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

type Conn = wire.ClientConn

type DialConfig struct {
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
	Compression      string
	TLS              *tls.Config
}

func NewClient(conn net.Conn, compression string) (*Conn, error) {
	return wire.NewClientConn(conn, compression)
}

func Authenticate(conn *Conn, principal, token string) error {
	if principal == "" && token == "" {
		return nil
	}
	if conn == nil {
		return fmt.Errorf("authentication connection is nil")
	}
	if principal == "" || token == "" || strings.ContainsAny(principal, " \t\r\n") || strings.ContainsAny(token, " \t\r\n") {
		return fmt.Errorf("invalid authentication credentials")
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set authentication deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if err := conn.Send([]byte(fmt.Sprintf("AUTH principal=%s token=%s", principal, token))); err != nil {
		return fmt.Errorf("send authentication command: %w", err)
	}
	response, err := conn.Receive()
	if err != nil {
		return err
	}
	fields := strings.Fields(strings.TrimSpace(string(response)))
	if len(fields) == 0 || fields[0] != "OK" {
		return fmt.Errorf("unexpected authentication response: %s", response)
	}
	return nil
}

// Dial is the single SDK socket/TLS/Wire-v2 establishment path.
func Dial(ctx context.Context, addr string, config DialConfig) (*Conn, error) {
	if ctx == nil {
		return nil, fmt.Errorf("transport context is nil")
	}
	if addr == "" {
		return nil, fmt.Errorf("broker address is empty")
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = 5 * time.Second
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = 5 * time.Second
	}
	dialer := &net.Dialer{Timeout: config.DialTimeout, KeepAlive: 30 * time.Second}
	raw, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	closeOnError := func(err error) (*Conn, error) {
		_ = raw.Close()
		return nil, err
	}
	if tcpConn, ok := raw.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		_ = tcpConn.SetReadBuffer(2 * 1024 * 1024)
		_ = tcpConn.SetWriteBuffer(2 * 1024 * 1024)
	}
	conn := raw
	if config.TLS != nil {
		tlsConfig := config.TLS.Clone()
		if tlsConfig.ServerName == "" {
			host, _, splitErr := net.SplitHostPort(addr)
			if splitErr != nil {
				return closeOnError(fmt.Errorf("TLS address %s: %w", addr, splitErr))
			}
			tlsConfig.ServerName = host
		}
		tlsConn := tls.Client(raw, tlsConfig)
		handshakeCtx, cancel := context.WithTimeout(ctx, config.HandshakeTimeout)
		err = tlsConn.HandshakeContext(handshakeCtx)
		cancel()
		if err != nil {
			return closeOnError(fmt.Errorf("TLS handshake with %s: %w", addr, err))
		}
		conn = tlsConn
	}
	deadline := time.Now().Add(config.HandshakeTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return closeOnError(fmt.Errorf("set Wire v2 handshake deadline: %w", err))
	}
	framed, err := NewClient(conn, config.Compression)
	if err != nil {
		return closeOnError(fmt.Errorf("wire v2 handshake with %s: %w", addr, err))
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return closeOnError(fmt.Errorf("clear Wire v2 handshake deadline: %w", err))
	}
	return framed, nil
}
