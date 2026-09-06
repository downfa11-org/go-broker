package server

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
)

var brokerCompressions = []wire.Compression{
	wire.CompressionNone,
	wire.CompressionGZIP,
	wire.CompressionSnappy,
	wire.CompressionLZ4,
}

// serverWireConn exposes connection lifecycle operations to handlers and sends
// every handler payload as a correlated Wire v2 response or stream frame.
type serverWireConn struct {
	net.Conn
	connection *wire.Connection

	mu      sync.Mutex
	request wire.Frame
}

func newServerWireConn(conn net.Conn, connection *wire.Connection) *serverWireConn {
	return &serverWireConn{Conn: conn, connection: connection}
}

func (c *serverWireConn) setRequest(request wire.Frame) {
	c.mu.Lock()
	request.Payload = nil
	c.request = request
	c.mu.Unlock()
}

func (c *serverWireConn) WritePayload(payload []byte) error {
	if c == nil || c.connection == nil {
		return fmt.Errorf("server Wire v2 connection is not initialized")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeMessage(payload)
}

func (c *serverWireConn) writeMessage(payload []byte) error {
	status := wire.StatusOK
	if parsed, ok := wireprotocol.ParseErrorResponse(string(payload)); ok {
		status = wire.StatusError
		class, err := wire.ParseErrorClass(string(parsed.Class))
		if err != nil {
			return err
		}
		fields := make(map[string]string, len(parsed.Fields))
		for key, value := range parsed.Fields {
			if key != "class" && key != "retryable" {
				fields[key] = value
			}
		}
		payload, err = wire.EncodeError(wire.ErrorPayload{
			Code: parsed.Code, Class: class, Retryable: parsed.Retryable,
			Message: joinErrorDetails(parsed.Details), Fields: fields,
		})
		if err != nil {
			return err
		}
	}
	kind := wire.KindResponse
	if c.request.Command == wire.CommandStream {
		kind = wire.KindStream
		if isStreamClosePayload(payload) {
			status = wire.StatusStreamEnd
		}
	}
	return c.connection.WriteFrame(wire.Frame{
		Kind: kind, Command: c.request.Command, Status: status, RequestID: c.request.RequestID, Payload: payload,
	})
}

func joinErrorDetails(details []string) string {
	return strings.Join(details, " ")
}

func isStreamClosePayload(payload []byte) bool {
	fields := strings.Fields(string(payload))
	if len(fields) == 0 || !strings.EqualFold(fields[0], "STREAM_CONTROL") {
		return false
	}
	for _, field := range fields[1:] {
		key, value, ok := strings.Cut(field, "=")
		if ok && strings.EqualFold(key, "type") && strings.EqualFold(value, "close") {
			return true
		}
	}
	return false
}

func negotiateServerConnection(conn net.Conn) (*wire.Connection, *serverWireConn, error) {
	return negotiateServerConnectionWithAdmission(conn, nil)
}

func negotiateServerConnectionWithAdmission(conn net.Conn, admit wire.FrameAdmission) (*wire.Connection, *serverWireConn, error) {
	connection, err := wire.ServerHandshakeWithAdmission(conn, brokerCompressions, admit)
	if err != nil {
		return nil, nil, err
	}
	return connection, newServerWireConn(conn, connection), nil
}

func readWireRequest(connection *wire.Connection) (wire.Frame, error) {
	frame, release, err := readWireRequestWithAdmission(connection, nil)
	if release != nil {
		release()
	}
	return frame, err
}

func readWireRequestWithAdmission(connection *wire.Connection, admit wire.FrameAdmission) (wire.Frame, func(), error) {
	frame, release, err := connection.ReadFrameWithAdmission(func(frame wire.Frame, encoded, decoded uint32) (func(), error) {
		if frame.Kind != wire.KindRequest || frame.Status != wire.StatusNone || frame.RequestID == 0 {
			return nil, fmt.Errorf("invalid Wire v2 request frame")
		}
		if admit != nil {
			return admit(frame, encoded, decoded)
		}
		return nil, nil
	})
	if err != nil {
		return wire.Frame{}, nil, err
	}
	accepted := false
	defer func() {
		if !accepted {
			release()
		}
	}()
	if wire.IsBatch(frame.Payload) {
		if frame.Command != wire.CommandPublish {
			return wire.Frame{}, nil, fmt.Errorf("wire v2 batch requires PUBLISH command")
		}
		accepted = true
		return frame, release, nil
	}
	payload, err := wire.DecodeCommandPayload(frame.Payload)
	if err != nil {
		return wire.Frame{}, nil, fmt.Errorf("decode %s request: %w", frame.Command, err)
	}
	command, err := wire.RenderCommand(frame.Command, payload)
	if err != nil {
		return wire.Frame{}, nil, err
	}
	frame.Payload = []byte(command)
	accepted = true
	return frame, release, nil
}

func (c *serverWireConn) SetDeadline(deadline time.Time) error {
	return c.Conn.SetDeadline(deadline)
}
