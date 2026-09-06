package wire

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrameBudgetExactBoundaryAndIdempotentRelease(t *testing.T) {
	budget := NewFrameBudget(2, 2*(HeaderSize+20))
	first, err := budget.Reserve(Frame{}, 10, 10)
	require.NoError(t, err)
	second, err := budget.Reserve(Frame{}, 10, 10)
	require.NoError(t, err)
	_, err = budget.Reserve(Frame{}, 0, 0)
	require.ErrorIs(t, err, ErrAdmissionLimit)
	first()
	first()
	require.Equal(t, 1, budget.Snapshot().Frames)
	second()
	require.Zero(t, budget.Snapshot().Bytes)
	_, err = budget.Reserve(Frame{}, 100, 100)
	require.ErrorIs(t, err, ErrAdmissionLimit)
	require.Equal(t, uint64(2), budget.Snapshot().Rejected)
	_, err = NewFrameBudget(0, 0).Reserve(Frame{}, 0, 0)
	require.ErrorIs(t, err, ErrAdmissionLimit)
}

func TestFrameBudgetConcurrentReservations(t *testing.T) {
	budget := NewFrameBudget(3, 3*HeaderSize)
	releaseAll := make(chan struct{})
	var attempts, finished sync.WaitGroup
	var accepted atomic.Int32
	attempts.Add(40)
	finished.Add(40)
	for i := 0; i < 40; i++ {
		go func() {
			defer finished.Done()
			release, err := budget.Reserve(Frame{}, 0, 0)
			if err == nil {
				accepted.Add(1)
			}
			attempts.Done()
			if err == nil {
				<-releaseAll
				release()
			}
		}()
	}
	attempts.Wait()
	require.Equal(t, int32(3), accepted.Load())
	close(releaseAll)
	finished.Wait()
	require.Zero(t, budget.Snapshot().Bytes)
	require.Zero(t, budget.Snapshot().Frames)
}

func TestFrameAdmissionRejectsBeforePayloadReadForEveryCompression(t *testing.T) {
	for _, compression := range []Compression{CompressionNone, CompressionGZIP, CompressionSnappy, CompressionLZ4} {
		t.Run(compression.String(), func(t *testing.T) {
			codec, err := NewCodec(compression)
			require.NoError(t, err)
			encoded, err := codec.Encode(Frame{Kind: KindRequest, Command: CommandPublish, RequestID: 1, Payload: bytes.Repeat([]byte("x"), 4096)})
			require.NoError(t, err)
			budget := NewFrameBudget(1, 1024)
			_, release, err := codec.ReadFrameWithAdmission(bytes.NewReader(encoded[:HeaderSize]), budget.Reserve)
			require.ErrorIs(t, err, ErrAdmissionLimit)
			require.Nil(t, release)
			require.Zero(t, budget.Snapshot().Bytes)
		})
	}
}

func TestFrameAdmissionReleasesReadAndDecodeFailures(t *testing.T) {
	codec, err := NewCodec(CompressionGZIP)
	require.NoError(t, err)
	encoded, err := codec.Encode(Frame{Kind: KindRequest, Command: CommandPublish, RequestID: 1, Payload: []byte("hello")})
	require.NoError(t, err)
	for _, failure := range []string{"partial", "checksum", "decoded length", "invalid header"} {
		t.Run(failure, func(t *testing.T) {
			data := append([]byte(nil), encoded...)
			switch failure {
			case "partial":
				data = data[:len(data)-1]
			case "checksum":
				data[len(data)-1] ^= 1
			case "decoded length":
				binary.BigEndian.PutUint32(data[24:28], 6)
			case "invalid header":
				data[0] ^= 1
			}
			budget := NewFrameBudget(1, 4096)
			_, release, err := codec.ReadFrameWithAdmission(bytes.NewReader(data), budget.Reserve)
			require.Error(t, err)
			require.Nil(t, release)
			require.Zero(t, budget.Snapshot().Frames)
			require.Zero(t, budget.Snapshot().Bytes)
			frame, release, err := codec.ReadFrameWithAdmission(bytes.NewReader(encoded), budget.Reserve)
			require.NoError(t, err)
			require.Equal(t, []byte("hello"), frame.Payload)
			require.Equal(t, 1, budget.Snapshot().Frames)
			release()
			require.Zero(t, budget.Snapshot().Frames)
		})
	}
}

func TestServerHandshakeRejectsOversizedHeaderWithoutBody(t *testing.T) {
	codec, err := NewCodec(CompressionNone)
	require.NoError(t, err)
	encoded, err := codec.Encode(Frame{Kind: KindNegotiationRequest, Command: CommandNegotiate, Payload: make([]byte, 1025)})
	require.NoError(t, err)
	server, client := net.Pipe()
	defer func() { _ = server.Close(); _ = client.Close() }()
	_ = server.SetDeadline(time.Now().Add(time.Second))
	done := make(chan error, 1)
	go func() { _, err := client.Write(encoded[:HeaderSize]); done <- err }()
	_, err = ServerHandshake(server, []Compression{CompressionNone})
	require.ErrorContains(t, err, "negotiation header or payload size")
	require.NoError(t, <-done)
}
