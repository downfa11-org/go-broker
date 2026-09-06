package sdk

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestProducerBatchAckRequiresExactIdentity(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	p := &Producer{config: cfg, client: mustNewProducerClient(cfg)}
	first := Message{ProducerID: p.client.ID, Epoch: p.client.Epoch, SeqNum: 11}
	last := first
	last.SeqNum = 13
	valid := AckResponse{Status: "OK", ProducerID: first.ProducerID, ProducerEpoch: first.Epoch, SeqStart: 11, SeqEnd: 13}
	for _, idempotent := range []bool{false, true} {
		cfg.EnableIdempotence = idempotent
		for _, field := range []string{"valid", "producer", "epoch", "start", "end", "missing", "partial", "status"} {
			t.Run(fmt.Sprintf("%t/%s", idempotent, field), func(t *testing.T) {
				ack := valid
				switch field {
				case "producer":
					ack.ProducerID = "other"
				case "epoch":
					ack.ProducerEpoch++
				case "start":
					ack.SeqStart--
				case "end":
					ack.SeqEnd--
				case "missing":
					ack = AckResponse{Status: "OK"}
				case "partial":
					ack.Status = "PARTIAL"
				case "status":
					ack.Status = ""
				}
				data, err := json.Marshal(ack)
				require.NoError(t, err)
				_, err = p.parseAckResponseForBatch(data, 0, first, last)
				if field == "valid" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			})
		}
	}
}

func TestProducerBatchRetryDiscardsUnusableConnection(t *testing.T) {
	for _, failure := range []string{"producer", "epoch", "sequence", "header", "empty-body", "body", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			cfg := NewDefaultPublisherConfig()
			cfg.BrokerAddrs = []string{listener.Addr().String()}
			cfg.MaxRetries = 1
			cfg.RetryBackoffMS = 1
			cfg.AckTimeoutMS = 100
			client := mustNewProducerClient(cfg)
			defer func() { _ = client.Close() }()
			p := &Producer{config: cfg, client: client, done: make(chan struct{})}
			first := Message{ProducerID: client.ID, Epoch: client.Epoch, SeqNum: 11, Payload: "one"}
			last := first
			last.SeqNum = 12
			payload, err := EncodeBatchMessages(cfg.Topic, 0, cfg.Acks, false, []Message{first, last})
			require.NoError(t, err)
			finished := make(chan error, 1)
			go func() {
				for attempt := 0; attempt < 2; attempt++ {
					conn, err := listener.Accept()
					if err != nil {
						finished <- err
						return
					}
					defer func() { _ = conn.Close() }()
					_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
					framed, request, _, err := acceptWireTestRequest(conn)
					if err != nil {
						finished <- err
						return
					}
					if string(request.Payload) != string(payload) {
						finished <- fmt.Errorf("retry changed batch")
						return
					}
					ack := AckResponse{Status: "OK", ProducerID: first.ProducerID, ProducerEpoch: first.Epoch, SeqStart: first.SeqNum, SeqEnd: last.SeqNum}
					if attempt == 0 {
						switch failure {
						case "producer":
							ack.ProducerID = "old"
						case "epoch":
							ack.ProducerEpoch--
						case "sequence":
							ack.SeqEnd--
						}
					}
					data, err := json.Marshal(ack)
					if err != nil {
						finished <- err
						return
					}
					if attempt == 0 && (failure == "header" || failure == "empty-body" || failure == "body" || failure == "timeout") {
						codec, _ := wire.NewCodec(wire.CompressionNone)
						encoded, encodeErr := codec.Encode(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: data})
						if encodeErr != nil {
							finished <- encodeErr
							return
						}
						count := 0
						switch failure {
						case "header":
							count = 1
						case "empty-body":
							count = wire.HeaderSize
						case "body":
							count = wire.HeaderSize + 1
						}
						if count > 0 {
							_, err = conn.Write(encoded[:count])
						}
					} else {
						err = writeWireTestResponse(framed, request, string(data))
					}
					if err != nil {
						finished <- err
						return
					}
					if attempt == 0 {
						_, err = conn.Read(make([]byte, 1))
						if err != io.EOF {
							finished <- fmt.Errorf("old connection was reused or not closed: %v", err)
							return
						}
					}
				}
				finished <- nil
			}()
			_, err = p.sendWithRetryForBatch(payload, 0, first, last)
			require.NoError(t, err)
			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-time.After(4 * time.Second):
				t.Fatal("broker retry did not finish")
			}
		})
	}
}

func TestProducerFlushAndCloseReportPermanentDeliveryFailure(t *testing.T) {
	p, result := newProducerDrainTestHarness(t, "ERROR: NOT_AUTHORIZED_FOR_TOPIC class=authorization retryable=false")
	p.config.MaxRetries = 3
	_, err := p.Send("rejected")
	require.NoError(t, err)
	require.ErrorContains(t, p.Flush(), "NOT_AUTHORIZED_FOR_TOPIC")
	require.ErrorContains(t, p.Close(), "NOT_AUTHORIZED_FOR_TOPIC")
	require.Zero(t, p.GetUniqueAckCount())
	require.NoError(t, (<-result).err)
}

func TestProducerReconnectFailureRemovesOldConnection(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	client := mustNewProducerClient(cfg)
	cfg.BrokerAddrs = nil
	server, conn := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	connections := []net.Conn{conn}
	client.conns.Store(&connections)
	require.Error(t, client.ReconnectPartition(0, ""))
	require.Nil(t, client.GetConn(0))
	_, err := server.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}
