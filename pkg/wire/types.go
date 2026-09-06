package wire

import "fmt"

const (
	ProtocolVersion         uint16 = 2
	HeaderSize                     = 32
	MaxFramePayload                = 64 * 1024 * 1024
	MaxBatchRecords                = 100_000
	DefaultStreamPageEvents        = 256
	MaxStreamPageEvents            = 1024
)

type Kind uint8

const (
	KindNegotiationRequest Kind = iota + 1
	KindNegotiationResponse
	KindRequest
	KindResponse
	KindStream
)

func (k Kind) valid() bool {
	return k >= KindNegotiationRequest && k <= KindStream
}

func (k Kind) negotiation() bool {
	return k == KindNegotiationRequest || k == KindNegotiationResponse
}

type Status uint16

const (
	StatusNone Status = iota
	StatusOK
	StatusError
	StatusStreamEnd
)

func (s Status) valid() bool {
	return s >= StatusOK && s <= StatusStreamEnd
}

type Compression uint8

const (
	CompressionNone Compression = iota
	CompressionGZIP
	CompressionSnappy
	CompressionLZ4
)

func ParseCompression(value string) (Compression, error) {
	switch value {
	case "", "none":
		return CompressionNone, nil
	case "gzip":
		return CompressionGZIP, nil
	case "snappy":
		return CompressionSnappy, nil
	case "lz4":
		return CompressionLZ4, nil
	default:
		return CompressionNone, fmt.Errorf("unsupported compression type: %s", value)
	}
}

func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGZIP:
		return "gzip"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

func (c Compression) valid() bool {
	return c <= CompressionLZ4
}

type Frame struct {
	Version   uint16
	Kind      Kind
	Command   Command
	Status    Status
	RequestID uint64
	Payload   []byte
}

type NegotiationRequest struct {
	MinimumVersion uint16
	MaximumVersion uint16
	Compressions   []Compression
}

type NegotiationResponse struct {
	Version     uint16
	Compression Compression
}
