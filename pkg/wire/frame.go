package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
)

const (
	frameMagic uint32 = 0x43525332 // CRS2

	flagCompressionExplicit uint8 = 1 << 0
	flagCompressionShift          = 1
	flagCompressionMask     uint8 = 0x0e
	validFrameFlags               = flagCompressionExplicit | flagCompressionMask
)

var (
	ErrInvalidFrame        = errors.New("invalid Wire v2 frame")
	ErrFrameTooLarge       = errors.New("wire v2 frame exceeds size limit")
	ErrChecksumMismatch    = errors.New("wire v2 checksum mismatch")
	ErrCompressionMismatch = errors.New("wire v2 compression mismatch")
	crc32cTable            = crc32.MakeTable(crc32.Castagnoli)
)

type Codec struct {
	compression Compression
}

func NewCodec(compression Compression) (*Codec, error) {
	if !compression.valid() {
		return nil, fmt.Errorf("%w: unsupported compression %d", ErrCompressionMismatch, compression)
	}
	return &Codec{compression: compression}, nil
}

func (c *Codec) Compression() Compression {
	if c == nil {
		return CompressionNone
	}
	return c.compression
}

func (c *Codec) Encode(frame Frame) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: nil codec", ErrInvalidFrame)
	}
	version := frame.Version
	if version == 0 {
		version = ProtocolVersion
	}
	if version != ProtocolVersion || !frame.Kind.valid() {
		return nil, fmt.Errorf("%w: version=%d kind=%d", ErrInvalidFrame, version, frame.Kind)
	}
	if err := validateFrameSemantics(frame); err != nil {
		return nil, err
	}
	if len(frame.Payload) > MaxFramePayload {
		return nil, fmt.Errorf("%w: decoded=%d maximum=%d", ErrFrameTooLarge, len(frame.Payload), MaxFramePayload)
	}

	compression := c.compression
	flags := flagCompressionExplicit | uint8(compression)<<flagCompressionShift
	if frame.Kind.negotiation() {
		compression = CompressionNone
		flags = 0
	}
	encoded, err := Compress(frame.Payload, compression)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxFramePayload {
		return nil, fmt.Errorf("%w: encoded=%d maximum=%d", ErrFrameTooLarge, len(encoded), MaxFramePayload)
	}

	result := make([]byte, HeaderSize+len(encoded))
	header := result[:HeaderSize]
	binary.BigEndian.PutUint32(header[0:4], frameMagic)
	binary.BigEndian.PutUint16(header[4:6], version)
	header[6] = byte(frame.Kind)
	header[7] = flags
	binary.BigEndian.PutUint16(header[8:10], uint16(frame.Command))
	binary.BigEndian.PutUint16(header[10:12], uint16(frame.Status))
	binary.BigEndian.PutUint64(header[12:20], frame.RequestID)
	// #nosec G115 -- both lengths are checked against MaxFramePayload above.
	binary.BigEndian.PutUint32(header[20:24], uint32(len(encoded)))
	// #nosec G115 -- both lengths are checked against MaxFramePayload above.
	binary.BigEndian.PutUint32(header[24:28], uint32(len(frame.Payload)))
	binary.BigEndian.PutUint32(header[28:32], crc32.Checksum(encoded, crc32cTable))
	copy(result[HeaderSize:], encoded)
	return result, nil
}

func (c *Codec) Decode(encodedFrame []byte) (Frame, error) {
	if len(encodedFrame) < HeaderSize {
		err := fmt.Errorf("%w: header is %d bytes", ErrInvalidFrame, len(encodedFrame))
		recordProtocolFailure(err)
		return Frame{}, err
	}
	header, err := c.decodeHeader(encodedFrame[:HeaderSize])
	if err != nil {
		recordProtocolFailure(err)
		return Frame{}, err
	}
	if len(encodedFrame)-HeaderSize != int(header.encodedSize) {
		err = fmt.Errorf("%w: encoded length=%d actual=%d", ErrInvalidFrame, header.encodedSize, len(encodedFrame)-HeaderSize)
		recordProtocolFailure(err)
		return Frame{}, err
	}
	frame, err := c.decodePayload(header, encodedFrame[HeaderSize:])
	if err != nil {
		recordProtocolFailure(err)
	}
	return frame, err
}

func (c *Codec) WriteFrame(writer io.Writer, frame Frame) error {
	encoded, err := c.Encode(frame)
	if err != nil {
		return err
	}
	if err := writeAll(writer, encoded); err != nil {
		return fmt.Errorf("write Wire v2 frame: %w", err)
	}
	return nil
}

func (c *Codec) ReadFrame(reader io.Reader) (Frame, error) {
	frame, release, err := c.ReadFrameWithAdmission(reader, nil)
	if release != nil {
		release()
	}
	return frame, err
}

func (c *Codec) ReadFrameWithAdmission(reader io.Reader, admit FrameAdmission) (Frame, func(), error) {
	if c == nil {
		return Frame{}, nil, fmt.Errorf("%w: nil codec", ErrInvalidFrame)
	}
	headerBytes := make([]byte, HeaderSize)
	if received, err := io.ReadFull(reader, headerBytes); err != nil {
		var timeout net.Error
		if received == 0 && errors.As(err, &timeout) && timeout.Timeout() {
			return Frame{}, nil, timeout
		}
		return Frame{}, nil, fmt.Errorf("read Wire v2 header: %w", err)
	}
	header, err := c.decodeHeader(headerBytes)
	if err != nil {
		recordProtocolFailure(err)
		return Frame{}, nil, err
	}
	release := func() {}
	if admit != nil {
		reserved, admissionErr := admit(header.frame, header.encodedSize, header.decodedSize)
		if reserved != nil {
			release = reserved
		}
		if admissionErr != nil {
			release()
			return Frame{}, nil, admissionErr
		}
	}
	payload := make([]byte, header.encodedSize)
	if _, err := io.ReadFull(reader, payload); err != nil {
		release()
		return Frame{}, nil, fmt.Errorf("read Wire v2 payload: %w", err)
	}
	frame, err := c.decodePayload(header, payload)
	if err != nil {
		recordProtocolFailure(err)
		release()
		return Frame{}, nil, err
	}
	return frame, release, nil
}

type decodedHeader struct {
	frame       Frame
	compression Compression
	encodedSize uint32
	decodedSize uint32
	checksum    uint32
}

func (c *Codec) decodeHeader(header []byte) (decodedHeader, error) {
	if c == nil || len(header) != HeaderSize {
		return decodedHeader{}, fmt.Errorf("%w: invalid codec or header", ErrInvalidFrame)
	}
	if binary.BigEndian.Uint32(header[0:4]) != frameMagic {
		return decodedHeader{}, fmt.Errorf("%w: bad magic", ErrInvalidFrame)
	}
	version := binary.BigEndian.Uint16(header[4:6])
	kind := Kind(header[6])
	flags := header[7]
	if version != ProtocolVersion || !kind.valid() || flags&^validFrameFlags != 0 {
		return decodedHeader{}, fmt.Errorf("%w: version=%d kind=%d flags=%02x", ErrInvalidFrame, version, kind, flags)
	}

	compression := CompressionNone
	if kind.negotiation() {
		if flags != 0 {
			return decodedHeader{}, fmt.Errorf("%w: negotiation frame must be uncompressed", ErrCompressionMismatch)
		}
	} else {
		if flags&flagCompressionExplicit == 0 {
			return decodedHeader{}, fmt.Errorf("%w: compression flag is not explicit", ErrCompressionMismatch)
		}
		compression = Compression((flags & flagCompressionMask) >> flagCompressionShift)
		if !compression.valid() || compression != c.compression {
			return decodedHeader{}, fmt.Errorf("%w: negotiated=%s frame=%s", ErrCompressionMismatch, c.compression, compression)
		}
	}

	encodedSize := binary.BigEndian.Uint32(header[20:24])
	decodedSize := binary.BigEndian.Uint32(header[24:28])
	if encodedSize > MaxFramePayload || decodedSize > MaxFramePayload {
		return decodedHeader{}, fmt.Errorf("%w: encoded=%d decoded=%d maximum=%d", ErrFrameTooLarge, encodedSize, decodedSize, MaxFramePayload)
	}
	frame := Frame{
		Version: version, Kind: kind,
		Command:   Command(binary.BigEndian.Uint16(header[8:10])),
		Status:    Status(binary.BigEndian.Uint16(header[10:12])),
		RequestID: binary.BigEndian.Uint64(header[12:20]),
	}
	if err := validateFrameSemantics(frame); err != nil {
		return decodedHeader{}, err
	}
	return decodedHeader{
		frame: frame, compression: compression, encodedSize: encodedSize, decodedSize: decodedSize,
		checksum: binary.BigEndian.Uint32(header[28:32]),
	}, nil
}

func (c *Codec) decodePayload(header decodedHeader, encoded []byte) (Frame, error) {
	if crc32.Checksum(encoded, crc32cTable) != header.checksum {
		return Frame{}, ErrChecksumMismatch
	}
	payload, err := Decompress(encoded, header.compression, header.decodedSize)
	if err != nil {
		return Frame{}, err
	}
	header.frame.Payload = payload
	return header.frame, nil
}

func validateFrameSemantics(frame Frame) error {
	switch frame.Kind {
	case KindNegotiationRequest:
		if frame.Status != StatusNone {
			return fmt.Errorf("%w: request status must be zero", ErrInvalidFrame)
		}
	case KindNegotiationResponse, KindResponse:
		if frame.Status != StatusOK && frame.Status != StatusError {
			return fmt.Errorf("%w: response status must be OK or error", ErrInvalidFrame)
		}
		if frame.Kind == KindResponse && frame.RequestID == 0 {
			return fmt.Errorf("%w: response request id is required", ErrInvalidFrame)
		}
	case KindRequest:
		if frame.Status != StatusNone {
			return fmt.Errorf("%w: request status must be zero", ErrInvalidFrame)
		}
		if frame.RequestID == 0 {
			return fmt.Errorf("%w: request id is required", ErrInvalidFrame)
		}
	case KindStream:
		if !frame.Status.valid() {
			return fmt.Errorf("%w: invalid stream status %d", ErrInvalidFrame, frame.Status)
		}
		if frame.RequestID == 0 {
			return fmt.Errorf("%w: stream request id is required", ErrInvalidFrame)
		}
	}
	if frame.Kind.negotiation() && frame.Command != CommandNegotiate {
		return fmt.Errorf("%w: negotiation frame command=%s", ErrInvalidFrame, frame.Command)
	}
	if !frame.Command.valid() {
		return fmt.Errorf("%w: unknown command id %d", ErrInvalidFrame, frame.Command)
	}
	return nil
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
