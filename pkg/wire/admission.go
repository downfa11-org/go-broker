package wire

import (
	"errors"
	"fmt"
	"sync"
)

var ErrAdmissionLimit = errors.New("wire request admission limit exceeded")

// FrameAdmission reserves capacity after header validation and before payload
// allocation. A successful read transfers the release function to its caller.
type FrameAdmission func(frame Frame, encodedBytes, decodedBytes uint32) (release func(), err error)

type FrameBudget struct {
	mu        sync.Mutex
	maxFrames int
	maxBytes  int64
	frames    int
	bytes     int64
	rejected  uint64
}

type FrameBudgetSnapshot struct {
	Frames, MaxFrames int
	Bytes, MaxBytes   int64
	Rejected          uint64
}

func NewFrameBudget(maxFrames int, maxBytes int64) *FrameBudget {
	return &FrameBudget{maxFrames: maxFrames, maxBytes: maxBytes}
}

func (b *FrameBudget) Reserve(_ Frame, encodedBytes, decodedBytes uint32) (func(), error) {
	if b == nil {
		return nil, ErrAdmissionLimit
	}
	size := int64(HeaderSize) + int64(encodedBytes) + int64(decodedBytes)
	b.mu.Lock()
	if b.frames >= b.maxFrames || size > b.maxBytes-b.bytes {
		b.rejected++
		b.mu.Unlock()
		return nil, fmt.Errorf("%w: requested bytes=%d", ErrAdmissionLimit, size)
	}
	b.frames++
	b.bytes += size
	b.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.frames--
			b.bytes -= size
			b.mu.Unlock()
		})
	}, nil
}

func (b *FrameBudget) Snapshot() FrameBudgetSnapshot {
	if b == nil {
		return FrameBudgetSnapshot{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return FrameBudgetSnapshot{Frames: b.frames, MaxFrames: b.maxFrames, Bytes: b.bytes, MaxBytes: b.maxBytes, Rejected: b.rejected}
}
