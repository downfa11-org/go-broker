package wire_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

type frameTimeoutReader struct{}

func (frameTimeoutReader) Read([]byte) (int, error) { return 0, context.DeadlineExceeded }

func TestFrameTimeoutDistinguishesIdleFromPartialRequest(t *testing.T) {
	codec, err := wire.NewCodec(wire.CompressionNone)
	require.NoError(t, err)
	encoded, err := codec.Encode(wire.Frame{Kind: wire.KindRequest, Command: wire.CommandHelp, RequestID: 1, Payload: []byte("hello")})
	require.NoError(t, err)
	for _, received := range []int{0, 1, wire.HeaderSize - 1, wire.HeaderSize, wire.HeaderSize + 1} {
		reader := io.MultiReader(bytes.NewReader(encoded[:received]), frameTimeoutReader{})
		_, release, err := codec.ReadFrameWithAdmission(reader, nil)
		require.Error(t, err)
		require.Nil(t, release)
		timeout, resumable := err.(net.Error)
		if received == 0 {
			require.True(t, resumable, "idle read must remain a resumable timeout")
			require.True(t, timeout.Timeout())
		} else {
			require.False(t, resumable, "partial request cannot be resumed as a fresh header")
		}
	}
}
