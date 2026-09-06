package controller

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedTransactionWaitsForBrokerCapabilityRegistration(t *testing.T) {
	attempts := 0
	err := waitForPreparedTransactionTopology(func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("broker capability registration pending")
		}
		return nil
	}, time.Second)
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestPreparedTransactionTopologyWaitDoesNotBypassUnsupportedBroker(t *testing.T) {
	want := fmt.Errorf("broker protocol unsupported")
	err := waitForPreparedTransactionTopology(func() error { return want }, 30*time.Millisecond)
	require.ErrorIs(t, err, want)
}
