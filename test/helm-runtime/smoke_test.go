package helm_runtime_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

func TestHelmSDKPersistence(t *testing.T) {
	phase := os.Getenv("HELM_TEST_PHASE")
	if phase == "" {
		t.Skip("run in the isolated Helm runtime fixture")
	}
	require.Contains(t, []string{"seed", "restored"}, phase)
	address := os.Getenv("HELM_BROKER_ADDRESS")
	require.NotEmpty(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := sdk.NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{address}
	cfg.Topic, cfg.Partitions = "helm-persistence", 3
	cfg.AutoCreateTopics = phase == "seed"
	cfg.EnableIdempotence, cfg.Acks = true, "all"
	cfg.BatchSize, cfg.LingerMS = 10, 10
	cfg.UseTLS = true
	cfg.TLSCertPath, cfg.TLSKeyPath = "/tls/tls.crt", "/tls/tls.key"
	cfg.Principal, cfg.AuthToken = "runtime-test", os.Getenv("HELM_AUTH_TOKEN")
	producer, err := sdk.NewProducerWithContext(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, producer.Close()) })
	first, total := 0, 60
	if phase == "restored" {
		first, total = 60, 90
	}
	for i := first; i < total; i++ {
		_, err = producer.Send(fmt.Sprintf("helm-record-%03d", i))
		require.NoError(t, err)
	}
	require.NoError(t, producer.Flush())
	require.NoError(t, producer.Close())

	consumerCfg := sdk.NewDefaultConsumerConfig()
	consumerCfg.BrokerAddrs = cfg.BrokerAddrs
	consumerCfg.Topic = cfg.Topic
	consumerCfg.GroupID = "helm-" + phase
	consumerCfg.UseTLS = true
	consumerCfg.TLSCertPath, consumerCfg.TLSKeyPath = cfg.TLSCertPath, cfg.TLSKeyPath
	consumerCfg.Principal, consumerCfg.AuthToken = cfg.Principal, cfg.AuthToken
	consumerCfg.Mode, consumerCfg.EnableAutoCommit = sdk.ModePolling, false
	consumerCfg.PollTimeoutMS = 1000
	consumer, err := sdk.NewConsumerWithContext(ctx, consumerCfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, consumer.Close()) })
	messages := make(chan sdk.Message, 256)
	finished := make(chan error, 1)
	go func() {
		finished <- consumer.Start(func(message sdk.Message) error {
			select {
			case messages <- message:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	seen := make(map[string]bool)
	partitions := make(map[int]int)
	quiet := time.NewTimer(2 * time.Minute)
	defer quiet.Stop()
	for {
		select {
		case message := <-messages:
			require.False(t, seen[message.Payload], "duplicate payload %q", message.Payload)
			seen[message.Payload] = true
			partitions[message.Partition]++
			require.LessOrEqual(t, len(seen), total)
			if len(seen) == total {
				quiet.Reset(2 * time.Second)
			}
		case err := <-finished:
			t.Fatalf("consumer exited before verification: %v", err)
		case <-ctx.Done():
			t.Fatalf("received %d/%d records: %v", len(seen), total, ctx.Err())
		case <-quiet.C:
			require.Len(t, seen, total)
			for i := 0; i < total; i++ {
				require.True(t, seen[fmt.Sprintf("helm-record-%03d", i)])
			}
			require.Len(t, partitions, 3)
			require.NoError(t, consumer.Close())
			require.NoError(t, <-finished)
			t.Logf("%s: verified %d distinct records across %d partitions", phase, total, len(partitions))
			return
		}
	}
}
