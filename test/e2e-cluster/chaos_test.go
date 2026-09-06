package e2e_cluster

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
)

// TestChaosLeaderFailoverRetryAndOffsetDurability is intentionally opt-in because
// it restarts Docker cluster members and waits for multiple elections. Run with
// RUN_E2E_CHAOS=1 when validating broker failure semantics before release.
func TestChaosLeaderFailoverRetryAndOffsetDurability(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run long cluster chaos validation")
	}

	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("chaos-eos-test").
		WithPartitions(1).
		WithNumMessages(100).
		WithIdempotent(true).
		WithAcks("all").
		WithMinInSyncReplicas(2)
	defer ctx.Cleanup()

	leaderNode, actions := ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		CommitOffset(0, 50).
		SimulateLeaderFailure()

	actions.RetryPublishMessages().
		RecoverFollower(leaderNode)
	actions.Then().
		Expect(MessagesPublishedWithQuorum()).
		And(ExpectDataConsistent()).
		And(ExpectOffsetMatched(0, 50)).
		And(ExpectPartitionWatermarks(0, 100, 100)).
		And(e2e.PublisherRetriedSuccessfully())

	ctx.WithConsumerGroup("chaos-readback-group")
	ctx.WhenCluster().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(e2e.MessagesConsumed(100)).
		And(e2e.NoDuplicateMessages())
}

func TestChaosTransactionCoordinatorRestartPreservesAtomicVisibility(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run long cluster chaos validation")
	}

	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("chaos-transaction-recovery").
		WithPartitions(2).
		WithAcks("all").
		WithMinInSyncReplicas(2)
	defer ctx.Cleanup()

	actions := ctx.WhenCluster().StartCluster().CreateTopic().WaitForTopicMetadata()
	topic := ctx.GetTopic()
	txnID := "coordinator-restart-transaction"
	offsetGroup := "transaction-offset-group"
	visibilityGroup := "transaction-visibility-group"

	offsetClient := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer offsetClient.Close()
	generation, memberID, err := offsetClient.JoinGroup(topic, offsetGroup)
	if err != nil {
		t.Fatalf("join offset group: %v", err)
	}
	if _, err := offsetClient.SyncGroup(topic, offsetGroup, generation, memberID); err != nil {
		t.Fatalf("sync offset group: %v", err)
	}

	txnClient := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	producer, err := txnClient.InitTransactionProducer(txnID)
	if err != nil {
		t.Fatalf("initialize transaction producer: %v", err)
	}
	if err := txnClient.BeginTransaction(txnID, producer); err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	for partition := 0; partition < 2; partition++ {
		payload := fmt.Sprintf("transaction-partition-%d", partition)
		if err := txnClient.TransactionalPublish(txnID, topic, partition, producer, 1, payload); err != nil {
			t.Fatalf("stage partition %d: %v", partition, err)
		}
	}
	if err := txnClient.SendOffsetsToTransaction(txnID, topic, offsetGroup, memberID, generation, producer, map[int]uint64{0: 1, 1: 1}); err != nil {
		t.Fatalf("stage offsets: %v", err)
	}

	visibilityClient, visibilityGeneration, visibilityMember := joinClusterGroup(t, ctx.GetBrokerAddrs(), topic, visibilityGroup)
	defer visibilityClient.Close()
	for partition := 0; partition < 2; partition++ {
		messages := consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), topic, partition, visibilityGroup, visibilityMember, visibilityGeneration)
		if len(messages) != 0 {
			t.Fatalf("open transaction became visible on partition %d: %v", partition, messages)
		}
	}

	coordinatorID, err := txnClient.FindTransactionCoordinator(txnID)
	if err != nil {
		t.Fatalf("find transaction coordinator: %v", err)
	}
	coordinatorParts := strings.Split(strings.TrimPrefix(coordinatorID, "broker-"), "-")
	coordinatorNode, err := strconv.Atoi(coordinatorParts[0])
	if err != nil || coordinatorNode < 1 || coordinatorNode > 3 {
		t.Fatalf("unexpected transaction coordinator %q", coordinatorID)
	}
	txnClient.Close()
	actions.KillBroker(coordinatorNode)
	actions.StartBroker(coordinatorNode)
	requireClusterLifecycleProtocolEventually(t, ctx.GetBrokerAddrs(), fsm.PreparedTransactionProtocolVersion)

	recoveryClient := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer recoveryClient.Close()
	var status e2e.TransactionStatus
	if err := eventually(t, "transaction coordinator recovery", clusterReadyTimeout, func() (bool, string, error) {
		current, statusErr := recoveryClient.GetTransactionStatus(txnID)
		if statusErr != nil {
			return false, "TXN_STATUS failed", statusErr
		}
		status = current
		return status.State == "open" && status.Messages == 2 && status.Offsets == 2, fmt.Sprintf("state=%s messages=%d offsets=%d", status.State, status.Messages, status.Offsets), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := recoveryClient.EndTransaction(txnID, producer, "commit"); err != nil {
		t.Fatalf("commit recovered transaction: %v", err)
	}
	if err := recoveryClient.EndTransaction(txnID, producer, "commit"); err != nil {
		t.Fatalf("retry committed transaction: %v", err)
	}

	for partition := 0; partition < 2; partition++ {
		messages := consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), topic, partition, visibilityGroup, visibilityMember, visibilityGeneration)
		expected := fmt.Sprintf("transaction-partition-%d", partition)
		if len(messages) != 1 || messages[0] != expected {
			t.Fatalf("partition %d expected only %q after commit, got %v", partition, expected, messages)
		}
		offset, err := offsetClient.FetchCommittedOffset(topic, partition, offsetGroup)
		if err != nil {
			t.Fatalf("fetch committed offset for partition %d: %v", partition, err)
		}
		if offset != 1 {
			t.Fatalf("partition %d expected committed offset 1, got %d", partition, offset)
		}
	}
}

func joinClusterGroup(t *testing.T, addrs []string, topic, group string) (*e2e.BrokerClient, int, string) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	generation, memberID, err := client.JoinGroup(topic, group)
	if err != nil {
		client.Close()
		t.Fatalf("join group %s: %v", group, err)
	}
	if _, err := client.SyncGroup(topic, group, generation, memberID); err != nil {
		client.Close()
		t.Fatalf("sync group %s: %v", group, err)
	}
	return client, generation, memberID
}

func consumeFromPartitionLeader(t *testing.T, addrs []string, topic string, partition int, group, member string, generation int) []string {
	t.Helper()
	var lastErr error
	for _, addr := range addrs {
		client := e2e.NewBrokerClient([]string{addr})
		messages, err := client.ConsumeMessages(topic, partition, group, member, generation, 2*time.Second)
		client.Close()
		if err == nil {
			return messages
		}
		lastErr = err
		var brokerErr *wire.BrokerError
		if !errors.As(err, &brokerErr) || (brokerErr.Code != "NOT_LEADER" && brokerErr.Code != "NOT_PARTITION_LEADER") {
			break
		}
	}
	t.Fatalf("consume partition %d: %v", partition, lastErr)
	return nil
}
