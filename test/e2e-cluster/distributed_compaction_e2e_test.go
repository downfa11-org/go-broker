package e2e_cluster

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestDistributedCompactionPreservesReplicaAndConsumerOffsets(t *testing.T) {
	if os.Getenv("RUN_E2E_DISTRIBUTED_COMPACTION") != "1" {
		t.Skip("set RUN_E2E_DISTRIBUTED_COMPACTION=1 to run distributed compaction validation")
	}

	ctx := GivenCompactionClusterRestart(t).
		WithClusterSize(3).
		WithTopic("distributed-compaction-state").
		WithPartitions(1).
		WithAcks("all")
	actions := ctx.WhenCluster().StartCluster()
	requireClusterLifecycleProtocolEventually(t, ctx.GetBrokerAddrs(), fsm.DistributedCompactionProtocolVersion)

	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	createResponse, err := client.SendCommand("admin", fmt.Sprintf(
		"CREATE topic=%s partitions=1 replication_factor=3 cleanup_policy=compact",
		ctx.GetTopic(),
	), 5*time.Second)
	require.NoError(t, err)
	require.True(t, successfulBrokerResponse(createResponse), createResponse)
	actions.WaitForTopicMetadata()
	waitForFullISRAndZeroUnderReplicated(t, ctx, "compact topic creation")

	group := "distributed-compaction-reader"
	groupClient, generation, member := joinClusterGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group)
	defer groupClient.Close()
	require.NoError(t, groupClient.CommitOffset(ctx.GetTopic(), 0, group, 0))

	const recordCount = 18
	payloadSuffix := strings.Repeat("x", 196*1024)
	for offset := 0; offset < recordCount; offset++ {
		command := fmt.Sprintf(
			"PUBLISH topic=%s partition=0 acks=all producerId=state-writer isIdempotent=true seqNum=%d epoch=1 key=shared-key message=value-%02d-%s",
			ctx.GetTopic(), offset+1, offset, payloadSuffix,
		)
		response, publishErr := client.SendCommand(ctx.GetTopic(), command, 10*time.Second)
		require.NoError(t, publishErr, "publish offset %d", offset)
		require.True(t, successfulBrokerResponse(response), "publish offset %d: %s", offset, response)
		requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), uint64(offset+1))
	}
	waitForFullISRAndZeroUnderReplicated(t, ctx, "compacted records publish")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), recordCount)
	requireCompactionCompletedOnAllBrokers(t)
	requireCommittedOffsetReadableAfterCompaction(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group, member, generation)
	groupClient.Close()

	failedLeader, _ := actions.SimulateLeaderFailure()
	availableAddrs := availableBrokerAddrs(ctx.GetBrokerAddrs(), failedLeader)
	requireFailoverISRReady(t, availableAddrs, ctx.GetTopic(), failedLeader, recordCount)
	requireReplicaOffsetsEventually(t, availableAddrs, ctx.GetTopic(), recordCount)
	failoverWriter := e2e.NewBrokerClient(availableAddrs)
	for offset := recordCount; offset < recordCount+2; offset++ {
		response, publishErr := failoverWriter.SendCommand(ctx.GetTopic(), fmt.Sprintf(
			"PUBLISH topic=%s partition=0 acks=all producerId=failover-writer isIdempotent=true seqNum=%d epoch=1 key=shared-key message=value-%02d",
			ctx.GetTopic(), offset-recordCount+1, offset,
		), 10*time.Second)
		require.NoError(t, publishErr, "failover publish offset %d", offset)
		require.True(t, successfulBrokerResponse(response), response)
		requireReplicaOffsetsEventually(t, availableAddrs, ctx.GetTopic(), uint64(offset+1))
	}
	failoverWriter.Close()
	const finalRecordCount = recordCount + 2
	requireReplicaOffsetsEventually(t, availableAddrs, ctx.GetTopic(), finalRecordCount)
	failoverGroupClient, failoverGeneration, failoverMember := joinClusterGroup(t, availableAddrs, ctx.GetTopic(), group)
	failoverGroupClient.Close()
	requireCommittedOffsetReadableAfterCompaction(
		t, availableAddrs, ctx.GetTopic(), group, failoverMember, failoverGeneration,
	)

	actions.RecoverFollower(failedLeader)
	actions.WaitForTopicMetadata()
	waitForFullISRAndZeroUnderReplicated(t, ctx, "failed leader recovery")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), finalRecordCount)

	for node := 1; node <= 3; node++ {
		actions.StopBroker(node)
		actions.StartBroker(node)
		actions.WaitForTopicMetadata()
		waitForFullISRAndZeroUnderReplicated(t, ctx, fmt.Sprintf("broker-%d rolling restart", node))
		requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), finalRecordCount)
	}

	restartedClient, restartedGeneration, restartedMember := joinClusterGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group)
	defer restartedClient.Close()
	require.Equal(t, uint64(0), fetchCommittedOffset(t, restartedClient, ctx.GetTopic(), group))
	requireCommittedOffsetReadableAfterCompaction(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group, restartedMember, restartedGeneration)
	requireNoUnsupportedCompactionErrors(t)

	controlClient := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer controlClient.Close()
	deleteTopic := "distributed-delete-control"
	deleteResponse, err := controlClient.SendCommand("admin", "CREATE topic="+deleteTopic+" partitions=1 replication_factor=3", 5*time.Second)
	require.NoError(t, err)
	require.True(t, successfulBrokerResponse(deleteResponse), deleteResponse)
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), deleteTopic, 0)
	response, publishErr := controlClient.SendCommand(deleteTopic, fmt.Sprintf(
		"PUBLISH topic=%s partition=0 acks=all producerId=delete-writer isIdempotent=true seqNum=1 epoch=1 message=value",
		deleteTopic,
	), 5*time.Second)
	require.NoError(t, publishErr)
	require.True(t, successfulBrokerResponse(response), response)
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), deleteTopic, 1)
}

func successfulBrokerResponse(response string) bool {
	return strings.HasPrefix(response, "OK") || strings.Contains(response, `"status":"OK"`)
}

func requireCompactionCompletedOnAllBrokers(t *testing.T) {
	t.Helper()
	require.NoError(t, eventually(t, "successful compaction on every broker", 2*clusterReadyTimeout, func() (bool, string, error) {
		for node := 1; node <= 3; node++ {
			value, err := fetchMetricWithLabels(node, "cursus_broker_log_compaction_runs_total", map[string]string{
				"result": "completed",
				"reason": "none",
			})
			if err != nil || value < 1 {
				return false, fmt.Sprintf("broker-%d completed=%v err=%v", node, value, err), nil
			}
		}
		return true, "all brokers completed a compaction pass", nil
	}))
}

func requireCommittedOffsetReadableAfterCompaction(
	t *testing.T,
	addrs []string,
	topicName, group, member string,
	generation int,
) {
	t.Helper()
	offsetClient := e2e.NewBrokerClient(addrs)
	require.Equal(t, uint64(0), fetchCommittedOffset(t, offsetClient, topicName, group))
	offsetClient.Close()

	var messages []string
	var offsets []uint64
	var lastErr error
	for _, addr := range addrs {
		client := e2e.NewBrokerClient([]string{addr})
		messages, offsets, lastErr = client.ConsumeMessagesAtOffsetWithOffsets(topicName, 0, 0, group, member, generation, 5*time.Second)
		client.Close()
		if lastErr == nil {
			break
		}
		var brokerErr *wire.BrokerError
		if !errors.As(lastErr, &brokerErr) || (brokerErr.Code != "NOT_LEADER" && brokerErr.Code != "NOT_PARTITION_LEADER") {
			break
		}
	}
	require.NoError(t, lastErr)
	require.NotEmpty(t, messages)
	require.Len(t, messages, len(offsets))
	require.Greater(t, offsets[0], uint64(0), "committed offset inside a compacted hole must advance to a retained record")
	for index := 1; index < len(offsets); index++ {
		require.Greater(t, offsets[index], offsets[index-1])
	}
}

func fetchCommittedOffset(t *testing.T, client *e2e.BrokerClient, topicName, group string) uint64 {
	t.Helper()
	offset, err := client.FetchCommittedOffset(topicName, 0, group)
	require.NoError(t, err)
	return offset
}

func requireReplicaOffsetsEventually(t *testing.T, addrs []string, topicName string, expected uint64) {
	t.Helper()
	want := fmt.Sprintf("leo=%d:hwm=%d", expected, expected)
	require.NoError(t, eventually(t, "replica LEO/HWM convergence", 2*clusterReadyTimeout, func() (bool, string, error) {
		for _, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("", fmt.Sprintf("LIST_OFFSETS topic=%s partition=0", topicName), 5*time.Second)
			client.Close()
			if err != nil || !strings.Contains(response, want) {
				return false, fmt.Sprintf("%s response=%q err=%v", addr, response, err), nil
			}
		}
		return true, want, nil
	}))
}

func requireFailoverISRReady(t *testing.T, addrs []string, topicName string, unavailableNode int, expected uint64) {
	t.Helper()
	require.NoError(t, eventually(t, "failover ISR convergence", 2*clusterReadyTimeout, func() (bool, string, error) {
		return failoverISRConverged(addrs, topicName, unavailableNode, expected, func(addr string) (string, error) {
			client := e2e.NewBrokerClient([]string{addr})
			defer client.Close()
			return client.SendCommand("", "CLUSTER_STATUS", 5*time.Second)
		})
	}))
}

func failoverISRConverged(addrs []string, topicName string, unavailableNode int, expected uint64, fetch func(string) (string, error)) (bool, string, error) {
	wantISR := make([]string, 0, len(addrs))
	for node := 1; node <= len(addrs)+1; node++ {
		if node != unavailableNode {
			wantISR = append(wantISR, fmt.Sprintf("broker-%d-9000", node))
		}
	}
	slices.Sort(wantISR)
	var leader string
	var epoch int
	for _, addr := range addrs {
		response, err := fetch(addr)
		if err != nil {
			return false, fmt.Sprintf("%s CLUSTER_STATUS failed: %v", addr, err), nil
		}
		status, err := parseLocalClusterStatus(response)
		if err != nil {
			return false, addr + ": " + response, err
		}
		var partition *LocalPartitionStatus
		for i := range status.Partitions {
			if status.Partitions[i].Topic == topicName && status.Partitions[i].Partition == 0 {
				partition = &status.Partitions[i]
				break
			}
		}
		if partition == nil {
			return false, addr + ": partition not found", nil
		}
		slices.Sort(partition.ISR)
		if !partition.LeaderAvailable || !slices.Contains(wantISR, partition.Leader) || partition.LeaderEpoch <= 0 ||
			!slices.Equal(partition.ISR, wantISR) || partition.CommittedHWM != expected {
			return false, fmt.Sprintf("%s partition=%+v", addr, *partition), nil
		}
		if leader == "" {
			leader, epoch = partition.Leader, partition.LeaderEpoch
		} else if partition.Leader != leader || partition.LeaderEpoch != epoch {
			return false, fmt.Sprintf("%s leader=%s/%d want=%s/%d", addr, partition.Leader, partition.LeaderEpoch, leader, epoch), nil
		}
	}
	return len(addrs) > 0, fmt.Sprintf("leader=%s/%d isr=%v hwm=%d", leader, epoch, wantISR, expected), nil
}

func TestFailoverISRConvergedRequiresEveryReplicaFence(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*LocalPartitionStatus)
		ready  bool
	}{
		{"matching", func(*LocalPartitionStatus) {}, true},
		{"reordered ISR", func(p *LocalPartitionStatus) { slices.Reverse(p.ISR) }, true},
		{"stale epoch", func(p *LocalPartitionStatus) { p.LeaderEpoch-- }, false},
		{"different leader", func(p *LocalPartitionStatus) { p.Leader = "broker-3-9000" }, false},
		{"dead leader", func(p *LocalPartitionStatus) { p.Leader = "broker-2-9000" }, false},
		{"unavailable leader", func(p *LocalPartitionStatus) { p.LeaderAvailable = false }, false},
		{"dead ISR member", func(p *LocalPartitionStatus) { p.ISR[1] = "broker-2-9000" }, false},
		{"duplicate ISR member", func(p *LocalPartitionStatus) { p.ISR[1] = p.ISR[0] }, false},
		{"stale HWM", func(p *LocalPartitionStatus) { p.CommittedHWM-- }, false},
		{"missing partition", func(p *LocalPartitionStatus) { p.Topic = "other" }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var probed []string
			ready, detail, err := failoverISRConverged([]string{"first", "second"}, "orders", 2, 18, func(addr string) (string, error) {
				probed = append(probed, addr)
				partition := LocalPartitionStatus{Topic: "orders", Leader: "broker-1-9000", LeaderEpoch: 3,
					ISR: []string{"broker-1-9000", "broker-3-9000"}, CommittedHWM: 18, LeaderAvailable: true}
				if addr == "second" {
					test.change(&partition)
				}
				data, err := json.Marshal(LocalClusterStatus{RaftState: "Follower", Partitions: []LocalPartitionStatus{partition}})
				return "OK cluster=" + string(data), err
			})
			require.NoError(t, err)
			require.Equal(t, test.ready, ready, detail)
			require.Equal(t, []string{"first", "second"}, probed)
		})
	}
}

func availableBrokerAddrs(addrs []string, unavailableNode int) []string {
	available := make([]string, 0, len(addrs)-1)
	for index, addr := range addrs {
		if index+1 != unavailableNode {
			available = append(available, addr)
		}
	}
	return available
}

func fetchMetricWithLabels(node int, metricName string, labels map[string]string) (float64, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", baseExporterPort+node)
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url) // #nosec G107 -- loopback ports are fixed by the test compose file.
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", response.StatusCode)
	}
	for scanner := bufio.NewScanner(response.Body); scanner.Scan(); {
		line := scanner.Text()
		if !strings.HasPrefix(line, metricName+"{") {
			continue
		}
		matched := true
		for key, value := range labels {
			if !strings.Contains(line, fmt.Sprintf(`%s=%q`, key, value)) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, fmt.Errorf("invalid metric line %q", line)
		}
		return strconv.ParseFloat(fields[1], 64)
	}
	return 0, fmt.Errorf("metric %s with labels %v not found", metricName, labels)
}

func requireNoUnsupportedCompactionErrors(t *testing.T) {
	t.Helper()
	const unsupported = "log compaction is not supported in distributed mode"
	for node := 1; node <= 3; node++ {
		container := fmt.Sprintf("broker-%d", node)
		// #nosec G204 -- node is bounded by the fixed three-broker fixture.
		output, err := exec.Command("docker", "logs", container).CombinedOutput()
		require.NoError(t, err)
		require.NotContains(t, string(output), unsupported, container)
	}
}
