package e2e_cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
)

type LocalClusterStatus struct {
	RaftLeader            string                 `json:"raft_leader"`
	RaftState             string                 `json:"raft_state"`
	RaftAppliedIndex      uint64                 `json:"raft_applied_index"`
	RaftCommitIndex       uint64                 `json:"raft_commit_index"`
	RaftLastLogIndex      uint64                 `json:"raft_last_log_index"`
	RaftLastSnapshotIndex uint64                 `json:"raft_last_snapshot_index"`
	RaftLastSnapshotTerm  uint64                 `json:"raft_last_snapshot_term"`
	Partitions            []LocalPartitionStatus `json:"partitions"`
}

type LocalPartitionStatus struct {
	Topic           string   `json:"topic"`
	Partition       int      `json:"partition"`
	Leader          string   `json:"leader"`
	LeaderEpoch     int      `json:"leader_epoch"`
	ISR             []string `json:"isr"`
	CommittedHWM    uint64   `json:"committed_hwm"`
	LeaderAvailable bool     `json:"leader_available"`
}

type LocalTopicDefinition struct {
	Topic         string
	Partitions    int
	CleanupPolicy string
}

func (a *ClusterActions) sendCommandToNode(nodeIndex int, command string) (string, error) {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		return "", fmt.Errorf("invalid broker index %d: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}
	client := e2e.NewBrokerClient([]string{a.ctx.GetBrokerAddrs()[nodeIndex-1]})
	defer client.Close()
	return client.SendCommand("", command, 2*time.Second)
}

func (a *ClusterActions) LocalClusterStatus(nodeIndex int) (LocalClusterStatus, error) {
	resp, err := a.sendCommandToNode(nodeIndex, "CLUSTER_STATUS")
	if err != nil {
		return LocalClusterStatus{}, err
	}
	return parseLocalClusterStatus(resp)
}

func parseLocalClusterStatus(resp string) (LocalClusterStatus, error) {
	const prefix = "OK cluster="
	if !strings.HasPrefix(resp, prefix) {
		return LocalClusterStatus{}, fmt.Errorf("unexpected CLUSTER_STATUS response: %s", resp)
	}
	var status LocalClusterStatus
	if err := json.Unmarshal([]byte(strings.TrimPrefix(resp, prefix)), &status); err != nil {
		return LocalClusterStatus{}, fmt.Errorf("decode CLUSTER_STATUS: %w", err)
	}
	if strings.TrimSpace(status.RaftState) == "" {
		return LocalClusterStatus{}, fmt.Errorf("CLUSTER_STATUS has blank raft_state")
	}
	return status, nil
}

func (a *ClusterActions) LocalTopicDefinition(nodeIndex int, topic string) (LocalTopicDefinition, bool, error) {
	resp, err := a.sendCommandToNode(nodeIndex, fmt.Sprintf("METADATA topic=%s", topic))
	if err != nil {
		var brokerErr *wire.BrokerError
		if errors.As(err, &brokerErr) && brokerErr.Code == "topic_not_found" {
			return LocalTopicDefinition{}, false, nil
		}
		return LocalTopicDefinition{}, false, err
	}
	definition, err := parseLocalTopicDefinition(resp)
	return definition, err == nil, err
}

func parseLocalTopicDefinition(resp string) (LocalTopicDefinition, error) {
	if !strings.HasPrefix(resp, "OK ") {
		return LocalTopicDefinition{}, fmt.Errorf("unexpected METADATA response: %s", resp)
	}
	values := make(map[string]string)
	for _, field := range strings.Fields(resp) {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 {
			values[parts[0]] = parts[1]
		}
	}
	partitions, err := strconv.Atoi(values["partitions"])
	if err != nil || partitions <= 0 {
		return LocalTopicDefinition{}, fmt.Errorf("invalid METADATA partitions %q", values["partitions"])
	}
	definition := LocalTopicDefinition{
		Topic:         values["topic"],
		Partitions:    partitions,
		CleanupPolicy: values["cleanup_policy"],
	}
	if definition.Topic == "" || definition.CleanupPolicy == "" {
		return LocalTopicDefinition{}, fmt.Errorf("incomplete METADATA response: %s", resp)
	}
	return definition, nil
}

func (a *ClusterActions) WaitForLocalTopicDefinition(nodeIndex int, expected LocalTopicDefinition, present bool) error {
	description := fmt.Sprintf("topic %s present=%t on broker-%d", expected.Topic, present, nodeIndex)
	return eventually(a.ctx.GetT(), description, clusterReadyTimeout, func() (bool, string, error) {
		actual, found, err := a.LocalTopicDefinition(nodeIndex, expected.Topic)
		if err != nil {
			return false, "METADATA failed", err
		}
		if !present {
			return !found, fmt.Sprintf("found=%t definition=%+v", found, actual), nil
		}
		matches := found && actual == expected
		return matches, fmt.Sprintf("found=%t definition=%+v", found, actual), nil
	})
}

func (a *ClusterActions) WaitForLocalSnapshot(nodeIndex int, minimumIndex uint64) (LocalClusterStatus, error) {
	var observed LocalClusterStatus
	err := eventually(a.ctx.GetT(), fmt.Sprintf("snapshot index >= %d on broker-%d", minimumIndex, nodeIndex), clusterReadyTimeout, func() (bool, string, error) {
		status, err := a.LocalClusterStatus(nodeIndex)
		if err != nil {
			return false, "CLUSTER_STATUS failed", err
		}
		observed = status
		return status.RaftLastSnapshotIndex >= minimumIndex &&
				status.RaftAppliedIndex >= minimumIndex,
			fmt.Sprintf("state=%s applied=%d snapshot=%d", status.RaftState, status.RaftAppliedIndex, status.RaftLastSnapshotIndex), nil
	})
	return observed, err
}

func (a *ClusterActions) CreateNamedTopic(topic string, partitions int) error {
	return a.ctx.GetClient().CreateTopic(topic, partitions, false)
}

func (a *ClusterActions) DeleteNamedTopic(topic string) error {
	return a.ctx.GetClient().DeleteTopic(topic)
}
