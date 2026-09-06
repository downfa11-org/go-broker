package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type RaftInterface interface {
	Apply([]byte, time.Duration) raft.ApplyFuture
	AddVoter(raft.ServerID, raft.ServerAddress, uint64, time.Duration) raft.IndexFuture
	RemoveServer(raft.ServerID, uint64, time.Duration) raft.IndexFuture
	Leader() raft.ServerAddress
	State() raft.RaftState
	GetConfiguration() raft.ConfigurationFuture
	BootstrapCluster(raft.Configuration) raft.Future
	Shutdown() raft.Future
	Stats() map[string]string
}

type RaftStatus struct {
	State             string
	AppliedIndex      uint64
	CommitIndex       uint64
	LastLogIndex      uint64
	LastSnapshotIndex uint64
	LastSnapshotTerm  uint64
}

const recoveredPartitionReplayNoProgressTimeout = 2 * time.Minute

type raftStatsReader interface {
	Stats() map[string]string
}

type recoveredPartitionState interface {
	AppliedIndex() uint64
	HasPendingPartitionRecovery() bool
	FinalizeRecoveredPartitions() error
	ValidateLocalLeaderLogs(string) error
}

type recoveredPartitionReplayProgress struct {
	snapshotIndex uint64
	commitIndex   uint64
	lastLogIndex  uint64
	appliedIndex  uint64
	targetIndex   uint64
}

func (p recoveredPartitionReplayProgress) advancedSince(previous recoveredPartitionReplayProgress) bool {
	return p.snapshotIndex > previous.snapshotIndex ||
		p.commitIndex > previous.commitIndex ||
		p.lastLogIndex > previous.lastLogIndex ||
		p.appliedIndex > previous.appliedIndex ||
		p.targetIndex > previous.targetIndex
}

func (p recoveredPartitionReplayProgress) mergeMax(other recoveredPartitionReplayProgress) recoveredPartitionReplayProgress {
	if other.snapshotIndex > p.snapshotIndex {
		p.snapshotIndex = other.snapshotIndex
	}
	if other.commitIndex > p.commitIndex {
		p.commitIndex = other.commitIndex
	}
	if other.lastLogIndex > p.lastLogIndex {
		p.lastLogIndex = other.lastLogIndex
	}
	if other.appliedIndex > p.appliedIndex {
		p.appliedIndex = other.appliedIndex
	}
	if other.targetIndex > p.targetIndex {
		p.targetIndex = other.targetIndex
	}
	return p
}

type ISRManagerInterface interface {
	HasQuorum(topic string, partition int, minISR int) bool
	UpdateHeartbeat(brokerID string)
	BuildCatchupProofs() []fsm.ISRCatchupProof
	SubmitCatchupProofs(nodeID string, proofs []fsm.ISRCatchupProof) error
	GetISR(topic string, partition int) []string
	ComputeISR(topic string, partition int) []string
	SetLeader(isLeader bool)
	Start()
}

type RaftReplicationManager struct {
	raft       RaftInterface
	fsm        *fsm.BrokerFSM
	isrManager ISRManagerInterface

	brokerID  string
	localAddr string
	peers     map[string]string // brokerID -> addr
	mu        sync.RWMutex
	raftStore *raftboltdb.BoltStore

	isLeader atomic.Bool
	leaderCh chan bool
}

func NewRaftReplicationManager(ctx context.Context, cfg *config.Config, brokerID string, topicManager *topic.TopicManager, coordinator *coordinator.Coordinator, client client.TCPClusterClient) (*RaftReplicationManager, error) {
	brokerFSM := fsm.NewBrokerFSM(topicManager, coordinator)

	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
	raftCfg, err := buildRaftConfig(cfg, brokerID)
	if err != nil {
		return nil, err
	}

	notifyCh := make(chan bool, 10)
	raftCfg.NotifyCh = notifyCh

	if len(cfg.StaticClusterMembers) >= 3 {
		raftCfg.PreVoteDisabled = true
	}

	dataDir := filepath.Join(cfg.LogDir, "raft")
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		util.Error("Failed to create raft data directory %s: %v", dataDir, err)
		return nil, fmt.Errorf("failed to create raft data directory: %w", err)
	}
	if err := ensureRaftRecoveryFormat(dataDir); err != nil {
		return nil, err
	}

	raftStore, err := newDurableRaftStore(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open durable raft store: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		_ = raftStore.Close()
		util.Error("Failed to create snapshot store: %v", err)
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	advertiseTCPAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		_ = raftStore.Close()
		util.Error("Failed to resolve advertised address %s: %v", localAddr, err)
		return nil, fmt.Errorf("failed to resolve advertised address: %w", err)
	}

	bindAddr := fmt.Sprintf("0.0.0.0:%d", cfg.RaftPort)
	transport, err := newRaftNetworkTransport(cfg, bindAddr, advertiseTCPAddr)
	if err != nil {
		_ = raftStore.Close()
		util.Error("Failed to create raft transport: %v", err)
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	boundary, err := persistedReplayBoundary(raftStore, snapshots)
	if err != nil {
		_ = raftStore.Close()
		_ = transport.Close()
		return nil, fmt.Errorf("inspect persisted Raft recovery boundary: %w", err)
	}
	if boundary.index != 0 {
		brokerFSM.BeginPartitionRecovery()
	}
	r, err := raft.NewRaft(raftCfg, brokerFSM, raftStore, raftStore, snapshots, transport)
	if err != nil {
		_ = raftStore.Close()
		_ = transport.Close()
		util.Error("Failed to create raft instance: %v", err)
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	if cfg.BootstrapCluster {
		util.Info("🚀 Raft node %s checking if bootstrap is needed", brokerID)
		if confFuture := r.GetConfiguration(); confFuture.Error() == nil {
			conf := confFuture.Configuration()
			if len(conf.Servers) == 0 {
				util.Info("🚀 No Raft servers found, starting static cluster bootstrap (members=%v)", cfg.StaticClusterMembers)

				var servers []raft.Server
				for _, member := range cfg.StaticClusterMembers {
					member = strings.TrimSpace(member)
					if member == "" {
						continue
					}

					var memberID, memberAddr string
					if strings.Contains(member, "@") {
						parts := strings.SplitN(member, "@", 2)
						if len(parts) == 2 {
							memberID = parts[0]
							memberAddr = parts[1]
						} else {
							continue
						}
					} else {
						memberAddr = member
						memberID = memberAddr
					}

					util.Info("🔗 Adding Raft voter: ID=%s, Addr=%s", memberID, memberAddr)
					servers = append(servers, raft.Server{
						ID:       raft.ServerID(memberID),
						Address:  raft.ServerAddress(memberAddr),
						Suffrage: raft.Voter,
					})
				}

				if len(servers) > 0 {
					bootstrapConfig := raft.Configuration{Servers: servers}
					if err := r.BootstrapCluster(bootstrapConfig).Error(); err != nil {
						util.Error("❌ Raft bootstrap failed for node %s: %v", brokerID, err)
						return nil, fmt.Errorf("bootstrap failed: %w", err)
					}
					util.Info("✅ Raft cluster bootstrap initiated with %d servers on node %s", len(servers), brokerID)
				}
			} else {
				util.Info("ℹ️ Raft node %s already has %d servers in configuration, skipping bootstrap", brokerID, len(conf.Servers))
			}
		} else {
			util.Error("❌ Failed to get Raft configuration for node %s: %v", brokerID, confFuture.Error())
		}
	}
	if err := awaitRecoveredPartitionReplay(ctx, r, raftStore, brokerFSM, brokerID, recoveredPartitionReplayNoProgressTimeout, boundary); err != nil {
		_ = r.Shutdown().Error()
		_ = raftStore.Close()
		_ = transport.Close()
		return nil, err
	}

	rm := &RaftReplicationManager{
		raft:      r,
		fsm:       brokerFSM,
		brokerID:  brokerID,
		localAddr: localAddr,
		peers:     make(map[string]string),
		leaderCh:  make(chan bool, 10),
		raftStore: raftStore,
	}

	rm.isrManager = NewISRManager(ctx, brokerFSM, brokerID, 5*time.Second, rm)
	go rm.isrManager.Start()

	go rm.observeLeadership(notifyCh)
	go rm.reconcileTopicMaterializations(ctx, 5*time.Second)

	return rm, nil
}

func awaitRecoveredPartitionReplay(ctx context.Context, r raftStatsReader, logStore raft.LogStore, brokerFSM recoveredPartitionState, brokerID string, noProgressTimeout time.Duration, boundary replayBoundary) error {
	if brokerFSM == nil || !brokerFSM.HasPendingPartitionRecovery() {
		return nil
	}
	if noProgressTimeout <= 0 {
		noProgressTimeout = recoveredPartitionReplayNoProgressTimeout
	}
	deadline := time.NewTimer(noProgressTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	progressLog := time.NewTicker(10 * time.Second)
	defer progressLog.Stop()

	var furthestProgress recoveredPartitionReplayProgress
	var latestProgress recoveredPartitionReplayProgress
	var scannedSnapshotIndex uint64
	var scannedCommitIndex uint64
	var targetIndex uint64
	targetKnown := false
	initialized := false
	for {
		stats := r.Stats()
		snapshotIndex, snapshotErr := strconv.ParseUint(stats["last_snapshot_index"], 10, 64)
		commitIndex, commitErr := strconv.ParseUint(stats["commit_index"], 10, 64)
		lastLogIndex, lastLogErr := strconv.ParseUint(stats["last_log_index"], 10, 64)
		latestProgress = recoveredPartitionReplayProgress{
			snapshotIndex: snapshotIndex,
			commitIndex:   commitIndex,
			lastLogIndex:  lastLogIndex,
			appliedIndex:  brokerFSM.AppliedIndex(),
		}
		if snapshotErr == nil && commitErr == nil && lastLogErr == nil && commitIndex >= snapshotIndex && lastLogIndex >= commitIndex {
			scanStart := snapshotIndex
			if targetKnown && snapshotIndex == scannedSnapshotIndex && commitIndex >= scannedCommitIndex {
				scanStart = scannedCommitIndex
			} else {
				targetIndex = 0
				targetKnown = false
			}
			if !targetKnown || commitIndex > scannedCommitIndex {
				candidate, err := highestFSMCommandIndex(logStore, scanStart, commitIndex)
				if err == nil {
					if candidate > targetIndex {
						targetIndex = candidate
					}
					scannedSnapshotIndex = snapshotIndex
					scannedCommitIndex = commitIndex
					targetKnown = true
				} else if !errors.Is(err, raft.ErrLogNotFound) {
					return fmt.Errorf("inspect committed Raft replay range: %w", err)
				}
			}
			latestProgress.targetIndex = targetIndex
			boundaryCommitted, boundaryErr := boundary.committed(logStore, stats, commitIndex, snapshotIndex)
			if boundaryErr != nil && !errors.Is(boundaryErr, raft.ErrLogNotFound) {
				return fmt.Errorf("inspect Raft recovery boundary: %w", boundaryErr)
			}
			if boundaryErr == nil && boundaryCommitted && targetKnown && latestProgress.appliedIndex >= targetIndex {
				latestCommit, parseErr := strconv.ParseUint(r.Stats()["commit_index"], 10, 64)
				if parseErr == nil && latestCommit == commitIndex {
					if err := brokerFSM.FinalizeRecoveredPartitions(); err != nil {
						return fmt.Errorf("finalize recovered partitions after Raft replay: %w", err)
					}
					if err := brokerFSM.ValidateLocalLeaderLogs(brokerID); err != nil {
						return err
					}
					return nil
				}
			}
		}
		if !initialized {
			furthestProgress = latestProgress
			initialized = true
		} else if latestProgress.advancedSince(furthestProgress) {
			furthestProgress = furthestProgress.mergeMax(latestProgress)
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}
			deadline.Reset(noProgressTimeout)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for recovered partition Raft replay: %w", ctx.Err())
		case <-deadline.C:
			return fmt.Errorf(
				"timed out waiting for recovered partition Raft replay after %s without progress (snapshot_index=%d commit_index=%d last_log_index=%d applied_index=%d target_index=%d)",
				noProgressTimeout,
				latestProgress.snapshotIndex,
				latestProgress.commitIndex,
				latestProgress.lastLogIndex,
				latestProgress.appliedIndex,
				latestProgress.targetIndex,
			)
		case <-progressLog.C:
			util.Info(
				"Waiting for recovered partition Raft replay on broker %s: snapshot_index=%d commit_index=%d last_log_index=%d applied_index=%d target_index=%d",
				brokerID,
				latestProgress.snapshotIndex,
				latestProgress.commitIndex,
				latestProgress.lastLogIndex,
				latestProgress.appliedIndex,
				latestProgress.targetIndex,
			)
		case <-ticker.C:
		}
	}
}

func highestFSMCommandIndex(logStore raft.LogStore, snapshotIndex, commitIndex uint64) (uint64, error) {
	var target uint64
	for index := snapshotIndex + 1; index <= commitIndex; index++ {
		var entry raft.Log
		if err := logStore.GetLog(index, &entry); err != nil {
			return 0, err
		}
		if entry.Type == raft.LogCommand {
			target = index
		}
	}
	return target, nil
}

func buildRaftConfig(cfg *config.Config, brokerID string) (*raft.Config, error) {
	if cfg == nil {
		return nil, fmt.Errorf("raft config is required")
	}
	if cfg.RaftSnapshotIntervalMS < 5 {
		return nil, fmt.Errorf("raft_snapshot_interval_ms must be at least 5")
	}
	if cfg.RaftSnapshotThreshold == 0 {
		return nil, fmt.Errorf("raft_snapshot_threshold must be greater than zero")
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(brokerID)
	raftCfg.SnapshotInterval = time.Duration(cfg.RaftSnapshotIntervalMS) * time.Millisecond
	raftCfg.SnapshotThreshold = cfg.RaftSnapshotThreshold
	raftCfg.TrailingLogs = cfg.RaftTrailingLogs

	// Raft Security Rule: HeartbeatTimeout must be larger than LeaderLeaseTimeout.
	raftCfg.HeartbeatTimeout = 1000 * time.Millisecond
	raftCfg.ElectionTimeout = 2000 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 800 * time.Millisecond
	raftCfg.CommitTimeout = 50 * time.Millisecond
	raftCfg.LogLevel = "Info"

	if err := raft.ValidateConfig(raftCfg); err != nil {
		return nil, fmt.Errorf("invalid raft config: %w", err)
	}
	return raftCfg, nil
}

func (rm *RaftReplicationManager) reconcileTopicMaterializations(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := rm.fsm.ReconcileTopicMaterializations(); err != nil {
				util.Warn("Topic materialization reconcile pending on broker %s: %v", rm.brokerID, err)
			}
		}
	}
}
func (rm *RaftReplicationManager) observeLeadership(notifyCh <-chan bool) {
	for isLeader := range notifyCh {
		rm.isLeader.Store(isLeader)

		if rm.isrManager != nil {
			rm.isrManager.SetLeader(isLeader)
		}

		select {
		case rm.leaderCh <- isLeader:
		default:
		}
	}
}

func (rm *RaftReplicationManager) GetISRManager() ISRManagerInterface {
	return rm.isrManager
}

func (rm *RaftReplicationManager) IsLeader() bool {
	return rm.isLeader.Load()
}

func (rm *RaftReplicationManager) LeaderCh() <-chan bool {
	return rm.leaderCh
}

func (rm *RaftReplicationManager) GetLeaderAddress() string {
	return string(rm.raft.Leader())
}

func (rm *RaftReplicationManager) GetRaftStatus() (RaftStatus, error) {
	if rm == nil || rm.raft == nil {
		return RaftStatus{}, fmt.Errorf("raft is not available")
	}
	stats := rm.raft.Stats()
	state := strings.TrimSpace(stats["state"])
	if state == "" {
		return RaftStatus{}, fmt.Errorf("raft stat %q is missing or blank", "state")
	}
	status := RaftStatus{State: state}
	fields := []struct {
		key    string
		target *uint64
	}{
		{key: "applied_index", target: &status.AppliedIndex},
		{key: "commit_index", target: &status.CommitIndex},
		{key: "last_log_index", target: &status.LastLogIndex},
		{key: "last_snapshot_index", target: &status.LastSnapshotIndex},
		{key: "last_snapshot_term", target: &status.LastSnapshotTerm},
	}
	for _, field := range fields {
		value, ok := stats[field.key]
		if !ok {
			return RaftStatus{}, fmt.Errorf("raft stat %q is missing", field.key)
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return RaftStatus{}, fmt.Errorf("parse raft stat %q: %w", field.key, err)
		}
		*field.target = parsed
	}
	return status, nil
}

func (rm *RaftReplicationManager) GetFSM() *fsm.BrokerFSM {
	return rm.fsm
}

func (rm *RaftReplicationManager) GetConfiguration() raft.ConfigurationFuture {
	return rm.raft.GetConfiguration()
}

func (rm *RaftReplicationManager) ApplyCommand(prefix string, data []byte) error {
	fullCmd := []byte(fmt.Sprintf("%s:%s", prefix, string(data)))
	future := rm.raft.Apply(fullCmd, 5*time.Second)
	return future.Error()
}

func (rm *RaftReplicationManager) AddVoter(id string, addr string) error {
	future := rm.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	rm.mu.Lock()
	rm.peers[id] = addr
	rm.mu.Unlock()
	return nil
}

func (rm *RaftReplicationManager) RemoveServer(id string) error {
	future := rm.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	if err := future.Error(); err == nil {
		rm.mu.Lock()
		delete(rm.peers, id)
		rm.mu.Unlock()
	}
	return future.Error()
}

func (rm *RaftReplicationManager) ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			return types.AckResponse{}, fmt.Errorf("insufficient in-sync replicas")
		}
	}

	cmd := types.MessageCommand{
		Topic:          topic,
		Partition:      partition,
		LifecycleEpoch: rm.partitionLifecycleEpoch(topic, partition),
		Messages:       []types.Message{msg},
		Acks:           "-1",
		IsIdempotent:   isIdempotent,
		SequenceScope:  sequenceScope,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return types.AckResponse{}, err
	}

	return rm.ApplyResponse("MESSAGE", data, 5*time.Second)
}

func (rm *RaftReplicationManager) ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	if len(messages) == 0 {
		return types.AckResponse{}, nil
	}

	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			return types.AckResponse{}, fmt.Errorf("insufficient in-sync replicas")
		}
	}

	batchData := types.MessageCommand{
		Topic:          topic,
		Partition:      partition,
		LifecycleEpoch: rm.partitionLifecycleEpoch(topic, partition),
		IsIdempotent:   isIdempotent,
		SequenceScope:  sequenceScope,
		Messages:       messages,
		Acks:           acks,
	}

	data, err := json.Marshal(batchData)
	if err != nil {
		return types.AckResponse{}, err
	}

	return rm.ApplyResponse("MESSAGE", data, 5*time.Second)
}

func (rm *RaftReplicationManager) partitionLifecycleEpoch(topicName string, partition int) uint64 {
	if rm != nil && rm.fsm != nil {
		if metadata := rm.fsm.GetPartitionMetadata(topicName + "-" + strconv.Itoa(partition)); metadata != nil && metadata.LifecycleEpoch != 0 {
			return metadata.LifecycleEpoch
		}
	}
	return topic.InitialLifecycleEpoch
}

func (rm *RaftReplicationManager) ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error) {
	fullCmd := []byte(fmt.Sprintf("%s:%s", prefix, string(data)))

	future := rm.raft.Apply(fullCmd, timeout)
	if err := future.Error(); err != nil {
		return types.AckResponse{}, err
	}

	response := future.Response()
	if response == nil {
		return types.AckResponse{}, fmt.Errorf("fsm returned nil response")
	}

	resp, ok := response.(types.AckResponse)
	if !ok {
		return types.AckResponse{}, fmt.Errorf("invalid response type")
	}

	return resp, nil
}

func newDurableRaftStore(dataDir string) (*raftboltdb.BoltStore, error) {
	return raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
}

func (rm *RaftReplicationManager) Shutdown() error {
	var shutdownErr error
	if rm.raft != nil {
		if err := rm.raft.Shutdown().Error(); err != nil {
			shutdownErr = err
		}
	}
	if rm.raftStore != nil {
		if err := rm.raftStore.Close(); err != nil {
			if shutdownErr != nil {
				return fmt.Errorf("raft shutdown failed: %v; store close failed: %w", shutdownErr, err)
			}
			return fmt.Errorf("close raft store: %w", err)
		}
	}
	return shutdownErr
}
