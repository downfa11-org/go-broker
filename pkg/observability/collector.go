package observability

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clustercontroller "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/prometheus/client_golang/prometheus"
)

type topicSource interface {
	RuntimeSnapshot() topic.RuntimeSnapshot
}

type groupSource interface {
	ExportState() map[string]*coordinator.GroupStateSnapshot
}

type groupLifecycleSource interface {
	ObserveConsumerGroups() []coordinator.ConsumerGroupObservation
}

type diskSource interface {
	RuntimeSnapshot() disk.RuntimeSnapshot
}

type streamSource interface {
	ActiveCount() int
}

type clusterSource interface {
	RuntimeSnapshot() clustercontroller.RuntimeSnapshot
}

type transactionSource interface {
	RuntimeSnapshot() transaction.RuntimeSnapshot
}

// ReadinessSource reports whether the broker is ready to accept client work.
type ReadinessSource interface {
	IsReady() bool
}

// Collector exports scrape-time state rather than retaining stale gauge labels.
type Collector struct {
	topics         topicSource
	groups         groupSource
	disk           diskSource
	streams        streamSource
	cluster        clusterSource
	readiness      ReadinessSource
	descriptors    []*prometheus.Desc
	transactions   transactionSource
	requestBudgets map[string]*wire.FrameBudget

	observationMu       sync.Mutex
	observationFailures map[observationFailureKey]uint64

	ready                           *prometheus.Desc
	topicCount                      *prometheus.Desc
	metadataLoadFailure             *prometheus.Desc
	metadataRestoredTopics          *prometheus.Desc
	metadataOrphanTopics            *prometheus.Desc
	metadataDurabilityWarning       *prometheus.Desc
	metadataDurabilityWarningsTotal *prometheus.Desc
	partitionCount                  *prometheus.Desc
	logStart                        *prometheus.Desc
	logEnd                          *prometheus.Desc
	highWatermark                   *prometheus.Desc
	groupMembers                    *prometheus.Desc
	groupState                      *prometheus.Desc
	groupCoordinatorUp              *prometheus.Desc
	groupLastActivity               *prometheus.Desc
	groupLastRebalance              *prometheus.Desc
	groupObservationFailures        *prometheus.Desc
	groupGeneration                 *prometheus.Desc
	groupAssignments                *prometheus.Desc
	groupCommittedOffset            *prometheus.Desc
	groupLag                        *prometheus.Desc
	groupOffsetOutOfRange           *prometheus.Desc
	consumerMetadataRecovery        *prometheus.Desc
	consumerMetadataRestoredGroups  *prometheus.Desc
	consumerMetadataRestoredOffsets *prometheus.Desc
	consumerMetadataReplayedRecords *prometheus.Desc
	consumerMetadataOrphanRecords   *prometheus.Desc
	consumerMetadataCorruptRecords  *prometheus.Desc
	activeStreams                   *prometheus.Desc
	storageHandlers                 *prometheus.Desc
	storageSegments                 *prometheus.Desc
	storageBytes                    *prometheus.Desc
	storagePendingWrites            *prometheus.Desc
	storageActiveReaders            *prometheus.Desc
	storageStatFailures             *prometheus.Desc
	storageSegmentCacheEntries      *prometheus.Desc
	storageSegmentCacheHits         *prometheus.Desc
	storageSegmentCacheMisses       *prometheus.Desc
	storageSegmentCacheEvictions    *prometheus.Desc
	wireProtocolFailures            *prometheus.Desc
	wireDecompressionRejections     *prometheus.Desc
	distributionEnabled             *prometheus.Desc
	clusterBrokers                  *prometheus.Desc
	clusterHasLeader                *prometheus.Desc
	clusterIsLeader                 *prometheus.Desc
	clusterOffline                  *prometheus.Desc
	clusterUnderReplicated          *prometheus.Desc
	topicMaterializationPending     *prometheus.Desc
	topicMaterializationAttempts    *prometheus.Desc
	topicMaterializationOldest      *prometheus.Desc
	partitionReplicas               *prometheus.Desc
	partitionInSync                 *prometheus.Desc
	partitionLeaderEpoch            *prometheus.Desc
	partitionLeader                 *prometheus.Desc
	isrCatchupProofs                *prometheus.Desc
	transactionRecovery             *prometheus.Desc
	transactionStates               *prometheus.Desc
	transactionExpired              *prometheus.Desc
	transactionOldestActive         *prometheus.Desc
	transactionRetainedBytes        *prometheus.Desc
	requestInFlight                 *prometheus.Desc
	requestInFlightLimit            *prometheus.Desc
	requestBytes                    *prometheus.Desc
	requestByteLimit                *prometheus.Desc
	requestRejected                 *prometheus.Desc
}

type observationFailureKey struct {
	topic  string
	group  string
	reason string
}

type observationGroupKey struct {
	topic string
	group string
}

type observationFailureSample struct {
	key   observationFailureKey
	value uint64
}

// NewCollector creates a broker runtime collector. Nil sources are supported.
func NewCollector(topics topicSource, groups groupSource, diskState diskSource, streams streamSource, cluster clusterSource, readiness ReadinessSource, transactions ...transactionSource) *Collector {
	c := &Collector{
		topics:                          topics,
		groups:                          groups,
		disk:                            diskState,
		streams:                         streams,
		cluster:                         cluster,
		readiness:                       readiness,
		ready:                           prometheus.NewDesc("cursus_broker_ready", "Whether the broker is ready to accept client work.", nil, nil),
		topicCount:                      prometheus.NewDesc("cursus_broker_topics", "Number of topics loaded by this broker.", nil, nil),
		metadataLoadFailure:             prometheus.NewDesc("cursus_topic_metadata_manifest_load_failure", "Whether the current durable topic manifest load failed.", nil, nil),
		metadataRestoredTopics:          prometheus.NewDesc("cursus_topic_metadata_restored_topics", "Topics restored from the durable standalone manifest during startup.", nil, nil),
		metadataOrphanTopics:            prometheus.NewDesc("cursus_topic_metadata_orphan_topics", "Persisted topic directories confirmed absent from the durable manifest.", nil, nil),
		metadataDurabilityWarning:       prometheus.NewDesc("cursus_topic_metadata_durability_warning", "Whether the latest committed topic metadata update has directory-sync durability uncertainty.", nil, nil),
		metadataDurabilityWarningsTotal: prometheus.NewDesc("cursus_topic_metadata_durability_warnings_total", "Topic metadata updates committed with directory-sync durability uncertainty.", nil, nil),
		partitionCount:                  prometheus.NewDesc("cursus_broker_partitions", "Number of topic partitions loaded by this broker.", nil, nil),
		logStart:                        prometheus.NewDesc("cursus_partition_log_start_offset", "Earliest retained offset for a partition.", []string{"topic", "partition"}, nil),
		logEnd:                          prometheus.NewDesc("cursus_partition_log_end_offset", "Next offset allocated in a partition.", []string{"topic", "partition"}, nil),
		highWatermark:                   prometheus.NewDesc("cursus_partition_high_watermark", "Next offset visible to committed readers.", []string{"topic", "partition"}, nil),
		groupMembers:                    prometheus.NewDesc("cursus_consumer_group_members", "Active members reported by the authoritative consumer group coordinator.", []string{"topic", "group"}, nil),
		groupState:                      prometheus.NewDesc("cursus_consumer_group_state", "Current authoritative consumer group state as a one-hot gauge.", []string{"topic", "group", "state"}, nil),
		groupCoordinatorUp:              prometheus.NewDesc("cursus_consumer_group_coordinator_up", "Whether this broker successfully resolved itself as the current group coordinator.", []string{"topic", "group"}, nil),
		groupLastActivity:               prometheus.NewDesc("cursus_consumer_group_last_activity_timestamp_seconds", "Unix timestamp of the last authoritative heartbeat or group lifecycle activity.", []string{"topic", "group"}, nil),
		groupLastRebalance:              prometheus.NewDesc("cursus_consumer_group_last_rebalance_timestamp_seconds", "Unix timestamp of the last authoritative completed group rebalance.", []string{"topic", "group"}, nil),
		groupObservationFailures:        prometheus.NewDesc("cursus_consumer_group_observation_failures_total", "Failed consumer group observation attempts by bounded reason.", []string{"topic", "group", "reason"}, nil),
		groupGeneration:                 prometheus.NewDesc("cursus_consumer_group_generation", "Current consumer group generation.", []string{"group", "topic"}, nil),
		groupAssignments:                prometheus.NewDesc("cursus_consumer_group_assigned_partitions", "Partitions assigned to active group members.", []string{"group", "topic"}, nil),
		groupCommittedOffset:            prometheus.NewDesc("cursus_consumer_group_committed_offset", "Committed next offset for a consumer group partition.", []string{"group", "topic", "partition"}, nil),
		groupLag:                        prometheus.NewDesc("cursus_consumer_group_lag", "Committed-reader lag, max(high watermark - committed next offset, 0).", []string{"group", "topic", "partition"}, nil),
		groupOffsetOutOfRange:           prometheus.NewDesc("cursus_consumer_group_offset_out_of_range", "Whether a committed offset is outside the retained committed-readable range.", []string{"group", "topic", "partition"}, nil),
		consumerMetadataRecovery:        prometheus.NewDesc("cursus_consumer_metadata_recovery_ready", "Whether durable consumer metadata recovery completed without error.", []string{"phase"}, nil),
		consumerMetadataRestoredGroups:  prometheus.NewDesc("cursus_consumer_metadata_restored_groups", "Consumer groups restored during startup.", nil, nil),
		consumerMetadataRestoredOffsets: prometheus.NewDesc("cursus_consumer_metadata_restored_offsets", "Committed next-offset keys restored during startup.", nil, nil),
		consumerMetadataReplayedRecords: prometheus.NewDesc("cursus_consumer_metadata_replayed_records", "Internal metadata records scanned during startup.", nil, nil),
		consumerMetadataOrphanRecords:   prometheus.NewDesc("cursus_consumer_metadata_orphan_records", "Internal records fenced by a newer lifecycle or tombstone.", nil, nil),
		consumerMetadataCorruptRecords:  prometheus.NewDesc("cursus_consumer_metadata_corrupt_records", "Corrupt or inconsistent internal records found during startup.", nil, nil),
		activeStreams:                   prometheus.NewDesc("cursus_streams_active", "Currently registered streaming consumers.", nil, nil),
		storageHandlers:                 prometheus.NewDesc("cursus_storage_handlers", "Open partition storage handlers.", nil, nil),
		storageSegments:                 prometheus.NewDesc("cursus_storage_segments", "Open storage segments including active segments.", nil, nil),
		storageBytes:                    prometheus.NewDesc("cursus_storage_bytes", "Bytes used by segment and offset index files for open handlers.", nil, nil),
		storagePendingWrites:            prometheus.NewDesc("cursus_storage_pending_writes", "Messages waiting in storage write queues.", nil, nil),
		storageActiveReaders:            prometheus.NewDesc("cursus_storage_active_readers", "Readers currently accessing storage segments.", nil, nil),
		storageStatFailures:             prometheus.NewDesc("cursus_storage_stat_failures", "Storage files that could not be inspected during this scrape.", nil, nil),
		storageSegmentCacheEntries:      prometheus.NewDesc("cursus_storage_segment_cache_entries", "Open memory-mapped segment readers retained in the bounded cache.", nil, nil),
		storageSegmentCacheHits:         prometheus.NewDesc("cursus_storage_segment_cache_hits", "Segment reader cache hits for currently open handlers.", nil, nil),
		storageSegmentCacheMisses:       prometheus.NewDesc("cursus_storage_segment_cache_misses", "Segment reader cache misses for currently open handlers.", nil, nil),
		storageSegmentCacheEvictions:    prometheus.NewDesc("cursus_storage_segment_cache_evictions", "Segment reader cache evictions for currently open handlers.", nil, nil),
		wireProtocolFailures:            prometheus.NewDesc("cursus_wire_protocol_failures_total", "Wire v2 frames rejected by bounded protocol failure reason.", []string{"reason"}, nil),
		wireDecompressionRejections:     prometheus.NewDesc("cursus_wire_decompression_rejections_total", "Wire v2 payloads rejected by bounded decompression reason.", []string{"reason"}, nil),
		distributionEnabled:             prometheus.NewDesc("cursus_distribution_enabled", "Whether distributed cluster mode is enabled.", nil, nil),
		clusterBrokers:                  prometheus.NewDesc("cursus_cluster_brokers", "Brokers present in replicated cluster metadata.", nil, nil),
		clusterHasLeader:                prometheus.NewDesc("cursus_cluster_has_leader", "Whether this broker can resolve the cluster leader.", nil, nil),
		clusterIsLeader:                 prometheus.NewDesc("cursus_cluster_is_leader", "Whether this broker is the current cluster leader.", nil, nil),
		clusterOffline:                  prometheus.NewDesc("cursus_cluster_offline_partitions", "Partitions without an assigned leader.", nil, nil),
		clusterUnderReplicated:          prometheus.NewDesc("cursus_cluster_under_replicated_partitions", "Partitions whose in-sync replica count is below their replica count.", nil, nil),
		topicMaterializationPending:     prometheus.NewDesc("cursus_cluster_topic_materializations_pending", "Node-local topic materialization operations waiting to converge.", []string{"operation"}, nil),
		topicMaterializationAttempts:    prometheus.NewDesc("cursus_cluster_topic_materialization_attempts_total", "Node-local topic materialization attempts by operation and result.", []string{"operation", "result"}, nil),
		topicMaterializationOldest:      prometheus.NewDesc("cursus_cluster_topic_materialization_oldest_pending_seconds", "Age of the oldest pending node-local topic materialization.", nil, nil),
		partitionReplicas:               prometheus.NewDesc("cursus_cluster_partition_replicas", "Configured replica count for a partition.", []string{"topic", "partition"}, nil),
		partitionInSync:                 prometheus.NewDesc("cursus_cluster_partition_in_sync_replicas", "In-sync replica count for a partition.", []string{"topic", "partition"}, nil),
		partitionLeaderEpoch:            prometheus.NewDesc("cursus_cluster_partition_leader_epoch", "Current partition leader epoch.", []string{"topic", "partition"}, nil),
		partitionLeader:                 prometheus.NewDesc("cursus_cluster_partition_leader", "Current partition leader identity.", []string{"topic", "partition", "broker_id"}, nil),
		isrCatchupProofs:                prometheus.NewDesc("cursus_cluster_isr_catchup_proofs_total", "ISR catch-up proofs by outcome and bounded reason.", []string{"outcome", "reason"}, nil),
		transactionRecovery:             prometheus.NewDesc("cursus_transaction_recovery_ready", "Whether transaction state recovery completed before serving.", nil, nil),
		transactionStates:               prometheus.NewDesc("cursus_transactions", "Transactions retained by coordinator state.", []string{"state"}, nil),
		transactionExpired:              prometheus.NewDesc("cursus_transactions_expired", "Expired transaction identities awaiting replacement or compaction.", nil, nil),
		transactionOldestActive:         prometheus.NewDesc("cursus_transaction_oldest_active_seconds", "Age of the oldest open or committing transaction.", nil, nil),
		transactionRetainedBytes:        prometheus.NewDesc("cursus_transaction_retained_bytes", "Conservative admission charge for retained transaction state; not process RSS.", nil, nil),
		requestInFlight:                 prometheus.NewDesc("cursus_request_inflight", "Admitted requests holding a reservation.", []string{"listener"}, nil),
		requestInFlightLimit:            prometheus.NewDesc("cursus_request_inflight_limit", "Maximum admitted requests.", []string{"listener"}, nil),
		requestBytes:                    prometheus.NewDesc("cursus_request_reserved_bytes", "Encoded plus decoded request bytes reserved; not process RSS.", []string{"listener"}, nil),
		requestByteLimit:                prometheus.NewDesc("cursus_request_byte_limit", "Request byte reservation budget.", []string{"listener"}, nil),
		requestRejected:                 prometheus.NewDesc("cursus_request_rejected_total", "Requests rejected before payload allocation due to admission limits.", []string{"listener"}, nil),
		observationFailures:             make(map[observationFailureKey]uint64),
	}
	if len(transactions) > 0 {
		c.transactions = transactions[0]
	}
	c.descriptors = []*prometheus.Desc{
		c.ready, c.topicCount, c.metadataLoadFailure, c.metadataRestoredTopics, c.metadataOrphanTopics,
		c.metadataDurabilityWarning, c.metadataDurabilityWarningsTotal,
		c.partitionCount, c.logStart, c.logEnd, c.highWatermark,
		c.groupMembers, c.groupState, c.groupCoordinatorUp, c.groupLastActivity,
		c.groupLastRebalance, c.groupObservationFailures,
		c.groupGeneration, c.groupAssignments, c.groupCommittedOffset,
		c.groupLag, c.groupOffsetOutOfRange,
		c.consumerMetadataRecovery, c.consumerMetadataRestoredGroups, c.consumerMetadataRestoredOffsets,
		c.consumerMetadataReplayedRecords, c.consumerMetadataOrphanRecords, c.consumerMetadataCorruptRecords,
		c.activeStreams, c.storageHandlers,
		c.storageSegments, c.storageBytes, c.storagePendingWrites, c.storageActiveReaders,
		c.storageStatFailures, c.storageSegmentCacheEntries, c.storageSegmentCacheHits,
		c.storageSegmentCacheMisses, c.storageSegmentCacheEvictions,
		c.wireProtocolFailures, c.wireDecompressionRejections,
		c.distributionEnabled, c.clusterBrokers, c.clusterHasLeader,
		c.clusterIsLeader, c.clusterOffline, c.clusterUnderReplicated, c.topicMaterializationPending,
		c.topicMaterializationAttempts, c.topicMaterializationOldest, c.partitionReplicas,
		c.partitionInSync, c.partitionLeaderEpoch, c.partitionLeader, c.isrCatchupProofs,
		c.transactionRecovery, c.transactionStates, c.transactionExpired, c.transactionOldestActive,
		c.transactionRetainedBytes,
		c.requestInFlight, c.requestInFlightLimit, c.requestBytes, c.requestByteLimit, c.requestRejected,
	}
	return c
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, descriptor := range c.descriptors {
		ch <- descriptor
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ready := false
	if c.readiness != nil {
		ready = c.readiness.IsReady()
	}
	ch <- gauge(c.ready, boolValue(ready))

	topicState := topic.RuntimeSnapshot{}
	if c.topics != nil {
		topicState = c.topics.RuntimeSnapshot()
	}
	ch <- gauge(c.topicCount, float64(topicState.TopicCount))
	ch <- gauge(c.metadataLoadFailure, boolValue(topicState.MetadataLoadFailure != ""))
	ch <- gauge(c.metadataRestoredTopics, float64(topicState.MetadataRestoredTopicCount))
	ch <- gauge(c.metadataOrphanTopics, float64(topicState.MetadataOrphanTopicCount))
	ch <- gauge(c.metadataDurabilityWarning, boolValue(topicState.MetadataDurabilityWarning != ""))
	ch <- prometheus.MustNewConstMetric(c.metadataDurabilityWarningsTotal, prometheus.CounterValue, float64(topicState.MetadataDurabilityWarningsTotal))
	ch <- gauge(c.partitionCount, float64(len(topicState.Partitions)))
	partitionState := make(map[string]topic.PartitionRuntimeSnapshot, len(topicState.Partitions))
	partitionKeysByTopic := make(map[string][]string, topicState.TopicCount)
	for _, partition := range topicState.Partitions {
		partitionLabel := strconv.Itoa(partition.Partition)
		key := partitionKey(partition.Topic, partition.Partition)
		partitionState[key] = partition
		partitionKeysByTopic[partition.Topic] = append(partitionKeysByTopic[partition.Topic], key)
		ch <- gauge(c.logStart, float64(partition.LogStart), partition.Topic, partitionLabel)
		ch <- gauge(c.logEnd, float64(partition.LogEnd), partition.Topic, partitionLabel)
		ch <- gauge(c.highWatermark, float64(partition.HighWatermark), partition.Topic, partitionLabel)
	}
	c.collectGroupLifecycle(ch, partitionKeysByTopic)
	c.collectGroups(ch, partitionState, partitionKeysByTopic)
	if recoverySource, ok := c.groups.(interface {
		RecoverySnapshot() coordinator.ConsumerMetadataRecoveryStatus
	}); ok {
		status := recoverySource.RecoverySnapshot()
		phase := status.Phase
		if phase == "" {
			phase = "unknown"
		}
		ch <- gauge(c.consumerMetadataRecovery, boolValue(status.Ready && status.Failure == ""), phase)
		ch <- gauge(c.consumerMetadataRestoredGroups, float64(status.RestoredGroups))
		ch <- gauge(c.consumerMetadataRestoredOffsets, float64(status.RestoredOffsets))
		ch <- gauge(c.consumerMetadataReplayedRecords, float64(status.ReplayedRecords))
		ch <- gauge(c.consumerMetadataOrphanRecords, float64(status.OrphanRecords))
		ch <- gauge(c.consumerMetadataCorruptRecords, float64(status.CorruptRecords))
	}

	activeStreams := 0
	if c.streams != nil {
		activeStreams = c.streams.ActiveCount()
	}
	ch <- gauge(c.activeStreams, float64(activeStreams))
	c.collectStorage(ch)
	c.collectWire(ch)
	c.collectCluster(ch)
	c.collectTransactions(ch)
	for listener, budget := range c.requestBudgets {
		state := budget.Snapshot()
		ch <- gauge(c.requestInFlight, float64(state.Frames), listener)
		ch <- gauge(c.requestInFlightLimit, float64(state.MaxFrames), listener)
		ch <- gauge(c.requestBytes, float64(state.Bytes), listener)
		ch <- gauge(c.requestByteLimit, float64(state.MaxBytes), listener)
		ch <- prometheus.MustNewConstMetric(c.requestRejected, prometheus.CounterValue, float64(state.Rejected), listener)
	}
}

// WithRequestBudgets configures admission metrics before collector registration.
func (c *Collector) WithRequestBudgets(client, internal *wire.FrameBudget) *Collector {
	c.requestBudgets = map[string]*wire.FrameBudget{"client": client, "internal": internal}
	return c
}

func (c *Collector) collectTransactions(ch chan<- prometheus.Metric) {
	state := transaction.RuntimeSnapshot{}
	if c.transactions != nil {
		state = c.transactions.RuntimeSnapshot()
	}
	ch <- gauge(c.transactionRecovery, boolValue(state.RecoveryReady))
	for _, transactionState := range []transaction.State{transaction.StateOpen, transaction.StateCommitting, transaction.StateCommitted, transaction.StateAborted} {
		ch <- gauge(c.transactionStates, float64(state.ByState[transactionState]), string(transactionState))
	}
	ch <- gauge(c.transactionExpired, float64(state.Expired))
	ch <- gauge(c.transactionOldestActive, state.OldestActiveAgeSeconds)
	ch <- gauge(c.transactionRetainedBytes, float64(state.RetainedBytes))
}
func (c *Collector) collectGroups(
	ch chan<- prometheus.Metric,
	partitions map[string]topic.PartitionRuntimeSnapshot,
	partitionKeysByTopic map[string][]string,
) {
	if c.groups == nil {
		return
	}
	groups := c.groups.ExportState()
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		state := groups[name]
		if state == nil {
			continue
		}
		assignments := 0
		for _, memberAssignments := range state.Members {
			assignments += len(memberAssignments)
		}
		ch <- gauge(c.groupGeneration, float64(state.Generation), name, state.TopicName)
		ch <- gauge(c.groupAssignments, float64(assignments), name, state.TopicName)

		positions := make(map[string]uint64)
		if !strings.ContainsAny(state.TopicName, "*?") {
			for _, key := range partitionKeysByTopic[state.TopicName] {
				positions[key] = 0
			}
		}
		for topicName, offsets := range state.Offsets {
			for partition, offset := range offsets {
				positions[partitionKey(topicName, partition)] = offset
			}
		}

		keys := make([]string, 0, len(positions))
		for key := range positions {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			partition, exists := partitions[key]
			if !exists {
				continue
			}
			committed := positions[key]
			lag := uint64(0)
			if partition.HighWatermark > committed {
				lag = partition.HighWatermark - committed
			}
			outOfRange := committed < partition.LogStart || committed > partition.HighWatermark
			partitionLabel := strconv.Itoa(partition.Partition)
			ch <- gauge(c.groupCommittedOffset, float64(committed), name, partition.Topic, partitionLabel)
			ch <- gauge(c.groupLag, float64(lag), name, partition.Topic, partitionLabel)
			ch <- gauge(c.groupOffsetOutOfRange, boolValue(outOfRange), name, partition.Topic, partitionLabel)
		}
	}
}

func (c *Collector) collectGroupLifecycle(ch chan<- prometheus.Metric, partitionKeysByTopic map[string][]string) {
	if c.groups == nil {
		return
	}

	observations := c.consumerGroupObservations()
	failed := make([]observationFailureKey, 0)
	active := make(map[observationGroupKey]struct{}, len(observations))
	for _, observation := range observations {
		active[observationGroupKey{topic: observation.TopicName, group: observation.GroupName}] = struct{}{}

		failureReason := boundedObservationFailureReason(observation.ObservationError)
		if failureReason == "" && !topicObserved(observation.TopicName, partitionKeysByTopic) {
			failureReason = coordinator.ObservationFailureTopicLookup
		}
		if failureReason != "" {
			failed = append(failed, observationFailureKey{
				topic: observation.TopicName, group: observation.GroupName, reason: failureReason,
			})
		}

		ch <- gauge(c.groupCoordinatorUp, boolValue(observation.CoordinatorUp), observation.TopicName, observation.GroupName)
		if !observation.CoordinatorUp || failureReason != "" {
			continue
		}

		ch <- gauge(c.groupMembers, float64(observation.MemberCount), observation.TopicName, observation.GroupName)
		for _, state := range []string{coordinator.ConsumerGroupStateStable, coordinator.ConsumerGroupStateEmpty} {
			ch <- gauge(c.groupState, boolValue(observation.State == state), observation.TopicName, observation.GroupName, state)
		}
		ch <- gauge(c.groupLastActivity, timestampSeconds(observation.LastActivity), observation.TopicName, observation.GroupName)
		ch <- gauge(c.groupLastRebalance, timestampSeconds(observation.LastRebalance), observation.TopicName, observation.GroupName)
	}
	c.collectObservationFailureCounters(ch, active, failed)
}

func (c *Collector) consumerGroupObservations() []coordinator.ConsumerGroupObservation {
	if source, ok := c.groups.(groupLifecycleSource); ok {
		return source.ObserveConsumerGroups()
	}

	groups := c.groups.ExportState()
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	observations := make([]coordinator.ConsumerGroupObservation, 0, len(names))
	for _, name := range names {
		state := groups[name]
		if state == nil || state.Deleted {
			continue
		}
		groupState := coordinator.ConsumerGroupStateStable
		if len(state.Members) == 0 {
			groupState = coordinator.ConsumerGroupStateEmpty
		}
		observations = append(observations, coordinator.ConsumerGroupObservation{
			TopicName:     state.TopicName,
			GroupName:     name,
			MemberCount:   len(state.Members),
			State:         groupState,
			LastActivity:  state.LastActivity,
			LastRebalance: state.LastRebalance,
			CoordinatorUp: true,
		})
	}
	return observations
}

func (c *Collector) collectObservationFailureCounters(
	ch chan<- prometheus.Metric,
	active map[observationGroupKey]struct{},
	failed []observationFailureKey,
) {
	c.observationMu.Lock()
	for key := range c.observationFailures {
		if _, ok := active[observationGroupKey{topic: key.topic, group: key.group}]; !ok {
			delete(c.observationFailures, key)
		}
	}
	for _, key := range failed {
		if _, ok := active[observationGroupKey{topic: key.topic, group: key.group}]; ok {
			c.observationFailures[key]++
		}
	}
	samples := make([]observationFailureSample, 0, len(c.observationFailures))
	for key, value := range c.observationFailures {
		samples = append(samples, observationFailureSample{key: key, value: value})
	}
	c.observationMu.Unlock()

	sort.Slice(samples, func(i, j int) bool {
		if samples[i].key.topic != samples[j].key.topic {
			return samples[i].key.topic < samples[j].key.topic
		}
		if samples[i].key.group != samples[j].key.group {
			return samples[i].key.group < samples[j].key.group
		}
		return samples[i].key.reason < samples[j].key.reason
	})

	for _, sample := range samples {
		key := sample.key
		ch <- counter(c.groupObservationFailures, float64(sample.value), key.topic, key.group, key.reason)
	}
}

func topicObserved(topicName string, partitionKeysByTopic map[string][]string) bool {
	if topicName == "" || strings.ContainsAny(topicName, "*?") {
		return true
	}
	_, ok := partitionKeysByTopic[topicName]
	return ok
}

func boundedObservationFailureReason(reason string) string {
	switch reason {
	case "":
		return ""
	case coordinator.ObservationFailureCoordinatorLookup,
		coordinator.ObservationFailureGroupLookup,
		coordinator.ObservationFailureTopicLookup:
		return reason
	default:
		return coordinator.ObservationFailureCoordinatorLookup
	}
}

func timestampSeconds(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix())
}

func (c *Collector) collectStorage(ch chan<- prometheus.Metric) {
	state := disk.RuntimeSnapshot{}
	if c.disk != nil {
		state = c.disk.RuntimeSnapshot()
	}
	ch <- gauge(c.storageHandlers, float64(state.Handlers))
	ch <- gauge(c.storageSegments, float64(state.Segments))
	ch <- gauge(c.storageBytes, float64(state.Bytes))
	ch <- gauge(c.storagePendingWrites, float64(state.PendingWrites))
	ch <- gauge(c.storageActiveReaders, float64(state.ActiveReaders))
	ch <- gauge(c.storageStatFailures, float64(state.StatFailures))
	ch <- gauge(c.storageSegmentCacheEntries, float64(state.SegmentCacheEntries))
	ch <- gauge(c.storageSegmentCacheHits, float64(state.SegmentCacheHits))
	ch <- gauge(c.storageSegmentCacheMisses, float64(state.SegmentCacheMisses))
	ch <- gauge(c.storageSegmentCacheEvictions, float64(state.SegmentCacheEvictions))
}

func (c *Collector) collectWire(ch chan<- prometheus.Metric) {
	state := wire.RuntimeMetrics()
	for _, reason := range sortedCounterKeys(state.ProtocolFailures) {
		ch <- counter(c.wireProtocolFailures, float64(state.ProtocolFailures[reason]), reason)
	}
	for _, reason := range sortedCounterKeys(state.DecompressionRejections) {
		ch <- counter(c.wireDecompressionRejections, float64(state.DecompressionRejections[reason]), reason)
	}
}

func (c *Collector) collectCluster(ch chan<- prometheus.Metric) {
	state := clustercontroller.RuntimeSnapshot{}
	if c.cluster != nil {
		state = c.cluster.RuntimeSnapshot()
	}
	ch <- gauge(c.distributionEnabled, boolValue(state.Enabled))
	ch <- gauge(c.clusterBrokers, float64(state.BrokerCount))
	ch <- gauge(c.clusterHasLeader, boolValue(state.HasLeader))
	ch <- gauge(c.clusterIsLeader, boolValue(state.IsLeader))
	ch <- gauge(c.clusterOffline, float64(state.Offline))
	ch <- gauge(c.clusterUnderReplicated, float64(state.UnderReplicated))
	for _, metric := range replication.ISRProofMetrics() {
		ch <- counter(c.isrCatchupProofs, float64(metric.Count), metric.Outcome, metric.Reason)
	}
	for _, operation := range []string{"create", "restore", "delete"} {
		ch <- gauge(c.topicMaterializationPending, float64(state.TopicMaterializationsPending[operation]), operation)
		attempts := state.TopicMaterializationAttempts[operation]
		ch <- counter(c.topicMaterializationAttempts, float64(attempts.Success), operation, "success")
		ch <- counter(c.topicMaterializationAttempts, float64(attempts.Failure), operation, "failure")
	}
	ch <- gauge(c.topicMaterializationOldest, state.TopicMaterializationOldestPending)
	for _, partition := range state.PartitionDetails {
		partitionLabel := strconv.Itoa(partition.Partition)
		ch <- gauge(c.partitionReplicas, float64(partition.Replicas), partition.Topic, partitionLabel)
		ch <- gauge(c.partitionInSync, float64(partition.InSync), partition.Topic, partitionLabel)
		ch <- gauge(c.partitionLeaderEpoch, float64(partition.LeaderEpoch), partition.Topic, partitionLabel)
		if partition.Leader != "" {
			ch <- gauge(c.partitionLeader, 1, partition.Topic, partitionLabel, partition.Leader)
		}
	}
}

func sortedCounterKeys(counters map[string]uint64) []string {
	keys := make([]string, 0, len(counters))
	for key := range counters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func partitionKey(topicName string, partition int) string {
	return fmt.Sprintf("%s\x00%d", topicName, partition)
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func counter(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labels...)
}

func gauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}
