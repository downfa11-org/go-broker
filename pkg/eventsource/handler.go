package eventsource

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

// Handler processes event sourcing commands (APPEND_STREAM, READ_STREAM, etc.).
type Handler struct {
	tm *topic.TopicManager

	mu          sync.RWMutex
	indexSyncMu sync.Mutex
	closed      bool
	wg          sync.WaitGroup
	indexes     map[string]*StreamIndex   // key: "topic:partition"
	indexedHWM  map[string]uint64         // committed log tail represented by each index
	snapshots   map[string]*SnapshotStore // key: "topic:partition"
}

type AppendOptions struct {
	LeaderAppend bool
	AfterAppend  func(topic string, partition int, msg types.Message) error
	AfterCommit  func(topic string, partition int, hwm uint64) error
}

type AppendResult struct {
	Topic     string
	Key       string
	Version   uint64
	Offset    uint64
	Partition int
	Message   types.Message
}

type SnapshotResult struct {
	Topic          string `json:"topic"`
	Key            string `json:"key"`
	Version        uint64 `json:"version"`
	Partition      int    `json:"partition"`
	Payload        string `json:"payload"`
	LifecycleEpoch uint64 `json:"lifecycle_epoch,omitempty"`
}

// NewHandler creates a new event sourcing command handler.
func NewHandler(tm *topic.TopicManager) *Handler {
	return &Handler{
		tm:         tm,
		indexes:    make(map[string]*StreamIndex),
		indexedHWM: make(map[string]uint64),
		snapshots:  make(map[string]*SnapshotStore),
	}
}

// getIndex returns the StreamIndex for the given topic and partition, creating it lazily.
func (h *Handler) getIndex(topicName string, partitionID int) (*StreamIndex, error) {
	key := topicName + ":" + strconv.Itoa(partitionID)

	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return nil, fmt.Errorf("handler is closed")
	}
	idx, ok := h.indexes[key]
	h.mu.RUnlock()
	if ok {
		return idx, idx.recoveryError()
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Double-check after acquiring write lock.
	if idx, ok := h.indexes[key]; ok {
		return idx, idx.recoveryError()
	}

	dir := h.tm.GetLogDir(topicName, partitionID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create dir for stream index %s:%d: %w", topicName, partitionID, err)
	}
	idx, err := NewStreamIndex(dir, partitionID)
	if err != nil {
		return nil, fmt.Errorf("open stream index for %s:%d: %w", topicName, partitionID, err)
	}
	indexedThrough, err := h.recoverIndexFromLog(topicName, partitionID, idx)
	if err != nil {
		_ = idx.Close()
		return nil, err
	}
	t := h.tm.GetTopic(topicName)
	if t == nil {
		_ = idx.Close()
		return nil, fmt.Errorf("topic %s not found during stream index recovery", topicName)
	}
	h.indexes[key] = idx
	h.indexedHWM[key] = indexedThrough
	return idx, nil
}

// getSnapshot returns the SnapshotStore for the given topic and partition, creating it lazily.
func (h *Handler) getSnapshot(topicName string, partitionID int) (*SnapshotStore, error) {
	key := topicName + ":" + strconv.Itoa(partitionID)

	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return nil, fmt.Errorf("handler is closed")
	}
	ss, ok := h.snapshots[key]
	h.mu.RUnlock()
	if ok {
		return ss, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if ss, ok := h.snapshots[key]; ok {
		return ss, nil
	}

	dir := h.tm.GetLogDir(topicName, partitionID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create dir for snapshot store %s:%d: %w", topicName, partitionID, err)
	}
	ss, err := NewSnapshotStore(dir, partitionID)
	if err != nil {
		return nil, fmt.Errorf("open snapshot store for %s:%d: %w", topicName, partitionID, err)
	}
	h.snapshots[key] = ss
	return ss, nil
}

// PrepareCommittedIndex captures the currently indexed committed tail before a
// follower advances its HWM.
func (h *Handler) PrepareCommittedIndex(topicName string, partitionID int) error {
	_, err := h.getIndex(topicName, partitionID)
	return err
}

// IndexCommittedToHWM advances the derived stream index only through records
// visible below the partition's stable committed boundary.
func (h *Handler) IndexCommittedToHWM(topicName string, partitionID int, targetHWM uint64) error {
	h.indexSyncMu.Lock()
	defer h.indexSyncMu.Unlock()

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return err
	}
	t := h.tm.GetTopic(topicName)
	if t == nil || !t.IsEventSourcing {
		return nil
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return fmt.Errorf("partition lookup for committed index topic=%s partition=%d: %w", topicName, partitionID, err)
	}

	key := topicName + ":" + strconv.Itoa(partitionID)
	h.mu.RLock()
	start := h.indexedHWM[key]
	h.mu.RUnlock()

	scanEnd := targetHWM
	if stable := p.LastStableOffset(); stable < scanEnd {
		scanEnd = stable
	}
	if scanEnd <= start {
		return nil
	}

	const batchSize = 256
	for offset := start; offset < scanEnd; {
		msgs, err := p.ReadCommitted(offset, batchSize)
		if err != nil {
			return fmt.Errorf("read committed stream index range offset=%d: %w", offset, err)
		}
		if len(msgs) == 0 {
			break
		}

		bounded := msgs[:0]
		for _, msg := range msgs {
			if msg.Offset >= scanEnd {
				break
			}
			bounded = append(bounded, msg)
		}
		if len(bounded) == 0 {
			break
		}
		if err := h.indexMessages(idx, bounded); err != nil {
			return err
		}
		next := bounded[len(bounded)-1].Offset + 1
		if next <= offset {
			return fmt.Errorf("stream index scan did not advance from offset %d", offset)
		}
		offset = next
	}

	h.mu.Lock()
	if h.indexedHWM[key] < scanEnd {
		h.indexedHWM[key] = scanEnd
	}
	h.mu.Unlock()
	return nil
}

func (h *Handler) RecoverIndexFromLog(topicName string, partitionID int, idx *StreamIndex) error {
	_, err := h.recoverIndexFromLog(topicName, partitionID, idx)
	return err
}

func (h *Handler) recoverIndexFromLog(topicName string, partitionID int, idx *StreamIndex) (indexedThrough uint64, recoveryErr error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	defer func() { idx.recoveryErr = recoveryErr }()
	t := h.tm.GetTopic(topicName)
	if t == nil || !t.IsEventSourcing {
		return 0, fmt.Errorf("event-sourcing topic %s not found during index recovery", topicName)
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return 0, fmt.Errorf("partition lookup for index recovery topic=%s partition=%d: %w", topicName, partitionID, err)
	}
	if earliest := p.OffsetRange().Earliest; earliest != 0 {
		return 0, fmt.Errorf("event history unavailable topic=%s partition=%d earliest=%d: restore the complete log before serving streams", topicName, partitionID, earliest)
	}
	if err := idx.resetLocked(); err != nil {
		return 0, fmt.Errorf("reset stream index topic=%s partition=%d: %w", topicName, partitionID, err)
	}
	latest := p.LastStableOffset()
	const batchSize = 256
	for offset := uint64(0); offset < latest; {
		msgs, err := p.ReadCommitted(offset, batchSize)
		if err != nil {
			return 0, fmt.Errorf("recover stream index from log offset=%d: %w", offset, err)
		}
		if len(msgs) == 0 {
			break
		}
		bounded := msgs[:0]
		for _, msg := range msgs {
			if msg.Offset >= latest {
				break
			}
			bounded = append(bounded, msg)
		}
		if len(bounded) == 0 {
			break
		}
		if err := h.indexMessagesLocked(idx, bounded); err != nil {
			return 0, err
		}
		next := bounded[len(bounded)-1].Offset + 1
		if next <= offset {
			return 0, fmt.Errorf("stream index recovery did not advance from offset %d", offset)
		}
		offset = next
	}
	return latest, nil
}

func (h *Handler) indexMessages(idx *StreamIndex, messages []types.Message) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.recoveryErr != nil {
		return idx.recoveryErr
	}
	return h.indexMessagesLocked(idx, messages)
}

func (h *Handler) indexMessagesLocked(idx *StreamIndex, messages []types.Message) error {
	for _, msg := range messages {
		if msg.Key == "" || msg.AggregateVersion == 0 {
			continue
		}
		var current uint64
		if state := idx.states[msg.Key]; state != nil {
			current = state.currentVersion
		}
		switch {
		case msg.AggregateVersion <= current:
			continue
		case msg.AggregateVersion != current+1:
			return fmt.Errorf("stream index gap key=%s current=%d next=%d", msg.Key, current, msg.AggregateVersion)
		}
		if err := idx.appendLocked(msg.Key, msg.AggregateVersion, msg.Offset, 0); err != nil {
			return err
		}
	}
	return nil
}

// HandleAppendStream processes:
//
//	APPEND_STREAM topic=<name> key=<aggregate_key> version=<expected> event_type=<type> message=<payload>
func (h *Handler) HandleAppendStream(cmd string) string {
	result, errResp := h.AppendStream(cmd, AppendOptions{})
	if errResp != "" {
		return errResp
	}
	return result.Response()
}

func (r *AppendResult) Response() string {
	return fmt.Sprintf("OK version=%d offset=%d partition=%d", r.Version, r.Offset, r.Partition)
}

func (h *Handler) AppendStream(cmd string, opts AppendOptions) (*AppendResult, string) {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("APPEND_STREAM "):])

	topicName := args["topic"]
	if topicName == "" {
		return nil, "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return nil, "ERROR: missing_key"
	}
	versionStr := args["version"]
	if versionStr == "" {
		return nil, "ERROR: missing_version"
	}
	expectedVersion, err := strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		return nil, "ERROR: invalid_version"
	}
	payload, ok := args["message"]
	if !ok || payload == "" {
		return nil, "ERROR: missing_message"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return nil, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return nil, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	msg := types.Message{
		Key:              key,
		Payload:          payload,
		EventType:        args["event_type"],
		SchemaVersion:    1,
		AggregateVersion: expectedVersion,
		Metadata:         args["metadata"],
	}
	if svStr := args["schema_version"]; svStr != "" {
		sv, err := strconv.ParseUint(svStr, 10, 32)
		if err != nil {
			return nil, "ERROR: invalid_schema_version"
		}
		msg.SchemaVersion = uint32(sv)
	}

	partitionID := t.GetPartitionForMessage(msg)
	if partitionID < 0 {
		return nil, "ERROR: no_partitions_available"
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: partition_lookup_failed partition=%d reason=%q", partitionID, err.Error())
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: stream_index_failed partition=%d reason=%q", partitionID, err.Error())
	}

	var appendedOffset uint64
	var appendedMsg types.Message
	ok, current, err := idx.CheckEnqueueAndAppend(key, expectedVersion, func() (uint64, error) {
		if opts.LeaderAppend {
			batch := []types.Message{msg}
			if err := p.EnqueueBatchLeader(batch); err != nil {
				return 0, err
			}
			appendedMsg = batch[0]
			appendedOffset = batch[0].Offset
		} else {
			if err := p.EnqueueSync(msg); err != nil {
				return 0, err
			}
			appendedMsg = msg
			appendedOffset = p.NextOffset() - 1
			appendedMsg.Offset = appendedOffset
		}

		if opts.LeaderAppend {
			p.FlushDisk()
		}
		if opts.AfterAppend != nil {
			if err := opts.AfterAppend(topicName, partitionID, appendedMsg); err != nil {
				return 0, err
			}
		}

		if opts.LeaderAppend {
			hwm := p.NextOffset()
			if opts.AfterCommit != nil {
				if err := opts.AfterCommit(topicName, partitionID, hwm); err != nil {
					return 0, err
				}
			} else {
				p.AdvanceHWM()
			}
		}
		return appendedOffset, nil
	})
	recoveredCommit := false
	if err != nil {
		if recoveryErr := h.RecoverIndexFromLog(topicName, partitionID, idx); recoveryErr != nil {
			return nil, fmt.Sprintf("ERROR: append_stream_failed reason=%q recovery_reason=%q", err.Error(), recoveryErr.Error())
		}
		if idx.GetVersion(key) != expectedVersion {
			return nil, fmt.Sprintf("ERROR: append_stream_failed reason=%q", err.Error())
		}
		// The log and HWM commit succeeded even though the derived index append
		// failed. Recovery found the committed event, so report its success.
		recoveredCommit = true
	}
	if !ok && !recoveredCommit {
		return nil, fmt.Sprintf("ERROR: version_conflict current=%d expected=%d", current, expectedVersion)
	}

	indexKey := topicName + ":" + strconv.Itoa(partitionID)
	h.mu.Lock()
	if committed := p.GetHWM(); h.indexedHWM[indexKey] < committed {
		h.indexedHWM[indexKey] = committed
	}
	h.mu.Unlock()

	return &AppendResult{Topic: topicName, Key: key, Version: expectedVersion, Offset: appendedOffset, Partition: partitionID, Message: appendedMsg}, ""
}

// HandleReadStream writes event data directly to conn.
// Protocol: two length-prefixed frames — JSON envelope + binary batch.
func (h *Handler) HandleReadStream(cmd string, conn net.Conn) {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("READ_STREAM "):])
	limit := wire.DefaultStreamPageEvents
	paged := args["limit"] != ""
	if paged {
		parsed, err := strconv.Atoi(args["limit"])
		if err != nil || parsed < 1 || parsed > wire.MaxStreamPageEvents {
			writeError(conn, "invalid_stream_page_limit")
			return
		}
		limit = parsed
	}
	throughVersion, err := parseOptionalStreamUint(args, "through_version")
	if err != nil {
		writeError(conn, err.Error())
		return
	}
	requestedEpoch, err := parseOptionalStreamUint(args, "lifecycle_epoch")
	if err != nil {
		writeError(conn, err.Error())
		return
	}

	topicName := args["topic"]
	if topicName == "" {
		writeError(conn, "missing_topic")
		return
	}
	key := args["key"]
	if key == "" {
		writeError(conn, "missing_key")
		return
	}
	fromVersion := uint64(1)
	if fv := args["from_version"]; fv != "" {
		v, err := strconv.ParseUint(fv, 10, 64)
		if err != nil || v == 0 {
			writeError(conn, "invalid_from_version")
			return
		}
		fromVersion = v
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		writeError(conn, fmt.Sprintf("topic_not_found topic=%s", topicName))
		return
	}
	if !t.IsEventSourcing {
		writeError(conn, fmt.Sprintf("event_sourcing_not_enabled topic=%s", topicName))
		return
	}
	lifecycleEpoch := t.Definition().LifecycleEpoch
	if requestedEpoch != 0 && requestedEpoch != lifecycleEpoch {
		writeError(conn, "stream_lifecycle_changed")
		return
	}

	// Determine the partition for this key.
	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		writeError(conn, "no_partitions_available")
		return
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		writeError(conn, err.Error())
		return
	}

	var snap *SnapshotData
	if args["snapshot"] != "false" {
		if args["snapshot"] != "" && args["snapshot"] != "true" {
			writeError(conn, "invalid_snapshot_option")
			return
		}
		ss, err := h.getSnapshot(topicName, partitionID)
		if err != nil {
			writeError(conn, err.Error())
			return
		}
		snap, err = ss.Read(key)
		if err != nil {
			writeError(conn, fmt.Sprintf("snapshot_read_failed reason=%q", err.Error()))
			return
		}
	}

	actualFromVersion := fromVersion
	useSnapshot := snap != nil && snap.Version >= fromVersion && args["snapshot"] != "false" && (throughVersion == 0 || snap.Version <= throughVersion)
	if useSnapshot {
		// Start reading from the version after the snapshot.
		if snap.Version == ^uint64(0) {
			writeError(conn, "snapshot_version_overflow")
			return
		}
		actualFromVersion = snap.Version + 1
	}

	entries, streamVersion, hasMore, err := idx.LookupPage(key, actualFromVersion, throughVersion, limit)
	if err != nil {
		writeError(conn, fmt.Sprintf("index_lookup_failed reason=%q", err.Error()))
		return
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		writeError(conn, err.Error())
		return
	}

	if useSnapshot && snap.Version > streamVersion {
		writeError(conn, "snapshot_ahead_of_stream")
		return
	}
	msgs, byteLimited, err := readIndexedStreamPage(p, entries, key, topicName, partitionID, wire.MaxFramePayload)
	if err != nil {
		writeError(conn, err.Error())
		return
	}
	hasMore = hasMore || byteLimited
	if hasMore && !paged {
		writeError(conn, "stream_page_required reason=\"retry READ_STREAM with limit and from_version\"")
		return
	}

	// Build JSON envelope.
	envelope := struct {
		Status         string        `json:"status"`
		Topic          string        `json:"topic"`
		Key            string        `json:"key"`
		Partition      int           `json:"partition"`
		Count          int           `json:"count"`
		Snapshot       *SnapshotData `json:"snapshot,omitempty"`
		StreamVersion  uint64        `json:"stream_version"`
		NextVersion    uint64        `json:"next_version"`
		HasMore        bool          `json:"has_more"`
		LifecycleEpoch uint64        `json:"lifecycle_epoch"`
	}{
		Status:         "OK",
		Topic:          topicName,
		Key:            key,
		Partition:      partitionID,
		Count:          len(msgs),
		StreamVersion:  streamVersion,
		HasMore:        hasMore,
		LifecycleEpoch: lifecycleEpoch,
	}
	if hasMore {
		envelope.NextVersion = msgs[len(msgs)-1].AggregateVersion + 1
	}
	if useSnapshot {
		envelope.Snapshot = snap
	}
	batchData, err := util.EncodeBatchMessages(topicName, partitionID, "1", false, msgs)
	if err != nil {
		writeError(conn, fmt.Sprintf("encode_stream_failed reason=%q", err.Error()))
		return
	}

	envJSON, err := json.Marshal(envelope)
	if err != nil {
		writeError(conn, fmt.Sprintf("marshal envelope: %v", err))
		return
	}
	if len(envJSON) > wire.MaxFramePayload {
		writeError(conn, "stream_snapshot_too_large")
		return
	}

	// Frame 1: JSON envelope.
	if err := util.WriteWithLength(conn, envJSON); err != nil {
		_ = conn.Close()
		return
	}

	// Frame 2: binary batch.
	if err := util.WriteWithLength(conn, batchData); err != nil {
		_ = conn.Close()
	}
}

func parseOptionalStreamUint(args map[string]string, key string) (uint64, error) {
	if args[key] == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(args[key], 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("invalid_%s", key)
	}
	return value, nil
}

// HandleSaveSnapshot processes:
//
//	SAVE_SNAPSHOT topic=<name> key=<aggregate_key> version=<N> message=<json_payload>
func (h *Handler) HandleSaveSnapshot(cmd string) string {
	result, errResp := h.SaveSnapshot(cmd, nil)
	if errResp != "" {
		return errResp
	}
	return result.Response()
}

func (r *SnapshotResult) Response() string {
	return fmt.Sprintf("OK version=%d partition=%d", r.Version, r.Partition)
}

func (h *Handler) SaveSnapshot(cmd string, afterSave func(result SnapshotResult) error) (*SnapshotResult, string) {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("SAVE_SNAPSHOT "):])

	topicName := args["topic"]
	if topicName == "" {
		return nil, "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return nil, "ERROR: missing_key"
	}
	versionStr := args["version"]
	if versionStr == "" {
		return nil, "ERROR: missing_version"
	}
	version, err := strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		return nil, "ERROR: invalid_version"
	}
	payload := args["message"]
	if payload == "" {
		return nil, "ERROR: missing_message"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return nil, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return nil, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return nil, "ERROR: no_partitions_available"
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: stream_index_failed partition=%d reason=%q", partitionID, err.Error())
	}
	currentVersion := idx.GetVersion(key)
	if version > currentVersion {
		return nil, fmt.Sprintf("ERROR: snapshot_version_exceeds_stream version=%d current=%d", version, currentVersion)
	}

	result := SnapshotResult{Topic: topicName, Key: key, Version: version, Partition: partitionID, Payload: payload}
	if errResp := h.SaveSnapshotReplica(result); errResp != "" {
		return nil, errResp
	}
	if afterSave != nil {
		if err := afterSave(result); err != nil {
			return nil, fmt.Sprintf("ERROR: snapshot_replicate_failed reason=%q", err.Error())
		}
	}
	return &result, ""
}

func (h *Handler) SaveSnapshotReplica(result SnapshotResult) string {
	t := h.tm.GetTopic(result.Topic)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", result.Topic)
	}
	if !t.IsEventSourcing {
		return fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", result.Topic)
	}
	if _, err := t.GetPartition(result.Partition); err != nil {
		return fmt.Sprintf("ERROR: partition_lookup_failed partition=%d reason=%q", result.Partition, err.Error())
	}
	ss, err := h.getSnapshot(result.Topic, result.Partition)
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot_store_failed partition=%d reason=%q", result.Partition, err.Error())
	}
	if err := ss.Save(result.Key, result.Version, result.Payload); err != nil {
		return fmt.Sprintf("ERROR: snapshot_save_failed reason=%q", err.Error())
	}
	return ""
}

// ListSnapshots returns all latest snapshots for a topic partition.
func (h *Handler) ListSnapshots(topicName string, partitionID int) ([]SnapshotResult, string) {
	t := h.tm.GetTopic(topicName)
	if t == nil {
		return nil, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return nil, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}
	if _, err := t.GetPartition(partitionID); err != nil {
		return nil, fmt.Sprintf("ERROR: partition_lookup_failed partition=%d reason=%q", partitionID, err.Error())
	}
	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: snapshot_store_failed partition=%d reason=%q", partitionID, err.Error())
	}
	records, err := ss.List()
	if err != nil {
		return nil, fmt.Sprintf("ERROR: snapshot_list_failed reason=%q", err.Error())
	}
	result := make([]SnapshotResult, 0, len(records))
	for _, rec := range records {
		result = append(result, SnapshotResult{Topic: topicName, Key: rec.Key, Version: rec.Version, Partition: partitionID, Payload: rec.Payload})
	}
	return result, ""
}

// FetchSnapshot returns the latest snapshot for a topic partition and aggregate key.
func (h *Handler) FetchSnapshot(topicName string, partitionID int, key string) (*SnapshotResult, string) {
	if key == "" {
		return nil, "ERROR: missing_key"
	}
	snaps, errResp := h.ListSnapshots(topicName, partitionID)
	if errResp != "" {
		return nil, errResp
	}
	for _, snap := range snaps {
		if snap.Key == key {
			return &snap, ""
		}
	}
	return nil, ""
}

// HandleReadSnapshot processes:
//
//	READ_SNAPSHOT topic=<name> key=<aggregate_key>
func (h *Handler) HandleReadSnapshot(cmd string) string {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("READ_SNAPSHOT "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing_key"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return "ERROR: no_partitions_available"
	}

	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot_store_failed partition=%d reason=%q", partitionID, err.Error())
	}

	snap, err := ss.Read(key)
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot_read_failed reason=%q", err.Error())
	}
	if snap == nil {
		return "OK snapshot=null"
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_snapshot_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK snapshot=%s", string(data))
}

// HandleStreamVersion processes:
//
//	STREAM_VERSION topic=<name> key=<aggregate_key>
func (h *Handler) HandleStreamVersion(cmd string) string {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("STREAM_VERSION "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing_key"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return "ERROR: no_partitions_available"
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: stream_index_failed partition=%d reason=%q", partitionID, err.Error())
	}

	version := idx.GetVersion(key)
	return fmt.Sprintf("OK version=%d", version)
}

// DeleteTopic closes cached stream indexes and snapshot stores for a deleted topic.
func (h *Handler) DeleteTopic(topicName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	prefix := topicName + ":"
	var firstErr error
	for key, idx := range h.indexes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if err := idx.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close index %s: %w", key, err)
		}
		delete(h.indexes, key)
		delete(h.indexedHWM, key)
	}
	for key, ss := range h.snapshots {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if err := ss.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close snapshot store %s: %w", key, err)
		}
		delete(h.snapshots, key)
	}
	return firstErr
}

// Close closes all StreamIndex and SnapshotStore instances held by this handler.
// After Close, getIndex and getSnapshot will return errors.
func (h *Handler) Close() error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()

	h.wg.Wait()

	h.mu.Lock()
	defer h.mu.Unlock()

	var firstErr error
	for key, idx := range h.indexes {
		if err := idx.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close index %s: %w", key, err)
		}
	}
	for key, ss := range h.snapshots {
		if err := ss.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close snapshot store %s: %w", key, err)
		}
	}

	h.indexes = nil
	h.snapshots = nil
	return firstErr
}

// writeError writes the canonical textual error envelope used by the wire
// transport to produce a typed broker error.
func writeError(conn net.Conn, msg string) {
	msg = strings.TrimSpace(msg)
	if !strings.HasPrefix(msg, "ERROR:") {
		msg = "ERROR: " + msg
	}
	_ = util.WriteWithLength(conn, []byte(msg))
}

// parseKeyValueArgs parses "key=value" pairs from a command argument string.
// The "message" key receives all text after "message=" (preserving spaces).
func parseKeyValueArgs(argsStr string) map[string]string {
	result := make(map[string]string)

	messageIdx := strings.Index(argsStr, "message=")

	if messageIdx != -1 {
		beforeMessage := argsStr[:messageIdx]
		parts := strings.Fields(beforeMessage)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
		result["message"] = strings.TrimSpace(argsStr[messageIdx+8:])
	} else {
		parts := strings.Fields(argsStr)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
	}
	return result
}
