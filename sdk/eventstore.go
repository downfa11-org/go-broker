package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk/internal/transport"
)

// Event represents a domain event to be appended to a stream.
type Event struct {
	Type          string // e.g., "OrderCreated"
	SchemaVersion uint32 // default 1
	Payload       string // serialized event data
	Metadata      string // optional JSON metadata
}

// StreamEvent is an event read from a stream, with version and offset.
// StreamEvent metadata is populated from the broker batch wire format.
type StreamEvent struct {
	Version       uint64
	Offset        uint64
	Type          string
	SchemaVersion uint32
	Payload       string
	Metadata      string
}

// Snapshot holds a stored aggregate snapshot.
type Snapshot struct {
	Version uint64 `json:"version"`
	Payload string `json:"payload"`
}

// StreamData is the result of reading a stream.
type StreamData struct {
	Snapshot       *Snapshot
	Events         []StreamEvent
	StreamVersion  uint64
	NextVersion    uint64
	HasMore        bool
	LifecycleEpoch uint64
}

var ErrStreamPageRequired = errors.New("stream exceeds one page; use ReadStreamPage or WalkStream")

// AppendResult is returned after successfully appending an event.
type AppendResult struct {
	Version   uint64
	Offset    uint64
	Partition int
}

// EventStore provides event sourcing operations against a Cursus broker.
type EventStore struct {
	topic      string
	producerID string
	addr       string
	requestMu  sync.Mutex
	mu         sync.Mutex
	conn       net.Conn
}

// NewEventStore creates an EventStore for the given topic.
func NewEventStore(addr, topic, producerID string) *EventStore {
	return &EventStore{
		topic:      topic,
		producerID: producerID,
		addr:       addr,
	}
}

// getConn returns an existing or new TCP connection.
func (es *EventStore) getConn() (net.Conn, error) {
	if err := validateSDKTopicName(es.topic); err != nil {
		return nil, err
	}

	es.mu.Lock()
	if es.conn != nil {
		c := es.conn
		es.mu.Unlock()
		return c, nil
	}
	es.mu.Unlock()

	conn, err := transport.Dial(context.Background(), es.addr, transport.DialConfig{
		DialTimeout: 5 * time.Second, HandshakeTimeout: 5 * time.Second, Compression: "none",
	})
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", es.addr, err)
	}

	es.mu.Lock()
	if es.conn != nil {
		// Another goroutine connected while we were dialing.
		existing := es.conn
		es.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	es.conn = conn
	es.mu.Unlock()
	return conn, nil
}

// resetConn closes and clears the connection (for retry).
func (es *EventStore) resetConn() {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.conn != nil {
		_ = es.conn.Close()
		es.conn = nil
	}
}

// sendCommand sends a text command and returns the response string.
func (es *EventStore) sendCommand(cmd string) (string, error) {
	es.requestMu.Lock()
	defer es.requestMu.Unlock()

	conn, err := es.getConn()
	if err != nil {
		return "", err
	}

	data := []byte(cmd)
	if err := WriteWithLength(conn, data); err != nil {
		es.resetConn()
		return "", fmt.Errorf("write: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		es.resetConn()
		return "", fmt.Errorf("read: %w", err)
	}

	return string(resp), nil
}

// CreateTopic creates an event-sourcing-enabled topic if it doesn't exist.
func (es *EventStore) CreateTopic(partitions int) error {
	resp, err := es.sendCommand(fmt.Sprintf("CREATE topic=%s partitions=%d event_sourcing=true cleanup_policy=delete", es.topic, partitions))
	if err != nil {
		return err
	}
	respStr := strings.TrimSpace(resp)
	if !strings.HasPrefix(respStr, "OK") {
		return fmt.Errorf("unexpected create response: %s", respStr)
	}
	return nil
}

// Append appends an event to an aggregate stream with optimistic concurrency.
// expectedVersion is the current version of the aggregate (0 for new aggregates).
func (es *EventStore) Append(key string, expectedVersion uint64, event *Event) (*AppendResult, error) {
	sv := event.SchemaVersion
	if sv == 0 {
		sv = 1
	}

	nextVersion := expectedVersion + 1
	cmd := fmt.Sprintf("APPEND_STREAM topic=%s key=%s version=%d event_type=%s schema_version=%d producerId=%s",
		es.topic, key, nextVersion, event.Type, sv, es.producerID)
	if event.Metadata != "" {
		cmd += fmt.Sprintf(" metadata=%s", event.Metadata)
	}
	cmd += fmt.Sprintf(" message=%s", event.Payload)

	resp, err := es.sendCommand(cmd)
	if err != nil {
		return nil, err
	}

	result, err := parseAppendResponse(resp)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// parseAppendResponse parses "OK version=N offset=N partition=N" into AppendResult.
func parseAppendResponse(resp string) (*AppendResult, error) {
	respStr := strings.TrimSpace(resp)
	if !strings.HasPrefix(respStr, "OK") {
		return nil, fmt.Errorf("unexpected append response: %s", respStr)
	}

	result := &AppendResult{}
	hasVersion := false
	hasOffset := false
	hasPartition := false
	for _, p := range strings.Fields(respStr) {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "version":
			v, err := strconv.ParseUint(kv[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid append version in response: %s", respStr)
			}
			result.Version = v
			hasVersion = true
		case "offset":
			o, err := strconv.ParseUint(kv[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid append offset in response: %s", respStr)
			}
			result.Offset = o
			hasOffset = true
		case "partition":
			partition, err := strconv.Atoi(kv[1])
			if err != nil {
				return nil, fmt.Errorf("invalid append partition in response: %s", respStr)
			}
			result.Partition = partition
			hasPartition = true
		}
	}
	if !hasVersion || !hasOffset || !hasPartition {
		return nil, fmt.Errorf("missing append fields in response: %s", respStr)
	}
	return result, nil
}

// ReadStream reads a complete stream within one page, automatically using snapshots.
func (es *EventStore) ReadStream(key string) (*StreamData, error) {
	return es.ReadStreamFrom(key, 0)
}

// ReadStreamFrom reads events starting from a specific version.
// If fromVersion is 0, the broker auto-resolves using snapshots.
func (es *EventStore) ReadStreamFrom(key string, fromVersion uint64) (*StreamData, error) {
	page, err := es.readStreamPage(key, fromVersion, wire.DefaultStreamPageEvents, 0, 0, true)
	if err != nil {
		return nil, err
	}
	if page.HasMore {
		return nil, ErrStreamPageRequired
	}
	return page, nil
}

// ReadStreamPage returns one bounded page. Version zero permits a snapshot;
// positive versions read event history without skipping to a newer snapshot.
func (es *EventStore) ReadStreamPage(key string, fromVersion uint64, limit int) (*StreamData, error) {
	return es.readStreamPage(key, fromVersion, limit, 0, 0, fromVersion == 0)
}

// WalkStream visits bounded pages through the first page's stream version.
// A callback error stops replay; callbacks may use other EventStore operations.
func (es *EventStore) WalkStream(key string, fromVersion uint64, visit func(*StreamData) error) error {
	if visit == nil {
		return fmt.Errorf("stream visitor is required")
	}
	var through, epoch uint64
	for {
		page, err := es.readStreamPage(key, fromVersion, wire.DefaultStreamPageEvents, through, epoch, fromVersion == 0)
		if err != nil {
			return err
		}
		next, version, pageEpoch, hasMore := page.NextVersion, page.StreamVersion, page.LifecycleEpoch, page.HasMore
		if err := visit(page); err != nil {
			return err
		}
		if !hasMore {
			return nil
		}
		through, epoch, fromVersion = version, pageEpoch, next
	}
}

func (es *EventStore) readStreamPage(key string, fromVersion uint64, limit int, throughVersion, lifecycleEpoch uint64, useSnapshot bool) (_ *StreamData, readErr error) {
	es.requestMu.Lock()
	defer es.requestMu.Unlock()
	if limit < 1 || limit > wire.MaxStreamPageEvents || key == "" || strings.ContainsAny(key, " \t\r\n") {
		return nil, fmt.Errorf("invalid stream key or page limit")
	}

	cmd := fmt.Sprintf("READ_STREAM topic=%s key=%s limit=%d snapshot=%t", es.topic, key, limit, useSnapshot)
	if fromVersion > 0 {
		cmd += fmt.Sprintf(" from_version=%d", fromVersion)
	}
	if throughVersion > 0 {
		cmd += fmt.Sprintf(" through_version=%d", throughVersion)
	}
	if lifecycleEpoch > 0 {
		cmd += fmt.Sprintf(" lifecycle_epoch=%d", lifecycleEpoch)
	}

	conn, envData, err := es.readStreamEnvelope(cmd)
	if err != nil {
		return nil, err
	}
	defer func() {
		if readErr != nil {
			es.resetConn()
		} else {
			_ = conn.SetDeadline(time.Time{})
		}
	}()
	var envelope struct {
		Status         string    `json:"status"`
		Error          string    `json:"error"`
		Snapshot       *Snapshot `json:"snapshot"`
		Count          int       `json:"count"`
		Topic          string    `json:"topic"`
		Key            string    `json:"key"`
		Partition      int       `json:"partition"`
		StreamVersion  uint64    `json:"stream_version"`
		NextVersion    uint64    `json:"next_version"`
		HasMore        bool      `json:"has_more"`
		LifecycleEpoch uint64    `json:"lifecycle_epoch"`
	}
	if err := json.Unmarshal(envData, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	if envelope.Status == "ERROR" {
		if envelope.Error == "" {
			envelope.Error = "read stream failed"
		}
		return nil, fmt.Errorf("broker: %s", envelope.Error)
	}
	if envelope.Status != "OK" {
		return nil, fmt.Errorf("unexpected read stream status: %s", envelope.Status)
	}
	if envelope.Topic != es.topic || envelope.Key != key || envelope.Count < 0 || envelope.Count > limit || envelope.LifecycleEpoch == 0 ||
		(lifecycleEpoch != 0 && envelope.LifecycleEpoch != lifecycleEpoch) || (throughVersion != 0 && envelope.StreamVersion != throughVersion) {
		return nil, fmt.Errorf("invalid stream page identity, count, or boundary")
	}

	// Frame 2: Binary batch
	batchData, err := ReadWithLength(conn)
	if err != nil {
		es.resetConn()
		return nil, fmt.Errorf("read batch: %w", err)
	}

	result := &StreamData{
		Snapshot:       envelope.Snapshot,
		StreamVersion:  envelope.StreamVersion,
		NextVersion:    envelope.NextVersion,
		HasMore:        envelope.HasMore,
		LifecycleEpoch: envelope.LifecycleEpoch,
	}
	nextExpected := fromVersion
	if nextExpected == 0 {
		nextExpected = 1
	}
	if envelope.Snapshot != nil {
		if !useSnapshot || envelope.Snapshot.Version < nextExpected || envelope.Snapshot.Version > envelope.StreamVersion || envelope.Snapshot.Version == ^uint64(0) {
			return nil, fmt.Errorf("invalid stream snapshot version")
		}
		nextExpected = envelope.Snapshot.Version + 1
	}
	lastVersion := nextExpected - 1

	if len(batchData) > 0 {
		msgs, batchTopic, batchPartition, err := DecodeBatchMessages(batchData)
		if err != nil {
			return nil, fmt.Errorf("decode batch: %w", err)
		}
		if batchTopic != es.topic || batchPartition != envelope.Partition || len(msgs) != envelope.Count {
			return nil, fmt.Errorf("stream batch does not match envelope")
		}
		for _, m := range msgs {
			if m.Key != key || m.AggregateVersion != nextExpected || m.AggregateVersion > envelope.StreamVersion {
				return nil, fmt.Errorf("stream event identity or version mismatch")
			}
			lastVersion = m.AggregateVersion
			nextExpected++
			result.Events = append(result.Events, StreamEvent{
				Version:       m.AggregateVersion,
				Offset:        m.Offset,
				Type:          m.EventType,
				SchemaVersion: m.SchemaVersion,
				Payload:       m.Payload,
				Metadata:      m.Metadata,
			})
		}
	}
	if len(result.Events) != envelope.Count {
		return nil, fmt.Errorf("stream page count mismatch")
	}
	if envelope.HasMore {
		if len(result.Events) == 0 || lastVersion >= envelope.StreamVersion || envelope.NextVersion != nextExpected {
			return nil, fmt.Errorf("invalid stream continuation")
		}
	} else if envelope.NextVersion != 0 || lastVersion < envelope.StreamVersion {
		return nil, fmt.Errorf("incomplete terminal stream page")
	}

	return result, nil
}

func (es *EventStore) readStreamEnvelope(cmd string) (net.Conn, []byte, error) {
	for redirects := 0; ; redirects++ {
		conn, err := es.getConn()
		if err != nil {
			return nil, nil, err
		}
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			es.resetConn()
			return nil, nil, err
		}
		if err := WriteWithLength(conn, []byte(cmd)); err != nil {
			es.resetConn()
			return nil, nil, fmt.Errorf("write: %w", err)
		}
		data, err := ReadWithLength(conn)
		if err == nil {
			return conn, data, nil
		}
		es.resetConn()
		var brokerErr *BrokerError
		if redirects >= 3 || !errors.As(err, &brokerErr) || brokerErr.Code != "NOT_LEADER" || brokerErr.Class != ErrorClassRouting {
			return nil, nil, fmt.Errorf("read envelope: %w", err)
		}
		leader := brokerErr.Fields["leader"]
		host, port, addressErr := net.SplitHostPort(leader)
		portNumber, portErr := strconv.Atoi(port)
		if addressErr != nil || host == "" || portErr != nil || portNumber < 1 || portNumber > 65535 {
			return nil, nil, fmt.Errorf("invalid stream leader address %q: %w", leader, err)
		}
		es.mu.Lock()
		es.addr = leader
		es.mu.Unlock()
	}
}

// SaveSnapshot saves a snapshot for an aggregate at the given version.
func (es *EventStore) SaveSnapshot(key string, version uint64, payload string) error {
	cmd := fmt.Sprintf("SAVE_SNAPSHOT topic=%s key=%s version=%d message=%s",
		es.topic, key, version, payload)

	resp, err := es.sendCommand(cmd)
	if err != nil {
		return err
	}
	respStr := strings.TrimSpace(resp)
	if !strings.HasPrefix(respStr, "OK") {
		return fmt.Errorf("unexpected save snapshot response: %s", respStr)
	}
	return nil
}

// ReadSnapshot reads the latest snapshot for an aggregate.
func (es *EventStore) ReadSnapshot(key string) (*Snapshot, error) {
	resp, err := es.sendCommand(fmt.Sprintf("READ_SNAPSHOT topic=%s key=%s", es.topic, key))
	if err != nil {
		return nil, err
	}
	respStr := strings.TrimSpace(resp)
	snapshotJSON, ok, err := parseSnapshotResponse(respStr)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	var snap Snapshot
	if err := json.Unmarshal([]byte(snapshotJSON), &snap); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return &snap, nil
}

// StreamVersion returns the current version of an aggregate stream.
func (es *EventStore) StreamVersion(key string) (uint64, error) {
	resp, err := es.sendCommand(fmt.Sprintf("STREAM_VERSION topic=%s key=%s", es.topic, key))
	if err != nil {
		return 0, err
	}
	respStr := strings.TrimSpace(resp)
	return parseStreamVersionResponse(respStr)
}

func parseSnapshotResponse(respStr string) (string, bool, error) {
	if respStr == "OK snapshot=null" {
		return "", false, nil
	}
	if strings.HasPrefix(respStr, "OK snapshot=") {
		return strings.TrimPrefix(respStr, "OK snapshot="), true, nil
	}
	return "", false, fmt.Errorf("unexpected snapshot response: %s", respStr)
}

func parseStreamVersionResponse(respStr string) (uint64, error) {
	if !strings.HasPrefix(respStr, "OK") {
		return 0, fmt.Errorf("unexpected version response: %s", respStr)
	}
	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "version=") {
			v, err := strconv.ParseUint(strings.TrimPrefix(part, "version="), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse version: %w", err)
			}
			return v, nil
		}
	}
	return 0, fmt.Errorf("missing version in response: %s", respStr)
}

// Close closes the underlying connection.
func (es *EventStore) Close() error {
	es.requestMu.Lock()
	defer es.requestMu.Unlock()

	es.mu.Lock()
	defer es.mu.Unlock()
	if es.conn != nil {
		err := es.conn.Close()
		es.conn = nil
		return err
	}
	return nil
}
