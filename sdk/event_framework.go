package sdk

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// EventEnvelope is the framework-level contract carried by an event-sourcing stream.
type EventEnvelope struct {
	EventID          string          `json:"event_id"`
	EventType        string          `json:"event_type"`
	SchemaVersion    uint32          `json:"schema_version"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      string          `json:"aggregate_id"`
	AggregateVersion uint64          `json:"aggregate_version"`
	OccurredAt       time.Time       `json:"occurred_at"`
	CorrelationID    string          `json:"correlation_id,omitempty"`
	AssociationKey   string          `json:"association_key,omitempty"`
	CausationID      string          `json:"causation_id,omitempty"`
	Payload          json.RawMessage `json:"payload"`
}

// NewEventEnvelope creates a framework event and assigns its identity metadata.
func NewEventEnvelope(aggregateType, aggregateID, eventType string, payload any) (EventEnvelope, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return EventEnvelope{}, fmt.Errorf("marshal event payload: %w", err)
	}
	return EventEnvelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		SchemaVersion: 1,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		OccurredAt:    time.Now().UTC(),
		Payload:       encoded,
	}, nil
}

func (e EventEnvelope) validate() error {
	if e.EventID == "" || e.EventType == "" || e.AggregateType == "" || e.AggregateID == "" {
		return fmt.Errorf("event envelope identity is incomplete")
	}
	if e.SchemaVersion == 0 {
		return fmt.Errorf("event schema version must be positive")
	}
	if e.AggregateVersion == 0 {
		return fmt.Errorf("aggregate version must be positive")
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("event payload must not be empty")
	}
	return nil
}

func (e EventEnvelope) wireEvent() (*Event, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal event envelope: %w", err)
	}
	return &Event{Type: e.EventType, SchemaVersion: e.SchemaVersion, Payload: string(payload)}, nil
}

func decodeEventEnvelope(streamEvent StreamEvent) (EventEnvelope, error) {
	var envelope EventEnvelope
	if err := json.Unmarshal([]byte(streamEvent.Payload), &envelope); err != nil {
		return EventEnvelope{}, fmt.Errorf("decode event envelope at offset %d: %w", streamEvent.Offset, err)
	}
	if envelope.EventType == "" {
		envelope.EventType = streamEvent.Type
	}
	if envelope.SchemaVersion == 0 {
		envelope.SchemaVersion = streamEvent.SchemaVersion
	}
	if envelope.EventType != streamEvent.Type {
		return EventEnvelope{}, fmt.Errorf("event envelope type %q does not match stream type %q", envelope.EventType, streamEvent.Type)
	}
	if envelope.SchemaVersion != streamEvent.SchemaVersion {
		return EventEnvelope{}, fmt.Errorf("event envelope schema version %d does not match stream schema version %d", envelope.SchemaVersion, streamEvent.SchemaVersion)
	}
	if envelope.AggregateVersion == 0 {
		envelope.AggregateVersion = streamEvent.Version
	}
	if envelope.AggregateVersion != streamEvent.Version {
		return EventEnvelope{}, fmt.Errorf("event envelope version %d does not match stream version %d", envelope.AggregateVersion, streamEvent.Version)
	}
	if envelope.AggregateID == "" {
		return EventEnvelope{}, fmt.Errorf("event envelope at offset %d has no aggregate_id", streamEvent.Offset)
	}
	if err := envelope.validate(); err != nil {
		return EventEnvelope{}, fmt.Errorf("invalid event envelope at offset %d: %w", streamEvent.Offset, err)
	}
	return envelope, nil
}

// AppendEnvelope appends a framework event to an aggregate stream.
func (es *EventStore) AppendEnvelope(key string, expectedVersion uint64, envelope EventEnvelope) (*AppendResult, error) {
	if envelope.AggregateID != "" && envelope.AggregateID != key {
		return nil, fmt.Errorf("event aggregate id %q does not match stream key %q", envelope.AggregateID, key)
	}
	if envelope.AggregateVersion != 0 && envelope.AggregateVersion != expectedVersion+1 {
		return nil, fmt.Errorf("event aggregate version %d does not match expected version %d", envelope.AggregateVersion, expectedVersion+1)
	}
	if envelope.AggregateID == "" {
		envelope.AggregateID = key
	}
	if envelope.EventID == "" {
		envelope.EventID = uuid.NewString()
	}
	if envelope.OccurredAt.IsZero() {
		envelope.OccurredAt = time.Now().UTC()
	}
	if envelope.AggregateVersion == 0 {
		envelope.AggregateVersion = expectedVersion + 1
	}
	if envelope.SchemaVersion == 0 {
		envelope.SchemaVersion = 1
	}
	wireEvent, err := envelope.wireEvent()
	if err != nil {
		return nil, err
	}
	return es.Append(key, expectedVersion, wireEvent)
}

// ReadEnvelopes reads and decodes framework events from an aggregate stream.
func (es *EventStore) ReadEnvelopes(key string) ([]EventEnvelope, error) {
	stream, err := es.ReadStream(key)
	if err != nil {
		return nil, err
	}
	result := make([]EventEnvelope, 0, len(stream.Events))
	for _, event := range stream.Events {
		envelope, err := decodeEventEnvelope(event)
		if err != nil {
			return nil, err
		}
		result = append(result, envelope)
	}
	return result, nil
}

// StreamStore is the minimal persistence contract required by AggregateRepository.
type StreamStore interface {
	ReadStream(string) (*StreamData, error)
	Append(string, uint64, *Event) (*AppendResult, error)
}

// Aggregate is the event-sourced aggregate contract.
type Aggregate interface {
	ID() string
	Type() string
	Version() uint64
	Apply(EventEnvelope) error
}

// SnapshotRestorer is optional and allows a repository to restore snapshots.
type SnapshotRestorer interface {
	RestoreSnapshot(payload string, version uint64) error
}

// AggregateFactory creates an empty aggregate for replay.
type AggregateFactory func(id string) Aggregate

// AggregateRepository loads and saves aggregates using optimistic concurrency.
type AggregateRepository struct {
	store   StreamStore
	factory AggregateFactory
}

func NewAggregateRepository(store StreamStore, factory AggregateFactory) (*AggregateRepository, error) {
	if store == nil || factory == nil {
		return nil, fmt.Errorf("stream store and aggregate factory are required")
	}
	return &AggregateRepository{store: store, factory: factory}, nil
}

func (r *AggregateRepository) Load(id string) (Aggregate, error) {
	aggregate := r.factory(id)
	if aggregate == nil {
		return nil, fmt.Errorf("aggregate factory returned nil for %q", id)
	}
	err := walkStoreStream(r.store, id, 0, func(stream *StreamData) error {
		if stream.Snapshot != nil {
			restorer, ok := aggregate.(SnapshotRestorer)
			if !ok {
				return fmt.Errorf("aggregate %q has a snapshot but does not implement SnapshotRestorer", id)
			}
			if err := restorer.RestoreSnapshot(stream.Snapshot.Payload, stream.Snapshot.Version); err != nil {
				return fmt.Errorf("restore aggregate snapshot: %w", err)
			}
		}
		for _, streamEvent := range stream.Events {
			envelope, err := decodeEventEnvelope(streamEvent)
			if err != nil {
				return err
			}
			if envelope.AggregateID != id {
				return fmt.Errorf("event aggregate id %q does not match %q", envelope.AggregateID, id)
			}
			if err := aggregate.Apply(envelope); err != nil {
				return fmt.Errorf("apply %s v%d: %w", envelope.EventType, envelope.AggregateVersion, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return aggregate, nil
}

func walkStoreStream(store StreamStore, key string, fromVersion uint64, visit func(*StreamData) error) error {
	if walker, ok := store.(interface {
		WalkStream(string, uint64, func(*StreamData) error) error
	}); ok {
		return walker.WalkStream(key, fromVersion, visit)
	}
	stream, err := store.ReadStream(key)
	if err != nil {
		return err
	}
	if stream == nil {
		return fmt.Errorf("stream store returned nil")
	}
	if stream.HasMore {
		return ErrStreamPageRequired
	}
	return visit(stream)
}

func (r *AggregateRepository) Save(aggregate Aggregate, events []EventEnvelope) error {
	if aggregate == nil {
		return fmt.Errorf("aggregate is required")
	}
	if len(events) > 1 {
		return fmt.Errorf("saving multiple events is not supported without atomic batch append")
	}
	expected := aggregate.Version()
	for _, event := range events {
		expected++
		if event.AggregateType == "" {
			event.AggregateType = aggregate.Type()
		}
		if event.AggregateID == "" {
			event.AggregateID = aggregate.ID()
		}
		if event.AggregateID != aggregate.ID() {
			return fmt.Errorf("event aggregate id %q does not match aggregate %q", event.AggregateID, aggregate.ID())
		}
		event.AggregateVersion = expected
		wireEvent, err := event.wireEvent()
		if err != nil {
			return fmt.Errorf("prepare %s v%d: %w", event.EventType, expected, err)
		}
		result, err := r.store.Append(aggregate.ID(), expected-1, wireEvent)
		if err != nil {
			return err
		}
		if result.Version != expected {
			return fmt.Errorf("append returned version %d, want %d", result.Version, expected)
		}
		if err := aggregate.Apply(event); err != nil {
			return fmt.Errorf("apply saved %s v%d: %w", event.EventType, expected, err)
		}
	}
	return nil
}
