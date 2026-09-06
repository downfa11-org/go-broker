package sdk

import (
	"fmt"
	"sync"
	"time"
)

// RetryPolicy describes bounded retry behavior for a handler or saga worker.
type RetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

func (p RetryPolicy) ShouldRetry(attempt int) bool {
	return p.MaxAttempts > 0 && attempt < p.MaxAttempts
}

func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := p.InitialDelay
	if delay <= 0 {
		delay = time.Second
	}
	multiplier := p.Multiplier
	if multiplier < 1 {
		multiplier = 2
	}
	for i := 1; i < attempt; i++ {
		delay = time.Duration(float64(delay) * multiplier)
		if p.MaxDelay > 0 && delay >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

// CompensationCommand creates a command for a failed saga step.
func CompensationCommand(commandType string, state SagaState, causationID, payload string) Command {
	return Command{Type: commandType, SagaID: state.ID, CorrelationID: state.CorrelationID, CausationID: causationID, Payload: payload}
}

// EventUpcaster transforms an immutable event into the next schema version.
type EventUpcaster func(EventEnvelope) (EventEnvelope, error)

// UpcasterRegistry stores schema migration functions by event type and source version.
type UpcasterRegistry struct {
	mu      sync.RWMutex
	entries map[string]map[uint32]EventUpcaster
}

func NewUpcasterRegistry() *UpcasterRegistry {
	return &UpcasterRegistry{entries: make(map[string]map[uint32]EventUpcaster)}
}

func (r *UpcasterRegistry) Register(eventType string, fromVersion uint32, upcaster EventUpcaster) error {
	if eventType == "" || fromVersion == 0 || upcaster == nil {
		return fmt.Errorf("event type, source version, and upcaster are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries[eventType] == nil {
		r.entries[eventType] = make(map[uint32]EventUpcaster)
	}
	if _, exists := r.entries[eventType][fromVersion]; exists {
		return fmt.Errorf("upcaster already registered for %s v%d", eventType, fromVersion)
	}
	r.entries[eventType][fromVersion] = upcaster
	return nil
}

func (r *UpcasterRegistry) Upcast(event EventEnvelope) (EventEnvelope, error) {
	for {
		r.mu.RLock()
		upcaster := r.entries[event.EventType][event.SchemaVersion]
		r.mu.RUnlock()
		if upcaster == nil {
			return event, nil
		}
		updated, err := upcaster(event)
		if err != nil {
			return EventEnvelope{}, err
		}
		if updated.SchemaVersion <= event.SchemaVersion {
			return EventEnvelope{}, fmt.Errorf("upcaster for %s did not advance schema version", event.EventType)
		}
		event = updated
	}
}

// Replay reads committed stream events and applies optional schema upcasters.
func Replay(store StreamStore, key string, fromVersion uint64, registry *UpcasterRegistry, handler func(EventEnvelope) error) error {
	if store == nil || handler == nil {
		return fmt.Errorf("stream store and replay handler are required")
	}
	if fromVersion == 0 {
		fromVersion = 1
	}
	return walkStoreStream(store, key, fromVersion, func(stream *StreamData) error {
		if stream.Snapshot != nil && stream.Snapshot.Version >= fromVersion {
			return fmt.Errorf("stream store omitted requested event history behind a snapshot")
		}
		for _, raw := range stream.Events {
			event, err := decodeEventEnvelope(raw)
			if err != nil {
				return err
			}
			if fromVersion > 0 && event.AggregateVersion < fromVersion {
				continue
			}
			if registry != nil {
				event, err = registry.Upcast(event)
				if err != nil {
					return err
				}
			}
			if err := handler(event); err != nil {
				return fmt.Errorf("replay %s v%d: %w", event.EventType, event.AggregateVersion, err)
			}
		}
		return nil
	})
}

type deadline struct {
	at       time.Time
	callback func()
}

// DeadlineManager is a deterministic, application-owned deadline registry.
// A worker calls RunDue periodically; persistence belongs to the service.
type DeadlineManager struct {
	mu      sync.Mutex
	entries map[string]deadline
}

func NewDeadlineManager() *DeadlineManager {
	return &DeadlineManager{entries: make(map[string]deadline)}
}

func (m *DeadlineManager) Schedule(id string, at time.Time, callback func()) error {
	if id == "" || callback == nil {
		return fmt.Errorf("deadline id and callback are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[id] = deadline{at: at, callback: callback}
	return nil
}

func (m *DeadlineManager) Cancel(id string) { m.mu.Lock(); defer m.mu.Unlock(); delete(m.entries, id) }

func (m *DeadlineManager) RunDue(now time.Time) int {
	m.mu.Lock()
	var due []deadline
	for id, entry := range m.entries {
		if !entry.at.After(now) {
			due = append(due, entry)
			delete(m.entries, id)
		}
	}
	m.mu.Unlock()
	for _, entry := range due {
		entry.callback()
	}
	return len(due)
}

// FrameworkHelp returns the client-side framework quick reference.
func FrameworkHelp() string {
	return "Cursus Client Framework: NewEventEnvelope, NewAggregateRepository, NewSagaManager, Replay, UpcasterRegistry, RetryPolicy, DeadlineManager"
}
