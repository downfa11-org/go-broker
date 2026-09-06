package transaction

import (
	"fmt"
	"sort"
)

// StateWithoutTopicReferences returns a detached state in which completed
// transactions no longer retain operations for topicName. Active references
// fail closed because deleting their records would invalidate an in-flight
// transaction.
func (m *Manager) StateWithoutTopicReferences(topicName string) (map[string]*Snapshot, []string, error) {
	if m == nil {
		return nil, nil, fmt.Errorf("transaction manager is not available")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return stateWithoutTopicReferencesLocked(m.txns, topicName)
}

// PruneTopicReferences applies StateWithoutTopicReferences atomically to the
// in-memory transaction manager. Distributed TOPIC_DELETE uses this inside the
// serialized FSM apply path.
func (m *Manager) PruneTopicReferences(topicName string) ([]string, error) {
	if m == nil {
		return nil, fmt.Errorf("transaction manager is not available")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state, affected, err := stateWithoutTopicReferencesLocked(m.txns, topicName)
	if err != nil {
		return nil, err
	}
	m.txns = make(map[string]*Transaction, len(state))
	m.sizes = make(map[string]int64, len(state))
	m.totalBytes = 0
	for id, snap := range state {
		m.replaceLocked(id, transactionFromSnapshot(snap))
	}
	return affected, nil
}

func stateWithoutTopicReferencesLocked(current map[string]*Transaction, topicName string) (map[string]*Snapshot, []string, error) {
	next := make(map[string]*Snapshot, len(current))
	affected := make([]string, 0)
	active := make([]string, 0)
	for id, tx := range current {
		if tx == nil {
			continue
		}
		snap := snapshot(tx)
		if !transactionReferencesTopic(snap, topicName) {
			next[id] = snap
			continue
		}
		if !tx.Expired && (tx.State == StateOpen || tx.State == StateCommitting) {
			active = append(active, id)
			next[id] = snap
			continue
		}
		snap.Messages = filterTopicMessages(snap.Messages, topicName)
		snap.Offsets = filterTopicOffsets(snap.Offsets, topicName)
		next[id] = snap
		affected = append(affected, id)
	}
	sort.Strings(active)
	if len(active) != 0 {
		return nil, nil, fmt.Errorf("topic %q is referenced by active transaction(s): %v", topicName, active)
	}
	sort.Strings(affected)
	return next, affected, nil
}

func transactionReferencesTopic(snap *Snapshot, topicName string) bool {
	for _, operation := range snap.Messages {
		if operation.Topic == topicName {
			return true
		}
	}
	for _, operation := range snap.Offsets {
		if operation.Topic == topicName {
			return true
		}
	}
	return false
}

func filterTopicMessages(operations []MessageOperation, topicName string) []MessageOperation {
	filtered := make([]MessageOperation, 0, len(operations))
	for _, operation := range operations {
		if operation.Topic != topicName {
			filtered = append(filtered, operation)
		}
	}
	return filtered
}

func filterTopicOffsets(operations []OffsetOperation, topicName string) []OffsetOperation {
	filtered := make([]OffsetOperation, 0, len(operations))
	for _, operation := range operations {
		if operation.Topic != topicName {
			filtered = append(filtered, operation)
		}
	}
	return filtered
}
